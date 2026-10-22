#ifndef JCX_RELAIS_LOCALREPO_H
#define JCX_RELAIS_LOCALREPO_H

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/repository/PgRepo.h"
#include "jcailloux/relais/repository/KeyDedup.h"
#include "jcailloux/relais/repository/Precaution.h"
#include "jcailloux/relais/repository/RecheckGuard.h"
#include "jcailloux/relais/repository/RowNotes.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/cache/CacheTier.h"
#include "jcailloux/relais/cache/CacheMetadata.h"
#include "jcailloux/relais/cache/CacheView.h"
#include "jcailloux/relais/cache/MultiView.h"
#include "jcailloux/relais/config/CacheConfig.h"
#include "jcailloux/relais/runtime/CachedClock.h"
#include "jcailloux/relais/cache/Metrics.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais {

/**
 * Repo with L1 RAM cache backed by CacheTier (ChunkMap + GDSF + ghosts).
 *
 * Thin wrapper: all L1 mechanics (find, store, evict, cleanup, inflight dedup,
 * GDSF admission, ghost lifecycle) are delegated to CacheTier<Key, E, Metadata>.
 *
 * LocalRepo adds only domain-specific concerns:
 * - Generation counter for stale write prevention
 * - CacheView wrapping for find() results
 * - Mixin chain delegation (Base = PgRepo)
 *
 * Note: L1 config constraints are verified in Repo.h to avoid
 * eager evaluation issues with std::conditional_t.
 */
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires CacheableEntity<E>
class LocalRepo : public PgRepo<E, Name, Cfg, Key> {
    // Materialize the L1 TTL ns into a scalar constant: GCC miscompiles
    // std::chrono::nanoseconds(Cfg.l1_ttl) — a duration built straight from a
    // class-type NTTP subobject — under -fsanitize=thread (it folds to 0 at the
    // construction site, though a plain field read is fine). Building durations
    // from kL1TtlNs sidesteps it.
    static constexpr int64_t kL1TtlNs = Cfg.l1_ttl.ns;
    static constexpr bool HasTTL = (kL1TtlNs > 0);
    static constexpr bool HasGDSF = cache::GDSFPolicy::enabled;
    static constexpr bool HasCleanup = HasGDSF || HasTTL;

    using Base = PgRepo<E, Name, Cfg, Key>;

    using Metadata = cache::CacheMetadata<HasGDSF, HasTTL>;
    using Tier = cache::CacheTier<Key, E, Metadata>;

    /// Read-fill recheck guard (one static slot array per Name/Key/SlotsLog2).
    using Recheck = RecheckGuard<Name, Key, Cfg.recheck_slots_log2>;

    /// Rows an uncertain write may have changed (see noteRowsChanged).
    using Notes = detail::RowNotes<E, Key, Cfg.l1_chunk_count_log2>;

    /// TTL in seconds (compile-time, for metadata construction).
    static constexpr uint32_t kTtlSec = static_cast<uint32_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::nanoseconds(kL1TtlNs)).count());

public:
    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using FindResultType = cache::CacheView<E>;
    using Base::name;

    static constexpr auto l1Ttl() { return std::chrono::nanoseconds(kL1TtlNs); }

#if RELAIS_ENABLE_METRICS
    static inline cache::L1Counters l1_counters_{};
#endif

    // =========================================================================
    // Queries
    // =========================================================================

    /// Find by ID with L1 -> DB fallback.
    /// L1 hit: zero overhead (Immediate holds CacheView directly, no Task).
    static io::Immediate<cache::CacheView<E>> find(const Key& id) {
        auto hit = lookup(id);
        if (hit) {
            RELAIS_METRICS_INC(l1_counters_.hits);
            return cache::CacheView<E>(
                static_cast<const E*>(hit.value), std::move(hit.guard));
        }
        RELAIS_METRICS_INC(l1_counters_.misses);
        return findSlow(id);
    }

    /// Batched multi-id read. Probes L1 for every (deduplicated) id under a
    /// single batch EpochGuard, then issues one ANY (L3) over the L1 misses.
    /// Returns a guarded MultiView<E>:
    /// view[i] maps to ids[i] (nullptr = absent in every tier), L1 hits are
    /// zero-copy slot pointers kept alive by the batch guard.
    ///
    /// Duplicate ids collapse to one key downstream; their positions share one
    /// entry. Empty ids → empty view (no guard, no I/O, no frame). All-L1-hit →
    /// synchronous Immediate (no coroutine frame).
    static io::Immediate<cache::MultiView<E>> findMany(std::span<const Key> ids) {
        const size_t n = ids.size();
        if (n == 0) return cache::MultiView<E>{};

        auto [unique, slot] = detail::dedupWithSlots<Key>(ids);

        // One batch EpochGuard pins the global epoch for every L1-slot pointer
        // taken below — per-Hit guards are dropped, one ticket covers the N.
        // Acquired before the first probe so any entry read here stays unfreed
        // even if evicted later (held across the await on the miss path).
        auto guard = epoch::EpochGuard::acquire();

        std::vector<const E*> uniqueHit(unique.size(), nullptr);
        std::vector<size_t> missU;
        for (size_t k = 0; k < unique.size(); ++k) {
            auto hit = lookup(unique[k]);
            if (hit) {
                RELAIS_METRICS_INC(l1_counters_.hits);
                uniqueHit[k] = static_cast<const E*>(hit.value);
            } else {
                RELAIS_METRICS_INC(l1_counters_.misses);
                missU.push_back(k);
            }
        }

        // Hot path: every distinct id hit L1 → zero-copy, synchronous, no frame.
        if (missU.empty()) {
            cache::MultiView<E> view(n);
            view.setGuard(std::move(guard));
            for (size_t i = 0; i < n; ++i) view.pointAt(i, uniqueHit[slot[i]]);
            return view;
        }

        return findManySlow(std::move(guard), std::move(unique), std::move(slot),
                            std::move(uniqueHit), std::move(missU));
    }

    /// Find by ID and return JSON string (empty if not found).
    static io::Immediate<std::string> findJson(const Key& id) {
        auto hit = lookup(id);
        if (hit) {
            RELAIS_METRICS_INC(l1_counters_.hits);
            return hit.value->json();
        }
        RELAIS_METRICS_INC(l1_counters_.misses);
        return findJsonSlow(id);
    }

    /// Find by ID and return binary (BEVE) vector (empty if not found).
    static io::Immediate<std::vector<uint8_t>> findBinary(const Key& id)
        requires HasBinarySerialization<E>
    {
        auto hit = lookup(id);
        if (hit) {
            RELAIS_METRICS_INC(l1_counters_.hits);
            return hit.value->binary();
        }
        RELAIS_METRICS_INC(l1_counters_.misses);
        return findBinarySlow(id);
    }

    // =========================================================================
    // Mutations
    // =========================================================================

    /// Insert entity and cache it. Returns epoch-guarded view. With a key the
    /// caller assigns, another loop may write that key meanwhile: the row is
    /// stored through as an update's (see storeThrough). A key the database
    /// assigns has no slot to read before the statement is sent: the row is
    /// stored as is.
    static io::Task<cache::CacheView<E>> insert(const E& entity)
        requires CreatableEntity<E, Key> && (!Cfg.read_only)
    {
        if constexpr (kInsertKeyKnown<E>) {
            const Key key = entity.key();
            const uint64_t snap = Recheck::snapshot(key);
            bool coalesced = false;
            auto result = co_await Base::insertRaw(entity, &coalesced);
            if (result) co_return settleKeyed(key, snap, coalesced, std::move(*result));
        } else {
            auto result = co_await Base::insertRaw(entity);
            if (result) {
                bumpGeneration(result->key());
                co_return storeAndView(result->key(), std::move(*result));
            }
        }
        co_return {};
    }

    /// Upsert entity and cache it (store-through the committed row, see
    /// storeThrough). Returns epoch-guarded view. Unlike insert, a pre-existing
    /// L1 value must be superseded cleanly: onMutation applies the update
    /// penalty to a ghost — this is upsert-L1 = update-L1, not insert. On an
    /// uncertain outcome, evict L1 by precaution (local, cannot fail).
    static io::Task<cache::CacheView<E>> upsert(const E& entity)
        requires UpsertableEntity<E, Key> && HasUpsertSql<E> && (!Cfg.read_only)
    {
        try {
            const Key key = entity.key();
            const uint64_t snap = Recheck::snapshot(key);
            bool coalesced = false;
            auto result = co_await Base::upsertRaw(entity, &coalesced);
            if (result) {
                if (!coalesced) tier().onMutation(result->key());
                co_return settleKeyed(key, snap, coalesced, std::move(*result));
            }
            co_return {};
        } catch (const io::PgUncertainError& e) {
            detail::precautionSync(e.settleTicket(), [key = entity.key()] { evict(key); });
            throw;
        }
    }

    /// Update entity in database with L1 cache handling.
    /// Returns: rows affected (0 if not found), or nullopt on DB error.
    static io::Task<std::optional<size_t>> update(const Key& id, const E& entity)
        requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
    {
        auto outcome = co_await updateOutcome(id, entity);
        co_return outcome.affected;
    }

    /// Partial update as the tiers above see it (WithBefore: see
    /// PgRepo::patchRaw). L1 is left as is until the commit, as for update:
    /// the committed row is then stored through (see settlePatched). Uncertain:
    /// keep the L1 copy in `uncertain_before` (see eraseOutcome), then evict L1
    /// by precaution.
    template<bool WithBefore, typename... Updates>
    static io::Task<typename Base::PatchedRow> patchRow(
        const Key& id, std::optional<E>* uncertain_before, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        try {
            const uint64_t snap = Recheck::snapshot(id);
            auto patched = co_await Base::template patchRaw<WithBefore>(
                id, std::forward<Updates>(updates)...);
            if (!patched) co_return typename Base::PatchedRow{};
            co_return settlePatched(id, snap, std::move(*patched));
        } catch (const io::PgUncertainError& e) {
            keepUncertainBefore(id, uncertain_before);
            detail::precautionSync(e.settleTicket(), [id] { evict(id); });
            throw;
        }
    }

    /// Guarded partial update as the tiers above see it, stored through and
    /// evicted on uncertainty as patchRow. nullopt on DB error. A refused
    /// guard wrote nothing: L1 keeps its entry.
    template<bool WithBefore, typename... Gs, typename... Updates>
    static io::Task<std::optional<typename Base::PatchedRow>> patchIfRow(
        const Key& id, std::optional<E>* uncertain_before,
        const entity::Guards<Gs...>& guard, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        try {
            const uint64_t snap = Recheck::snapshot(id);
            auto outcome = co_await Base::template patchIfRaw<WithBefore>(
                id, guard, std::forward<Updates>(updates)...);
            if (outcome.error) co_return std::nullopt;
            if (!outcome.patched) co_return typename Base::PatchedRow{};
            co_return settlePatched(id, snap, std::move(*outcome.patched));
        } catch (const io::PgUncertainError& e) {
            keepUncertainBefore(id, uncertain_before);
            detail::precautionSync(e.settleTicket(), [id] { evict(id); });
            throw;
        }
    }

    /// Erase entity by ID.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Cfg.read_only)
    {
        auto outcome = co_await eraseOutcome(id);
        co_return outcome.affected;
    }

protected:
    /// Erase returning full outcome (WithBefore: see PgRepo::eraseOutcome).
    /// On an uncertain outcome, the L1 entry is still the version the DELETE
    /// targeted (L1 changes only after a commit): it is copied into
    /// `uncertain_before`, if asked and still empty, just before the
    /// precautionary evict. The normal path copies nothing.
    template<bool WithBefore = false>
    static io::Task<typename Base::EraseOutcome> eraseOutcome(
        const Key& id, const E* hint = nullptr,
        std::optional<E>* uncertain_before = nullptr)
        requires (!Cfg.read_only)
    {
        // Provide L1 hint for partition pruning (free: ~0ns RAM lookup)
        std::optional<E> local_hint;
        if constexpr (HasPartitionHint<E>) {
            if (!hint) {
                auto hit = lookup(id);
                if (hit) { local_hint.emplace(*hit.value); hint = &*local_hint; }
            }
        }

        try {
            auto outcome = co_await Base::template eraseOutcome<WithBefore>(
                id, hint, uncertain_before);
            if (outcome.affected.has_value() && !outcome.coalesced) {
                evict(id);
            }
            co_return outcome;
        } catch (const io::PgUncertainError& e) {
            // Uncertain: the DELETE may have committed. Evict L1 by precaution
            // (local, cannot fail).
            keepUncertainBefore(id, uncertain_before);
            detail::precautionSync(e.settleTicket(), [id] { evict(id); });
            throw;
        }
    }

    /// Update returning full outcome (WithBefore: see PgRepo::updateOutcome).
    /// Write-through stores the committed row, as the database wrote it, not
    /// the caller's entity (see storeThrough). On an uncertain outcome, the L1
    /// entry is kept in `uncertain_before` before the precautionary evict, as
    /// for eraseOutcome.
    template<bool WithBefore = false>
    static io::Task<typename Base::UpdateOutcome> updateOutcome(
        const Key& id, const E& entity,
        std::optional<E>* uncertain_before = nullptr)
        requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
    {
        using enum config::UpdateStrategy;
        constexpr bool kWriteThrough = Cfg.update_strategy == PopulateImmediately;

        try {
            uint64_t snap = 0;
            if constexpr (kWriteThrough) snap = Recheck::snapshot(id);
            auto outcome = co_await Base::template updateOutcome<
                WithBefore, WithBefore || kWriteThrough>(id, entity, uncertain_before);
            if (outcome.affected.value_or(0) > 0 && !outcome.coalesced) {
                if constexpr (!kWriteThrough) {
                    evict(id);
                } else if (!outcome.after) {
                    evict(id);
                } else {
                    tier().onMutation(id);
                    // The tiers above read `after` only with the pre-image.
                    if constexpr (WithBefore)
                        storeThrough(id, snap, std::as_const(*outcome.after));
                    else
                        storeThrough(id, snap, std::move(*outcome.after));
                }
            }
            co_return outcome;
        } catch (const io::PgUncertainError& e) {
            // Uncertain: the UPDATE may have committed. Evict L1 by precaution so
            // the next read re-fetches; the L1 evict is local and cannot fail.
            keepUncertainBefore(id, uncertain_before);
            detail::precautionSync(e.settleTicket(), [id] { evict(id); });
            throw;
        }
    }

public:
    /// Invalidate L1 for a key (generation bump, then evict).
    static io::Task<void> invalidate(const Key& id) {
        evict(id);
        co_return;
    }

    /// Every copy (see PgRepo::invalidateEveryRow): a note with an empty
    /// predicate (see noteRowsChanged), O(1).
    static void invalidateEveryRow(const std::shared_ptr<io::SettleTicket>& ticket) {
        noteRowsChanged(ticket, {});
        Base::invalidateEveryRow(ticket);
    }

    /// Batch invalidation common path — L1 entity tier: point-evict L1 (one per
    /// key, never purgeAll). The L1 entity tier has no deferred work.
    template<bool WithLists = true>
    static io::Task<void> invalidateManyCritical(std::span<const E> entities) {
        for (const auto& e : entities) evict(e.key());
        co_return;
    }

    /// Rows changed in place: the L1 point-evicts. No store-through: the
    /// eviction suffices.
    static io::Task<void> invalidateManyUpdatedCritical(std::span<const Change<E>> changes) {
        for (const auto& c : changes) evict(c.after.key());
        co_return;
    }

    /// Invalidate L1 cache only. Non-coroutine since there is no async work.
    /// Removes both real entries and ghosts.
    /// Increments the generation counter to prevent stale fetches from caching.
    static void evict(const Key& id) {
        bumpGeneration(id);
        tier().evict(id);
    }

    [[nodiscard]] static size_t size() {
        return tier().size();
    }

    // =========================================================================
    // Cleanup
    // =========================================================================

    /// Sweep a specific chunk (called by GDSFPolicy::sweep via sweep_fn).
    static bool sweep(long chunk_id) {
        return tier().sweepChunk(chunk_id, noExtraPred).removed_any;
    }

    /// Sweep all chunks.
    static size_t purge() {
        return tier().purgeAll(noExtraPred);
    }

    /// Prime L1 cache at startup.
    /// ListMixin overrides this via method hiding to also warm up the list cache.
    static void warmup() {
        RELAIS_LOG_DEBUG << name() << ": warming up L1 cache...";
        (void)tier();
        RELAIS_LOG_DEBUG << name() << ": L1 cache primed";
    }

    /// Current average construction time in us (exposed for testing/debugging).
    static float avgConstructionTime() {
        return tier().avgCost();
    }

protected:
    /// Backward-compat alias for test accessors.
    using L1Cache = typename Tier::Map;

    /// Returns the underlying ChunkMap (via CacheTier).
    static L1Cache& cache() { return tier().map(); }

    /// Get from cache as CacheView.
    static cache::CacheView<E> getFromCache(const Key& key) {
        auto hit = lookup(key);
        if (!hit) return {};
        return cache::CacheView<E>(
            static_cast<const E*>(hit.value), std::move(hit.guard));
    }

    /// Put entity in cache (copy). Returns Hit for flexible use.
    /// No tick — the sweep counter is driven by fetch paths (DB misses),
    /// not by local cache mutations (update, insert via API).
    static typename Tier::Hit putInCache(const Key& key, const E& src,
        uint32_t now_sec = runtime::CachedClock::now())
    {
        return tier().store(key, E(src), buildMetadata(now_sec));
    }

    /// Put entity in cache (move). Returns Hit.
    static typename Tier::Hit putInCache(const Key& key, E&& src,
        uint32_t now_sec = runtime::CachedClock::now())
    {
        return tier().store(key, std::move(src), buildMetadata(now_sec));
    }

    /// Build metadata. Accepts pre-computed now_sec to avoid redundant
    /// CachedClock::now() calls.
    static Metadata buildMetadata(uint32_t now_sec = runtime::CachedClock::now()) {
        if constexpr (HasGDSF) {
            uint32_t ttl_sec = 0;
            if constexpr (HasTTL) {
                ttl_sec = now_sec + kTtlSec;
            }
            return Metadata{cache::GDSFScoreData::kCountScale, ttl_sec};
        } else if constexpr (HasTTL) {
            return Metadata{now_sec + kTtlSec};
        } else {
            return Metadata{};
        }
    }

    /// Returns the CacheTier singleton.
    static Tier& tier() { return state().tier; }

    /// Rows an uncertain write may have changed, named by what they satisfied
    /// (`pred`; empty: every row). Until `ticket` settles, a copy the predicate
    /// may hold for is evicted whenever read. Once settled, only the copies
    /// stored before the seal are, and a pass over the cache evicts those left;
    /// found aborted, the note is dropped. Without a pending ticket, the note
    /// is settled at once.
    static void noteRowsChanged(const std::shared_ptr<io::SettleTicket>& ticket,
                                typename Notes::Predicate pred) {
        auto& n = notes();
        if (ticket && ticket->state == io::SettleTicket::State::Pending) {
            const uint64_t id = n.addPending(std::move(pred), &Recheck::bumpAll);
            ticket->replays.emplace_back([id](io::WriteOutcome outcome) -> io::Task<void> {
                if (outcome == io::WriteOutcome::Aborted) {
                    notes().discard(id);
                    co_return;
                }
                notes().settle(id, &Recheck::bumpAll);
                startNoteDrain();
            });
            return;
        }
        n.addSettled(std::move(pred), &Recheck::bumpAll);
        startNoteDrain();
    }

    /// Rows an uncertain write may have erased (see
    /// PgRepo::rowsMayHaveBeenErased): their copies kept, then evicted.
    static io::Task<void> rowsMayHaveBeenErased(std::shared_ptr<io::SettleTicket> ticket,
                                                std::span<const Key> keys,
                                                std::vector<std::optional<E>>* before) {
        for (size_t i = 0; i < keys.size(); ++i)
            keepUncertainBefore(keys[i], before ? &(*before)[i] : nullptr);
        detail::precautionSync(ticket, [ids = std::vector<Key>(keys.begin(), keys.end())] {
            for (const auto& id : ids) evict(id);
        });
        co_await Base::rowsMayHaveBeenErased(std::move(ticket), keys, before);
    }

    /// What the database says of this table's columns, for the predicates of
    /// uncertain writes (see detail::ColumnFacts).
    static const detail::ColumnFacts& columnFacts() { return state().facts; }

    /// Rows an uncertain predicate write may have changed: a note on the
    /// entity tier (see noteRowsChanged), then the tiers below.
    template<typename... Updates>
    static void rowsMayHaveChanged(const std::shared_ptr<io::SettleTicket>& ticket,
                                   const detail::RowPredicate<E>& rows,
                                   const Updates&... updates) {
        readColumnFacts();  // a read that failed is tried again
        noteRowsChanged(ticket, rows);
        Base::rowsMayHaveChanged(ticket, rows, updates...);
    }

private:
    // =========================================================================
    // CacheTier singleton
    // =========================================================================

    struct State {
        Tier tier;
        Notes notes;
        detail::ColumnFacts facts;
    };

    static inline std::atomic<State*> state_ptr_{nullptr};

    /// Hot path: single pointer check (no guard variable, no function call).
    /// First call delegates to state_init_slow() which constructs and registers.
    static State& state() {
        // Acquire: pairs with the release stores in state_init_slow() so a reader
        // that observes the published pointer also observes the enrolled Tier.
        auto* p = state_ptr_.load(std::memory_order_acquire);
        if (p) [[likely]] return *p;
        return state_init_slow();
    }

    static Notes& notes() { return state().notes; }

    /// Cold path: construct CacheTier, register with GDSFPolicy, cache pointer.
    [[gnu::noinline]] static State& state_init_slow() {
        struct Holder {
            State instance;
            Holder() {
                instance.tier.enroll({
                    .sweep_fn = +[](long chunk_id) -> bool {
                        return sweep(chunk_id);
                    },
                    .size_fn = +[]() -> size_t {
                        return size();
                    },
                    .name = static_cast<const char*>(Name)
                });
                // Release: publish only after the Tier is fully enrolled.
                state_ptr_.store(&instance, std::memory_order_release);
            }
        };
        static Holder h;
        // Redundant publish after the thread-safe static guard completes.
        state_ptr_.store(&h.instance, std::memory_order_release);
        readColumnFacts();
        return h.instance;
    }

    /// Read the column facts from the catalog, once, without making the
    /// caller wait. Needs a loop thread: from another, or on a failure, the
    /// next uncertain predicate write tries again. Until read, every column
    /// compares as Maybe.
    static void readColumnFacts() {
        if (!PgProvider::initialized()) return;
        auto& facts = state_ptr_.load(std::memory_order_acquire)->facts;
        if (facts.known() || !facts.claimRead()) return;
        readColumnFactsTask(facts);
    }

#ifdef RELAIS_BUILDING_TESTS
    /// Makes the next `count` reads of the column facts fail.
    static inline int column_facts_faults = 0;
#endif

    static io::DetachedTask readColumnFactsTask(detail::ColumnFacts& facts) {
        try {
#ifdef RELAIS_BUILDING_TESTS
            if (column_facts_faults > 0) {
                --column_facts_faults;
                throw io::PgError("injected: column facts unreadable");
            }
#endif
            io::PgParams params;
            params.push(std::string(E::MappingType::table_name));
            auto result = co_await PgProvider::queryParams(detail::ColumnFacts::kSql, params);
            std::vector<std::string> exact;
            for (int r = 0; r < result.rows(); ++r) {
                if (result[r].template get<bool>(1))
                    exact.push_back(result[r].template get<std::string>(0));
            }
            facts.publish(std::move(exact));
        } catch (const std::exception& e) {
            facts.readFailed();
            RELAIS_LOG_WARN << name() << ": column facts unreadable, read again on the next "
                                         "uncertain predicate write - " << e.what();
        } catch (...) {
            facts.readFailed();
        }
    }

    // =========================================================================
    // Rows an uncertain write may have changed
    // =========================================================================

    /// L1 lookup as every read sees it: a copy a note touches is evicted and
    /// reads as a miss. Without notes, one atomic load more than Tier::find.
    static typename Tier::Hit lookup(const Key& id) {
        auto& s = state();
        auto hit = s.tier.find(id);
        if (hit && s.notes.active()) [[unlikely]] {
            if (evictNoted(s, id, hit)) hit = {};
        }
        return hit;  // one named return: the hit is built in place, never moved
    }

    /// Evict the copy `hit` holds if a note touches it.
    [[gnu::noinline]] static bool evictNoted(State& s, const Key& id,
                                             const typename Tier::Hit& hit) {
        if (!s.notes.touches(id, *hit.value, hit.key_hash)) return false;
        s.tier.evictIfSame(id, hit.value);
        return true;
    }

    /// A copy read from the database (fetch started at `snap`) and stored:
    /// exempt from the settled notes (see RowNotes::exemptStored).
    static void exemptIfNoted(const Key& id, const E& value, uint64_t snap) {
        auto& n = notes();
        if (!n.active()) [[likely]] return;
        n.exemptStored(id, value, [&id, snap] { return !Recheck::changed(id, snap); });
    }

    static void startNoteDrain() {
        if (notes().claimDrain()) drainNotes();
    }

    /// Evict, chunk by chunk, the copies the settled notes touch, handing the
    /// loop back between two chunks; ascending order (see RowNotes).
    static io::DetachedTask drainNotes() {
        auto& n = notes();
        constexpr long kChunks = static_cast<long>(Notes::kChunks);
        try {
            do {
                for (long chunk = 0; chunk < kChunks; ++chunk) {
                    const uint32_t cutoff = n.cutoff();
                    const auto c = static_cast<size_t>(chunk);
                    tier().evictInChunk(chunk, kChunks, [&n, cutoff, c](const Key& key, const E& value) {
                        return n.touchesSettled(key, value, cutoff, c);
                    });
                    n.drainChunk(cutoff, c);
                    co_await PgProvider::yieldToLoop();
                }
            } while (n.continueDraining());
        } catch (const std::exception& e) {
            n.abortDraining();
            RELAIS_LOG_ERROR << name() << ": draining the rows an uncertain write may have "
                                          "changed failed: " << e.what();
        } catch (...) {
            n.abortDraining();
            RELAIS_LOG_ERROR << name() << ": draining the rows an uncertain write may have "
                                          "changed failed";
        }
    }

    // =========================================================================
    // Store + view helper
    // =========================================================================

    /// Store a committed patch through (see storeThrough). A coalesced
    /// follower leaves L1 to its leader, which wrote the same row.
    static typename Base::PatchedRow settlePatched(const Key& id, uint64_t snap,
                                                   typename Base::Patched&& patched) {
        if (patched.coalesced)
            return {std::move(patched.before), uncachedView(std::move(patched.after))};
        tier().onMutation(id);
        return {std::move(patched.before),
                storeThroughAndView(id, snap, std::move(patched.after))};
    }

    /// The committed row of an insert or upsert keyed by the caller, `snap`
    /// read on `key` before the statement was sent. A coalesced follower
    /// leaves L1 to its leader. A row the database stored under another key (a
    /// trigger rewrote it) has no slot read before the statement: that key is
    /// evicted.
    static cache::CacheView<E> settleKeyed(const Key& key, uint64_t snap, bool coalesced,
                                           E&& row) {
        if (coalesced) return uncachedView(std::move(row));
        if (!(row.key() == key)) {
            evict(row.key());
            return uncachedView(std::move(row));
        }
        return storeThroughAndView(key, snap, std::move(row));
    }

    /// Store a committed row through. `snap` is the key's recheck slot, read
    /// before the write was sent. The bump makes a read-fill that fetched the
    /// previous version fail its recheck. PostgreSQL orders two writes on one
    /// row, but each result is handled by the loop that sent it, the older one
    /// possibly last: if the slot moved since `snap`, this row may be older
    /// than what L1 holds, and the key is evicted instead (empty hit). `row`
    /// is moved or copied only once the bump succeeded. A mutation landing
    /// during the store is caught by the check after it, as for a read-fill
    /// (see RecheckGuard::changed).
    template<typename Row>
    static typename Tier::Hit storeThrough(const Key& id, uint64_t snap, Row&& row) {
        if (!Recheck::bumpFrom(id, snap)) {
            tier().evict(id);
            return {};
        }
        auto hit = tier().store(id, E(std::forward<Row>(row)), buildMetadata());
        if (Recheck::changedAfterStore(id, snap + 1)) tier().evictIfSame(id, hit.value);
        return hit;
    }

    /// storeThrough, viewed: a row not stored is still returned, uncached.
    static cache::CacheView<E> storeThroughAndView(const Key& id, uint64_t snap, E&& row) {
        auto hit = storeThrough(id, snap, std::move(row));
        if (!hit) return uncachedView(std::move(row));  // not consumed: not stored
        return cache::CacheView<E>(static_cast<const E*>(hit.value), std::move(hit.guard));
    }

    /// A committed row returned to the caller without being cached.
    static cache::CacheView<E> uncachedView(E&& row) {
        auto hit = tier().transientHit(std::move(row));
        return cache::CacheView<E>(static_cast<const E*>(hit.value), std::move(hit.guard));
    }

    /// Copy the L1 entry into `slot` (if asked and still empty): the version
    /// an uncertain write targeted, kept before the precautionary evict. A
    /// failed copy leaves the pre-image unknown; the evict must still run.
    static void keepUncertainBefore(const Key& id, std::optional<E>* slot) noexcept {
        if (!slot || *slot) return;
        try {
            if (auto hit = lookup(id)) slot->emplace(*hit.value);
        } catch (...) {
        }
    }

    /// Store entity in cache and return CacheView.
    static cache::CacheView<E> storeAndView(const Key& key, E&& src) {
        auto hit = tier().store(key, std::move(src), buildMetadata());
        return cache::CacheView<E>(
            static_cast<const E*>(hit.value), std::move(hit.guard));
    }

    // =========================================================================
    // Slow paths (delegated to CacheTier::findOrFetch with inflight dedup)
    // =========================================================================

    /// Slow path for find(): L1 miss -> dedup -> DB -> cache.
    /// Snapshot the recheck slot at fetch-start; the admit gate skips the L1
    /// store if a mutation landed during the fetch (read-fill recheck).
    static io::Task<cache::CacheView<E>> findSlow(const Key& id) {
        uint64_t snap = Recheck::snapshot(id);
        auto hit = co_await tier().findOrFetch(
            id,
            [&id]() -> io::Task<std::optional<E>> {
                co_return co_await Base::findRaw(id);
            },
            [](const E&, float) -> Metadata { return buildMetadata(); },
            [&id, snap]() { return !Recheck::changed(id, snap); }
        );
        if (hit.meta) exemptIfNoted(id, *hit.value, snap);
        if (hit) {
            co_return cache::CacheView<E>(
                static_cast<const E*>(hit.value), std::move(hit.guard));
        }
        co_return {};
    }

    /// Slow path for findJson(): L1 miss -> dedup -> DB -> cache -> JSON.
    static io::Task<std::string> findJsonSlow(const Key& id) {
        uint64_t snap = Recheck::snapshot(id);
        auto hit = co_await tier().findOrFetch(
            id,
            [&id]() -> io::Task<std::optional<E>> {
                co_return co_await Base::findRaw(id);
            },
            [](const E&, float) -> Metadata { return buildMetadata(); },
            [&id, snap]() { return !Recheck::changed(id, snap); }
        );
        if (hit.meta) exemptIfNoted(id, *hit.value, snap);
        if (hit) co_return hit.value->json();
        co_return std::string{};
    }

    /// Slow path for findBinary(): L1 miss -> dedup -> DB -> cache -> binary.
    static io::Task<std::vector<uint8_t>> findBinarySlow(const Key& id)
        requires HasBinarySerialization<E>
    {
        uint64_t snap = Recheck::snapshot(id);
        auto hit = co_await tier().findOrFetch(
            id,
            [&id]() -> io::Task<std::optional<E>> {
                co_return co_await Base::findRaw(id);
            },
            [](const E&, float) -> Metadata { return buildMetadata(); },
            [&id, snap]() { return !Recheck::changed(id, snap); }
        );
        if (hit.meta) exemptIfNoted(id, *hit.value, snap);
        if (hit) co_return hit.value->binary();
        co_return std::vector<uint8_t>{};
    }

    /// Miss path for findMany: fetch the L1 misses from L3 (one ANY),
    /// force-insert each into L1 — same as the single-find
    /// fetch path in the non-GDSF build — and point the view at the fresh slots.
    /// The batch guard is held across the await so both the pre-await L1 hits
    /// and the freshly stored misses stay valid. Absent ids keep their nullptr.
    static io::Task<cache::MultiView<E>> findManySlow(
        epoch::EpochGuard guard,
        std::vector<Key> unique,
        std::vector<size_t> slot,
        std::vector<const E*> uniqueHit,
        std::vector<size_t> missU)
    {
        std::vector<Key> missIds;
        missIds.reserve(missU.size());
        for (size_t k : missU) missIds.push_back(unique[k]);

        // Snapshot the recheck slots at fetch-start, one per miss key.
        std::vector<uint64_t> snaps;
        snaps.reserve(missIds.size());
        for (const auto& mid : missIds) snaps.push_back(Recheck::snapshot(mid));

        auto fetched = co_await Base::findManyRaw(missIds);

        cache::MultiView<E> view(slot.size());
        view.setGuard(std::move(guard));
        // Worst case: every miss straddled a mutation and is parked un-cached.
        view.reserveOwned(missU.size());

        for (size_t j = 0; j < missU.size(); ++j) {
            if (!fetched[j]) continue;
            if (!Recheck::changed(missIds[j], snaps[j])) {
                Tier::tickSweep();
                auto hit = tier().store(unique[missU[j]], std::move(*fetched[j]),
                                        buildMetadata());
                // A mutation between the check and the store: evict this entry
                // again. The view's guard keeps the value readable.
                if (Recheck::changedAfterStore(missIds[j], snaps[j]))
                    tier().evictIfSame(missIds[j], hit.value);
                else
                    exemptIfNoted(missIds[j], *hit.value, snaps[j]);
                uniqueHit[missU[j]] = static_cast<const E*>(hit.value);
            } else {
                // A mutation straddled the batch fetch → return the value to
                // the caller (mono/batch parity) but do NOT cache it. Parked in
                // the view's owned_, kept alive by the view itself.
                uniqueHit[missU[j]] = view.adoptValue(std::move(*fetched[j]));
            }
        }

        for (size_t i = 0; i < slot.size(); ++i)
            view.pointAt(i, uniqueHit[slot[i]]);
        co_return view;
    }

    // =========================================================================
    // Cleanup predicate (no domain-specific eviction for entities)
    // =========================================================================

    static constexpr auto noExtraPred =
        [](const Key&, const Metadata&, const E&, long) { return false; };

    // =========================================================================
    // Read-fill recheck — stale write prevention (lock-free, cross-thread)
    // =========================================================================
    //
    // Delegates to RecheckGuard. bumpGeneration is the
    // write-side hook; the read side snapshots at fetch-start and gates the
    // store. See RecheckGuard.h for the full rationale.

    /// Bump the generation for a key (called on every confirmed write path).
    static void bumpGeneration(const Key& id) { Recheck::bump(id); }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::relais

#endif //JCX_RELAIS_LOCALREPO_H
