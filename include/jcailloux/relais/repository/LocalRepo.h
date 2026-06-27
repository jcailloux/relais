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
#include "jcailloux/relais/repository/RedisRepo.h"
#include "jcailloux/relais/repository/RecheckGuard.h"
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
 * - Mixin chain delegation (Base = RedisRepo or PgRepo)
 *
 * Supports two modes based on Cfg.cache_level:
 * - CacheLevel::L1:    RAM -> Database (Redis bypassed)
 * - CacheLevel::L1_L2: RAM -> Redis -> Database (full hierarchy)
 *
 * Note: L1 config constraints are verified in Repo.h to avoid
 * eager evaluation issues with std::conditional_t.
 */
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires CacheableEntity<E>
class LocalRepo : public std::conditional_t<
    Cfg.cache_level == config::CacheLevel::L1,
    PgRepo<E, Name, Cfg, Key>,
    RedisRepo<E, Name, Cfg, Key>
> {
    static constexpr bool HasRedis = (Cfg.cache_level == config::CacheLevel::L1_L2);
    // Materialize the L1 TTL ns into a scalar constant: GCC miscompiles
    // std::chrono::nanoseconds(Cfg.l1_ttl) — a duration built straight from a
    // class-type NTTP subobject — under -fsanitize=thread (it folds to 0 at the
    // construction site, though a plain field read is fine). Building durations
    // from kL1TtlNs sidesteps it. Mirrors RedisRepo::kL2TtlNs.
    static constexpr int64_t kL1TtlNs = Cfg.l1_ttl.ns;
    static constexpr bool HasTTL = (kL1TtlNs > 0);
    static constexpr bool HasGDSF = cache::GDSFPolicy::enabled;
    static constexpr bool HasCleanup = HasGDSF || HasTTL;

    using Base = std::conditional_t<
        HasRedis,
        RedisRepo<E, Name, Cfg, Key>,
        PgRepo<E, Name, Cfg, Key>
    >;

    using Metadata = cache::CacheMetadata<HasGDSF, HasTTL>;
    using Tier = cache::CacheTier<Key, E, Metadata>;

    /// Read-fill recheck guard — shared with RedisRepo for the same repo
    /// (same Name/Key/SlotsLog2 → one static slot array seen by both tiers).
    using Recheck = RecheckGuard<Name, Key, Cfg.recheck_slots_log2>;

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

    /// Find by ID with L1 -> (L2) -> DB fallback.
    /// L1 hit: zero overhead (Immediate holds CacheView directly, no Task).
    static io::Immediate<cache::CacheView<E>> find(const Key& id) {
        auto hit = tier().find(id);
        if (hit) {
            RELAIS_METRICS_INC(l1_counters_.hits);
            return cache::CacheView<E>(
                static_cast<const E*>(hit.value), std::move(hit.guard));
        }
        RELAIS_METRICS_INC(l1_counters_.misses);
        return findSlow(id);
    }

    /// Batched multi-id read. Probes L1 for every (deduplicated) id under a
    /// single batch EpochGuard, then issues one MGET (L2) + one ANY (L3) over
    /// the L1 misses via the lower tiers. Returns a guarded MultiView<E>:
    /// view[i] maps to ids[i] (nullptr = absent in every tier), L1 hits are
    /// zero-copy slot pointers kept alive by the batch guard.
    ///
    /// Duplicate ids collapse to one key downstream; their positions share one
    /// entry. Empty ids → empty view (no guard, no I/O, no frame). All-L1-hit →
    /// synchronous Immediate (no coroutine frame). The detached L2 warm-fill of
    /// the L3 misses is handled one layer down (RedisRepo::findManyRaw).
    static io::Immediate<cache::MultiView<E>> findMany(std::span<const Key> ids) {
        const size_t n = ids.size();
        if (n == 0) return cache::MultiView<E>{};

        // Dedup (small-N linear scan, the dominant case): unique[] holds the
        // distinct ids in first-seen order, slot[i] maps ids[i] -> unique index.
        std::vector<Key> unique;
        unique.reserve(n);
        std::vector<size_t> slot(n);
        for (size_t i = 0; i < n; ++i) {
            size_t u = unique.size();
            for (size_t k = 0; k < unique.size(); ++k) {
                if (unique[k] == ids[i]) { u = k; break; }
            }
            if (u == unique.size()) unique.push_back(ids[i]);
            slot[i] = u;
        }

        // One batch EpochGuard pins the global epoch for every L1-slot pointer
        // taken below — per-Hit guards are dropped, one ticket covers the N.
        // Acquired before the first probe so any entry read here stays unfreed
        // even if evicted later (held across the await on the miss path).
        auto guard = epoch::EpochGuard::acquire();

        std::vector<const E*> uniqueHit(unique.size(), nullptr);
        std::vector<size_t> missU;
        for (size_t k = 0; k < unique.size(); ++k) {
            auto hit = tier().find(unique[k]);
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
        auto hit = tier().find(id);
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
        auto hit = tier().find(id);
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

    /// Insert entity and cache it. Returns epoch-guarded view.
    static io::Task<cache::CacheView<E>> insert(const E& entity)
        requires CreatableEntity<E, Key> && (!Cfg.read_only)
    {
        auto result = co_await Base::insertRaw(entity);
        if (result) {
            bumpGeneration(result->key());
            co_return storeAndView(result->key(), std::move(*result));
        }
        co_return {};
    }

    /// Update entity in database with L1 cache handling.
    /// Returns: rows affected (0 if not found), or nullopt on DB error.
    static io::Task<std::optional<size_t>> update(const Key& id, const E& entity)
        requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
    {
        using enum config::UpdateStrategy;

        try {
            auto outcome = co_await Base::updateOutcome(id, entity);
            if (outcome.affected.value_or(0) > 0 && !outcome.coalesced) {
                if constexpr (Cfg.update_strategy == InvalidateAndLazyReload) {
                    evict(id);
                } else {
                    tier().onMutation(id);
                    bumpGeneration(id);
                    tier().store(id, E(entity), buildMetadata());
                }
            }
            co_return outcome.affected;
        } catch (const io::PgQueryTimeout&) {
            // Uncertain: the UPDATE may have committed. Evict L1 by precaution so
            // the next read re-fetches; the L1 evict is local and cannot fail.
            // RedisRepo already evicted L2 before this unwound (L2-before-L1).
            evict(id);
            throw;
        }
    }

    /// Partial update: invalidates L1, delegates to Base::patchRaw,
    /// then moves result into cache.
    template<typename... Updates>
    static io::Task<cache::CacheView<E>> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        tier().onMutation(id);
        bumpGeneration(id);
        tier().evict(id);
        auto entity = co_await Base::patchRaw(id, std::forward<Updates>(updates)...);
        if (entity) {
            co_return storeAndView(id, std::move(*entity));
        }
        co_return {};
    }

    /// Erase entity by ID.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Cfg.read_only)
    {
        // Provide L1 hint for partition pruning (free: ~0ns RAM lookup)
        const E* hint = nullptr;
        std::optional<E> local_hint;
        if constexpr (HasPartitionHint<E>) {
            auto hit = tier().find(id);
            if (hit) { local_hint.emplace(*hit.value); hint = &*local_hint; }
        }

        try {
            auto outcome = co_await Base::eraseOutcome(id, hint);
            if (outcome.affected.has_value() && !outcome.coalesced) {
                evict(id);
            }
            co_return outcome.affected;
        } catch (const io::PgQueryTimeout&) {
            // Uncertain: the DELETE may have committed. Evict L1 by precaution
            // (local, cannot fail). L2 was already evicted as this unwound.
            evict(id);
            throw;
        }
    }

    /// Invalidate L1 and L2 caches for a key.
    static io::Task<void> invalidate(const Key& id) {
        evict(id);
        if constexpr (HasRedis) {
            co_await Base::evictRedis(id);
        }
    }

    /// Batch invalidation common path — L1 entity tier. Delegate to L2/L3
    /// FIRST, then point-evict L1 (one per affected key, each ~0ns RAM). Never
    /// purgeAll: that would drop unrelated hot entries; point-evicts stay exact.
    ///
    /// Order matters — L2-before-L1, matching mono erase (RedisRepo::eraseOutcome
    /// UNLINK then LocalRepo::erase evict). L1-before-L2 leaves a window where a
    /// concurrent L1-miss reads the not-yet-UNLINKed L2 phantom (a deleted row)
    /// and re-stores it into the shared L1 → a PERSISTENT phantom (nothing
    /// re-evicts L1, survives until TTL). With L2 cleared first, a racing reader
    /// can only L1-hit the not-yet-evicted entry (bounded stale, self-heals at
    /// the evict below) — never resurrect a deleted row. The dead generation
    /// counter (bumpGeneration is unconsulted) gates nothing here; the ordering
    /// is the actual anti-stale-write invariant.
    template<bool WithLists = true>
    static io::Task<void> invalidateManyCritical(std::span<const E> entities) {
        co_await Base::template invalidateManyCritical<WithLists>(entities);
        for (const auto& e : entities) evict(e.key());
    }

    /// L1 entity tier has no deferred work — the point-evict is critical RAM.
    /// Pass the deferred cascade down to the own-list / cross-target tiers.
    template<bool WithLists = true>
    static io::Task<void> invalidateManyDeferred(std::span<const E> entities) {
        co_await Base::template invalidateManyDeferred<WithLists>(entities);
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
        auto hit = tier().find(key);
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
    /// Hot path: single pointer check (no guard variable, no function call).
    /// First call delegates to tier_init_slow() which constructs and registers.
    static Tier& tier() {
        // Acquire: pairs with the release stores in tier_init_slow() so a reader
        // that observes the published pointer also observes the enrolled Tier.
        auto* p = tier_ptr_.load(std::memory_order_acquire);
        if (p) [[likely]] return *p;
        return tier_init_slow();
    }

private:
    // =========================================================================
    // CacheTier singleton
    // =========================================================================

    static inline std::atomic<Tier*> tier_ptr_{nullptr};

    /// Cold path: construct CacheTier, register with GDSFPolicy, cache pointer.
    [[gnu::noinline]] static Tier& tier_init_slow() {
        struct Holder {
            Tier instance;
            Holder() {
                instance.enroll({
                    .sweep_fn = +[](long chunk_id) -> bool {
                        return sweep(chunk_id);
                    },
                    .size_fn = +[]() -> size_t {
                        return size();
                    },
                    .name = static_cast<const char*>(Name)
                });
                // Release: publish only after the Tier is fully enrolled.
                tier_ptr_.store(&instance, std::memory_order_release);
            }
        };
        static Holder h;
        // Redundant publish after the thread-safe static guard completes.
        tier_ptr_.store(&h.instance, std::memory_order_release);
        return h.instance;
    }

    // =========================================================================
    // Store + view helper
    // =========================================================================

    /// Store entity in cache and return CacheView.
    static cache::CacheView<E> storeAndView(const Key& key, E&& src) {
        auto hit = tier().store(key, std::move(src), buildMetadata());
        return cache::CacheView<E>(
            static_cast<const E*>(hit.value), std::move(hit.guard));
    }

    // =========================================================================
    // Slow paths (delegated to CacheTier::findOrFetch with inflight dedup)
    // =========================================================================

    /// Slow path for find(): L1 miss -> dedup -> (L2) -> DB -> cache.
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
        if (hit) {
            co_return cache::CacheView<E>(
                static_cast<const E*>(hit.value), std::move(hit.guard));
        }
        co_return {};
    }

    /// Slow path for findJson(): L1 miss -> dedup -> (L2) -> DB -> cache -> JSON.
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
        if (hit) co_return hit.value->json();
        co_return std::string{};
    }

    /// Slow path for findBinary(): L1 miss -> dedup -> (L2) -> DB -> cache -> binary.
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
        if (hit) co_return hit.value->binary();
        co_return std::vector<uint8_t>{};
    }

    /// Miss path for findMany: fetch the L1 misses through the lower tiers (one
    /// MGET + one ANY), force-insert each into L1 — same as the single-find
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
                auto hit = tier().store(unique[missU[j]], std::move(*fetched[j]),
                                        buildMetadata());
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
    // Delegates to RecheckGuard (shared with RedisRepo). bumpGeneration is the
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
