#ifndef JCX_RELAIS_CACHE_CACHETIER_H
#define JCX_RELAIS_CACHE_CACHETIER_H

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "jcailloux/relais/cache/ChunkMap.h"
#include "jcailloux/relais/cache/CacheMetadata.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/cache/TaggedEntry.h"
#include "jcailloux/relais/runtime/CachedClock.h"
#include "jcailloux/relais/runtime/RuntimeThread.h"
#include "jcailloux/relais/io/Task.h"

#include <utils/epoch.h>

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais::cache::detail {

// =============================================================================
// Feature detection concepts
// =============================================================================

/// Metadata that supports GDSF scoring (has access_count via gdsfData()).
template<typename M>
concept GDSFAware = requires(M& m) {
    { m.gdsfData() } -> std::same_as<GDSFScoreData&>;
};

/// Metadata that supports TTL expiration.
template<typename M>
concept Expirable = requires(const M& m, uint32_t now_sec) {
    { m.isExpired(now_sec) } -> std::same_as<bool>;
};

// =============================================================================
// CacheTier<Key, Value, Metadata> — unified L1 cache with GDSF + ghosts
// =============================================================================
//
// Encapsulates the full L1 cache mechanics: ChunkMap storage, GDSF scoring,
// ghost admission control, TTL expiration, inflight dedup, and cleanup.
//
// LocalRepo and ListCache become thin domain wrappers around this.
//
// Template parameters:
//   Key      — lookup key (int64_t, tuple, std::string, ...)
//   Value    — stored value (Entity, ListWrapper, ...)
//   Metadata — per-entry metadata (CacheMetadata variants, ListCacheMetadataImpl)
//
// Feature detection:
//   - GDSFAware<Metadata>      → ghost entries, admission control, GDSF scoring
//   - Expirable<Metadata>      → TTL expiration checks
//
// With GDSF, an entry is counted in bytes (see EntryLayout), and every block
// the L1 holds reported to the L1 ledger (see L1Ledger): the entries and the
// table, and what the tier keeps beside them, which no score weighs: the
// values it served without storing them (transientPool), the fetches in
// flight (one shared state per key and its waiters), and the ghosts a sweep
// collects in its chunk.
//

template<typename Key, typename Value, typename Metadata>
class CacheTier {
public:
    static constexpr bool kHasGDSF = GDSFAware<Metadata> && GDSFPolicy::enabled;
    static constexpr bool kHasGhosts = kHasGDSF;
    static constexpr bool kHasTTL = Expirable<Metadata>;
    static constexpr bool kHasCleanup = kHasGDSF || kHasTTL;

    using Map = ChunkMap<Key, Value, Metadata, kHasGhosts, /*CountsBytes=*/kHasGDSF>;

    /// The bytes an entry costs the cache, the denominator of its score: the
    /// entry, and what its key costs the table (Map::slotBytes). A value is
    /// weighed the same when it is admitted, when it is swept, and when it
    /// waits as a ghost.
    [[nodiscard]] static size_t scoredBytes(const Key& key, size_t entry_bytes) {
        return entry_bytes + Map::slotBytes(key);
    }

    // =========================================================================
    // Hit — result of a cache lookup
    // =========================================================================

    struct Hit {
        Value* value = nullptr;
        Metadata* meta = nullptr;
        epoch::EpochGuard guard;
        size_t key_hash = 0;  // hash of the key, for caller chunk_id computation

        explicit operator bool() const noexcept { return value != nullptr; }
    };

    // =========================================================================
    // Construction
    // =========================================================================

    CacheTier() {
        runtime::detail::RuntimeThread::ensureStarted();
        // The allocator's size classes are read before the first entry is
        // weighed: no request pays for the read.
        if constexpr (kHasGDSF) (void)relais::detail::BlockSizes::process();
    }

    ~CacheTier() = default;

    CacheTier(const CacheTier&) = delete;
    CacheTier& operator=(const CacheTier&) = delete;
    CacheTier(CacheTier&&) = delete;
    CacheTier& operator=(CacheTier&&) = delete;

    // =========================================================================
    // Hot path — synchronous, zero alloc
    // =========================================================================

    /// Find by key. Ghost → miss, TTL expired → evict, GDSF access_count++.
    /// Hit carries key_hash for caller-side chunk_id computation (single-hash).
    Hit find(const Key& key) {
        auto hk = Map::make_key(key);
        auto result = map_.find(hk);
        if (!result) return {};

        // Ghost: treated as miss (slow path handles admission)
        if constexpr (kHasGhosts) {
            if (result.isGhost()) return {};
        }

        auto* ce = result.asReal();

        // TTL check
        if constexpr (kHasTTL) {
            if (ce->metadata.isExpired(runtime::detail::CachedClock::now())) {
                map_.remove_if(key, [ce](auto* e) { return e == ce; });
                return {};
            }
        }

        // GDSF access count bump
        // Relaxed OK: best-effort counter, cleanup is single-threaded per chunk
        if constexpr (kHasGDSF) {
            ce->metadata.gdsfData().access_count.fetch_add(
                GDSFScoreData::kCountScale, std::memory_order_relaxed);
        }

        return Hit{&ce->value, &ce->metadata, std::move(result.guard),
                    Map::get_hash(hk)};
    }

    // =========================================================================
    // Store / evict
    // =========================================================================

    /// Upsert value + metadata, without admission: the store of a write.
    /// Returns Hit pointing to the stored entry. Does NOT tick the sweep
    /// counter. A value read from the database goes through admitFill instead.
    Hit store(const Key& key, Value&& v, Metadata m) {
        auto hk = Map::make_key(key);
        auto r = map_.upsert(hk, std::move(v), std::move(m));
        auto* ce = r.asReal();
        return Hit{&ce->value, &ce->metadata, std::move(r.guard),
                    Map::get_hash(hk)};
    }

    /// Count one addition of memory toward the maintenance sweep, which the
    /// maintenance thread runs on one chunk every few hundred ticks: a store
    /// read from the database, or a list modification tracked. A tier with
    /// nothing to sweep pays nothing.
    static void tickSweep() {
        if constexpr (kHasCleanup) GDSFPolicy::instance().tickInsertion();
    }

    /// Remove real entry + ghost for the given key.
    void evict(const Key& key) {
        if constexpr (kHasGhosts) {
            // Remove ghost if present, then invalidate real
            auto r = map_.find(key);
            if (r && r.isGhost()) {
                map_.remove(key);
            }
        }
        map_.invalidate(key);
    }

    /// Conditional eviction: remove entry only if the value pointer matches,
    /// atomically (a newer entry stored meanwhile stays).
    void evictIfSame(const Key& key, const Value* expected) {
        map_.remove_if(key, [expected](auto* header) {
            auto* ce = static_cast<typename Map::CacheEntry*>(header);
            return &ce->value == expected;
        });
    }

    /// The recheck a value read from the database passes, in two checks
    /// around its store (see admitFill). admits(), before the store: false
    /// sends the value to the caller uncached (a write landed during the
    /// fetch). keeps(value, metadata), once it is stored and behind a seq_cst
    /// fence: false evicts the entry again (a write landed between the first
    /// check and the store). AlwaysAdmit, the default, has nothing to recheck:
    /// no fence, no second check.
    struct AlwaysAdmit {
        static constexpr bool admits() noexcept { return true; }
        static constexpr bool keeps(const Value&, const Metadata&) noexcept { return true; }
    };

    /// The admission of a value read from the database, one decision for
    /// every fill (find, findMany, a list page):
    ///   1. the gate's first check (see AlwaysAdmit);
    ///   2. with GDSF: the count the entry would start from (its ghost's,
    ///      raised by this access); the score it would have after the next
    ///      decay, against the eviction threshold (it would survive the next
    ///      sweep); and the room the budget leaves. A value that fails either
    ///      is not stored: a newcomer leaves a ghost, which counts its next
    ///      accesses;
    ///   3. a tick of the maintenance sweep, when the value is stored, when a
    ///      newcomer is ghosted, or when a ghost qualifies but finds no room
    ///      (demand for room); a ghost read again without qualifying ticks
    ///      nothing;
    ///   4. the store, which takes the ghost's count, then the gate's second
    ///      check.
    ///
    /// Returns the stored entry, `v` moved from; or an empty Hit, `v` left
    /// intact, which the caller serves uncached its own way (transientHit, or
    /// a view that owns it). `cost_us`, when given, is blended into the tier's
    /// cost estimate once the gate admits the value, before its score is
    /// computed. `make_meta()` builds the entry's metadata, only once the
    /// value is to be stored.
    template<typename MakeMeta, typename Gate = AlwaysAdmit>
    Hit admitFill(const Key& key, Value& v, std::optional<float> cost_us,
                  MakeMeta&& make_meta, const Gate& gate = {}) {
        if (!gate.admits()) return {};

        [[maybe_unused]] uint32_t count = 0;
        [[maybe_unused]] bool has_ghost = false;
        if constexpr (kHasGDSF) {
            if (cost_us) recordCost(*cost_us);
            auto& policy = GDSFPolicy::instance();

            // What the entry would cost (independent of the ghost lookup).
            const uint32_t est_bytes = saturate32(scoredBytes(key, Map::entryBytes(v)));
            constexpr uint8_t est_flags = 0;

            // Scoped: the guard the lookup holds keeps what a concurrent sweep
            // retires allocated, and counted by the L1 ledger, until released.
            {
                auto ghost = map_.find(key);
                has_ghost = ghost && ghost.isGhost();
                count = (has_ghost ? ghost.ghostCount() : 0) + GDSFScoreData::kCountScale;
                if (has_ghost) {
                    map_.update_ghost(key, [count, est_bytes](TaggedEntry te) {
                        return te.withGhostCount(count).withGhostBytes(est_bytes, est_flags);
                    });
                }
            }

            const float avg_cost = avg_cost_us_.load(std::memory_order_relaxed);
            const float score = static_cast<float>(count) * policy.decayRate() * avg_cost
                              / static_cast<float>(std::max(est_bytes, uint32_t{1}));
            const bool qualifies = score >= policy.threshold();
            if (!qualifies || policy.isOverBudget(est_bytes)) {
                if (!has_ghost) {
                    map_.insert_ghost(key, count, est_bytes, est_flags);
                    tickSweep();
                } else if (qualifies) {
                    tickSweep();
                }
                return {};
            }
            tickSweep();
            policy.recordAdmission(est_bytes);
        } else {
            tickSweep();
        }

        auto meta = make_meta();  // before the store moves `v` away
        auto hk = Map::make_key(key);
        auto r = map_.upsert(hk, std::move(v), std::move(meta));
        if constexpr (kHasGDSF) {
            if (has_ghost) {
                r.entry()->metadata.gdsfData().access_count.store(
                    count, std::memory_order_relaxed);
            }
        }
        auto* ce = r.asReal();

        // The first check is a check-then-act: a write on another thread can
        // land and evict between it and the store, and the store then outlives
        // the evict. The second check, after the store, closes the gap: either
        // it sees the write and removes this entry (only this one: a newer
        // store is kept), or the write comes after the store and its eviction,
        // sequenced after it, finds the entry. The fence orders the store
        // before the gate's reload; the write carries the matching fence after
        // it is published. The returned Hit's guard keeps the value readable.
        if constexpr (!std::is_same_v<Gate, AlwaysAdmit>) {
            std::atomic_thread_fence(std::memory_order_seq_cst);
            if (!gate.keeps(std::as_const(ce->value), std::as_const(ce->metadata)))
                evictIfSame(key, &ce->value);
        }
        return Hit{&ce->value, &ce->metadata, std::move(r.guard), Map::get_hash(hk)};
    }

    /// Return a value to the caller WITHOUT caching it — kept alive by the
    /// returned EpochGuard in the transient pool. Used when a fill is not
    /// stored (see admitFill) or a store-through is refused.
    Hit transientHit(Value&& v) {
        auto guard = epoch::EpochGuard::acquire();
        auto* ptr = transientPool().New(std::move(v));
        transientPool().Retire(ptr);
        return Hit{ptr, nullptr, std::move(guard)};
    }

    // =========================================================================
    // Cold path — async, deduped, GDSF admission
    // =========================================================================

    /// Find in cache or fetch asynchronously. Coalescences concurrent misses.
    ///
    /// Fetcher: () -> Task<optional<Value>>
    /// MetaBuilder: (const Value&, float elapsed_us) -> Metadata
    /// AdmitGate: the recheck of the fill (see AlwaysAdmit), always-admit by
    ///   default.
    ///
    /// Returns Hit pointing to the cached entry, or empty if not found. A
    /// value the admission does not store (see admitFill) is returned through
    /// the transient pool.
    template<typename Fetcher, typename MetaBuilder, typename AdmitGate = AlwaysAdmit>
    io::Task<Hit> findOrFetch(const Key& key, Fetcher&& f, MetaBuilder&& mb,
                              AdmitGate gate = {}) {
        auto [entry, is_leader] = inflightMap().acquire(key);

        if (is_leader) {
            Hit result;
            try {
                result = co_await fetchAndAdmit(
                    key, std::forward<Fetcher>(f), std::forward<MetaBuilder>(mb),
                    gate);
                {
                    std::lock_guard lk(entry->mu);
                    entry->outcome = result
                        ? InflightEntry::Outcome::found
                        : InflightEntry::Outcome::not_found;
                    // Release: publishes outcome to follower coroutines
                    entry->done.store(true, std::memory_order_release);
                }
            } catch (...) {
                {
                    std::lock_guard lk(entry->mu);
                    entry->outcome = InflightEntry::Outcome::error;
                    entry->error = std::current_exception();
                    // Release: publishes outcome to follower coroutines
                    entry->done.store(true, std::memory_order_release);
                }
                for (auto h : entry->waiters) h.resume();
                inflightMap().erase(key);
                throw;
            }

            typename InflightEntry::Waiters to_resume;
            {
                std::lock_guard lk(entry->mu);
                to_resume = std::move(entry->waiters);
            }
            for (auto h : to_resume) h.resume();
            inflightMap().erase(key);
            co_return result;
        }

        // Follower: wait for leader
        co_await DedupAwaiter{entry};

        switch (entry->outcome) {
        case InflightEntry::Outcome::found: {
            auto hit = find(key);
            if (hit) co_return hit;
            // Evicted between leader store and follower read — fallback
            co_return co_await fetchAndAdmit(
                key, std::forward<Fetcher>(f), std::forward<MetaBuilder>(mb),
                gate);
        }
        case InflightEntry::Outcome::not_found:
            co_return Hit{};
        case InflightEntry::Outcome::error:
            std::rethrow_exception(entry->error);
        default:
            co_return Hit{};
        }
    }

    // =========================================================================
    // Cleanup
    // =========================================================================

    /// Context passed to cleanup predicates. The ghosts to insert and to
    /// decay once the chunk is done, each with a copy of its key, are counted
    /// while held (see LedgerList).
    struct CleanupContext {
        uint32_t now_sec;
        float threshold;
        struct GhostCandidate { Key key; uint32_t count; uint32_t bytes; uint8_t flags; };
        using GhostCandidates = LedgerList<GhostCandidate, kHasGDSF>;
        GhostCandidates* ghost_candidates = nullptr;
        struct GhostDecay { Key key; uint32_t decayed_count; };
        using GhostDecays = LedgerList<GhostDecay, kHasGDSF>;
        GhostDecays* ghost_decays = nullptr;
    };

    struct SweepResult {
        size_t removed = 0;  // entries removed from the map
        long chunk_id = -1;  // -1: nothing to sweep on this tier
    };

    /// Sweep one chunk. chunk_id is determined by GDSFPolicy (global cursor).
    /// ExtraPred(key, meta, value, chunk_id) → bool adds domain-specific eviction.
    /// Returns the entries removed, and chunk_id for post-sweep operations
    /// (e.g., drainChunk).
    template<typename ExtraPred>
    SweepResult sweepChunk(long chunk_id, ExtraPred&& extra) {
        return sweepChunk(chunk_id, GDSFPolicy::instance().chunkCount(),
                          std::forward<ExtraPred>(extra));
    }

    /// Sweep chunk `chunk_id` of the buckets split in `n_chunks`, for a caller
    /// that counts its own chunks (a list cache and its tracker).
    template<typename ExtraPred>
    SweepResult sweepChunk(long chunk_id, long n_chunks, ExtraPred&& extra) {
        if constexpr (!kHasCleanup) {
            return {};
        } else {
            assert(map_.num_buckets() >= n_chunks);
            auto& policy = GDSFPolicy::instance();
            CleanupContext ctx{runtime::detail::CachedClock::now(),
                               kHasGDSF ? policy.threshold() : 0.0f};

            typename CleanupContext::GhostCandidates candidates;
            typename CleanupContext::GhostDecays ghost_decays;
            if constexpr (kHasGDSF) {
                ctx.ghost_candidates = &candidates;
                ctx.ghost_decays = &ghost_decays;
            }

            size_t removed = 0;

            if constexpr (kHasGDSF) {
                removed = map_.cleanup_chunk(chunk_id, n_chunks,
                    [this, &ctx, &extra, chunk_id](const Key& key, auto te) {
                        if (te.isGhost())
                            return ghostCleanupPred(key, te, ctx);
                        auto* ce = static_cast<typename Map::CacheEntry*>(
                            te.template asReal<typename Map::EntryHeader>());
                        return realCleanupPred(key, *ce, ctx)
                            || extra(key, ce->metadata, ce->value, chunk_id);
                    });
            } else {
                removed = map_.cleanup_chunk(chunk_id, n_chunks,
                    [this, &ctx, &extra, chunk_id](const Key& key, auto te) {
                        auto* ce = static_cast<typename Map::CacheEntry*>(
                            te.template asReal<typename Map::EntryHeader>());
                        return realCleanupPred(key, *ce, ctx)
                            || extra(key, ce->metadata, ce->value, chunk_id);
                    });
            }

            if (removed > 0) map_.reclaim();

            // Post-sweep: apply ghost decays + insertions
            if constexpr (kHasGDSF) {
                // update_ghost runs under its caller's guard: during a growth,
                // it copies buckets and walks overflow links that writers
                // retire meanwhile.
                auto guard = epoch::EpochGuard::acquire();
                for (auto& gd : ghost_decays) {
                    map_.update_ghost(gd.key, [&gd](TaggedEntry te) {
                        return te.withGhostCount(gd.decayed_count);
                    });
                }
                for (auto& gc : candidates) {
                    map_.insert_ghost(gc.key, gc.count, gc.bytes, gc.flags);
                }
            }

            return SweepResult{removed, chunk_id};
        }
    }

    /// Evict the entries of one chunk that pred(key, value) selects; ghosts
    /// stay. Chunks split the buckets as sweepChunk does, in `n_chunks`.
    template<typename Pred>
    size_t evictInChunk(long chunk_id, long n_chunks, Pred&& pred) {
        assert(map_.num_buckets() >= n_chunks);
        size_t removed = map_.cleanup_chunk(chunk_id, n_chunks,
            [&pred](const Key& key, auto te) {
                if constexpr (kHasGhosts) {
                    if (te.isGhost()) return false;
                }
                auto* ce = static_cast<typename Map::CacheEntry*>(
                    te.template asReal<typename Map::EntryHeader>());
                return pred(key, std::as_const(ce->value));
            });
        if (removed > 0) map_.reclaim();
        return removed;
    }

    /// Visit the metadata and value of every entry (ghosts skipped),
    /// read-only. Linear in the table: never on a request's path.
    template<typename Fn>
    void forEachEntry(Fn&& fn) {
        map_.for_each([&fn](const Key&, auto te) {
            if constexpr (kHasGhosts) {
                if (te.isGhost()) return;
            }
            auto* ce = static_cast<typename Map::CacheEntry*>(
                te.template asReal<typename Map::EntryHeader>());
            fn(std::as_const(ce->metadata), std::as_const(ce->value));
        });
    }

    // =========================================================================
    // Ghost write-path
    // =========================================================================

    /// Remove ghost + apply update penalty (called on write paths).
    void onMutation(const Key& key) {
        if constexpr (kHasGDSF) {
            auto r = map_.find(key);
            if (r && r.isGhost()) {
                // Apply update penalty before removal
                uint32_t count = r.ghostCount();
                uint32_t penalized = static_cast<uint32_t>(
                    static_cast<float>(count) * GDSFScoreData::kUpdatePenalty);
                map_.update_ghost(key, [penalized](TaggedEntry te) {
                    return te.withGhostCount(penalized);
                });
            }
        }
    }

    /// Remove ghost entry only (no penalty). Used by evict paths.
    void removeGhost(const Key& key) {
        if constexpr (kHasGDSF) {
            auto r = map_.find(key);
            if (r && r.isGhost()) {
                map_.remove(key);
            }
        }
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /// Register with GDSFPolicy for global sweep coordination.
    /// sweep_fn and size_fn are the caller's callbacks.
    void enroll(RepoRegistryEntry desc) {
        if constexpr (kHasCleanup) {
            GDSFPolicy::instance().enroll(std::move(desc));
        }
        if constexpr (kHasGDSF) {
            long nc = GDSFPolicy::instance().chunkCount();
            chunk_bits_ = static_cast<uint8_t>(
                std::countr_zero(static_cast<unsigned long>(nc)));
            chunk_mask_ = static_cast<size_t>(nc - 1);
        }
    }

    [[nodiscard]] size_t size() {
        return static_cast<size_t>(map_.size());
    }

    /// Current average construction cost (EMA, microseconds).
    [[nodiscard]] float avgCost() const {
        return avg_cost_us_.load(std::memory_order_relaxed);
    }

    /// Record a construction cost measurement (updates EMA).
    void recordCost(float elapsed_us) {
        constexpr float kAlpha = 0.1f;  // EMA smoothing: 10% new, 90% history
        float old_avg = avg_cost_us_.load(std::memory_order_relaxed);
        float new_avg;
        if (old_avg == 0.0f) {
            new_avg = elapsed_us;  // First measurement: seed the EMA
        } else {
            new_avg = kAlpha * elapsed_us + (1.0f - kAlpha) * old_avg;
        }
        avg_cost_us_.compare_exchange_weak(old_avg, new_avg,
            std::memory_order_relaxed);
    }

    /// Transient pool for values returned without caching (ghost REJECT path).
    /// With GDSF, each value is counted from its construction to its release,
    /// once no reader holds it: its pool block, read on it, and what it owns.
    epoch::memory_pool<Value>& transientPool() {
        static auto* p = [] {
            auto* pool = new epoch::memory_pool<Value>();
            if constexpr (kHasGDSF) {
                static constexpr epoch::pool_accounting<Value> kAccounting{&kL1Ledger, &weighTransient};
                pool->set_accounting(&kAccounting);
            }
            return pool;
        }();
        return *p;
    }

    /// Direct access to the underlying ChunkMap (for tests and advanced use).
    Map& map() { return map_; }
    const Map& map() const { return map_; }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif

private:
    /// A value of the transient pool: its block, read on it, and what it owns.
    static size_t weighTransient(const Value& v, const void* block, size_t block_size) {
        return relais::detail::blockBytes(block_size, block) + relais::detail::heapBytes(v);
    }

    Map map_;
    std::atomic<float> avg_cost_us_{0.0f};
    uint8_t chunk_bits_{0};
    size_t chunk_mask_{0};

    // =========================================================================
    // Inflight dedup
    // =========================================================================

    // With GDSF, a fetch in flight counts what it holds while it lasts: its
    // map node, the shared state (allocate_shared: one block with its counts),
    // the waiters' array, and what the key owns.
    template<typename T>
    using InflightAllocator = LedgerAllocatorIf<kHasGDSF, T>;
    static constexpr bool kCountsInflightKeys =
        kHasGDSF && !relais::detail::heap_bytes::owns_nothing<Key>;

    struct InflightEntry {
        using Waiters = std::vector<std::coroutine_handle<>, InflightAllocator<std::coroutine_handle<>>>;

        std::mutex mu;
        std::atomic<bool> done{false};
        Waiters waiters;

        enum class Outcome : uint8_t { pending, found, not_found, error };
        Outcome outcome = Outcome::pending;
        std::exception_ptr error;
    };

    struct ShardedInflightMap {
        static constexpr int kShards = 16;
        using Slot = std::pair<const Key, std::shared_ptr<InflightEntry>>;
        struct Shard {
            std::mutex mu;
            std::unordered_map<Key, std::shared_ptr<InflightEntry>, AutoHash<Key>,
                               std::equal_to<Key>, InflightAllocator<Slot>> map;
        };
        std::array<Shard, kShards> shards;

        Shard& shard_for(const Key& k) {
            return shards[AutoHash<Key>{}(k)
                          & static_cast<size_t>(kShards - 1)];
        }

        struct AcquireResult {
            std::shared_ptr<InflightEntry> entry;
            bool is_leader;
        };

        AcquireResult acquire(const Key& k) {
            auto& s = shard_for(k);
            std::lock_guard lk(s.mu);
            auto [it, inserted] = s.map.try_emplace(k);
            if constexpr (kCountsInflightKeys) {
                if (inserted) L1Ledger::add(keyHeap(it->first));
            }
            auto& slot = it->second;
            if (!slot) {
                slot = std::allocate_shared<InflightEntry>(InflightAllocator<InflightEntry>{});
                return {slot, true};
            }
            return {slot, false};
        }

        void erase(const Key& k) {
            auto& s = shard_for(k);
            std::lock_guard lk(s.mu);
            auto it = s.map.find(k);
            if (it == s.map.end()) return;
            if constexpr (kCountsInflightKeys) L1Ledger::add(-keyHeap(it->first));
            s.map.erase(it);
        }
    };

    struct DedupAwaiter {
        std::shared_ptr<InflightEntry> entry;

        bool await_ready() const noexcept {
            // Acquire: pairs with leader's release on done
            return entry->done.load(std::memory_order_acquire);
        }

        bool await_suspend(std::coroutine_handle<> h) {
            std::lock_guard lk(entry->mu);
            if (entry->done.load(std::memory_order_relaxed)) {
                return false;
            }
            entry->waiters.push_back(h);
            return true;
        }

        void await_resume() noexcept {}
    };

    static ShardedInflightMap& inflightMap() {
        static ShardedInflightMap map;
        return map;
    }

    // =========================================================================
    // Fetch + admission
    // =========================================================================

    /// The miss path of findOrFetch: the fetch, then the admission every fill
    /// passes (admitFill). The fetch's wall time is the cost sample.
    template<typename Fetcher, typename MetaBuilder, typename AdmitGate>
    io::Task<Hit> fetchAndAdmit(const Key& key, Fetcher&& f, MetaBuilder&& mb,
                                AdmitGate gate) {
        std::chrono::steady_clock::time_point start{};
        if constexpr (kHasGDSF) start = std::chrono::steady_clock::now();
        auto opt = co_await f();
        if (!opt) co_return Hit{};

        float elapsed_us = 0.0f;
        if constexpr (kHasGDSF) {
            elapsed_us = static_cast<float>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - start).count());
        }
        auto hit = admitFill(key, *opt, elapsed_us,
                             [&] { return mb(std::as_const(*opt), elapsed_us); }, gate);
        if (hit) co_return hit;
        co_return transientHit(std::move(*opt));
    }

    /// A byte count as a uint32_t, saturated.
    static uint32_t saturate32(size_t n) {
        return static_cast<uint32_t>(std::min<size_t>(n, UINT32_MAX));
    }

    // =========================================================================
    // Cleanup predicates
    // =========================================================================

    /// Real entry cleanup: decay + score + histogram + TTL.
    bool realCleanupPred(const Key& key, const typename Map::CacheEntry& entry,
                          CleanupContext& ctx) {
        const Metadata& meta = entry.metadata;
        if constexpr (kHasGDSF) {
            auto& gdsf = const_cast<Metadata&>(meta).gdsfData();

            // Inline decay
            float dr = GDSFPolicy::instance().decayRate();
            uint32_t old_count = gdsf.rawCount();
            gdsf.access_count.store(
                static_cast<uint32_t>(static_cast<float>(old_count) * dr),
                std::memory_order_relaxed);

            // Score
            size_t entry_bytes = entry.bytes();
            size_t scored = scoredBytes(key, entry_bytes);
            float avg_cost = avg_cost_us_.load(std::memory_order_relaxed);
            float score = gdsf.computeScore(avg_cost, scored);

            // Histogram: what evicting the entry frees. Its key stays in the
            // table, as a ghost or a free slot of the bucket array.
            GDSFPolicy::instance().recordEntry(score, entry_bytes);

            // TTL
            if constexpr (kHasTTL) {
                if (meta.isExpired(ctx.now_sec)) return true;
            }

            // Score threshold — evicted entries become ghost candidates
            if (score < ctx.threshold) {
                if (ctx.ghost_candidates) {
                    ctx.ghost_candidates->push_back({key, old_count,
                        saturate32(scored), 0});
                }
                return true;
            }

            return false;
        } else if constexpr (kHasTTL) {
            return meta.isExpired(ctx.now_sec);
        } else {
            return false;
        }
    }

    /// Ghost cleanup: decay or remove. Surviving ghosts are collected for post-sweep update.
    bool ghostCleanupPred(const Key& key, TaggedEntry te,
                           CleanupContext& ctx) {
        float dr = GDSFPolicy::instance().decayRate();
        uint32_t count = te.ghostCount();
        uint32_t decayed = static_cast<uint32_t>(
            static_cast<float>(count) * dr);
        if (decayed == 0) {
            GDSFPolicy::instance().recordEntry(0.0f, Map::slotBytes(key));
            return true;
        }
        if (ctx.ghost_decays) {
            ctx.ghost_decays->push_back({key, decayed});
        }
        return false;
    }
};

}  // namespace jcailloux::relais::cache::detail

#endif  // JCX_RELAIS_CACHE_CACHETIER_H
