#ifndef JCX_RELAIS_CACHE_CACHETIER_H
#define JCX_RELAIS_CACHE_CACHETIER_H

#include <array>
#include <atomic>
#include <bit>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "jcailloux/relais/cache/ChunkMap.h"
#include "jcailloux/relais/cache/GDSFMetadata.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/cache/TaggedEntry.h"
#include "jcailloux/relais/config/CachedClock.h"
#include "jcailloux/relais/io/Task.h"

#include <utils/epoch.h>

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais::cache {

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

/// Value that can report its memory usage.
template<typename V>
concept MemoryTrackable = requires(const V& v) {
    { v.memoryUsage() } -> std::convertible_to<size_t>;
};

// =============================================================================
// CacheTier<Key, Value, Metadata> — unified L1 cache with GDSF + ghosts
// =============================================================================
//
// Encapsulates the full L1 cache mechanics: ChunkMap storage, GDSF scoring,
// ghost admission control, TTL expiration, inflight dedup, and cleanup.
//
// CachedRepo and ListCache become thin domain wrappers around this.
//
// Template parameters:
//   Key      — lookup key (int64_t, tuple, std::string, ...)
//   Value    — stored value (EntityWrapper, ListWrapper, ...)
//   Metadata — per-entry metadata (CacheMetadata variants, ListCacheMetadataImpl)
//
// Feature detection:
//   - GDSFAware<Metadata>      → ghost entries, admission control, GDSF scoring
//   - Expirable<Metadata>      → TTL expiration checks
//   - MemoryTrackable<Value>   → memory-aware GDSF scoring
//

template<typename Key, typename Value, typename Metadata>
class CacheTier {
public:
    static constexpr bool kHasGDSF = GDSFAware<Metadata> && GDSFPolicy::enabled;
    static constexpr bool kHasGhosts = kHasGDSF;
    static constexpr bool kHasTTL = Expirable<Metadata>;
    static constexpr bool kHasCleanup = kHasGDSF || kHasTTL;

    using Map = ChunkMap<Key, Value, Metadata, kHasGhosts>;

    // --- Entry overhead constants ---

    template<typename T>
    struct EpochWrapperMirror_ { T value; };

    static constexpr size_t kEpochWrapperOverhead =
        sizeof(EpochWrapperMirror_<typename Map::CacheEntry>)
        - sizeof(typename Map::CacheEntry);

    /// Overhead per live entry beyond the Value itself.
    static constexpr size_t kEntryOverhead =
        sizeof(typename Map::CacheEntry) - sizeof(Value) + kEpochWrapperOverhead;

    /// Bucket slot size in the hash map (for score denominator).
    static constexpr size_t kBucketSlotSize =
        sizeof(Key) + sizeof(TaggedEntry);

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
        config::CachedClock::ensureStarted();
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
            if (ce->metadata.isExpired(config::CachedClock::now())) {
                map_.remove_if(key, [ce](auto* e) { return e == ce; });
                return {};
            }
        }

        // GDSF access count bump
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

    /// Upsert value + metadata. Returns Hit pointing to the stored entry.
    /// Upsert value + metadata. Returns Hit pointing to the stored entry.
    /// Does NOT tick the sweep counter — ticking is the fetch path's
    /// responsibility (fetchAndAdmit calls tickInsertion on every DB miss).
    Hit store(const Key& key, Value&& v, Metadata m) {
        auto hk = Map::make_key(key);
        auto r = map_.upsert(hk, std::move(v), std::move(m));
        auto* ce = r.asReal();
        return Hit{&ce->value, &ce->metadata, std::move(r.guard),
                    Map::get_hash(hk)};
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

    /// Conditional eviction: remove entry only if the value pointer matches.
    /// Used by ListCache for stale-entry removal after modification checks.
    void evictIfSame(const Key& key, const Value* expected) {
        map_.remove_if(key, [expected](auto* header) {
            auto* ce = static_cast<typename Map::CacheEntry*>(header);
            return &ce->value == expected;
        });
    }

    // =========================================================================
    // Cold path — async, deduped, GDSF admission
    // =========================================================================

    /// Find in cache or fetch asynchronously. Coalescences concurrent misses.
    ///
    /// Fetcher: () -> Task<optional<Value>>
    /// MetaBuilder: (const Value&, float elapsed_us) -> Metadata
    ///
    /// Returns Hit pointing to the cached entry, or empty if not found.
    /// When GDSF has admission pressure, the entry may be ghosted instead
    /// of cached (returned via transient pool in that case).
    template<typename Fetcher, typename MetaBuilder>
    io::Task<Hit> findOrFetch(const Key& key, Fetcher&& f, MetaBuilder&& mb) {
        auto [entry, is_leader] = inflightMap().acquire(key);

        if (is_leader) {
            Hit result;
            try {
                result = co_await fetchAndAdmit(
                    key, std::forward<Fetcher>(f), std::forward<MetaBuilder>(mb));
                {
                    std::lock_guard lk(entry->mu);
                    entry->outcome = result
                        ? InflightEntry::Outcome::found
                        : InflightEntry::Outcome::not_found;
                    entry->done.store(true, std::memory_order_release);
                }
            } catch (...) {
                {
                    std::lock_guard lk(entry->mu);
                    entry->outcome = InflightEntry::Outcome::error;
                    entry->error = std::current_exception();
                    entry->done.store(true, std::memory_order_release);
                }
                for (auto h : entry->waiters) h.resume();
                inflightMap().erase(key);
                throw;
            }

            std::vector<std::coroutine_handle<>> to_resume;
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
                key, std::forward<Fetcher>(f), std::forward<MetaBuilder>(mb));
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

    /// Context passed to cleanup predicates.
    struct CleanupContext {
        uint32_t now_sec;
        float threshold;
        struct GhostCandidate { Key key; uint32_t count; uint32_t bytes; uint8_t flags; };
        std::vector<GhostCandidate>* ghost_candidates = nullptr;
        struct GhostDecay { Key key; uint32_t decayed_count; };
        std::vector<GhostDecay>* ghost_decays = nullptr;
    };

    struct SweepResult {
        bool removed_any = false;
        long chunk_id = -1;
    };

    /// Sweep one chunk. ExtraPred(key, meta, value, chunk_id) → bool adds domain-specific eviction.
    /// Returns SweepResult with chunk_id for post-sweep operations (e.g., drainChunk).
    template<typename ExtraPred>
    SweepResult sweepChunk(ExtraPred&& extra) {
        if constexpr (!kHasCleanup) {
            return {};
        } else {
            auto& policy = GDSFPolicy::instance();
            CleanupContext ctx{config::CachedClock::now(),
                               kHasGDSF ? policy.threshold() : 0.0f};

            std::vector<typename CleanupContext::GhostCandidate> candidates;
            std::vector<typename CleanupContext::GhostDecay> ghost_decays;
            if constexpr (kHasGDSF) {
                if (policy.hasAdmissionPressure() && !policy.isOverBudget())
                    ctx.ghost_candidates = &candidates;
                ctx.ghost_decays = &ghost_decays;
            }

            long n_chunks = policy.chunkCount();
            long chunk_id = map_.advance_cleanup_cursor(n_chunks);

            size_t removed = 0;

            if constexpr (kHasGDSF) {
                removed = map_.cleanup_chunk(chunk_id, n_chunks,
                    [this, &ctx, &extra, chunk_id](const Key& key, auto te) {
                        if (te.isGhost())
                            return ghostCleanupPred(key, te, ctx);
                        auto* ce = static_cast<typename Map::CacheEntry*>(
                            te.template asReal<typename Map::EntryHeader>());
                        return realCleanupPred(key, ce->metadata, ce->value, ctx)
                            || extra(key, ce->metadata, ce->value, chunk_id);
                    });
            } else {
                removed = map_.cleanup_chunk(chunk_id, n_chunks,
                    [this, &ctx, &extra, chunk_id](const Key& key, auto te) {
                        auto* ce = static_cast<typename Map::CacheEntry*>(
                            te.template asReal<typename Map::EntryHeader>());
                        return realCleanupPred(key, ce->metadata, ce->value, ctx)
                            || extra(key, ce->metadata, ce->value, chunk_id);
                    });
            }

            if (removed > 0) map_.reclaim();

            // Post-sweep: apply ghost decays + insertions
            if constexpr (kHasGDSF) {
                for (auto& gd : ghost_decays) {
                    map_.update_ghost(gd.key, [&gd](TaggedEntry te) {
                        return te.withGhostCount(gd.decayed_count);
                    });
                }
                for (auto& gc : candidates) {
                    map_.insert_ghost(gc.key, gc.count, gc.bytes, gc.flags);
                }
            }

            return SweepResult{removed > 0, chunk_id};
        }
    }

    /// Sweep all chunks. ExtraPred(key, meta, value, chunk_id) → bool for domain-specific eviction.
    template<typename ExtraPred>
    size_t purgeAll(ExtraPred&& extra) {
        if constexpr (!kHasCleanup) {
            return 0;
        } else {
            auto& policy = GDSFPolicy::instance();
            CleanupContext ctx{config::CachedClock::now(),
                               kHasGDSF ? policy.threshold() : 0.0f};

            std::vector<typename CleanupContext::GhostCandidate> candidates;
            std::vector<typename CleanupContext::GhostDecay> ghost_decays;
            if constexpr (kHasGDSF) {
                if (policy.hasAdmissionPressure() && !policy.isOverBudget())
                    ctx.ghost_candidates = &candidates;
                ctx.ghost_decays = &ghost_decays;
            }

            size_t removed = 0;

            if constexpr (kHasGDSF) {
                long n_chunks = policy.chunkCount();
                for (long chunk_id = 0; chunk_id < n_chunks; ++chunk_id) {
                    removed += map_.cleanup_chunk(chunk_id, n_chunks,
                        [this, &ctx, &extra, chunk_id](const Key& key, auto te) {
                            if (te.isGhost())
                                return ghostCleanupPred(key, te, ctx);
                            auto* ce = static_cast<typename Map::CacheEntry*>(
                                te.template asReal<typename Map::EntryHeader>());
                            return realCleanupPred(key, ce->metadata, ce->value, ctx)
                                || extra(key, ce->metadata, ce->value, chunk_id);
                        });
                }
            } else {
                removed = map_.full_cleanup(
                    [this, &ctx, &extra](const Key& key, auto te) {
                        auto* ce = static_cast<typename Map::CacheEntry*>(
                            te.template asReal<typename Map::EntryHeader>());
                        return realCleanupPred(key, ce->metadata, ce->value, ctx)
                            || extra(key, ce->metadata, ce->value, long{-1});
                    });
            }

            if (removed > 0) map_.collect();

            // Post-sweep
            if constexpr (kHasGDSF) {
                for (auto& gd : ghost_decays) {
                    map_.update_ghost(gd.key, [&gd](TaggedEntry te) {
                        return te.withGhostCount(gd.decayed_count);
                    });
                }
                for (auto& gc : candidates) {
                    map_.insert_ghost(gc.key, gc.count, gc.bytes, gc.flags);
                }
            }

            return removed;
        }
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
        constexpr float kAlpha = 0.1f;
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
    epoch::memory_pool<Value>& transientPool() {
        static auto* p = new epoch::memory_pool<Value>();
        return *p;
    }

    /// Direct access to the underlying ChunkMap (for tests and advanced use).
    Map& map() { return map_; }
    const Map& map() const { return map_; }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif

private:
    Map map_;
    std::atomic<float> avg_cost_us_{0.0f};
    uint8_t chunk_bits_{0};
    size_t chunk_mask_{0};

    // =========================================================================
    // Inflight dedup
    // =========================================================================

    struct InflightEntry {
        std::mutex mu;
        std::atomic<bool> done{false};
        std::vector<std::coroutine_handle<>> waiters;

        enum class Outcome : uint8_t { pending, found, not_found, error };
        Outcome outcome = Outcome::pending;
        std::exception_ptr error;
    };

    struct ShardedInflightMap {
        static constexpr int kShards = 16;
        struct Shard {
            std::mutex mu;
            std::unordered_map<Key, std::shared_ptr<InflightEntry>,
                               detail::AutoHash<Key>> map;
        };
        std::array<Shard, kShards> shards;

        Shard& shard_for(const Key& k) {
            return shards[detail::AutoHash<Key>{}(k)
                          & static_cast<size_t>(kShards - 1)];
        }

        struct AcquireResult {
            std::shared_ptr<InflightEntry> entry;
            bool is_leader;
        };

        AcquireResult acquire(const Key& k) {
            auto& s = shard_for(k);
            std::lock_guard lk(s.mu);
            auto& slot = s.map[k];
            if (!slot) {
                slot = std::make_shared<InflightEntry>();
                return {slot, true};
            }
            return {slot, false};
        }

        void erase(const Key& k) {
            auto& s = shard_for(k);
            std::lock_guard lk(s.mu);
            s.map.erase(k);
        }
    };

    struct DedupAwaiter {
        std::shared_ptr<InflightEntry> entry;

        bool await_ready() const noexcept {
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

    template<typename Fetcher, typename MetaBuilder>
    io::Task<Hit> fetchAndAdmit(const Key& key, Fetcher&& f, MetaBuilder&& mb) {
        if constexpr (kHasGDSF) {
            auto start = std::chrono::steady_clock::now();
            auto opt = co_await f();
            if (!opt) co_return Hit{};

            auto elapsed_us = static_cast<float>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - start).count());
            recordCost(elapsed_us);

            auto& policy = GDSFPolicy::instance();
            policy.tickInsertion();

            if (policy.hasAdmissionPressure()) {
                // Lookup existing ghost
                auto ghost_result = map_.find(key);
                bool has_ghost = ghost_result && ghost_result.isGhost();

                // Compute estimated bytes
                size_t mem = 0;
                if constexpr (MemoryTrackable<Value>) {
                    mem = opt->memoryUsage();
                } else {
                    mem = sizeof(Value);
                }
                uint32_t est_bytes = static_cast<uint32_t>(
                    mem + kEntryOverhead + kBucketSlotSize);
                uint8_t est_flags = 0;

                uint32_t count;
                if (has_ghost) {
                    uint32_t old_count = ghost_result.ghostCount();
                    count = old_count + GDSFScoreData::kCountScale;
                    map_.update_ghost(key, [count, est_bytes, est_flags](TaggedEntry te) {
                        return te.withGhostCount(count).withGhostBytes(est_bytes, est_flags);
                    });
                } else {
                    count = GDSFScoreData::kCountScale;
                }

                float avg_cost = avg_cost_us_.load(std::memory_order_relaxed);
                float decayed = static_cast<float>(count) * policy.decayRate();
                float score = decayed * avg_cost
                    / static_cast<float>(std::max(est_bytes, uint32_t{1}));

                if (score >= policy.threshold()) {
                    // === CACHE (or PROMOTE from ghost) ===
                    auto meta = mb(*opt, elapsed_us);
                    auto r = map_.upsert(Map::make_key(key), std::move(*opt), std::move(meta));
                    if (has_ghost) {
                        r.entry()->metadata.gdsfData().access_count.store(
                            count, std::memory_order_relaxed);
                    }
                    auto* ce = r.asReal();
                    co_return Hit{&ce->value, &ce->metadata, std::move(r.guard)};
                } else {
                    // === GHOST (create or keep) ===
                    if (!has_ghost) {
                        map_.insert_ghost(key, GDSFScoreData::kCountScale,
                                         est_bytes, est_flags);
                    }
                    // Return via transient pool
                    auto guard = epoch::EpochGuard::acquire();
                    auto* ptr = transientPool().New(std::move(*opt));
                    transientPool().Retire(ptr);
                    co_return Hit{ptr, nullptr, std::move(guard)};
                }
            }

            // No pressure: cache normally, remove stale ghost
            auto ghost_result = map_.find(key);
            if (ghost_result && ghost_result.isGhost()) {
                map_.remove(key);
            }

            auto meta = mb(*opt, elapsed_us);
            auto r = map_.upsert(Map::make_key(key), std::move(*opt), std::move(meta));
            auto* ce = r.asReal();
            co_return Hit{&ce->value, &ce->metadata, std::move(r.guard)};
        } else {
            // Non-GDSF path
            auto opt = co_await f();
            if (!opt) co_return Hit{};

            auto meta = mb(*opt, 0.0f);
            auto r = map_.upsert(Map::make_key(key), std::move(*opt), std::move(meta));
            auto* ce = r.asReal();
            co_return Hit{&ce->value, &ce->metadata, std::move(r.guard)};
        }
    }

    // =========================================================================
    // Cleanup predicates
    // =========================================================================

    /// Real entry cleanup: decay + score + histogram + TTL.
    bool realCleanupPred(const Key& key, const Metadata& meta,
                          const Value& value, CleanupContext& ctx) {
        if constexpr (kHasGDSF) {
            auto& gdsf = const_cast<Metadata&>(meta).gdsfData();

            // Inline decay
            float dr = GDSFPolicy::instance().decayRate();
            uint32_t old_count = gdsf.rawCount();
            gdsf.access_count.store(
                static_cast<uint32_t>(static_cast<float>(old_count) * dr),
                std::memory_order_relaxed);

            // Score
            size_t mem = 0;
            if constexpr (MemoryTrackable<Value>) {
                mem = value.memoryUsage();
            } else {
                mem = sizeof(Value);
            }
            float avg_cost = avg_cost_us_.load(std::memory_order_relaxed);
            float score = gdsf.computeScore(avg_cost, mem + kBucketSlotSize);

            // Histogram
            size_t freeable = ctx.ghost_candidates
                ? (mem - std::min(mem, kBucketSlotSize))
                : mem;
            GDSFPolicy::instance().recordEntry(score, freeable);

            // TTL
            if constexpr (kHasTTL) {
                if (meta.isExpired(ctx.now_sec)) return true;
            }

            // Score threshold
            if (score < ctx.threshold) {
                if (ctx.ghost_candidates) {
                    ctx.ghost_candidates->push_back({key, old_count,
                        static_cast<uint32_t>(mem), 0});
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
            GDSFPolicy::instance().recordEntry(0.0f, kBucketSlotSize);
            return true;
        }
        if (ctx.ghost_decays) {
            ctx.ghost_decays->push_back({key, decayed});
        }
        return false;
    }
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_CACHETIER_H
