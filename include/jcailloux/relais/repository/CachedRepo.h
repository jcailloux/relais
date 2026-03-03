#ifndef JCX_RELAIS_CACHEDREPO_H
#define JCX_RELAIS_CACHEDREPO_H

#include <atomic>
#include <chrono>
#include <coroutine>
#include <exception>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_map>
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/repository/RedisRepo.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/cache/CachedWrapper.h"
#include "jcailloux/relais/cache/ChunkMap.h"
#include "jcailloux/relais/cache/GDSFMetadata.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/cache/TaggedEntry.h"
#include "jcailloux/relais/wrapper/EntityView.h"
#include "jcailloux/relais/wrapper/BufferView.h"
#include "jcailloux/relais/config/repo_config.h"
#include "jcailloux/relais/config/CachedClock.h"
#include "jcailloux/relais/cache/Metrics.h"
#include "jcailloux/relais/cache/AccessCounter.h"
#include "jcailloux/relais/cache/AccessTargets.h"
#include <array>
#include <bit>

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais {

/**
 * Repo with L1 RAM cache backed by lock-free ChunkMap (ParlayHash).
 *
 * Supports two modes based on Cfg.cache_level:
 * - CacheLevel::L1:    RAM -> Database (Redis bypassed)
 * - CacheLevel::L1_L2: RAM -> Redis -> Database (full hierarchy)
 *
 * All find methods return epoch-guarded views (EntityView / JsonView / BinaryView).
 * Views are thread-agnostic and safe to hold across co_await.
 *
 * Eviction policy depends on compile-time configuration:
 * - GDSF (score = frequency x cost) when RELAIS_GDSF_ENABLED
 * - TTL-only when l1_ttl > 0 but no GDSF
 * - No cleanup when neither is configured (default)
 *
 * When GDSF is enabled, ghost entries provide admission control under high
 * memory pressure (>= admission_pressure, default 90%). Below that threshold,
 * all fetches are cached directly and the sweep handles eviction. Ghosts are
 * zero-allocation: 8B of data (access count, estimated bytes, flags) encoded
 * inline in a tagged pointer stored directly in the ParlayHash bucket.
 *
 * Note: L1RepoConfig constraint is verified in Repo.h to avoid
 * eager evaluation issues with std::conditional_t.
 */
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires CacheableEntity<Entity>
class CachedRepo : public std::conditional_t<
    Cfg.cache_level == config::CacheLevel::L1,
    BaseRepo<Entity, Name, Cfg, Key>,
    RedisRepo<Entity, Name, Cfg, Key>
> {
    static constexpr bool HasRedis = (Cfg.cache_level == config::CacheLevel::L1_L2);
    static constexpr bool HasTTL = (std::chrono::nanoseconds(Cfg.l1_ttl).count() > 0);
    static constexpr bool HasGDSF = cache::GDSFPolicy::enabled;
    static constexpr bool HasCleanup = HasGDSF || HasTTL;

    using Base = std::conditional_t<
        HasRedis,
        RedisRepo<Entity, Name, Cfg, Key>,
        BaseRepo<Entity, Name, Cfg, Key>
    >;

    using Mapping = typename Entity::MappingType;
    using Clock = std::chrono::steady_clock;
    using Metadata = cache::CacheMetadata<HasGDSF, HasTTL>;
    using ValueType = std::conditional_t<HasGDSF, cache::CachedWrapper<Entity>, Entity>;

public:
    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using FindResultType = wrapper::EntityView<Entity>;
    using Base::name;

    static constexpr auto l1Ttl() { return std::chrono::nanoseconds(Cfg.l1_ttl); }

#if RELAIS_ENABLE_METRICS
    static inline cache::L1Counters l1_counters_{};
#endif

    // =========================================================================
    // Queries
    // =========================================================================

    /// Find by ID with L1 -> (L2) -> DB fallback.
    /// Returns epoch-guarded EntityView (empty if not found).
    /// L1 hit: zero overhead (Immediate holds EntityView directly, no Task).
    static io::Immediate<wrapper::EntityView<Entity>> find(const Key& id) {
        if (auto view = getFromCache(id)) {
            RELAIS_METRICS_INC(l1_counters_.hits);
            return std::move(view);
        }
        RELAIS_METRICS_INC(l1_counters_.misses);
        return findSlow(id);
    }

    /// Find by ID and return JSON buffer view.
    /// Returns epoch-guarded JsonView (empty if not found).
    /// L1 hit: zero overhead (Immediate holds JsonView directly, no Task).
    static io::Immediate<wrapper::JsonView> findJson(const Key& id) {
        auto result = findInCache(id);
        if (result) {
            RELAIS_METRICS_INC(l1_counters_.hits);
            auto* ce = result.asReal();
            return wrapper::JsonView(ce->value.json(), std::move(result.guard));
        }
        RELAIS_METRICS_INC(l1_counters_.misses);
        return findJsonSlow(id);
    }

    /// Find by ID and return binary (BEVE) buffer view.
    /// Returns epoch-guarded BinaryView (empty if not found).
    /// L1 hit: zero overhead (Immediate holds BinaryView directly, no Task).
    static io::Immediate<wrapper::BinaryView> findBinary(const Key& id)
        requires HasBinarySerialization<Entity>
    {
        auto result = findInCache(id);
        if (result) {
            RELAIS_METRICS_INC(l1_counters_.hits);
            auto* ce = result.asReal();
            return wrapper::BinaryView(ce->value.binary(), std::move(result.guard));
        }
        RELAIS_METRICS_INC(l1_counters_.misses);
        return findBinarySlow(id);
    }

    // =========================================================================
    // Mutations
    // =========================================================================

    /// Insert entity and cache it. Returns epoch-guarded view.
    static io::Task<wrapper::EntityView<Entity>> insert(const Entity& entity)
        requires CreatableEntity<Entity, Key> && (!Cfg.read_only)
    {
        auto result = co_await Base::insertRaw(entity);
        if (result) {
            bumpGeneration(result->key());
            co_return putInCacheAndView(result->key(), std::move(*result));
        }
        co_return {};
    }

    /// Update entity in database with L1 cache handling.
    /// Returns true on success, false on error.
    /// Skips L1 cache operations when the write was coalesced (follower).
    static io::Task<bool> update(const Key& id, const Entity& entity)
        requires MutableEntity<Entity> && (!Cfg.read_only)
    {
        using enum config::UpdateStrategy;

        auto outcome = co_await Base::updateOutcome(id, entity);
        if (outcome.success && !outcome.coalesced) {
            if constexpr (Cfg.update_strategy == InvalidateAndLazyReload) {
                evict(id);  // evict() calls bumpGeneration internally
            } else {
                // Apply ghost penalty if a ghost exists for this key
                if constexpr (HasGDSF) {
                    applyGhostUpdatePenalty(id);
                }
                bumpGeneration(id);
                putInCache(id, entity);
            }
        }
        co_return outcome.success;
    }

    /// Partial update: invalidates L1, delegates to Base::patchRaw,
    /// then moves result into cache.
    template<typename... Updates>
    static io::Task<wrapper::EntityView<Entity>> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Cfg.read_only)
    {
        if constexpr (HasGDSF) {
            applyGhostUpdatePenalty(id);
        }
        bumpGeneration(id);
        evict(id);
        auto entity = co_await Base::patchRaw(id, std::forward<Updates>(updates)...);
        if (entity) {
            co_return putInCacheAndView(id, std::move(*entity));
        }
        co_return {};
    }

    /// Erase entity by ID.
    /// Returns: rows deleted (0 if not found), or nullopt on DB error.
    /// Invalidates L1 cache unless DB error occurred or write was coalesced.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Cfg.read_only)
    {
        // Provide L1 hint for partition pruning (free: ~0ns RAM lookup)
        const Entity* hint = nullptr;
        std::optional<Entity> local_hint;
        if constexpr (HasPartitionHint<Entity>) {
            auto view = getFromCache(id);
            if (view) { local_hint.emplace(*view); hint = &*local_hint; }
        }

        auto outcome = co_await Base::eraseOutcome(id, hint);
        if (outcome.affected.has_value() && !outcome.coalesced) {
            evict(id);
        }
        co_return outcome.affected;
    }

    /// Invalidate L1 and L2 caches for a key.
    static io::Task<void> invalidate(const Key& id) {
        evict(id);
        if constexpr (HasRedis) {
            co_await Base::evictRedis(id);
        }
    }

    /// Invalidate L1 cache only. Non-coroutine since there is no async work.
    /// Removes both real entries and ghosts.
    /// Increments the generation counter to prevent stale fetches from caching.
    /// With GDSF: captures hash+ptr BEFORE invalidate, then unregisters the
    /// target AFTER (entry removed from map → no reader can re-write the slot).
    static void evict(const Key& id) {
        if constexpr (HasGDSF) {
            removeGhost(id);

            // Pre-read: capture hash + metadata ptr for unregisterTarget
            auto hk = L1Cache::make_key(id);
            size_t hash = L1Cache::get_hash(hk);
            long cid = static_cast<long>((hash >> (48 - chunk_bits_)) & chunk_mask_);
            void* meta_ptr = nullptr;
            {
                auto r = cache().find(hk);
                if (r && !r.isGhost()) {
                    auto* ce = r.entry();
                    meta_ptr = static_cast<void*>(
                        const_cast<cache::GDSFScoreData*>(
                            static_cast<const cache::GDSFScoreData*>(&ce->metadata)));
                }
            }

            bumpGeneration(id);
            cache().invalidate(id);

            if (meta_ptr) {
                access_targets_[cid].unregisterTarget(hash, meta_ptr);
            }
        } else {
            bumpGeneration(id);
            cache().invalidate(id);
        }
    }

    [[nodiscard]] static size_t size() {
        return static_cast<size_t>(cache().size());
    }

    // =========================================================================
    // Cleanup
    // =========================================================================

    /// Context passed to cleanup predicates.
    struct CleanupContext {
        Clock::time_point now;
        float threshold;
        struct GhostCandidate { Key key; uint32_t count; uint32_t bytes; uint8_t flags; };
        std::vector<GhostCandidate>* ghost_candidates = nullptr;
        struct GhostDecay { Key key; uint32_t decayed_count; };
        std::vector<GhostDecay>* ghost_decays = nullptr;
    };

    /// Sweep one chunk (lock-free, always succeeds).
    /// Target-indirected flush: TL maps point to access target slots, not to
    /// entries directly. Flush reads slot atomically under EpochGuard.
    static bool trySweep() {
        if constexpr (!HasCleanup) {
            return false;
        } else {
            auto& policy = cache::GDSFPolicy::instance();
            CleanupContext ctx{Clock::now(), HasGDSF ? policy.threshold() : 0.0f};

            std::vector<typename CleanupContext::GhostCandidate> candidates;
            std::vector<typename CleanupContext::GhostDecay> ghost_decays;
            if constexpr (HasGDSF) {
                if (policy.hasAdmissionPressure() && !policy.isOverBudget())
                    ctx.ghost_candidates = &candidates;
                ctx.ghost_decays = &ghost_decays;
            }

            long n_chunks = policy.chunkCount();
            long chunk_id = cache().advance_cleanup_cursor(n_chunks);
            size_t removed = 0;

            if constexpr (HasGDSF) {
                auto guard = epoch::EpochGuard::acquire();

                // FLUSH: accumulate TL counts per slot (no slot target dereference).
                // Safety: slot targets may point to entries freed in a previous
                // epoch — guard at E does NOT protect entries retired at E-1.
                // Resolution happens during cleanup_chunk on live entries only.
                std::unordered_map<void*, uint32_t> acc;
                access_chunks_[chunk_id].flush_all(
                    [&acc](void* slot_ptr, uint8_t count) {
                        acc[slot_ptr] += static_cast<uint32_t>(count) *
                            cache::GDSFScoreData::kCountScale;
                    });

                // SWEEP: cleanup_chunk iterates live entries → resolve TL counts
                // + eviction predicate. Collect (hash, ptr) for deferred
                // unregisterTarget AFTER remove().
                struct UnregInfo { size_t hash; void* ptr; };
                std::vector<UnregInfo> to_unreg;

                removed = cache().cleanup_chunk(chunk_id, n_chunks,
                    [&ctx, &to_unreg, &acc, chunk_id](const Key& key, auto te) {
                        if (te.isGhost())
                            return ghostCleanupPredicate(key, te, ctx);
                        auto* ce = static_cast<typename L1Cache::CacheEntry*>(
                            te.template asReal<typename L1Cache::EntryHeader>());

                        // Resolve accumulated TL counts for this live entry
                        if (!acc.empty()) {
                            auto hk = L1Cache::make_key(key);
                            size_t h = L1Cache::get_hash(hk);
                            auto* slot = access_targets_[chunk_id].slot(h);
                            if (slot) {
                                auto it = acc.find(static_cast<void*>(slot));
                                if (it != acc.end() && it->second > 0) {
                                    ce->metadata.access_count.fetch_add(
                                        it->second, std::memory_order_relaxed);
                                    it->second = 0;
                                }
                            }
                        }

                        bool evict = cleanupPredicate(key, ce->metadata, ce->value, ctx);
                        if (evict) {
                            auto hk = L1Cache::make_key(key);
                            to_unreg.push_back({L1Cache::get_hash(hk),
                                static_cast<void*>(
                                    const_cast<cache::GDSFScoreData*>(
                                        static_cast<const cache::GDSFScoreData*>(
                                            &ce->metadata)))});
                        }
                        return evict;
                    });

                // UNREGISTER: after remove() (entries no longer findable in map),
                // still under guard (entries not freed yet). Safe timing.
                for (auto& u : to_unreg) {
                    access_targets_[chunk_id].unregisterTarget(u.hash, u.ptr);
                }
            } else {
                removed = cache().cleanup_chunk(chunk_id, n_chunks,
                    [&ctx](const Key& key, auto te) {
                        auto* ce = static_cast<typename L1Cache::CacheEntry*>(
                            te.template asReal<typename L1Cache::EntryHeader>());
                        return cleanupPredicate(key, ce->metadata, ce->value, ctx);
                    });
            }

            // 4. RECLAIM: epoch-based reclamation is global. Stale pointers in
            //    TL maps point to access target SLOTS (not to entries directly),
            //    so a freed entry's slot reads nullptr → flush skips safely.
            if (removed > 0) cache().reclaim();

            // Post-sweep: apply ghost decays + insertions
            if constexpr (HasGDSF) {
                for (auto& gd : ghost_decays) {
                    cache().update_ghost(gd.key, [&gd](cache::TaggedEntry te) {
                        return te.withGhostCount(gd.decayed_count);
                    });
                }
                for (auto& gc : candidates) {
                    cache().insert_ghost(gc.key, gc.count, gc.bytes, gc.flags);
                }
            }

            return removed > 0;
        }
    }

    /// Sweep one chunk (identical to trySweep in lock-free design).
    static bool sweep() {
        return trySweep();
    }

    /// Sweep all chunks.
    /// Target-indirected: single EpochGuard per chunk for flush + cleanup + unregister.
    static size_t purge() {
        if constexpr (!HasCleanup) {
            return 0;
        } else {
            auto& policy = cache::GDSFPolicy::instance();
            CleanupContext ctx{Clock::now(), HasGDSF ? policy.threshold() : 0.0f};

            std::vector<typename CleanupContext::GhostCandidate> candidates;
            std::vector<typename CleanupContext::GhostDecay> ghost_decays;
            if constexpr (HasGDSF) {
                if (policy.hasAdmissionPressure() && !policy.isOverBudget()) {
                    ctx.ghost_candidates = &candidates;
                }
                ctx.ghost_decays = &ghost_decays;
            }

            size_t removed = 0;

            if constexpr (HasGDSF) {
                long n_chunks = policy.chunkCount();
                struct UnregInfo { size_t hash; void* ptr; };
                std::vector<UnregInfo> to_unreg;

                for (long chunk_id = 0; chunk_id < n_chunks; ++chunk_id) {
                    auto guard = epoch::EpochGuard::acquire();

                    // FLUSH: accumulate TL counts (no slot target dereference)
                    std::unordered_map<void*, uint32_t> acc;
                    access_chunks_[chunk_id].flush_all(
                        [&acc](void* slot_ptr, uint8_t count) {
                            acc[slot_ptr] += static_cast<uint32_t>(count) *
                                cache::GDSFScoreData::kCountScale;
                        });

                    to_unreg.clear();
                    removed += cache().cleanup_chunk(chunk_id, n_chunks,
                        [&ctx, &to_unreg, &acc, chunk_id](const Key& key, auto te) {
                            if (te.isGhost())
                                return ghostCleanupPredicate(key, te, ctx);
                            auto* ce = static_cast<typename L1Cache::CacheEntry*>(
                                te.template asReal<typename L1Cache::EntryHeader>());

                            // Resolve accumulated TL counts
                            if (!acc.empty()) {
                                auto hk = L1Cache::make_key(key);
                                size_t h = L1Cache::get_hash(hk);
                                auto* slot = access_targets_[chunk_id].slot(h);
                                if (slot) {
                                    auto it = acc.find(static_cast<void*>(slot));
                                    if (it != acc.end() && it->second > 0) {
                                        ce->metadata.access_count.fetch_add(
                                            it->second, std::memory_order_relaxed);
                                        it->second = 0;
                                    }
                                }
                            }

                            bool evict_it = cleanupPredicate(key, ce->metadata, ce->value, ctx);
                            if (evict_it) {
                                auto hk = L1Cache::make_key(key);
                                to_unreg.push_back({L1Cache::get_hash(hk),
                                    static_cast<void*>(
                                        const_cast<cache::GDSFScoreData*>(
                                            static_cast<const cache::GDSFScoreData*>(
                                                &ce->metadata)))});
                            }
                            return evict_it;
                        });

                    for (auto& u : to_unreg) {
                        access_targets_[chunk_id].unregisterTarget(u.hash, u.ptr);
                    }
                }
            } else {
                removed = cache().full_cleanup(
                    [&ctx](const Key& key, auto te) {
                        auto* ce = static_cast<typename L1Cache::CacheEntry*>(
                            te.template asReal<typename L1Cache::EntryHeader>());
                        return cleanupPredicate(key, ce->metadata, ce->value, ctx);
                    });
            }

            if (removed > 0) cache().collect();

            // Post-sweep: apply ghost decays + insertions
            if constexpr (HasGDSF) {
                for (auto& gd : ghost_decays) {
                    cache().update_ghost(gd.key, [&gd](cache::TaggedEntry te) {
                        return te.withGhostCount(gd.decayed_count);
                    });
                }
                for (auto& gc : candidates) {
                    cache().insert_ghost(gc.key, gc.count, gc.bytes, gc.flags);
                }
            }

            return removed;
        }
    }

    /// Prime L1 cache at startup.
    /// ListMixin overrides this via method hiding to also warm up the list cache.
    static void warmup() {
        RELAIS_LOG_DEBUG << name() << ": warming up L1 cache...";
        // Ensure the static ChunkMap instance is constructed + repo registered.
        (void)cache();
        RELAIS_LOG_DEBUG << name() << ": L1 cache primed";
    }

    /// Current average construction time in us (exposed for testing/debugging).
    static float avgConstructionTime() {
        return avg_construction_time_us_.load(std::memory_order_relaxed);
    }

protected:
    using L1Cache = cache::ChunkMap<Key, ValueType, Metadata, HasGDSF>;

    /// Returns the static ChunkMap instance.
    /// Hot path: single pointer check (no guard variable, no function call).
    /// First call delegates to cache_init_slow() which constructs and registers.
    static L1Cache& cache() {
        auto* p = cache_ptr_;
        if (p) [[likely]] return *p;
        return cache_init_slow();
    }

    /// Cold path: construct the ChunkMap, register with GDSFPolicy, cache the pointer.
    [[gnu::noinline]] static L1Cache& cache_init_slow() {
        struct Holder {
            L1Cache instance;
            Holder() {
                config::CachedClock::ensureStarted();
                if constexpr (HasCleanup) {
                    cache::GDSFPolicy::instance().enroll({
                        .sweep_fn = +[]() -> bool { return sweep(); },
                        .size_fn = +[]() -> size_t { return size(); },
                        .name = static_cast<const char*>(Name)
                    });
                }
                if constexpr (HasGDSF) {
                    long nc = cache::GDSFPolicy::instance().chunkCount();
                    chunk_bits_ = static_cast<uint8_t>(
                        std::countr_zero(static_cast<unsigned long>(nc)));
                    chunk_mask_ = static_cast<size_t>(nc - 1);
                }
                cache_ptr_ = &instance;
            }
        };
        static Holder h;
        cache_ptr_ = &h.instance;  // also set here for safety (after static init)
        return h.instance;
    }

    /// L1 cache lookup with TTL check and thread-local access counting.
    /// Returns raw FindResult for flexible use by find/findJson/findBinary.
    /// Ghosts are treated as L1 miss (the slow path handles admission).
    ///
    /// GDSF hot path: instead of a shared atomic fetch_add (MESI bouncing),
    /// accesses are recorded in per-thread maps and flushed in batch during sweep.
    /// Kept minimal (~4 lines) so the compiler inlines into getFromCache/find.
    static typename L1Cache::FindResult findInCache(const Key& key) {
        auto hk = L1Cache::make_key(key);
        auto result = cache().find(hk);
        if (!result) return {};

        // Ghost: treated as L1 miss (slow path will handle admission)
        if constexpr (HasGDSF) {
            if (result.isGhost()) return {};
        }

        auto* entry = result.entry();

        // TTL expiration check: two-phase eviction (find → check → evict)
        if constexpr (HasTTL) {
            if (entry->metadata.isExpired(config::CachedClock::now())) {
                // Remove only if pointer identity matches (guards against concurrent Upsert)
                cache().remove_if(key, [entry](auto* e) { return e == entry; });
                // Unregister target after remove (entry not freed yet — result holds guard)
                if constexpr (HasGDSF) {
                    size_t hash = L1Cache::get_hash(hk);
                    long cid = static_cast<long>(
                        (hash >> (48 - chunk_bits_)) & chunk_mask_);
                    access_targets_[cid].unregisterTarget(hash,
                        static_cast<void*>(
                            const_cast<cache::GDSFScoreData*>(
                                static_cast<const cache::GDSFScoreData*>(
                                    &entry->metadata))));
                }
                return {};
            }
        }

        if constexpr (HasGDSF) {
            // Thread-local access counting via target-indirected slots.
            // chunk_bits_ / chunk_mask_ are pre-computed static constants.
            size_t hash = L1Cache::get_hash(hk);
            long chunk_id = static_cast<long>(
                (hash >> (48 - chunk_bits_)) & chunk_mask_);
            recordAccess(
                static_cast<cache::GDSFScoreData*>(&entry->metadata),
                hash, chunk_id);
        }

        return result;
    }

    /// Get from cache as EntityView (convenience wrapper around findInCache).
    /// Get from cache as EntityView (convenience wrapper around findInCache).
    static wrapper::EntityView<Entity> getFromCache(const Key& key) {
        auto result = findInCache(key);
        if (!result) return {};
        auto* ce = result.asReal();
        return wrapper::EntityView<Entity>(
            static_cast<const Entity*>(&ce->value), std::move(result.guard));
    }

    /// Put entity in cache (copy). Returns FindResult for flexible use.
    /// No tick here — the sweep counter is driven by fetchAndCache (DB fetches),
    /// not by local cache mutations (update, insert via API).
    /// With GDSF: flush TL counters for the target slot before upsert so that
    /// mergeFrom reads the correct access_count from the old entry.
    ///
    /// Safety: find-based flush — find() the live entry under its own guard,
    /// then transfer counts directly to its metadata. Never dereference slot
    /// targets (guard at epoch E does NOT protect entries retired at E-1).
    static typename L1Cache::FindResult putInCache(const Key& key, const Entity& src,
        Clock::time_point now = Clock::now())
    {
        if constexpr (HasGDSF) {
            auto hk = L1Cache::make_key(key);
            size_t hash = L1Cache::get_hash(hk);
            long cid = static_cast<long>((hash >> (48 - chunk_bits_)) & chunk_mask_);
            auto* slot = access_targets_[cid].slot(hash);
            if (slot) {
                auto existing = cache().find(hk);
                if (existing) {
                    auto* md = static_cast<cache::GDSFScoreData*>(
                        &existing.asReal()->metadata);
                    access_chunks_[cid].flush_one_all(slot, hash,
                        [md](void*, uint8_t count) {
                            md->access_count.fetch_add(
                                static_cast<uint32_t>(count) *
                                    cache::GDSFScoreData::kCountScale,
                                std::memory_order_relaxed);
                        });
                }
            }
            auto result = cache().upsert(hk, buildValue(src), buildMetadata(now));
            // Register the NEW entry so the slot never holds a dangling ptr.
            access_targets_[cid].registerAndSlot(hash,
                static_cast<void*>(
                    static_cast<cache::GDSFScoreData*>(&result.asReal()->metadata)));
            return result;
        } else {
            return cache().upsert(L1Cache::make_key(key), buildValue(src), buildMetadata(now));
        }
    }

    /// Put entity in cache (move — zero copy). Returns FindResult.
    static typename L1Cache::FindResult putInCache(const Key& key, Entity&& src,
        Clock::time_point now = Clock::now())
    {
        if constexpr (HasGDSF) {
            auto hk = L1Cache::make_key(key);
            size_t hash = L1Cache::get_hash(hk);
            long cid = static_cast<long>((hash >> (48 - chunk_bits_)) & chunk_mask_);
            auto* slot = access_targets_[cid].slot(hash);
            if (slot) {
                auto existing = cache().find(hk);
                if (existing) {
                    auto* md = static_cast<cache::GDSFScoreData*>(
                        &existing.asReal()->metadata);
                    access_chunks_[cid].flush_one_all(slot, hash,
                        [md](void*, uint8_t count) {
                            md->access_count.fetch_add(
                                static_cast<uint32_t>(count) *
                                    cache::GDSFScoreData::kCountScale,
                                std::memory_order_relaxed);
                        });
                }
            }
            auto result = cache().upsert(hk, buildValue(std::move(src)), buildMetadata(now));
            access_targets_[cid].registerAndSlot(hash,
                static_cast<void*>(
                    static_cast<cache::GDSFScoreData*>(&result.asReal()->metadata)));
            return result;
        } else {
            return cache().upsert(L1Cache::make_key(key), buildValue(std::move(src)), buildMetadata(now));
        }
    }

    /// Put entity in cache and return EntityView (copy path).
    static wrapper::EntityView<Entity> putInCacheAndView(const Key& key, const Entity& src) {
        auto result = putInCache(key, src);
        auto* ce = result.asReal();
        return wrapper::EntityView<Entity>(
            static_cast<const Entity*>(&ce->value), std::move(result.guard));
    }

    /// Put entity in cache and return EntityView (move path — zero copy).
    static wrapper::EntityView<Entity> putInCacheAndView(const Key& key, Entity&& src) {
        auto result = putInCache(key, std::move(src));
        auto* ce = result.asReal();
        return wrapper::EntityView<Entity>(
            static_cast<const Entity*>(&ce->value), std::move(result.guard));
    }

private:
    // =========================================================================
    // Inflight dedup — coalesce concurrent misses on the same key
    // =========================================================================

    struct InflightEntry {
        std::mutex mu;
        std::atomic<bool> done{false};
        std::vector<std::coroutine_handle<>> waiters;

        enum class Outcome : uint8_t { pending, found, not_found, error };
        Outcome outcome = Outcome::pending;
        std::exception_ptr error;  // set if outcome == error
    };

    struct ShardedInflightMap {
        static constexpr int kShards = 16;
        struct Shard {
            std::mutex mu;
            std::unordered_map<Key, std::shared_ptr<InflightEntry>,
                               cache::detail::AutoHash<Key>> map;
        };
        std::array<Shard, kShards> shards;

        Shard& shard_for(const Key& k) {
            return shards[cache::detail::AutoHash<Key>{}(k)
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

    /// Coroutine awaiter for dedup followers.
    /// await_suspend returns bool: false if already done (no suspend needed).
    struct DedupAwaiter {
        std::shared_ptr<InflightEntry> entry;

        bool await_ready() const noexcept {
            return entry->done.load(std::memory_order_acquire);
        }

        bool await_suspend(std::coroutine_handle<> h) {
            std::lock_guard lk(entry->mu);
            // Re-check under lock to avoid race between await_ready and suspend
            if (entry->done.load(std::memory_order_relaxed)) {
                return false;  // don't suspend, resume immediately
            }
            entry->waiters.push_back(h);
            return true;  // suspend
        }

        void await_resume() noexcept {}
    };

    static ShardedInflightMap& inflightMap() {
        static ShardedInflightMap map;
        return map;
    }

    /// Slow path for find(): L1 miss → dedup → (L2) → DB → cache.
    static io::Task<wrapper::EntityView<Entity>> findSlow(const Key& id) {
        auto [entry, is_leader] = inflightMap().acquire(id);

        if (is_leader) {
            // Leader: execute the actual fetch
            wrapper::EntityView<Entity> result;
            try {
                result = co_await fetchAndCache(id);
                // Determine outcome
                {
                    std::lock_guard lk(entry->mu);
                    entry->outcome = result
                        ? InflightEntry::Outcome::found
                        : InflightEntry::Outcome::not_found;
                    entry->done.store(true, std::memory_order_release);
                }
            } catch (...) {
                // Propagate error to followers
                {
                    std::lock_guard lk(entry->mu);
                    entry->outcome = InflightEntry::Outcome::error;
                    entry->error = std::current_exception();
                    entry->done.store(true, std::memory_order_release);
                }
                // Resume followers, erase, then rethrow
                for (auto h : entry->waiters) h.resume();
                inflightMap().erase(id);
                throw;
            }

            // Resume all waiting followers
            // Copy waiters under lock then resume outside lock
            std::vector<std::coroutine_handle<>> to_resume;
            {
                std::lock_guard lk(entry->mu);
                to_resume = std::move(entry->waiters);
            }
            for (auto h : to_resume) h.resume();

            // Remove from map
            inflightMap().erase(id);

            co_return result;
        }

        // Follower: wait for leader to complete
        co_await DedupAwaiter{entry};

        switch (entry->outcome) {
        case InflightEntry::Outcome::found: {
            // Entity should be in L1 cache now
            auto view = getFromCache(id);
            if (view) co_return view;
            // Evicted between leader store and follower read — fallback
            co_return co_await fetchAndCache(id);
        }
        case InflightEntry::Outcome::not_found:
            co_return {};
        case InflightEntry::Outcome::error:
            std::rethrow_exception(entry->error);
        default:
            co_return {};
        }
    }

    /// Slow path for findJson(): L1 miss → (L2) → DB → cache → JSON.
    static io::Task<wrapper::JsonView> findJsonSlow(const Key& id) {
        auto view = co_await fetchAndCache(id);
        if (view) {
            auto* p = view->json();  // evaluate before take_guard() nulls ptr_
            co_return wrapper::JsonView(p, view.take_guard());
        }
        co_return {};
    }

    /// Slow path for findBinary(): L1 miss → (L2) → DB → cache → binary.
    static io::Task<wrapper::BinaryView> findBinarySlow(const Key& id)
        requires HasBinarySerialization<Entity>
    {
        auto view = co_await fetchAndCache(id);
        if (view) {
            auto* p = view->binary();  // evaluate before take_guard() nulls ptr_
            co_return wrapper::BinaryView(p, view.take_guard());
        }
        co_return {};
    }

    /// Fetch from Base via findRaw and decide: cache, ghost, or reject.
    ///
    /// Under GDSF with memory pressure (>= 50%), the entity is scored against
    /// the eviction threshold. If the score is too low, a lightweight ghost
    /// is created instead of caching the full entity. Ghosts are zero-allocation
    /// (data encoded inline in TaggedEntry) and track access frequency so that
    /// popular data is admitted on subsequent fetches.
    ///
    /// Without pressure or without GDSF, every fetch is cached (existing behavior).
    static io::Task<wrapper::EntityView<Entity>> fetchAndCache(const Key& id) {
        if constexpr (HasGDSF) {
            auto start = Clock::now();
            auto entity = co_await Base::findRaw(id);
            if (!entity) co_return {};

            auto now = Clock::now();
            auto elapsed_us = static_cast<float>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    now - start).count());
            updateAvgConstructionTime(elapsed_us);

            auto& policy = cache::GDSFPolicy::instance();

            // Tick on every DB fetch (not just insertions). This ensures
            // periodic sweeps fire even when ghosts prevent putInCache,
            // breaking the deadlock: ghosts block insertion → no ticks →
            // no sweeps → threshold stale → ghosts never promoted.
            policy.tickInsertion();

            if (policy.hasAdmissionPressure()) {
                // --- Lookup existing ghost ---
                auto ghost_result = cache().find(id);
                bool has_ghost = ghost_result && ghost_result.isGhost();

                // --- Compute score ---
                uint32_t est_bytes = static_cast<uint32_t>(
                    entity->memoryUsage() + kChargeOverhead + kBucketSlotSize);
                uint8_t est_flags = static_cast<uint8_t>(
                    (entity->hasBinaryCache() ? 0x01 : 0) |
                    (entity->hasJsonCache()   ? 0x02 : 0));

                uint32_t count;
                if (has_ghost) {
                    // Ghost exists: bump counter, update estimated_bytes
                    uint32_t old_count = ghost_result.ghostCount();
                    count = old_count + cache::GDSFScoreData::kCountScale;
                    cache().update_ghost(id, [count, est_bytes, est_flags](cache::TaggedEntry te) {
                        return te.withGhostCount(count).withGhostBytes(est_bytes, est_flags);
                    });
                } else {
                    // No ghost: initial score
                    count = cache::GDSFScoreData::kCountScale;
                }

                float avg_cost = avg_construction_time_us_.load(std::memory_order_relaxed);
                float decayed = static_cast<float>(count) * policy.decayRate();
                float score = decayed * avg_cost
                    / static_cast<float>(std::max(est_bytes, uint32_t{1}));

                if (score >= policy.threshold()) {
                    // === CACHE (or PROMOTION if ghost existed) ===
                    // No ghost discharge — bucket slot is tracked by the hook.
                    auto r = putInCache(id, std::move(*entity), now);
                    if (has_ghost) {
                        // Transfer counter from ghost to real entry
                        r.entry()->metadata.access_count.store(count,
                            std::memory_order_relaxed);
                    }
                    auto* ce = r.asReal();
                    co_return wrapper::EntityView<Entity>(
                        static_cast<const Entity*>(&ce->value), std::move(r.guard));
                } else {
                    // === GHOST (create or keep existing) ===
                    if (!has_ghost) {
                        // Create new ghost (insert_ghost: never replaces a real entry)
                        // No ghost charge — bucket slot is tracked by the hook.
                        cache().insert_ghost(id,
                                cache::GDSFScoreData::kCountScale,
                                est_bytes, est_flags);
                        // If insert fails → a real entry was inserted concurrently.
                        // We continue: the entity is returned as transient anyway.
                    }
                    // Return entity without caching (transient pool)
                    auto guard = epoch::EpochGuard::acquire();
                    auto* ptr = transientPool().New(std::move(*entity));
                    transientPool().Retire(ptr);
                    co_return wrapper::EntityView<Entity>(
                        static_cast<const Entity*>(ptr), std::move(guard));
                }
            }

            // --- No pressure: cache normally ---
            // Remove ghost if one exists (no longer relevant without pressure)
            // No ghost discharge — bucket slot is tracked by the hook.
            auto ghost_result = cache().find(id);
            if (ghost_result && ghost_result.isGhost()) {
                cache().remove(id);
            }
            auto r = putInCache(id, std::move(*entity), now);
            auto* ce = r.asReal();
            co_return wrapper::EntityView<Entity>(
                static_cast<const Entity*>(&ce->value), std::move(r.guard));
        } else {
            // Non-GDSF path (unchanged)
            auto entity = co_await Base::findRaw(id);
            if (entity) {
                auto r = putInCache(id, std::move(*entity));
                auto* ce = r.asReal();
                co_return wrapper::EntityView<Entity>(
                    static_cast<const Entity*>(&ce->value), std::move(r.guard));
            }
            co_return {};
        }
    }

    /// Epoch wrapper: memory_pool wraps each CacheEntry in a node with a next pointer.
    template<typename T>
    struct EpochWrapperMirror { T value; };

    static constexpr size_t kEpochWrapperOverhead =
        sizeof(EpochWrapperMirror<typename L1Cache::CacheEntry>)
        - sizeof(typename L1Cache::CacheEntry);

    /// Overhead charged per live entry (excludes bucket slot — tracked by hook).
    static constexpr size_t kChargeOverhead =
        sizeof(typename L1Cache::CacheEntry) - sizeof(Entity)
        + kEpochWrapperOverhead;

    /// ParlayHash bucket slot. Used for:
    /// - Score denominator (full incompressible cost of keeping an entry)
    /// - Histogram ghost adjustment (net gain live → ghost)
    /// - Ghost admission estimation in fetchAndCache
    /// NOT charged to GDSFPolicy (already in hook's bucket array charge).
    static constexpr size_t kBucketSlotSize =
        sizeof(Key) + sizeof(cache::TaggedEntry);

    /// Transient pool for entities returned without caching (ghost REJECT path).
    /// Epoch-based reclamation ensures the entity lives until the EpochGuard drops.
    static epoch::memory_pool<Entity>& transientPool() {
        static auto* p = new epoch::memory_pool<Entity>();
        return *p;
    }

    /// Build ValueType from entity (copy path).
    static ValueType buildValue(const Entity& src) {
        if constexpr (HasGDSF) {
            return cache::CachedWrapper<Entity>(Entity(src), kChargeOverhead);
        } else {
            return Entity(src);
        }
    }

    /// Build ValueType from entity (move path — zero copy).
    static ValueType buildValue(Entity&& src) {
        if constexpr (HasGDSF) {
            return cache::CachedWrapper<Entity>(std::move(src), kChargeOverhead);
        } else {
            return std::move(src);
        }
    }

    /// Build metadata. Accepts a pre-computed timestamp to avoid redundant
    /// Clock::now() calls (e.g., reuse timing from GDSF measurement).
    static Metadata buildMetadata(Clock::time_point now = Clock::now()) {
        if constexpr (HasGDSF) {
            int64_t ttl_rep = 0;
            if constexpr (HasTTL) {
                ttl_rep = (now + l1Ttl()).time_since_epoch().count();
            }
            return Metadata{cache::GDSFScoreData::kCountScale, ttl_rep};
        } else if constexpr (HasTTL) {
            return Metadata{(now + l1Ttl()).time_since_epoch().count()};
        } else {
            return Metadata{};
        }
        
    }

    // =========================================================================
    // Thread-local access counting (GDSF hot path optimization)
    // =========================================================================
    //
    // Instead of a shared atomic fetch_add on every L1 hit (MESI bouncing,
    // ~40-80ns under 6T contention), accesses are recorded in per-thread maps
    // and batch-flushed during sweep. This reduces the hot path to ~2-3ns.
    //
    // One ChunkAccessCounter per chunk, one TLS map per (repo, thread, chunk).
    // The template parameters make CachedRepo<E,N,C,K> a distinct type, so
    // `static thread_local` gives automatic per-(repo, thread) isolation.

    static constexpr long kMaxChunks = 64;

    /// Per-chunk access counter pools + registries (one per repo instantiation).
    static inline cache::ChunkAccessCounter access_chunks_[kMaxChunks]{};

    /// Per-chunk access targets for safe TL counter indirection.
    /// TL maps store pointers to slots in these arrays (not to entries directly).
    /// Each slot is atomic<void*>: non-null = live entry, nullptr = evicted.
    static inline cache::AccessTargets access_targets_[kMaxChunks]{};

    /// Cached pointer to the L1Cache instance (set once in cache_init_slow).
    /// Avoids the guard-variable function call on every findInCache.
    static inline L1Cache* cache_ptr_{nullptr};

    /// Pre-computed chunk constants (set once in Holder, never modified).
    /// Avoids loading chunkCount() on every findInCache call.
    static inline uint8_t chunk_bits_{0};   // = countr_zero(chunkCount())
    static inline size_t chunk_mask_{0};    // = chunkCount() - 1

    /// No-op flush lambda: drains map without dereferencing slot targets.
    /// Used for anti-overflow flush (kMapFull) and flushAccessCounters.
    /// Why no-op: slot targets may point to entries freed in a previous epoch
    /// (guard at epoch E does NOT protect entries retired at E-1), so any
    /// slot->load() → dereference is unsafe even under EpochGuard.
    /// Safe flush patterns: captured-sd (count=255), find-based (putInCache),
    /// accumulate+resolve (sweep).
    static constexpr auto kNoopFn = [](void*, uint8_t) {};

    /// Per-thread map pointers for each chunk. Acquired lazily on first access
    /// to a chunk, released back to pool on thread exit.
    /// cached_data[] caches AccessTargets::data_ to avoid load(acquire) on ARM.
    /// data_ never changes after allocation → cached pointer is valid for life.
    struct ThreadMaps {
        cache::AccessCounterMap* maps[kMaxChunks]{};
        std::atomic<void*>* cached_data[kMaxChunks]{};
        ~ThreadMaps() {
            long nc = cache::GDSFPolicy::instance().chunkCount();
            for (long i = 0; i < nc; ++i) {
                if (maps[i]) access_chunks_[i].release(maps[i]);
            }
        }
    };

    static ThreadMaps& threadMaps() {
        static thread_local ThreadMaps tm;
        return tm;
    }

    /// Record an access in the thread-local map for the given chunk.
    /// Hot path: 0 load(acquire) (data_ TLS-cached), 1 store(relaxed) to slot.
    /// Overflow: count=255 → surgical flush_one (~10ns); kMapFull → noop flush
    /// (anti-overflow, counts lost but re-accumulated in ~1 sweep).
    static void recordAccess(cache::GDSFScoreData* sd, size_t hash, long chunk_id) {
        auto& tm = threadMaps();

        // TLS-cached data_ — data_ never changes after ensureAllocated(),
        // so cached pointer is valid for the lifetime of the AccessTargets.
        auto*& d = tm.cached_data[chunk_id];
        if (!d) [[unlikely]] {
            access_targets_[chunk_id].ensureAllocated();
            d = access_targets_[chunk_id].data_.load(std::memory_order_acquire);
        }

        // Register target + get slot ptr (single array deref, no load(acquire))
        auto& cell = d[(hash >> 16) & cache::AccessTargets::kMask];
        cell.store(static_cast<void*>(sd), std::memory_order_relaxed);
        auto* slot = &cell;

        auto*& map = tm.maps[chunk_id];
        if (!map) [[unlikely]]
            map = access_chunks_[chunk_id].acquire();
        auto rr = map->record(static_cast<void*>(slot), hash);
        if (rr != cache::RecordResult::kOk) [[unlikely]] {
            if (rr == cache::RecordResult::kCountFull) {
                // count=255 overflow: surgical flush for this entry only.
                // Direct write to sd (captured, protected by findInCache's
                // caller guard). Never dereference slot targets.
                map->flush_one(static_cast<void*>(slot), hash,
                    [sd](void*, uint8_t count) {
                        sd->access_count.fetch_add(
                            static_cast<uint32_t>(count) *
                                cache::GDSFScoreData::kCountScale,
                            std::memory_order_relaxed);
                    });
            } else {
                // kMapFull: >= 75% full or probe failure.
                // Anti-overflow: drain the map. Counts lost (no safe dereference
                // without EpochGuard). Re-accumulated in ~1 sweep cycle.
                // Frequency: ~1x per 3072 unique accesses (75% of 4096).
                map->try_flush(kNoopFn);
            }
        }
    }

    /// Flush all TL counters for all chunks (drains maps without dereferencing).
    /// Counts are lost (not transferred to entries) — use before trySweep if
    /// precise counts are needed (trySweep resolves via accumulate+iterate).
    static void flushAccessCounters() {
        long nc = cache::GDSFPolicy::instance().chunkCount();
        for (long i = 0; i < nc; ++i) {
            access_chunks_[i].flush_all(kNoopFn);
        }
    }

    /// Cleanup predicate for real entries: inline decay + score + histogram.
    /// When evicting, accumulates ghost candidates for post-sweep insertion.
    static bool cleanupPredicate(const Key& key, const Metadata& meta,
                                  const ValueType& value, CleanupContext& ctx) {
        if constexpr (HasGDSF) {
            // Inline decay: single writer per chunk during sweep, plain store (no CAS)
            float dr = cache::GDSFPolicy::instance().decayRate();
            uint32_t old_count = meta.rawCount();
            meta.access_count.store(
                static_cast<uint32_t>(static_cast<float>(old_count) * dr),
                std::memory_order_relaxed);

            // Score = access_count x avg_cost / memoryUsage (includes bucket slot)
            size_t mem = value.memoryUsage();
            float avg_cost = avg_construction_time_us_.load(std::memory_order_relaxed);
            float score = meta.computeScore(avg_cost, mem + kBucketSlotSize);

            // Histogram: net freeable bytes. When ghost candidates are active,
            // evicting a live entry only frees (mem - kBucketSlotSize) since
            // a ghost occupies the bucket slot.
            size_t freeable = ctx.ghost_candidates
                ? (mem - std::min(mem, kBucketSlotSize))
                : mem;
            cache::GDSFPolicy::instance().recordEntry(score, freeable);

            // Evict if TTL expired
            if constexpr (HasTTL) {
                if (meta.isExpired(ctx.now)) return true;
            }

            // Evict if score below threshold
            if (score < ctx.threshold) {
                if (ctx.ghost_candidates) {
                    ctx.ghost_candidates->push_back({key, old_count,
                        static_cast<uint32_t>(mem),
                        static_cast<uint8_t>(
                            (value.hasBinaryCache() ? 0x01 : 0) |
                            (value.hasJsonCache()   ? 0x02 : 0))});
                }
                return true;
            }

            return false;
        } else if constexpr (HasTTL) {
            return meta.isExpired(ctx.now);
        } else {
            return false;  // unreachable — cleanup is disabled
        }
    }

    /// Ghost cleanup predicate (two-phase): read ghost data inline, decide
    /// remove or decay. Ghosts that survive are collected for post-sweep update.
    static bool ghostCleanupPredicate(const Key& key, cache::TaggedEntry te,
                                       CleanupContext& ctx) {
        float dr = cache::GDSFPolicy::instance().decayRate();
        uint32_t count = te.ghostCount();
        uint32_t decayed = static_cast<uint32_t>(static_cast<float>(count) * dr);
        if (decayed == 0) {
            // Ghost eviction frees the bucket slot (modeled in histogram,
            // not in charge — bucket array is tracked by the hook).
            cache::GDSFPolicy::instance().recordEntry(0.0f, kBucketSlotSize);
            return true;
        }
        // Collect for post-sweep batch update
        if (ctx.ghost_decays) {
            ctx.ghost_decays->push_back({key, decayed});
        }
        return false;
    }

    /// Remove a ghost entry for the given key (if it exists).
    /// Used by evict/invalidate to clean up ghosts on write paths.
    static void removeGhost(const Key& id) {
        auto r = cache().find(id);
        if (r && r.isGhost()) {
            // No ghost discharge — bucket slot is tracked by the hook.
            cache().remove(id);
        }
    }

    /// Apply update penalty to a ghost's access count (if ghost exists).
    /// Called by update/patch to erode scores of frequently-mutated data.
    static void applyGhostUpdatePenalty(const Key& id) {
        auto r = cache().find(id);
        if (r && r.isGhost()) {
            uint32_t count = r.ghostCount();
            uint32_t penalized = static_cast<uint32_t>(
                static_cast<float>(count) * cache::GDSFScoreData::kUpdatePenalty);
            cache().update_ghost(id, [penalized](cache::TaggedEntry te) {
                return te.withGhostCount(penalized);
            });
        }
    }

    /// EMA update for average construction time (measured on L1 miss).
    /// CAS without retry: if contention causes a lost update, the EMA converges naturally.
    static void updateAvgConstructionTime(float elapsed_us) {
        constexpr float kAlpha = 0.1f;
        float old_avg = avg_construction_time_us_.load(std::memory_order_relaxed);
        float new_avg;
        if (old_avg == 0.0f) {
            new_avg = elapsed_us;  // First measurement: seed the EMA
        } else {
            new_avg = kAlpha * elapsed_us + (1.0f - kAlpha) * old_avg;
        }
        avg_construction_time_us_.compare_exchange_weak(old_avg, new_avg, std::memory_order_relaxed);
    }

    // Per-repo GDSF state (one instance per template specialization).
    static inline std::atomic<float> avg_construction_time_us_{0.0f};

    // =========================================================================
    // Generation counter — stale write prevention (lock-free, cross-thread)
    // =========================================================================
    //
    // Flat array of atomic counters indexed by hash(key) % kGenSlots.
    // Zero allocation, zero epoch overhead (unlike ParlayHash Upsert which
    // allocates + retires a node on every update).
    //
    // Hash collisions are safe: two keys sharing a slot may cause an
    // unnecessary cache miss (pessimistic), never stale data.

    static constexpr size_t kGenSlots = 4096;
    using GenHash = cache::detail::AutoHash<Key>;
    static inline std::array<std::atomic<uint32_t>, kGenSlots> generation_slots_{};

    /// Increment the generation for a key (called on every write path).
    static void bumpGeneration(const Key& id) {
        generation_slots_[GenHash{}(id) & (kGenSlots - 1)]
            .fetch_add(1, std::memory_order_relaxed);
    }

    /// Read current generation for a key's slot.
    static uint32_t readGeneration(const Key& id) {
        return generation_slots_[GenHash{}(id) & (kGenSlots - 1)]
            .load(std::memory_order_relaxed);
    }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::relais

#endif //JCX_RELAIS_CACHEDREPO_H
