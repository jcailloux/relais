#ifndef JCX_RELAIS_CACHEDREPO_H
#define JCX_RELAIS_CACHEDREPO_H

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <type_traits>
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/repository/RedisRepo.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/cache/CachedWrapper.h"
#include "jcailloux/relais/cache/ChunkMap.h"
#include "jcailloux/relais/cache/GDSFMetadata.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/cache/GhostEntry.h"
#include "jcailloux/relais/wrapper/EntityView.h"
#include "jcailloux/relais/wrapper/BufferView.h"
#include "jcailloux/relais/config/repo_config.h"
#include "jcailloux/relais/config/CachedClock.h"
#include <array>

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
 * When GDSF is enabled, ghost entries provide admission control under memory
 * pressure (>= 50%). Evicted/rejected data is tracked as lightweight ghosts
 * (~20B) that accumulate access frequency. Only data that proves popular
 * enough is (re-)admitted to the cache.
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
    static constexpr long kChunkCount = 1L << Cfg.l1_chunk_count_log2;

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

    // =========================================================================
    // Queries
    // =========================================================================

    /// Find by ID with L1 -> (L2) -> DB fallback.
    /// Returns epoch-guarded EntityView (empty if not found).
    /// L1 hit: zero overhead (Immediate holds EntityView directly, no Task).
    static io::Immediate<wrapper::EntityView<Entity>> find(const Key& id) {
        if (auto view = getFromCache(id))
            return std::move(view);
        return findSlow(id);
    }

    /// Find by ID and return JSON buffer view.
    /// Returns epoch-guarded JsonView (empty if not found).
    /// L1 hit: zero overhead (Immediate holds JsonView directly, no Task).
    static io::Immediate<wrapper::JsonView> findJson(const Key& id) {
        auto result = findInCache(id);
        if (result) {
            auto* ce = result.asReal();
            return wrapper::JsonView(ce->value.json(), std::move(result.guard));
        }
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
            auto* ce = result.asReal();
            return wrapper::BinaryView(ce->value.binary(), std::move(result.guard));
        }
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
    static void evict(const Key& id) {
        if constexpr (HasGDSF) {
            removeGhost(id);
        }
        bumpGeneration(id);
        cache().invalidate(id);
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
    };

    /// Sweep one chunk (lock-free, always succeeds).
    static bool trySweep() {
        if constexpr (!HasCleanup) {
            return false;
        } else {
            auto& policy = cache::GDSFPolicy::instance();
            CleanupContext ctx{Clock::now(), HasGDSF ? policy.threshold() : 0.0f};

            // Ghost candidates collected during sweep (inserted after).
            // Only collect when under moderate pressure (50-100%), not during
            // emergency sweeps (>100%) — those need to free memory, not add ghosts.
            std::vector<typename CleanupContext::GhostCandidate> candidates;
            if constexpr (HasGDSF) {
                if (policy.hasMemoryPressure() && !policy.isOverBudget()) {
                    ctx.ghost_candidates = &candidates;
                }
            }

            auto removed = cache().cleanup_next_chunk(kChunkCount,
                [&ctx](const Key& key, auto& header) {
                    if constexpr (HasGDSF) {
                        if (header.metadata.isGhost()) {
                            return ghostCleanupPredicate(header.metadata);
                        }
                    }
                    auto& ce = static_cast<typename L1Cache::CacheEntry&>(header);
                    return cleanupPredicate(key, ce.metadata, ce.value, ctx);
                });

            // Insert ghosts for evicted entries (post-sweep)
            if constexpr (HasGDSF) {
                for (auto& gc : candidates) {
                    Metadata meta(gc.count | cache::GDSFScoreData::kGhostFlag, 0);
                    if (cache().insert_ghost(gc.key,
                            cache::GhostData{gc.bytes, gc.flags}, meta)) {
                        policy.charge(static_cast<int64_t>(kGhostOverhead));
                    }
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
    static size_t purge() {
        if constexpr (!HasCleanup) {
            return 0;
        } else {
            auto& policy = cache::GDSFPolicy::instance();
            CleanupContext ctx{Clock::now(), HasGDSF ? policy.threshold() : 0.0f};

            std::vector<typename CleanupContext::GhostCandidate> candidates;
            if constexpr (HasGDSF) {
                if (policy.hasMemoryPressure() && !policy.isOverBudget()) {
                    ctx.ghost_candidates = &candidates;
                }
            }

            auto removed = cache().full_cleanup(
                [&ctx](const Key& key, auto& header) {
                    if constexpr (HasGDSF) {
                        if (header.metadata.isGhost()) {
                            return ghostCleanupPredicate(header.metadata);
                        }
                    }
                    auto& ce = static_cast<typename L1Cache::CacheEntry&>(header);
                    return cleanupPredicate(key, ce.metadata, ce.value, ctx);
                });

            if constexpr (HasGDSF) {
                for (auto& gc : candidates) {
                    Metadata meta(gc.count | cache::GDSFScoreData::kGhostFlag, 0);
                    if (cache().insert_ghost(gc.key,
                            cache::GhostData{gc.bytes, gc.flags}, meta)) {
                        policy.charge(static_cast<int64_t>(kGhostOverhead));
                    }
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
    using L1Cache = cache::ChunkMap<Key, ValueType, Metadata,
        std::conditional_t<HasGDSF, cache::GhostData, void>>;

    /// Returns the static ChunkMap instance.
    /// On first call, auto-registers this repo with GDSFPolicy for global coordination
    /// (when cleanup is enabled: GDSF or TTL > 0).
    static L1Cache& cache() {
        static L1Cache instance;
        static std::once_flag init_flag;
        std::call_once(init_flag, []() {
            config::CachedClock::ensureStarted();
        });
        if constexpr (HasCleanup) {
            static std::once_flag registry_flag;
            std::call_once(registry_flag, []() {
                cache::GDSFPolicy::instance().enroll({
                    .sweep_fn = +[]() -> bool { return sweep(); },
                    .size_fn = +[]() -> size_t { return size(); },
                    .name = static_cast<const char*>(Name)
                });
            });
        }
        return instance;
    }

    /// L1 cache lookup with TTL check and GDSF score bump.
    /// Returns raw FindResult for flexible use by find/findJson/findBinary.
    /// Ghosts are treated as L1 miss (the slow path handles admission).
    static typename L1Cache::FindResult findInCache(const Key& key) {
        auto result = cache().find(key);
        if (!result) return {};

        // Ghost: treated as L1 miss (slow path will handle admission)
        if constexpr (HasGDSF) {
            if (result.entry->metadata.isGhost()) return {};
        }

        auto* entry = result.entry;

        // TTL expiration check: two-phase eviction (find → check → evict)
        if constexpr (HasTTL) {
            if (entry->metadata.isExpired(config::CachedClock::now())) {
                // Remove only if pointer identity matches (guards against concurrent Upsert)
                cache().remove_if(key, [entry](auto* e) { return e == entry; });
                return {};
            }
        }

        if constexpr (HasGDSF) bumpScore(entry->metadata);

        return result;
    }

    /// Get from cache as EntityView (convenience wrapper around findInCache).
    static wrapper::EntityView<Entity> getFromCache(const Key& key) {
        auto result = findInCache(key);
        if (!result) return {};
        auto* ce = result.asReal();
        return wrapper::EntityView<Entity>(
            static_cast<const Entity*>(&ce->value), std::move(result.guard));
    }

    /// Put entity in cache (copy). Returns FindResult for flexible use.
    /// Triggers a global sweep on insertion when the hash check passes.
    static typename L1Cache::FindResult putInCache(const Key& key, const Entity& src,
        Clock::time_point now = Clock::now())
    {
        auto hk = L1Cache::make_key(key);
        auto result = cache().upsert(hk, buildValue(src), buildMetadata(now));
        if constexpr (HasCleanup) {
            if (result.was_insert
                && (L1Cache::get_hash(hk) & cache::GDSFPolicy::kCleanupMask) == 0) {
                fireCleanup();
            }
        }
        return result;
    }

    /// Put entity in cache (move — zero copy). Returns FindResult.
    static typename L1Cache::FindResult putInCache(const Key& key, Entity&& src,
        Clock::time_point now = Clock::now())
    {
        auto hk = L1Cache::make_key(key);
        auto result = cache().upsert(hk, buildValue(std::move(src)), buildMetadata(now));
        if constexpr (HasCleanup) {
            if (result.was_insert
                && (L1Cache::get_hash(hk) & cache::GDSFPolicy::kCleanupMask) == 0) {
                fireCleanup();
            }
        }
        return result;
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

    /// Fire a global sweep as a detached coroutine (fire-and-forget).
    static io::DetachedTask fireCleanup() {
        cache::GDSFPolicy::instance().sweep();
        co_return;
    }

private:
    /// Slow path for find(): L1 miss → (L2) → DB → cache.
    static io::Task<wrapper::EntityView<Entity>> findSlow(const Key& id) {
        co_return co_await fetchAndCache(id);
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
    /// (~20B) is created instead of caching the full entity. Ghosts track
    /// access frequency so that popular data is admitted on subsequent fetches.
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

            if (policy.hasMemoryPressure()) {
                // --- Lookup existing ghost ---
                auto ghost_result = cache().find(id);
                auto* ge = ghost_result ? ghost_result.asGhost() : nullptr;

                // --- Compute score ---
                uint32_t est_bytes = static_cast<uint32_t>(
                    entity->memoryUsage() + kCacheEntryOverhead);
                uint8_t est_flags = static_cast<uint8_t>(
                    (entity->hasBinaryCache() ? 0x01 : 0) |
                    (entity->hasJsonCache()   ? 0x02 : 0));

                uint32_t count;
                if (ge) {
                    // Ghost exists: bump counter, update estimated_bytes
                    uint32_t raw = ge->metadata.access_count.load(std::memory_order_relaxed);
                    uint32_t old_count = raw & cache::GDSFScoreData::kCountMask;
                    count = old_count + cache::GDSFScoreData::kCountScale;
                    ge->metadata.access_count.store(
                        count | cache::GDSFScoreData::kGhostFlag,
                        std::memory_order_relaxed);
                    ge->value.estimated_bytes.store(est_bytes, std::memory_order_relaxed);
                    ge->value.flags.store(est_flags, std::memory_order_relaxed);
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
                    if (ge) {
                        policy.charge(-static_cast<int64_t>(kGhostOverhead));
                        // upsert real → retire ghost automatically via retire()
                    }
                    auto r = putInCache(id, std::move(*entity), now);
                    if (ge) {
                        // Transfer counter (without ghost flag)
                        r.entry->metadata.access_count.store(count,
                            std::memory_order_relaxed);
                    }
                    auto* ce = r.asReal();
                    co_return wrapper::EntityView<Entity>(
                        static_cast<const Entity*>(&ce->value), std::move(r.guard));
                } else {
                    // === GHOST (create or keep existing) ===
                    if (!ge) {
                        // Create new ghost (insert_ghost: never replaces a real entry)
                        if (cache().insert_ghost(id,
                                cache::GhostData{est_bytes, est_flags},
                                Metadata(cache::GDSFScoreData::kCountScale
                                    | cache::GDSFScoreData::kGhostFlag, 0))) {
                            policy.charge(static_cast<int64_t>(kGhostOverhead));
                        }
                        // If insert fails → a real entry was inserted concurrently.
                        // We continue: the entity is returned as transient anyway.
                    }
                    // Return entity without caching (transient pool)
                    auto guard = epoch::EpochGuard::acquire();
                    auto* ptr = transientPool().New(std::move(*entity));
                    // Install ghost hook if ghost exists (re-lookup)
                    auto gr = cache().find(id);
                    auto* ghost = gr ? gr.asGhost() : nullptr;
                    if (ghost) ptr->setMemoryHook(&cache::ghostMemoryHook, &ghost->value);
                    transientPool().Retire(ptr);
                    co_return wrapper::EntityView<Entity>(
                        static_cast<const Entity*>(ptr), std::move(guard));
                }
            }

            // --- No pressure: cache normally ---
            // Remove ghost if one exists (no longer relevant without pressure)
            auto ghost_result = cache().find(id);
            if (ghost_result && ghost_result.entry->metadata.isGhost()) {
                policy.charge(-static_cast<int64_t>(kGhostOverhead));
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

    /// Per-entry memory overhead beyond Entity::memoryUsage().
    /// CachedWrapper fields + Metadata + padding + ParlayHash bucket (key + pointer).
    static constexpr size_t kCacheEntryOverhead =
        sizeof(typename L1Cache::CacheEntry) - sizeof(Entity)
        + sizeof(Key) + sizeof(void*);

    /// Per-ghost memory overhead (EntryHeader + GhostData + key + pointer in map).
    static constexpr size_t kGhostOverhead = [] {
        if constexpr (HasGDSF) {
            return sizeof(typename L1Cache::GhostCacheEntry)
                 + sizeof(Key) + sizeof(void*);
        } else {
            return size_t{0};
        }
    }();

    /// Transient pool for entities returned without caching (ghost REJECT path).
    /// Epoch-based reclamation ensures the entity lives until the EpochGuard drops.
    static epoch::memory_pool<Entity>& transientPool() {
        static auto* p = new epoch::memory_pool<Entity>();
        return *p;
    }

    /// Build ValueType from entity (copy path).
    static ValueType buildValue(const Entity& src) {
        if constexpr (HasGDSF) {
            return cache::CachedWrapper<Entity>(Entity(src), kCacheEntryOverhead);
        } else {
            return Entity(src);
        }
    }

    /// Build ValueType from entity (move path — zero copy).
    static ValueType buildValue(Entity&& src) {
        if constexpr (HasGDSF) {
            return cache::CachedWrapper<Entity>(std::move(src), kCacheEntryOverhead);
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

    /// Bump GDSF access count (simple fetch_add, no decay on read path).
    static void bumpScore(Metadata& meta) {
        meta.access_count.fetch_add(cache::GDSFScoreData::kCountScale,
                                     std::memory_order_relaxed);
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

            // Score = access_count x avg_cost / memoryUsage
            size_t mem = value.memoryUsage();
            float score = meta.computeScore(
                avg_construction_time_us_.load(std::memory_order_relaxed), mem);

            // Record in histogram (ALL entries, before eviction decision)
            cache::GDSFPolicy::instance().recordEntry(score, mem);

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

    /// Ghost cleanup predicate: decay counter, remove if counter reaches 0.
    static bool ghostCleanupPredicate(const Metadata& meta) {
        float dr = cache::GDSFPolicy::instance().decayRate();
        uint32_t count = meta.rawCount();
        uint32_t decayed = static_cast<uint32_t>(static_cast<float>(count) * dr);
        meta.access_count.store(decayed | cache::GDSFScoreData::kGhostFlag,
            std::memory_order_relaxed);
        if (decayed == 0) {
            cache::GDSFPolicy::instance().charge(-static_cast<int64_t>(kGhostOverhead));
            return true;
        }
        return false;
    }

    /// Remove a ghost entry for the given key (if it exists).
    /// Used by evict/invalidate to clean up ghosts on write paths.
    static void removeGhost(const Key& id) {
        auto r = cache().find(id);
        if (r && r.entry->metadata.isGhost()) {
            cache::GDSFPolicy::instance().charge(-static_cast<int64_t>(kGhostOverhead));
            cache().remove(id);
        }
    }

    /// Apply update penalty to a ghost's access count (if ghost exists).
    /// Called by update/patch to erode scores of frequently-mutated data.
    static void applyGhostUpdatePenalty(const Key& id) {
        auto r = cache().find(id);
        if (r && r.entry->metadata.isGhost()) {
            uint32_t raw = r.entry->metadata.access_count.load(std::memory_order_relaxed);
            uint32_t count = raw & cache::GDSFScoreData::kCountMask;
            r.entry->metadata.access_count.store(
                static_cast<uint32_t>(static_cast<float>(count) * cache::GDSFScoreData::kUpdatePenalty)
                | cache::GDSFScoreData::kGhostFlag, std::memory_order_relaxed);
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
