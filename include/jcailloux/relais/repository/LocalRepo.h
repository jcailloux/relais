#ifndef JCX_RELAIS_LOCALREPO_H
#define JCX_RELAIS_LOCALREPO_H

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <type_traits>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/repository/RedisRepo.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/cache/CacheTier.h"
#include "jcailloux/relais/cache/CacheMetadata.h"
#include "jcailloux/relais/cache/CacheView.h"
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
    static constexpr bool HasTTL = (std::chrono::nanoseconds(Cfg.l1_ttl).count() > 0);
    static constexpr bool HasGDSF = cache::GDSFPolicy::enabled;
    static constexpr bool HasCleanup = HasGDSF || HasTTL;

    using Base = std::conditional_t<
        HasRedis,
        RedisRepo<E, Name, Cfg, Key>,
        PgRepo<E, Name, Cfg, Key>
    >;

    using Metadata = cache::CacheMetadata<HasGDSF, HasTTL>;
    using Tier = cache::CacheTier<Key, E, Metadata>;

    /// TTL in seconds (compile-time, for metadata construction).
    static constexpr uint32_t kTtlSec = static_cast<uint32_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::nanoseconds(Cfg.l1_ttl)).count());

public:
    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using FindResultType = cache::CacheView<E>;
    using Base::name;

    static constexpr auto l1Ttl() { return std::chrono::nanoseconds(Cfg.l1_ttl); }

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
    static io::Task<bool> update(const Key& id, const E& entity)
        requires MutableEntity<E> && (!Cfg.read_only)
    {
        using enum config::UpdateStrategy;

        auto outcome = co_await Base::updateOutcome(id, entity);
        if (outcome.success && !outcome.coalesced) {
            if constexpr (Cfg.update_strategy == InvalidateAndLazyReload) {
                evict(id);
            } else {
                tier().onMutation(id);
                bumpGeneration(id);
                tier().store(id, E(entity), buildMetadata());
            }
        }
        co_return outcome.success;
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
        bumpGeneration(id);
        tier().evict(id);
    }

    [[nodiscard]] static size_t size() {
        return tier().size();
    }

    // =========================================================================
    // Cleanup
    // =========================================================================

    /// Sweep one chunk (lock-free, always succeeds).
    static bool trySweep() {
        return tier().sweepChunk(noExtraPred).removed_any;
    }

    /// Sweep one chunk (identical to trySweep in lock-free design).
    static bool sweep() {
        return trySweep();
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
        auto* p = tier_ptr_;
        if (p) [[likely]] return *p;
        return tier_init_slow();
    }

private:
    // =========================================================================
    // CacheTier singleton
    // =========================================================================

    static inline Tier* tier_ptr_{nullptr};

    /// Cold path: construct CacheTier, register with GDSFPolicy, cache pointer.
    [[gnu::noinline]] static Tier& tier_init_slow() {
        struct Holder {
            Tier instance;
            Holder() {
                instance.enroll({
                    .sweep_fn = +[]() -> bool { return sweep(); },
                    .size_fn = +[]() -> size_t { return size(); },
                    .name = static_cast<const char*>(Name)
                });
                tier_ptr_ = &instance;
            }
        };
        static Holder h;
        tier_ptr_ = &h.instance;  // also set here for safety (after static init)
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
    static io::Task<cache::CacheView<E>> findSlow(const Key& id) {
        auto hit = co_await tier().findOrFetch(
            id,
            [&id]() -> io::Task<std::optional<E>> {
                co_return co_await Base::findRaw(id);
            },
            [](const E&, float) -> Metadata { return buildMetadata(); }
        );
        if (hit) {
            co_return cache::CacheView<E>(
                static_cast<const E*>(hit.value), std::move(hit.guard));
        }
        co_return {};
    }

    /// Slow path for findJson(): L1 miss -> dedup -> (L2) -> DB -> cache -> JSON.
    static io::Task<std::string> findJsonSlow(const Key& id) {
        auto hit = co_await tier().findOrFetch(
            id,
            [&id]() -> io::Task<std::optional<E>> {
                co_return co_await Base::findRaw(id);
            },
            [](const E&, float) -> Metadata { return buildMetadata(); }
        );
        if (hit) co_return hit.value->json();
        co_return std::string{};
    }

    /// Slow path for findBinary(): L1 miss -> dedup -> (L2) -> DB -> cache -> binary.
    static io::Task<std::vector<uint8_t>> findBinarySlow(const Key& id)
        requires HasBinarySerialization<E>
    {
        auto hit = co_await tier().findOrFetch(
            id,
            [&id]() -> io::Task<std::optional<E>> {
                co_return co_await Base::findRaw(id);
            },
            [](const E&, float) -> Metadata { return buildMetadata(); }
        );
        if (hit) co_return hit.value->binary();
        co_return std::vector<uint8_t>{};
    }

    // =========================================================================
    // Cleanup predicate (no domain-specific eviction for entities)
    // =========================================================================

    static constexpr auto noExtraPred =
        [](const Key&, const Metadata&, const E&, long) { return false; };

    // =========================================================================
    // Generation counter — stale write prevention (lock-free, cross-thread)
    // =========================================================================
    //
    // Flat array of atomic counters indexed by hash(key) % kGenSlots.
    // Zero allocation, zero epoch overhead.
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

#endif //JCX_RELAIS_LOCALREPO_H
