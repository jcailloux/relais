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
#include "jcailloux/relais/cache/GDSFMetadata.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"
#include <jcailloux/shardmap/ShardMap.h>
#include "jcailloux/relais/config/repo_config.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais {

/**
 * Repo with L1 RAM cache.
 *
 * Supports two modes based on Cfg.cache_level:
 * - CacheLevel::L1:    RAM -> Database (Redis bypassed)
 * - CacheLevel::L1_L2: RAM -> Redis -> Database (full hierarchy)
 *
 * Eviction policy depends on compile-time configuration:
 * - GDSF (score = frequency x cost) when RELAIS_L1_MAX_MEMORY > 0
 * - TTL-only when l1_ttl > 0 but no GDSF
 * - No cleanup when neither is configured (default)
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
    static constexpr bool HasGDSF = (cache::GDSFPolicy::kMaxMemory > 0);
    static constexpr bool HasCleanup = HasGDSF || HasTTL;
    static constexpr size_t kShardCount = 1u << Cfg.l1_shard_count_log2;

    using Base = std::conditional_t<
        HasRedis,
        RedisRepo<Entity, Name, Cfg, Key>,
        BaseRepo<Entity, Name, Cfg, Key>
    >;

    using Mapping = typename Entity::MappingType;
    using Clock = std::chrono::steady_clock;
    using Metadata = cache::CacheMetadata<HasGDSF, HasTTL>;

public:
    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::WrapperPtrType;
    using Base::name;

    using EntityPtr = WrapperPtrType;

    static constexpr auto l1Ttl() { return std::chrono::nanoseconds(Cfg.l1_ttl); }

    static constexpr shardmap::ShardMapConfig kShardMapConfig{
        .shard_count_log2 = Cfg.l1_shard_count_log2
    };

    /// Find by ID with L1 -> (L2) -> DB fallback.
    /// Returns shared_ptr to immutable entity (nullptr if not found).
    static io::Task<WrapperPtrType> find(const Key& id) {
        if (auto cached = getFromCache(id)) {
            co_return cached;
        }

        if constexpr (HasGDSF) {
            auto start = Clock::now();
            auto ptr = co_await Base::find(id);
            if (ptr) {
                auto elapsed_us = static_cast<float>(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        Clock::now() - start).count());
                updateAvgConstructionTime(elapsed_us);
                putInCache(id, ptr);
            }
            co_return ptr;
        } else {
            auto ptr = co_await Base::find(id);
            if (ptr) {
                putInCache(id, ptr);
            }
            co_return ptr;
        }
    }

    /// Find by ID and return raw JSON string.
    /// Returns shared_ptr to JSON string (nullptr if not found).
    static io::Task<std::shared_ptr<const std::string>> findJson(const Key& id) {
        if (auto cached = getFromCache(id)) {
            co_return cached->json();
        }

        auto ptr = co_await find(id);
        co_return ptr ? ptr->json() : nullptr;
    }

    /// Find by ID and return raw binary (BEVE).
    /// Returns shared_ptr to binary data (nullptr if not found).
    static io::Task<std::shared_ptr<const std::vector<uint8_t>>> findBinary(const Key& id)
        requires HasBinarySerialization<Entity>
    {
        if (auto cached = getFromCache(id)) {
            co_return cached->binary();
        }

        if constexpr (HasRedis) {
            if constexpr (HasGDSF) {
                auto start = Clock::now();
                auto bin = co_await Base::findBinary(id);
                if (bin) {
                    auto elapsed_us = static_cast<float>(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            Clock::now() - start).count());
                    updateAvgConstructionTime(elapsed_us);
                    if (auto entity = Entity::fromBinary(*bin)) {
                        putInCache(id, std::make_shared<const Entity>(std::move(*entity)));
                    }
                }
                co_return bin;
            } else {
                auto bin = co_await Base::findBinary(id);
                if (bin) {
                    if (auto entity = Entity::fromBinary(*bin)) {
                        putInCache(id, std::make_shared<const Entity>(std::move(*entity)));
                    }
                }
                co_return bin;
            }
        } else {
            if constexpr (HasGDSF) {
                auto start = Clock::now();
                auto ptr = co_await Base::find(id);
                if (ptr) {
                    auto elapsed_us = static_cast<float>(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            Clock::now() - start).count());
                    updateAvgConstructionTime(elapsed_us);
                    putInCache(id, ptr);
                    co_return ptr->binary();
                }
                co_return nullptr;
            } else {
                auto ptr = co_await Base::find(id);
                if (ptr) {
                    putInCache(id, ptr);
                    co_return ptr->binary();
                }
                co_return nullptr;
            }
        }
    }

    /// Insert entity and cache it. Returns shared_ptr to immutable entity.
    /// Compile-time error if Cfg.read_only is true.
    static io::Task<WrapperPtrType> insert(WrapperPtrType wrapper)
        requires CreatableEntity<Entity, Key> && (!Cfg.read_only)
    {
        auto inserted = co_await Base::insert(wrapper);
        if (inserted) {
            putInCache(inserted->key(), inserted);
            co_return inserted;
        }
        co_return nullptr;
    }

    /// Update entity in database with L1 cache handling.
    /// Returns true on success, false on error.
    /// Compile-time error if Cfg.read_only is true.
    static io::Task<bool> update(const Key& id, WrapperPtrType wrapper)
        requires MutableEntity<Entity> && (!Cfg.read_only)
    {
        using enum config::UpdateStrategy;

        bool success = co_await Base::update(id, wrapper);
        if (success) {
            if constexpr (Cfg.update_strategy == InvalidateAndLazyReload) {
                evict(id);
            } else {
                putInCache(id, wrapper);
            }
        }
        co_return success;
    }

    /// Partial update: invalidates L1 then delegates to Base::patch.
    /// Returns the re-fetched entity (nullptr on error or not found).
    template<typename... Updates>
    static io::Task<WrapperPtrType> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Cfg.read_only)
    {
        evict(id);
        co_return co_await Base::patch(id, std::forward<Updates>(updates)...);
    }

    /// Erase entity by ID.
    /// Returns: rows deleted (0 if not found), or nullopt on DB error.
    /// Invalidates L1 cache unless DB error occurred (self-healing).
    /// For CompositeKey entities, provides L1 cache hint for partition pruning.
    /// Compile-time error if Cfg.read_only is true.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Cfg.read_only)
    {
        // Provide L1 hint for partition pruning (free: ~0ns RAM lookup)
        WrapperPtrType hint = nullptr;
        if constexpr (HasPartitionHint<Entity>) {
            hint = getFromCache(id);
        }

        auto result = co_await Base::eraseImpl(id, std::move(hint));
        if (result.has_value()) {
            evict(id);
        }
        co_return result;
    }

    /// Invalidate L1 and L2 caches for a key.
    static io::Task<void> invalidate(const Key& id) {
        evict(id);
        if constexpr (HasRedis) {
            co_await Base::evictRedis(id);
        }
    }

    /// Invalidate L1 cache only. Non-coroutine since there is no async work.
    static void evict(const Key& id) {
        cache().invalidate(id);
    }

    [[nodiscard]] static size_t size() {
        return cache().size();
    }

    /// Context passed to cleanup predicates. Accumulates score statistics
    /// across all entries visited in the shard (mutable for const& access).
    struct CleanupContext {
        Clock::time_point now;
        float threshold;
        mutable float score_sum{0.0f};
        mutable size_t score_count{0};
        mutable float kept_score_sum{0.0f};
        mutable size_t kept_count{0};
    };

    /// Try to sweep one shard (non-blocking).
    /// Returns immediately if a sweep is already in progress.
    static bool trySweep() {
        if constexpr (!HasCleanup) {
            return false;
        } else {
            CleanupContext ctx{Clock::now(), HasGDSF ? cache::GDSFPolicy::instance().threshold() : 0.0f};
            auto result = cache().try_cleanup(ctx, cleanupPredicate);
            if (result.has_value()) {
                if constexpr (HasGDSF) postCleanup(ctx);
                return true;
            }
            return false;
        }
    }

    /// Sweep one shard (blocking, waits if cleanup in progress).
    static bool sweep() {
        if constexpr (!HasCleanup) {
            return false;
        } else {
            CleanupContext ctx{Clock::now(), HasGDSF ? cache::GDSFPolicy::instance().threshold() : 0.0f};
            auto result = cache().cleanup(ctx, cleanupPredicate);
            if constexpr (HasGDSF) postCleanup(ctx);
            return result.removed > 0;
        }
    }

    /// Sweep all shards.
    static size_t purge() {
        if constexpr (!HasCleanup) {
            return 0;
        } else {
            CleanupContext ctx{Clock::now(), HasGDSF ? cache::GDSFPolicy::instance().threshold() : 0.0f};
            auto removed = cache().full_cleanup(ctx, cleanupPredicate);
            if constexpr (HasGDSF) postCleanup(ctx);
            return removed;
        }
    }

    /// Prime L1 cache at startup.
    /// ListMixin overrides this via method hiding to also warm up the list cache.
    static void warmup() {
        RELAIS_LOG_DEBUG << name() << ": warming up L1 cache...";
        // Ensure the static ShardMap instance is constructed + repo registered.
        (void)cache();
        RELAIS_LOG_DEBUG << name() << ": L1 cache primed";
    }

    /// Current repo score (exposed for testing/debugging).
    static float repoScore() {
        return repo_score_.load(std::memory_order_relaxed);
    }

    /// Current average construction time in us (exposed for testing/debugging).
    static float avgConstructionTime() {
        return avg_construction_time_us_.load(std::memory_order_relaxed);
    }

protected:
    using L1Cache = shardmap::ShardMap<Key, EntityPtr, Metadata, kShardMapConfig>;

    /// Returns the static ShardMap instance.
    /// On first call, auto-registers this repo with GDSFPolicy for global coordination
    /// (only when GDSF is enabled).
    static L1Cache& cache() {
        static L1Cache instance;
        if constexpr (HasGDSF) {
            static std::once_flag registry_flag;
            std::call_once(registry_flag, []() {
                cache::GDSFPolicy::instance().enroll({
                    .sweep_fn = +[]() -> bool { return sweep(); },
                    .size_fn = +[]() -> size_t { return size(); },
                    .repo_score_fn = +[]() -> float {
                        return repo_score_.load(std::memory_order_relaxed);
                    },
                    .name = static_cast<const char*>(Name)
                });
            });
        }
        return instance;
    }

    /// Get from cache with optional GDSF score bump + optional TTL check.
    /// Returns nullptr if not found or TTL-expired (invalidated).
    static EntityPtr getFromCache(const Key& key) {
        auto result = cache().get(key, [](const EntityPtr&, auto& meta) {
            if constexpr (HasTTL && !HasGDSF) {
                if (meta.isExpired(Clock::now())) {
                    return shardmap::GetAction::Invalidate;
                }
            }
            if constexpr (HasGDSF) {
                if constexpr (HasTTL) {
                    if (meta.isExpired(Clock::now())) {
                        return shardmap::GetAction::Invalidate;
                    }
                }
                auto& policy = cache::GDSFPolicy::instance();
                policy.decay(meta);
                float cost = avg_construction_time_us_.load(std::memory_order_relaxed);
                meta.score.fetch_add(cost, std::memory_order_relaxed);
            }
            return shardmap::GetAction::Accept;
        });

        if constexpr (HasCleanup && Cfg.l1_cleanup_every_n_gets > 0) {
            maybeCleanup();
        }

        return result;
    }

    /// Attempt a partial cleanup if N gets have elapsed and min interval has passed.
    static void maybeCleanup() {
        static std::atomic<uint32_t> get_counter{0};
        static std::atomic<typename Clock::rep> last_cleanup_time{0};

        if (get_counter.fetch_add(1, std::memory_order_relaxed) % Cfg.l1_cleanup_every_n_gets != 0)
            return;

        const auto now = Clock::now().time_since_epoch().count();
        auto last = last_cleanup_time.load(std::memory_order_relaxed);
        const auto min_interval = std::chrono::duration_cast<typename Clock::duration>(
            std::chrono::nanoseconds(Cfg.l1_cleanup_min_interval)).count();

        if (now - last < min_interval)
            return;

        if (!last_cleanup_time.compare_exchange_strong(last, now, std::memory_order_relaxed))
            return;

        // Check memory budget first — emergency cleanup if over budget
        if constexpr (HasGDSF) {
            auto& policy = cache::GDSFPolicy::instance();
            if (policy.isOverBudget()) {
                policy.emergencyCleanup();
                return;
            }
        }
        trySweep();
    }

    /// Put shared_ptr in cache with appropriate metadata.
    /// When GDSF is enabled, wraps the entity in CachedWrapper for memory tracking.
    static void putInCache(const Key& key, EntityPtr ptr) {
        if constexpr (HasGDSF) {
            float cost = avg_construction_time_us_.load(std::memory_order_relaxed);
            uint32_t gen = cache::GDSFPolicy::instance().generation();

            int64_t ttl_rep = 0;
            if constexpr (HasTTL) {
                ttl_rep = (Clock::now() + l1Ttl()).time_since_epoch().count();
            }

            constexpr size_t kOverhead = sizeof(Key) + sizeof(Metadata);
            auto cached = std::make_shared<const cache::CachedWrapper<Entity>>(
                Entity(*ptr), kOverhead);
            // Implicit conversion: shared_ptr<const CachedWrapper<Entity>> -> shared_ptr<const Entity>
            EntityPtr tracked = cached;

            cache().put(key, std::move(tracked), Metadata{cost, gen, ttl_rep});
        } else if constexpr (HasTTL) {
            int64_t ttl_rep = (Clock::now() + l1Ttl()).time_since_epoch().count();
            cache().put(key, ptr, Metadata{ttl_rep});
        } else {
            cache().put(key, ptr, Metadata{});
        }
    }

private:
    /// Cleanup predicate: evict based on GDSF score and/or TTL expiration.
    static bool cleanupPredicate(const Key&, const Metadata& meta, const CleanupContext& ctx) {
        if constexpr (HasGDSF) {
            cache::GDSFPolicy::instance().decay(meta);

            float score = meta.score.load(std::memory_order_relaxed);
            ctx.score_sum += score;
            ctx.score_count++;

            // Evict if TTL expired
            if constexpr (HasTTL) {
                if (meta.isExpired(ctx.now)) return true;
            }

            // Evict if score below threshold
            if (score < ctx.threshold) return true;

            // Survivor
            ctx.kept_score_sum += score;
            ctx.kept_count++;
            return false;
        } else if constexpr (HasTTL) {
            return meta.isExpired(ctx.now);
        } else {
            return false;  // unreachable — cleanup is disabled
        }
    }

    /// Post-cleanup: update repo_score, correction coefficient, and tick generation.
    static void postCleanup(const CleanupContext& ctx) {
        if (ctx.kept_count > 0) {
            float avg_kept = ctx.kept_score_sum / static_cast<float>(ctx.kept_count);
            float old_rs = repo_score_.load(std::memory_order_relaxed);
            float new_rs = (old_rs * static_cast<float>(kShardCount - 1) + avg_kept)
                         / static_cast<float>(kShardCount);

            // CAS without retry — approximation is fine
            repo_score_.compare_exchange_weak(old_rs, new_rs, std::memory_order_relaxed);

            // Update correction coefficient
            cache::GDSFPolicy::instance().updateCorrection(avg_kept, old_rs);
        }

        cache::GDSFPolicy::instance().tick();
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
    // Kept even when !HasGDSF (2 floats per type, negligible) to simplify test accessors.
    static inline std::atomic<float> avg_construction_time_us_{0.0f};
    static inline std::atomic<float> repo_score_{0.0f};

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::relais

#endif //JCX_RELAIS_CACHEDREPO_H
