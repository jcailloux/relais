#ifndef JCX_DROGON_CACHEDREPOSITORY_H
#define JCX_DROGON_CACHEDREPOSITORY_H

#include <atomic>
#include <chrono>
#include <memory>
#include <type_traits>
#include "jcailloux/drogon/smartrepo/RedisRepository.h"
#include <jcailloux/shardmap/ShardMap.h>
#include "jcailloux/drogon/config/repository_config.h"

#ifdef SMARTREPO_BUILDING_TESTS
namespace smartrepo_test { struct TestInternals; }
#endif

namespace jcailloux::drogon::smartrepo {

/// Metadata stored alongside each entity in the L1 cache.
/// When RefreshOnGet is true, uses std::atomic<int64_t> for thread-safe
/// mutation under ShardMap's shared lock (concurrent get() refresh TTL).
/// When false, plain int64_t suffices (writes only under exclusive lock in put()).
template<bool RefreshOnGet>
struct EntityCacheMetadata {
    using Rep = std::conditional_t<RefreshOnGet, std::atomic<int64_t>, int64_t>;
    Rep expiration_rep{0};

    EntityCacheMetadata() = default;
    explicit EntityCacheMetadata(std::chrono::steady_clock::time_point tp)
        : expiration_rep(tp.time_since_epoch().count()) {}

    // Non-atomic: trivially copyable
    EntityCacheMetadata(const EntityCacheMetadata&) requires (!RefreshOnGet) = default;
    EntityCacheMetadata& operator=(const EntityCacheMetadata&) requires (!RefreshOnGet) = default;
    EntityCacheMetadata(EntityCacheMetadata&&) requires (!RefreshOnGet) = default;
    EntityCacheMetadata& operator=(EntityCacheMetadata&&) requires (!RefreshOnGet) = default;

    // Atomic: manual copy/move via relaxed load/store
    EntityCacheMetadata(const EntityCacheMetadata& o) requires RefreshOnGet
        : expiration_rep(o.expiration_rep.load(std::memory_order_relaxed)) {}
    EntityCacheMetadata& operator=(const EntityCacheMetadata& o) requires RefreshOnGet {
        expiration_rep.store(o.expiration_rep.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
        return *this;
    }
    EntityCacheMetadata(EntityCacheMetadata&& o) noexcept requires RefreshOnGet
        : expiration_rep(o.expiration_rep.load(std::memory_order_relaxed)) {}
    EntityCacheMetadata& operator=(EntityCacheMetadata&& o) noexcept requires RefreshOnGet {
        expiration_rep.store(o.expiration_rep.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
        return *this;
    }

    std::chrono::steady_clock::time_point expiration() const {
        if constexpr (RefreshOnGet) {
            return std::chrono::steady_clock::time_point{
                std::chrono::steady_clock::duration{expiration_rep.load(std::memory_order_relaxed)}};
        } else {
            return std::chrono::steady_clock::time_point{
                std::chrono::steady_clock::duration{expiration_rep}};
        }
    }
    void setExpiration(std::chrono::steady_clock::time_point tp) {
        if constexpr (RefreshOnGet) {
            expiration_rep.store(tp.time_since_epoch().count(), std::memory_order_relaxed);
        } else {
            expiration_rep = tp.time_since_epoch().count();
        }
    }
};

/**
 * Repository with L1 RAM cache.
 *
 * Supports two modes based on Cfg.cache_level:
 * - CacheLevel::L1:    RAM -> Database (Redis bypassed)
 * - CacheLevel::L1_L2: RAM -> Redis -> Database (full hierarchy)
 *
 * Note: L1RepoConfig constraint is verified in Repository.h to avoid
 * eager evaluation issues with std::conditional_t.
 */
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires CacheableEntity<Entity, typename Entity::Model>
class CachedRepository : public std::conditional_t<
    Cfg.cache_level == config::CacheLevel::L1,
    BaseRepository<Entity, Name, Cfg, Key>,
    RedisRepository<Entity, Name, Cfg, Key>
> {
    static constexpr bool HasRedis = (Cfg.cache_level == config::CacheLevel::L1_L2);

    using Base = std::conditional_t<
        HasRedis,
        RedisRepository<Entity, Name, Cfg, Key>,
        BaseRepository<Entity, Name, Cfg, Key>
    >;

    using Model = typename Entity::Model;
    using Clock = std::chrono::steady_clock;
    using Metadata = EntityCacheMetadata<Cfg.l1_refresh_on_get>;

public:
    using typename Base::EntityType;
    using typename Base::ModelType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::WrapperPtrType;
    using Base::name;

    // Alias for backward compatibility and internal use
    using EntityPtr = WrapperPtrType;

    static constexpr auto l1Ttl() { return std::chrono::nanoseconds(Cfg.l1_ttl); }

    static constexpr shardmap::ShardMapConfig kShardMapConfig{
        .shard_count_log2 = Cfg.l1_shard_count_log2
    };

    /// Find by ID with L1 -> (L2) -> DB fallback.
    /// Returns shared_ptr to immutable entity (nullptr if not found).
    static ::drogon::Task<WrapperPtrType> findById(const Key& id) {
        if (auto cached = getFromCache(id)) {
            co_return cached;
        }

        auto ptr = co_await Base::findById(id);
        if (ptr) {
            putInCache(id, ptr);
        }
        co_return ptr;
    }

    /// Find by ID and return raw JSON string.
    /// Returns shared_ptr to JSON string (nullptr if not found).
    static ::drogon::Task<std::shared_ptr<const std::string>> findByIdAsJson(const Key& id) {
        if (auto cached = getFromCache(id)) {
            co_return cached->toJson();
        }

        if constexpr (HasRedis) {
            auto json = co_await Base::findByIdAsJson(id);
            if (json) {
                if (auto entity = Entity::fromJson(*json)) {
                    putInCache(id, std::make_shared<const Entity>(std::move(*entity)));
                }
            }
            co_return json;
        } else {
            auto ptr = co_await Base::findById(id);
            if (ptr) {
                putInCache(id, ptr);
                co_return ptr->toJson();
            }
            co_return nullptr;
        }
    }

    /// Create entity and cache it. Returns shared_ptr to immutable entity.
    /// Compile-time error if Cfg.read_only is true.
    static ::drogon::Task<WrapperPtrType> create(WrapperPtrType wrapper)
        requires CreatableEntity<Entity, Model, Key> && (!Cfg.read_only)
    {
        auto inserted = co_await Base::create(wrapper);
        if (inserted) {
            // Populate L1 cache with the new entity
            putInCache(inserted->getPrimaryKey(), inserted);
            co_return inserted;
        }
        co_return nullptr;
    }

    /// Update entity in database with L1 cache handling.
    /// Returns true on success, false on error.
    /// Compile-time error if Cfg.read_only is true.
    static ::drogon::Task<bool> update(const Key& id, WrapperPtrType wrapper)
        requires MutableEntity<Entity, Model> && (!Cfg.read_only)
    {
        using enum config::UpdateStrategy;

        bool success = co_await Base::update(id, wrapper);
        if (success) {
            // Update L1 cache based on configured strategy
            if constexpr (Cfg.update_strategy == InvalidateAndLazyReload) {
                invalidateL1Internal(id);
            } else {
                putInCache(id, wrapper);
            }
        }
        co_return success;
    }

    /// Partial update: invalidates L1 then delegates to Base::updateBy.
    /// Returns the re-fetched entity (nullptr on error or not found).
    template<typename... Updates>
    static ::drogon::Task<WrapperPtrType> updateBy(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Cfg.read_only)
    {
        invalidateL1Internal(id);
        co_return co_await Base::updateBy(id, std::forward<Updates>(updates)...);
    }

    /// Remove entity by ID.
    /// Returns: rows deleted (0 if not found), or nullopt on DB error.
    /// Invalidates L1 cache unless DB error occurred (self-healing).
    /// For PartialKey repos, provides L1 cache hint for partition pruning.
    /// Compile-time error if Cfg.read_only is true.
    static ::drogon::Task<std::optional<size_t>> remove(const Key& id)
        requires (!Cfg.read_only)
    {
        WrapperPtrType hint;
        if constexpr (!std::is_same_v<Key, typename Model::PrimaryKeyType>) {
            hint = getFromCache(id);  // Free L1 check, no DB
        }
        auto result = co_await Base::removeImpl(id, std::move(hint));
        if (result.has_value()) {
            invalidateL1Internal(id);
        }
        co_return result;
    }

    /// Invalidate L1 and L2 caches for a key.
    static ::drogon::Task<void> invalidate(const Key& id) {
        invalidateL1Internal(id);
        if constexpr (HasRedis) {
            co_await Base::invalidateRedis(id);
        }
    }

    /// Invalidate L1 cache only. Non-coroutine since there is no async work.
    static void invalidateL1(const Key& id) {
        invalidateL1Internal(id);
    }

private:
    /// Internal L1 invalidation.
    static void invalidateL1Internal(const Key& id) {
        cache().invalidate(id);
    }

public:

    [[nodiscard]] static size_t cacheSize() {
        return cache().size();
    }

    /// Context passed to cleanup callbacks.
    struct CleanupContext {
        Clock::time_point now;
    };

    /// Try to trigger a cleanup (non-blocking).
    /// Returns true if cleanup was performed, false if another cleanup is in progress.
    static bool triggerCleanup() {
        CleanupContext ctx{Clock::now()};
        return cache().try_cleanup(ctx, [](const Key&,
                                            const Metadata& meta,
                                            const CleanupContext& ctx) {
            return meta.expiration() < ctx.now;
        }).has_value();
    }

    /// Full cleanup (blocking) - processes all shards.
    static size_t fullCleanup() {
        CleanupContext ctx{Clock::now()};
        return cache().full_cleanup(ctx, [](const Key&,
                                             const Metadata& meta,
                                             const CleanupContext& ctx) {
            return meta.expiration() < ctx.now;
        });
    }

    /// Prime L1 cache at startup.
    /// ListMixin overrides this via method hiding to also warm up the list cache.
    static void warmup() {
        LOG_DEBUG << name() << ": warming up L1 cache...";
        // Ensure the static ShardMap instance is constructed.
        (void)cache();
        LOG_DEBUG << name() << ": L1 cache primed";
    }

protected:
    using L1Cache = shardmap::ShardMap<Key, EntityPtr, Metadata, kShardMapConfig>;

    static L1Cache& cache() {
        static L1Cache instance;
        return instance;
    }

    /// Get from cache with TTL validation via callback.
    /// Returns nullptr if not found or expired (based on Cfg.l1_accept_expired_on_get).
    static EntityPtr getFromCache(const Key& key) {
        const auto now = Clock::now();
        auto result = cache().get(key, [&now](const EntityPtr&, Metadata& meta) {
            if constexpr (!Cfg.l1_accept_expired_on_get) {
                if (meta.expiration() < now) {
                    return shardmap::GetAction::Invalidate;
                }
            }

            if constexpr (Cfg.l1_refresh_on_get) {
                meta.setExpiration(now + l1Ttl());
            }

            return shardmap::GetAction::Accept;
        });

        if constexpr (Cfg.l1_cleanup_every_n_gets > 0) {
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

        triggerCleanup();
    }

    /// Put entity in cache (wraps in shared_ptr).
    static void putInCache(const Key& key, const Entity& entity) {
        cache().put(key, std::make_shared<const Entity>(entity),
                    Metadata{Clock::now() + l1Ttl()});
    }

    /// Put shared_ptr in cache (no copy).
    static void putInCache(const Key& key, EntityPtr ptr) {
        cache().put(key, std::move(ptr),
                    Metadata{Clock::now() + l1Ttl()});
    }

#ifdef SMARTREPO_BUILDING_TESTS
    friend struct ::smartrepo_test::TestInternals;
#endif
};

}  // namespace jcailloux::drogon::smartrepo

#endif //JCX_DROGON_CACHEDREPOSITORY_H
