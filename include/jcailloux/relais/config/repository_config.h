#ifndef JCX_DROGON_REPOSITORY_CONFIG_H
#define JCX_DROGON_REPOSITORY_CONFIG_H

#include <chrono>
#include "jcailloux/drogon/config/Duration.h"

namespace jcailloux::drogon::smartrepo::config {
    using namespace std::chrono_literals;

    // =========================================================================
    // Cache levels - determines which caching layers are active
    // =========================================================================
    enum class CacheLevel {
        None,   // DB only (BaseRepository)
        L1,     // RAM -> DB (CachedRepository without Redis)
        L2,     // Redis -> DB (RedisRepository)
        L1_L2   // RAM -> Redis -> DB (CachedRepository with Redis)
    };

    // =========================================================================
    // Update strategy - how cache handles updates
    // =========================================================================
    enum class UpdateStrategy {
        InvalidateAndLazyReload,  // Safe: invalidate cache, reload on next read
        PopulateImmediately       // Optimistic: write-through to cache
    };

    // =========================================================================
    // CacheConfig - structural aggregate for NTTP usage
    // =========================================================================
    //
    // All fields are public structural types → usable as template parameter.
    //
    // Usage:
    //   using MyRepo = Repository<MyWrapper, "MyEntity">;                // defaults to Local
    //   using MyRepo = Repository<MyWrapper, "MyEntity", config::Both>;  // preset
    //   using MyRepo = Repository<MyWrapper, "MyEntity",
    //       config::Local.with_l1_ttl(30min).with_read_only()>;          // customized
    //

    struct CacheConfig {
        CacheLevel cache_level = CacheLevel::None;
        bool read_only = false;
        UpdateStrategy update_strategy = UpdateStrategy::InvalidateAndLazyReload;

        // L1 (RAM cache)
        Duration l1_ttl = std::chrono::hours(1);
        uint8_t l1_shard_count_log2 = 3;  // 2^3 = 8 shards (ShardMap default)
        bool l1_refresh_on_get = true;
        bool l1_accept_expired_on_get = true;
        size_t l1_cleanup_every_n_gets = 500;
        Duration l1_cleanup_min_interval = std::chrono::seconds(30);

        // L2 (Redis cache)
        Duration l2_ttl = std::chrono::hours(4);
        bool l2_refresh_on_get = false;

        // Fluent chainable modifiers (compile-time only)
        consteval CacheConfig with_cache_level(CacheLevel v) const { auto c = *this; c.cache_level = v; return c; }
        consteval CacheConfig with_read_only(bool v = true) const { auto c = *this; c.read_only = v; return c; }
        consteval CacheConfig with_update_strategy(UpdateStrategy v) const { auto c = *this; c.update_strategy = v; return c; }
        consteval CacheConfig with_l1_ttl(Duration v) const { auto c = *this; c.l1_ttl = v; return c; }
        consteval CacheConfig with_l1_shard_count_log2(uint8_t v) const { auto c = *this; c.l1_shard_count_log2 = v; return c; }
        consteval CacheConfig with_l1_refresh_on_get(bool v) const { auto c = *this; c.l1_refresh_on_get = v; return c; }
        consteval CacheConfig with_l1_accept_expired_on_get(bool v) const { auto c = *this; c.l1_accept_expired_on_get = v; return c; }
        consteval CacheConfig with_l1_cleanup_every_n_gets(size_t v) const { auto c = *this; c.l1_cleanup_every_n_gets = v; return c; }
        consteval CacheConfig with_l1_cleanup_min_interval(Duration v) const { auto c = *this; c.l1_cleanup_min_interval = v; return c; }
        consteval CacheConfig with_l2_ttl(Duration v) const { auto c = *this; c.l2_ttl = v; return c; }
        consteval CacheConfig with_l2_refresh_on_get(bool v) const { auto c = *this; c.l2_refresh_on_get = v; return c; }

        constexpr auto operator<=>(const CacheConfig&) const = default;
    };

    // =========================================================================
    // Presets - common cache configurations
    // =========================================================================

    /// No caching - direct database access only.
    /// E.g., logs history, write-only tables.
    inline constexpr CacheConfig Uncached{};

    /// RAM cache only (L1) - fast local cache, no Redis.
    /// Perfect for data always accessed via the same API instance.
    /// E.g., guild/user-related data.
    inline constexpr CacheConfig Local{ .cache_level = CacheLevel::L1 };

    /// Redis cache only (L2) - shared cache across instances, no local RAM cache.
    /// Perfect for data that can be accessed via any API instance.
    /// E.g., admin metrics, global counters.
    inline constexpr CacheConfig Redis{
        .cache_level = CacheLevel::L2,
        .l2_ttl = std::chrono::hours(4),
    };

    /// Full caching (L1 + L2) - RAM cache backed by Redis.
    /// Typical use-case: short L1 TTL + long L2 TTL.
    /// E.g., slash commands data, feature flags, DB statistics.
    inline constexpr CacheConfig Both{
        .cache_level = CacheLevel::L1_L2,
        .l1_ttl = std::chrono::minutes(1),
        .l2_ttl = std::chrono::hours(1),
    };

}  // namespace jcailloux::drogon::smartrepo::config

#endif //JCX_DROGON_REPOSITORY_CONFIG_H
