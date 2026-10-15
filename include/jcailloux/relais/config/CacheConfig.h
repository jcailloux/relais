#ifndef JCX_RELAIS_CACHE_CONFIG_H
#define JCX_RELAIS_CACHE_CONFIG_H

#include <chrono>
#include "jcailloux/relais/config/Duration.h"

namespace jcailloux::relais::config {
    using namespace std::chrono_literals;

    // =========================================================================
    // Cache levels - determines which caching layers are active
    // =========================================================================
    enum class CacheLevel {
        None,   // DB only (PgRepo)
        L1      // RAM -> DB (LocalRepo)
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
    //   using MyRepo = Repo<MyEntity, "MyEntity">;                 // Repo default: Local (L1)
    //   using MyRepo = Repo<MyEntity, "MyEntity", config::Uncached>; // preset
    //   using MyRepo = Repo<MyEntity, "MyEntity",
    //       config::Local.with_l1_ttl(30min).with_read_only()>;    // customized
    //

    struct CacheConfig {
        CacheLevel cache_level = CacheLevel::None;
        bool read_only = false;
        UpdateStrategy update_strategy = UpdateStrategy::InvalidateAndLazyReload;

        // L1 (RAM cache) — eviction is GDSF-based (score = frequency × cost)
        Duration l1_ttl = std::chrono::hours(1);  // Duration{0} = no TTL
        uint8_t l1_chunk_count_log2 = 3;  // 2^3 = 8 chunks (ChunkMap default)

        // Read-fill recheck — sharded generation counter sizing (power-of-2).
        // Bounds memory by accepting collisions (pessimistic miss, never stale).
        // Scales with WRITE concurrency (write_rate × fetch_duration), NOT key
        // count. 2^12 = 4096 slots = 32 KB/repo. Raise it for write-heavy repos.
        uint8_t recheck_slots_log2 = 12;

        // Fluent chainable modifiers (compile-time only)
        consteval CacheConfig with_cache_level(CacheLevel v) const { auto c = *this; c.cache_level = v; return c; }
        consteval CacheConfig with_read_only(bool v = true) const { auto c = *this; c.read_only = v; return c; }
        consteval CacheConfig with_update_strategy(UpdateStrategy v) const { auto c = *this; c.update_strategy = v; return c; }
        consteval CacheConfig with_l1_ttl(Duration v) const { auto c = *this; c.l1_ttl = v; return c; }
        consteval CacheConfig with_l1_chunk_count_log2(uint8_t v) const { auto c = *this; c.l1_chunk_count_log2 = v; return c; }
        consteval CacheConfig with_recheck_slots_log2(uint8_t v) const { auto c = *this; c.recheck_slots_log2 = v; return c; }

        constexpr auto operator<=>(const CacheConfig&) const = default;
    };

    // =========================================================================
    // Presets - common cache configurations
    // =========================================================================

    /// No caching - direct database access only.
    /// E.g., logs history, write-only tables.
    inline constexpr CacheConfig Uncached{};

    /// RAM cache (L1) in front of the database.
    /// E.g., guild/user-related data.
    inline constexpr CacheConfig Local{ .cache_level = CacheLevel::L1 };

}  // namespace jcailloux::relais::config

#endif //JCX_RELAIS_CACHE_CONFIG_H
