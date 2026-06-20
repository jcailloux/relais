#ifndef JCX_RELAIS_REPO_H
#define JCX_RELAIS_REPO_H

#include <optional>
#include <span>
#include <type_traits>
#include <vector>
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/repository/LocalRepo.h"
#include "jcailloux/relais/repository/InvalidationMixin.h"
#include "jcailloux/relais/repository/ListMixin.h"
#include "jcailloux/relais/config/FixedString.h"
#include "jcailloux/relais/entity/EntityConcepts.h"
#include "jcailloux/relais/cache/Metrics.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"

namespace jcailloux::relais {

// =============================================================================
// RepoBuilder — assembles the mixin chain from template parameters
// =============================================================================
//
// Chain (bottom to top):
//   PgRepo
//     ↑ (if L2 or L1_L2)
//   RedisRepo
//     ↑ (if L1 or L1_L2)
//   LocalRepo
//     ↑ (if E has ListDescriptor)
//   ListMixin
//     ↑ (if Invalidations... non-empty)
//   InvalidationMixin
//
// The final Repo class sits on top and adds convenience methods.
//

namespace detail {

/// Select the cache layer based on CacheConfig::cache_level
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
struct CacheLayerSelector {
    using type = std::conditional_t<
        Cfg.cache_level == config::CacheLevel::L1 || Cfg.cache_level == config::CacheLevel::L1_L2,
        LocalRepo<E, Name, Cfg, Key>,
        std::conditional_t<
            Cfg.cache_level == config::CacheLevel::L2,
            RedisRepo<E, Name, Cfg, Key>,
            PgRepo<E, Name, Cfg, Key>
        >
    >;
};

/// Stack optional mixins on top of the cache layer
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key,
         typename... Invalidations>
struct MixinStack {
    using CacheLayer = typename CacheLayerSelector<E, Name, Cfg, Key>::type;

    using WithList = std::conditional_t<
        HasListDescriptor<E>,
        ListMixin<CacheLayer>,
        CacheLayer
    >;

    using type = std::conditional_t<
        (sizeof...(Invalidations) > 0),
        InvalidationMixin<WithList, Invalidations...>,
        WithList
    >;
};

}  // namespace detail

// =============================================================================
// Repo — final class with convenience methods
// =============================================================================
//
// Usage:
//   using MyRepo = Repo<MyEntity, "MyEntity">;                      // L1 (default)
//   using MyRepo = Repo<MyEntity, "MyEntity", config::Both>;        // L1+L2
//   using MyRepo = Repo<MyEntity, "MyEntity", config::Local,
//       Invalidate<OtherRepo, &MyStruct::other_id>>;                // with cross-inv
//

template<typename E, config::FixedString Name, config::CacheConfig Cfg = config::Local,
         typename... Invalidations>
class Repo
    : public detail::MixinStack<
          E, Name, Cfg,
          decltype(std::declval<const E>().key()),
          Invalidations...
      >::type
{
    using Key = decltype(std::declval<const E>().key());
    using Base = typename detail::MixinStack<E, Name, Cfg, Key, Invalidations...>::type;

    // Compile-time validation
    static_assert(ReadableEntity<E>,
        "E must satisfy ReadableEntity (provide fromRow)");

    static_assert(
        Cfg.cache_level == config::CacheLevel::None ||
        CacheableEntity<E>,
        "Cached entities must satisfy CacheableEntity (provide JSON or binary serialization)");

    static_assert(
        Cfg.cache_level != config::CacheLevel::L1 &&
        Cfg.cache_level != config::CacheLevel::L1_L2 ||
        (Cfg.l1_chunk_count_log2 >= 1),
        "L1 cache requires l1_chunk_count_log2 >= 1");

    static_assert(
        Cfg.cache_level != config::CacheLevel::L2 &&
        Cfg.cache_level != config::CacheLevel::L1_L2 ||
        Cfg.l2_ttl.ns > 0,
        "L2 cache requires l2_ttl > 0");

public:
    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::FindResultType;

    // Re-export all Base methods via using declarations
    using Base::name;
    using Base::find;
    using Base::config;

#if RELAIS_ENABLE_METRICS
    // =======================================================================
    // Metrics — aggregated from all active cache layers
    // =======================================================================

    [[nodiscard]] static cache::MetricsSnapshot metrics() {
        cache::MetricsSnapshot snap{};

        // L1 entity counters
        if constexpr (Cfg.cache_level == config::CacheLevel::L1
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using CachedLayer = LocalRepo<E, Name, Cfg, Key>;
            snap.l1_hits   = CachedLayer::l1_counters_.hits.load();
            snap.l1_misses = CachedLayer::l1_counters_.misses.load();
        }

        // L2 entity counters
        if constexpr (Cfg.cache_level == config::CacheLevel::L2
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using RedisLayer = RedisRepo<E, Name, Cfg, Key>;
            snap.l2_hits   = RedisLayer::l2_counters_.hits.load();
            snap.l2_misses = RedisLayer::l2_counters_.misses.load();
        }

        // List counters
        if constexpr (HasListDescriptor<E>) {
            using CacheLayer = typename detail::CacheLayerSelector<E, Name, Cfg, Key>::type;
            using ListLayer = ListMixin<CacheLayer>;
            snap.list_l1_hits   = ListLayer::list_l1_counters_.hits.load();
            snap.list_l1_misses = ListLayer::list_l1_counters_.misses.load();
            snap.list_l2_hits   = ListLayer::list_l2_counters_.hits.load();
            snap.list_l2_misses = ListLayer::list_l2_counters_.misses.load();
        }

        // Sweep counters (global, shared across all repos)
        auto& sc = cache::GDSFPolicy::instance().sweepCounters();
        snap.sweep_count    = sc.count.load(std::memory_order_relaxed);
        snap.sweep_total_ns = sc.total_ns.load(std::memory_order_relaxed);
        snap.sweep_last_ns  = sc.last_ns.load(std::memory_order_relaxed);
        snap.sweep_max_ns   = sc.max_ns.load(std::memory_order_relaxed);

        return snap;
    }

    static void resetMetrics() {
        if constexpr (Cfg.cache_level == config::CacheLevel::L1
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using CachedLayer = LocalRepo<E, Name, Cfg, Key>;
            CachedLayer::l1_counters_.hits.reset();
            CachedLayer::l1_counters_.misses.reset();
        }

        if constexpr (Cfg.cache_level == config::CacheLevel::L2
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using RedisLayer = RedisRepo<E, Name, Cfg, Key>;
            RedisLayer::l2_counters_.hits.reset();
            RedisLayer::l2_counters_.misses.reset();
        }

        if constexpr (HasListDescriptor<E>) {
            using CacheLayer = typename detail::CacheLayerSelector<E, Name, Cfg, Key>::type;
            using ListLayer = ListMixin<CacheLayer>;
            ListLayer::list_l1_counters_.hits.reset();
            ListLayer::list_l1_counters_.misses.reset();
            ListLayer::list_l2_counters_.hits.reset();
            ListLayer::list_l2_counters_.misses.reset();
        }

        // Sweep counters (global)
        cache::GDSFPolicy::instance().sweepCounters().reset();
    }
#endif

    // =======================================================================
    // Convenience methods (need correct dispatch through method hiding)
    // =======================================================================

    /// Update entity from JSON string.
    /// Parses JSON to create entity, then updates via the full mixin chain.
    static io::Task<bool> updateJson(const Key& id, std::string_view json)
        requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
    {
        auto entity_opt = E::fromJson(json);
        if (!entity_opt) {
            RELAIS_LOG_ERROR << name() << ": updateJson failed to parse JSON";
            co_return false;
        }
        co_return co_await Base::update(id, *entity_opt);
    }

    /// Update entity from binary data.
    /// Creates entity from binary, then updates via the full mixin chain.
    static io::Task<bool> updateBinary(const Key& id, std::span<const uint8_t> buffer)
        requires MutableEntity<E> && HasFullUpdate<E> && HasBinarySerialization<E> && (!Cfg.read_only)
    {
        auto entity_opt = E::fromBinary(buffer);
        if (!entity_opt) {
            RELAIS_LOG_ERROR << name() << ": updateBinary failed to parse binary data";
            co_return false;
        }
        co_return co_await Base::update(id, *entity_opt);
    }

    // =======================================================================
    // Batch erase / invalidate (enumerated keys)
    // =======================================================================
    //
    // Entry is span<const Key> — the "set of keys" convention shared with
    // findMany (zero-copy, caller-owned lifetime). Both dedup the input by
    // equality first (composite/partition Keys model == but not <), then drive
    // the shared invalidateManyImpl cascade resolved at the top of the chain.

    /// Delete the enumerated rows from L3, then evict the deleted set across the
    /// cache hierarchy (L1 + L2 + own lists + deduplicated cross-inval). Returns
    /// the number of rows actually deleted (duplicates and absent ids contribute
    /// nothing) — parité mono erase: nullopt signals a DB error, 0 a non-matching
    /// (but valid) set. eraseManyRaw's RETURNING is the source of the affected
    /// entity set, so no extra read is needed.
    static io::Task<std::optional<size_t>> eraseMany(std::span<const Key> ids)
        requires (!Cfg.read_only)
    {
        if (ids.empty()) co_return std::optional<size_t>{0};
        auto keys = detail::dedupStable<Key>(ids);
        auto deleted = co_await Base::eraseManyRaw(std::span<const Key>(keys));
        if (!deleted) co_return std::nullopt;  // DB error
        co_await Base::template invalidateManyImpl<true>(
            std::span<const E>(*deleted));
        co_return deleted->size();
    }

    /// Evict the enumerated entities from the cache hierarchy without deleting
    /// them from L3 — a later find repopulates from the DB. Materializes each
    /// entity through the cache-first read path (exactly mono invalidate's
    /// `find` + propagate), then runs the cascade with WithLists=false: lists are
    /// left intact because the rows still exist (parité mono invalidate). Best
    /// effort, void return: a missing entity simply contributes no eviction.
    static io::Task<void> invalidateMany(std::span<const Key> ids) {
        if (ids.empty()) co_return;
        auto keys = detail::dedupStable<Key>(ids);
        std::vector<E> entities;
        entities.reserve(keys.size());
        for (const auto& k : keys) {
            auto view = co_await Base::find(k);
            if (view) entities.push_back(*view);
        }
        co_await Base::template invalidateManyImpl<false>(
            std::span<const E>(entities));
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_REPO_H
