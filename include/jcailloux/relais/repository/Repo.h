#ifndef JCX_RELAIS_REPO_H
#define JCX_RELAIS_REPO_H

#include <span>
#include <type_traits>
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/repository/CachedRepo.h"
#include "jcailloux/relais/repository/InvalidationMixin.h"
#include "jcailloux/relais/repository/ListMixin.h"
#include "jcailloux/relais/config/FixedString.h"
#include "jcailloux/relais/wrapper/EntityConcepts.h"
#include "jcailloux/relais/cache/Metrics.h"

namespace jcailloux::relais {

// =============================================================================
// RepoBuilder — assembles the mixin chain from template parameters
// =============================================================================
//
// Chain (bottom to top):
//   BaseRepos
//     ↑ (if L2 or L1_L2)
//   RedisRepo
//     ↑ (if L1 or L1_L2)
//   CachedRepo
//     ↑ (if Entity has ListDescriptor)
//   ListMixin
//     ↑ (if Invalidations... non-empty)
//   InvalidationMixin
//
// The final Repo class sits on top and adds convenience methods.
//

namespace detail {

/// Select the cache layer based on CacheConfig::cache_level
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
struct CacheLayerSelector {
    using type = std::conditional_t<
        Cfg.cache_level == config::CacheLevel::L1 || Cfg.cache_level == config::CacheLevel::L1_L2,
        CachedRepo<Entity, Name, Cfg, Key>,
        std::conditional_t<
            Cfg.cache_level == config::CacheLevel::L2,
            RedisRepo<Entity, Name, Cfg, Key>,
            BaseRepo<Entity, Name, Cfg, Key>
        >
    >;
};

/// Stack optional mixins on top of the cache layer
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key,
         typename... Invalidations>
struct MixinStack {
    using CacheLayer = typename CacheLayerSelector<Entity, Name, Cfg, Key>::type;

    using WithList = std::conditional_t<
        HasListDescriptor<Entity>,
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
//   using MyRepo = Repo<MyWrapper, "MyEntity">;                     // L1 (default)
//   using MyRepo = Repo<MyWrapper, "MyEntity", config::Both>;       // L1+L2
//   using MyRepo = Repo<MyWrapper, "MyEntity", config::Local,
//       Invalidate<OtherRepo, &MyStruct::other_id>>;                      // with cross-inv
//

template<typename Entity, config::FixedString Name, config::CacheConfig Cfg = config::Local,
         typename... Invalidations>
class Repo
    : public detail::MixinStack<
          Entity, Name, Cfg,
          decltype(std::declval<const Entity>().key()),
          Invalidations...
      >::type
{
    using Key = decltype(std::declval<const Entity>().key());
    using Base = typename detail::MixinStack<Entity, Name, Cfg, Key, Invalidations...>::type;

    // Compile-time validation
    static_assert(ReadableEntity<Entity>,
        "Entity must satisfy ReadableEntity (provide fromRow)");

    static_assert(
        Cfg.cache_level == config::CacheLevel::None ||
        CacheableEntity<Entity>,
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
            using CachedLayer = CachedRepo<Entity, Name, Cfg, Key>;
            snap.l1_hits   = CachedLayer::l1_counters_.hits.load();
            snap.l1_misses = CachedLayer::l1_counters_.misses.load();
        }

        // L2 entity counters
        if constexpr (Cfg.cache_level == config::CacheLevel::L2
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using RedisLayer = RedisRepo<Entity, Name, Cfg, Key>;
            snap.l2_hits   = RedisLayer::l2_counters_.hits.load();
            snap.l2_misses = RedisLayer::l2_counters_.misses.load();
        }

        // List counters
        if constexpr (HasListDescriptor<Entity>) {
            using CacheLayer = typename detail::CacheLayerSelector<Entity, Name, Cfg, Key>::type;
            using ListLayer = ListMixin<CacheLayer>;
            snap.list_l1_hits   = ListLayer::list_l1_counters_.hits.load();
            snap.list_l1_misses = ListLayer::list_l1_counters_.misses.load();
            snap.list_l2_hits   = ListLayer::list_l2_counters_.hits.load();
            snap.list_l2_misses = ListLayer::list_l2_counters_.misses.load();
        }

        return snap;
    }

    static void resetMetrics() {
        if constexpr (Cfg.cache_level == config::CacheLevel::L1
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using CachedLayer = CachedRepo<Entity, Name, Cfg, Key>;
            CachedLayer::l1_counters_.hits.reset();
            CachedLayer::l1_counters_.misses.reset();
        }

        if constexpr (Cfg.cache_level == config::CacheLevel::L2
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using RedisLayer = RedisRepo<Entity, Name, Cfg, Key>;
            RedisLayer::l2_counters_.hits.reset();
            RedisLayer::l2_counters_.misses.reset();
        }

        if constexpr (HasListDescriptor<Entity>) {
            using CacheLayer = typename detail::CacheLayerSelector<Entity, Name, Cfg, Key>::type;
            using ListLayer = ListMixin<CacheLayer>;
            ListLayer::list_l1_counters_.hits.reset();
            ListLayer::list_l1_counters_.misses.reset();
            ListLayer::list_l2_counters_.hits.reset();
            ListLayer::list_l2_counters_.misses.reset();
        }
    }
#endif

    // =======================================================================
    // Convenience methods (need correct dispatch through method hiding)
    // =======================================================================

    /// Update entity from JSON string.
    /// Parses JSON to create entity, then updates via the full mixin chain.
    static io::Task<bool> updateJson(const Key& id, std::string_view json)
        requires MutableEntity<Entity> && (!Cfg.read_only)
    {
        auto entity_opt = Entity::fromJson(json);
        if (!entity_opt) {
            RELAIS_LOG_ERROR << name() << ": updateJson failed to parse JSON";
            co_return false;
        }
        co_return co_await Base::update(id, *entity_opt);
    }

    /// Update entity from binary data.
    /// Creates entity from binary, then updates via the full mixin chain.
    static io::Task<bool> updateBinary(const Key& id, std::span<const uint8_t> buffer)
        requires MutableEntity<Entity> && HasBinarySerialization<Entity> && (!Cfg.read_only)
    {
        auto entity_opt = Entity::fromBinary(buffer);
        if (!entity_opt) {
            RELAIS_LOG_ERROR << name() << ": updateBinary failed to parse binary data";
            co_return false;
        }
        co_return co_await Base::update(id, *entity_opt);
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_REPO_H
