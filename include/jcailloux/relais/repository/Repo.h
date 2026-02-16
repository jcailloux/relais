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
        (Cfg.l1_shard_count_log2 >= 1 && Cfg.l1_ttl.ns > 0),
        "L1 cache requires l1_shard_count_log2 >= 1 and l1_ttl > 0");

    static_assert(
        Cfg.cache_level != config::CacheLevel::L2 &&
        Cfg.cache_level != config::CacheLevel::L1_L2 ||
        Cfg.l2_ttl.ns > 0,
        "L2 cache requires l2_ttl > 0");

public:
    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::WrapperPtrType;

    // Re-export all Base methods via using declarations
    using Base::name;
    using Base::find;
    using Base::config;

    // =======================================================================
    // Convenience methods (need correct dispatch through method hiding)
    // =======================================================================

    /// Update entity from JSON string.
    /// Parses JSON to create wrapper, then updates via the full mixin chain.
    static io::Task<bool> updateFromJson(const Key& id, std::string_view json)
        requires MutableEntity<Entity> && (!Cfg.read_only)
    {
        auto wrapper_opt = Entity::fromJson(json);
        if (!wrapper_opt) {
            RELAIS_LOG_ERROR << name() << ": updateFromJson failed to parse JSON";
            co_return false;
        }
        auto wrapper = std::make_shared<const Entity>(std::move(*wrapper_opt));
        co_return co_await Base::update(id, std::move(wrapper));
    }

    /// Update entity from binary data.
    /// Creates wrapper from binary, then updates via the full mixin chain.
    static io::Task<bool> updateFromBinary(const Key& id, std::span<const uint8_t> buffer)
        requires MutableEntity<Entity> && HasBinarySerialization<Entity> && (!Cfg.read_only)
    {
        auto wrapper_opt = Entity::fromBinary(buffer);
        if (!wrapper_opt) {
            RELAIS_LOG_ERROR << name() << ": updateFromBinary failed to parse binary data";
            co_return false;
        }
        auto wrapper = std::make_shared<const Entity>(std::move(*wrapper_opt));
        co_return co_await Base::update(id, std::move(wrapper));
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_REPO_H
