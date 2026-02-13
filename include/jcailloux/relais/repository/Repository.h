#ifndef JCX_DROGON_REPOSITORY_H
#define JCX_DROGON_REPOSITORY_H

#include <span>
#include <type_traits>
#include "jcailloux/relais/repository/CachedRepository.h"
#include "jcailloux/relais/repository/InvalidationMixin.h"
#include "jcailloux/relais/repository/ListMixin.h"
#include "jcailloux/relais/config/FixedString.h"
#include "jcailloux/relais/wrapper/EntityConcepts.h"

namespace jcailloux::relais {

// =============================================================================
// RepositoryBuilder — assembles the mixin chain from template parameters
// =============================================================================
//
// Chain (bottom to top):
//   BaseRepository
//     ↑ (if L2 or L1_L2)
//   RedisRepository
//     ↑ (if L1 or L1_L2)
//   CachedRepository
//     ↑ (if Entity has ListDescriptor)
//   ListMixin
//     ↑ (if Invalidations... non-empty)
//   InvalidationMixin
//
// The final Repository class sits on top and adds convenience methods.
//

namespace detail {

/// Select the cache layer based on CacheConfig::cache_level
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
struct CacheLayerSelector {
    using type = std::conditional_t<
        Cfg.cache_level == config::CacheLevel::L1 || Cfg.cache_level == config::CacheLevel::L1_L2,
        CachedRepository<Entity, Name, Cfg, Key>,
        std::conditional_t<
            Cfg.cache_level == config::CacheLevel::L2,
            RedisRepository<Entity, Name, Cfg, Key>,
            BaseRepository<Entity, Name, Cfg, Key>
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
// Repository — final class with convenience methods
// =============================================================================
//
// Usage:
//   using MyRepo = Repository<MyWrapper, "MyEntity">;                     // L1 (default)
//   using MyRepo = Repository<MyWrapper, "MyEntity", config::Both>;       // L1+L2
//   using MyRepo = Repository<MyWrapper, "MyEntity", config::Local,
//       Invalidate<OtherRepo, &MyStruct::other_id>>;                      // with cross-inv
//

template<typename Entity, config::FixedString Name, config::CacheConfig Cfg = config::Local,
         typename... Invalidations>
class Repository
    : public detail::MixinStack<
          Entity, Name, Cfg,
          decltype(std::declval<const Entity>().getPrimaryKey()),
          Invalidations...
      >::type
{
    using Key = decltype(std::declval<const Entity>().getPrimaryKey());
    using Model = typename Entity::Model;
    using Base = typename detail::MixinStack<Entity, Name, Cfg, Key, Invalidations...>::type;

    // Compile-time validation
    static_assert(ReadableEntity<Entity, Model>,
        "Entity must satisfy ReadableEntity (provide fromModel)");

    static_assert(
        Cfg.cache_level == config::CacheLevel::None ||
        CacheableEntity<Entity, Model>,
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
    using typename Base::ModelType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::WrapperPtrType;

    // Re-export all Base methods via using declarations
    using Base::name;
    using Base::findById;
    using Base::config;

    // =======================================================================
    // Convenience methods (need correct dispatch through method hiding)
    // =======================================================================

    /// Update entity from JSON string.
    /// Parses JSON to create wrapper, then updates via the full mixin chain.
    static ::drogon::Task<bool> updateFromJson(const Key& id, std::string_view json)
        requires MutableEntity<Entity, Model> && (!Cfg.read_only)
    {
        auto wrapper_opt = Entity::fromJson(json);
        if (!wrapper_opt) {
            LOG_ERROR << name() << ": updateFromJson failed to parse JSON";
            co_return false;
        }
        auto wrapper = std::make_shared<const Entity>(std::move(*wrapper_opt));
        co_return co_await Base::update(id, std::move(wrapper));
    }

    /// Update entity from binary data.
    /// Creates wrapper from binary, then updates via the full mixin chain.
    static ::drogon::Task<bool> updateFromBinary(const Key& id, std::span<const uint8_t> buffer)
        requires MutableEntity<Entity, Model> && HasBinarySerialization<Entity> && (!Cfg.read_only)
    {
        auto wrapper_opt = Entity::fromBinary(buffer);
        if (!wrapper_opt) {
            LOG_ERROR << name() << ": updateFromBinary failed to parse binary data";
            co_return false;
        }
        auto wrapper = std::make_shared<const Entity>(std::move(*wrapper_opt));
        co_return co_await Base::update(id, std::move(wrapper));
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_DROGON_REPOSITORY_H
