#ifndef JCX_DROGON_WRAPPER_ENTITY_CONCEPTS_H
#define JCX_DROGON_WRAPPER_ENTITY_CONCEPTS_H

#include <concepts>
#include <optional>

#include "jcailloux/relais/wrapper/SerializationTraits.h"

namespace jcailloux::relais {

// =============================================================================
// Entity Wrapper Concepts
//
// Hierarchical concepts for entity wrappers used in repositories.
// Each level adds requirements on top of the previous one.
//
//   Readable          — can be constructed from a DB model (fromModel)
//   Serializable      — can be cached (toJson/fromJson or toBinary/fromBinary)
//   Writable          — can write back to DB (toModel)
//   Keyed             — has a primary key (getPrimaryKey)
//
// Composed concepts for repository constraints:
//
//   ReadableEntity    = Readable               (BaseRepository)
//   CacheableEntity   = Readable + Serializable (RedisRepository, CachedRepository)
//   MutableEntity     = Readable + Writable     (create/update methods)
//   CreatableEntity   = Mutable  + Keyed        (create with cache population)
// =============================================================================

// -----------------------------------------------------------------------------
// Building blocks
// -----------------------------------------------------------------------------

/// Can be constructed from a Drogon ORM model
template<typename W, typename Model>
concept Readable = requires(const Model& m) {
    { W::fromModel(m) } -> std::convertible_to<std::optional<W>>;
};

/// Can be serialized for cache storage (JSON or binary)
template<typename W>
concept Serializable = HasJsonSerialization<W>
                    || HasBinarySerialization<W>;

/// Can be converted back to a Drogon ORM model for DB writes
template<typename W, typename Model>
concept Writable = requires(const W& w) {
    { W::toModel(w) } -> std::convertible_to<Model>;
};

/// Has a primary key for cache key generation
template<typename W, typename Key = int64_t>
concept Keyed = requires(const W& w) {
    { w.getPrimaryKey() } -> std::convertible_to<Key>;
};

// -----------------------------------------------------------------------------
// Composed concepts for repository constraints
// -----------------------------------------------------------------------------

/// Minimum requirement for BaseRepository (DB-only read)
template<typename W, typename Model>
concept ReadableEntity = Readable<W, Model>;

/// Required for RedisRepository / CachedRepository (read + cache)
template<typename W, typename Model>
concept CacheableEntity = ReadableEntity<W, Model> && Serializable<W>;

/// Required for create() / update() methods (read + DB write)
template<typename W, typename Model>
concept MutableEntity = ReadableEntity<W, Model> && Writable<W, Model>;

/// Required for create() with cache population (read + DB write + primary key)
template<typename W, typename Model, typename Key = int64_t>
concept CreatableEntity = MutableEntity<W, Model> && Keyed<W, Key>;

// -----------------------------------------------------------------------------
// Partial key detection
// -----------------------------------------------------------------------------

/// Entity provides makeKeyCriteria for partitioned tables
template<typename E, typename Model, typename Key>
concept HasPartialKey = requires(const Key& k) {
    { E::template makeKeyCriteria<Model>(k) };
};

// -----------------------------------------------------------------------------
// ListDescriptor detection
// -----------------------------------------------------------------------------

/// Entity's Mapping has a ListDescriptor (for declarative list caching)
template<typename Entity>
concept HasListDescriptor = requires {
    typename Entity::MappingType::ListDescriptor;
};

}  // namespace jcailloux::relais

#endif  // JCX_DROGON_WRAPPER_ENTITY_CONCEPTS_H