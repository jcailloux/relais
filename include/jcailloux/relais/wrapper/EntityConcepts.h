#ifndef JCX_RELAIS_WRAPPER_ENTITY_CONCEPTS_H
#define JCX_RELAIS_WRAPPER_ENTITY_CONCEPTS_H

#include <concepts>
#include <optional>

#include "pqcoro/pg/PgResult.h"
#include "pqcoro/pg/PgParams.h"
#include "jcailloux/relais/wrapper/SerializationTraits.h"

namespace jcailloux::relais {

// =============================================================================
// Entity Wrapper Concepts
//
// Hierarchical concepts for entity wrappers used in repositories.
// Each level adds requirements on top of the previous one.
//
//   Readable          — can be constructed from a PgResult::Row (fromRow)
//   Serializable      — can be cached (toJson/fromJson or toBinary/fromBinary)
//   Writable          — can produce insert params (toInsertParams)
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

/// Can be constructed from a PostgreSQL result row
template<typename W>
concept Readable = requires(const pqcoro::PgResult::Row& row) {
    { W::fromRow(row) } -> std::convertible_to<std::optional<W>>;
};

/// Can be serialized for cache storage (JSON or binary)
template<typename W>
concept Serializable = HasJsonSerialization<W>
                    || HasBinarySerialization<W>;

/// Can produce SQL insert parameters for DB writes
template<typename W>
concept Writable = requires(const W& w) {
    { W::toInsertParams(w) } -> std::convertible_to<pqcoro::PgParams>;
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
template<typename W>
concept ReadableEntity = Readable<W>;

/// Required for RedisRepository / CachedRepository (read + cache)
template<typename W>
concept CacheableEntity = ReadableEntity<W> && Serializable<W>;

/// Required for create() / update() methods (read + DB write)
template<typename W>
concept MutableEntity = ReadableEntity<W> && Writable<W>;

/// Required for create() with cache population (read + DB write + primary key)
template<typename W, typename Key = int64_t>
concept CreatableEntity = MutableEntity<W> && Keyed<W, Key>;

// -----------------------------------------------------------------------------
// ListDescriptor detection
// -----------------------------------------------------------------------------

/// Entity's Mapping has a ListDescriptor (for declarative list caching)
template<typename Entity>
concept HasListDescriptor = requires {
    typename Entity::MappingType::ListDescriptor;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_WRAPPER_ENTITY_CONCEPTS_H
