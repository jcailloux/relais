#ifndef JCX_RELAIS_WRAPPER_ENTITY_CONCEPTS_H
#define JCX_RELAIS_WRAPPER_ENTITY_CONCEPTS_H

#include <concepts>
#include <optional>

#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/wrapper/SerializationTraits.h"

namespace jcailloux::relais {

// =============================================================================
// Entity Wrapper Concepts
//
// Hierarchical concepts for entity wrappers used in repositories.
// Each level adds requirements on top of the previous one.
//
//   Readable          — can be constructed from a PgResult::Row (fromRow)
//   Serializable      — can be cached (json/fromJson or binary/fromBinary)
//   Writable          — can produce insert params (toInsertParams)
//   Keyed             — has a primary key (key)
//
// Composed concepts for repository constraints:
//
//   ReadableEntity    = Readable               (BaseRepo)
//   CacheableEntity   = Readable + Serializable (RedisRepo, CachedRepo)
//   MutableEntity     = Readable + Writable     (insert/update methods)
//   CreatableEntity   = Mutable  + Keyed        (insert with cache population)
// =============================================================================

// -----------------------------------------------------------------------------
// Building blocks
// -----------------------------------------------------------------------------

/// Can be constructed from a PostgreSQL result row
template<typename W>
concept Readable = requires(const io::PgResult::Row& row) {
    { W::fromRow(row) } -> std::convertible_to<std::optional<W>>;
};

/// Can be serialized for cache storage (JSON or binary)
template<typename W>
concept Serializable = HasJsonSerialization<W>
                    || HasBinarySerialization<W>;

/// Can produce SQL insert parameters for DB writes
template<typename W>
concept Writable = requires(const W& w) {
    { W::toInsertParams(w) } -> std::convertible_to<io::PgParams>;
};

/// Has a primary key for cache key generation
template<typename W, typename Key = int64_t>
concept Keyed = requires(const W& w) {
    { w.key() } -> std::convertible_to<Key>;
};

// -----------------------------------------------------------------------------
// Composed concepts for repository constraints
// -----------------------------------------------------------------------------

/// Minimum requirement for BaseRepo (DB-only read)
template<typename W>
concept ReadableEntity = Readable<W>;

/// Required for RedisRepo / CachedRepo (read + cache)
template<typename W>
concept CacheableEntity = ReadableEntity<W> && Serializable<W>;

/// Required for insert() / update() methods (read + DB write)
template<typename W>
concept MutableEntity = ReadableEntity<W> && Writable<W>;

/// Required for insert() with cache population (read + DB write + primary key)
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

/// Entity's Mapping has partition key support (partition-pruned DELETE).
/// Auto-detected from Mapping providing delete_by_full_pk SQL and
/// makeFullKeyParams method (generated when @relais partition_key is used).
/// Distinct from a future HasCompositeKey where ALL key parts are required
/// for identification — here, the cache key alone suffices but the partition
/// column enables single-partition pruning when available from cache.
template<typename Entity>
concept HasPartitionKey = requires(const Entity& e) {
    { Entity::MappingType::SQL::delete_by_full_pk } -> std::convertible_to<const char*>;
    { Entity::MappingType::makeFullKeyParams(e) } -> std::convertible_to<io::PgParams>;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_WRAPPER_ENTITY_CONCEPTS_H
