#ifndef JCX_RELAIS_ENTITY_CONCEPTS_H
#define JCX_RELAIS_ENTITY_CONCEPTS_H

#include <concepts>
#include <optional>

#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/entity/SerializationTraits.h"

namespace jcailloux::relais {

// =============================================================================
// Entity Concepts
//
// Hierarchical concepts for entity types used in repositories.
// Each level adds requirements on top of the previous one.
//
//   Readable          — can be constructed from a PgResult::Row (fromRow)
//   Serializable      — can be cached (json/fromJson or binary/fromBinary)
//   Writable          — can produce insert params (toInsertParams)
//   Keyed             — has a primary key (key)
//
// Composed concepts for repository constraints:
//
//   ReadableEntity    = Readable               (PgRepo)
//   CacheableEntity   = Readable + Serializable (RedisRepo, LocalRepo)
//   MutableEntity     = Readable + Writable     (insert/update methods)
//   CreatableEntity   = Mutable  + Keyed        (insert with cache population)
// =============================================================================

// -----------------------------------------------------------------------------
// Building blocks
// -----------------------------------------------------------------------------

/// Can be constructed from a PostgreSQL result row
template<typename E>
concept Readable = requires(const io::PgResult::Row& row) {
    { E::fromRow(row) } -> std::convertible_to<std::optional<E>>;
};

/// Can be serialized for cache storage (JSON or binary)
template<typename E>
concept Serializable = HasJsonSerialization<E>
                    || HasBinarySerialization<E>;

/// Can produce SQL insert parameters for DB writes
template<typename E>
concept Writable = requires(const E& e) {
    { E::toInsertParams(e) } -> std::convertible_to<io::PgParams>;
};

/// Has a primary key for cache key generation
template<typename E, typename Key>
concept Keyed = requires(const E& e) {
    { e.key() } -> std::convertible_to<Key>;
};

// -----------------------------------------------------------------------------
// Composed concepts for repository constraints
// -----------------------------------------------------------------------------

/// Minimum requirement for PgRepo (DB-only read)
template<typename E>
concept ReadableEntity = Readable<E>;

/// Required for RedisRepo / LocalRepo (read + cache)
template<typename E>
concept CacheableEntity = ReadableEntity<E> && Serializable<E>;

/// Required for insert() / update() methods (read + DB write)
template<typename E>
concept MutableEntity = ReadableEntity<E> && Writable<E>;

/// Required for insert() with cache population (read + DB write + primary key)
template<typename E, typename Key>
concept CreatableEntity = MutableEntity<E> && Keyed<E, Key>;

// -----------------------------------------------------------------------------
// ListDescriptor detection
// -----------------------------------------------------------------------------

/// E's Mapping has a ListDescriptor (for declarative list caching)
template<typename E>
concept HasListDescriptor = requires {
    typename E::MappingType::ListDescriptor;
};

/// E's Mapping has partition hint support (partition-pruned DELETE).
/// Auto-detected from Mapping providing delete_with_partition SQL and
/// makePartitionHintParams method (generated when @relais partition_key is used).
/// Distinct from composite keys where ALL key parts are required for
/// identification — here, the cache key alone suffices but the partition
/// column enables single-partition pruning when available from cache.
template<typename E>
concept HasPartitionHint = requires(const E& e) {
    { E::MappingType::SQL::delete_with_partition } -> std::convertible_to<const char*>;
    { E::MappingType::makePartitionHintParams(e) } -> std::convertible_to<io::PgParams>;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_ENTITY_CONCEPTS_H
