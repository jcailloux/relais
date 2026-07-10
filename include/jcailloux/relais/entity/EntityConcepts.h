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

// -----------------------------------------------------------------------------
// FilterSet detection
// -----------------------------------------------------------------------------

/// E's Mapping declares a FilterSet (predicate spec — filters tuple + named
/// Values aggregate). Drives the where-variants (eraseWhere/invalidateWhere).
/// Decoupled from HasListDescriptor: an entity may declare filters WITHOUT a
/// cached list and still satisfy this (étape 0b decorrelation); conversely a
/// list entity's ListDescriptor inherits its FilterSet, so list entities
/// satisfy both.
template<typename E>
concept HasFilterSet = requires {
    typename E::MappingType::FilterSet;
    typename E::MappingType::FilterSet::Values;
};

/// User-facing predicate aggregate for the where-variants — a struct of named
/// optionals (one per filter), built with designated initializers:
///   repo.eraseWhere({.author_id = 42, .category = "tech"});
/// Named by HTTP param, robust to filter reordering (unlike a positional tuple).
template<typename E>
    requires HasFilterSet<E>
using FilterSet = typename E::MappingType::FilterSet::Values;

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

/// E's Mapping emitted a native upsert (INSERT ... ON CONFLICT DO UPDATE). The
/// generator emits SQL::upsert only for writable, caller-assigned-PK entities
/// with at least one non-PK column to SET (see the generator's _supports_upsert):
/// serial-PK, all-PK junctions and read-only views have no such member. Gates
/// every upsert path so the method is cleanly absent (SFINAE) rather than
/// referencing a suppressed SQL::upsert at the call site.
template<typename E>
concept HasUpsertSql = requires {
    { E::MappingType::SQL::upsert } -> std::convertible_to<const char*>;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_ENTITY_CONCEPTS_H
