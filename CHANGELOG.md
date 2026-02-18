# Changelog

## [Unreleased]

### Changed (Breaking)

- **Cache key identity**: list cache keys are now canonical binary buffers instead of XXH3 hashes — eliminates collision risk and enables direct use as Redis keys
  - `ListQuery::query_hash` (size_t) → `ListQuery::cache_key` (std::string)
  - `ListQuery::hash()` → `ListQuery::cacheKey()`
  - `ListDescriptorQuery` gains `group_key` (filters+sort) and `cache_key` (full page key)
  - L1 list cache `CacheKey` type changed from `size_t` to `std::string`
  - InvalidationData: `optional<shared_ptr<const T>>` → `shared_ptr<const T>` — affects custom invalidation handlers that access `old_entity`/`new_entity`
  - Master group tracking: `{name}:dlist_groups` changed from Redis SET to HASH (stores sort field index per group)
- xxhash dependency removed from list query pipeline

### Added

- **L2 (Redis) declarative list caching** in ListMixin — pages stored as BEVE binary with bounds header
  - `invalidateAllListGroups()` — manually invalidate all L2 list groups
  - Redis key scheme: `{name}:dlist:p:{cache_key}` (pages), `{name}:dlist:g:{group_key}` (groups), `{name}:dlist_groups` (master hash)
- **Selective L2 list eviction** — mutations now invalidate only matching groups (filter-match) and affected pages (SortBounds check) in a single Redis round-trip instead of flushing all groups
- **Offset pagination** — `ListDescriptorQuery::offset` for traditional offset+limit pagination, mutually exclusive with cursor; `parseListQueryStrict` rejects conflicting cursor + offset (`ConflictingPagination` error)
- **Deterministic keyset pagination** — `COALESCE` null-safe sort + secondary sort on primary key for stable cursor ordering
- `DetachedTask` coroutine type — eager fire-and-forget for async work that doesn't need to be awaited
- `ParseUtils.h` — lightweight parsing utilities extracted from the removed `QueryParser.h`
- Entity generator now sorts filters alphabetically for deterministic cache keys

### Removed

- `QueryCacheKey.h` — replaced by canonical binary buffer approach (no more `HashBuffer`, `QueryCacheKey<Filters>`, `HashableFilters` concept)
- `QueryParser.h` — parsing utilities moved to `ParseUtils.h`, query-level parsing handled by `HttpQueryParser`

## [0.4.0] - 2026-02-17

### Added

- `column=` annotation in entity generator: maps C++ field names to different PostgreSQL column names (e.g. `// @relais column=product_name`), falls back to the C++ name when omitted

### Fixed

- `init_test_db.sh` migration directory path resolution

## [0.3.1] - 2026-02-17

### Fixed
- `patch` now uses explicit `RETURNING` column list instead of `RETURNING *`, ensuring consistent column ordering with `fromRow` index-based mapping

## [0.3.0] - 2026-02-17

### Added

- `sweep()` on CachedRepo and ListCache — single-shard cleanup that waits for the lock
- Symmetric sweep API on ListMixin:
    - Unified: `trySweep()`, `sweep()`, `purge()`
    - Entity only: `trySweepEntities()`, `sweepEntities()`, `purgeEntities()`
    - List only: `trySweepLists()`, `sweepLists()`, `purgeLists()`
- Compile-time test suites: `test_relais_base_compile`, `test_relais_repo_compile`
- `findBinary(id)` on RedisRepo and CachedRepo — returns raw BEVE bytes directly from Redis without deserialization

### Changed (Breaking)

All public API names have been shortened and aligned with SQL/STL/cache conventions.

**Class & file renames:**
- `Repository` → `Repo`, `BaseRepository` → `BaseRepo`, `RedisRepository` → `RedisRepo`
- `CachedRepository` → `CachedRepo`, `ListCacheRepository` → `ListCacheRepo`
- `repository_config.h` → `repo_config.h`

**CRUD methods:**
- `findById` → `find`
- `create` → `insert`
- `updateBy` → `patch` (partial update)
- `remove` → `erase`
- `updateFromJson` → `updateJson`, `updateFromBinary` → `updateBinary`
- `findByIdAsJson` → `findJson`

**Serialization accessors (EntityWrapper, ListWrapper, ListQuery):**
- `toJson()` → `json()`, `toBinary()` → `binary()`
- `getPrimaryKey()` → `key()`

**ListWrapper accessors:**
- `firstItem` → `front`, `lastItem` → `back`
- `totalCount` → `count`, `nextCursor` → `cursor`

**Cache management:**
- `invalidateL1` → `evict`, `invalidateRedis` → `evictRedis`
- `triggerCleanup` → `trySweep`, `fullCleanup` → `purge`
- `cacheSize` → `size`, `listCacheSize` → `listSize`

**Context variants (ListMixin, InvalidationMixin):**
- `removeWithContext` → `eraseWithContext`
- `updateByWithContext` → `patchWithContext`

### Removed

- `PartialKeyValidator` class (obsolete)

### Fixed

- `findJson` on RedisRepo was reading binary data as a JSON string when Redis stores BEVE

## [0.2.0] - 2026-02-16

### Added

- CMake `install()` rules with `find_package(jcailloux-relais)` support
- Package config with `find_dependency` for transitive dependencies
- `FIND_PACKAGE_ARGS` with minimum versions for shardmap (1.0) and glaze (7.0)

## [0.1.0] - 2025-02-16

Initial release.

### Features

- Header-only C++23 repository pattern with compile-time mixin composition
- Multi-tier caching: L1 (RAM via [shardmap](https://github.com/jcailloux/shardmap)), L2 (Redis), L3 (PostgreSQL)
- Cache presets: Uncached, Local, Redis, Both
- Cross-invalidation mechanisms
- List caching with declarative filters and sorts
- Async I/O layer (PostgreSQL + Redis)
- Entity generator: `relais_generate_wrappers()` CMake function
- Partition key support for PostgreSQL partitioned tables
