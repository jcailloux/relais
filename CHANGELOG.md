# Changelog

## [Unreleased]

### Added

- **Lock-free L1 cache** — replaced ShardMap with ParlayHash-backed `ChunkMap` and epoch-based reclamation; all `find`/`insert`/`patch` return lightweight `EntityView` (12 bytes) instead of `shared_ptr`
- **Epoch-guarded views** — `EntityView<Entity>`, `JsonView`, `BinaryView` hold an `EpochGuard` ticket preventing reclamation while alive; thread-agnostic and safe across `co_await`
- **`Task::fromValue(T)` / `Task<void>::ready()`** — pre-resolved coroutine tasks that skip heap allocation (`await_ready() = true`)
- **GDSF eviction policy** — L1 eviction via score = frequency x cost, controlled by `RELAIS_L1_MAX_MEMORY` CMake option (0 = disabled, default); quadratic pressure factor, 4-variant `CacheMetadata` (0/8/8/16 bytes), zero overhead when disabled via `if constexpr`
- **`RELAIS_CLEANUP_FREQUENCY_LOG2`** CMake option — compile-time L1 sweep frequency (default 9 = every 512 insertions)
- **Zero-copy RowView serialization** — `findJson()`/`findBinary()`/`queryJson()`/`queryBinary()` serialize directly from PgResult rows when no L1 cache is in the chain
- **Configurable L2 serialization format** (`CacheConfig::l2_format`) — `Binary` (BEVE, default) or `Json` for non-C++ interop; `findJson()` transcodes BEVE directly via `glz::beve_to_json`
- **Composite primary keys** — `std::tuple`-based keys from multiple `@relais primary_key` fields, with full CRUD, L1/L2 caching, and Redis support
- **L2 (Redis) declarative list caching** — list pages stored as BEVE binary in Redis with bounds-based and selective Lua-based invalidation
- **Offset pagination** — `ListDescriptorQuery::offset` for offset+limit, mutually exclusive with cursor
- **Deterministic keyset pagination** — `COALESCE` null-safe sort + secondary PK sort for stable cursor ordering
- `DetachedTask` coroutine type for fire-and-forget async work

### Changed (Breaking)

- **Return types**: `find`/`insert`/`patch` return `EntityView<Entity>` instead of `shared_ptr<const Entity>`; `findJson` returns `JsonView`; `findBinary` returns `BinaryView`
- **Serialization accessors**: `json()` returns `const std::string*`, `binary()` returns `const std::vector<uint8_t>*` (was `shared_ptr`); lazy init via atomic CAS instead of `std::call_once`
- **L1 backend**: ShardMap replaced by lock-free ChunkMap (ParlayHash + epoch-based reclamation); `l1_shard_count_log2` → `l1_chunk_count_log2`
- **GDSF sweep**: `emergencyCleanup()` → `sweep()` with `atomic_flag`; striped memory slots now use per-cache-line alignment to eliminate false sharing
- **Dependency**: `shardmap` replaced by `parlayhash`
- **Composite key entity generator output**: `key()` returns `std::tuple`, SQL uses multi-column WHERE clauses
- **Partition key concept renamed**: `HasPartitionKey` → `HasPartitionHint`
- **List cache keys**: canonical binary buffers replace XXH3 hashes — `ListQuery::query_hash` → `cache_key`
- `InvalidationData`: `optional<shared_ptr<const T>>` → `shared_ptr<const T>`
- `Keyed` and `CreatableEntity` concepts no longer default `Key` to `int64_t`

### Removed

- `QueryCacheKey.h` — replaced by canonical binary buffer approach
- `QueryParser.h` — parsing moved to `ParseUtils.h` and `HttpQueryParser`
- `EntityWrapper::releaseCaches()` — no longer needed with atomic pointer caches
- `CacheConfig::l1_cleanup_every_n_gets` / `l1_cleanup_min_interval` — replaced by `RELAIS_CLEANUP_FREQUENCY_LOG2`
- `shardmap` dependency

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
