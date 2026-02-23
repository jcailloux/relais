# Changelog

## [Unreleased]

### Added

- **Lock-free L1 cache** — ParlayHash-backed `ChunkMap` with epoch-based reclamation; `find`/`insert`/`patch` return lightweight `EntityView` instead of `shared_ptr`
- **GDSF eviction** — size-aware L1 eviction via `RELAIS_GDSF_ENABLED` CMake option + runtime `RELAIS_L1_MAX_MEMORY` env var; score = access_count × avg_cost / memoryUsage; histogram-based threshold with three-zone eviction curve; inline decay during cleanup; access count persistence across upserts; zero overhead when disabled
- **Zero-copy RowView serialization** — `findJson()`/`findBinary()`/`queryJson()`/`queryBinary()` serialize directly from PgResult rows, skipping entity construction
- **Configurable L2 format** — `CacheConfig::l2_format`: `Binary` (BEVE, default) or `Json` for non-C++ interop
- **Composite primary keys** — `std::tuple`-based keys from multiple `@relais primary_key` fields with full CRUD and caching support
- **L2 declarative list caching** — Redis-backed list pages with selective Lua-based invalidation
- **Offset pagination** — `ListDescriptorQuery::offset`, mutually exclusive with cursor
- **Deterministic keyset cursor** — null-safe COALESCE sort with PK tiebreaker
- **Adaptive I/O batching** — `BatchScheduler` with Nagle-like strategy: first query immediate, subsequent batched during RTT
- **PostgreSQL pipeline mode** — batched reads via `ANY()` arrays, pipelined writes with sync-point error isolation
- **Redis command pipelining** — `RedisClient::pipelineExec()` queues N commands, single flush/read cycle
- **Write coalescing** — identical concurrent writes share a single DB round-trip; `WriteOutcome::coalesced` flag
- **Multi-worker I/O pool** — `IoPool` with per-worker event loop, connection pools, and batch scheduler; optional core pinning
- **`RedisPool`** — fixed-size connection pool with atomic round-robin dispatch
- **`EpollIoContext`** — production epoll event loop with timers and thread-safe `post()`
- **`TimingEstimator`** — adaptive RTT profiling for batch readiness heuristics (EMA, bootstrap, staleness detection)
- `ConcurrencyGate` coroutine semaphore for shared PG+Redis I/O budget
- `DetachedTask` coroutine type for fire-and-forget async work
- `Task::fromValue(T)` / `Task<void>::ready()` for pre-resolved coroutines

### Changed (Breaking)

- **Return types**: `find`/`insert`/`patch` return `EntityView<Entity>` instead of `shared_ptr<const Entity>`
- **Serialization accessors**: `json()` returns `const std::string*`, `binary()` returns `const std::vector<uint8_t>*`; lazy init via atomic CAS
- **L1 backend**: ShardMap → lock-free ChunkMap; `l1_shard_count_log2` → `l1_chunk_count_log2`
- **Insert/update signatures**: take `const Entity&` instead of `shared_ptr<const Entity>`
- **`DbProvider::execute()`** returns `std::pair<int, bool>` (row count + coalesced flag)
- **`DbProvider::init()`** now requires an `io` context parameter
- **Composite keys**: `key()` returns `std::tuple`, SQL uses multi-column WHERE clauses
- **Partition key concept**: `HasPartitionKey` → `HasPartitionHint`
- **GDSF config**: `RELAIS_L1_MAX_MEMORY` CMake option → `RELAIS_GDSF_ENABLED` (compile-time toggle) + `GDSFConfig::max_memory` / `RELAIS_L1_MAX_MEMORY` env var (runtime budget); `GDSFPolicy::kMaxMemory` → `GDSFPolicy::enabled` + `GDSFPolicy::maxMemory()`
- **GDSF internals**: `GDSFScoreData` stores `atomic<uint32_t> access_count` (4 B) instead of `atomic<float> score` + `atomic<uint32_t> last_generation` (8 B); `GDSFConfig::correction_alpha` → `histogram_alpha`; `RepoRegistryEntry::repo_score_fn` removed
- **List cache keys**: canonical binary buffers replace XXH3 hashes (`query_hash` → `cache_key`)
- `InvalidationData`: `optional<shared_ptr<const T>>` → `shared_ptr<const T>`
- `Keyed`/`CreatableEntity` concepts no longer default `Key` to `int64_t`

### Removed

- `QueryCacheKey.h`, `QueryParser.h`
- `EntityWrapper::releaseCaches()`
- `CacheConfig::l1_cleanup_every_n_gets` / `l1_cleanup_min_interval`
- `shardmap` dependency (replaced by vendored ParlayHash)
- `GDSFPolicy::decay()`, `decayFactor()`, `generation()`, `tick()`, `correction()`, `updateCorrection()`, `pressureFactor()`
- `CachedRepo::repoScore()`, `CachedRepo::postCleanup()`
- `ListCacheConfig::cleanup_every_n_gets`

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
