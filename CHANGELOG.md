# Changelog

## [Unreleased]

### Fixed

- **Silent corruption on `update()` of a simple, caller-assigned primary key.** `updateOutcome` built UPDATE params from `toInsertParams` for non-tuple keys, which leads with the PK column, shifting every `SET` value by one slot against the generated `SET <non-pk>=$2.. WHERE <pk>=$1` layout. The first row written looked correct; later updates either failed silently or wrote the PK value into the adjacent column. Simple keys now use `toUpdateParams` unconditionally (same path as composite keys). `db_managed` PKs (e.g. `BIGSERIAL` ids) were unaffected — their PK is excluded from insert params too, so the two param sets coincided, which is why the suite never caught it. Regression coverage added via a non-`db_managed` single-key fixture (`tests/relais/test_simple_assigned_key.cpp`)
- **Generator dropped a field whose declaration carried a `(` in its trailing comment.** `_parse_members` excluded methods by testing the whole line for `(`; a field like `int64_t x = 0; // ... (idle)` was silently removed from the mapping (absent from INSERT, `fromRow`, …). The method/parenthesis test now runs on the code portion only (line truncated before `//` and `/*`); `@relais` tags are still read from the full line

## [0.5.0-alpha.2] - 2026-06-09

**Alpha release — API may change. Not recommended for production.**

### Added

- **Shared-nothing N-loop scaling** — run one event loop per core, each with its own pools; a request stays on its loop end to end. Throughput scales ~linearly with cores at unchanged per-request latency (single-loop is N=1). See `docs/runtime-and-threading.md`
- **`spawnOn`** (`runtime/Spawn.h`) — drive a lazy `Task` to completion on an event-loop thread from another thread (`Outcome<T>` result). Used to bootstrap pools from a startup thread
- **Run relais on a foreign event loop** — `IoContext` conformance harness (`testing/IoContextConformance.h`) + authoring guide (`docs/io-context-adapters.md`) to write and verify an adapter (e.g. Drogon/trantor), so relais co-locates on your framework's loops
- **Runtime & threading guide** (`docs/runtime-and-threading.md`), a Quick Start runtime section, and runnable `examples/` (CI-compiled: `event_loop_basics`, `iopool_nloop`)

### Changed (Breaking)

- **`PgProvider::init` must be called on the event-loop thread it serves** (providers are now `thread_local`, was process-global). Single-loop: call it on the loop thread; N-loop: once per loop. Debug builds assert this for adapters exposing `isInLoopThread()`
- **Custom `IoContext` implementations must add `postDelayed`/`cancelTimer`/`TimerToken`** — the concept now requires timer support (used by `BatchScheduler` for adaptive batch flushing). The bundled `EpollIoContext` already satisfies it

### Fixed

- **`IoPool` is now functional.** Announced in 0.5.0-alpha.1, it never compiled (missing `<condition_variable>`/`<mutex>` includes) and had a startup lost-wakeup; both fixed, and `BatchScheduler` no longer relied on an unsound `static_cast<EpollIoContext&>` (undefined behavior on any non-Epoll loop). Covered by `test_io_pool_integration`
- `PgProvider::init`: passing an explicit `nullptr` Redis argument no longer breaks template deduction

### Documentation

- README is now a concise hub pointing to topical guides under `docs/`; the generator/caching/invalidation/list manuals moved out of the monolithic README
- Clarified the generator's `entity::generated` namespace, the FetchContent `CMAKE_MODULE_PATH` step, the `glz::meta` ODR rule, and the relative-include path constraint
- Documented L1 bounds: `l1_ttl` is a staleness ceiling, GDSF only evicts when `RELAIS_L1_MAX_MEMORY` is set — `with_l1_ttl(0s)` without a budget is unbounded
- Corrected stale references (`DbProvider`→`PgProvider`, `list/decl`→`list/spec`, `--scan`/`--files`→`--sources`) and dropped the non-functional `@relais output=` annotation from docs

## [0.5.0-alpha.1] - 2026-05-19

**Alpha release — API may change. Not recommended for production.**

### Added

- **Lock-free L1 cache** — ParlayHash-backed `ChunkMap` with epoch-based reclamation; `find`/`insert`/`patch` return lightweight `EntityView` instead of `shared_ptr`
- **GDSF eviction** — size-aware L1 eviction via `RELAIS_GDSF_ENABLED` CMake option + runtime `RELAIS_L1_MAX_MEMORY` env var; score = access_count × avg_cost / memoryUsage; histogram-based threshold with three-zone eviction curve; inline decay during cleanup; access count persistence across upserts; ghost admission control under memory pressure (≥ 50%); cross-repo sweep coordination; zero overhead when disabled
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
- `Immediate<T>` coroutine type — zero-allocation synchronous fast path for L1 cache hits; thread-local coroutine frame pool for the async path

### Fixed

- Use-after-free in async I/O callbacks: coroutine handle saved before `removeCurrentWatch()` destroys the enclosing lambda (PgConnection, RedisConnection)
- Inaccurate L1 memory accounting: ParlayHash internal allocations now charged to GDSF budget

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
