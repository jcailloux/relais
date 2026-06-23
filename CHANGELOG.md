# Changelog

## [Unreleased]

## [1.0.0] - 2026-06-24

### Added

- **Descriptor-tagged keyset cursor (`Repo::Cursor`).** `Repo::Cursor` is
  `list::spec::TypedCursor<Descriptor>`, obtained only through
  `Repo::Cursor::decode(token)` (the raw constructor is private). `.after()` and
  `ListQueryParams::cursor` take it, so handing one list's cursor to another
  list's query/builder is a compile error, not a runtime mis-decode. The base64
  wire token is unchanged (`view->cursor()` is still a `std::string`); the tag
  lives on the construction side only. Limit: `decode` does not verify a token's
  provenance — a well-formed token from another list still decodes (its keyset
  bytes are then meaningless), as the token is opaque and arrives off the wire.

- **Typed self-sealing list query builder.** `Repo::queryBuilder()` is the
  primary C++ construction path for a list query: `.filter<"name">(v)`,
  `.sortBy<"name", Dir>()` (and `.sortAsc`/`.sortDesc`), `.limit(n)`,
  `.after(cursor)`/`.offset(n)`, terminated by `.build()`. Filters and sort are
  set **by name, checked at compile time** (an unknown name is a `static_assert`,
  not a runtime miss); a `Sort<>` reorder can no longer silently bind the wrong
  column. `.limit()` is the trusted path — exact page size, no grid
  normalization. `.build()` is the single sealing point.

- **Cross-instance L2 coherence (`CacheConfig::l2_shared_across_instances`).**
  Topology flag, default `false`. Set it `true` when more than one process
  targets the same Redis: a Redis-side per-slot generation (versioned CAS)
  rejects any cache fill that straddles an invalidation on *any* instance,
  guaranteeing zero stale writes to L2 cross-process. The L2 hit stays a plain
  `GET` — the generation is consulted only at a miss-fill. `false`
  (single-instance) is the safe, zero-extra-Redis-cost default: coherence is
  process-local. Multi-instance with the flag off leaves a `l2_ttl`-bounded
  stale window on the rare straddling race.

- **`CacheConfig::recheck_slots_log2`** (default 12 → 4096 slots, 32 KB/repo) —
  sizes the process-local read-fill recheck table. It scales with write
  concurrency, not key count; a slot collision yields a pessimistic cache miss,
  never a stale read. Raise it for write-heavy repos where write concurrency
  approaches the slot count.

- **`l2_refresh_on_get` now covers multi-key reads.** When set, `findMany`
  refreshes the L2 TTL of every hit key in a single round-trip, mirroring the
  single-`get` behavior. Default `false` leaves `MGET` untouched.

### Changed (Breaking)

- **`update`/`updateJson`/`updateBinary` return `Task<std::optional<size_t>>`**
  (was `Task<bool>`) — rows affected (`0` = not found), `nullopt` = DB error,
  parity with the `erase` family. Migration: a former truthy `if (update(...))`
  must guard on `*r > 0` — an `optional` is truthy even at `0`.

### Changed

- **List queries are now sealed and immutable.** The construction type split in
  two: `Repo::ListQueryParams` (mutable — `filters`, `sort`, `limit`, `cursor`,
  `offset`; no key fields) and `Repo::ListQuery` (immutable, constructible only
  via `seal()`, const getters `.filters()`/`.sort()`/`.limit()`/`.cursor()`/
  `.offset()`/`.groupKey()`/`.cacheKey()`). `seal<Descriptor>(params)` — or
  `.build()` — computes both cache keys **once** from the final params. The
  manual `q.group_key = …` / `q.cache_key = …` assignment is removed: a query
  that reaches `query()` always carries keys consistent with its contents, by
  construction. `query()`/`queryJson()`/`queryBinary()` and
  `parseListQuery`/`parseListQueryStrict` now operate on `ListQuery` (sealed).
  `sortBy<"name", Dir>` is `consteval` (sort resolved by name at compile time).

- **Batch and predicate erase / invalidate.** Four set-oriented methods beside
  the per-id pair. `eraseMany(span<const Key>)` / `invalidateMany(span<const Key>)`
  take an enumerated, deduped key set; `eraseWhere(pred)` / `invalidateWhere(pred)`
  take a generated `FilterSet<E>` predicate (designated initializers) that resolves
  its own set server-side. `erase*` deletes from L3 and returns `optional<size_t>`
  rows deleted (`nullopt` = DB error, parité mono); `invalidate*` only evicts caches
  (rows persist) and returns `void`. The cascade collapses to one round-trip per
  tier — `eraseMany` is one `DELETE … WHERE pk = ANY RETURNING` + one batched
  `UNLINK`; cross-invalidation is deduplicated across the whole set. `eraseWhere`'s
  own-list invalidation is predicate-driven (one `RangeModification` L1 + one `EVAL`
  L2, O(1)/O(groups), filter-aware, never-miss — never `purgeAll`), not the resolved
  id set. `eraseMany`/`invalidateMany` need an L1-bearing repo and a `Key`;
  `eraseWhere`/`invalidateWhere` need `HasFilterSet<E>`; `erase*` needs `!read_only`.

- **Batch erase/invalidate detach their deferred cleanup.** `eraseMany`/
  `eraseWhere`/`invalidateMany`/`invalidateWhere` await only the critical pass —
  the entity-tier L2 `UNLINK`, L1 evict, and L1 list-tracker bump — then fire the
  cross-target invalidation and selective L2 list `EVAL`s fire-and-forget.
  Caller-visible `erase*` latency is one `DELETE` + one entity round-trip; the
  deferred L2 work no longer blocks the return. L1 list reads stay guarded (the
  `ModificationTracker` bump is in the critical pass); detached L2 list staleness
  is `l2_ttl`-bounded, unchanged.

- **Fewer round-trips on concurrent reads and batch invalidation.** Concurrent
  point reads (`find` + `findMany`) on the same table coalesce into one
  `WHERE pk = ANY` server round-trip with per-caller fan-out. Batch invalidation
  gathers its cross-target `UNLINK`s (M mono-key → one multi-key) and its L2 list
  `EVAL`s into a single flush instead of one round-trip each. PostgreSQL and Redis
  no longer share one concurrency budget — PG keeps its full connection pool under
  mixed load. No API change; identical results.

- **`nin` list filter operator (set anti-membership).** `filterable:nin` (alias `not_in`) is the logical inverse of `in`: it matches a column against *none* of a comma-separated value set, reusing the `in` parsing, canonicalization, binary format, and cache key. Element types, the 256 bound, and the compile-time `enum`/converter rejection are shared with `in`. SQL uses `!= ALL($n)` mirroring `= ANY($n)`. NULL column values match neither `in` nor `nin` (three-valued logic — `nin` is the negation of `in` only on non-NULL rows); the empty set is the universe (`nin {}` matches everything, opposite of `in {}`).

### Removed

- **Positional filter accessor `Filters::get<N>()` / `FilterTags::get<N>()`.**
  Filters are reachable only by name (`get<"field">()`, `.filter<"name">()`).
  The index was a footgun: slots follow alphabetical param-name order, never
  declaration order, so a numeric index silently bound the wrong filter on any
  reorder or addition. Generic iteration uses `std::get<I>(obj.values)` directly.

### Fixed

- **Read-fill race re-cached deleted entities.** A read whose L3 fetch straddled
  a concurrent `erase`/`invalidate` (mono or batch) wrote the just-deleted row
  back into L1 and L2 *after* eviction had cleared them, leaving a
  `l2_ttl`-bounded phantom until expiry. A process-local read-fill recheck
  (generation snapshot at the miss, recompare before the cache store) now skips
  the store on a straddling mutation — zero cost on cache hits, no per-entry
  metadata. Cross-instance L2 is closed under `l2_shared_across_instances` (see
  Added).

- **Static-destruction-order use-after-free in the epoch entity/list pools.** The
  per-repo `memory_pool` was constructed before its `ThreadIdPool` dependency
  (lazily created on the first retire), so at process exit the pool's destructor
  read a freed `ThreadIdPool`. The pool accessors now force the epoch and thread
  pool to construct first, guaranteeing reverse-order teardown.

### Documentation

- **Read-on-error contract.** `find`/`findJson`/`findBinary`/`findMany` collapse
  a DB error into the empty result, indistinguishable from a genuine miss; reads
  never throw. Distinguish the two via the raw `PgProvider` path, which rethrows
  `PgError`. Error visibility is a write-side guarantee (`erase*` → `nullopt`),
  not a read-side one.

## [0.5.0-alpha.8] - 2026-06-18

### Added

- **`findMany(ids)` batched multi-id read** on L1-bearing repos (`Local`, `Both`). Returns a guarded `MultiView<E>` where `view[i]` maps positionally to `ids[i]` (`nullptr` = absent in every tier, duplicate ids collapse to one entry). L1 misses are batched into one `MGET` (L2) and one `WHERE pk = ANY($1)` (L3) — one round-trip per tier, not N. L1 hits are zero-copy slot pointers pinned by a single batch `EpochGuard` and must not outlive the view; L2 warm-fills of L3 hits are detached (fire-and-forget).

## [0.5.0-alpha.7] - 2026-06-16

### Added

- **Per-model list page-size grids (`@relais_list limits=…`).** The grid is honored end-to-end at arbitrary length: the generator sorts and deduplicates it and emits `allowedLimits`/`defaultLimit`/`maxLimit` on the `ListDescriptor`, driving limit normalization (tolerant: round up, cap at `maxLimit`; strict: exact membership → `InvalidLimit`) and the canonical cache key. Omitting `limits=` yields `{10,25,50}`.

### Changed

- **Default list page size is now the descriptor's `defaultLimit`** (grid front) when a request omits the `limit` param, instead of the fixed `20`. Default-page cache entries keyed on the previous size become orphaned and expire at TTL.

## [0.5.0-alpha.6] - 2026-06-16

### Changed

- **glaze dependency bumped to v7.8.1** (from v7.0.2). `FIND_PACKAGE_ARGS` now requires a system glaze ≥ 7.8.

### Fixed

- **`EpollIoContext` use-after-free on watch self-removal.** A watch callback that calls `removeWatch(fd)` on its own handle — which relais does on connection EOF/error — erased the map node holding the `std::function` being invoked, freeing its captured state mid-call. The dispatcher now copies the callback onto the stack before invoking, keeping the target alive across self-removal.

### Added

- **`in` list filter operator (set membership).** `filterable:in` matches a column against a comma-separated value set (`?authors=1,2,3`), canonicalized (deduplicated, sorted) before use. Element types are `int64`, `int32`, `std::string`, and `bool`; `enum` and field converters are rejected at compile time. The set is bounded at 256 elements; an empty or all-invalid set leaves the filter inactive. SQL uses `= ANY($n)` with a single array param, kept consistent with the L1 and L2 membership paths.

- **Boolean parsing for HTTP list filters.** Boolean filter values — for `in` and scalar `eq` — accept the standard HTTP / HTML-form conventions case-insensitively (`true/1/t/yes/y/on`, `false/0/f/no/n/off`). Any other token leaves the filter inactive instead of defaulting to `false`; previously a `filterable` bool could never activate via HTTP.

- **`IoContext` conformance check C10 (`checkRemoveWatchReentrant`).** `removeWatch` must be safe to call from inside its own watch callback. Adapters that tear down loop-owned watch state synchronously fail `runAll` under ASan instead of corrupting memory; the rule is documented on the concept and in `docs/io-context-adapters.md`.

## [0.5.0-alpha.5] - 2026-06-11

### Added

- **Array column mapping (`T[]` ↔ `std::vector<T>`).** PostgreSQL array columns map to `std::vector<T>` for scalar `T` (`int*`, `double`, `bool`, `std::string`), read and write, with `text[]` quoting handled both ways. Unquoted `NULL` elements are rejected; arrays of structs stay on the `json_field` path. Unblocks `array_agg` read-only views as point-lookup entities.

- **Composite-key list pagination.** The keyset cursor spans every primary-key column, so an entity with a composite key (e.g. an all-PK junction) can carry a `@relais_list`. Key components must be integers; scalar-key cursors are unchanged.

## [0.5.0-alpha.4] - 2026-06-11

### Fixed

- **Entities with no updatable column failed to compile** (`no type named 'Field'`). A pure all-primary-key junction (e.g. `MemberRole(user_id, role_id)`) and a `@relais read_only` view have no fields to update, so the generator skipped the `Field` enum entirely — but `Entity<>` aliases `TraitsType::Field` unconditionally, so the entity could not be instantiated. The generator now always emits the enum, empty when nothing is updatable. Regression coverage: `tests/relais/test_repo_compile.cpp` (`[junction]`/`[readonly]`) with `TestAllPkJunction`/`TestReadOnlyView` fixtures
- **Malformed `SQL::update` for all-PK junctions.** With every column in the primary key, the `SET` clause was empty (`UPDATE t SET  WHERE …`) — a valid string literal that only fails when sent to PostgreSQL. `SQL::update`/`toUpdateParams` are now emitted only when a non-PK, non-`db_managed` column exists

### Changed

- **Full-update methods are gated on a new `HasFullUpdate` concept.** `update`, `updateOutcome`, `updateWithContext`, `updateJson`, and `updateBinary` are now absent (across every mixin layer) for entities with no updatable column, rather than failing to instantiate at the call site. `insert()`/`erase()` are unaffected; entities with updatable columns are unchanged

## [0.5.0-alpha.3] - 2026-06-10

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
