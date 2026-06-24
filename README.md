# relais

**A header-only C++23 repository library for PostgreSQL, with an optional Redis L2
tier** — L1/L2/L3 caching (RAM → Redis → PostgreSQL) assembled at compile time from
template parameters: zero virtual calls, zero runtime configuration overhead.

The backends are deliberate: batching, pipelining, and invalidation build on libpq's
pipeline mode and Redis RESP, not a generic SQL/KV abstraction. Adopting relais means
plugging in a component, not an ecosystem — its coroutine runtime, Redis client, and
L1 map are all internal (see [Requirements](#requirements)).

```cpp
using UserRepo = relais::Repo<UserEntity, "User", config::Both>;

auto user = co_await UserRepo::find(123);   // L1 → L2 → L3, transparently
```

One `using` alias declares a fully-cached repository. The cache tower, list
caching, and cross-invalidation are all selected by the compiler from the
config and the entity's traits — you pay for exactly what you enable.

## Why relais

- **Compile-time mixin tower.** `PgRepo → RedisRepo → LocalRepo → ListMixin →
  InvalidationMixin` is assembled by name-hiding (no CRTP, no virtuals). Layers
  you don't configure don't exist in the binary.
- **NTTP configuration.** `CacheConfig` is a structural aggregate passed as a
  template parameter, tuned with `consteval` fluent chaining. Cache strategy is
  a type, not a runtime flag.
- **Decoupled entities.** Pure framework-agnostic structs; `Entity<Struct,
  Mapping>` adds ORM + on-demand BEVE/JSON only at the API layer.
- **Auto-detected features.** List caching activates when the entity has a
  `ListDescriptor`; cross-invalidation activates when you pass `Invalidate...`
  descriptors.
- **Type-safe by construction.** Concepts rooted at `ReadableEntity` —
  `CacheableEntity` (read + serialize) and `MutableEntity` → `CreatableEntity`
  (read + write [+ key]) as independent branches — and `requires` clauses turn
  misuse (writing to a read-only repo, `patch` without a `Field` enum) into
  compile errors.
- **Automatic SQL batching & pipelining.** Concurrent coroutines on one loop have
  their reads coalesced (`SELECT … WHERE id = ANY($1)`) and their writes pipelined
  into far fewer round-trips — no code to write, and a win even with caching off.
- **Shared-nothing runtime, on your loop or ours.** Async I/O over epoll
  coroutines; per-loop connection pools, no cross-thread hops on the hot path. The
  event loop is an extension point (`IoContext`): the bundled `IoPool` is the easy
  path, but relais runs inline on a framework loop (Drogon/asio/libuv) just as well.

## When relais fits

relais always brokers the database; the cache tiers are **independent** additions
on top — L1 and L2 are orthogonal, so it's a menu, not a ladder. The baseline (no
cache) already carries the type-safety and the batching:

| Config | Tiers | Adds |
|---|---|---|
| `Uncached` | L3 only | Compile-time-checked API + SQL batching/pipelining + shared-nothing runtime — fewer round-trips and foot-guns, no cache, no Redis. |
| `Local` | L1 only | In-process hot reads (~50 ns, no I/O). Per-instance, no Redis. |
| `Redis` | L2 only | Out-of-process cache — outlives the process, scales past the L1 RAM budget, shareable across instances. |
| `Both` | L1 + L2 | RAM reads backed by Redis (warm after a restart, shared across instances). |

**Multi-instance.** L1 coherence then depends on how data is partitioned across
processes. Disjoint key ownership keeps every invalidation local — nothing extra.
Shared keys need a finite `l1_ttl` (staleness ceiling) plus
`l2_shared_across_instances`, which moves fill-validity to Redis so a fill
straddling another instance's invalidation is rejected. Details:
[caching.md](docs/caching.md) ·
[api-reference.md › CacheConfig](docs/api-reference.md#cacheconfig).

So relais is not "a read cache": the type-safety and the batching pay off even on a
write-heavy, uncached, single-instance workload. Storage other than PostgreSQL
(+ optional Redis) is out of scope.

## Requirements

relais has exactly **one external library dependency** — glaze. Everything else is
either part of relais or a backing service you already run.

**Build dependencies** — resolved by the consumer:
- C++23 compiler (GCC 13+, Clang 17+), CMake 3.20+
- [glaze](https://github.com/stephenberry/glaze) — JSON/BEVE serialization (fetched automatically)
- libpq — PostgreSQL client headers (`find_package(PostgreSQL)`)

**Runtime services** — the assumed stack; processes you connect to, not libraries you link:
- PostgreSQL
- Redis — optional, only for the L2 tier

**Internal to relais** — shipped with it, nothing to install or adopt:
- The coroutine runtime (`io::Task`, `IoPool`) and per-loop connection pools
- The Redis client — native RESP, no `hiredis` / `redis++`
- The L1 hash map (`ChunkMap`) over a vendored lock-free backend (parlay_hash)

## Install

```cmake
include(FetchContent)

FetchContent_Declare(relais
    GIT_REPOSITORY https://github.com/jcailloux/relais.git GIT_TAG main)
FetchContent_MakeAvailable(relais)            # pulls glaze transitively

target_link_libraries(my_app PRIVATE jcailloux::relais)
```

relais needs `find_package(PostgreSQL)` to succeed (libpq dev headers) and pulls
glaze itself; the L1 hash map (ChunkMap) and its lock-free backend are vendored —
no extra `FetchContent` to declare.

Alternatively, after `cmake --install`, consume the installed package with
`find_package(jcailloux-relais REQUIRED)` (target `jcailloux::relais`). The
vendored parlay_hash headers ship with it, but — like PostgreSQL — **glaze must
be findable** by the consumer (`find_package(glaze)` resolvable), since it is an
external dependency, not vendored.

To run the entity generator as a build step, also put relais's `cmake/` on the
module path — see
[entities.md › CMake integration](docs/entities.md#cmake-integration).

## Quick start

### 1 — Define the entity (pure struct)

```cpp
// entities/User.h
// @relais table=users
struct User {
    int64_t id = 0;            // @relais primary_key db_managed
    std::string username;
    std::string email;
    int32_t balance = 0;
    std::string created_at;    // @relais timestamp
};
```

### 2 — Generate the Mapping

```bash
python scripts/generate_entities.py --sources entities/ --output-dir entities/generated/
```

This writes `entities/generated/UserEntity.h`. **Everything it emits lives in
`namespace entity::generated`** — both `UserMapping` *and* the
`UserEntity = Entity<User, UserMapping>` alias. Import the alias before use:

```cpp
#include "entities/generated/UserEntity.h"
using entity::generated::UserEntity;          // ← bring the alias into scope
```

> Full annotation reference, the generated namespace, custom JSON field names,
> and the `glz::meta` ODR rule are in
> **[docs/entities.md](docs/entities.md)**.

### 3 — Declare the repository

```cpp
#include <jcailloux/relais/repository/Repo.h>
namespace relais  = jcailloux::relais;
namespace config  = relais::config;

using UserRepo = relais::Repo<UserEntity, "User">;                       // L1 (default)
using FlagRepo = relais::Repo<FlagEntity, "Flag", config::Both>;          // L1 + L2
using SessRepo = relais::Repo<SessionEntity, "Session",
    config::Local.with_l1_ttl(std::chrono::minutes{30}).with_read_only()>;
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `Entity` | type | — | `Entity<Struct, Mapping>` |
| `Name` | `FixedString` | — | Repo name + Redis key prefix |
| `Cfg` | `CacheConfig` | `config::Local` | Cache behavior (NTTP) |
| `Invalidations...` | types | — | Cross-invalidation descriptors |

Key is auto-deduced from `Entity::key()` — the accessor relais exposes over your
`primary_key` field (here `id`).

### 4 — Stand up the runtime, then call

Repo calls run **on an event loop** and route through per-loop connection pools.
The built-in `IoPool` is the easy path:

```cpp
#include <jcailloux/relais/io/IoPool.h>
using namespace jcailloux::relais;

io::IoPoolConfig cfg;
cfg.num_workers = 4;                                       // N epoll loops
cfg.pg_conninfo = "host=localhost dbname=app user=app";   // empty → PG* env
auto pool = io::IoPool::create(cfg);                       // blocks until ready
```

```cpp
io::Task<void> example() {
    auto user = co_await UserRepo::find(123);              // cached read

    UserEntity u; u.username = "alice"; u.email = "alice@example.com";
    auto created = co_await UserRepo::insert(u);           // takes const E&

    using F = UserEntity::Field;
    using jcailloux::relais::entity::set;
    co_await UserRepo::patch(created->id, set<F::balance>(999));  // partial update

    co_await UserRepo::erase(created->id);
}
```

Calls must run **on a loop thread**. Full threading contract, N-loop scaling,
and running relais on a foreign loop (Drogon/asio/…) are in
**[docs/runtime.md](docs/runtime.md)**.

## Cache presets

| Preset | Tiers | Use case |
|---|---|---|
| `config::Uncached` | none | Write-only tables, audit sinks |
| `config::Local` | L1 | Per-instance data (default) |
| `config::Redis` | L2 | Cross-instance, or cache that must outlive the process |
| `config::Both` | L1 + L2 | High-read, feature flags |

Start from a preset and override with `.with_*()` chaining. Read path:
`L1 → L2 → DB`, back-filling each tier on the way up.

## Navigating this documentation

Each file owns one subject and reads on its own. Pick by what you're trying to do:

| Intention | Doc | What's there |
|---|---|---|
| **Learn** (5 min) | this README › [Quick start](#quick-start) | Struct → generate → repo → call. |
| **Understand** the model | [docs/concepts.md](docs/concepts.md) | How the pieces fit: entity/mapping split, the compile-time mixin tower, read/write flow, shared-nothing runtime, type-safety by concepts. |
| **Do** a task — entities | [docs/entities.md](docs/entities.md) | Struct, `@relais` annotations, the generator. |
| **Do** a task — caching | [docs/caching.md](docs/caching.md) | `CacheConfig`, presets, `patch`, partition keys. |
| **Do** a task — invalidation | [docs/invalidation.md](docs/invalidation.md) | The four `Invalidate*` mechanisms, resolvers. |
| **Do** a task — lists | [docs/lists.md](docs/lists.md) | `filterable`/`sortable`, paginated `query()`, cursors. |
| **Do** a task — runtime | [docs/runtime.md](docs/runtime.md) | `IoPool`, N-loop scaling, the threading rule. |
| **Look up** a signature | [docs/api-reference.md](docs/api-reference.md) | The exhaustive public surface — every method, config field, descriptor, list/query type, runtime type, annotation index. Every guide links here. |
| **Integrate** on an existing loop | [docs/runtime.md](docs/runtime.md) → [docs/foreign-event-loops.md](docs/foreign-event-loops.md) | Co-locate relais on a framework loop (Drogon/asio/…) via an `IoContext` adapter — advanced, optional. |
| **Dissect** the implementation | [docs/internals.md](docs/internals.md) | Mixin chain, layer selection, cache tiers, ListCache, ModificationTracker — for contributors. |
| **Run** an example | [examples/](examples/README.md) | CI-compiled counterparts to the runtime snippets. |

**Reading paths**

- **New here?** README → [concepts.md](docs/concepts.md) → the guide for the subject you need.
- **AI agent?** Read [concepts.md](docs/concepts.md) then [api-reference.md](docs/api-reference.md) — together they are enough to use relais (declare a repo, read/write, list, invalidate, stand up the runtime) **without opening a single header.**
- **New to coroutines and event loops?** `examples/event_loop_basics.cpp` drives a coroutine on a single loop **without a database** — the smallest thing that compiles and runs.

**Performance** comes from four independent levers: the right cache preset,
automatic SQL batching/pipelining (independent of the cache), the shared-nothing
`IoPool` runtime, and optional co-location on a foreign loop. Full model and the
numbers: [concepts.md › Performance model](docs/concepts.md#performance-model).

## Testing

Tests need PostgreSQL (`relais_test`/`relais_test` @ `localhost:5432`) and Redis
(`localhost:6379`). Each test rolls back via `TransactionGuard` (begin txn +
flush Redis in ctor, rollback + flush in dtor).

```bash
cmake -B .build/dev -DRELAIS_BUILD_TESTS=ON
cmake --build .build/dev
ctest --test-dir .build/dev --output-on-failure
```

Cache contamination across executables means specific suites should be run
directly, e.g. `./.build/dev/test_relais_redis "[list-selective]"`. See
[tests/README.md](tests/README.md).

## License

MIT — see [LICENSE](LICENSE).
</content>