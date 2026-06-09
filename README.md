# relais

**Header-only C++23 repository pattern with integrated L1/L2/L3 caching.**
RAM → Redis → PostgreSQL, assembled at compile time from template parameters —
zero virtual calls, zero runtime configuration overhead.

```cpp
using UserRepo = relais::Repo<UserEntity, "User", config::Both>;

auto user = co_await UserRepo::find(123);   // L1 hit → L2 → DB, transparently
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
  Mapping>` adds ORM + thread-safe lazy BEVE/JSON only at the API layer.
- **Auto-detected features.** List caching activates when the entity has a
  `ListDescriptor`; cross-invalidation activates when you pass `Invalidate...`
  descriptors.
- **Type-safe by construction.** Hierarchical concepts (`ReadableEntity` →
  `CacheableEntity` → `MutableEntity` → `CreatableEntity`) and `requires`
  clauses turn misuse (writing to a read-only repo, `patch` without a `Field`
  enum) into compile errors.
- **Shared-nothing runtime.** Async I/O over epoll coroutines; per-loop
  connection pools, no cross-thread hops on the hot path.

## Requirements

- C++23 compiler (GCC 13+, Clang 17+), CMake 3.20+
- PostgreSQL (libpq)
- [shardmap](https://github.com/jcailloux/shardmap) — in-memory TTL cache
- [glaze](https://github.com/stephenberry/glaze) — JSON/BEVE serialization
- Redis (optional, for L2)

## Install

```cmake
include(FetchContent)

FetchContent_Declare(shardmap
    GIT_REPOSITORY https://github.com/jcailloux/shardmap.git GIT_TAG main)
FetchContent_MakeAvailable(shardmap)          # declare shardmap first

FetchContent_Declare(relais
    GIT_REPOSITORY https://github.com/jcailloux/relais.git GIT_TAG main)
FetchContent_MakeAvailable(relais)

target_link_libraries(my_app PRIVATE jcailloux::relais)
```

To run the entity generator as a build step, also put relais's `cmake/` on the
module path — see
[entities-and-generator.md › CMake integration](docs/entities-and-generator.md#cmake-integration).

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
> **[docs/entities-and-generator.md](docs/entities-and-generator.md)**.

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

Key is auto-deduced from `Entity::key()`.

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
    auto created = co_await UserRepo::insert(
        std::make_shared<const UserEntity>(std::move(u)));

    using F = UserEntity::Field;
    using jcailloux::relais::entity::set;
    co_await UserRepo::patch(created->id, set<F::balance>(999));  // partial update

    co_await UserRepo::erase(created->id);
}
```

Calls must run **on a loop thread**. Full threading contract, N-loop scaling,
and running relais on a foreign loop (Drogon/asio/…) are in
**[docs/runtime-and-threading.md](docs/runtime-and-threading.md)**.

## Cache presets

| Preset | Tiers | Use case |
|---|---|---|
| `config::Uncached` | none | Write-only tables, audit sinks |
| `config::Local` | L1 | Per-instance data (default) |
| `config::Redis` | L2 | Shared / cross-instance data |
| `config::Both` | L1 + L2 | High-read, feature flags |

Start from a preset and override with `.with_*()` chaining. Read path:
`L1 → L2 → DB`, back-filling each tier on the way up.

## Documentation

| Topic | Doc |
|---|---|
| **Entities & code generator** — struct → Mapping, annotations, `entity::generated` namespace, `glz::meta`/ODR, CMake wiring | [docs/entities-and-generator.md](docs/entities-and-generator.md) |
| **Caching, config & `patch`** — `CacheConfig`, presets, read-only, partition keys, partial updates | [docs/caching.md](docs/caching.md) |
| **Cross-invalidation** — the four `Invalidate*` mechanisms, resolvers, selective list pages | [docs/invalidation.md](docs/invalidation.md) |
| **List cache** — `filterable`/`sortable`, paginated `query()`, modification tracking | [docs/lists.md](docs/lists.md) |
| **Runtime & threading** — `IoPool`, N-loop scaling, the one threading rule | [docs/runtime-and-threading.md](docs/runtime-and-threading.md) |
| **Foreign event loops** — writing an `IoContext` adapter (Drogon/asio/libuv) | [docs/io-context-adapters.md](docs/io-context-adapters.md) |
| **Runnable examples** — CI-compiled counterparts to the runtime snippets | [examples/](examples/README.md) |
| **Internals** — mixin chain, cache tier, contribution guide | [INTERNALS.md](INTERNALS.md) |

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