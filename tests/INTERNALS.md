# Test Internals

Technical details about the relais test infrastructure.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Test Files (.cpp)                           │
│  test_pg_repo.cpp              PgRepo (no cache) + patch │
│  test_decl_list_cache.cpp      ListMixin (L1 lists)                  │
│  test_local_repo.cpp           LocalRepo (L1 cache)          │
│  test_partition_key.cpp         PartitionKey (composite PK, partitions) │
│  test_generated_entity.cpp     Struct + Entity + ListWrapper          │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       TestRepositories.h                            │
│  Configs: Uncached, Local, ShortTTL, WriteThrough, FewChunks…       │
│  Entity repos: UncachedTestItemRepo, L1TestItemRepo, L1TestUserRepo…│
│  List repos: TestArticleListRepo, TestPurchaseListRepo  │
│  (inherit from relais::Repo<...> + ListCacheRepo)    │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Entity layer                                  │
│    TestItem.h (pure struct)     TestUser.h     TestArticle.h        │
│    TestPurchase.h     TestEvent.h (composite PK: id+region)         │
│    generated/*Entity.h   (Mapping + Entity aliases)                 │
│    - fromRow / toInsertParams / key                       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        test_helper.h                                │
│  - initTest(): PgProvider + PostgreSQL initialization               │
│  - TransactionGuard: RAII test isolation (deletes test data)        │
│  - sync(): Coroutine synchronous execution                          │
│  - insertTestItem/User/Purchase/Article: Direct DB helpers          │
│  - getCacheSize<Repo>(), forceFullCleanup<Repo>()                  │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Components

### test_helper.h

#### `initTest()`

Initializes the I/O layer for testing:
1. Creates `IoContext` and starts the event loop in a background thread
2. Creates `PgPool` with hardcoded config (localhost:5432, relais_test)
3. Calls `PgProvider::init(io, pool, max_concurrent)` to register providers

**Important**: Called once per test run (guarded by atomic flag).

#### `TransactionGuard`

RAII class for test isolation: its constructor and its destructor both delete
the rows of every test table and reset the GDSF state. No test leaves data behind,
even on failure.

#### `sync<T>()`

Runs a coroutine synchronously:
```cpp
auto result = sync(Repo::find(123));
// Blocks until the coroutine completes
```

Uses a promise/future pair to block the calling thread until the `io::Task` completes.

#### Direct DB Helpers

Bypass the repository to set up test data:
- `insertTestItem(name, value, description, is_active)` - Returns generated ID
- `updateTestItem(id, name, value)` - Direct UPDATE
- `deleteTestItem(id)` - Direct DELETE

### Entity Structs + Entity

Each test entity has two layers:

1. **Pure data struct** (e.g., `TestItem.h`): Framework-agnostic struct with `@relais` annotations and `glz::meta` specialization
2. **Entity alias** (in `TestEntities.h`): `Entity<Struct, Mapping>` combining the struct with its generated ORM mapping

```cpp
// Pure struct
struct TestItem { int64_t id; std::string name; ... };

// Entity for API use
using TestItemEntity = Entity<TestItem, generated::TestItemMapping>;
```

`Entity` inherits from the struct and adds:
- `fromRow(PgResult::Row)` / `toInsertParams(Entity)` — delegated to Mapping
- `binary()` / `json()` — thread-safe lazy serialization via Glaze
- `key()` — delegated to Mapping
- `Field` enum, `TraitsType` — from Mapping

Entities are **immutable** (stored as `shared_ptr<const Entity>`). To update:
```cpp
auto original = sync(Repo::find(id));
TestItemEntity updated = *original;  // Copy
updated.name = "New Name";
sync(Repo::update(id, makeEntity(updated)));
```

### TestRepositories.h

Repositories come in two cache configurations, plus `Local` variants tuned for
specific tests (`ShortTTL`, `WriteThrough`, `FewChunks`…):

```cpp
// No caching - tests PgRepo
using UncachedTestItemRepo = Repo<TestItemEntity, "test:uncached", cfg::Uncached>;

// L1 - tests LocalRepo
using L1TestItemRepo = Repo<TestItemEntity, "test:l1">;
```

All entity types are `Entity<Struct, Mapping>` aliases defined in `TestEntities.h`. List types use `ListWrapper<EntityType>`.

## Test Database Schema

```sql
CREATE TABLE relais_test_items (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value INTEGER NOT NULL DEFAULT 0,
    description TEXT,                    -- nullable
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
```

## Common Test Patterns

### Basic CRUD Test
```cpp
TEST_CASE("[Repo] CRUD", "[integration][db]") {
    initTest();

    SECTION("find returns entity") {
        TransactionGuard tx;

        auto id = insertTestItem("Test", 42);
        auto result = sync(Repo::find(id));

        REQUIRE(result != nullptr);
        REQUIRE(result->name == "Test");
    }
}
```

### Cache Behavior Test
```cpp
SECTION("L1 cache returns stale data") {
    TransactionGuard tx;

    auto id = insertTestItem("Original", 1);

    // Populate cache
    sync(L1Repo::find(id));

    // Modify directly in DB (bypass cache)
    updateTestItem(id, "Modified", 2);

    // Should still return cached value
    auto cached = sync(L1Repo::find(id));
    REQUIRE(cached->name == "Original");  // Stale!
}
```

### Invalidation Test
```cpp
SECTION("invalidate clears cache") {
    TransactionGuard tx;

    auto id = insertTestItem("Test", 1);
    sync(Repo::find(id));  // Populate cache

    updateTestItem(id, "Updated", 2);
    sync(Repo::invalidate(id));  // Clear cache

    auto fresh = sync(Repo::find(id));
    REQUIRE(fresh->name == "Updated");  // Fresh from DB
}
```

## Debugging Tips

### View SQL Queries
Set the relais log callback before `initTest()`:
```cpp
jcailloux::relais::log::setCallback([](auto level, const char* msg, size_t len) {
    std::cerr << std::string_view(msg, len) << "\n";
});
initTest();
```

### Check Cache State
```cpp
// L1 cache size
REQUIRE(L1Repo::size() >= 1);

// Clear L1 cache
L1Repo::clearCache();
```

### Test Single Case
```bash
./test_relais_base "[PgRepo] CRUD Operations" -s
```

## ListDescriptor Tests (`test_decl_list_cache.cpp`)

Integration tests for the declarative L1 list cache system:

| Section | Tag | Content |
|---------|-----|---------|
| 1 | `[query]` | Article list — filters, sorts, limits, empty results |
| 2 | `[itemview]` | Article `ItemView` accessors on cached results |
| 3 | `[query]` | Purchase list — user_id/status filters |
| 4 | `[itemview]` | Purchase `ItemView` accessors |
| 5 | `[sortbounds]` | SortBounds invalidation precision (insert, update, delete, filter mismatch) |
| 6 | `[tracker-cleanup]` | `ModificationTracker` cleanup cycles |

## Cross-Invalidation Patterns

The library provides four cross-invalidation mechanisms, all declared via `InvalidateOn`:

### `Invalidate<Cache, &Entity::key>`

Direct entity→entity invalidation. When a source entity is created/updated/deleted, the target cache is invalidated using the extracted key.

```cpp
using Invalidates = InvalidateOn<
    Invalidate<UserCache, &Purchase::user_id>  // Purchase change → invalidate User
>;
```

On **update**, both old and new key values are invalidated (handles FK changes).

### `InvalidateList<ListCache>`

Direct entity→list notification. The source entity change triggers `onEntityModified` on the target list cache, which decides how to invalidate.

```cpp
using Invalidates = InvalidateOn<
    InvalidateList<PurchaseListCache>  // Purchase change → notify PurchaseList
>;
```

### `InvalidateVia<Cache, &Entity::key, &Resolver::resolve>`

Indirect entity→entity with a custom resolver. The resolver maps the source key to one or more target keys.

```cpp
struct PurchaseToArticleResolver {
    static Task<std::vector<int64_t>> resolve(int64_t user_id);
};
using Invalidates = InvalidateOn<
    InvalidateVia<ArticleCache, &Purchase::user_id, &PurchaseToArticleResolver::resolve>
>;
```

### `InvalidateListVia<ListRepo, &Entity::key, &Resolver::resolve>`

Indirect entity→list with a typed `GroupKey` resolver and three granularities.

```cpp
using GroupKey = ArticleListRepo::GroupKey;
using Target = ListInvalidationTarget<GroupKey>;

struct Resolver {
    static Task<vector<Target>> resolve(int64_t user_id);
    // Or: static Task<optional<vector<Target>>> resolve(int64_t user_id);
};

using Invalidates = InvalidateOn<
    InvalidateListVia<ArticleListRepo, &Purchase::user_id, &Resolver::resolve>
>;
```

## `ListInvalidationTarget<GroupKey>` Design

### GroupKey

Each list repository defines its own `GroupKey` struct containing the typed filter values that identify a cache group. It represents the logical group identity, not a cache key.

```cpp
struct GroupKey {
    std::string category;  // matches the filter declared in ListDescriptor
};
```

### `invalidateByTarget(GroupKey, optional<int64_t>)`

Defined in the concrete list repository. Translates the typed `GroupKey` + optional sort value into actual cache operations.

### `invalidateAllListGroups()`

Defined in the mixin chain:
- **PgRepo**: no-op (no cache to invalidate)
- **ListMixin**: records one full-range modification on the L1 tracker; every cached page is dropped lazily on its next read or sweep

Called when the resolver returns `std::nullopt` (full pattern invalidation).

### Cache-level agnosticism

The `ListInvalidationTarget<GroupKey>` API is identical regardless of cache level. The resolver always works with typed values — the cache-level-specific logic lives in `invalidateByTarget` and `invalidateAllListGroups` implementations, not in the resolver.

## Partition Key Repositories (`test_partition_key.cpp`)

Tests for repositories with partitioned PostgreSQL tables where the composite PK includes a partition column (e.g., `(id, region)`) but the repository key is only the logical identifier (`id`).

### Test Entity: `TestEvent`

- **Table**: `relais_test_events` — range-partitioned by `region` (eu, us, ap)
- **Composite PK**: `(id BIGINT, region VARCHAR)` — `id` from shared sequence, `region` as partition key
- **Repo Key**: `int64_t` (only `id` — partition key is a separate hint)

### Tested Configurations

| Repository | Config | Cache Level |
|-----------|--------|-------------|
| `UncachedTestEventRepo` | `Uncached` | None |
| `L1TestEventRepo` | `Local` | L1 |

### `patch` — Criteria-Based

`patch` builds a dynamic SQL query:
```sql
UPDATE table SET "col1"=$1, "col2"=$2 WHERE "id"=$3 RETURNING *
```

Helpers in `FieldUpdate.h`: `fieldColumnName<Traits>(update)` extracts the quoted column name, `fieldValue<Traits>(update)` extracts the properly-typed value.

### `erase` — Opportunistic Full PK via Cache Hint

For partition pruning, `DELETE WHERE id=$1 AND region=$2` scans 1 partition vs `DELETE WHERE id=$1` scans N. The optimization uses cached entities as hints:

```
LocalRepo::erase(id)
  → hint = getFromCache(id)        // Free L1 check
  → PgRepo::eraseImpl(id, hint)
    → if (hint) deleteByPrimaryKey(fullPK)  // Pruned: 1 partition
    → else      deleteByKey(id)              // Scan: N partitions
```

**Performance rule**: Never add a DB round-trip just for partition pruning. Only use full PK when the entity is already in L1.

### Test Coverage

| Section | Content                                                                 |
|---------|-------------------------------------------------------------------------|
| 1 | CRUD: find, insert, update, erase (Uncached)                           |
| 2 | L1 caching: cache hit, staleness, invalidation                          |
| 3 | Cross-invalidation: Event as source (→ User L1)                         |
| 4 | Cross-invalidation: Event as target (Purchase → Event via resolver)     |
| 5 | Serialization: JSON + BEVE round-trip                                   |
| 6a | patch Uncached: single/multi field, partition preservation, re-fetch |
| 6b | patch L1: cache invalidation + re-fetch                              |
| 6c | patch cross-invalidation: Event patch → User L1                   |
| 7 | erase L1 hint: cache hit vs miss paths                                 |
