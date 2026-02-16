# Test Internals

Technical details about the relais test infrastructure.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Test Files (.cpp)                           │
│  test_base_repository.cpp      BaseRepo (no cache) + updateBy │
│  test_redis_repository.cpp     RedisRepo (L2 cache)           │
│  test_decl_list_cache.cpp      ListMixin (L1 lists)                  │
│  test_cached_repository.cpp    CachedRepo (L1 cache)          │
│  test_partial_key.cpp          PartialKey (composite PK, partitions) │
│  test_generated_wrapper.cpp    Struct + EntityWrapper + ListWrapper  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       TestRepositories.h                            │
│  Configs: Uncached, L1Only, L2Only, Both, ShortTTL, WriteThrough…  │
│  Entity repos: L2TestItemRepo, L1TestUserRepo, L2TestPurchaseRepo… │
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
│    generated/*Wrapper.h  (Mapping + EntityWrapper aliases)          │
│    - fromRow / toInsertParams / getPrimaryKey                       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        test_helper.h                                │
│  - initTest(): DbProvider + PostgreSQL + Redis initialization       │
│  - TransactionGuard: RAII test isolation (BEGIN/ROLLBACK + flush)   │
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
3. Creates `RedisClient` (127.0.0.1:6379)
4. Calls `DbProvider::init(pool, redis)` to register providers

**Important**: Called once per test run (guarded by atomic flag).

#### `TransactionGuard`

RAII class for test isolation:
```cpp
TransactionGuard() {
    flushRedis();           // Clear Redis
    execSql("BEGIN");       // Start transaction
}

~TransactionGuard() {
    execSql("ROLLBACK");    // Undo all changes
    flushRedis();           // Clear Redis again
}
```

All database modifications within a test are automatically rolled back.

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

### Entity Structs + EntityWrapper

Each test entity has two layers:

1. **Pure data struct** (e.g., `TestItem.h`): Framework-agnostic struct with `@relais` annotations and `glz::meta` specialization
2. **EntityWrapper alias** (in `TestEntities.h`): `EntityWrapper<Struct, Mapping>` combining the struct with its generated ORM mapping

```cpp
// Pure struct
struct TestItem { int64_t id; std::string name; ... };

// Wrapped for API use
using TestItemEntity = EntityWrapper<TestItem, generated::TestItemMapping>;
```

`EntityWrapper` inherits from the struct and adds:
- `fromRow(PgResult::Row)` / `toInsertParams(Entity)` — delegated to Mapping
- `toBinary()` / `toJson()` — thread-safe lazy serialization via Glaze
- `getPrimaryKey()` — delegated to Mapping
- `Field` enum, `TraitsType` — from Mapping

Entities are **immutable** (stored as `shared_ptr<const Entity>`). To update:
```cpp
auto original = sync(Repo::find(id));
TestItemEntity updated = *original;  // Copy
updated.name = "New Name";
sync(Repo::update(id, makeEntity(updated)));
```

### TestRepositories.h

Four repository classes with different cache configurations:

```cpp
// No caching - tests BaseRepo
class UncachedTestItemRepo : public Repo<..., config::Uncached> {};

// L1 only - tests CachedRepo without Redis
class L1TestItemRepo : public Repo<..., config::L1Only> {};

// L2 only - tests RedisRepo
class L2TestItemRepo : public Repo<..., config::L2Only> {};

// Full hierarchy - tests CachedRepo with Redis
class FullCacheTestItemRepo : public Repo<..., config::Both> {};
```

All entity types are `EntityWrapper<Struct, Mapping>` aliases defined in `TestEntities.h`. List types use `ListWrapper<EntityType>`.

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
REQUIRE(L1Repo::cacheSize() >= 1);

// Clear L1 cache
L1Repo::clearCache();
```

### Test Single Case
```bash
./test_relais_base "[BaseRepo] CRUD Operations" -s
```

## Redis Repo Tests (`test_redis_repository.cpp`)

Comprehensive integration tests for the L2 (Redis) cache layer, organized in 17 logical sections:

| Section | Tag                                    | Content |
|---------|----------------------------------------|---------|
| 1 | `[item]`                               | `find` — cache hit, miss, multi-entity |
| 2 | `[item]`                               | `insert` — insert + populate Redis |
| 3 | `[item]`                               | `update` — invalidate Redis (lazy reload) |
| 4 | `[item]`                               | `remove` — invalidate Redis |
| 5 | `[binary]`                             | Binary (BEVE) serialization in Redis |
| 6 | `[updateBy]`                           | Partial field updates with Redis invalidation |
| 7 | `[json]`                               | `findAsJson` raw JSON retrieval |
| 8 | `[invalidate]`                         | Explicit `invalidateRedis` + isolation |
| 9 | `[readonly]`                           | Read-only repository caching |
| 10 | `[cross-inv]`                          | `Invalidate<>` entity→entity (insert, update, delete, FK change) |
| 11 | `[custom-inv]`                         | `InvalidateVia<>` with custom resolver |
| 12 | `[readonly-inv]`                       | Cross-invalidation targeting read-only caches |
| 13 | `[list]` + `[fb-list]`                 | List caching — JSON and BEVE binary |
| 14 | `[list-inv]` + `[list-custom]`         | `InvalidateList<>` and list custom resolver |
| 15 | `[list-tracked]`                       | Tracked pagination + group invalidation + tracking data |
| 16 | `[list-selective]` + `[list-resolver]` | SortBounds Lua selective invalidation + `InvalidateListVia` with GroupKey |
| 17 | `[list-granularity]`                   | Three granularities: per-page, per-group, full pattern |

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

Each list repository defines its own `GroupKey` struct containing the typed filter values that identify a cache group. This is **not** a Redis key string — it represents the logical group identity.

```cpp
struct GroupKey {
    std::string category;  // matches the filter declared in ListDescriptor
};
```

### `invalidateByTarget(GroupKey, optional<int64_t>)`

Defined in the concrete list repository. Translates the typed `GroupKey` + optional sort value into actual cache operations:

- **`sort_value` present** → calls `invalidateListGroupSelective()` (per-page, Lua-based)
- **`sort_value` absent** → calls `invalidateListGroup()` (per-group, deletes all pages)

### `invalidateAllListGroups()`

Defined in the CRTP hierarchy:
- **BaseRepo**: no-op (no cache to invalidate)
- **RedisRepo**: `SCAN` with pattern `name() + ":list:*"` and `DEL` each match

Called when the resolver returns `std::nullopt` (full pattern invalidation).

### Cache-level agnosticism

The `ListInvalidationTarget<GroupKey>` API is identical regardless of cache level. The resolver always works with typed values — the cache-level-specific logic lives in `invalidateByTarget` and `invalidateAllListGroups` implementations, not in the resolver.

## PartialKey Repositories (`test_partial_key.cpp`)

Tests for repositories where `Key` differs from `Model::PrimaryKeyType` — typically partitioned PostgreSQL tables with a composite PK (e.g., `(id, region)`) but queried by a single unique key (`id`).

### Test Entity: `TestEvent`

- **Table**: `relais_test_events` — range-partitioned by `region` (eu, us, ap)
- **Composite PK**: `(id BIGINT, region VARCHAR)` — `id` from shared sequence, `region` as partition key
- **Repo Key**: `int64_t` (partial — only `id`)
- **Config requires**: `key_is_unique = true` + `makeKeyCriteria<Model>(id)`

### Tested Configurations

| Repository | Config | Cache Level |
|-----------|--------|-------------|
| `UncachedTestEventRepo` | `EventPartialKeyUncached` | None |
| `L1TestEventRepo` | `EventPartialKeyL1` | L1 |
| `L2TestEventRepo` | `EventPartialKeyL2` | L2 |
| `L1L2TestEventRepo` | `EventPartialKeyBoth` | L1+L2 |

### `updateBy` — Criteria-Based for PartialKey

For PartialKey entities, `updateBy` builds a dynamic SQL query:
```sql
UPDATE table SET "col1"=$1, "col2"=$2 WHERE "id"=$3 RETURNING *
```

Helpers in `FieldUpdate.h`: `fieldColumnName<Traits>(update)` extracts the quoted column name, `fieldValue<Traits>(update)` extracts the properly-typed value.

### `remove` — Opportunistic Full PK via Cache Hint

For partition pruning, `DELETE WHERE id=$1 AND region=$2` scans 1 partition vs `DELETE WHERE id=$1` scans N. The optimization uses cached entities as hints:

```
CachedRepo::remove(id)
  → hint = getFromCache(id)        // Free L1 check
  → RedisRepo::removeImpl(id, hint)
    → if (!hint && PartialKey) {
        hint = getFromRedis(id)    // ~0.1ms L2 check
      }
    → BaseRepo::removeImpl(id, hint)
      → if (hint) deleteByPrimaryKey(fullPK)  // Pruned: 1 partition
      → else      deleteBy(criteria)           // Scan: N partitions
```

**Performance rule**: Never add a DB round-trip just for partition pruning. Only use full PK when the entity is already available from cache (free or near-free).

### Test Coverage

| Section | Content                                                                 |
|---------|-------------------------------------------------------------------------|
| 1 | CRUD: find, insert, update, remove (Uncached)                           |
| 2 | L1 caching: cache hit, staleness, invalidation                          |
| 3 | L2 caching: Redis hit, staleness, invalidation                          |
| 4 | Cross-invalidation: Event as source (→ User L1)                         |
| 5 | Cross-invalidation: Event as target (Purchase → Event via resolver)     |
| 6 | PartialKeyValidator runtime checks                                      |
| 7 | Serialization: JSON + BEVE round-trip                                   |
| 8a | updateBy Uncached: single/multi field, partition preservation, re-fetch |
| 8b | updateBy L1: cache invalidation + re-fetch                              |
| 8c | updateBy L2: Redis invalidation + re-fetch                              |
| 8d | updateBy cross-invalidation: Event updateBy → User L1                   |
| 9a | remove L1 hint: cache hit vs miss paths                                 |
| 9b | remove L2 hint: Redis hit vs miss paths                                 |
| 9c | remove L1+L2 chain: L1 hit, L1 miss/L2 hit, both miss                   |