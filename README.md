# relais

A header-only C++23 repository pattern library with integrated multi-tier caching (L1 RAM, L2 Redis, L3 PostgreSQL).

## Features

- **Multi-tier caching**: L1 (in-memory) and L2 (Redis) cache layers with automatic fallback
- **Mixin architecture**: Zero virtual call overhead through compile-time method hiding — no CRTP
- **Zero boilerplate**: One `using` alias per repository, auto-assembled from template parameters
- **CacheConfig NTTP**: Compile-time configuration via structural aggregate with `consteval` fluent chaining
- **Configurable**: Choose caching strategy per repository (none, L1, L2, or both) via presets
- **Wrapper-centric design**: Repositories work only with immutable wrappers (`WrapperPtr = shared_ptr<const Entity>`)
- **Struct-based entities**: Pure C++ structs with `EntityWrapper<Struct, Mapping>` adding ORM + serialization at the API layer
- **ORM decoupled**: Entity structs are framework-agnostic and can be shared across projects
- **Auto-detected list caching**: ListMixin activates when the entity's Mapping has an embedded `ListDescriptor`
- **Smart invalidation**: Cross-cache dependency propagation via variadic `Invalidations...` pack
- **Partial updates**: Type-safe `patch()` with compile-time field enum — updates only specified columns
- **Partition key optimization**: `@relais partition_key` enables single-partition DELETE via cache hints (`HasPartitionKey` concept)
- **Type-safe**: Hierarchical concepts (`ReadableEntity`, `CacheableEntity`, `MutableEntity`, `CreatableEntity`)
- **Read-only enforcement**: Compile-time `requires` clause prevents insert/update/delete on read-only repositories
- **Annotation-based generator**: Auto-generate ORM mappings from `@relais` annotations in struct headers

## Requirements

- C++23 compiler (GCC 13+, Clang 17+)
- PostgreSQL (libpq)
- [shardmap](https://github.com/jcailloux/shardmap) (in-memory TTL cache with callback-based validation)
- [glaze](https://github.com/stephenberry/glaze) (JSON/BEVE serialization)
- Redis (optional, for L2 caching)

## Installation

### CMake FetchContent

```cmake
include(FetchContent)

# Required: add shardmap first
FetchContent_Declare(shardmap
    GIT_REPOSITORY https://github.com/jcailloux/shardmap.git
    GIT_TAG main
)
FetchContent_MakeAvailable(shardmap)

# Then add relais
FetchContent_Declare(relais
    GIT_REPOSITORY https://github.com/jcailloux/relais.git
    GIT_TAG main
)
FetchContent_MakeAvailable(relais)

target_link_libraries(my_app PRIVATE jcailloux::relais)
```

### As a subdirectory

```cmake
add_subdirectory(lib/shardmap)
add_subdirectory(lib/jcailloux-relais)
target_link_libraries(my_app PRIVATE jcailloux::relais)
```

## Quick Start

### 1. Define your entity

Entities are pure C++ structs with `@relais` inline annotations. The Python generator reads these annotations and produces a standalone Mapping struct. At the API layer, `EntityWrapper<Struct, Mapping>` combines the struct with its ORM mapping and provides thread-safe lazy serialization.

```cpp
// 1. Pure data struct (framework-agnostic, shareable)
// @relais table=users
// @relais output=entities/generated/UserWrapper.h
struct User {
    int64_t id = 0;    // @relais primary_key db_managed
    std::string username;
    std::string email;
    int32_t balance = 0;
    std::string created_at;  // @relais timestamp
};

// glz::meta for JSON/BEVE serialization
template<> struct glz::meta<User> {
    using T = User;
    static constexpr auto value = glz::object(
        "id", &T::id, "username", &T::username,
        "email", &T::email, "balance", &T::balance,
        "created_at", &T::created_at);
};

// 2. Generated Mapping (by generate_entities.py) provides:
//    - fromRow<Entity>, toInsertParams<Entity>, key<Entity>
//    - SQL struct (select_by_pk, insert, update, delete_by_pk)
//    - makeFullKeyParams (if partition_key annotation present)
//    - TraitsType with Field enum + FieldInfo (for patch)
//    - glaze_value (fallback Glaze metadata)
//    - ListDescriptor (if filterable/sortable annotations present)

// 3. EntityWrapper alias (generated):
using UserWrapper = jcailloux::relais::wrapper::EntityWrapper<User, generated::UserMapping>;
// UserWrapper inherits from User and adds:
// - fromRow/toInsertParams (delegated to Mapping)
// - binary/json (thread-safe lazy BEVE/JSON via Glaze)
// - key
```

### 2. Create your repository

```cpp
#include <jcailloux/relais/repository/Repo.h>

namespace relais = jcailloux::relais;
namespace config = relais::config;

// Simple — L1 RAM cache (default)
using UserRepo = relais::Repo<UserWrapper, "User">;

// Custom cache preset
using MetricsRepo = relais::Repo<MetricsWrapper, "Metrics", config::Both>;

// Customized preset with fluent chaining
using SessionRepo = relais::Repo<
    SessionWrapper, "Session",
    config::Local.with_l1_ttl(std::chrono::minutes{30}).with_read_only()>;

// With cross-invalidation
using PurchaseRepo = relais::Repo<
    PurchaseWrapper, "Purchase", config::Local,
    cache::Invalidate<UserStatsRepo, &Purchase::user_id>>;
```

**Inherited methods:**
- `find(id)` — `Task<WrapperPtr>` (cached)
- `findJson(id)` — `Task<shared_ptr<const string>>`
- `insert(wrapper)` — `Task<WrapperPtr>` (requires `!read_only`)
- `update(id, wrapper)` — `Task<bool>` (requires `!read_only`)
- `patch(id, set<F>(v)...)` — `Task<WrapperPtr>` (requires `HasFieldUpdate`)
- `erase(id)` — `Task<optional<size_t>>` (requires `!read_only`)
- `invalidate(id)` — `Task<void>`

If the entity has an embedded `ListDescriptor` (from `filterable`/`sortable` annotations), the repository also provides:
- `query(ListQuery)` — `Task<ListResult>` (paginated, cached)
- `listSize()` — current L1 list cache entry count
- `warmup()` — primes both entity and list caches

### Template Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `Entity` | type | — | `EntityWrapper<Struct, Mapping>` |
| `Name` | `FixedString` | — | Compile-time string literal (repository name, Redis key prefix) |
| `Cfg` | `CacheConfig` | `config::Local` | NTTP aggregate configuring cache behavior |
| `Invalidations...` | types | — | Variadic cross-invalidation descriptors |

Key is auto-deduced from `Entity::key()` return type.

### Partition Key Repositories

For PostgreSQL partitioned tables with composite primary keys (e.g., `PRIMARY KEY (id, region) PARTITION BY LIST (region)`), the repository key can be a subset of the full PK. Annotate the partition column with `@relais partition_key`:

```cpp
// @relais table=events
// @relais output=entities/generated/EventWrapper.h
struct Event {
    int64_t id = 0;         // @relais primary_key db_managed
    std::string region;     // @relais partition_key
    std::string title;
};
```

The generator produces:
- `SQL::delete_by_pk`: `DELETE ... WHERE id = $1` (scans all partitions)
- `SQL::delete_by_full_pk`: `DELETE ... WHERE id = $1 AND region = $2` (single partition)
- `makeFullKeyParams(entity)`: extracts `(id, region)` as `PgParams`

The `HasPartitionKey` concept auto-detects these capabilities. At runtime, `erase(id)` uses an **opportunistic hint** pattern:

```
CachedRepo::erase(id)
  |-- hint = L1 cache lookup (~0ns, free)
  v
RedisRepo::eraseImpl(id, hint)
  |-- if no hint: try L2 Redis (~0.1ms)
  v
BaseRepo::eraseImpl(id, hint)
  |-- if hint: DELETE ... WHERE id=$1 AND region=$2  → 1 partition
  |-- else:    DELETE ... WHERE id=$1                → N partitions
```

**Performance rule**: never add a DB round-trip just for partition pruning. Only use the full key when the entity is free (L1) or near-free (L2).

```cpp
// Repo — no special configuration needed
using EventRepo = relais::Repo<EventWrapper, "Event", config::Both>;
// Key = int64_t (from key)
// HasPartitionKey auto-detected from Mapping
```

### 3. Use the repository

```cpp
io::Task<void> example() {
    // Find by ID (automatically uses cache)
    auto user = co_await UserRepo::find(123);
    if (user) {
        std::cout << "Found: " << user->username << "\n";
    }

    // insert (automatically caches result) - only if !read_only
    UserWrapper newUser;
    newUser.username = "alice";
    newUser.email = "alice@example.com";
    auto created = co_await UserRepo::insert(
        std::make_shared<const UserWrapper>(std::move(newUser)));

    // Update - only if !read_only
    UserWrapper modified = *created;
    modified.balance = 100;
    co_await UserRepo::update(created->id,
        std::make_shared<const UserWrapper>(std::move(modified)));

    // Partial update - only modifies specified fields (requires generated entity with Field enum)
    using F = UserWrapper::Field;
    using jcailloux::relais::wrapper::set;
    auto updated = co_await UserRepo::patch(created->id,
        set<F::balance>(999),
        set<F::username>("bob"));
    // updated is a re-fetched WrapperPtr with all fields populated

    // Delete - only if !read_only
    co_await UserRepo::erase(created->id);

    // Explicit invalidation (propagates to dependent caches)
    co_await UserRepo::invalidate(456);
}
```

## Data Format

Entities use Glaze for both binary (BEVE) and JSON serialization. Both formats are lazily computed on first access via `std::call_once` (thread-safe, lock-free fast path after initialization).

- **L1 (RAM)**: Stores `shared_ptr<const Entity>` directly
- **L2 (Redis)**: Stores BEVE binary data; deserialized on retrieval
- **L3 (PostgreSQL)**: Source of truth; entities are constructed via `fromRow()`

The struct's `glz::meta` specialization drives both BEVE and JSON serialization — no manual serialization code is needed.

### Custom JSON Field Names

By default, JSON and BEVE field names match the C++ member names (e.g., `user_id` in C++ -> `"user_id"` in JSON). To use custom names (e.g., camelCase for a REST API), define a `glz::meta<Struct>` specialization in the shared struct header:

```cpp
// entities/Product.h — shared between API and BEVE consumers
struct Product {
    int64_t id = 0;
    std::string product_name;
    int32_t unit_price = 0;
};

// Optional: custom JSON/BEVE field names
template<>
struct glz::meta<Product> {
    using T = Product;
    static constexpr auto value = glz::object(
        "id", &T::id,
        "productName", &T::product_name,   // camelCase in JSON/BEVE
        "unitPrice", &T::unit_price
    );
};
```

`EntityWrapper` automatically detects and uses `glz::meta<Struct>` when it exists. If no specialization is defined, the generated `Mapping::glaze_value` is used (member names as keys).

**Sharing the naming contract:** Since `glz::meta<Struct>` lives in the struct header (which is framework-agnostic), any C++ application that includes this header — whether the API or a BEVE consumer — uses the same field names. Both sides need matching `glz::meta` keys for BEVE interoperability.

## CacheConfig — Compile-Time Configuration

Configuration uses a **structural aggregate** passed as a Non-Type Template Parameter (NTTP). All fields have sensible defaults; only override what differs.

```cpp
namespace config = jcailloux::relais::config;

struct CacheConfig {
    CacheLevel cache_level = CacheLevel::None;
    bool read_only = false;
    UpdateStrategy update_strategy = UpdateStrategy::InvalidateAndLazyReload;

    // L1 (RAM cache)
    Duration l1_ttl = 1h;
    uint8_t l1_shard_count_log2 = 3;  // 2^3 = 8 shards
    bool l1_refresh_on_get = true;
    bool l1_accept_expired_on_get = true;
    size_t l1_cleanup_every_n_gets = 500;
    Duration l1_cleanup_min_interval = 30s;

    // L2 (Redis cache)
    Duration l2_ttl = 4h;
    bool l2_refresh_on_get = false;

    // Fluent chaining (consteval — compile-time only)
    consteval CacheConfig with_cache_level(CacheLevel v) const;
    consteval CacheConfig with_read_only(bool v = true) const;
    consteval CacheConfig with_update_strategy(UpdateStrategy v) const;
    consteval CacheConfig with_l1_ttl(Duration d) const;
    consteval CacheConfig with_l1_shard_count_log2(uint8_t v) const;
    consteval CacheConfig with_l1_refresh_on_get(bool v) const;
    consteval CacheConfig with_l1_accept_expired_on_get(bool v) const;
    consteval CacheConfig with_l1_cleanup_every_n_gets(size_t v) const;
    consteval CacheConfig with_l1_cleanup_min_interval(Duration v) const;
    consteval CacheConfig with_l2_ttl(Duration d) const;
    consteval CacheConfig with_l2_refresh_on_get(bool v) const;
};
```

`Duration` is a structural wrapper for `std::chrono::duration` (which has private members and cannot appear in NTTP aggregates). `FixedString` enables string literals as template parameters.

### Configuration Presets

| Preset | Cache Level | Use Case |
|--------|-------------|----------|
| `config::Uncached` | None | Write-only tables, audit logs |
| `config::Local` | L1 only | Guild/user data (same API instance) |
| `config::Redis` | L2 only | Shared metrics, cross-instance data |
| `config::Both` | L1 + L2 | High-read data, feature flags |

### Composing Presets

```cpp
// Start from a preset, override specific fields
inline constexpr auto ShortTtl = config::Local.with_l1_ttl(std::chrono::minutes{5});

// Read-only with custom TTL
inline constexpr auto ReadOnlyShort =
    config::Local.with_l1_ttl(std::chrono::minutes{5}).with_read_only();

// Use directly as template argument
using MyRepo = Repo<MyWrapper, "My", config::Local.with_l1_ttl(30min)>;
```

## Read-Only Repositories

Mark repositories as read-only to disable modification operations at compile-time:

```cpp
using AuditLogRepo = Repo<AuditLogWrapper, "AuditLog",
    config::Local.with_read_only()>;
// find() — available
// insert(), update(), erase() — COMPILE ERROR if called
```

This is enforced via `requires` clauses:
```cpp
static Task<bool> update(const Key& id, WrapperPtr wrapper)
    requires MutableEntity<Entity> && (!Cfg.read_only);
```

## Partial Updates with `patch`

Generated entities expose a `Field` enum for type-safe partial updates. Only the specified columns are written to the database (via dynamic `UPDATE ... SET` built from `FieldInfo::column_name`), and the full entity is re-fetched after the update.

```cpp
using F = UserWrapper::Field;
using jcailloux::relais::wrapper::set;
using jcailloux::relais::wrapper::setNull;

// Update a single field
auto updated = co_await UserRepo::patch(id, set<F::balance>(999));

// Update multiple fields
auto updated = co_await UserRepo::patch(id,
    set<F::balance>(999),
    set<F::username>("alice"));

// Set a nullable field to NULL
co_await ArticleRepo::patch(id, setNull<F::view_count>());
```

Requirements:
- The entity must be generated with a `TraitsType` that includes `Field` enum and `FieldInfo` specializations
- The repository must not be `read_only`
- Hand-written entities without `TraitsType` do not support `patch` (the `HasFieldUpdate` concept gates availability)

Cache handling:
- **CachedRepo**: L1 is invalidated before the update
- **RedisRepo**: L2 is invalidated before the update
- The re-fetched entity repopulates caches on subsequent `find` calls

### Partition Key `patch`

For partition key repositories, `patch` builds a dynamic `UPDATE ... WHERE pk=$N RETURNING *` using only the partial key. This is acceptable since the `id` column is indexed across all partitions. The `FieldInfo::column_name` is used to build the SET clause:

```cpp
auto updated = co_await EventRepo::patch(eventId,
    set<EF::title>(std::string("Updated")),
    set<EF::priority>(99));
// UPDATE events SET "title"=$1, "priority"=$2 WHERE "id"=$3 RETURNING *
```

## Cross-Invalidation System

Cross-invalidation is declared via the variadic `Invalidations...` pack on `Repo`. The `InvalidationMixin` sits at the top of the mixin chain and intercepts insert/update/erase to propagate invalidations to dependent caches.

### Declaring Dependencies

```cpp
using PurchaseRepo = Repo<PurchaseWrapper, "Purchase", config::Local,
    cache::Invalidate<UserStatsRepo, &Purchase::user_id>,   // Table -> Table
    cache::InvalidateList<PurchaseListRepo>                  // Table -> List
>;
```

### Four Invalidation Mechanisms

| Mechanism | Description | Use Case |
|-----------|-------------|----------|
| `Invalidate<Cache, KeyExtractor>` | Extract key from entity, invalidate target cache | Direct foreign key |
| `InvalidateList<ListRepo>` | Pass full entity to target list cache | Direct list invalidation |
| `InvalidateVia<Target, KeyExtractor, Resolver>` | Async resolver for indirect relationships | Junction tables |
| `InvalidateListVia<ListRepo, KeyExtractor, Resolver>` | Selective list page invalidation | Indirect list invalidation |

### Indirect Invalidation via Resolver

When the source entity doesn't contain the target cache's key directly (e.g., through a junction table), use `InvalidateVia` with an async resolver:

```cpp
// Resolver: query junction table to find target keys
struct UserToGuildsResolver {
    static io::Task<std::vector<int64_t>> resolve(int64_t user_id) {
        auto result = co_await DbProvider::queryArgs(
            "SELECT guild_id FROM guild_members WHERE user_id = $1", user_id);
        std::vector<int64_t> guild_ids;
        for (size_t i = 0; i < result.size(); ++i)
            guild_ids.push_back(result[i].get<int64_t>(0));
        co_return guild_ids;
    }
};

using UserRepo = Repo<UserWrapper, "User", config::Local,
    cache::InvalidateVia<GuildDetailRepo, &User::user_id, &UserToGuildsResolver::resolve>
>;
```

The resolver must return `Task<iterable<Key>>` (e.g., `Task<std::vector<int64_t>>`). If the target cache has its own `Invalidations...`, those cascade automatically.

### Selective List Invalidation via `InvalidateListVia`

For indirect list cache invalidation where a source entity change should selectively invalidate cached list pages, use `InvalidateListVia` with a typed resolver.

The target list repository defines a `GroupKey` type (typed filter values) and an `invalidateByTarget` method that translates these values into cache operations. The resolver returns `ListInvalidationTarget<GroupKey>` objects — this API is **cache-level agnostic** and works identically for L1 (RAM) and L2 (Redis).

```cpp
using Target = cache::ListInvalidationTarget<ArticleListRepo::GroupKey>;

struct PurchaseToArticleResolver {
    static io::Task<std::vector<Target>> resolve(int64_t user_id) {
        auto result = co_await DbProvider::queryArgs(
            "SELECT category, view_count FROM articles WHERE author_id = $1", user_id);

        std::vector<Target> targets;
        for (size_t i = 0; i < result.size(); ++i) {
            Target t;
            t.filters.category = result[i].get<std::string>(0);
            t.sort_value = result[i].get<int64_t>(1);
            targets.push_back(std::move(t));
        }
        co_return targets;
    }
};

using PurchaseRepo = Repo<PurchaseWrapper, "Purchase", config::Local,
    cache::InvalidateListVia<ArticleListRepo, &Purchase::user_id,
        &PurchaseToArticleResolver::resolve>
>;
```

**Three invalidation granularities:**

| Granularity | Resolver returns | Effect |
|-------------|-----------------|--------|
| **Per-page** | `sort_value` present | Only pages whose sort range includes the value |
| **Per-group** | `sort_value` absent (`nullopt`) | All pages in the targeted group |
| **Full pattern** | Resolver returns `std::nullopt` | All groups in the repository |

Each cached list page in Redis is prefixed with a 19-byte binary header containing sort bounds metadata. For per-page granularity, the Lua script reads these headers atomically (`GETRANGE`) and only deletes pages whose sort range is affected — all in a single Redis round-trip.

### Propagation Behavior

**Modification operations** (`insert`, `update`, `erase`):
- Fetch old value before operation (for updates/deletes)
- Perform the database operation
- Propagate to dependent caches with old/new entity data

**Explicit invalidation** (`invalidate(id)`):
- Fetches entity for propagation
- Invalidates all cache tiers
- Propagates to dependent caches

## List Cache System — ListMixin

The list cache provides paginated query results with lazy validation via modification tracking. It is automatically activated when the entity's Mapping has an embedded `ListDescriptor` (detected via the `HasListDescriptor` concept).

### Defining a List Entity

Annotate fields with `filterable` and `sortable` in the struct header. The generator embeds the `ListDescriptor` inside the Mapping:

```cpp
// @relais table=audit_logs
// @relais output=entities/generated/AuditLogWrapper.h
// @relais_list limits=10,25,50,100
struct AuditLog {
    int64_t id = 0;            // @relais primary_key db_managed sortable:asc
    int64_t guild_id = 0;      // @relais filterable
    int64_t user_id = 0;       // @relais filterable
    std::string module;        // @relais filterable
    std::string action_type;   // @relais filterable
    std::string created_at;    // @relais timestamp sortable:desc
};

// Generated in the Mapping:
//   struct ListDescriptor {
//       static constexpr auto filters = std::tuple{
//           Filter<"guild_id", &AuditLog::guild_id, "guild_id">{}, ...};
//       static constexpr auto sorts = std::tuple{
//           Sort<"id", &AuditLog::id, "id", SortDirection::Asc>{}, ...};
//       static constexpr uint16_t defaultLimit = 10;
//       static constexpr uint16_t maxLimit = 100;
//   };
```

### Using List Queries

```cpp
// Repo — one line, list support auto-detected
using AuditLogRepo = Repo<AuditLogWrapper, "AuditLog">;

// In controller:
#include <jcailloux/relais/list/decl/HttpQueryParser.h>

// parseListQueryStrict takes a generic Map (default: unordered_map<string, string>)
io::Task<std::string> handleAuditLogList(
    const std::unordered_map<std::string, std::string>& params)
{
    // Parse and validate query parameters against the ListDescriptor
    auto query_result = parseListQueryStrict<AuditLogRepo::ListDescriptorType>(params);
    if (!query_result) {
        // Handle validation error (query_result.error())...
    }

    // Execute paginated query (L1 cached with lazy invalidation)
    auto result = co_await AuditLogRepo::query(std::move(*query_result));

    // result.json() returns shared_ptr<const std::string>
    co_return *result.json();
}
```

### CRUD-to-List Notification

ListMixin intercepts `insert()`, `update()`, `erase()`, and `patch()` to automatically notify the list cache of entity changes. The ModificationTracker records these changes, and list cache entries are validated lazily on the next `query()` call.

No manual `notifyCreated`/`notifyUpdated`/`notifyDeleted` calls are needed for same-repo entities.

### Modification Tracking

Entity changes are tracked via `ModificationTracker`:

```cpp
// Automatically called by ListMixin's CRUD interception:
listCache().onEntityCreated(entity);
listCache().onEntityUpdated(old_entity, new_entity);
listCache().onEntityDeleted(entity);
```

Modifications are validated lazily on `get()` — cache entries affected by recent modifications are automatically invalidated.

## Python Entity Mapping Generator

Generates standalone ORM Mapping structs from `@relais` annotations in C++ struct headers.

### Annotations

Annotations are inline comments on struct declarations and data members:

```cpp
// @relais table=audit_logs
// @relais output=entities/generated/AuditLogWrapper.h
// @relais_list limits=10,25,50,100
struct AuditLog {
    int64_t id = 0;            // @relais primary_key db_managed sortable:asc
    int64_t guild_id = 0;      // @relais filterable
    std::string module;        // @relais filterable
    std::string action_type;   // @relais filterable
    std::string created_at;    // @relais timestamp sortable:desc
};
```

| Annotation (struct-level) | Description |
|--------------------------|-------------|
| `table=table_name` | PostgreSQL table name |
| `output=path/to/Wrapper.h` | Generated file path (relative to `--output-dir`) |
| `read_only` | Mark entity as read-only |

| Annotation (field-level) | Description |
|--------------------------|-------------|
| `primary_key` | Marks the primary key field |
| `db_managed` | Excluded from `toInsertParams` (auto-generated by DB) |
| `timestamp` | Timestamp field — stored as `std::string` (ISO 8601 format) |
| `nullable` | `std::optional<T>` handling with `setNull` |
| `raw_json` | `glz::raw_json_t` — stored as raw string in DB (optional: auto-detected from type) |
| `json_field` | Struct/vector stored as JSON in DB |
| `enum` | Auto-resolve DB <-> enum mapping from `glz::meta<EnumType>` in source header |
| `enum=val1:Enum1,val2:Enum2` | Explicit string DB <-> enum C++ mapping (overrides `glz::meta`) |
| `filterable` | Declares field as a list filter (see below) |
| `sortable` | Declares field as a list sort (see below) |
| `partition_key` | Marks field as partition column for full-key DELETE optimization |

#### Declarative list annotations

Filter and sort capabilities are declared per-field. Pagination limits stay at class level via `@relais_list`:

```cpp
// @relais table=articles
// @relais output=entities/generated/ArticleWrapper.h
// @relais_list limits=10,25,50
struct Article {
    int64_t id = 0;            // @relais primary_key db_managed
    std::string category;       // @relais filterable
    int64_t author_id = 0;     // @relais filterable
    int32_t view_count = 0;    // @relais sortable:desc
    std::string created_at;    // @relais timestamp sortable:desc
};
```

**`filterable` syntax:**

| Annotation | HTTP param | Operator |
|------------|-----------|----------|
| `filterable` | field name | EQ (default) |
| `filterable:custom_name` | `custom_name` | EQ |
| `filterable:ge` | field name | GE (known operator) |
| `filterable:date_from:ge` | `date_from` | GE |

Known operators: `eq`, `ne`, `gt`, `ge`/`gte`, `lt`, `le`/`lte`.

Multiple filters on the same field (e.g. range queries):
```cpp
std::string created_at;  // @relais timestamp filterable:date_from:gte filterable:date_to:lte sortable:desc
```

**`sortable` syntax:**

| Annotation | HTTP param | Direction |
|------------|-----------|-----------|
| `sortable` | field name | DESC (default) |
| `sortable:asc` | field name | ASC |
| `sortable:desc` | field name | DESC |
| `sortable:custom_name:asc` | `custom_name` | ASC |

Column names are derived automatically from the field name.

### Generated Output

The generator produces standalone Mapping structs with template `fromRow<Entity>` / `toInsertParams<Entity>` / `key<Entity>` methods. These are used by `EntityWrapper<Struct, Mapping>`.

For each entity:
- **Mapping struct**: SQL strings, `TraitsType`, `FieldInfo` specializations, `glaze_value`
- **Partition key entities** (`partition_key` annotation): `SQL::delete_by_full_pk` + `makeFullKeyParams()` for single-partition DELETE
- **List entities**: Embedded `ListDescriptor` struct (filters, sorts, limits) inside the Mapping
- **EntityWrapper alias**: `using XxxWrapper = EntityWrapper<Struct, Mapping>;`
- **ListWrapper alias** (if list): `using XxxListWrapper = ListWrapper<XxxWrapper>;`

### Usage

```bash
# Scan a directory for @relais annotations
python scripts/generate_entities.py --scan src/entities/ --output-dir src/

# Or specify files directly
python scripts/generate_entities.py --files src/entities/User.h --output-dir src/
```

## API Reference

### Repo Methods

| Method | Return Type | Constraint | Description |
|--------|-------------|------------|-------------|
| `find(id)` | `Task<WrapperPtr>` | - | Find by primary key (cached) |
| `findJson(id)` | `Task<shared_ptr<const string>>` | - | Find and return JSON directly |
| `insert(wrapper)` | `Task<WrapperPtr>` | `!read_only` | Insert and cache |
| `update(id, wrapper)` | `Task<bool>` | `!read_only` | Full update and handle cache |
| `patch(id, set<F>()...)` | `Task<WrapperPtr>` | `!read_only`, `HasFieldUpdate` | Partial update, re-fetches entity |
| `erase(id)` | `Task<optional<size_t>>` | `!read_only` | Delete and invalidate cache |
| `invalidate(id)` | `Task<void>` | - | Explicit cache invalidation |
| `updateJson(id, json)` | `Task<bool>` | `!read_only` | Parse JSON and update |
| `updateBinary(id, data)` | `Task<bool>` | `!read_only`, `HasBinarySerialization` | Parse binary and update |

### CachedRepo Additional Methods

| Method | Description |
|--------|-------------|
| `trySweep()` | Try to cleanup expired entries (non-blocking) |
| `purge()` | Force cleanup of all expired entries |
| `size()` | Current L1 cache entry count |
| `warmup()` | Prime cache structures at startup |

### ListMixin Methods (auto-detected)

| Method | Description |
|--------|-------------|
| `query(ListQuery)` | Execute paginated list query (L1 cached) |
| `listSize()` | Current list cache entry count |
| `trySweep()` | Cleanup entity + list L1 caches (non-blocking) |
| `purge()` | Full cleanup entity + list L1 caches (blocking) |
| `warmup()` | Primes both entity and list caches |
| `ListDescriptorType` | Type alias for `parseListQueryStrict<>()` |

### InvalidationMixin (auto-activated)

When `Invalidations...` is non-empty, the mixin intercepts `insert()`, `update()`, `erase()`, `patch()`, and `invalidate()` to propagate cross-invalidation. No additional API — it wraps the existing methods transparently.

## Testing

Build and run tests:

```bash
cmake -B build -DRELAIS_BUILD_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

### Test Files

| File | Coverage |
|------|----------|
| `test_base_repository.cpp` | BaseRepo (L3): CRUD, edge cases, read-only, uncached list queries |
| `test_redis_repository.cpp` | RedisRepo (L2): CRUD caching, cross-invalidation, list caching, selective Lua invalidation, InvalidateListVia |
| `test_cached_repository.cpp` | CachedRepo (L1+L2): RAM cache, TTL, config variants |
| `test_decl_list_cache.cpp` | ListMixin: query/ItemView, SortBounds invalidation, ModificationTracker |
| `test_partition_key.cpp` | Partition key repositories: composite PK, criteria-based patch, erase with cache hints, cross-invalidation |

### Running specific test tags

```bash
# Run only L2 selective invalidation tests
./test_relais_redis "[list-selective]"

# Run only enriched resolver tests
./test_relais_redis "[list-resolver]"

# Run only base list tests
./test_relais_base "[list]"
```

## Further Reading

For implementation details and contribution guidelines, see [INTERNALS.md](INTERNALS.md).

## License

MIT License — see [LICENSE](LICENSE) file.
