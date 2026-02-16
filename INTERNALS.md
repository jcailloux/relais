# Internals

This document describes the internal architecture of relais for contributors and advanced users.

## Repository Mixin Chain

The library uses a **mixin architecture with method hiding** for compile-time composition. No CRTP — no `Derived` template parameter. Each layer calls `Base::method()` downward.

```
BaseRepo<Entity, Name, Cfg, Key>
       |
       |  (adds Redis caching — if L2 or L1_L2)
       v
RedisRepo<Entity, Name, Cfg, Key>
       |
       |  (adds RAM caching — if L1 or L1_L2)
       v
CachedRepo<Entity, Name, Cfg, Key>
       |
       |  (adds list caching — if Entity has ListDescriptor)
       v
ListMixin<CacheLayer>
       |
       |  (adds cross-invalidation — if Invalidations... non-empty)
       v
InvalidationMixin<WithList, Invalidations...>
       |
       v
Repo<Entity, Name, Cfg, Invalidations...>  (final class)
```

### Template Parameters

All layers share the same four template parameters:

```cpp
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
```

- **Entity**: `EntityWrapper<Struct, Mapping>` type
- **Name**: `FixedString` NTTP — compile-time string literal for the repository name and Redis key prefix
- **Cfg**: `CacheConfig` NTTP — structural aggregate (all fields are public structural types)
- **Key**: Auto-deduced from `decltype(std::declval<const Entity>().getPrimaryKey())`

No `Config` struct, no `typename Derived`, no separate ORM model type.

### Method Hiding

Each mixin layer can override methods from its `Base` by declaring a method with the same name. The upper method calls `Base::method()` explicitly to delegate. This is standard C++ name hiding — no virtuals, no CRTP.

Example: `ListMixin::update()` hides `CachedRepo::update()`:

```cpp
template<typename Base>
class ListMixin : public Base {
    static io::Task<bool> update(const Key& id, WrapperPtrType wrapper) {
        auto old = co_await Base::find(id);     // cache-aware
        bool ok = co_await Base::update(id, wrapper); // delegates down
        if (ok) listCache().onEntityUpdated(old, wrapper);
        co_return ok;
    }
};
```

### RepoBuilder

`Repo.h` contains the `RepoBuilder` that auto-assembles the mixin chain:

```cpp
namespace detail {

/// Select the cache layer based on CacheConfig::cache_level
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
struct CacheLayerSelector {
    using type = std::conditional_t<
        Cfg.cache_level == CacheLevel::L1 || Cfg.cache_level == CacheLevel::L1_L2,
        CachedRepo<Entity, Name, Cfg, Key>,
        std::conditional_t<
            Cfg.cache_level == CacheLevel::L2,
            RedisRepo<Entity, Name, Cfg, Key>,
            BaseRepo<Entity, Name, Cfg, Key>
        >
    >;
};

/// Stack optional mixins on top of the cache layer
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key,
         typename... Invalidations>
struct MixinStack {
    using CacheLayer = typename CacheLayerSelector<Entity, Name, Cfg, Key>::type;

    using WithList = std::conditional_t<
        HasListDescriptor<Entity>,
        ListMixin<CacheLayer>,
        CacheLayer
    >;

    using type = std::conditional_t<
        sizeof...(Invalidations) > 0,
        InvalidationMixin<WithList, Invalidations...>,
        WithList
    >;
};

}  // namespace detail
```

The `Repo<>` class inherits from `MixinStack::type` and adds:
- Compile-time `static_assert` validation (L1/L2 TTL > 0, segments >= 2, concept checks)
- Convenience methods (`updateFromJson`, `updateFromBinary`)
- Re-export of base type aliases (`EntityType`, `KeyType`, `MappingType`, etc.)

### Cache Layer Selection

| `Cfg.cache_level` | Selected Base |
|--------------------|---------------|
| `CacheLevel::None` | `BaseRepo` |
| `CacheLevel::L2` | `RedisRepo` |
| `CacheLevel::L1` | `CachedRepo` (inherits `BaseRepo`) |
| `CacheLevel::L1_L2` | `CachedRepo` (inherits `RedisRepo`) |

### Conditional Inheritance in CachedRepo

`CachedRepo` uses `std::conditional_t` to choose its base class at compile time:

```cpp
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
class CachedRepo : public std::conditional_t<
    Cfg.cache_level == CacheLevel::L1,
    BaseRepo<Entity, Name, Cfg, Key>,      // L1-only: skip Redis
    RedisRepo<Entity, Name, Cfg, Key>       // L1_L2: include Redis layer
> { ... };
```

## Configuration System — CacheConfig NTTP

All configuration lives in a single structural aggregate, `CacheConfig`, passed as a Non-Type Template Parameter:

```cpp
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
    consteval CacheConfig with_l1_ttl(Duration d) const { auto c = *this; c.l1_ttl = d; return c; }
    consteval CacheConfig with_read_only(bool v = true) const { auto c = *this; c.read_only = v; return c; }
    // ... one with_* per field
};
```

**Structural types only:** All fields are enum, bool, `size_t`, or `Duration` — all structural in C++23, so `CacheConfig` can appear as an NTTP.

**`Duration`:** A wrapper for `std::chrono::duration` (which has private members and is not structural). Stores nanoseconds as `int64_t`. Implicitly convertible from any `std::chrono::duration`, explicitly convertible to `std::chrono::nanoseconds`.

**`FixedString`:** Allows passing string literals as template arguments. `FixedString<N>` stores `char[N]` and is implicitly convertible to `const char*`.

**Presets:**

```cpp
inline constexpr CacheConfig Uncached{};
inline constexpr CacheConfig Local{ .cache_level = CacheLevel::L1 };
inline constexpr CacheConfig Redis{ .cache_level = CacheLevel::L2, .l2_ttl = 4h };
inline constexpr CacheConfig Both{ .cache_level = CacheLevel::L1_L2, .l1_ttl = 1min, .l2_ttl = 1h };
```

**Composable:** `config::Local.with_l1_ttl(30min).with_read_only()`

**Accessing config in layers:** All layers use `Cfg.field` directly (e.g., `Cfg.l1_ttl`, `Cfg.read_only`). The repository name comes from the `Name` template parameter: `static constexpr const char* name() { return Name; }`.

## L1 Cache: shardmap Integration

The L1 (RAM) cache uses [shardmap](../jcailloux-shardmap), a thread-safe concurrent hash map with TTL-based expiration and callback-based validation.

### Cache Storage with Metadata

```cpp
// CachedRepo.h
struct EntityCacheMetadata {
    std::atomic<int64_t> expiration_rep{0};  // Atomic for shared-lock safety
    // ... constructor from time_point, expiration()/setExpiration() accessors
};

static constexpr shardmap::ShardMapConfig kShardMapConfig{
    .shard_count_log2 = Cfg.l1_shard_count_log2
};
using L1Cache = shardmap::ShardMap<Key, EntityPtr, EntityCacheMetadata, kShardMapConfig>;

static L1Cache& cache() {
    static L1Cache instance;
    return instance;
}
```

### Callback-Based TTL Validation

Cache entries are validated on `get()` via a callback that can Accept, Reject, or Invalidate:

```cpp
static EntityPtr getFromCache(const Key& key) {
    return cache().get(key, [](const EntityPtr&, EntityCacheMetadata& meta) {
        const auto now = Clock::now();

        // TTL check
        if constexpr (!Cfg.l1_accept_expired_on_get) {
            if (meta.expiration_date < now) {
                return shardmap::GetAction::Invalidate;
            }
        }

        // Optional TTL refresh (sliding window)
        if constexpr (Cfg.l1_refresh_on_get) {
            meta.expiration_date = now + l1Ttl();
        }

        return shardmap::GetAction::Accept;
    });
}
```

### Cleanup with Context

Cleanup uses a context object and callback to determine which entries to remove:

```cpp
struct CleanupContext {
    Clock::time_point now;
};

static bool triggerCleanup() {
    CleanupContext ctx{Clock::now()};
    return cache().try_cleanup(ctx, [](const Key&, const EntityPtr&,
                                        const EntityCacheMetadata& meta,
                                        const CleanupContext& ctx) {
        return meta.expiration_date < ctx.now;  // true = remove
    });
}
```

## L2 Cache: Redis Integration

Redis caching uses the custom async Redis client (`io::RedisClient`) with coroutine support.

### Key Format

```cpp
static std::string makeRedisKey(const Key& id) {
    return std::string(name()) + ":" + std::to_string(id);
    // Example: "User:123"
}
```

### TTL Refresh with GETEX

When `Cfg.l2_refresh_on_get = true`, uses Redis 6.2+ `GETEX` command:

```cpp
co_await redis->execCommandCoro("GETEX %s EX %lld", key, ttl_seconds);
```

## Cache Flow on Operations

### Read: `find(id)`

```
L1 Cache Hit? --yes--> Return shared_ptr
      |no
      v
L2 Cache Hit? --yes--> Deserialize --> Store in L1 --> Return
      |no
      v
Database Query --> Store in L2 --> Store in L1 --> Return
```

### Delete: `remove(id)`

For standard repositories:
```
CachedRepo::remove(id)
    |-- invalidateL1Internal(id)
    v
RedisRepo::remove(id)
    |-- co_await BaseRepo::remove(id)  -> deleteByPrimaryKey(id)
    |-- co_await invalidateRedisInternal(id)
```

For PartialKey repositories, `remove` uses an **opportunistic hint** pattern — cache layers pass any already-available entity to the base layer to enable full PK deletion (partition pruning):

```
CachedRepo::remove(id)
    |-- hint = getFromCache(id)         // Free L1 check (~0ns)
    |
    v
RedisRepo::removeImpl(id, hint)
    |-- if (!hint && PartialKey):
    |     hint = co_await getFromCache(redisKey)  // L2 check (~0.1-1ms)
    |
    v
BaseRepo::removeImpl(id, hint)
    |-- if (hint):
    |     params = Entity::makeFullKeyParams(*hint)
    |     DELETE ... WHERE id=$1 AND region=$2     // Full composite PK -> 1 partition
    |-- else:
    |     DELETE ... WHERE id=$1                   // Partial key -> N partitions
```

**Performance rule**: Never add a DB round-trip just for partition pruning. Only use full PK when entity is free (L1) or near-free (L2 ~0.1ms).

### Write: `update(id, wrapper)`

Two strategies controlled by `Cfg.update_strategy`:

**InvalidateAndLazyReload** (safe, default):
```
Fetch old entity (for cross-invalidation)
      |
      v
Database Update --> Invalidate L1 --> Invalidate L2
      |
      v
Propagate cross-invalidations with old/new data
```

**PopulateImmediately** (optimistic):
```
Fetch old entity (for cross-invalidation)
      |
      v
Database Update --> Write to L1 --> Write to L2
      |
      v
Propagate cross-invalidations with old/new data
```

## ListMixin — Automatic List Caching

The `ListMixin` is an optional layer activated when `Entity::MappingType::ListDescriptor` exists (detected via the `HasListDescriptor` concept). It sits between the cache layer and `InvalidationMixin`.

### Detection

```cpp
template<typename Entity>
concept HasListDescriptor = requires { typename Entity::MappingType::ListDescriptor; };
```

The `ListDescriptor` is embedded in the generated Mapping struct and contains filters, sorts, allowed limits, and default/max limits as compile-time data.

### Augmented Descriptor

The embedded `ListDescriptor` doesn't carry the `Entity` type alias (it only references member pointers and column names). `ListMixin` creates an augmented descriptor:

```cpp
struct Descriptor : Entity::MappingType::ListDescriptor {
    using Entity = ListMixin::Entity;
};
```

This augmented descriptor is exposed as `ListDescriptorType` for use with `parseListQueryStrict<>()`.

### Two Separate Cache Instances

ListMixin uses **two independent shardmap instances** — one for entities, one for lists:

- **Entity cache** (from CachedRepo): `shardmap::ShardMap<Key, EntityPtr, EntityCacheMetadata>` — keys = PK values
- **List cache** (from ListMixin): `shardmap::ShardMap<size_t, ResultPtr, ListCacheMetadata>` — keys = XXH3 hash of query

These are different template instantiations, so they are distinct static objects with independent `KeyTracker`, cleanup state, and segments. No collision is possible.

### List Cache Storage

```cpp
using ListCacheType = cache::list::ListCache<Entity, int64_t, Traits>;

static ListCacheType& listCache() {
    static ListCacheType instance(listCacheConfig());
    return instance;
}
```

The `ListCacheConfig` is derived from the entity `CacheConfig`:

```cpp
static cache::list::ListCacheConfig listCacheConfig() {
    return {
        // ShardMap shard count is configured via l1_shard_count_log2 NTTP
        .cleanup_every_n_gets = Cfg.l1_cleanup_every_n_gets,
        .default_ttl = duration_cast<seconds>(nanoseconds(Cfg.l1_ttl)),
        .accept_expired_on_get = Cfg.l1_accept_expired_on_get,
        .refresh_on_get = Cfg.l1_refresh_on_get,
    };
}
```

### Traits Adapter

`ListMixin` provides a `Traits` struct that bridges the declarative filter/sort helpers to the `ListCache` interface. It adapts between the two `SortDirection` enums (`cache::list::decl::SortDirection` vs `cache::list::SortDirection`) and delegates to compile-time descriptor helpers:

```cpp
struct Traits {
    using Filters = DescriptorFilters;
    using SortField = size_t;

    static bool matchesFilters(const Entity& e, const Filters& f);
    static int compare(const Entity& a, const Entity& b, SortField, SortDirection);
    static cache::list::Cursor extractCursor(const Entity&, const SortSpec<size_t>&);
    static constexpr SortSpec<size_t> defaultSort();
    static uint16_t normalizeLimit(uint16_t requested);
    // ...
};
```

### CRUD Interception

`ListMixin` intercepts `create()`, `update()`, `remove()`, and `updateBy()` to notify the list cache of entity changes:

```cpp
static io::Task<bool> update(const Key& id, WrapperPtrType wrapper) {
    auto old = co_await Base::find(id);
    bool ok = co_await Base::update(id, wrapper);
    if (ok) listCache().onEntityUpdated(old, wrapper);
    co_return ok;
}
```

The `ModificationTracker` records these changes. List cache entries are validated lazily on the next `query()` call via the shardmap `get()` callback.

### Cross-Invalidation Entry Points

For external cross-invalidation (via `InvalidateList`), `ListMixin` exposes:

```cpp
static void notifyCreated(WrapperPtrType entity);
static void notifyUpdated(WrapperPtrType old, WrapperPtrType new_entity);
static void notifyDeleted(WrapperPtrType entity);
```

### DB Query Path

`queryFromDb` returns rows from PostgreSQL via `DbProvider::queryParams`. Sort bounds are extracted from the entities built from these rows:

```cpp
auto entities = co_await queryFromDb(query);

// Extract bounds from entities
bounds.first_value = extractSortValue<Descriptor>(entities.front(), sort.field);
bounds.last_value  = extractSortValue<Descriptor>(entities.back(), sort.field);
```

## InvalidationMixin — Cross-Repository Invalidation

Optional layer activated when the `Invalidations...` pack is non-empty. Sits at the top of the mixin chain, intercepts CRUD operations, and propagates invalidations via `InvalidateOn<Invalidations...>`.

### Structure

```cpp
template<typename Base, typename... Invalidations>
class InvalidationMixin : public Base {
    using InvList = cache::InvalidateOn<Invalidations...>;
    // ...
};
```

### Interception Pattern

Each CRUD method fetches the old entity (for propagation data), delegates to `Base`, then propagates:

```cpp
static io::Task<bool> update(const Key& id, WrapperPtrType wrapper) {
    auto old = co_await Base::find(id);  // cache-aware
    bool ok = co_await Base::update(id, std::move(wrapper));
    if (ok) co_await cache::propagateUpdate<Entity, InvList>(old, wrapper);
    co_return ok;
}
```

`Base::find()` resolves through the full cache chain (L1 -> L2 -> DB), so the old entity lookup is typically a cache hit.

### Four Invalidation Mechanisms

```cpp
// Declared as Invalidations... variadic pack on Repo:
cache::Invalidate<TargetCache, &Entity::foreign_key>          // table -> table (simple)
cache::InvalidateList<ListRepo>                                // table -> list (full entity)
cache::InvalidateVia<Target, &Entity::key, &Resolver::resolve> // table -> table (async resolver)
cache::InvalidateListVia<ListRepo, &Entity::key, &Resolver::resolve> // table -> list (selective)
```

### Propagation Functions

`InvalidateOn<Deps...>` uses fold expressions to dispatch to each dependency:

- `propagateCreate<Entity, InvList>(new_entity)` — called after `create()`
- `propagateUpdate<Entity, InvList>(old, new_entity)` — called after `update()`/`updateBy()`
- `propagateDelete<Entity, InvList>(old_entity)` — called after `remove()`/`invalidate()`

### Indirect Invalidation: `InvalidateVia`

For indirect relationships through junction tables:

```
Source entity modified
  -> extractKey(entity) -> source_key
  -> co_await Resolver(source_key) -> [target_key_1, target_key_2, ...]
  -> co_await TargetCache::invalidate(target_key_1)
  -> co_await TargetCache::invalidate(target_key_2)
  -> ...
```

**Cascade:** `TargetCache::invalidate(key)` calls `propagate=true` by default, so the target's own `Invalidations...` are triggered automatically. Multi-hop chains work:

```
C modified -> InvalidateVia resolves c_id -> [a_id]
  -> RepoA::invalidate(a_id, propagate=true)
    -> RepoA::Invalidations... -> Invalidate<RepoD, &A::d_id>
      -> RepoD::invalidate(d_id, propagate=true)
```

### Selective List Invalidation: `InvalidateListVia`

For fine-grained Lua-based list page invalidation:

```
Source entity modified
  -> extractKey(entity) -> source_key
  -> co_await Resolver(source_key) -> [{group_key, sort_val}, ...]
  -> co_await ListRepo::invalidateListGroupByKey(group_key, sort_val)
```

Each `invalidateListGroupByKey` executes a Lua script that reads the 19-byte binary header of each cached page (via `GETRANGE 0 18`) to check if the sort value falls within the page's bounds.

## List Cache Architecture

### File Structure

```
list/
├── ListCache.h              # Core cache, SortBounds, PaginationMode, ListBoundsHeader
├── ListCacheRepo.h    # Low-level mixin (used by ListMixin internally)
├── ListCacheTraits.h        # Traits concepts for filter/sort
├── ListQuery.h              # Query and result types
├── ModificationTracker.h    # Unified modification tracking
└── decl/
    ├── ListMixin.h          # High-level mixin (in relais/ namespace, part of mixin chain)
    ├── ListDescriptor.h     # HasListDescriptor concept, descriptor requirements
    ├── ListDescriptorQuery.h # ListDescriptorQuery<Descriptor> — query type for parsers
    ├── HttpQueryParser.h    # parseListQueryStrict<Descriptor>(req)
    ├── FilterDescriptor.h   # Filter<Name, MemberPtr, ColPtr> template
    ├── SortDescriptor.h     # Sort<Name, EntityPtr, ColPtr, ModelPtr, Dir> template
    ├── GeneratedFilters.h   # buildCriteria, matchesFilters, extractTags
    ├── GeneratedTraits.h    # extractSortValue, extractCursor, compare
    └── GeneratedCriteria.h  # sortColumnName, parseSortField
```

### ListCache Storage

ListCache uses shardmap with metadata for lazy validation:

```cpp
template<typename FilterSet, typename SortFieldEnum>
struct ListCacheMetadata {
    ListQuery<FilterSet, SortFieldEnum> query;
    std::chrono::steady_clock::time_point cached_at;
    SortBounds sort_bounds;
    uint16_t result_count;
};

template<typename Entity, typename Key, typename Traits>
class ListCache {
    using CacheKey = size_t;  // Hash of query
    using Metadata = ListCacheMetadata<...>;

    shardmap::ShardMap<CacheKey, ResultPtr, Metadata> cache_;
    ModificationTracker<Entity> modifications_;
};
```

### Lazy Validation on Get

Cache entries are validated lazily when retrieved:

```cpp
ResultPtr get(const Query& query) {
    if (++get_counter_ % config_.cleanup_every_n_gets == 0) {
        triggerCleanup();
    }

    return cache_.get(query.hash(), [this, &query](const ResultPtr& result, Metadata& meta) {
        const auto now = Clock::now();

        // 1. TTL check
        if (!config_.accept_expired_on_get) {
            if (meta.cached_at + config_.default_ttl < now) {
                return shardmap::GetAction::Invalidate;
            }
        }

        // 2. Lazy validation against modifications
        if (isAffectedByModifications(meta.cached_at, meta.sort_bounds, *result, query)) {
            return shardmap::GetAction::Invalidate;
        }

        // 3. Optional TTL refresh
        if (config_.refresh_on_get) {
            meta.cached_at = now;
        }

        return shardmap::GetAction::Accept;
    });
}
```

### SortBounds — O(1) Range Checking

For efficient invalidation, list cache entries store numeric bounds of the sort field:

```cpp
struct SortBounds {
    int64_t first_value{0};   // Sort field value for first item
    int64_t last_value{0};    // Sort field value for last item
    bool is_valid{false};

    [[nodiscard]] bool isValueInRange(
        int64_t value, bool is_first_page,
        bool is_incomplete, bool is_descending) const noexcept;
};
```

Range checking rules by page type:

| Page Type | ASC Rule | DESC Rule |
|-----------|----------|-----------|
| Single incomplete | Always in range | Always in range |
| First page (complete) | `value <= last` | `value >= last` |
| Middle page | `first <= value <= last` | `last <= value <= first` |
| Last page (incomplete) | `value >= first` | `value <= first` |

### ListBoundsHeader — Redis L2 Binary Header

For Redis L2 list caching, each cached page value is prefixed with a 19-byte binary header. This enables atomic Lua-based selective invalidation: the Lua script reads headers via `GETRANGE` without deserializing the full payload.

**Format (19 bytes):**

| Offset | Size | Field |
|--------|------|-------|
| 0 | 2 | Magic bytes: `0x52 0x4C` ("SR" = Relais) |
| 2 | 8 | `first_value` (int64_t, little-endian) |
| 10 | 8 | `last_value` (int64_t, little-endian) |
| 18 | 1 | Flags byte (see below) |

**Flags byte:**

| Bit | Meaning |
|-----|---------|
| 0 | `sort_direction` (0=ASC, 1=DESC) |
| 1 | `is_first_page` |
| 2 | `is_incomplete` (fewer items than limit) |
| 3 | `pagination_mode` (0=Offset, 1=Cursor) |
| 4-7 | Reserved |

**Backward compatibility:** If the first 2 bytes != `0x52 0x4C`, the value is treated as old format (no header) and always invalidated conservatively.

### Lua-Based Selective Invalidation

Two Lua scripts execute atomically within Redis (~100-200us for 10 pages):

**Create/Delete script** (`invalidateListGroupSelective`):
```
SMEMBERS {groupKey}:_keys  -> [page_key_1, page_key_2, ...]
For each page_key:
  GETRANGE page_key 0 18   -> 19-byte header
  Decode header, check if entity_sort_val is in range
  If affected: DEL page_key, SREM from tracking set
Return count of deleted pages
```

**Update script** (`invalidateListGroupSelectiveUpdate`):
```
Same as above, but checks interval overlap:
  [page_min, page_max] intersect [min(old,new), max(old,new)]
  For offset mode: cascade from affected segment
  For cursor mode: localized check
```

**Invalidation modes by pagination:**

| Mode | Create/Delete | Update |
|------|---------------|--------|
| Offset | Affected segment + all after (cascade) | Interval overlap `[min(old,new), max(old,new)]` |
| Cursor | Only affected segment(s) (localized) | Only affected segment(s) |

### List Method Hierarchy

List query methods exist at all repository levels for config-level switching:

| Method | BaseRepo | RedisRepo |
|--------|---------------|-----------------|
| `cachedList(query, keyParts...)` | Pass-through (executes query) | Redis-cached |
| `cachedListTracked(query, limit, offset, groupParts...)` | Pass-through | Redis-cached + tracked |
| `cachedListTrackedWithHeader(query, limit, offset, headerBuilder, groupParts...)` | Pass-through | Redis-cached + tracked + header |
| `cachedListAs<ListEntity>(query, keyParts...)` | Pass-through | Redis-cached (binary) |
| `invalidateListGroup(groupParts...)` | No-op (returns 0) | Full group invalidation |
| `invalidateListGroupSelective(sortVal, groupParts...)` | No-op (returns 0) | Lua selective invalidation |
| `invalidateListGroupSelectiveUpdate(oldVal, newVal, groupParts...)` | No-op (returns 0) | Lua selective invalidation |
| `makeGroupKey(groupParts...)` | Key generation | Key generation |
| `invalidateListGroupByKey(groupKey, sortVal)` | No-op (returns 0) | Lua selective invalidation |

This ensures switching `Cfg.cache_level` from `L2` to `None` doesn't break compilation.

### Sort Value Encoding Limitation

Sort values are encoded as `int64_t` via `toInt64ForCursor()`. This works for:
- **Integers**: Direct cast
- **Timestamps** (`std::string`): Parsed to microseconds since epoch
- **Enums**: Cast to underlying type

**Limitation**: Non-numeric types (e.g. `std::string`) fall back to `0`, which breaks cursor pagination and sort bounds range checks. All current sort fields are numeric or date types.

## Modification Tracking

### ModificationTracker

Tracks entity modifications for lazy cache validation:

```cpp
template<typename Entity>
struct EntityModification {
    enum class Type : uint8_t { Created, Updated, Deleted };

    Type type;
    std::shared_ptr<const Entity> old_entity;  // nullptr for Created
    std::shared_ptr<const Entity> new_entity;  // nullptr for Deleted
    std::chrono::steady_clock::time_point modified_at;
};

template<typename Entity>
class ModificationTracker {
    struct TrackedModification {
        EntityModification<Entity> modification;
        uint8_t cleanup_count = 0;  // Incremented each cleanup
    };

    std::vector<TrackedModification> modifications_;
    size_t num_segments_;
    std::mutex mutex_;
    std::atomic<TimePoint> latest_modification_time_;
};
```

### Cleanup Count Mechanism

Modifications are retained until all cache segments have been processed:

1. **On modification**: Add with `cleanup_count = 0`
2. **On each cleanup**: Increment `cleanup_count` for all modifications
3. **Remove when**: `cleanup_count >= num_segments + 1`

This guarantees every cache segment has had the opportunity to validate against each modification.

```cpp
void cleanup() {
    std::lock_guard lock(mutex_);

    for (auto& tracked : modifications_) {
        ++tracked.cleanup_count;
    }

    const auto threshold = static_cast<uint8_t>(num_segments_ + 1);
    std::erase_if(modifications_, [threshold](const TrackedModification& t) {
        return t.cleanup_count >= threshold;
    });
}
```

### Modification Validation

When validating a cache entry, check if any recent modification affects it:

```cpp
bool isAffectedByModifications(TimePoint cached_at,
                                const SortBounds& bounds,
                                const Result& result,
                                const Query& query) const {
    // Short-circuit: no modifications since cache creation
    if (!modifications_.hasModificationsSince(cached_at)) {
        return false;
    }

    bool affected = false;
    modifications_.forEachModification([&](const Modification& mod) {
        if (affected) return;
        if (mod.modified_at <= cached_at) return;

        if (isModificationAffecting(mod, query, bounds, result)) {
            affected = true;
        }
    });

    return affected;
}
```

A modification affects a cache entry if:
1. The entity (old or new) matches the query filters
2. The entity's sort value falls within the page's sort bounds

## Wrapper Concept Hierarchy

Wrappers declare their format via an explicit tag type:

```cpp
// wrapper/Format.h
struct StructFormat {};
```

### Entity Concepts (`wrapper/EntityConcepts.h`)

Building blocks:

```cpp
template<typename W>
concept ReadableEntity = requires {
    typename W::MappingType;
    { W::MappingType::SQL::select_by_pk } -> std::convertible_to<const char*>;
};

template<typename W>
concept Serializable = HasJsonSerialization<W> || HasBinarySerialization<W>;

template<typename W>
concept MutableEntity = ReadableEntity<W> && requires {
    { W::MappingType::SQL::insert } -> std::convertible_to<const char*>;
    { W::MappingType::SQL::update } -> std::convertible_to<const char*>;
};

template<typename W, typename Key = int64_t>
concept Keyed = requires(const W& w) {
    { w.getPrimaryKey() } -> std::convertible_to<Key>;
};
```

Composed concepts used in repository `requires` clauses:

| Concept | Definition | Used by |
|---------|------------|---------|
| `ReadableEntity<W>` | Has Mapping with SQL queries | `BaseRepo` |
| `CacheableEntity<W>` | `Readable + Serializable` | `RedisRepo`, `CachedRepo` |
| `MutableEntity<W>` | `Readable + has insert/update SQL` | `create()`, `update()` |
| `CreatableEntity<W, K>` | `Mutable + Keyed` | `create()` with cache population |

### Serialization Capabilities (`wrapper/SerializationTraits.h`)

Low-level capability detection:

```cpp
template<typename Entity>
concept HasJsonSerialization = requires(const Entity& e, std::string_view json) {
    { e.toJson() } -> std::convertible_to<std::shared_ptr<const std::string>>;
    { Entity::fromJson(json) } -> std::convertible_to<std::optional<Entity>>;
};

template<typename Entity>
concept HasBinarySerialization = requires(const Entity& e, std::span<const uint8_t> data) {
    { e.toBinary() } -> std::convertible_to<std::shared_ptr<const std::vector<uint8_t>>>;
    { Entity::fromBinary(data) } -> std::convertible_to<std::optional<Entity>>;
};
```

The repository automatically selects the serialization format based on entity capabilities:

```cpp
// In RedisRepo::getFromCache / setInCache:
if constexpr (HasBinarySerialization<Entity>) {
    // Binary path (BEVE via Glaze)
} else {
    // JSON path
}
```

### EntityWrapper<Struct, Mapping>

`EntityWrapper` inherits from the pure data `Struct` and adds API-layer concerns:

```cpp
template<typename Struct, typename Mapping>
class EntityWrapper : public Struct {
    // Delegates fromRow/toInsertParams/getPrimaryKey to Mapping
    // Conditionally exposes makeFullKeyParams (if Mapping has it)
    // Conditionally exposes ListDescriptor (if Mapping has it)
    // Thread-safe lazy BEVE/JSON serialization via std::call_once
};
```

- **Struct**: Pure C++ data type, framework-agnostic, shareable across projects
- **Mapping**: Generated standalone struct with template `fromRow<Entity>`, `toInsertParams<Entity>`, `getPrimaryKey<Entity>`
- **Serialization caches**: `std::once_flag` + mutable `shared_ptr` cache fields; both `toBinary()` and `toJson()` return `shared_ptr` for safe lifetime management
- **`releaseCaches()`**: Resets both BEVE and JSON shared_ptrs to nullptr. After release, `toBinary()`/`toJson()` return nullptr (once_flag already triggered)

#### Glaze Metadata Resolution

```cpp
template<typename Struct, typename Mapping>
struct glz::meta<EntityWrapper<Struct, Mapping>> {
    using T = EntityWrapper<Struct, Mapping>;
    static constexpr auto value = [] {
        if constexpr (requires { glz::meta<Struct>::value; }) {
            return glz::meta<Struct>::value;   // Struct defines custom names
        } else {
            return Mapping::template glaze_value<T>;  // Default: member names
        }
    }();
};
```

**Priority**: `glz::meta<Struct>` (if specialized) > `Mapping::glaze_value` (generated fallback).

**Why base-class member pointers work**: `EntityWrapper<Struct, Mapping>` inherits publicly from `Struct`. Member pointers like `&Struct::field` are implicitly convertible to `&EntityWrapper::field` in C++.

### ListWrapper<Item>

Generic template for paginated list results:

```cpp
template<typename Item>
class ListWrapper {
    std::vector<Item> items;
    int64_t total_count = 0;
    std::string next_cursor;
    // Thread-safe lazy BEVE/JSON serialization
    // Factory methods: fromItems()
};
```

## Partial Field Updates (`updateBy`)

### Field Enum and FieldInfo (Generated)

Each generated entity's `Traits` struct includes a `Field` enum and `FieldInfo` specializations:

```cpp
struct TraitsType {
    enum class Field : uint8_t { username, email, balance };

    template<Field> struct FieldInfo;
};

template<> struct TraitsType::FieldInfo<TraitsType::Field::balance> {
    using value_type = int32_t;
    static constexpr const char* column_name = "\"balance\"";
    static constexpr bool is_timestamp = false;
    static constexpr bool is_nullable = false;
};
```

### FieldUpdate Helpers (`wrapper/FieldUpdate.h`)

Type-safe update descriptors using NTTP:

```cpp
namespace jcailloux::relais::wrapper {

template<auto F, typename V> struct FieldUpdate { V value; };
template<auto F>             struct FieldSetNull {};

template<auto F> auto set(auto&& val);    // Returns FieldUpdate<F, V>
template<auto F> auto setNull();          // Returns FieldSetNull<F>
}
```

### updateBy Flow Across Tiers

```
CachedRepo::updateBy(id, set<F>(v)...)
    |-- invalidateL1Internal(id)
    |
    v
RedisRepo::updateBy(id, set<F>(v)...)
    |-- co_await invalidateRedisInternal(id)
    |
    v
BaseRepo::updateBy(id, set<F>(v)...)
    |-- [optional] Fetch old entity for cross-invalidation
    |
    |-- Build dynamic SQL: UPDATE table SET "col1"=$1, "col2"=$2 WHERE "pk"=$3 RETURNING *
    |     columns = { fieldColumnName<Traits>(updates)... }
    |     values = { fieldValue<Traits>(updates)... }
    |     co_await DbProvider::queryParams(sql, params)
    |
    |-- Build entity from RETURNING row
    |-- co_return re-fetched entity
```

Note: When `ListMixin` or `InvalidationMixin` are active, they intercept `updateBy()` to additionally notify the list cache and/or propagate cross-invalidation.

## PartialKey Repositories

PartialKey repositories handle tables where the repository key is a strict subset of the composite primary key. The canonical use case is PostgreSQL partitioned tables where `PK = (id, region)` but the application queries by `id` alone.

### Auto-Detection

PartialKey is auto-detected at compile time via the `HasPartitionKey` concept, which checks whether the generated Mapping provides `SQL::delete_by_full_pk` and `makeFullKeyParams()`.

### Operation Behavior

| Operation | Standard | PartialKey |
|-----------|----------|------------|
| `find` | `SELECT ... WHERE id = $1` | Same (id is unique across partitions) |
| `update` | `UPDATE ... WHERE id = $1` | Same |
| `updateBy` | `UPDATE ... SET cols WHERE id = $N RETURNING *` | Same |
| `remove` | `DELETE ... WHERE id = $1` | Opportunistic: `DELETE ... WHERE id=$1 AND region=$2` if entity in L1/L2, else `DELETE ... WHERE id=$1` |
| `create` | Standard | Standard |

## Namespace Organization

```
jcailloux::relais::                     # Repo, BaseRepo, RedisRepo,
                                        # CachedRepo, ListMixin, InvalidationMixin
jcailloux::relais::config::             # CacheConfig, CacheLevel, UpdateStrategy,
                                        # Duration, FixedString, presets
jcailloux::relais::wrapper::            # EntityWrapper<Struct, Mapping>, ListWrapper<Item>
                                        # FieldUpdate, set<F>(), setNull<F>()
jcailloux::relais::cache::              # RedisCache, InvalidateOn, Invalidate, InvalidateVia,
                                        # InvalidateList, InvalidateListVia, ListInvalidationTarget
jcailloux::relais::cache::list::        # ListCache, ListBoundsHeader, PaginationMode,
                                        # ListQuery, ModificationTracker, SortBounds
jcailloux::relais::cache::list::decl::  # Filter, Sort, ListDescriptorQuery,
                                        # HttpQueryParser, GeneratedFilters/Traits/Criteria
jcailloux::relais::io::                 # Task, PgPool, PgClient, PgResult, PgParams,
                                        # RedisClient, RedisResult, IoContext
```

### Wrapper Headers (`wrapper/`)

```
wrapper/
├── Format.h               # StructFormat tag
├── SerializationTraits.h   # HasJsonSerialization, HasBinarySerialization
├── EntityConcepts.h        # Readable, Serializable, Writable, Keyed + composed concepts
│                           # HasListDescriptor, HasPartialKey
├── FieldUpdate.h          # set<F>(), setNull<F>(), applyFieldUpdate
├── EntityWrapper.h        # EntityWrapper<Struct, Mapping>
└── ListWrapper.h          # ListWrapper<Item>
```

## Thread Safety

- **Entity serialization**: Thread-safe lazy caching via `std::call_once` (lock-free fast path after first call)
- **L1 Entity Cache**: Thread-safe via shardmap (callback validation)
- **L2 Cache**: Thread-safe via `io::RedisClient` (single-connection pipelining)
- **ListCache**: Thread-safe via shardmap + atomic counters
- **ModificationTracker**: Mutex-protected vector + atomic latest_modification_time

## Performance Considerations

1. **Warmup**: Call `Repo::warmup()` at startup to pre-allocate internal structures (both entity and list caches)
2. **Segment count**: More segments = less contention but more memory overhead
3. **Cleanup frequency**: `l1_cleanup_every_n_gets` balances memory usage and CPU overhead
4. **Lazy validation**: Modifications are checked on `get()`, not on notification
5. **Sort bounds**: O(1) range checking avoids iterating cache entries during invalidation
6. **Short-circuit**: `hasModificationsSince()` check avoids iteration when no recent modifications
7. **Double find on update**: When both `ListMixin` and `InvalidationMixin` are active, each fetches the old entity — but this is a L1 cache hit (O(1) via shardmap), so overhead is negligible

## Entity Mapping Generator

The `scripts/generate_entities.py` script generates standalone ORM Mapping structs from `@relais` annotations in C++ struct headers.

### How It Works

1. Scans `.h` files for `// @relais` annotations on struct declarations and data members
2. Parses data members via regex (type, name, default value, inline annotations)
3. Derives SQL column names from field names
4. Generates a standalone Mapping struct with template methods (`fromRow`, `toInsertParams`, `getPrimaryKey`)

### Generated Components

**For all entities:**
- `Mapping` struct with `TraitsType`, `FieldInfo` specializations, `glaze_value`
- `template<typename Entity> fromRow(const PgResult::Row&) -> optional<Entity>`
- `template<typename Entity> toInsertParams(const Entity&) -> PgParams`
- `template<typename Entity> getPrimaryKey(const Entity&) -> auto`
- `using XxxWrapper = EntityWrapper<Struct, Mapping>;`

**For partial-key entities (PK is `db_managed`):**
- `makeFullKeyParams(const Entity&) -> PgParams` (for single-partition DELETE)

**For list entities (with `filterable`/`sortable` annotations):**
- Embedded `ListDescriptor` struct inside the Mapping (not a standalone struct)
- `using XxxListWrapper = ListWrapper<XxxWrapper>;`

### Annotations

**Struct-level** (before the struct declaration):

| Annotation | Description |
|-----------|-------------|
| `table=table_name` | PostgreSQL table name |
| `output=path/to/Wrapper.h` | Generated file path (relative to `--output-dir`) |
| `read_only` | Mark entity as read-only |

**Field-level** (inline comment on data member):

| Annotation | Description |
|-----------|-------------|
| `primary_key` | Marks the primary key field |
| `db_managed` | Excluded from `toInsertParams` (auto-generated by DB) |
| `timestamp` | Timestamp field — stored as `std::string` (ISO 8601 format) |
| `raw_json` | `glz::raw_json_t` — stored as raw string in DB |
| `json_field` | Struct/vector serialized as JSON in DB |
| `enum` | Auto-resolve DB <-> enum mapping from `glz::meta<EnumType>` |
| `enum=val1:Enum1,...` | Explicit string DB <-> enum mapping |
| `filterable[:param[:op]]` | List filter (see README) |
| `sortable[:direction]` | List sort (see README) |

**Class-level list config** (`@relais_list`): `limits=10,25,50`

### Field Type Handling

| Annotation | Type C++ | `fromRow` | `toInsertParams` |
|-----------|----------|-----------|------------------|
| `timestamp` | `std::string` | `row.get<std::string>(col)` | Direct string parameter |
| `nullable` | `std::optional<T>` | `row.getOpt<T>(col)` | Optional parameter in PgParams |
| `raw_json` | `glz::raw_json_t` | `e.field.str = row.get<std::string>(col)` | `e.field.str` as parameter |
| `json_field` | Struct / `vector<T>` | `glz::read_json(e.field, row_str)` | `glz::write_json(e.field, json)` |
| `enum=...` | `enum class` | `glz::read<glz::opts{.format=JSON}>` | `enumToString()` helper |

### Test Entities

Test entities are pure C++ structs in `tests/fixtures/` with `@relais` annotations. Generated Mapping structs are in `tests/fixtures/generated/` and should not be edited manually.

### Usage

```bash
# Scan directory for @relais annotations
python scripts/generate_entities.py --scan src/entities/ --output-dir src/

# Or specific files
python scripts/generate_entities.py --files src/entities/User.h --output-dir src/
```
