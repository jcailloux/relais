# Internals

This document describes the internal architecture of relais for contributors and advanced users. For the user-facing mental model and API, start at [concepts.md](concepts.md) and [api-reference.md](api-reference.md); this file is the implementation dissection behind them.

> **Not an API contract.** Everything below is *implementation detail* — private
> types, member layouts, atomics, generated symbols, namespaces. It can change
> without notice and is **not** part of the public surface. Do not build against
> anything documented here; the stable contract is [api-reference.md](api-reference.md),
> with [concepts.md](concepts.md) and the guides. This page exists to explain *how*
> and *why* the internals work, for people extending or debugging relais.

## Contents

1. [Repository Mixin Chain](#repository-mixin-chain) — method-hiding tower, `CacheLayerSelector`/`MixinStack`, conditional layer selection
2. [Configuration System — CacheConfig NTTP](#configuration-system--cacheconfig-nttp) — compile-time config plumbing
3. [L1 Cache: CacheTier](#l1-cache-cachetier) — RAM tier over ChunkMap: by-value storage, singleton, TTL, cleanup
4. [L2 Cache: Redis Integration](#l2-cache-redis-integration) — key format, TTL refresh
5. [Cache Flow on Operations](#cache-flow-on-operations) — read/delete/write paths across tiers
6. [ListMixin — Automatic List Caching](#listmixin--automatic-list-caching) — detection, descriptor augmentation, interception
7. [InvalidationMixin — Cross-Repository Invalidation](#invalidationmixin--cross-repository-invalidation) — the four mechanisms, propagation
8. [List Cache Architecture](#list-cache-architecture) — `ListCache`, sort bounds, Lua selective invalidation
9. [Modification Tracking](#modification-tracking) — `ModificationTracker`: generation + chunk-bitmap drain, validation
10. [Entity Concept Hierarchy](#entity-concept-hierarchy) — concepts and serialization capabilities
11. [Partial Field Updates (`patch`)](#partial-field-updates-patch) — `FieldUpdate`, cross-tier patch flow
12. [Partition Key Repositories](#partition-key-repositories) — composite-key auto-detection
13. [Namespace Organization](#namespace-organization) — directory and namespace map
14. [Thread Safety](#thread-safety) — per-component guarantees
15. [Performance Considerations](#performance-considerations) — warmup, contention, lazy validation
16. [Entity Mapping Generator](#entity-mapping-generator) — `generate_entities.py` internals

## Repository Mixin Chain

The library uses a **mixin architecture with method hiding** for compile-time composition. No CRTP — no `Derived` template parameter. Each layer calls `Base::method()` downward.

```
PgRepo<E, Name, Cfg, Key>
       |
       |  (adds Redis caching — if L2 or L1_L2)
       v
RedisRepo<E, Name, Cfg, Key>
       |
       |  (adds RAM caching — if L1 or L1_L2)
       v
LocalRepo<E, Name, Cfg, Key>
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
Repo<E, Name, Cfg, Invalidations...>  (final class)
```

### Template Parameters

The three **cache layers** (`PgRepo`, `RedisRepo`, `LocalRepo`) share four
template parameters:

```cpp
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
```

- **E**: the `Entity<Struct, Mapping>` type
- **Name**: `FixedString` NTTP — compile-time string literal for the repository name and Redis key prefix
- **Cfg**: `CacheConfig` NTTP — structural aggregate (all fields are public structural types)
- **Key**: auto-deduced from `decltype(std::declval<const E>().key())`

The optional mixins take their base instead of the four parameters:
`ListMixin<Base>` and `InvalidationMixin<Base, Invalidations...>` each alias
`using Entity = typename Base::EntityType;` internally. No `Config` struct, no
`typename Derived`, no separate ORM model type.

### Method Hiding

Each mixin layer can override methods from its `Base` by declaring a method with the same name; the upper method calls `Base::method()` explicitly to delegate.

Repo methods are **`static`**: all state (tiers, trackers) is process-global per
repo *type* (see [§3](#l1-cache-cachetier)), there is no per-instance state to carry.

Example: `ListMixin::update()` hides `LocalRepo::update()`:

```cpp
template<typename Base>
class ListMixin : public Base {
    static io::Task<bool> update(const Key& id, const Entity& entity) {
        std::optional<Entity> old;                   // snapshot old via a CacheView
        if (auto v = co_await Base::find(id); v) old.emplace(*v);
        bool ok = co_await Base::update(id, entity); // delegates down
        if (ok && old) listCache().onEntityUpdated(*old, entity);
        co_return ok;
    }
};
```

### Compile-time assembly

`Repo.h` assembles the chain in `namespace detail` via `CacheLayerSelector`
(picks the cache base from `Cfg.cache_level`) and `MixinStack` (stacks the
optional list/invalidation mixins):

```cpp
namespace detail {

/// Select the cache layer based on CacheConfig::cache_level
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
struct CacheLayerSelector {
    using type = std::conditional_t<
        Cfg.cache_level == config::CacheLevel::L1 || Cfg.cache_level == config::CacheLevel::L1_L2,
        LocalRepo<E, Name, Cfg, Key>,
        std::conditional_t<
            Cfg.cache_level == config::CacheLevel::L2,
            RedisRepo<E, Name, Cfg, Key>,
            PgRepo<E, Name, Cfg, Key>
        >
    >;
};

/// Stack optional mixins on top of the cache layer
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key,
         typename... Invalidations>
struct MixinStack {
    using CacheLayer = typename CacheLayerSelector<E, Name, Cfg, Key>::type;

    using WithList = std::conditional_t<
        HasListDescriptor<E>,
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
- Compile-time `static_assert` validation: `ReadableEntity<E>` always;
  `CacheableEntity<E>` when `cache_level != None`; `l1_chunk_count_log2 >= 1` for
  L1 configs; `l2_ttl > 0` for L2 configs.
- Convenience methods (`updateJson`, `updateBinary`) and the predicate
  where-variants (`eraseWhere`/`invalidateWhere`, gated by `HasFilterSet<E>` — see
  [§10](#entity-concept-hierarchy)/[§16](#entity-mapping-generator)).
- Re-export of base type aliases: `EntityType`, `KeyType`, `WrapperType` (= `E`,
  defined in `PgRepo`), `FindResultType` (note: not `MappingType` — that lives on
  `Entity`; the `ListWrapper<E>` type is a separate `ListWrapperType` on `ListMixin`,
  not re-exported here).

### Cache Layer Selection

| `Cfg.cache_level` | Selected Base |
|--------------------|---------------|
| `CacheLevel::None` | `PgRepo` |
| `CacheLevel::L2` | `RedisRepo` |
| `CacheLevel::L1` | `LocalRepo` (inherits `PgRepo`) |
| `CacheLevel::L1_L2` | `LocalRepo` (inherits `RedisRepo`) |

### Conditional Inheritance in LocalRepo

`LocalRepo` uses `std::conditional_t` to choose its base class at compile time:

```cpp
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
class LocalRepo : public std::conditional_t<
    Cfg.cache_level == config::CacheLevel::L1,
    PgRepo<E, Name, Cfg, Key>,      // L1-only: skip Redis
    RedisRepo<E, Name, Cfg, Key>       // L1_L2: include Redis layer
> { ... };
```

## Configuration System — CacheConfig NTTP

All configuration lives in a single structural aggregate, `CacheConfig`, passed as a Non-Type Template Parameter:

```cpp
// config/CacheConfig.h
enum class CacheLevel { None, L1, L2, L1_L2 };
enum class L2Format   { Binary, Json };      // BEVE (default) or JSON in Redis
enum class UpdateStrategy { InvalidateAndLazyReload, PopulateImmediately };

struct CacheConfig {
    CacheLevel cache_level = CacheLevel::None;
    bool read_only = false;
    UpdateStrategy update_strategy = UpdateStrategy::InvalidateAndLazyReload;

    // L1 (RAM cache) — eviction is GDSF-based
    Duration l1_ttl = 1h;             // Duration{0} = no TTL
    uint8_t  l1_chunk_count_log2 = 3; // 2^3 = 8 ChunkMap chunks

    // Read-fill recheck — sharded generation-counter sizing (power-of-2).
    // Scales with WRITE concurrency, not key count. 2^12 = 4096 slots = 32 KB/repo.
    uint8_t  recheck_slots_log2 = 12;

    // L2 (Redis cache)
    Duration l2_ttl = 4h;
    bool     l2_refresh_on_get = false;
    L2Format l2_format = L2Format::Binary;

    // Cross-instance L2 coherence — a deployment FACT (is this Redis shared by
    // more than one process?), not a behavior knob. See "L2 cross-instance gen".
    bool     l2_shared_across_instances = false;

    // Fluent chaining (consteval — compile-time only), one with_* per field:
    consteval CacheConfig with_l1_ttl(Duration v) const { auto c = *this; c.l1_ttl = v; return c; }
    consteval CacheConfig with_read_only(bool v = true) const { auto c = *this; c.read_only = v; return c; }
    consteval CacheConfig with_l2_format(L2Format v) const { auto c = *this; c.l2_format = v; return c; }
    // ... with_cache_level / with_update_strategy / with_l1_chunk_count_log2 /
    //     with_recheck_slots_log2 / with_l2_ttl / with_l2_refresh_on_get /
    //     with_l2_shared_across_instances

    constexpr auto operator<=>(const CacheConfig&) const = default;
};
```

**Structural types only:** every field is an enum, `bool`, `uint8_t`, or `Duration` — all structural in C++23, so `CacheConfig` is usable as an NTTP. The defaulted `operator<=>` makes two configs comparable at compile time.

**`Duration`:** wraps `std::chrono::duration` (which has private members and is not structural). Stores nanoseconds as `int64_t`. Implicitly convertible from any `std::chrono::duration`, explicitly convertible to `std::chrono::nanoseconds`.

**`FixedString`:** lets string literals be template arguments. `FixedString<N>` stores `char[N]` and is implicitly convertible to `const char*`.

**Two subsystems worth naming** (both gated entirely by config, zero cost when off):

- **`recheck_slots_log2`** sizes the read-fill recheck guard — a sharded generation counter that rejects a cache fill if a mutation landed during the fetch (pessimistic miss, never stale).
- **`l2_shared_across_instances`** switches L2 coherence authority: `false` (default) uses the cheap process-local recheck; `true` moves the generation authority into Redis (a sharded gen hash, `HINCRBY` on invalidation, conditional set at fill) so a fill straddling *any* instance's invalidation is rejected — adds one `HGET` at miss + one `EVAL` at fill (cold path only; an L2 hit stays a plain `GET`).

**Presets:**

```cpp
inline constexpr CacheConfig Uncached{};                                   // CacheLevel::None
inline constexpr CacheConfig Local{ .cache_level = CacheLevel::L1 };
inline constexpr CacheConfig Redis{ .cache_level = CacheLevel::L2, .l2_ttl = 4h };
inline constexpr CacheConfig Both{  .cache_level = CacheLevel::L1_L2,
                                    .l1_ttl = 1min, .l2_ttl = 1h };
```

**Composable:** `config::Local.with_l1_ttl(30min).with_read_only()`

**Accessing config in layers:** each layer exposes the config as a data member `static constexpr auto config = Cfg;` and reads `Cfg.field` directly (e.g. `Cfg.l1_ttl`, `Cfg.read_only`). The repository name comes from the `Name` template parameter: `static constexpr const char* name() { return Name; }`.

## L1 Cache: CacheTier

The L1 (RAM) cache is a `cache::CacheTier<Key, E, Metadata>` (`cache/CacheTier.h`),
which wraps a `ChunkMap` (`cache/ChunkMap.h`, relais's own sharded map over the
vendored `parlay_hash` lock-free table) and adds GDSF admission/scoring, ghost
lifecycle, inflight-fetch dedup, TTL, and cleanup behind one interface. Two facts drive everything below:

- **Entities are stored by value** — `CacheTier<Key, E, ...>` holds `E`, not a
  `shared_ptr`. A read does not copy: `find()` returns a `Hit` (a `const E*` into
  the slot plus an epoch guard), which `LocalRepo` wraps in a `cache::CacheView<E>`.
  The view borrows the slot under the guard; the entity is never reference-counted.
- **The tier is a process-global singleton per repo type**, published through an
  atomic pointer — shared across all loop threads, not `thread_local`. (Only the
  I/O *providers* in `PgProvider` are `thread_local`.)

### Storage model and metadata

```cpp
// LocalRepo.h
using Metadata = cache::CacheMetadata<HasGDSF, HasTTL>;
using Tier     = cache::CacheTier<Key, E, Metadata>;
using FindResultType = cache::CacheView<E>;
```

`Metadata` is assembled at compile time from two flags: `HasGDSF`
(`GDSFPolicy::enabled`) adds a GDSF score, `HasTTL` (`Cfg.l1_ttl > 0`) adds a
`uint32_t` expiration in **seconds** (from `runtime::CachedClock::now()`, not a
`time_point`). `buildMetadata(now_sec)` stamps `now_sec + kTtlSec`. `store()`
deliberately does **not** tick the cleanup counter — the sweep cadence is driven
by fetch paths (DB misses), not by API-side mutations (`store`/`insert`).

### The tier singleton

```cpp
static inline std::atomic<Tier*> tier_ptr_{nullptr};

static Tier& tier() {
    auto* p = tier_ptr_.load(std::memory_order_acquire);   // hot path: one load
    if (p) [[likely]] return *p;
    return tier_init_slow();                               // cold path
}

[[gnu::noinline]] static Tier& tier_init_slow() {
    struct Holder {
        Tier instance;
        Holder() {
            instance.enroll({ .sweep_fn = ..., .size_fn = ..., .name = Name });
            tier_ptr_.store(&instance, std::memory_order_release);  // publish last
        }
    };
    static Holder h;                                       // thread-safe init
    tier_ptr_.store(&h.instance, std::memory_order_release); // redundant publish for
    return h.instance;                                       // the already-constructed path
}
```

The acquire load pairs with the release store: a reader that sees the published
pointer also sees the fully-`enroll`-ed tier (registered with `GDSFPolicy`'s sweep
registry).

### Read and TTL validation

`find()` probes the tier; a hit becomes a `CacheView<E>` with no copy, a miss
falls to `findSlow()` (L2 → DB via `CacheTier::findOrFetch`, with inflight dedup
and the read-fill recheck guard):

```cpp
static io::Immediate<cache::CacheView<E>> find(const Key& id) {
    auto hit = tier().find(id);
    if (hit)
        return cache::CacheView<E>(static_cast<const E*>(hit.value),
                                   std::move(hit.guard));
    return findSlow(id);
}
```

TTL is `ttl_expiration_sec` (a `uint32_t`, seconds since the steady_clock epoch;
`0` = no TTL, when `Cfg.l1_ttl` is `Duration{0}`). `find()` evaluates
`metadata.isExpired(CachedClock::now())` and **evicts an expired entry on the spot**
— it becomes a miss that falls to the fetch path. There is no accept-expired or
sliding-refresh knob on L1.

### Cleanup

Cleanup is per-chunk, driven by `GDSFPolicy`'s global sweep (`enroll`'s
`sweep_fn`), fired from `tickInsertion` on the fetch/admission path — **not** by
the background `RuntimeThread`, which only refreshes the cached clock and memory
readings and never sweeps L1. It is also not a global stop-the-world pass: the
tier walks one chunk at a time, evicting expired entries (and, when GDSF is on,
low-score entries against a byte target) so a sweep never holds a chunk locked
longer than that chunk.

## L2 Cache: Redis Integration

All Redis operations go through **`PgProvider::redis(cmd, args...)`** — a variadic,
type-erased command interface (with `redisDynamic` for runtime-sized arg lists).
`cache::RedisCache` builds on it and never names `io::RedisClient` directly.

### Key Format

`makeRedisKey` (`RedisRepo.h`) prefixes the repo `name()` and dispatches on the
key type:

```cpp
static std::string makeRedisKey(const Key& id) {
    if constexpr (is_tuple_v<Key>) {          // composite/partition key
        std::string key(name());
        std::apply([&](const auto&... p) { ((key += ":" + keyPartToString(p)), ...); }, id);
        return key;
    } else if constexpr (std::is_integral_v<Key>)
        return std::string(name()) + ":" + std::to_string(id);   // e.g. "User:123"
    else
        return std::string(name()) + ":" + std::string(id);      // string key
}
```

### TTL Refresh with GETEX

When `Cfg.l2_refresh_on_get = true`, the read path uses `getRawEx`, which issues a
Redis 6.2+ `GETEX` to fetch and slide the TTL in one round-trip:

```cpp
// RedisCache::getRawEx
co_await PgProvider::redis("GETEX", key, "EX", ttl_seconds);
```

(The multi-key read has no native counterpart — `mgetRawEx` fans out N `GETEX`
as gathered tasks.)

### Cross-instance generation (when `l2_shared_across_instances`)

When the config flags a shared Redis (see [§2](#configuration-system--cacheconfig-nttp)),
the recheck authority moves into Redis: a write calls `bumpGen(id)` (`HINCRBY` on a
sharded `{name}:l2gen` hash keyed by `Recheck::slotOf(id)`), a miss reads the gen,
and the fill is a conditional `EVAL` that rejects a write straddling any instance's
invalidation. With the default (`false`), `bumpGen` is a process-local counter — no
extra Redis ops.

## Cache Flow on Operations

### Read: `find(id)`

```
L1 Cache Hit? --yes--> Return CacheView (epoch-guarded borrow, no copy)
      |no
      v
L2 Cache Hit? --yes--> Deserialize --> Store in L1 --> Return CacheView
      |no
      v
Database Query --> Store in L2 --> Store in L1 --> Return CacheView
```

### Delete: `erase(id)`

`erase` threads an **opportunistic hint** (a `const E*` to an entity already held
in cache) down the chain so a partition-pruned `DELETE` is possible without an
extra read. The hint is taken **only for partition entities** (`HasPartitionHint<E>`)
and is an **owned copy** (`std::optional<E> local_hint` filled from the L1 hit),
not a borrow into the live slot. The real method is `eraseOutcome(id, hint)` at
each layer (L2-before-L1 eviction order, matching `eraseMany`):

```
LocalRepo::erase(id)
    |-- if constexpr (HasPartitionHint<E>):   // skipped entirely otherwise
    |     local_hint.emplace(*tier().find(id)) // copy out of L1 (~0ns), hint = &local_hint
    v
RedisRepo::eraseOutcome(id, hint)
    |-- outcome = co_await Base::eraseOutcome(id, hint)
    |-- bumpGen(id); evictRedis(id)          // L2 evicted here
    v
PgRepo::eraseOutcome(id, hint)
    |-- if constexpr (HasPartitionHint<E>) && hint:
    |     params = Mapping::makePartitionHintParams(*hint)
    |     SQL::delete_with_partition         // full key -> 1 partition
    |   else:
    |     SQL::delete_by_pk                   // PK only -> scans N partitions
    |
    (back in LocalRepo::erase: evict L1 last)
```

So the order is **DB delete → L2 UNLINK → L1 evict** (L2 still before L1, matching
`eraseMany`): the DB delete runs in `PgRepo` (innermost), then `RedisRepo` UNLINKs
L2, then control returns to `LocalRepo` which evicts L1 last. The reason for
L1-last is the **phantom-resurrection invariant**, not hint lifetime (the hint is
an owned copy that outlives the slot regardless). Were L1 evicted *before* L2, a
racing L1-miss could read the not-yet-UNLINKed L2 row (a deleted phantom) and
re-store it into the shared L1 — a persistent phantom nothing re-evicts. Clearing
L2 first removes that source: a racing reader can at worst L1-hit the
not-yet-evicted entry (bounded-stale, self-heals at the L1 evict below). (`bumpGen` here is the recheck bump — process-local by default,
`HINCRBY` only when `l2_shared_across_instances`; see [§2](#configuration-system--cacheconfig-nttp).)

**Performance rule**: never add a DB round-trip just for partition pruning. Use the
partition hint only when the entity is free (L1) or near-free (L2 ~0.1ms).

### Write: `update(id, entity)`

Both strategies (controlled by `Cfg.update_strategy`) share the same shape; only
the cache step differs:

```
Fetch old entity (for cross-invalidation)
      |
      v
Database Update --> {Invalidate | Write} L2 --> {Invalidate | Write} L1
      |              (RedisRepo, inner)          (LocalRepo, outer — runs last)
      v                  ^ InvalidateAndLazyReload (safe, default)
                         ^ PopulateImmediately      (optimistic write-through)
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

A list-enabled repo owns **two independent `CacheTier` instantiations** (distinct
singletons — no collision):

- **Entity cache** (from LocalRepo): `CacheTier<Key, E, Metadata>` — entities by value, keys = PK values.
- **List cache** (from ListMixin): a `list::ListCache` wrapping `CacheTier<std::string, ListWrapper<E>, MetadataImpl>` — results stored **by value**, keyed by the **canonical binary query string**.

### List Cache Storage

```cpp
using ListCacheType = list::ListCache<Entity, Base::config.l1_chunk_count_log2,
                                      Key, Traits, HasGDSF>;   // 5 template params

static ListCacheType& listCache() {
    static ListCacheType instance(listCacheConfig());
    // first touch enrolls "<name>:list" in GDSFPolicy's sweep registry — only
    // when GDSF is compiled in (the entity tier enrolls whenever GDSF or TTL is on)
    return instance;
}
```

`ListCacheConfig` carries a single field, derived from the entity `CacheConfig`:

```cpp
static list::ListCacheConfig listCacheConfig() {
    // duration_cast<seconds>(l1_ttl).count() — a uint32_t, NOT std::chrono::seconds
    return { .default_ttl_sec = toSec(Base::config.l1_ttl) };
}
```

(`Base::config` is the `static constexpr auto config = Cfg` member from
[§2](#configuration-system--cacheconfig-nttp); the layers read it as `Cfg.field`.)

### Traits Adapter

`ListMixin` provides a `Traits` struct that bridges the declarative filter/sort helpers (`list::spec`) to the `list::ListCache` interface, delegating to compile-time descriptor helpers:

```cpp
struct Traits {
    using Filters = DescriptorFilters;
    using SortField = size_t;

    static bool matchesFilters(const Entity& e, const Filters& f);
    static int compare(const Entity& a, const Entity& b, SortField, SortDirection);
    static list::Cursor extractCursor(const Entity&, const SortSpec<size_t>&);
    static constexpr SortSpec<size_t> defaultSort();
    static uint16_t normalizeLimit(uint16_t requested);
    // ...
};
```

### CRUD Interception

`ListMixin` intercepts `insert`/`update`/`erase`/`patch` the same way the method
hiding example in [§1](#repository-mixin-chain) shows, to notify the list cache.
The real `update` does not re-fetch: it reuses the already-resolved `old` via
**`Base::updateWithContext(id, entity, old)`** — the `*WithContext` variants take
the pre-fetched old entity as a parameter, so when both `ListMixin` and
`InvalidationMixin` are active the old value is looked up once, not per layer. It
then splits the notification: `onEntityUpdated` for the L1 list tier,
`invalidateL2Updated` for the L2 list tier. Either way the `ModificationTracker`
records the change at the current generation; list pages are validated lazily on
the next `getByKey` against that generation (validation lives in
`ListCache::getByKey`, not a cache callback).

### Cross-Invalidation Entry Points

For external cross-invalidation (via `InvalidateList`), `ListMixin` exposes:

```cpp
static void notifyCreated(const Entity& entity);
static void notifyUpdated(const Entity& old_entity, const Entity& new_entity);
static void notifyDeleted(const Entity& entity);
```

### DB Query Path

`queryFromDb` returns a `std::vector<Entity>` (via `PgProvider::queryParams`). The
caller `cachedListQuery` then extracts the page's sort bounds from the first/last
entity before caching:

```cpp
auto entities = co_await queryFromDb(query);          // -> std::vector<Entity>
// in cachedListQuery, before put():
bounds.first_value = extractSortValue<Descriptor>(entities.front(), sort.field);
bounds.last_value  = extractSortValue<Descriptor>(entities.back(), sort.field);
```

## InvalidationMixin — Cross-Repository Invalidation

Optional layer activated when the `Invalidations...` pack is non-empty. Sits at the top of the mixin chain, intercepts CRUD operations, and propagates invalidations via `InvalidateOn<Invalidations...>`.

### Structure

```cpp
template<typename Base, typename... Invalidations>
class InvalidationMixin : public Base {
    using InvList = InvalidateOn<Invalidations...>;
    // ...
};
```

### Interception Pattern

Each CRUD method fetches the old entity (for propagation data), delegates to `Base`, then propagates:

```cpp
static io::Task<bool> update(const Key& id, const Entity& entity) {
    std::optional<Entity> old;
    if (auto v = co_await Base::find(id); v) old.emplace(*v);  // cache-aware
    bool ok = co_await Base::update(id, entity);
    if (ok) co_await propagateUpdate<Entity, InvList>(old ? &*old : nullptr, entity);
    co_return ok;
}
```

`propagateUpdate` lives in root `jcailloux::relais` and takes the old entity as a
`const Entity*` (null when the row was absent). `Base::find()` resolves through the
full cache chain (L1 -> L2 -> DB), so the old-entity lookup is typically a cache hit.

### Four Invalidation Mechanisms

```cpp
// Declared as Invalidations... variadic pack on Repo:
Invalidate<TargetCache, &Entity::foreign_key>          // table -> table (simple)
InvalidateList<ListRepo>                                // table -> list (full entity)
InvalidateVia<Target, &Entity::key, &Resolver::resolve> // table -> table (async resolver)
InvalidateListVia<ListRepo, &Entity::key, &Resolver::resolve> // table -> list (selective)
```

### Propagation Functions

`InvalidateOn<Deps...>` uses fold expressions to dispatch to each dependency:

- `propagateCreate<Entity, InvList>(new_entity)` — called after `insert()`
- `propagateUpdate<Entity, InvList>(old, new_entity)` — called after `update()`/`patch()`
- `propagateDelete<Entity, InvList>(old_entity)` — called after `erase()`/`invalidate()`

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

**Cascade:** `TargetCache::invalidate(key)` always propagates — `InvalidationMixin::invalidate` runs `propagateDelete<Entity, Invalidations...>` then `Base::invalidate(id)` unconditionally (there is no opt-out flag), so the target's own `Invalidations...` fire automatically. Multi-hop chains work:

```
C modified -> InvalidateVia resolves c_id -> [a_id]
  -> RepoA::invalidate(a_id)        // propagates RepoA::Invalidations...
    -> Invalidate<RepoD, &A::d_id>
      -> RepoD::invalidate(d_id)    // propagates RepoD::Invalidations...
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
list/                            # namespace jcailloux::relais::list
├── ListCache.h              # Core cache, SortBounds, PaginationMode, ListBoundsHeader
├── ListCacheTraits.h        # Traits concepts for filter/sort
├── ListQuery.h              # Query and result types
├── ListWrapper.h            # ListWrapper<Item> result wrapper
├── ModificationTracker.h    # Unified modification tracking
├── SortDirection.h          # SortDirection enum
└── spec/                        # namespace list::spec — compile-time descriptors & parsers
    ├── ListDescriptor.h        # descriptor-shape concepts (HasSorts/HasFilters/ValidListDescriptor) — note: HasListDescriptor itself lives in entity/EntityConcepts.h
    ├── ListDescriptorQuery.h   # ListDescriptorQuery<Descriptor> — query type for parsers
    ├── FilterDescriptor.h      # Filter<Name, MemberPtr, ColumnName, Op, ...> template
    ├── SortDescriptor.h        # Sort<Name, MemberPtr, ColumnName, Dir> template
    ├── ListQueryBuilder.h      # typed self-sealing query builder
    ├── TypedCursor.h           # phantom-typed keyset cursor
    ├── CanonicalEncoding.h     # canonical binary query-key encoding
    ├── HttpQueryParser.h       # parseListQuery (tolerant) / parseListQueryStrict<Descriptor>
    ├── ParseUtils.h            # parsing helpers
    ├── GeneratedFilters.h      # buildCriteria, matchesFilters, extractTags
    ├── GeneratedTraits.h       # extractSortValue, extractCursor, compare
    └── GeneratedCriteria.h     # sortColumnName, parseSortField
```

The high-level `ListMixin` itself lives in `repository/ListMixin.h` (part of the
mixin chain, namespace `jcailloux::relais`), not under `list/`.

### ListCache Storage

`ListCache` is built on a `CacheTier` keyed by the canonical query string, storing
the result **by value**, with generation-stamped metadata:

```cpp
template<typename FilterSet, typename SortFieldEnum>
struct ListCacheMetadataImpl : cache::CacheMetadata<true, true> {
    uint32_t  stored_generation{0};   // tracker generation at cache time (NOT a timestamp)
    SortBounds sort_bounds;
    // + GDSF score + TTL (uint32_t seconds) from CacheMetadata<true,true>
};

template<typename E, uint8_t ChunkCountLog2 = 3, typename Key = int64_t,
         typename Traits = ListCacheTraits<E>, bool GDSF = true>
class ListCache {
    using CacheKey  = std::string;                       // canonical query buffer
    using Result    = list::ListWrapper<E>;
    using Tier      = cache::CacheTier<CacheKey, Result, ListCacheMetadataImpl<...>>;

    Tier                 tier_;          // owns its own ChunkMap + GDSF + cleanup
    ModTracker           modifications_; // ModificationTracker<E, ChunkCount, FilterSet>
    std::atomic<uint32_t> generation_{0}; // monotonic mutation counter (stamps each write)
};
```

`FilterSet` and `SortFieldEnum` (the metadata's params) are `Traits::Filters` and
`Traits::SortField` — derived from the `Traits` template parameter, not separate.

### Lazy Validation on Get

A get probes the tier, then validates against the modification tracker using the
**stored generation**, evicting only if the page is actually stale:

```cpp
ResultView getByKey(const std::string& key) {
    auto hit = tier_.find(key);                 // ghost / TTL / GDSF handled here
    if (!hit) return {};
    long chunk_id = (hit.key_hash >> (48 - ChunkCountLog2)) & (ChunkCount - 1);
    if (isAffectedByModificationsForChunk(*hit.meta, *hit.value, chunk_id)) {
        tier_.evictIfSame(key, hit.value);      // stale → drop this exact entry
        return {};
    }
    return ResultView(hit.value, std::move(hit.guard));   // epoch-guarded borrow
}
```

Validation short-circuits cheaply when nothing changed since the page was cached —
see [§9](#modification-tracking). There is no per-get counter or `GetAction`
callback; cleanup is the per-chunk `GDSFPolicy` sweep (via `ListCache::sweep` →
`drainChunk`) draining chunk bitmaps.

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
| 0 | 2 | Magic bytes: `0x53 0x52` ("SR" = Relais) |
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

**Backward compatibility:** If the first 2 bytes != `0x53 0x52`, the value is treated as old format (no header) and always invalidated conservatively.

### Lua-Based Selective Invalidation

Two Lua scripts execute atomically within Redis (~100-200us for 10 pages):

**Insert/Delete script** (`invalidateListGroupSelective`):
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

| Mode | Insert/Delete                    | Update |
|------|------------------------------------|--------|
| Offset | Affected segment + all after (cascade) | Interval overlap `[min(old,new), max(old,new)]` |
| Cursor | Only affected segment(s) (localized) | Only affected segment(s) |

### List Method Hierarchy

List query methods exist at all repository levels for config-level switching:

| Method | PgRepo | RedisRepo |
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

Sort values are encoded as `int64_t` via `toInt64ForCursor()`. It has exactly
three branches:
- **Integers**: direct cast
- **Enums**: cast to the underlying type
- **Optionals** of the above: unwrapped (recursively); `nullopt` → `0`

**Limitation**: every other type — including `std::string` (so timestamps stored
as ISO-8601 strings) — falls back to `0`, which breaks cursor pagination and sort
bounds range checks. Sort fields must be an integral or enum type (store
timestamps as an integral epoch column, not a string).

## Modification Tracking

### ModificationTracker

List validation is **generation-based**, not timestamp-based, and retention is
driven by a **per-chunk bitmap**, not a cleanup counter. Each modification carries
a monotonic `uint32_t` generation (from the owning `ListCache`'s atomic counter)
and a bitmap of the chunks that have not yet seen it.

```cpp
template<typename E>
struct EntityModification {
    enum class Type : uint8_t { Created, Updated, Deleted };
    Type type;
    std::unique_ptr<const E> old_entity;   // nullptr for Created
    std::unique_ptr<const E> new_entity;   // nullptr for Deleted
    uint32_t generation;                   // from the owning ListCache
    // factories: created(e, gen) / updated(old, new, gen) / deleted(e, gen)
};

template<typename E, size_t TotalSegments, typename RangePayload = detail::NoRangePayload>
class ModificationTracker {
    static_assert(TotalSegments >= 2 && TotalSegments <= 64);
    using BitmapType = detail::SmallestUintFor<TotalSegments>;  // u8/u16/u32/u64

    struct TrackedModification {
        EntityModification<E> modification;
        mutable BitmapType pending_segments;   // bit per not-yet-drained chunk
    };
    struct TrackedRange {                       // eraseWhere predicate fast-path
        RangePayload predicate;
        uint32_t generation;
        mutable BitmapType pending_segments;
    };

    std::vector<TrackedModification> modifications_;
    std::vector<TrackedRange>        ranges_;
    mutable std::shared_mutex        mutex_;
    std::atomic<uint32_t>            latest_generation_{0};
};
```

`TotalSegments` is the chunk count (compile-time, from the ChunkMap config), so
`BitmapType` is the smallest unsigned int that fits one bit per chunk. A new
modification starts with every chunk bit set (`initial_bitmap_`). The **`TrackedRange`**
track lets one entry stand in for an unbounded `eraseWhere` delete set
(`notifyRangeDeleted(predicate, gen)`) instead of N per-row modifications — it
shares the generation counter and the drain lifecycle.

### Chunk-bitmap drain

A modification is retained until **every chunk has validated against it**, then
erased. Cleanup is per-chunk, lock-light:

- `drainChunk(cutoff_gen, chunk_id)` — clears `chunk_id`'s bit in each entry's
  bitmap via `std::atomic_ref<BitmapType>::fetch_and`; an entry whose bitmap
  reaches `0` has been seen by all chunks and is erased (swap-with-last). Only
  entries with `generation <= cutoff_gen` are touched — the cutoff is captured
  *before* the chunk cleanup, so modifications added during cleanup are not
  prematurely drained. Two-phase: a `shared_lock` clears bits and collects drained
  indices, a `unique_lock` erases (taken only when there are removals).
- `drain(cutoff_gen)` — erases every entry with `generation <= cutoff_gen` in one
  pass; used by `purge()` after processing all chunks at once.

### Modification validation

A cached list page stores the tracker generation at cache time
(`ListCacheMetadataImpl::stored_generation`, a `uint32_t` — not a timestamp). On
read, validation short-circuits on the generation, then checks affectedness:

```cpp
bool isAffectedByModifications(const MetadataImpl& meta, const Result& result) const {
    uint32_t stored_gen = meta.stored_generation;
    if (!modifications_.hasModificationsSince(stored_gen)) return false;  // O(1) skip
    // else: forEachModification / forEachRange — a mod/range affects the
    // page if the entity matches the query filters AND its sort value falls within
    // the page's SortBounds (per-chunk variant isAffectedByModificationsForChunk
    // adds the bitmap skip: forEachModificationWithBitmap).
}
```

`hasModificationsSince(since_gen)` compares against `latest_generation_` — a single
atomic load, so an unmodified repo never iterates. `getByKey` runs the per-chunk
variant `isAffectedByModificationsForChunk(meta, value, chunk_id)` (same check,
restricted to the hit's chunk) and `evictIfSame(key, value)` drops the page if
affected.

## Entity Concept Hierarchy

Both `Entity` and `ListWrapper` expose `using Format = StructFormat;` (`entity/Format.h`)
— a marker tag declaring the value follows the relais struct-format contract for
serialization. (`StructFormat` is the only thing `Format.h` defines; there is no
`HasFormat` concept — the "Satisfies: … HasFormat" lines in the headers are
descriptive comments.)

### Entity Concepts (`entity/EntityConcepts.h`)

Building blocks — each checks a single capability, none require an `SQL::*`
member (the SQL strings live on the generated Mapping but the concepts don't
gate on them):

```cpp
template<typename E>
concept Readable = requires(const io::PgResult::Row& row) {
    { E::fromRow(row) } -> std::convertible_to<std::optional<E>>;
};

template<typename E>
concept Serializable = HasJsonSerialization<E> || HasBinarySerialization<E>;

template<typename E>
concept Writable = requires(const E& e) {
    { E::toInsertParams(e) } -> std::convertible_to<io::PgParams>;
};

template<typename E, typename Key>
concept Keyed = requires(const E& e) {
    { e.key() } -> std::convertible_to<Key>;   // no default Key
};
```

Composed concepts used in repository `requires` clauses:

| Concept | Definition | Used by |
|---------|------------|---------|
| `ReadableEntity<E>` | `Readable` (provides `fromRow`) | `PgRepo` |
| `CacheableEntity<E>` | `Readable + Serializable` | `RedisRepo`, `LocalRepo` |
| `MutableEntity<E>` | `Readable + Writable` (provides `toInsertParams`) | `insert()`, `update()` |
| `CreatableEntity<E, K>` | `Mutable + Keyed` | `insert()` with cache population |

Detection concepts in the same header gate the optional features:
`HasListDescriptor<E>` (`E::MappingType::ListDescriptor` exists → list caching) and
`HasPartitionHint<E>` (`SQL::delete_with_partition` + `makePartitionHintParams` →
partition pruning).

`HasFilterSet<E>` gates the where-variants. Mind three same-named things: the
generated **struct** `Mapping::FilterSet` (the predicate spec, [§16](#entity-mapping-generator))
exposes a member type **`::Values`** (the designated-init aggregate); the user-facing
**alias `FilterSet<E>`** = `E::MappingType::FilterSet::Values`. The `ModificationTracker`'s
`RangePayload` is instantiated to this `FilterSet` (a predicate stands in for an
`eraseWhere` delete set — [§9](#modification-tracking)).

### Serialization Capabilities (`entity/SerializationTraits.h`)

Low-level capability detection. `json()`/`binary()` return **by value** (on-demand
serialization), not `shared_ptr`:

```cpp
template<typename E>
concept HasJsonSerialization = requires(const E& e, std::string_view json) {
    { e.json() } -> std::convertible_to<std::string>;
    { E::fromJson(json) } -> std::convertible_to<std::optional<E>>;
};

template<typename E>
concept HasBinarySerialization = requires(const E& e, std::span<const uint8_t> data) {
    { e.binary() } -> std::convertible_to<std::vector<uint8_t>>;
    { E::fromBinary(data) } -> std::convertible_to<std::optional<E>>;
};
```

The L2 path picks its format from `Cfg.l2_format` (`Binary` default, or `Json`),
both backed by Glaze — see [§4](#l2-cache-redis-integration).

### Entity<Struct, Mapping>

`Entity` inherits from the pure data `Struct` and adds the ORM-layer surface,
delegating everything to the generated `Mapping`:

```cpp
template<typename Struct, typename Mapping>
class Entity : public Struct {
    using MappingType = Mapping;                 // exposed for concept detection
    auto key() const { return Mapping::key(*this); }
    static std::optional<Entity> fromRow(const io::PgResult::Row&);   // -> Mapping
    static io::PgParams toInsertParams(const Entity&);                // -> Mapping
    size_t memoryUsage() const;                  // struct + Mapping::dynamicSize

    // On-demand serialization — allocates each call, returns BY VALUE:
    std::vector<uint8_t> binary() const;         // glz::write_beve
    std::string          json()   const;         // glz::write_json
    static std::optional<Entity> fromBinary(std::span<const uint8_t>);
    static std::optional<Entity> fromJson(std::string_view);
};
```

- **Struct**: pure C++ data type, framework-agnostic, shareable across projects.
- **Mapping**: generated standalone struct with template `fromRow<Entity>`,
  `toInsertParams<Entity>`, `key`.
- **No serialization caches.** The class comment is explicit: *"No serialization
  caches — copies/moves are trivial (Struct-only)."* There is no `std::call_once`,
  no cached `shared_ptr`, no `releaseCaches()`. `binary()`/`json()` re-serialize on
  every call. For *cached* serialization, use `Repo::findJson()` / `findBinary()`,
  which serve the bytes off L1/L2.
- `Entity` does **not** conditionally expose `makePartitionHintParams` or `ListDescriptor`
  — those live on the `Mapping`; the repository detects them there via concepts.

#### Glaze Metadata Resolution

```cpp
template<typename Struct, typename Mapping>
struct glz::meta<Entity<Struct, Mapping>> {
    using T = Entity<Struct, Mapping>;
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

**Why base-class member pointers work**: `Entity<Struct, Mapping>` inherits publicly from `Struct`. Member pointers like `&Struct::field` are implicitly convertible to `&Entity::field` in C++.

### ListWrapper<Item>

Generic template for paginated list results:

```cpp
template<typename Item>
class ListWrapper {
    std::vector<Item> items;
    int64_t total_count = 0;
    std::string next_cursor;
    size_t size() const { return items.size(); }
    // On-demand BEVE/JSON serialization (no caching), by value.
    std::vector<uint8_t> binary() const;   // glz::write_beve
    std::string          json()   const;   // glz::write_json
    // Two factories:
    static ListWrapper fromRows(const io::PgResult&);          // build items from a result
    template<typename ItemPtr>                                  // from existing entity ptrs
    static ListWrapper fromItems(const std::vector<ItemPtr>& ptrs, std::string_view cursor = "");
};
```

## Partial Field Updates (`patch`)

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

### FieldUpdate Helpers (`entity/FieldUpdate.h`)

Type-safe update descriptors using NTTP:

```cpp
namespace jcailloux::relais::entity {

template<auto F, typename V> struct FieldUpdate { V value; };
template<auto F>             struct FieldSetNull {};

template<auto F> auto set(auto&& val);    // Returns FieldUpdate<F, V>
template<auto F> auto setNull();          // Returns FieldSetNull<F>
}
```

### patch Flow Across Tiers

The public `patch` delegates down via **`patchRaw`** (the cache layers hide the raw
form, not `patch` itself):

```
LocalRepo::patch(id, set<F>(v)...)
    |-- onMutation(id); bumpGeneration(id); tier().evict(id)  // evict L1 before the UPDATE
    |-- entity = co_await Base::patchRaw(id, updates...)
    |-- storeAndView(id, *entity)            // re-store the fresh RETURNING row in L1
    v
RedisRepo::patchRaw(id, updates...)
    |-- bumpGen(id); evictRedis(id)          // evict L2 before the UPDATE
    |-- co_return co_await Base::patchRaw(id, updates...)
    v
PgRepo::patchRaw(id, updates...)
    |-- sql = buildUpdateReturning(table, pk, {set-cols...}, SQL::returning_columns)
    |       // UPDATE "t" SET "c1"=$1,... WHERE "pk"=$N RETURNING <returning_columns>
    |-- co_await PgProvider::queryWrite(sql, params)   // write batch, not queryParams
    |-- co_return Entity::fromRow(returning_row)
```

The `RETURNING` list is `SQL::returning_columns` (the generated column set), not
`RETURNING *`, and the statement goes through the **write batch** (`queryWrite`).
When `ListMixin`/`InvalidationMixin` are active they intercept `patch` (via
`patchWithContext`) to additionally notify the list cache and/or propagate
cross-invalidation.

## Partition Key Repositories

Partition key repositories handle tables where the PostgreSQL primary key includes partition columns (e.g., `PK = (id, region)`) but the repository key is only the logical identifier (`id`). The partition column is declared with `@relais partition_key` and is **not** part of the repository key.

### Auto-Detection

Partition hint support is auto-detected at compile time via the `HasPartitionHint`
concept (`entity/EntityConcepts.h`), which checks whether the generated Mapping
provides `SQL::delete_with_partition` and `makePartitionHintParams(e)` — emitted
when `@relais partition_key` is used.

### Operation Behavior

| Operation | Standard | `HasPartitionHint` |
|-----------|----------|--------------------|
| `find` | `SELECT ... WHERE id = $1` | Same (id is unique across partitions) |
| `update` | `UPDATE ... WHERE id = $1` | Same |
| `patch` | `UPDATE ... SET cols WHERE id = $N RETURNING <cols>` | Same |
| `erase` | `SQL::delete_by_pk` (scans N partitions) | Opportunistic: `SQL::delete_with_partition` (`makePartitionHintParams`, 1 partition) if a hint is in L1/L2, else `delete_by_pk` |
| `insert` | Standard | Standard |

## Namespace Organization

```
jcailloux::relais::                     # Repo, PgRepo, RedisRepo, LocalRepo, ListMixin,
                                        # InvalidationMixin, Entity<Struct, Mapping>;
                                        # Invalidate, InvalidateList, InvalidateVia,
                                        # InvalidateListVia, InvalidateOn, ListInvalidationTarget,
                                        # propagateCreate/Update/Delete
jcailloux::relais::config::             # CacheConfig, CacheLevel, UpdateStrategy,
                                        # Duration, FixedString, presets
jcailloux::relais::entity::             # FieldUpdate, heapCapacity()
jcailloux::relais::cache::              # CacheTier, CacheView, MultiView, ChunkMap, CacheMetadata,
                                        # TaggedEntry, GDSFPolicy, RedisCache, Metrics
jcailloux::relais::list::               # ListCache, ListWrapper, ListQuery, ModificationTracker,
                                        # SortBounds, ListBoundsHeader, PaginationMode, Cursor
jcailloux::relais::list::spec::         # Filter, Sort, ListDescriptor, FilterDescriptor,
                                        # SortDescriptor, HttpQueryParser, ParseUtils
jcailloux::relais::runtime::            # CachedClock
jcailloux::relais::io::                 # Task, PgPool, PgClient, PgResult, PgParams,
                                        # RedisClient, RedisResult, IoContext
```

### Entity Headers (`entity/`)

```
entity/
├── Format.h               # StructFormat tag
├── SerializationTraits.h   # HasJsonSerialization, HasBinarySerialization
├── EntityConcepts.h        # Readable/Serializable/Writable/Keyed + composed concepts;
│                           # HasListDescriptor, HasFilterSet + FilterSet<E>, HasPartitionHint
├── FieldUpdate.h          # set<F>(), setNull<F>(), fieldColumnName/fieldValue extractors
└── Entity.h               # Entity<Struct, Mapping>
```

## Thread Safety

- **Entity serialization**: stateless — `json()`/`binary()` re-serialize on demand, no shared cache to guard.
- **L1 Entity Cache**: `CacheTier` over a sharded ChunkMap, epoch-guarded reads (a `CacheView` borrows a slot under an `EpochGuard`).
- **L2 Cache**: `RedisPool` of N `RedisClient`s (default 4), round-robin; each client serializes its own commands via a coroutine-mutex (pipelining per connection) — not a single connection.
- **ListCache**: `CacheTier` + the generation/atomic-counter `ModificationTracker`.
- **ModificationTracker**: `std::shared_mutex`-protected vectors + atomic `latest_generation_`; bitmaps mutated via `std::atomic_ref`.

## Performance Considerations

1. **Warmup**: call `Repo::warmup()` at startup to pre-allocate internal structures (both entity and list caches).
2. **Chunk count**: `l1_chunk_count_log2` trades contention against memory — more chunks = less contention, more overhead.
3. **Lazy validation**: modifications are checked on read (`getByKey`), not pushed on notification.
4. **Sort bounds**: O(1) range checking avoids iterating cache entries during invalidation.
5. **Short-circuit**: `hasModificationsSince()` (one atomic load) avoids iteration when nothing changed since the page was cached.
6. **Old-entity reuse on update**: when both `ListMixin` and `InvalidationMixin` are active, the old entity is fetched once and threaded via `*WithContext`, avoiding a second L1 lookup.

## Entity Mapping Generator

The `scripts/generate_entities.py` script generates standalone ORM Mapping structs from `@relais` annotations in C++ struct headers.

### How It Works

1. Scans `.h` files for `// @relais` annotations on struct declarations and data members
2. Parses data members via regex (type, name, default value, inline annotations)
3. Derives SQL column names from field names
4. Generates a standalone Mapping struct with template methods (`fromRow`, `toInsertParams`, `key`)

### Generated Components

**For all entities:**
- `Mapping` struct with `TraitsType`, `FieldInfo` specializations, `glaze_value`
- An `enum Col : uint8_t { ... }` — `fromRow` reads columns by index (`Col::`), not by name
- A nested `SQL` block of `static constexpr const char*` statements:
  `select_by_pk`, `select_by_pk_batch`, `insert`, `update`, `returning_columns`,
  `delete_by_pk`, `delete_by_pk_batch` (composite keys use the matching forms)
- `template<typename Entity> fromRow(const PgResult::Row&) -> optional<Entity>`
- `template<typename Entity> toInsertParams(const Entity&) -> PgParams`
- `template<typename Entity> key(const Entity&) -> auto` (a tuple for composite keys)
- `using XxxEntity = Entity<Struct, Mapping>;`

**For partition-hint entities (`@relais partition_key`):**
- `makePartitionHintParams(const Entity&) -> PgParams` + `SQL::delete_with_partition`
  (single-partition DELETE when a cache hint is available)

**For entities with filters/sorts:**
- An embedded `FilterSet` struct (predicate spec → `HasFilterSet`, drives
  `eraseWhere`/`invalidateWhere`) is emitted when there is **≥1 `filterable`** field.
- An embedded `ListDescriptor` (which composes over the `FilterSet`) is emitted
  **only when there is ≥1 `sortable`** field — that, and only that, activates
  `ListMixin`. `filterable` alone yields a `FilterSet` but no list caching.
- `using XxxListWrapper = ListWrapper<XxxEntity>;`

### Annotations

**Struct-level** (before the struct declaration):

| Annotation | Description |
|-----------|-------------|
| `table=table_name` | PostgreSQL table name |
| `model=Fqn` | Parsed; used only as a fallback in the "no `table=`" validation check, not for code generation |
| `read_only` | Mark entity as read-only |

The output file name is **not** an annotation — it is hard-coded as
`{ClassName}Entity.h` in the `--output-dir` (the struct header is included via a
computed relative path).

**Field-level** (inline comment on data member):

| Annotation | Description |
|-----------|-------------|
| `primary_key` | Marks a primary key field; **multiple** `primary_key` fields → composite key |
| `column=name` | Override the derived SQL column name |
| `db_managed` | Excluded from `toInsertParams` (auto-generated by DB) |
| `timestamp` | Timestamp field — stored as `std::string` (ISO 8601 format) |
| `raw_json` | `glz::raw_json_t` — stored as raw string in DB |
| `json_field` | Struct/vector serialized as JSON in DB |
| `enum` | Auto-resolve DB <-> enum mapping from `glz::meta<EnumType>` |
| `enum=val1:Enum1,...` | Explicit string DB <-> enum mapping |
| `filterable[:param[:op]]` | Predicate filter (and list filter; see README) |
| `sortable[:direction]` | List sort — presence of ≥1 activates `ListMixin` (see README) |

**Class-level list config** (`@relais_list`): `limits=10,25,50` and
`entity=Fqn` (the entity type the list wraps).

### Field Type Handling

| Annotation | Type C++ | `fromRow` | `toInsertParams` |
|-----------|----------|-----------|------------------|
| `timestamp` | `std::string` | `row.get<std::string>(col)` | Direct string parameter |
| `nullable` | `std::optional<T>` | `row.getOpt<T>(col)` | Optional parameter in PgParams |
| `raw_json` | `glz::raw_json_t` | `e.field.str = row.get<std::string>(col)` | `e.field.str` as parameter |
| `json_field` | Struct / `vector<T>` | `glz::read_json(e.field, row_str)` | `glz::write_json(e.field, json)` |
| `enum=...` | `enum class` | generated `if (s == "db_val") e.field = Enum::val;` chain | generated `switch` pushing the `db_val` string literals |

### Test Entities

Test entities are pure C++ structs in `tests/fixtures/` with `@relais` annotations. Generated Mapping structs are in `tests/fixtures/generated/` and should not be edited manually.

### Usage

```bash
# Scan directories and/or pass explicit files (both go to --sources)
python scripts/generate_entities.py --sources src/entities/ --output-dir src/generated/
python scripts/generate_entities.py --sources src/entities/User.h --output-dir src/generated/
```
