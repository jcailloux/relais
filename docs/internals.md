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
4. [Cache Flow on Operations](#cache-flow-on-operations) — read/delete/write paths, uncertain writes
5. [ListMixin — Automatic List Caching](#listmixin--automatic-list-caching) — detection, descriptor augmentation, interception
6. [InvalidationMixin — Cross-Repository Invalidation](#invalidationmixin--cross-repository-invalidation) — the four mechanisms, propagation
7. [List Cache Architecture](#list-cache-architecture) — `ListCache`, sort bounds
8. [Modification Tracking](#modification-tracking) — `ModificationTracker`: generation + chunk-bitmap drain, validation
9. [Entity Concept Hierarchy](#entity-concept-hierarchy) — concepts and serialization capabilities
10. [Partial Field Updates (`patch`)](#partial-field-updates-patch) — `FieldUpdate`, patch flow
11. [Partition Key Repositories](#partition-key-repositories) — composite-key auto-detection
12. [Namespace Organization](#namespace-organization) — directory and namespace map
13. [Thread Safety](#thread-safety) — per-component guarantees
14. [Performance Considerations](#performance-considerations) — warmup, contention, lazy validation
15. [Entity Mapping Generator](#entity-mapping-generator) — `generate_entities.py` internals

## Repository Mixin Chain

The library uses a **mixin architecture with method hiding** for compile-time composition. No CRTP — no `Derived` template parameter. Each layer calls `Base::method()` downward.

```
PgRepo<E, Name, Cfg, Key>
       |
       |  (adds RAM caching — if L1)
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

The two **cache layers** (`PgRepo`, `LocalRepo`) share four
template parameters:

```cpp
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
```

- **E**: the `Entity<Struct, Mapping>` type
- **Name**: `FixedString` NTTP — compile-time string literal for the repository name
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
        Cfg.cache_level == config::CacheLevel::L1,
        LocalRepo<E, Name, Cfg, Key>,
        PgRepo<E, Name, Cfg, Key>
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
  L1 configs.
- Convenience methods (`updateJson`, `updateBinary`) and the predicate
  where-variants (`eraseWhere`/`invalidateWhere`, gated by `HasFilterSet<E>` — see
  [§9](#entity-concept-hierarchy)/[§15](#entity-mapping-generator)).
- Re-export of base type aliases: `EntityType`, `KeyType`, `WrapperType` (= `E`,
  defined in `PgRepo`), `FindResultType` (note: not `MappingType` — that lives on
  `Entity`; the `ListWrapper<E>` type is a separate `ListWrapperType` on `ListMixin`,
  not re-exported here).

### Cache Layer Selection

| `Cfg.cache_level` | Selected Base |
|--------------------|---------------|
| `CacheLevel::None` | `PgRepo` |
| `CacheLevel::L1` | `LocalRepo` (inherits `PgRepo`) |

## Configuration System — CacheConfig NTTP

All configuration lives in a single structural aggregate, `CacheConfig`, passed as a Non-Type Template Parameter:

```cpp
// config/CacheConfig.h
enum class CacheLevel { None, L1 };
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

    // Fluent chaining (consteval — compile-time only), one with_* per field:
    consteval CacheConfig with_l1_ttl(Duration v) const { auto c = *this; c.l1_ttl = v; return c; }
    consteval CacheConfig with_read_only(bool v = true) const { auto c = *this; c.read_only = v; return c; }
    // ... with_cache_level / with_update_strategy / with_l1_chunk_count_log2 /
    //     with_recheck_slots_log2

    constexpr auto operator<=>(const CacheConfig&) const = default;
};
```

**Structural types only:** every field is an enum, `bool`, `uint8_t`, or `Duration` — all structural in C++23, so `CacheConfig` is usable as an NTTP. The defaulted `operator<=>` makes two configs comparable at compile time.

**`Duration`:** wraps `std::chrono::duration` (which has private members and is not structural). Stores nanoseconds as `int64_t`. Implicitly convertible from any `std::chrono::duration`, explicitly convertible to `std::chrono::nanoseconds`.

**`FixedString`:** lets string literals be template arguments. `FixedString<N>` stores `char[N]` and is implicitly convertible to `const char*`.

**`recheck_slots_log2`** sizes the read-fill recheck guard — a sharded generation counter that rejects a cache fill if a mutation landed during the fetch (pessimistic miss, never stale). It gates a write that stores its committed row the same way: if a mutation on the key landed since the write was sent, the key is evicted instead. It is process-local: it does not see another instance's writes.

**Presets:**

```cpp
inline constexpr CacheConfig Uncached{};                                   // CacheLevel::None
inline constexpr CacheConfig Local{ .cache_level = CacheLevel::L1 };
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
falls to `findSlow()` (DB via `CacheTier::findOrFetch`, with inflight dedup
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

## Cache Flow on Operations

### Read: `find(id)`

```
L1 Cache Hit? --yes--> Return CacheView (epoch-guarded borrow, no copy)
      |no
      v
Database Query --> Store in L1 (read-fill recheck) --> Return CacheView
```

### Delete: `erase(id)`

`erase` threads an **opportunistic hint** (a `const E*` to an entity already held
in cache) down the chain so a partition-pruned `DELETE` is possible without an
extra read. The hint is taken **only for partition entities** (`HasPartitionHint<E>`)
and is an **owned copy** (`std::optional<E> local_hint` filled from the L1 hit),
not a borrow into the live slot. The real method is `eraseOutcome(id, hint)` at
each layer:

```
LocalRepo::eraseOutcome(id, hint)
    |-- if constexpr (HasPartitionHint<E>):   // skipped entirely otherwise
    |     local_hint.emplace(*tier().find(id)) // copy out of L1 (~0ns), hint = &local_hint
    v
PgRepo::eraseOutcome(id, hint)
    |-- if constexpr (HasPartitionHint<E>) && hint:
    |     params = Mapping::makePartitionHintParams(*hint)
    |     SQL::delete_with_partition         // full key -> 1 partition
    |   else:
    |     SQL::delete_by_pk                   // PK only -> scans N partitions
    |
    (back in LocalRepo: evict L1 after the commit;
     on PgUncertainError: evict L1 now and once settled, then rethrow)
```

So the order is **DB delete → L1 evict**. A reader racing the delete can at worst
L1-hit the not-yet-evicted entry; the read-fill recheck rejects a fill fetched
before the eviction.

**Performance rule**: never add a DB round-trip just for partition pruning. Use the
partition hint only when the entity is already in L1.

### Write: `update(id, entity)`

Both strategies (controlled by `Cfg.update_strategy`) share the same shape; only
the cache step differs:

```
UPDATE … RETURNING   (prior + committed rows, when lists or cross-inval need them)
      |
      v
{Invalidate | Write} L1     (LocalRepo)
      ^ InvalidateAndLazyReload (safe, default)
      ^ PopulateImmediately      (optimistic write-through)
      |
      v
Lists + cross-invalidation from the prior/committed rows
```

### Write: `upsert(entity)`

`INSERT … ON CONFLICT (pk) DO UPDATE SET …=EXCLUDED.… RETURNING *` in one
statement. Unlike `update`, the committed `RETURNING` row is **stored through**
L1 unconditionally (it is authoritative), so there is no `UpdateStrategy`
branch:

```
Fetch old entity (pre-image; only if lists/cross-inval in the chain)
      |
      v
INSERT … ON CONFLICT DO UPDATE … RETURNING
      |
      +-- success ------> store-through L1 (onMutation + storeThrough)
      |                    lists: old ? onEntityUpdated(old,new) : onEntityCreated(new)
      |                    cross: old ? propagateUpdate(old,new)  : propagateCreate(new)
      |
      +-- uncertain ----> evict L1, invalidate lists from old∪input,
      |   (PgUncertainError) propagate from old/input, then rethrow   (the update model)
      |
      +-- deterministic error --> empty view, nothing cached
```

The create/update discriminant is `old.has_value()` from the pre-image — the same
machinery `update` uses. `onMutation` (absent from `insert`) applies the update
penalty to a ghost. `storeThrough` bumps the key's recheck slot, so a concurrent
read-fill cannot reinstate the old value, and evicts the key instead of storing
when the slot moved since the statement was sent: another write on the key was
handled meanwhile, possibly one committed later on another loop.

### Uncertain writes: precaution and settling

A write that fails with `io::PgUncertainError` may still commit after the failure.
Each layer handles it twice: a **precaution** at once, and the same work again once
the write is **settled**, i.e. can no longer commit
([runtime.md › Settling](runtime.md#settling-an-uncertain-write) gives the contract).

```
BatchScheduler, write handler
    |-- PgUncertainError: attach the connection's SettleTicket, enqueue it
    v       (one ticket per failure, shared by every caller it fails)
each layer, in its catch
    |-- precaution(ticket, evict)   // registers evict as a replay, then runs it
    |-- onSettled(ticket, work)     // registers work that needs the committed row
    |-- rethrow
SettleQueue, one per loop, off the request path
    |-- Terminate: pg_terminate_backend | Await: backend gone | Delay: settle_delay
    |-- outcome read when the server can tell it: Committed, Aborted or Unknown
    |-- ticket Settled -> replays run in registration order, given the outcome
```

- **`SettleTicket`** (`io/pg/SettleTicket.h`) names the backend by pid **and**
  `backend_start` (a pid is reused once its backend exits). The outcome is known
  only when the ticket is its backend's sole unresolved write: a pipeline failure
  leaves several, and the backend's transaction may be any of them.
- **Replays** (`repository/Precaution.h`) are registered before the eviction runs
  (the ticket may settle while it is suspended), own their captures (they outlive
  the caller's frame), and are skipped on `Aborted`. On a ticket already settled,
  the eviction runs once.
- **`SettleQueue`** (`io/pg/SettleQueue.h`) uses a connection of its own, outside
  the pool, closed once the queue is empty: the pool may be exhausted by
  connections blocked on the very lock the uncertain write holds. Tickets naming
  one backend are settled by one statement.

What each layer evicts, now and once settled:

| Layer | Precaution | Once settled |
|---|---|---|
| `LocalRepo` | evict the key; a write by predicate (`eraseWhere`, `patchWhere`, `claim`) leaves a `RowNotes` note instead, evicting any copy its predicate may match when read | the same; a settled note spares copies stored after the seal and is drained by a chunk pass |
| `ListMixin` | the row leaves every page holding it (`RowLeft` lane); a whole-row input enters as an estimate | the row is read again and enters the pages of its committed position (every page if unreadable) |
| `InvalidationMixin` | targets of the L1 copy and of the input | targets of the row read again (every target if unreadable) |

The healthy path pays one atomic load per hit: an L1 hit checks that no note exists,
a list hit that the `RowLeft` lane is empty.

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
`update`, `erase` and `patch` read nothing first. `Base::updateOutcome<true>` runs
an `UPDATE` that locks the row in a CTE and returns it before and after;
`Base::eraseOutcome<true>` runs `DELETE … RETURNING` and hands back the deleted row;
`patchRow<true>` returns both versions the same way. Those rows key the list
notification and, one layer up, `propagateUpdate`/`propagateDelete`. On an
uncertain write nothing comes back; `LocalRepo` copies its L1 entry (still the
targeted version: L1 changes only after a commit) into a slot the upper layers pass
down, just before its precautionary evict, and the row's pages are realigned as
[Uncertain writes](#uncertain-writes-precaution-and-settling) describes. `upsert` keeps a
best-effort pre-read (`findOldBestEffort`): an absent row cannot be locked, so the
statement cannot tell create from update. `onEntityUpdated` then has the
`ModificationTracker` record the change at the current generation; list pages are validated lazily on
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

The page is then stored in L1.

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

Each CRUD method delegates to `Base` through its `*Outcome<true>` variant, which
returns the rows the statement replaced or deleted, then propagates from them
(simplified — the uncertain path propagates from the L1 copy and rethrows):

```cpp
static io::Task<std::optional<size_t>> update(const Key& id, const Entity& entity) {
    std::optional<Entity> uncertain_before;
    auto outcome = co_await Base::template updateOutcome<true>(id, entity, &uncertain_before);
    if (outcome.after)
        co_await propagateUpdate<Entity, InvList>(
            outcome.before ? &*outcome.before : nullptr, *outcome.after);
    co_return outcome.affected;
}
```

`propagateUpdate` lives in root `jcailloux::relais` and takes the old entity as a
`const Entity*` (null when the row was absent). `upsert` and `invalidate` keep a
best-effort read (`findOldBestEffort`) through the cache chain.

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

```
Source entity modified
  -> extractKey(entity) -> source_key
  -> co_await Resolver(source_key) -> optional<[{filters, sort_value}, ...]>
  -> per target: co_await ListRepo::invalidateByTarget(filters, sort_value)  // user-defined
  -> nullopt:    co_await ListRepo::invalidateAllListGroups()   // one empty-predicate
                                                                // RangeModification, O(1)
```

## List Cache Architecture

### File Structure

```
list/                            # namespace jcailloux::relais::list
├── ListCache.h              # Core cache, SortBounds
├── ListCacheTraits.h        # Traits concepts for filter/sort
├── ListQuery.h              # Query and result types
├── ListWrapper.h            # ListWrapper<Item> result wrapper
├── ModificationTracker.h    # Unified modification tracking
├── SortDirection.h          # SortDirection enum
└── spec/                        # namespace list::spec — compile-time descriptors & parsers
    ├── ListDescriptor.h        # descriptor-shape concepts (HasSorts/HasFilters/ValidListDescriptor) — note: HasListDescriptor itself lives in entity/EntityConcepts.h
    ├── ListDescriptorQuery.h   # ListDescriptorQuery<Descriptor> — query type for parsers
    ├── FilterDescriptor.h      # Filter<Name, MemberPtr, ColumnName, Op, ...> template
    ├── SortDescriptor.h        # Sort<Name, MemberPtr, ColumnName, Dir, Converter> template
    ├── ListQueryBuilder.h      # typed self-sealing query builder
    ├── TypedCursor.h           # phantom-typed keyset cursor
    ├── CanonicalEncoding.h     # canonical binary query-key encoding
    ├── HttpQueryParser.h       # parseListQuery (tolerant) / parseListQueryStrict<Descriptor>
    ├── ParseUtils.h            # parsing helpers
    ├── GeneratedFilters.h      # buildCriteria, matchesFilters, extractTags
    ├── GeneratedTraits.h       # extractSortValue, extractCursor, compare, parseSortField, appendSortExpression
    └── GeneratedCriteria.h     # buildWhereClause
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
see [§8](#modification-tracking). There is no per-get counter or `GetAction`
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

The same `int64_t` cursor encoding constrains the **primary key**: every key
component is written into the cursor. `extractKeyComponents()` therefore `static_assert`s that each
component is integral (`"list keyset pagination requires an integer primary
key"`), so a string PK is a compile error rather than a `0` fallback — an entity
with a non-integral PK cannot carry a cached list at all.

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
shares the generation counter and the drain lifecycle. A third lane, **`RowLeft`**,
records rows an uncertain write moved to an unknown position: an entry touches
exactly the pages holding one of its rows. It is counted, so readers skip it without
the lock while it is empty.

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
| `CacheableEntity<E>` | `Readable + Serializable` | `LocalRepo` |
| `MutableEntity<E>` | `Readable + Writable` (provides `toInsertParams`) | `insert()`, `update()` |
| `CreatableEntity<E, K>` | `Mutable + Keyed` | `insert()` |

Detection concepts in the same header gate the optional features:
`HasListDescriptor<E>` (`E::MappingType::ListDescriptor` exists → list caching) and
`HasPartitionHint<E>` (`SQL::delete_with_partition` + `makePartitionHintParams` →
partition pruning).

`HasFilterSet<E>` gates the where-variants. Mind three same-named things: the
generated **struct** `Mapping::FilterSet` (the predicate spec, [§15](#entity-mapping-generator))
exposes a member type **`::Values`** (the designated-init aggregate); the user-facing
**alias `FilterSet<E>`** = `E::MappingType::FilterSet::Values`. The `ModificationTracker`'s
`RangePayload` is instantiated to this `FilterSet` (a predicate stands in for an
`eraseWhere` delete set — [§8](#modification-tracking)).

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

Both are backed by Glaze; `Repo::findJson()`/`findBinary()` and the list
`queryJson`/`queryBinary` use them on the cached value.

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
  every call. `Repo::findJson()` / `findBinary()` serialize the entity held in L1 on
  each hit; the bytes are not cached.
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

### patch Flow

The public `patch` goes down through **`patchRow`** (`LocalRepo`) to **`patchRaw`**
(`PgRepo`):

```
LocalRepo::patchRow(id, set<F>(v)...)
    |-- patched = co_await Base::patchRaw(id, updates...)  // L1 untouched until the commit
    |-- settlePatched(id, *patched)          // store the committed RETURNING row in L1
    |-- on PgUncertainError: evict L1 now and once settled, rethrow
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
`patchRow`) to additionally notify the list cache and/or propagate
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
| `erase` | `SQL::delete_by_pk` (scans N partitions) | Opportunistic: `SQL::delete_with_partition` (`makePartitionHintParams`, 1 partition) if the row is in L1, else `delete_by_pk` |
| `insert` | Standard | Standard |

## Namespace Organization

```
jcailloux::relais::                     # Repo, PgRepo, LocalRepo, ListMixin,
                                        # InvalidationMixin, Entity<Struct, Mapping>;
                                        # Invalidate, InvalidateList, InvalidateVia,
                                        # InvalidateListVia, InvalidateOn, ListInvalidationTarget,
                                        # propagateCreate/Update/Delete
jcailloux::relais::config::             # CacheConfig, CacheLevel, UpdateStrategy,
                                        # Duration, FixedString, presets
jcailloux::relais::entity::             # FieldUpdate, heapCapacity()
jcailloux::relais::cache::              # CacheTier, CacheView, MultiView, ChunkMap, CacheMetadata,
                                        # TaggedEntry, GDSFPolicy, Metrics
jcailloux::relais::list::               # ListCache, ListWrapper, ListQuery, ModificationTracker,
                                        # SortBounds, Cursor
jcailloux::relais::list::spec::         # Filter, Sort, ListDescriptor, FilterDescriptor,
                                        # SortDescriptor, HttpQueryParser, ParseUtils
jcailloux::relais::runtime::            # CachedClock
jcailloux::relais::io::                 # Task, PgPool, PgClient, PgResult, PgParams,
                                        # IoContext
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
- **ListCache**: `CacheTier` + the generation/atomic-counter `ModificationTracker`.
- **ModificationTracker**: `std::shared_mutex`-protected vectors + atomic `latest_generation_`; bitmaps mutated via `std::atomic_ref`.

## Performance Considerations

1. **Warmup**: call `Repo::warmup()` at startup to pre-allocate internal structures (both entity and list caches).
2. **Chunk count**: `l1_chunk_count_log2` trades contention against memory — more chunks = less contention, more overhead.
3. **Lazy validation**: modifications are checked on read (`getByKey`), not pushed on notification.
4. **Sort bounds**: O(1) range checking avoids iterating cache entries during invalidation.
5. **Short-circuit**: `hasModificationsSince()` (one atomic load) avoids iteration when nothing changed since the page was cached.
6. **No pre-read on writes**: `update`, `erase` and `patch` fetch nothing; their statement returns the prior row, which both `ListMixin` and `InvalidationMixin` use. Only `upsert` reads the row first, once for both layers.

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
  `delete_by_pk`, `delete_by_pk_batch` (composite keys use the matching forms);
  `update_returning` is the same update returning the committed row;
  `update_returning_changes` and `delete_by_pk_returning` are the same writes
  returning the prior row (and, for the update, the committed one)
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
| `table=table_name` | PostgreSQL table name (required) |
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
| — | `std::optional<T>` | `row.getOpt<T>(col)` | Optional parameter in PgParams |
| — | `glz::raw_json` | `e.field.str = row.get<std::string>(col)` | `e.field.str` as parameter |
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
