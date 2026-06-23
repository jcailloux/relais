# API Reference

The exhaustive public surface of relais in one file: every repository method, every `CacheConfig` field and preset, the invalidation descriptors, the list/query types, the entity surface and concept hierarchy, the runtime/I/O layer, and the `@relais` annotation index. Each entry is terse and uniform — signature, return, constraints (`requires` + config gating), one-line semantics, and which presets/tiers expose it — and links back to the guide that explains the *why*.

> **This file is the single source of truth for signatures.** The guides teach and give examples; they link here rather than restate a signature. Annotations are the one exception: their canonical home is [entities.md](entities.md), and the [Annotations](#annotations) section here is only an index.
>
> Reads return a borrowed, epoch-guarded view (`cache::CacheView<E>`, or `MultiView<E>` for `findMany`) — never a `shared_ptr` or owning handle. See [Reads](#repository-api).

New to relais? Read [concepts.md](concepts.md) first for the mental model, then use this file as a lookup. Section anchors are stable (`#repository-api`, `#cacheconfig`, `#invalidation-descriptors`, `#list-and-query-api`, `#entity-and-concepts`, `#runtime-and-io`, `#annotations`) — guides link at the section level plus the method name.

## Contents

1. [Repository API](#repository-api) — every method on the final `Repo` (reads, writes, deletes/invalidation, list, maintenance, metrics) and its type aliases.
2. [CacheConfig](#cacheconfig) — tier selection, TTLs, sizing, L2 format, update strategy: all fields, presets, `consteval with_*()` modifiers, and `RELAIS_L1_MAX_MEMORY`.
3. [Invalidation descriptors](#invalidation-descriptors) — the four `Invalidate*` templates, resolver contracts, `InvalidationData`, `ListInvalidationTarget`, and wiring.
4. [List and query API](#list-and-query-api) — `ListDescriptor`, filter operators, sort directions, the fluent builder, `ListQuery`/`ListQueryParams`/`TypedCursor`, HTTP parsers, and `FilterSet`.
5. [Entity and concepts](#entity-and-concepts) — the `Entity<Struct, Mapping>` surface, partial-update factories, and the full concept hierarchy (what each requires, what each enables).
6. [Runtime and I/O](#runtime-and-io) — `IoPool`/`IoPoolConfig`, the `Task`/`Immediate`/`DetachedTask` family, `Outcome`, `spawnOn`, the `IoContext` concept and its conformance harness, and `PgProvider` with its result/error types.
7. [Annotations](#annotations) — index of `@relais` annotations (canonical home: [entities.md](entities.md)).

---

## Repository API

The `Repo<E, Name, Cfg, Invalidations...>` class is the single user-facing type. It assembles a compile-time mixin tower (`PgRepo` → `RedisRepo` → `LocalRepo` → `ListMixin` → `InvalidationMixin`) selected by `Cfg.cache_level` and entity traits; every method below is reached through name-hiding on this final class.

Two contracts hold across the whole surface: every method is **`static`**, and every read/write **returns `io::Task<...>`** (or `io::Immediate<...>` on the L1 fast path, awaitable identically).

### Type aliases

`Repo` re-exports these from the active base layer.

| Alias | Definition | Notes |
|---|---|---|
| `EntityType` | `E` | The entity wrapper type. |
| `KeyType` | `decltype(declval<const E>().key())` | Auto-deduced primary (or composite) key. |
| `WrapperType` | `E` | Same as `EntityType`. |
| `FindResultType` | `cache::CacheView<E>` | The guarded view `find` resolves to. |

> List configs additionally expose `ListQuery`, `ListQueryParams`, `QueryBuilder`, `Cursor`, `ListResult`, `ListTraits`, `ListDescriptorType` (see Lists).

### Reads

Single-key reads resolve to an **epoch-guarded `cache::CacheView<E>`** (defined in `cache/CacheView.h`) — a borrowed view over a cache slot, never a `shared_ptr`/`EntityPtr`. An `EpochGuard` inside it defers reclamation of that slot's *epoch* (a reclamation generation) for the view's lifetime: safe to hold across `co_await`, but a long-lived view delays memory reclaim, so scope it to the request. `if (!view)` means not-found; `operator*`/`operator->` dereference the entity. `findMany` returns a **`cache::MultiView<E>`** — the batch analogue: one epoch guard over N slots, indexed `view[i]` (`nullptr` = absent). `findJson`/`findBinary` return serialized bytes **by value**.

| Method | Returns | Constraints | Notes |
|---|---|---|---|
| `find(const Key& id)` | `Immediate<CacheView<E>>` (L1) / `Task<CacheView<E>>` (L2, L3) | — | L1 hit returns synchronously via `Immediate` (no coroutine frame); miss falls through L2→L3. |
| `findJson(const Key& id)` | `Immediate<std::string>` / `Task<std::string>` | — | Empty string if absent. L2-BEVE hit transcodes via `glz::beve_to_json` (no entity build); L2-JSON hit returns raw. |
| `findBinary(const Key& id)` | `Immediate<std::vector<uint8_t>>` / `Task<...>` | `HasBinarySerialization<E>` | Empty vector if absent. L2-Binary hit returns raw bytes. |
| `findMany(std::span<const Key> ids)` | `Immediate<cache::MultiView<E>>` | **L1 only** (`LocalRepo`) | `view[i] ↔ ids[i]` (`nullptr` = absent). Dedups input; all-L1-hit is zero-copy & frameless; misses fold into one L2 MGET + one L3 `ANY`. |

> `Immediate<T>` is awaitable, so `co_await Repo::find(id)` compiles uniformly whatever `Cfg` selects — an L1 hit is synchronous and frameless, an L2/L3 miss suspends. See [caching.md](caching.md) for the epoch/eviction model.

### Writes

Available only when `!Cfg.read_only`. Each write flows down the full chain: L3 commit → L2 SET/evict → L1 store/evict → list invalidation → cross-invalidation. Write coalescing (identical SQL+params) propagates a `coalesced` flag so upper layers skip redundant cache ops.

| Method | Returns | Constraints | Notes |
|---|---|---|---|
| `insert(const E& e)` | `Task<CacheView<E>>` | `CreatableEntity<E,Key>` (L1/L2) / `MutableEntity<E>` (L3) `&& !Cfg.read_only` | Empty view on error. Populates L1+L2; notifies lists. |
| `update(const Key&, const E&)` | `Task<bool>` | `MutableEntity<E> && HasFullUpdate<E> && !Cfg.read_only` | `true` on success. Strategy via `Cfg.update_strategy`: `InvalidateAndLazyReload` (evict) vs optimistic write-through. |
| `updateJson(const Key&, std::string_view)` | `Task<bool>` | `MutableEntity<E> && HasFullUpdate<E> && !Cfg.read_only` | Parses JSON → `update`. `false` on parse failure. |
| `updateBinary(const Key&, std::span<const uint8_t>)` | `Task<bool>` | `… && HasBinarySerialization<E> && !Cfg.read_only` | Parses BEVE → `update`. `false` on parse failure. |
| `patch(const Key&, Updates&&...)` | `Task<CacheView<E>>` | `HasFieldUpdate<E> && !Cfg.read_only` | Variadic field updates (`UPDATE … RETURNING`); ≥1 update required. Evicts then re-fills cache; returns the refreshed view. |

> `HasFullUpdate<E>` = generator emitted `toUpdateParams` (false for all-PK junction tables, so `update`/`updateJson`/`updateBinary` are cleanly *absent* there — `patch` still works via `HasFieldUpdate`).

### Deletes & invalidation

`erase`/`eraseMany`/`eraseWhere` remove rows from L3 **and** evict the cache hierarchy. `invalidate`/`invalidateMany`/`invalidateWhere` evict the cache **without** touching L3 (a later read repopulates). Deletes require `!Cfg.read_only`; invalidations do not. The batch/predicate paths split into an awaited *critical* tier (L1 evict + L2 UNLINK + gen bump + L1 list bump) and a fire-and-forget *deferred* tier (cross-target + L2 list EVALs).

| Method | Returns | Constraints | Notes |
|---|---|---|---|
| `erase(const Key& id)` | `Task<std::optional<size_t>>` | `!Cfg.read_only` | Rows deleted (`0` = not found), `nullopt` = DB error. Uses L1/L2 partition hint when available. |
| `eraseMany(std::span<const Key> ids)` | `Task<std::optional<size_t>>` | `!Cfg.read_only` | Count deleted; `nullopt` = DB error, `0` = no match. Dedups input; `DELETE … RETURNING` drives eviction (no extra read). Lists invalidated. |
| `invalidate(const Key& id)` | `Task<void>` | — | Evicts L1+L2 (+ cross-inval at top). Lists left intact. |
| `invalidateMany(std::span<const Key> ids)` | `Task<void>` | — | Best-effort. Materializes each via the read path, then evicts entity tier (rows still exist). Missing id → no-op. |

`eraseWhere`/`invalidateWhere` take the generated named `FilterSet<E>` aggregate via designated initializers, e.g. `repo.eraseWhere({.author_id = 42})`.

```cpp
template<typename Pred = FilterSet<E>>
static Task<std::optional<size_t>> eraseWhere(Pred pred);

template<typename Pred = FilterSet<E>>
static Task<void> invalidateWhere(Pred pred);
```
- **returns**: `eraseWhere` → count deleted across all `K_pg`-bounded `DELETE` chunks, `nullopt` on DB error; `invalidateWhere` → `void` (best-effort).
- **requires**: `HasFilterSet<E> && std::same_as<Pred, FilterSet<E>>`; `eraseWhere` additionally `!Cfg.read_only`.
- **semantics**: `eraseWhere` deletes matching rows then evicts entity tier + one filter-aware predicate list invalidation (`O(groups)`). `invalidateWhere` resolves the set via `SELECT … WHERE pred` and evicts the entity tier only (oracle `invalidateWhere(P) ≡ invalidateMany(ids)`).
- **availability**: any `Cfg`; the list-invalidation portion is active only when the entity has a `ListDescriptor` and the preset includes that tier.

> **Never `purgeAll` on invalidation.** Batch and predicate paths point-evict the exact affected keys; unrelated hot entries are untouched.
> **Eviction ordering is L2-before-L1** (mirrors mono `erase`): clearing L2 first prevents a racing L1-miss from re-storing a not-yet-UNLINKed L2 phantom into the shared L1.

### List

Present only when the entity declares a `ListDescriptor` (the `ListMixin` layer). Brief — see [lists.md](lists.md) for query construction, cursors, and bounds.

| Method | Returns | Notes |
|---|---|---|
| `query(const ListQuery& q)` | `Immediate<ListResult>` (`ListResult = CacheView<ListWrapper<E>>`) | Paginated, L1/L2-cached; L1 hit zero-overhead. Also `queryJson` → `Immediate<std::string>` and `queryBinary` → `Immediate<std::vector<uint8_t>>` (the latter `requires HasBinarySerialization<E>`), serving serialized pages. |
| `queryBuilder()` | `QueryBuilder` (noexcept) | Fluent, name-checked builder: `.filter<"...">().sortDesc<"...">().limit(n).after(cursor).build()` seals a `ListQuery`. |
| `listSize()` | `size_t` (noexcept) | L1 list-cache entry count (`0` when no L1). |

> CRUD methods (`insert`/`update`/`erase`/`patch`) are intercepted here to invalidate list caches automatically (L1 `ModificationTracker` bump — the list cache's monotonic generation counter — + selective L2 Lua EVAL).

### Maintenance & metrics

| Method | Returns | Notes |
|---|---|---|
| `name()` | `const char*` (constexpr) | The compile-time repo name / Redis key prefix. |
| `config` | `static constexpr CacheConfig` | The `Cfg` NTTP (data member, not a call). |
| `size()` | `size_t` | L1 entity-cache entry count. (`LocalRepo`+ only.) |
| `sweep(long chunk_id)` | `bool` | Sweep one chunk (entity + list); driven by `GDSFPolicy`. Returns whether anything was removed. |
| `purge()` | `size_t` | Sweep/clear all chunks (entity + list); returns entries removed. |
| `warmup()` | `void` | Prime L1 entity (and list) caches at startup. |

```cpp
#if RELAIS_ENABLE_METRICS
static cache::MetricsSnapshot metrics();   // aggregated L1/L2/list/sweep counters
static void                  resetMetrics();
#endif
```
- **availability**: `metrics`/`resetMetrics` exist **only** when compiled with `RELAIS_ENABLE_METRICS`. The snapshot fields populate per active tier (L1, L2, list L1/L2) plus the global sweep counters; absent tiers stay zero.

> `size`, `sweep`, `purge`, `warmup` are L1-cache concepts (`LocalRepo`/`ListMixin`). On `Uncached`/`Redis`-only presets the chain terminates at `PgRepo`, which does not provide them.

<details><summary>Public-but-internal members (not part of the supported surface)</summary>

The mixin layers also expose a few `public` methods that exist for cross-invalidation wiring or debugging, not for application use: `invalidateAllListGroups()` / `invalidateListGroupByKey(key, sort_value)` (coarse/targeted list-group eviction, driven by `InvalidateListVia`), `makeGroupKey(...)` / `makeRedisKey(key)` (key derivation), `evictRedis(key)` (L2-only evict), and `avgConstructionTime()` (L1 timing, testing/debug). They are reachable on `Repo` by name-hiding but are not contract surface — prefer the documented methods above.
</details>

→ Guides: [caching.md](caching.md), [lists.md](lists.md), [invalidation.md](invalidation.md)

---

## CacheConfig

`CacheConfig` is the structural NTTP aggregate that configures a repository at compile time: which cache tiers are active, their TTLs and sizing, the L2 wire format, and the update/coherence policy. It is passed as the third template parameter of `Repo<Entity, Name, Cfg, Key>`. Because it is a structural type, every field and preset is usable directly as a template argument, and all customization happens through `consteval` fluent modifiers that return a new value.

```cpp
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
class Repo;
```

<details><summary>Why <code>Duration</code> and <code>FixedString</code> exist (NTTP structural types)</summary>

Neither `std::chrono::duration` (private members) nor a raw string literal is a *structural type*, so neither can appear inside an aggregate used as an NTTP. `config::Duration` (an `int64_t ns` with implicit construction from any `std::chrono::duration` and explicit conversion back) and `config::FixedString<N>` (a `char[N]` literal wrapper) are the structural stand-ins that make `.l1_ttl = 30min` and `Repo<E, "Name">` legal as template parameters.
</details>

### Enums

**`CacheLevel`** — selects which tiers the mixin chain assembles.

| Value | Meaning |
|---|---|
| `None` | DB only (`PgRepo`), no caching |
| `L1` | RAM → DB (`LocalRepo` without Redis) |
| `L2` | Redis → DB (`RedisRepo`) |
| `L1_L2` | RAM → Redis → DB (`LocalRepo` over `RedisRepo`) |

**`L2Format`** — how entities are serialized in Redis.

| Value | Meaning |
|---|---|
| `Binary` | BEVE binary (default) — compact and fast |
| `Json` | JSON — human-readable, interoperable with non-C++ consumers |

**`UpdateStrategy`** — how a write reconciles the cache.

| Value | Meaning |
|---|---|
| `InvalidateAndLazyReload` | Safe (default): the write invalidates the cached entry; the next read repopulates it. |
| `PopulateImmediately` | Optimistic write-through: the write also stores the new value into the cache, so a subsequent read hits without a DB round-trip. |

### Fields

| Field | Type | Default | Meaning |
|---|---|---|---|
| `cache_level` | `CacheLevel` | `None` | Active tiers (see above). |
| `read_only` | `bool` | `false` | Disables mutating repository methods at compile time. |
| `update_strategy` | `UpdateStrategy` | `InvalidateAndLazyReload` | Cache reconciliation on write (see above). |
| `l1_ttl` | `Duration` | `1h` | L1 entry time-to-live. `Duration{0}` = no TTL (entries never expire by time; eviction is GDSF/budget-driven only). |
| `l1_chunk_count_log2` | `uint8_t` | `3` | L1 `ChunkMap` shard count as a power of two (`2^3 = 8` chunks). |
| `recheck_slots_log2` | `uint8_t` | `12` | Read-fill recheck guard: sharded generation-counter table size, power of two (`2^12 = 4096` slots ≈ 32 KB/repo). See below. |
| `l2_ttl` | `Duration` | `4h` | L2 (Redis) entry time-to-live. |
| `l2_refresh_on_get` | `bool` | `false` | When `true`, an L2 hit resets the Redis TTL (sliding expiration). |
| `l2_format` | `L2Format` | `Binary` | Redis serialization format. |
| `l2_shared_across_instances` | `bool` | `false` | Cross-instance L2 coherence. A deployment *fact*, not a behavior knob. See below. |

<details>
<summary><code>recheck_slots_log2</code> — the read-fill recheck guard</summary>

The read-fill recheck closes the race where a read misses, fetches from the DB, and is about to store a value that an interleaving write has already invalidated. A sharded generation counter is snapshotted at miss and re-compared at fill; if the generation moved, the fill is rejected (pessimistic miss, **never stale**). The table is sized in slots rather than per-key, so memory is bounded by accepting hash collisions (a collision only costs an extra pessimistic miss). It scales with **write concurrency** (`write_rate × fetch_duration`), **not** with the number of keys — raise `recheck_slots_log2` for write-heavy repositories; the default `12` (4096 slots, ~32 KB/repo) suits typical workloads.
</details>

<details>
<summary><code>l2_shared_across_instances</code> — single- vs multi-instance L2</summary>

- **`false` (default, single-instance):** the recheck authority is the cheap process-local counter — no extra Redis ops. L2 coherence is guaranteed within the process and bounded by `l2_ttl` across instances.
- **`true` (multi-instance):** the recheck authority moves into Redis (a sharded generation hash; `HINCRBY` on invalidation, conditional `setIfGen` at fill via `EVAL`), so a fill that straddles *any* instance's invalidation is rejected. Cost is one `HGET` at miss plus one `EVAL` at fill — **cold path only**; an ordinary L2 hit stays a plain `GET`.
</details>

### Fluent modifiers

All modifiers are `consteval`, non-mutating (each returns a new `CacheConfig`), and chainable. `with_read_only` and `with_l2_shared_across_instances` default their argument to `true`.

```cpp
consteval CacheConfig with_cache_level(CacheLevel v) const;
consteval CacheConfig with_read_only(bool v = true) const;
consteval CacheConfig with_update_strategy(UpdateStrategy v) const;
consteval CacheConfig with_l1_ttl(Duration v) const;
consteval CacheConfig with_l1_chunk_count_log2(uint8_t v) const;
consteval CacheConfig with_recheck_slots_log2(uint8_t v) const;
consteval CacheConfig with_l2_ttl(Duration v) const;
consteval CacheConfig with_l2_refresh_on_get(bool v) const;
consteval CacheConfig with_l2_format(L2Format v) const;
consteval CacheConfig with_l2_shared_across_instances(bool v = true) const;
```

```cpp
// Customize a preset at compile time:
using MyRepo = Repo<MyEntity, "MyEntity",
    config::Local.with_l1_ttl(30min).with_read_only()>;
```

### Presets

Each preset is an `inline constexpr CacheConfig`. Fields not listed take the struct defaults.

| Preset | `cache_level` | `l1_ttl` | `l2_ttl` | All other fields |
|---|---|---|---|---|
| `Uncached` | `None` | `1h` (unused) | `4h` (unused) | defaults |
| `Local` | `L1` | `1h` | `4h` (unused) | defaults |
| `Redis` | `L2` | `1h` (unused) | `4h` | defaults |
| `Both` | `L1_L2` | `1min` | `1h` | defaults |

`Uncached` is the default-constructed value (DB only). `Local` is RAM-only, ideal for data always reached through the same process. `Redis` is shared-across-instances cache with no local RAM tier. `Both` pairs a short L1 TTL with a longer L2 TTL — the typical full-stack configuration.

### Environment: `RELAIS_L1_MAX_MEMORY`

The L1 memory budget (in **bytes**) is read **once**, at first construction of the process-wide `GDSFPolicy`, from the `RELAIS_L1_MAX_MEMORY` environment variable. A positive integer sets the budget; unset or non-positive means **no limit** (`maxMemory() == 0`). When the estimated live L1 heap would exceed the budget, GDSF eviction is triggered on the cache-miss admission path.

> **Gotcha — `l1_ttl = 0` plus a memory budget.** With `l1_ttl = Duration{0}` (no time expiry), L1 entries are reclaimed *only* by GDSF eviction. That makes `RELAIS_L1_MAX_MEMORY` the sole bound on L1 growth: if the variable is unset (no limit), an untimed L1 cache can grow without an eviction trigger. Pair `l1_ttl = 0` with a configured `RELAIS_L1_MAX_MEMORY`.

→ Guide: [caching.md](caching.md)

---

## Invalidation descriptors

A repository declares its cross-cache dependencies as a variadic pack of *invalidation descriptors*; on every successful mutation `InvalidationMixin` folds the pack and propagates eviction to the dependent entity caches and list caches, deduplicated, with old/new entity context.

| Descriptor | Direction | Resolver? | Granularity | Use case |
|---|---|---|---|---|
| `Invalidate<Cache, KeyExtractor>` | entity → entity | no | per-key | Source change evicts a target entity by FK (`&Source::fk` or callable). |
| `InvalidateList<ListRepo>` | entity → list | no | tracker-driven | Source change notifies a list cache (create/update/delete hooks). |
| `InvalidateVia<TargetCache, KeyExtractor, Resolver>` | entity → entity | yes | per-key | Indirect FK: an async resolver maps the source key to N target keys. |
| `InvalidateListVia<ListRepo, KeyExtractor, Resolver>` | entity → list | yes | per-page / per-group / full | Indirect, selective list-page invalidation via enriched resolver. |

```cpp
template<typename Cache, auto KeyExtractor>            struct Invalidate;
template<typename ListCache>                            struct InvalidateList;
template<typename TargetCache, auto SourceKeyExtractor, auto Resolver> struct InvalidateVia;
template<typename ListRepo,    auto SourceKeyExtractor, auto Resolver> struct InvalidateListVia;
```

`KeyExtractor` / `SourceKeyExtractor` is either a pointer-to-member (`&Source::fk`) or a callable (`extractKey(entity)`); the descriptor selects the form with `if constexpr (requires { KeyExtractor(entity); })` then falls back to `entity.*KeyExtractor`.

### 1. `Invalidate<Cache, KeyExtractor>`

Direct entity→entity. On a source mutation, evicts the target entity keyed by the extracted FK. On update it evicts both old and new keys (the new one only if it differs). No resolver.

### 2. `InvalidateList<ListRepo>`

Direct entity→list. Forwards the entity (or its old/new pair) to whichever list-cache hook is present, probed by `if constexpr`. The context-free path (`invalidate(const E&)`) probes `onEntityModified` → `onEntityCreated` → synchronous `notifyCreated`. The data-aware path (`invalidateWithData`) probes the richer set with old/new context: `onEntityModified(data)` → `onEntityUpdated`+`onEntityCreated`/`onEntityDeleted` → `notifyUpdated`+`notifyCreated`/`notifyDeleted`. No scalar key, so batch delete is a per-entity loop (the foreign list cache batches its own tracker internally).

### 3. `InvalidateVia<TargetCache, KeyExtractor, Resolver>`

Indirect entity→entity. The async `Resolver` maps a source key to a set of target keys, each then evicted from `TargetCache`. Use when the FK is not directly on the source row (e.g. join-table lookups).

Resolver contract — two overloads, the batch one opt-in:

```cpp
// Mono (required): resolve one source key -> target keys
io::Task<std::vector<TargetKey>> resolve(const SourceKey& src);

// Batch (optional, opt-in): resolve a deduplicated span in one round-trip
io::Task<std::vector<TargetKey>> resolve(std::span<const SourceKey> srcs);
```

The descriptor detects the span overload with `if constexpr (requires { Resolver(std::span<const KeyT>(sources)); })`; absent it, it loops the mono resolver once per *distinct* source. Source keys and target keys are both `dedupSorted` — a distinct key is never dropped, duplicates collapse idempotently.

### 4. `InvalidateListVia<ListRepo, KeyExtractor, Resolver>`

Indirect entity→list with **selective** page invalidation. The resolver returns typed `ListInvalidationTarget<GroupKey>` values (`GroupKey = ListRepo::GroupKey`, the canonical filter-key encoding — see [List and query API](#list-and-query-api)); each invalidates one list group via `ListRepo::invalidateByTarget(target.filters, target.sort_value)`.

Resolver contract — three granularities, two overloads:

```cpp
using Target = ListInvalidationTarget<typename ListRepo::GroupKey>;

// Per-group / per-page: a concrete set of targets
io::Task<std::vector<Target>> resolve(const SourceKey& src);

// Full invalidation: optional result, nullopt => invalidate ALL groups
io::Task<std::optional<std::vector<Target>>> resolve(const SourceKey& src);

// Batch (opt-in): one round-trip over the deduplicated source span
io::Task<std::vector<Target>>                resolve(std::span<const SourceKey>);
io::Task<std::optional<std::vector<Target>>> resolve(std::span<const SourceKey>);
```

```cpp
template<typename GroupKey>
struct ListInvalidationTarget {
    GroupKey               filters;      // group identity (the filter key)
    std::optional<int64_t> sort_value;   // page within the group; nullopt => whole group
};
```

- **Per-page** — `filters` set + `sort_value` present: invalidate one page.
- **Per-group** — `filters` set + `sort_value == nullopt`: invalidate the whole group.
- **Full** — resolver returns `std::nullopt`: `ListRepo::invalidateAllListGroups()`.

> The resolver result type is detected at compile time: returning `std::optional<std::vector<Target>>` (vs a bare `std::vector<Target>`) is what unlocks the `nullopt` = invalidate-all path. A bare vector can never trigger full invalidation.

### `InvalidationData<E>` — old/new entity context

Every descriptor's `invalidateWithData` receives the before/after snapshot, letting update evict both the stale and the fresh key.

```cpp
template<typename E>
struct InvalidationData {
    const E* old_entity = nullptr;  // null on create
    const E* new_entity = nullptr;  // null on delete

    static InvalidationData forCreate(const E& e);              // {nullptr, &e}
    static InvalidationData forUpdate(const E* old, const E& n);// {old, &n}
    static InvalidationData forDelete(const E& e);              // {&e, nullptr}

    bool isCreate() const;  // !old && new
    bool isUpdate() const;  //  old && new
    bool isDelete() const;  //  old && !new
};
```

> The pointers alias data owned by the caller's coroutine frame (the `const E&` argument or a local `optional<E>`). They are valid only because the fold `(co_await Dep::invalidateWithData(data), ...)` runs sequentially within that frame — never store an `InvalidationData` past the call. The batch deferred path materializes its target keys *by value* before any invalidation for exactly this reason.

### `InvalidateOn<...>` — the aggregator, and `HasInvalidates`

`InvalidateOn<Dependencies...>` folds the pack over the three propagation entry points; the empty specialization `InvalidateOn<>` is a no-op (`co_return`).

```cpp
template<typename... Dependencies>
struct InvalidateOn {
    template<typename E> static io::Task<void> propagate(const E&);                       // context-free fold over each Dep::invalidate(e)
    template<typename E> static io::Task<void> propagateWithData(const InvalidationData<E>&);
    template<typename E> static io::Task<void> propagateDeleteMany(std::span<const E>);    // batch delete
};

template<typename T>
concept HasInvalidates = requires { typename T::Invalidates; };
```

`HasInvalidates` lets other layers detect a configured invalidation set without depending on the concrete descriptor list.

### Wiring — the `Invalidations...` pack

`InvalidationMixin<Base, Invalidations...>` aliases the pack as `InvList = InvalidateOn<Invalidations...>` and re-exports it as `using Invalidates = InvList;` (this is what `HasInvalidates` keys on). It sits at the top of the chain (`InvalidationMixin → [ListMixin] → LocalRepo → [RedisRepo] → PgRepo`) and hides `insert`/`update`/`erase`/`patch`/`invalidate`: each calls `Base::method()` down the chain, and on success awaits the matching free helper — `propagateCreate<E, InvList>` / `propagateUpdate<E, InvList>` / `propagateDelete<E, InvList>` (free function templates in `jcailloux::relais`, not mixin members), which forward to `InvList::propagateWithData` / `propagateDeleteMany`. The layer is only assembled when the pack is non-empty.

<details>
<summary>Update reuses the pre-fetched old entity; batch split critical vs deferred</summary>

`update`/`erase`/`patch` fetch the old entity once (`Base::find(id)`) and, when `Base` is a `ListMixin` (detected via the `HasListMixin` concept = `typename T::ListDescriptorType`), pass it down through the `*WithContext` overloads to avoid a redundant L1 lookup — then feed the same snapshot into `propagateUpdate`/`propagateDelete`.

Batch delete is split into two protected passes:
- `invalidateManyCritical<WithLists>` — own entity tier + L1 list tracker, awaited before the facade returns (the entity UNLINK must precede the return).
- `invalidateManyDeferred<WithLists>` — deduplicated cross-target invalidation `whenAll`-pipelined with the own L2 list EVALs (disjoint caches share one flush), fired fire-and-forget (cross-target invalidate-stale is tolerated).

Per descriptor, batch delete folds N source events into M ≤ N distinct target keys: `Invalidate` collapses to a single `invalidateMany(span)` (one multi-key UNLINK, ⌈M/1000⌉ commands) when the target exposes that facade, else a per-key loop. `dedupSorted` is the unit-testable, I/O-free core; `dedupStable` handles composite/partition keys that model only equality.
</details>

→ Guide: [invalidation.md](invalidation.md)

---

## List and query API

The list subsystem turns a declarative `ListDescriptor` (filters + sorts + page-size grid, emitted by the generator from `@relais` annotations) into a paginated, L1/L2-cached query API on the `ListMixin` layer. A query is built by name — `Repo::queryBuilder().filter<"...">(v).sortDesc<"...">().limit(n).after(cursor).build()` — and **sealed** into an immutable `ListQuery` that carries its own canonical cache keys by construction. HTTP query strings parse into the same type via `parseListQuery` (tolerant) or `parseListQueryStrict` (validating). Everything below lives in `jcailloux::relais::list` / `jcailloux::relais::list::spec` and is re-exported by a list-enabled `Repo`.

### ListDescriptor — what the entity declares

A `ListDescriptor` is a struct embedded in the entity's `Mapping` (`Mapping::ListDescriptor`). `ListMixin` augments it with an `Entity` alias before use. It is validated by two concepts (`ListDescriptor.h`):

- `ValidFilterSet<D>` — `HasEntity<D> && HasFilters<D> && Readable<D::Entity>`. The predicate-only substrate (`Entity` + a `filters` tuple); satisfied by an entity that declares filters **without** a cached list. This is what `eraseWhere`/`invalidateWhere` operate on.
- `ValidListDescriptor<D>` — `ValidFilterSet<D> && HasSorts<D>` (≥1 sort). Adds the sort dimension required for keyset pagination + caching. Every list descriptor is a filter set.

| Member | Form | Meaning |
|---|---|---|
| `Entity` | type alias | The entity type the descriptor targets (injected by `ListMixin`). |
| `filters` | `static constexpr std::tuple<Filter<...>...>` | The declared filterable fields (see `Filter<>`), inherited from the embedded `FilterSet`. Order is the generator's (alphabetical); it is the canonical key order. May be empty. |
| `sorts` | `static constexpr std::tuple<Sort<...>...>` | The declared sortable fields (≥1). `sorts[0]` with its `default_direction` is the default sort. |
| `allowedLimits` | `static constexpr std::array<uint16_t, N>` *(optional)* | The page-size grid, ascending. Drives limit normalization + cache-key bucketing. Absent → `kDefaultLimits = {10, 25, 50, 100}`. |
| `defaultLimit` | `static constexpr uint16_t` *(optional)* | Page size when a request omits `limit`. Absent → `20` (the `ListQuery` struct default). |
| `maxLimit` | `static constexpr uint16_t` *(optional)* | `allowedLimits.back()` — the largest grid value (also exposed as `ListTraits::maxLimit`). |

The generator emits the embedded `FilterSet` (predicate spec: `filters` + the named-optionals `Values` struct) and an embedded `ListDescriptor : FilterSet` (adding `sorts` + the limit grid):

```cpp
struct FilterSet {                                           // satisfies HasFilterSet / ValidFilterSet
    static constexpr auto filters = std::tuple{
        Filter<"author_id", &TestArticle::author_id, "author_id">{},
        Filter<"category",  &TestArticle::category,  "category">{}
    };
    struct Values {                                          // → Repo::FilterSet<E>, designated init
        std::optional<int64_t>     author_id{};
        std::optional<std::string> category{};
        auto toFilterTuple() const { return std::tuple{author_id, category}; }
    };
};
struct ListDescriptor : FilterSet {                          // auto-detected by ListMixin
    static constexpr auto sorts = std::tuple{
        Sort<"id",         &TestArticle::id,         "id",         SortDirection::Desc>{},
        Sort<"view_count", &TestArticle::view_count, "view_count", SortDirection::Desc>{}
    };
    static constexpr std::array<uint16_t, 3> allowedLimits = {10, 25, 50};
    static constexpr uint16_t defaultLimit = 10;
    static constexpr uint16_t maxLimit    = 50;
};
```

> **Group key vs cache key.** There is no separately *declared* group key. The **group key** is the canonical encoding of `filters + sort` (same group regardless of pagination — used for Redis group tracking and selective invalidation); the **cache key** is `group_key + limit + cursor + offset` (identifies one page). Both are computed once at `seal()` (`CanonicalEncoding.h`), never hashed.

Helpers over a descriptor (`ListDescriptor.h`): `filter_count<D>`, `sort_count<D>`, `filter_at<D, I>`, `sort_at<D, I>`.

### Filter operators

`Op` (`FilterDescriptor.h`, `enum class Op : uint8_t`) selects the comparison a `Filter<>` field performs, both in the SQL `WHERE` clause and in the in-memory `matchesFilters` invalidation check.

| `Op` | HTTP/SQL semantics | Active value type | Invalidation (default) |
|---|---|---|---|
| `EQ` | equal (`= $n`) | `optional<element>` | `PreComputed` |
| `NE` | not equal (`!= $n`) | `optional<element>` | `PreComputed` |
| `GT` | greater than (`> $n`) | `optional<element>` | `Lazy` |
| `GE` | greater-or-equal (`>= $n`) | `optional<element>` | `Lazy` |
| `LT` | less than (`< $n`) | `optional<element>` | `Lazy` |
| `LE` | less-or-equal (`<= $n`) | `optional<element>` | `Lazy` |
| `IN` | membership, entity value ∈ set (`= ANY($n)`) | `optional<vector<element>>` | `PreComputed` |
| `NIN` | anti-membership, entity value ∉ set (`!= ALL($n)`) | `optional<vector<element>>` | `PreComputed` |

A filter is declared as:

```cpp
Filter<"author_id", &Article::author_id, "author_id">                  // Op::EQ (default)
Filter<"severity", &Article::severity, "severity", Op::EQ, AsString>   // enum → toString() (ADL)
Filter<"created_at", &Article::created_at, "created_at", Op::GE>       // range bound
Filter<"category", &Article::category, "category", Op::IN>             // set membership
```

Template: `Filter<FixedString Name, auto EntityMemberPtr, FixedString ColumnName, Op = EQ, typename Converter = NoConvert, InvalidationStrategy = defaultInvalidationStrategy(Op)>`. `Name` is the HTTP query-param key; `EntityMemberPtr` may be a data member **or** a const member function. Converters: `NoConvert` (default) and `AsString` (enum via ADL `toString`).

> **IN/NIN v1 guard-rails (compile-time).** A set-op filter rejects an enum element type and any converter other than `NoConvert` (a mis-sized element or `AsString` would silently desync the binary group-vs-entity blob). `is_set_op` is the single `(op == IN || op == NIN)` switch every consumer keys off.

<details><summary>InvalidationStrategy — when a filter triggers list invalidation</summary>

`InvalidationStrategy` (`PreComputed` / `Lazy` / `Disabled`) governs how the filter participates in cross-cache list invalidation; `defaultInvalidationStrategy(Op)` maps `EQ/NE/IN/NIN → PreComputed` (hash matched on modification) and `GT/GE/LT/LE → Lazy` (checked on cache access, via `ModificationTracker` range bumps). `Disabled` never invalidates (pagination-only fields). Per-filter exposed as `Filter::is_precomputed` / `is_lazy`. Range ops feed `predicateSortRange` for the `eraseWhere` fast path.
</details>

### Sort directions

`SortDirection` (`list/SortDirection.h`, `enum class : uint8_t { Asc, Desc }`). A sortable field is declared with `Sort<FixedString Name, auto EntityMemberPtr, FixedString ColumnName, SortDirection DefaultDir = Asc>`:

```cpp
Sort<"created_at", &Article::created_at_us, "created_at", SortDirection::Desc>
Sort<"id", &Article::id, "id">   // default Asc
```

> **Sort fields must be cursor-encodable.** `CursorEncodable<T>` requires an integral or enum type (optionally `optional<>`-wrapped) — string sorts are a **compile error**, because keyset cursors encode each sort value as `int64_t`. Use an integer timestamp (microseconds since epoch) instead of a string date.

At query time a sort is referenced by **name** (compile-checked) or, in HTTP, by the `sort=field:dir` param. Internally a sort resolves to `list::SortSpec<size_t>{ field, direction }` (member `field` is the index into the descriptor's `sorts` tuple). Do not confuse it with `spec::SortSpec<Descriptor>`, whose member is named `field_index`.

### QueryBuilder

`ListQueryBuilder<Descriptor>` (`ListQueryBuilder.h`) is the primary, name-checked construction path; `Repo::queryBuilder()` returns one. Filters and sort are set by name — an unknown name is a `static_assert` (`find_filter_index` / `find_sort_index`), not a runtime miss. Every setter returns `*this`; `build()` seals.

| Method | Returns | Semantics |
|---|---|---|
| `filter<Name>(V&& value)` | `ListQueryBuilder&` | Set filter `Name`; slot type resolved via `Filters::get<Name>()` (scalar `optional<element>` or set `optional<vector<element>>`). |
| `sortBy<Name, Dir = Asc>()` | `ListQueryBuilder&` | Set sort by name; index resolved + checked at compile time. |
| `sortAsc<Name>()` / `sortDesc<Name>()` | `ListQueryBuilder&` | Sugar for `sortBy<Name, Asc/Desc>()`. |
| `sort(DescriptorSortSpec spec)` | `ListQueryBuilder&` | Escape hatch: set a pre-resolved `SortSpec<size_t>` (e.g. from `parseSortField`). |
| `limit(uint16_t n)` | `ListQueryBuilder&` | **Exact** page size — no grid normalization (trusted path; caller owns cache-key cardinality). |
| `after(TypedCursor<D> cursor)` | `ListQueryBuilder&` | Keyset cursor for the next page. Mutually exclusive with `offset` — cursor wins in the cache key. |
| `offset(uint32_t n)` | `ListQueryBuilder&` | Offset+limit pagination. Ignored once a cursor is set. |
| `build()` | `ListQuery<D>` (`[[nodiscard]]`) | **Seals** the accumulated params into the immutable query — the single sealing point of the fluent path. |
| `params()` | `const ListQueryParams<D>&` | Inspect the accumulated mutable params without sealing. |

```cpp
auto q = Repo::queryBuilder()
            .filter<"gallery_id">(gid)
            .sortDesc<"created_at">()
            .limit(24)
            .after(cursor)        // TypedCursor<Descriptor>, from Repo::Cursor::decode(token)
            .build();
co_await Repo::query(q);
```

> `limit(n)` does not snap to `allowedLimits` (the HTTP parsers do; the builder is the trusted path). Each distinct `n` is a distinct cache key — prefer grid values on hot endpoints to bound cache cardinality.

### ListQuery / ListQueryParams — the type-state seal

Two types enforce one invariant: *a query reaching `query()` always carries cache keys consistent with its contents* (`ListDescriptorQuery.h`).

- **`ListQueryParams<Descriptor>`** — the mutable form. Public fields: `Filters<D> filters`, `optional<SortSpec<size_t>> sort`, `uint16_t limit{20}`, `TypedCursor<D> cursor`, `uint32_t offset{0}`. Holds **no** keys. This is what you fill (by hand or via the builder).
- **`ListQuery<Descriptor>`** — the sealed, immutable form. `ListQuery() = delete`; constructible **only** via `seal()` (friend) or `Builder::build()`. Read-only accessors: `filters()`, `sort()`, `limit()`, `cursor()`, `offset()`, `params()`, plus the two canonical keys `groupKey()` and `cacheKey()` (both `const std::string&`). It is the **sole** type `query()` / `queryJson()` / `queryBinary()` accept.

```cpp
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
ListQuery<Descriptor> seal(ListQueryParams<Descriptor> params);   // CanonicalEncoding.h
```

`seal()` computes `groupKey` (from `filters + sort`) and `cacheKey` (from `groupKey + limit + cursor + offset`) exactly once from the final params; the sealed type is then immutable, so its keys always reflect its contents and the hot path carries no empty-key branch.

> **Two `ListQuery`s exist; this section is about `spec::ListQuery<Descriptor>`.** `list::ListQuery<FilterSet, SortFieldEnum>` (in `ListQuery.h`) is the lower-level cache-key struct the L1 `ListCache` consumes; `ListMixin::toCacheQuery` adapts the sealed `spec::ListQuery<D>` into it. Public callers use `spec::ListQuery<D>` (the `Repo::ListQuery` alias).

### TypedCursor — phantom-typed keyset cursor

`TypedCursor<Descriptor>` (`TypedCursor.h`, aliased as `Repo::Cursor`) wraps the opaque byte-level `list::Cursor` and tags it with its owning descriptor, so a cursor minted for one list **cannot** be passed to another list's `query()`/builder — a compile error, not a runtime mis-decode against the wrong sort/key shape.

| Member | Returns | Semantics |
|---|---|---|
| `TypedCursor()` | — | Empty cursor = first page. |
| `static decode(string_view token)` | `optional<TypedCursor>` | Decode a server-minted base64 token. `nullopt` on a malformed token (trust boundary); empty token → empty (first-page) cursor. The **sole** runtime entry point — the type tag is conferred here. |
| `encode()` | `std::string` | Base64 wire token (passes straight through to `list::Cursor`). |
| `empty()` / `size()` | `bool` / `size_t` | Emptiness / byte length. |
| `raw()` | `const list::Cursor&` | The opaque byte cursor, for the keyset SQL / cache-key machinery. |

The wire token is plain base64 over the cursor bytes: `int64(sort_value)` followed by the `N` int64 primary-key components (one for a scalar key, N for a composite key) — the full keyset tiebreaker for the `ORDER BY` row-value comparison.

<details><summary>Pagination cycle — mint and hand-back</summary>

`query()` mints the next page's token into `ListResult`'s `next_cursor` when a full page is returned; the controller hands it back via `.after(Repo::Cursor::decode(token).value())`. The byte layout is built in `queryFromDb` and (de)serialized by `list::Cursor::encode`/`decode`.
</details>

> **Intra-descriptor coherence stays a runtime concern by design.** The type tag guards cross-descriptor confusion only; whether a cursor matches the *current request's* sort cannot be a compile-time type (the sort varies per request, the token arrives off the wire).

### HTTP parsers

Both take a parameter map (default `std::unordered_map<std::string, std::string>`, e.g. Drogon's `req->getParameters()`) and return a **sealed** `ListQuery<Descriptor>`. Recognized non-filter keys: `sort` (`field:dir`), `limit`, `after` (cursor), `offset`, `cursor`. Filter keys match `Filter::name`; IN/NIN values are comma-separated.

```cpp
template<typename Descriptor, typename Map = std::unordered_map<std::string, std::string>>
    requires ValidListDescriptor<Descriptor>
ListQuery<Descriptor> parseListQuery(const Map& params);                       // tolerant

template<typename Descriptor, typename Map = std::unordered_map<std::string, std::string>>
    requires ValidListDescriptor<Descriptor>
std::expected<ListQuery<Descriptor>, QueryValidationError>
    parseListQueryStrict(const Map& params);                                   // validating
```

| | `parseListQuery` | `parseListQueryStrict` |
|---|---|---|
| Returns | `ListQuery<D>` (always succeeds) | `std::expected<ListQuery<D>, QueryValidationError>` |
| Unknown filter param | **ignored** | rejected → `InvalidFilter` |
| Invalid sort field | dropped (default sort applies) | rejected → `InvalidSort` |
| `limit` handling | grid-normalized via `normalizeLimit<D>` (rounds up to next `allowedLimits` step) | must be a member of the grid (`isLimitAllowed<D>`), else `InvalidLimit` |
| missing `limit` | `defaultLimit<D>()` | `defaultLimit<D>()` |
| cursor + offset | offset ignored if cursor present | rejected → `ConflictingPagination` |
| Malformed cursor | dropped (first page) | dropped (no validation, decode-only) |

`QueryValidationError` (`GeneratedTraits.h`): `{ Type type; std::string field; uint16_t limit; }` with `Type ∈ { InvalidFilter, InvalidSort, InvalidLimit, ConflictingPagination }` and a `message()` formatter.

Descriptor-side limit helpers (`GeneratedTraits.h`): `normalizeLimit<D>(requested)` (round up to grid), `defaultLimit<D>()` (declared default or `20`), `isLimitAllowed<D>(limit)` (exact grid membership), `kDefaultLimits = {10, 25, 50, 100}` fallback.

> **Strict cursor is decode-only.** Even `parseListQueryStrict` does not *validate* the cursor contents (it only rejects a cursor+offset conflict); a structurally valid token that targets the wrong sort is a runtime concern, consistent with `TypedCursor`'s design.

### FilterSet — predicate aggregate for the where-variants

`FilterSet<E>` (`EntityConcepts.h`) is the user-facing predicate aggregate for `eraseWhere`/`invalidateWhere`: a generated **struct of named optionals** (one per filter), built with designated initializers.

```cpp
template<typename E>
    requires HasFilterSet<E>
using FilterSet = typename E::MappingType::FilterSet::Values;   // struct of named optionals

repo.eraseWhere({.author_id = 42, .category = "tech"});
repo.invalidateWhere({.gallery_id = gid});
```

`HasFilterSet<E>` requires `E::MappingType::FilterSet` and `::FilterSet::Values`. The aggregate is named by HTTP param (robust to filter reordering, unlike a positional tuple) and converts to the internal `Filters<D>` tuple via `.toFilterTuple()` inside the repo. The `eraseWhere`/`invalidateWhere` methods themselves are documented in the Repository API — this is only the value type they accept.

> An entity may declare a `FilterSet` (satisfying `HasFilterSet` / `ValidFilterSet`) **without** a cached `ListDescriptor`. The where-variants then work with no list tier present.

### Repo re-exports (list-enabled config)

When the entity has a `ListDescriptor`, `Repo` exposes these aliases and types (from `ListMixin`):

| Alias | Definition |
|---|---|
| `ListDescriptorType` | the augmented `Descriptor` (pass to `parseListQuery*<Repo::ListDescriptorType>(...)`). |
| `ListQuery` | `list::spec::ListQuery<Descriptor>` (sealed, immutable). |
| `ListQueryParams` | `list::spec::ListQueryParams<Descriptor>` (mutable; `seal()` it). |
| `QueryBuilder` | `list::spec::ListQueryBuilder<Descriptor>`. |
| `Cursor` | `list::spec::TypedCursor<Descriptor>`. |
| `ListResult` | `cache::CacheView<list::ListWrapper<E>>` (epoch-guarded, zero-copy). |
| `ListTraits` | the `Traits` adapter (sort parsing, limit normalization, etc., for controllers). |

`ListResult` wraps a `ListWrapper<E>`: `items` (`vector<E>`), `total_count` (`int64_t`), `next_cursor` (base64 string), with on-demand `json()` / `binary()` (BEVE) and accessors `front()`/`back()` (`const Item*`, `nullptr` when empty), `size()` (`size_t`), `empty()` (`bool`), `count()` (`int64_t`), `cursor()` (`string_view`). `query()` returns `Immediate<ListResult>` (L1 hit is frameless); `queryJson`/`queryBinary` return serialized pages by value. See the Repository API for the query method signatures and constraints.

→ Guide: [lists.md](lists.md)

---

## Entity and concepts

`Entity<Struct, Mapping>` is the single ORM type a user touches: it inherits the framework-agnostic data `Struct` and bolts on key access, SQL row mapping, on-demand serialization, and memory accounting — all delegated to the generated `Mapping`. The concepts in this module are pure compile-time capability checks; each one gates a specific repository method, so an unsupported operation is *absent* from the API rather than a failed instantiation.

### `Entity<Struct, Mapping>`

```cpp
template<typename Struct, typename Mapping>
class Entity : public Struct;
```

The public surface (namespace `jcailloux::relais`):

| Member | Signature | Notes |
| --- | --- | --- |
| `key()` | `auto key() const noexcept` | Delegates to `Mapping::key(*this)`. Return type deduces the repo's `Key`. |
| `fromRow()` | `static std::optional<Entity> fromRow(const io::PgResult::Row&)` | DB → entity. `nullopt` on mapping failure. Satisfies `Readable`. |
| `toInsertParams()` | `static io::PgParams toInsertParams(const Entity&)` | Entity → INSERT params. Satisfies `Writable`. |
| `toUpdateParams()` | `static io::PgParams toUpdateParams(const Entity&)` *(gated)* | `requires { Mapping::toUpdateParams<Entity>(e); }` — present only when the generator emitted an UPDATE (at least one updatable column). Drives `HasFullUpdate`. |
| `json()` | `std::string json() const` | On-demand Glaze JSON. Returns `"{}"` on write error. |
| `fromJson()` | `static std::optional<Entity> fromJson(std::string_view)` | `nullopt` on empty/parse error. |
| `binary()` | `std::vector<uint8_t> binary() const` | On-demand Glaze BEVE. Empty vector on write error. |
| `fromBinary()` | `static std::optional<Entity> fromBinary(std::span<const uint8_t>)` | `nullopt` on empty/parse error. |
| `memoryUsage()` | `size_t memoryUsage() const` | `sizeof(*this)` plus `Mapping::dynamicSize(*this)` when available. Feeds GDSF byte accounting. |
| `Field` | `using Field = TraitsType::Field` | The field enum used as the NTTP `F` in partial updates. |
| `MappingType` | `using MappingType = Mapping` | Exposed for concept detection (`HasListDescriptor`, `HasFilterSet`, `HasPartitionHint`). |
| `TraitsType` | `using TraitsType = Mapping::TraitsType` | Field metadata (`FieldInfo<F>`). |
| `Format` | `using Format = StructFormat` | Tag type satisfying `HasFormat`. |
| `read_only` | `static constexpr bool read_only` | Mirror of `Mapping::read_only`. |

> No serialization caching: every `json()` / `binary()` call allocates and reserializes. For cache-backed serialization use `Repo::findJson()` / `Repo::findBinary()`, which serve precomputed bytes from L1/L2.

> Copies and moves are `Struct`-only (no per-entity serialization state), so they are trivial.

<details><summary>Glaze field-naming contract</summary>

`glz::meta<Entity<Struct, Mapping>>` prefers a user-supplied `glz::meta<Struct>` specialization (e.g. custom JSON field names) when one exists, otherwise falls back to `Mapping::glaze_value<Entity>` (C++ member names as keys). This keeps the API surface (via `Entity`) and raw-struct BEVE consumers on the same field-naming contract.
</details>

### Partial updates

Factories in `jcailloux::relais::entity` build typed field-update descriptors for the patch path. `F` is a `TraitsType::Field` enum value (NTTP).

```cpp
namespace jcailloux::relais::entity {
    template<auto F, typename V> struct FieldUpdate { V value; };
    template<auto F>             struct FieldSetNull {};

    template<auto F> auto set(auto&& val);   // → FieldUpdate<F, decay_t<V>>
    template<auto F> auto setNull();         // → FieldSetNull<F>
}
```

```cpp
repo.patch(id, set<Field::title>("hi"), setNull<Field::deleted_at>());
```

> `setNull<F>()` is `static_assert`-rejected at SQL-binding time unless `FieldInfo<F>::is_nullable`. Timestamp fields bind their value as `std::string` (no conversion); other fields are `static_cast` to `FieldInfo<F>::value_type`.

### Concept hierarchy

The building blocks live in `EntityConcepts.h` / `SerializationTraits.h`; the update-gating concepts live in `repository/PgRepo.h`.

| Concept | Requires | Enables / Used by |
| --- | --- | --- |
| `HasJsonSerialization<E>` | `e.json() → string`, `E::fromJson(string_view) → optional<E>` | One arm of `Serializable`; `Repo::findJson()`. |
| `HasBinarySerialization<E>` | `e.binary() → vector<uint8_t>`, `E::fromBinary(span) → optional<E>` | One arm of `Serializable`; `Repo::findBinary()`. |
| `Readable<E>` | `E::fromRow(Row) → optional<E>` | Building block of every composite. |
| `Serializable<E>` | `HasJsonSerialization<E> \|\| HasBinarySerialization<E>` | Building block of `CacheableEntity`. |
| `Writable<E>` | `E::toInsertParams(e) → io::PgParams` | Building block of `MutableEntity`. |
| `Keyed<E, Key>` | `e.key() → convertible_to<Key>` | Building block of `CreatableEntity`. |
| `ReadableEntity<E>` | `Readable<E>` | **PgRepo** (L3, DB-only read). |
| `CacheableEntity<E>` | `ReadableEntity<E> && Serializable<E>` | **RedisRepo / LocalRepo** (read + cache store). |
| `MutableEntity<E>` | `ReadableEntity<E> && Writable<E>` | `insert()` / `update()` (read + DB write). |
| `CreatableEntity<E, Key>` | `MutableEntity<E> && Keyed<E, Key>` | `insert()` with cache population (write + key). |
| `HasFullUpdate<E>` | `E::toUpdateParams(e)` well-formed | Gates every full-row `update()` path; absent for all-PK junctions where no column is updatable. |
| `HasFieldUpdate<E>` | `E::TraitsType` and `E::TraitsType::Field` exist | Gates the `patch()` / partial-update path (`set<F>` / `setNull<F>`). |
| `HasListDescriptor<E>` | `E::MappingType::ListDescriptor` exists | Inserts **ListMixin** into the chain (declarative list caching). |
| `HasFilterSet<E>` | `E::MappingType::FilterSet` and `::FilterSet::Values` exist | Gates the where-variants `eraseWhere()` / `invalidateWhere()`; exposes the `FilterSet<E>` named-optionals aggregate. Decoupled from `HasListDescriptor`. |
| `HasPartitionHint<E>` | `MappingType::SQL::delete_with_partition → const char*` and `MappingType::makePartitionHintParams(e) → io::PgParams` | Enables single-partition pruned DELETE when the entity is cached; falls back to PK-only scan otherwise. |

> `HasPartitionKey` is not a concept in the code; partition support is detected through `HasPartitionHint`. The cache key alone identifies the row — the partition column is an *opportunistic hint* used only for single-partition pruning when available from L1/L2.

> `HasFilterSet` is intentionally decorrelated from `HasListDescriptor`: an entity may declare filters with no cached list (and still satisfy `HasFilterSet`); conversely a list entity's `ListDescriptor` carries its `FilterSet`, so list entities satisfy both. The `FilterSet<E>` value type and its usage are detailed under [List and query API → FilterSet](#list-and-query-api).

→ Guide: [entities.md](entities.md)

---

## Runtime and I/O

Repository calls don't reach PostgreSQL/Redis directly — they route through `PgProvider` (a `thread_local` service locator) to a per-loop `BatchScheduler` and connection pools running on an `IoContext` event loop. This section is the exhaustive reference for that runtime: the awaitable `Task`/`Immediate`/`DetachedTask` family, the `IoContext` extension point and its conformance harness, the `IoPool` reference runtime, the `spawnOn` cross-thread bridge, and `PgProvider` with its result/error types. Everything lives in `namespace jcailloux::relais::io` unless noted; `Outcome`/`spawnOn`/`PgProvider` are in root `jcailloux::relais`.

### IoPool & IoPoolConfig

`io::IoPool` is the reference shared-nothing runtime: N `EpollIoContext` loops, each pinned to a core, each owning its own `PgPool`/`RedisPool`/`BatchScheduler` and binding its own `thread_local` providers automatically (no manual `init()`).

```cpp
static std::unique_ptr<IoPool> create(const IoPoolConfig& config);   // io/IoPool.h
```
- **returns**: `std::unique_ptr<IoPool>`, **synchronously** — `create` blocks the calling thread until every worker has connected and bound its providers. It is **not** a coroutine/`Task`. Call it from outside any event loop (e.g. `main`).
- **other members**: `void stop()` (idempotent; also runs in `~IoPool`), `int numWorkers() const noexcept`, `Io& workerIo(int idx) noexcept` (the worker's `EpollIoContext`, for posting work / testing). `using Io = EpollIoContext`.
- **semantics**: each worker binds `PgProvider`'s `thread_local` providers to its own `BatchScheduler` on its own thread during `create()` — a coroutine running on a worker routes to that worker's resources with no cross-thread hop.

> The bundled `PgPool<Io>::create` / `RedisPool<Io>::create` are *coroutines* (lazy `Task`s doing connection I/O on the loop thread) — `IoPool::create` drives them internally. You only touch them directly in a bring-your-own-loop bootstrap (see `spawnOn`).

`IoPoolConfig` — all fields, defaults from the header:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `num_workers` | `int` | `1` | Number of event loops / worker threads (one per core). |
| `pg_conninfo` | `std::string` | `""` | libpq conninfo; empty → libpq `PG*` env vars. |
| `redis_unix_path` | `std::string` | `""` | Redis Unix socket path. Empty → use TCP (`redis_host`/`redis_port`). |
| `redis_host` | `std::string` | `"127.0.0.1"` | Redis TCP host (used when `redis_unix_path` empty). |
| `redis_port` | `int` | `6379` | Redis TCP port. |
| `pg_min_conns_per_worker` | `size_t` | `2` | PG pool floor per worker. |
| `pg_max_conns_per_worker` | `size_t` | `8` | PG pool ceiling per worker. Total PG conns = `num_workers × this` — keep under the DB's `max_connections`. |
| `redis_conns_per_worker` | `size_t` | `4` | Redis connections per worker. |
| `max_concurrent_per_worker` | `int` | `8` | Shared I/O budget per worker (PG + Redis combined). |
| `pin_to_cores` | `bool` | `true` | Pin each worker thread to a CPU core. |
| `first_core` | `int` | `1` | First core index for pinning (worker `i` → `first_core + i`); `1` avoids core 0 (OS/IRQ). |

### Task / Immediate / DetachedTask

The awaitable family in `io/Task.h`. All repository reads/writes resolve to one of these; `co_await` is the only way to consume them.

| Type | Form | Semantics |
|---|---|---|
| `Task<T>` (`T` defaults to `void`) | Lazy, move-only coroutine; symmetric transfer (resumes its continuation on completion). Frames come from a thread-local `FramePool` (~3-5 ns alloc; frames >1 KB fall back to global `new`). | Two creation paths: a coroutine body (`co_return`/`co_await` → heap frame) or `Task<T>::fromValue(T)` (pre-resolved, **zero allocation**, `await_ready()==true`). `await_resume` rethrows a captured exception. |
| `Task<void>` | Specialization. `Task<void>::ready()` is the pre-resolved (frameless) success. | `await_resume()` rethrows the stored exception if any, else returns. |
| `Immediate<T>` | Zero-overhead awaitable wrapping `variant<T, Task<T>>` (no extra discriminant). | Sync/async branch on the hot path: `Immediate(T)` is a ready value (no `Task` allocated); `Immediate(Task<T>)` delegates. Ready case: `await_ready()==true`, single move out of the variant, no coroutine frame. `take_task()` extracts the inner `Task` (valid only when `!await_ready()`). |
| `DetachedTask` | Eager, fire-and-forget coroutine (`initial_suspend`/`final_suspend` = `suspend_never`); self-destructs on completion. | Starts immediately on call. **Exceptions are swallowed** (logged if `RELAIS_LOG_ERROR`). For async work nobody awaits. |

> **`co_await` unifies `Task` and `Immediate`.** Both expose `await_ready`/`await_suspend`/`await_resume`, and a pre-resolved `Task` (`fromValue`/`ready`) or a ready `Immediate` short-circuits with `await_ready()==true` — no suspend, no frame. So `co_await Repo::find(id)` compiles and runs uniformly whether the active tier returns `Immediate<...>` (L1 fast path) or `Task<...>` (L2/L3).

### Outcome

```cpp
template<typename T>
using Outcome = std::expected<T, std::exception_ptr>;   // runtime/Spawn.h, root ns
```
- **semantics**: the completed result of a `Task<T>`: the value, or the `std::exception_ptr` it threw. `if (r)` / `*r` access the value; `r.error()` the exception pointer. For `T = void`, presence of a value (`r.has_value()`) means success. Construct an error with `std::unexpect`.

### spawnOn

```cpp
template<io::IoContext Io, typename T, typename OnDone>
void spawnOn(Io& io, io::Task<T> task, OnDone on_done);   // runtime/Spawn.h, root ns
```
- **returns**: `void`. Does **not** block the caller.
- **semantics**: drives a lazy `Task<T>` to completion **on `io`'s loop thread**, callable from any thread; `on_done(Outcome<T>)` is invoked exactly once, also on the loop thread.

<details><summary>Mechanism — how the driver reaches the loop allocation-free</summary>

A suspended driver coroutine is created holding `task` + `on_done`; `io.post()` hands its 8-byte `coroutine_handle` to the loop (fits `std::function`'s SBO — no queue allocation); the loop resumes it, it `co_await`s the `task`, and on completion calls `on_done`.
</details>
- **constraints**: `task` and `on_done` are *moved into* the driver frame, so `on_done` need **not** be copyable (move-only callbacks are fine). Cost: exactly one coroutine-frame allocation (the driver).
- **on_done**: receives `Outcome<T>` (or `Outcome<void>`). If it throws, the exception is **swallowed** (fire-and-forget contract). The driver frame self-destructs after `on_done` returns.

> Primary use is the one-time per-loop bootstrap: kick a lazy `PgPool::create()` onto a loop from the main thread and do `PgProvider::init` in `on_done`. Per-request reads should `co_await` inline on the loop instead — no `spawnOn`, no hop.

> **Teardown caveat:** if the posted resume never runs (the `IoContext` is destroyed with a non-empty queue), the suspended driver frame leaks — same fire-and-forget semantics as a dropped `post()`.

### The `IoContext` concept

The event-loop extension point (`io/IoContext.h`). `PgPool<Io>`, `RedisPool<Io>`, `BatchScheduler<Io>`, the awaiter machinery, and `spawnOn` are all generic over it: any epoll-family loop (e.g. a trantor shim for Drogon) that satisfies it can host relais inline. The concept fixes **only the signatures below**; the semantic contract (callbacks on the loop thread, FIFO posts, cross-thread wakeups, reentrant `removeWatch`) is encoded in the conformance harness, not the concept.

Required member types (2): `T::WatchHandle`, `T::TimerToken`.

Required expressions (6), each result-constrained via `-> std::same_as<...>`:

| Expression | Result |
|---|---|
| `ctx.addWatch(int fd, IoEvent events, std::function<void(IoEvent)> io_cb)` | `T::WatchHandle` |
| `ctx.removeWatch(T::WatchHandle handle)` | `void` |
| `ctx.updateWatch(T::WatchHandle handle, IoEvent events)` | `void` |
| `ctx.post(std::function<void()> cb)` | `void` |
| `ctx.postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb)` | `T::TimerToken` |
| `ctx.cancelTimer(T::TimerToken token)` | `void` |

`IoEvent` is a `uint8_t`-backed flag enum (`None`/`Read`/`Write`/`Error`) with `operator|`/`operator&`/`operator|=` and `hasEvent(set, flag)`. Watch callbacks receive the matching bits set.

<details><summary>Semantic rules the concept can't express (enforced by the conformance harness)</summary>

| Method | Contract beyond the signature |
|---|---|
| `addWatch` | `cb` runs on the loop thread when `fd` is ready for a masked event. |
| `removeWatch` | Must be safe to call **from inside that handle's own callback** (relais self-removes a watch on EOF/error — defer teardown of loop-owned state past the current event). |
| `post` | Runs once, FIFO, thread-safe, and **wakes a blocked loop promptly** even when posted cross-thread. |
| `postDelayed` | Fires once after the delay. |
| `cancelTimer` | No-op if the timer already fired or is unknown. |
</details>

> **Changing this concept breaks foreign adapters.** A real consumer (codiga/api, co-locating relais on Drogon via a `TrantorIoContext`) satisfies `IoContext` and is validated by the harness — any signature change to the six expressions or two member types breaks every external adapter. Treat it as a stable ABI-like contract.

<details>
<summary>EpollIoContext: methods beyond the concept</summary>

`EpollIoContext` (`io/EpollIoContext.h`; `using WatchHandle = int`, `using TimerToken = uint64_t`) is the bundled model. It satisfies `IoContext` and adds driving methods that are **not** part of the concept (a foreign adapter need not provide them — the harness's author-supplied `drive` substitutes):

| Method | Notes |
|---|---|
| `void run()` | Run until `stop()`. |
| `template<typename Pred> void runUntil(Pred&& pred)` | Run iterations until `pred()` is true. |
| `void runOnce(int timeout_ms = 0)` | One loop iteration (drain posts, fire timers, `epoll_wait`). |
| `void stop()` | Stop the loop; thread-safe (writes the wakeup pipe). |
| `bool isInLoopThread() const noexcept` | True on the thread currently driving the loop. `PgProvider::init` asserts on it in debug when present. |

</details>

### IoContextConformance

```cpp
template<io::IoContext Io, typename Drive>
static void runAll(Io& io, Drive drive);   // testing/IoContextConformance.h
```
- **role**: an *executable contract* for the `IoContext` extension point — runnable checks for the semantics the concept can only describe. An adapter author instantiates `runAll` against their type to prove the shim is correct before wiring relais pools onto it; relais CI runs it against `EpollIoContext`.
- **`drive`**: the one author-supplied primitive — `drive(io, pred)` pumps the loop **on the calling thread** until `pred()` returns true, re-checking `pred` between iterations (the loop's internal wait must be bounded). For `EpollIoContext`: `[](auto& c, auto pred){ c.runUntil(pred); }`.
- **checks (C1-C10)**: post executes exactly once / FIFO order / re-post from a callback; watch fires readable with the right mask / `updateWatch` remask / `removeWatch` stops delivery / **reentrant self-`removeWatch`** (C10, an ASan use-after-free trap); **cross-thread `post`** runs on the loop thread and wakes it (C7); `postDelayed` fires once; `cancelTimer` cancels.
- **framework-agnostic**: Catch-free — throws `ConformanceError` (a `std::runtime_error`) on failure, so it drops into any test framework.

> Modifying the `IoContext` concept and not re-running `runAll` against every foreign adapter is how a co-located consumer silently breaks. The harness is the gate.

### PgProvider & PgResult / Row & errors

`PgProvider` (`PgProvider.h`, root ns) is the type-erased service locator: it wraps `io::PgClient<Io>`/`io::RedisClient<Io>` behind `thread_local std::function`s so repositories don't name the concrete `IoContext`. **All query entry points are static methods of `PgProvider`** (not of `PgResult`/`Row`).

| Method | Returns | Notes |
|---|---|---|
| `query(const char* sql)` | `Task<PgResult>` | Parameterless query. `sql` must outlive the `co_await`. |
| `queryParams(const char* sql, const PgParams& params)` | `Task<PgResult>` | Parameterized; `sql` + `params` must outlive the `co_await`. |
| `template<typename... Args> queryArgs(const char* sql, Args&&...)` | `Task<PgResult>` | Builds a `PgParams` kept in the coroutine frame; forwards to `queryParams`. |
| `queryWrite(const char* sql, const PgParams&)` | `Task<batch::PgWriteResult>` | Sole write entry point (seq-ordered write batch). Result carries RETURNING rows + `affectedRows()`; `coalesced=true` ⇒ an identical write was already batched, no DB round-trip. |
| `template<typename... Args> redis(Args&&...)` | `Task<RedisResult>` | Variadic Redis command; args stringified, binary-safe. |
| `redisDynamic(std::vector<std::string> args)` | `Task<RedisResult>` | Runtime-sized argv (verb first), e.g. MGET over N keys. |
| `hasRedis()` / `initialized()` | `bool` (noexcept) | Whether Redis is configured / providers bound on this thread. |
| `reset()` | `void` (noexcept) | Clear this thread's providers (testing). |

`entityQueryParams` / `entityQueryParamsMany` also exist (entity-read paths routed through `submitEntityRead*` for `pk = ANY` batching/fusion) — repositories use these, not application code.

**Initialization** (call once **per loop thread**, on that thread):

```cpp
template<typename Io>
static void init(
    Io& io,
    std::shared_ptr<io::PgPool<Io>> pool,
    std::type_identity_t<std::shared_ptr<io::RedisClient<Io>>> redisClient = nullptr,
    int max_concurrent = 8);
```
- The Redis argument is **non-deduced** (`std::type_identity_t<...>`) so passing an explicit `nullptr` doesn't break `Io` deduction; `Io` comes from `io`/`pool` only. Default `nullptr` ⇒ no Redis. Default `max_concurrent = 8`.
- Providers are `thread_local`: `init` on thread A binds nothing for thread B. Debug builds assert `io.isInLoopThread()` for adapters that expose it. `bindBatcher<Io>(batcher, with_redis)` is the lower-level entry `IoPool` uses (one batcher per worker).

**`PgResult`** (`io/pg/PgResult.h`) — RAII wrapper over `PGresult`:

| Member | Returns | Notes |
|---|---|---|
| `rows()` | `int` | Row count (honors slice views). |
| `cols()` | `int` | Column count. |
| `valid()` / `empty()` / `ok()` | `bool` | Has a result / zero rows / status is OK (TUPLES/COMMAND/SINGLE_TUPLE). |
| `affectedRows()` | `int` | INSERT/UPDATE/DELETE count. |
| `pipelineAborted()` | `bool` | Pipeline-aborted status. |
| `operator[](int row)` | `Row` | Row proxy (no ownership). |
| `sliceRow` / `sliceRows` (static) | `PgResult` | Zero-copy single-/multi-row views sharing the same `PGresult` (fans a fused ANY-batch to per-key waiters). |

**`PgResult::Row`** — lightweight, non-owning proxy:

| Member | Returns | Notes |
|---|---|---|
| `get<T>(int col)` | `T` | Typed column. Specializations: `std::string`, `std::string_view`, `int32_t`, `int64_t`, `double`, `bool`, and `std::vector<Scalar>` (text-format PG arrays). Bad parse throws `PgError`. |
| `getOpt<T>(int col)` | `std::optional<T>` | `nullopt` when the column is NULL. |
| `isNull(int col)` | `bool` (noexcept) | NULL check. |
| `rawValue(int col)` | `std::string_view` (noexcept) | Raw bytes (borrows the `PGresult`). |
| `index()` | `int` (noexcept) | This row's index. |

**Errors** — no error enums; the I/O layer throws exception types (caught by the awaiter and rethrown from `await_resume`):

| Type | Base | Meaning |
|---|---|---|
| `io::PgError` | `std::runtime_error` | Any PostgreSQL-layer error. |
| `io::PgNoRows` | `PgError` | Query returned no rows. |
| `io::PgConnectionError` | `PgError` | Connection-level PG failure. |
| `io::RedisError` | `std::runtime_error` | Any Redis-layer error. |
| `io::RedisConnectionError` | `RedisError` | Connection-level Redis failure. |

> Catch `PgError`/`RedisError` to handle a whole layer; catch `PgNoRows` etc. for the specific case. There is no `error_code`-style enum — branch on type.

→ Guides: [runtime.md](runtime.md), [foreign-event-loops.md](foreign-event-loops.md)

---

## Annotations

`@relais` annotations are markers placed in C++ comments that the generator (`scripts/generate_entities.py`) scans to emit the Mapping. Struct-level annotations sit on comment lines immediately above the `struct`; field-level annotations go in a trailing `// @relais ...` comment on the member line; list pagination uses `@relais_list`. This table is an **index** — the full semantics, examples, and constraints live in [entities.md](entities.md).

| Annotation | Role (one line) | Details |
|---|---|---|
| `@relais table=name` | PostgreSQL table name (else derived from class name with a warning). | [entities.md](entities.md#struct-level) |
| `@relais read_only` (or `read_only=true`) | Read-only entity: no insert/update/patch, empty `Field` enum. | [entities.md](entities.md#struct-level) |
| `@relais model=name` | Parsed but ignored — `table=` is authoritative. | [entities.md](entities.md#struct-level) |
| `@relais_list limits=10,25,50` | List pagination limits (first = default, last = max). | [entities.md](entities.md#struct-level) |
| `@relais_list entity=Fqn` | Override the fully-qualified entity name for the list Descriptor. | [entities.md](entities.md#struct-level) |
| `primary_key` | Marks a primary-key field (repeat for a composite key). | [entities.md](entities.md#field-level) |
| `partition_key` | Partition column — enables single-partition DELETE pruning. | [entities.md](entities.md#field-level) |
| `db_managed` | Excluded from `INSERT` (DB-generated, e.g. serial id). | [entities.md](entities.md#field-level) |
| `timestamp` | Stored as `std::string` (ISO 8601). | [entities.md](entities.md#field-level) |
| `column=db_name` | Override the DB column name (defaults to field name). | [entities.md](entities.md#field-level) |
| `raw_json` | `glz::raw_json_t` — stored verbatim as a string column. | [entities.md](entities.md#field-level) |
| `json_field` | Struct (or `vector<Struct>`) serialized to/from a JSON column. | [entities.md](entities.md#field-level) |
| `enum` | Auto-resolve the DB↔enum mapping from `glz::meta<EnumType>`. | [entities.md](entities.md#field-level) |
| `enum=db1:V1,db2:V2` | Explicit DB↔enum mapping (overrides `glz::meta`). | [entities.md](entities.md#field-level) |
| `filterable[...]` | List filter — `filterable[:param][:op]`. Ops: `eq`/`ne`/`gt`/`ge`/`lt`/`le`/`in`/`nin` (aliases `gte`→`ge`, `lte`→`le`, `not_in`→`nin`); default `eq`. | [lists.md](lists.md) |
| `sortable[...]` | List sort — `sortable[:param][:asc\|\|desc]`; default direction `desc`. | [lists.md](lists.md) |

> **`nullable` is not a `@relais` annotation.** Field optionality (and thus `setNull` support in `patch`) derives from the C++ type being `std::optional<T>` — `FieldInfo<F>::is_nullable` mirrors the type, not a marker. Writing `// @relais nullable` adds an inert tag the generator does not consume.

> Full reference, examples, and constraints: **[entities.md](entities.md)**.

→ Guide: [entities.md](entities.md)
