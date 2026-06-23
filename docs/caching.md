# Caching, configuration & partial updates

> **Prerequisite:** the [mental model](concepts.md) — the mixin tower, the
> read/write flow, and why a tier you don't configure isn't in the binary. This
> guide is the how-to for the cache tiers; every method signature lives in
> [api-reference.md › Repository API](api-reference.md#repository-api).

relais composes up to three cache tiers at compile time:

```
L1 hit → return
L1 miss → L2 hit → fill L1 → return
L2 miss → DB → fill L2 → fill L1 → return
```

- **L1 (RAM)** — entities held **by value** in a process-global, sharded
  in-memory cache (`CacheTier` over `ChunkMap`) shared across all loop threads; reads
  borrow an epoch-guarded `CacheView<E>`, never a `shared_ptr`.
- **L2 (Redis)** — BEVE binary, shared across instances.
- **L3 (PostgreSQL)** — source of truth, rows hydrated via `fromRow()`.

Which tiers exist is chosen by the `CacheConfig` NTTP — there is zero runtime
branching on configuration.

## CacheConfig — compile-time configuration

Configuration is a **structural aggregate** passed as a Non-Type Template
Parameter. Every field has a default; override only what differs, via
`consteval` fluent chaining — each `with_*()` returns a new config at compile
time (see [Composing](#composing) below).

The full field list (tiers, TTLs, sizing, `update_strategy`, plus the
cross-instance and read-fill-guard knobs) with defaults, every preset, and all
`with_*()` signatures is the canonical reference in
[api-reference.md › CacheConfig](api-reference.md#cacheconfig). This guide covers
the fields it leans on: `cache_level`, `l1_ttl`, `l2_ttl`, `l2_format`,
`read_only`.

> `Duration` wraps `std::chrono::duration` (whose private members bar it from
> NTTP aggregates); `FixedString` lets string literals be template parameters —
> the structural stand-ins that make `.with_l1_ttl(30min)` and `Repo<E, "Name">`
> legal as template arguments.

### Bounding L1: TTL, and the optional memory budget

L1 grows without limit unless something reclaims entries. What's available
depends on **how relais was compiled**.

**1. TTL (`l1_ttl`) — always active.** A *staleness ceiling* against the source
of truth, not a memory cap. The default `1h` is a safety net: invalidation covers
writes through this repo, but **not** out-of-band mutations (another instance, a
migration, manual SQL). Expiry is **lazy** — an expired entry is dropped when it
is next looked up (`find()` evicts on read); there is no background sweep in the
default build, so a key written once and never read again lingers until touched.
`with_l1_ttl(0s)` disables time expiry entirely.

**2. Memory budget + GDSF (`RELAIS_L1_MAX_MEMORY`) — compile-time opt-in.**
Size-aware GDSF eviction (score = frequency × cost, evicts when over budget) and
the `RELAIS_L1_MAX_MEMORY` ceiling are **off unless you build with
`-DRELAIS_GDSF_ENABLED=1`** (the CMake option defaults `OFF`). In a default build
the budget env var is read but never consulted, GDSF never admits/scores/evicts,
and the insertion-driven sweep never runs — TTL is the only bound. With GDSF
enabled, the budget is a hard ceiling enforced by proactive eviction:

```bash
RELAIS_L1_MAX_MEMORY=536870912   # 512 MiB (GDSF builds only); unset/0/invalid = unlimited
```

The footgun, by build mode:

| Build | `l1_ttl` | L1 memory |
|---|---|---|
| default (no GDSF) | `> 0` | Bounded by TTL + access pattern (lazy expiry on lookup; cold entries linger) |
| default (no GDSF) | **`0`** | **Unbounded — nothing evicts, nothing expires** ⚠️ (the budget does nothing) |
| `-DRELAIS_GDSF_ENABLED=1` | `> 0` | Bounded by budget (GDSF proactive eviction); TTL trims staleness on top |
| `-DRELAIS_GDSF_ENABLED=1` | `0` | Bounded by budget (GDSF) only |

So in a **default build**, keep `l1_ttl > 0` — it is the only thing bounding L1.
To cap L1 by *memory* (a hard ceiling with proactive eviction), build with
`-DRELAIS_GDSF_ENABLED=1` and set `RELAIS_L1_MAX_MEMORY`.

## Presets

| Preset | Cache level | Use case |
|---|---|---|
| `config::Uncached` | None | Write-only tables, audit sinks |
| `config::Local` | L1 only | Per-instance data (default) |
| `config::Redis` | L2 only | Shared / cross-instance data |
| `config::Both` | L1 + L2 | High-read, feature flags |

### Composing

```cpp
// Start from a preset, override fields
inline constexpr auto ShortTtl   = config::Local.with_l1_ttl(std::chrono::minutes{5});
inline constexpr auto ReadOnly5m = config::Local.with_l1_ttl(5min).with_read_only();

// Use inline as a template argument
using MyRepo = Repo<MyEntity, "My", config::Local.with_l1_ttl(30min)>;
```

## Read-only repositories

```cpp
using AuditLogRepo = Repo<AuditLogEntity, "AuditLog",
    config::Local.with_read_only()>;
// find()                       — available
// insert(), update(), erase()  — COMPILE ERROR if called
```

Enforced via `requires` clauses, not runtime checks:

```cpp
static io::Task<bool> update(const Key& id, const E& entity)
    requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only);
```

## Partial updates with `patch`

Generated entities expose a `Field` enum for type-safe partial updates. Only the
named columns are written (dynamic `UPDATE ... SET` from
`FieldInfo::column_name`), then the full entity is re-fetched.

```cpp
using F = UserEntity::Field;
using jcailloux::relais::entity::set;
using jcailloux::relais::entity::setNull;

auto updated = co_await UserRepo::patch(id, set<F::balance>(999));

auto updated = co_await UserRepo::patch(id,
    set<F::balance>(999),
    set<F::username>("alice"));

co_await ArticleRepo::patch(id, setNull<F::view_count>());   // nullable → NULL
```

Requirements:

- Entity generated with a `TraitsType` carrying the `Field` enum + `FieldInfo`.
- Repository not `read_only`.
- Hand-written entities without `TraitsType` don't support `patch` — gated by
  the `HasFieldUpdate` concept.

Cache handling: L1 (and L2 if present) is evicted *before* the `UPDATE` runs;
`patch` then re-stores the fresh `RETURNING` row into L1 synchronously, so the
view it returns is already warm. Other tiers re-fill on the next read.

## Partition-key repositories

For PostgreSQL partitioned tables with a composite PK
(`PRIMARY KEY (id, region) PARTITION BY LIST (region)`), the repository key can
be a subset of the full PK. Annotate the partition column:

```cpp
// @relais table=events
struct Event {
    int64_t id = 0;         // @relais primary_key db_managed
    std::string region;     // @relais partition_key
    std::string title;
};

using EventRepo = Repo<EventEntity, "Event", config::Both>;
// Key = int64_t (id); HasPartitionKey auto-detected from the Mapping
```

The generator emits both `delete_by_pk` (`WHERE id = $1`, scans all partitions)
and `delete_with_partition` (`WHERE id = $1 AND region = $2`, single partition),
plus `makePartitionHintParams(entity)`.

At runtime `erase(id)` uses an **opportunistic hint** — it never adds a DB
round-trip just to prune:

```
LocalRepo::erase(id)
  └─ hint = L1 lookup (~0 ns, free)
RedisRepo::eraseImpl(id, hint)
  └─ if no hint: try L2 (~0.1 ms)
PgRepo::eraseImpl(id, hint)
  ├─ hint present: DELETE ... WHERE id=$1 AND region=$2   → 1 partition
  └─ no hint:      DELETE ... WHERE id=$1                 → N partitions
```

**Rule:** use the full key only when the entity is free (L1) or near-free (L2).

### Partition-key `patch`

`patch` builds `UPDATE ... WHERE pk=$N RETURNING *` from the partial key alone —
acceptable because `id` is indexed across partitions:

```cpp
auto updated = co_await EventRepo::patch(eventId,
    set<EF::title>(std::string("Updated")),
    set<EF::priority>(99));
// UPDATE events SET "title"=$1, "priority"=$2 WHERE "id"=$3 RETURNING *
```

## Batched reads with `findMany`

`findMany(ids)` reads many keys in one shot. It is available on L1-bearing repos
(`Local`, `Both`) and returns a `MultiView<E>` — a guarded, read-only view where
`view[i]` corresponds to `ids[i]`:

```cpp
std::vector<int64_t> ids = {7, 7, 4, -1};
auto view = co_await repo.findMany(ids);   // MultiView<E>
view.size();          // 4 — one slot per requested id
view[0];              // const E*  (id 7)
view[1];              // same pointer as view[0] — duplicate id, one entry
view[2];              // const E*  (id 4)
view[3];              // nullptr — absent in every tier
```

Contract:

- **Order preserved, holes are absent.** `view[i]` maps positionally to `ids[i]`;
  a `nullptr` slot means the id was found in no tier. Duplicate ids collapse to a
  single downstream entry and share one pointer.
- **One round-trip per tier.** The L1 misses are batched into a single `MGET`
  (L2) and a single `WHERE pk = ANY($1)` (L3) — never N sequential lookups. The
  saving scales with the miss count, not the request size.
- **L1 hits are zero-copy.** Hits return the live L1 slot pointer; nothing is
  copied or rehydrated.
- **Lifetime is the view's guard.** All slot pointers are pinned by one batch
  `EpochGuard` held inside the `MultiView`. They stay dereferenceable for as long
  as the view is alive — and must not outlive it.
- **Detached L2 warming.** On `Both`, L3 hits that missed L2 are written back to
  Redis fire-and-forget; the view returns without waiting for the fill.
- **Fast paths.** Empty `ids` → empty view, no guard, no I/O. All ids hitting L1
  → synchronous resolution with no coroutine frame (still allocates the dedup and
  view buffers — the batch wins on misses, not on the all-hit micro-path).

## Method signatures

Every repository method — reads, writes, deletes/invalidation, and the L1
maintenance calls — with exact return types and `requires` constraints lives in
[api-reference.md › Repository API](api-reference.md#repository-api). List repos
add `query()`/`listSize()` — see [lists.md](lists.md).
</content>