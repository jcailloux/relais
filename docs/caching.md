# Caching, configuration & partial updates

relais composes up to three cache tiers at compile time:

```
L1 hit → return
L1 miss → L2 hit → fill L1 → return
L2 miss → DB → fill L2 → fill L1 → return
```

- **L1 (RAM)** — `shared_ptr<const Entity>`, per-loop `thread_local` shardmap.
- **L2 (Redis)** — BEVE binary, shared across instances.
- **L3 (PostgreSQL)** — source of truth, rows hydrated via `fromRow()`.

Which tiers exist is chosen by the `CacheConfig` NTTP — there is zero runtime
branching on configuration.

## CacheConfig — compile-time configuration

Configuration is a **structural aggregate** passed as a Non-Type Template
Parameter. Every field has a default; override only what differs, via
`consteval` fluent chaining.

```cpp
namespace config = jcailloux::relais::config;

struct CacheConfig {
    CacheLevel     cache_level     = CacheLevel::None;
    bool           read_only       = false;
    UpdateStrategy update_strategy = UpdateStrategy::InvalidateAndLazyReload;

    // L1 (RAM) — eviction is GDSF (score = frequency × cost), not TTL-driven
    Duration l1_ttl              = 1h;   // Duration{0} = no TTL (eviction only)
    uint8_t  l1_chunk_count_log2 = 3;    // 2^3 = 8 chunks (ChunkMap)

    // L2 (Redis)
    Duration l2_ttl           = 4h;
    bool     l2_refresh_on_get = false;
    L2Format l2_format        = L2Format::Binary;   // Binary (BEVE) | Json

    // Fluent chaining (consteval — compile-time only)
    consteval CacheConfig with_cache_level(CacheLevel) const;
    consteval CacheConfig with_read_only(bool = true) const;
    consteval CacheConfig with_update_strategy(UpdateStrategy) const;
    consteval CacheConfig with_l1_ttl(Duration) const;
    consteval CacheConfig with_l1_chunk_count_log2(uint8_t) const;
    consteval CacheConfig with_l2_ttl(Duration) const;
    consteval CacheConfig with_l2_refresh_on_get(bool) const;
    consteval CacheConfig with_l2_format(L2Format) const;
};
```

`Duration` wraps `std::chrono::duration` (whose private members bar it from NTTP
aggregates). `FixedString` lets string literals be template parameters.

### Bounding L1: TTL *and* memory budget

L1 has two independent bounds, and **you need at least one of them** or the
cache grows without limit.

**1. TTL (`l1_ttl`)** — a *staleness ceiling* against the source of truth, not
the eviction mechanism. The default `1h` is a safety net: invalidation covers
writes through this repo, but **not** out-of-band mutations (another instance, a
migration, manual SQL). Setting `with_l1_ttl(0s)` disables time expiry entirely.

**2. Memory budget (`RELAIS_L1_MAX_MEMORY`)** — the GDSF eviction trigger. GDSF
(score = frequency × cost) only evicts **when over budget**; with no budget set,
`isOverBudget()` always returns `false` and **GDSF never fires**. The budget is
read once from the environment as a byte count:

```bash
RELAIS_L1_MAX_MEMORY=536870912   # 512 MiB; unset/0/invalid = unlimited
```

The footgun is the combination:

| `l1_ttl` | Budget set? | L1 memory |
|---|---|---|
| `> 0` | no | Bounded by TTL (entries expire on access/sweep) |
| `> 0` | yes | Bounded by budget (GDSF), TTL trims staleness on top |
| **`0`** | **no** | **Unbounded — nothing evicts, nothing expires** ⚠️ |
| `0` | yes | Bounded by budget (GDSF) only |

So **`with_l1_ttl(0s)` is only safe with `RELAIS_L1_MAX_MEMORY` set.** Dropping
the TTL removes the *only* bound a budget-less deployment had. If you turn off
the TTL for instance-owned data, set a budget — and vice-versa, a budget alone
(TTL kept) is the most robust setup.

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
static Task<bool> update(const Key& id, EntityPtr e)
    requires MutableEntity<Entity> && (!Cfg.read_only);
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

Cache handling: L1 (and L2 if present) is invalidated *before* the update; the
re-fetched entity repopulates caches on the next `find`.

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

## API surface

### Core (all repos)

| Method | Returns | Constraint |
|---|---|---|
| `find(id)` | `Task<EntityPtr>` | — |
| `findJson(id)` | `Task<shared_ptr<const string>>` | — |
| `insert(entity)` | `Task<EntityPtr>` | `!read_only` |
| `update(id, entity)` | `Task<bool>` | `!read_only` |
| `patch(id, set<F>()...)` | `Task<EntityPtr>` | `!read_only`, `HasFieldUpdate` |
| `erase(id)` | `Task<optional<size_t>>` | `!read_only` |
| `invalidate(id)` | `Task<void>` | — |
| `updateJson(id, json)` | `Task<bool>` | `!read_only` |
| `updateBinary(id, data)` | `Task<bool>` | `!read_only`, `HasBinarySerialization` |

### LocalRepo (L1) extras

| Method | Description |
|---|---|
| `trySweep()` | Non-blocking cleanup of expired entries |
| `purge()` | Force cleanup of all expired entries |
| `size()` | Current L1 entry count |
| `warmup()` | Prime cache structures at startup |

List repositories add `query()`, `listSize()`, and a `ListDescriptorType` alias
— see [lists.md](lists.md).
</content>