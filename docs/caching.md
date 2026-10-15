# Caching, configuration & partial updates

> **Prerequisite:** the [mental model](concepts.md) — the mixin tower, the
> read/write flow, and why a tier you don't configure isn't in the binary. This
> guide is the how-to for the cache tiers; every method signature lives in
> [api-reference.md › Repository API](api-reference.md#repository-api).

relais puts an optional in-process cache in front of PostgreSQL, chosen at
compile time:

```
L1 hit  → return
L1 miss → DB → fill L1 → return
```

- **L1 (RAM)** — entities held **by value** in a process-global, sharded
  in-memory cache (`CacheTier` over `ChunkMap`) shared across all loop threads; reads
  borrow an epoch-guarded `CacheView<E>`, never a `shared_ptr`.
- **PostgreSQL** — source of truth, rows hydrated via `fromRow()`.

Whether L1 exists is chosen by the `CacheConfig` NTTP — there is zero runtime
branching on configuration.

## CacheConfig — compile-time configuration

Configuration is a **structural aggregate** passed as a Non-Type Template
Parameter. Every field has a default; override only what differs, via
`consteval` fluent chaining — each `with_*()` returns a new config at compile
time (see [Composing](#composing) below).

The full field list (cache level, TTL, sizing, `update_strategy`, the
read-fill guard) with defaults, both presets, and all `with_*()` signatures is the
canonical reference in
[api-reference.md › CacheConfig](api-reference.md#cacheconfig). This guide covers
the fields it leans on: `cache_level`, `l1_ttl`, `read_only`.

> `Duration` wraps `std::chrono::duration` (whose private members bar it from
> NTTP aggregates); `FixedString` lets string literals be template parameters —
> the structural stand-ins that make `.with_l1_ttl(30min)` and `Repo<E, "Name">`
> legal as template arguments.

### Bounding L1: TTL, and the optional memory budget

L1 grows without limit unless something reclaims entries. What's available
depends on **how relais was compiled**.

**1. TTL (`l1_ttl`) — always active.** A *staleness ceiling* against the source of
truth, not a memory cap. The default `1h` is a safety net: invalidation covers
writes through this process, but **not** out-of-band mutations (another instance, a
migration, manual SQL) — see [Several instances](#several-instances). Expiry is
**lazy** — an expired entry is dropped when it is next looked up (`find()` evicts on
read); there is no background sweep in the default build, so a key written once and
never read again lingers until touched. `with_l1_ttl(0s)` disables time expiry
entirely.

**2. Memory budget + GDSF (`RELAIS_L1_MAX_MEMORY`) — compile-time opt-in.**
Size-aware GDSF eviction (score = frequency × cost ÷ entry size, evicts when over budget) and
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
| `config::Local` | L1 | Read-heavy data (default) |

Switching preset never breaks compilation: every `Repo` method has the same
signature, return type included, on both presets. Without L1, the L1
maintenance calls (`size`/`purge`/`warmup`) return `0` or do nothing.

### Composing

```cpp
// Start from a preset, override fields
inline constexpr auto ShortTtl   = config::Local.with_l1_ttl(std::chrono::minutes{5});
inline constexpr auto ReadOnly5m = config::Local.with_l1_ttl(5min).with_read_only();

// Use inline as a template argument
using MyRepo = Repo<MyEntity, "My", config::Local.with_l1_ttl(30min)>;
```

## Several instances

L1 lives in one process. A write invalidates the L1 of the process that made it,
never another's: an instance that cached a row keeps serving it until its
`l1_ttl` expires, whatever another instance wrote meanwhile. How to deploy
depends on who writes which rows:

| Deployment | L1 coherence |
|---|---|
| One instance | Exact: every write goes through this L1. |
| Several, each key written and read by one instance | Exact: route keys so ownership is disjoint. |
| Several sharing keys | Stale for up to `l1_ttl` on the instances that did not write. Choose `l1_ttl` as the tolerated staleness, or use `Uncached` for data that must never lag. |

`l1_ttl = 0` (no expiry) is only safe when no other process writes the table.

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
named columns are written (`UPDATE … SET` from `FieldInfo::column_name`), and the
same statement returns the committed row.

```cpp
using F = UserEntity::Field;
using jcailloux::relais::entity::set;
using jcailloux::relais::entity::setNull;
using jcailloux::relais::entity::increment;

auto updated = co_await UserRepo::patch(id, set<F::balance>(999));

auto updated = co_await UserRepo::patch(id,
    set<F::balance>(999),
    set<F::username>("alice"));

co_await ArticleRepo::patch(id, setNull<F::view_count>());   // nullable → NULL

co_await UserRepo::patch(id, increment<F::balance>(10));     // col = col + 10, in the DB
```

`increment` / `decrement` / `nowPlus` compute the value in the database, so
concurrent writers never lose each other's change. To write only when a condition
holds, or to pick rows by a predicate, see
[conditional-writes.md](conditional-writes.md).

Requirements:

- Entity generated with a `TraitsType` carrying the `Field` enum + `FieldInfo`.
- Repository not `read_only`.
- Hand-written entities without `TraitsType` don't support `patch` — gated by
  the `HasFieldUpdate` concept.

Cache handling: after the commit, the returned row is stored through L1, so the view
`patch` returns is already warm. With lists or cross-invalidation, the statement
also returns the row as it was before, so pages and targets are updated from both
versions without a prior read. On an uncertain outcome (`io::PgUncertainError`), the
entry is evicted from L1 before the exception propagates.

## Insert-or-update with `upsert`

`upsert(entity)` writes a row that may or may not already exist, in **one atomic
statement**: `INSERT … ON CONFLICT (pk) DO UPDATE SET <non-pk cols>=EXCLUDED.<col>
RETURNING <all cols>`. It returns the `CacheView<E>` of the row as it stands once
the conflict resolves — the database decides insert vs. update, not a prior read.

```cpp
using AccountRepo = Repo<AccountEntity, "Account", config::Local>;

auto view = co_await AccountRepo::upsert(makeAccount(id, balance));
// PK absent → INSERT; PK present → UPDATE of every non-PK column
```

### Applicability — caller-assigned primary keys only

The conflict target is the primary key, so the PK must be part of the INSERT —
**assigned by the caller**, never `db_managed`. A serial/identity PK is excluded:
its column isn't in the INSERT list, so there is nothing to conflict on. The
generator emits the `ON CONFLICT` SQL (and sets `Mapping::supports_upsert = true`)
only for an assigned-PK entity with at least one non-PK column; otherwise
`HasUpsertSql` is unsatisfied and **`upsert` is absent from the repository type** —
a compile error at the call site, not a runtime check.

| Entity shape | `upsert` |
|---|---|
| Assigned PK + ≥1 non-PK column | available |
| Serial / `db_managed` PK | absent — nothing to conflict on |
| All-PK junction (no non-PK column) | absent — nothing to `SET` |
| `read_only` repo | absent |

The PK is the only conflict target in this version; a UNIQUE-constraint target and
`ON CONFLICT DO NOTHING` for pure junctions are out of scope.

### Coherence and invalidation — identical to `update`

`upsert` **stores the committed `RETURNING` row through** L1,
unconditionally: it holds the authoritative row, so caching it is correct on both
branches. It is the one write that ignores `UpdateStrategy` — there is no
invalidate-then-reload variant, because the value to cache is already in hand. A
pre-existing L1 entry is superseded with the same generation bump `update` uses,
so a concurrent read-fill cannot reinstate the stale value.

List and cross-invalidation match `update` exactly. Membership is reconciled from a
**pre-image** (the row's prior state, read just before the statement): an insert
appends to matching pages and fires `propagateCreate`; an update **migrates** the
row between old and new pages when a filter/sort column changed and fires
`propagateUpdate(old, new)`. The pre-image read happens only when the chain carries
lists or cross-invalidation — a plain cached config is one SQL round-trip plus
store-through, no read.

### Failure — the `update` model, not `insert`'s

An **uncertain** write (timeout, mid-request connection loss) may have committed
over a key that already held a value, so on `io::PgUncertainError` `upsert` **evicts
the affected L1 entry and invalidates the possibly-affected pages by precaution**,
then rethrows (see the limit in [runtime.md › Failures and the
cache](runtime.md#failures-and-the-cache)). (`insert` evicts nothing on an uncertain
write; there was no prior value to go stale.) A *deterministic* failure returns an
empty view with nothing cached.

The insert-vs-update decision is read a moment before the statement runs, so a row
inserted concurrently in that window resolves as an update in SQL but propagates as
a create. **The write stays atomic**; only the list cleanup of the vanished old
state is skipped — exactly what `update` already tolerates when its pre-image is
absent, bounded by `l1_ttl`, timing not scope.

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

using EventRepo = Repo<EventEntity, "Event">;
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
PgRepo::eraseImpl(id, hint)
  ├─ hint present: DELETE ... WHERE id=$1 AND region=$2   → 1 partition
  └─ no hint:      DELETE ... WHERE id=$1                 → N partitions
```

**Rule:** use the full key only when the entity is already in L1.

### Partition-key `patch`

`patch` builds `UPDATE ... WHERE pk=$N RETURNING <all columns>` from the partial
key alone — acceptable because `id` is indexed across partitions:

```cpp
auto updated = co_await EventRepo::patch(eventId,
    set<EF::title>(std::string("Updated")),
    set<EF::priority>(99));
// UPDATE events SET title=$1, priority=$2 WHERE "id"=$3
//   RETURNING id, region, user_id, title, priority, created_at
```

## Batched reads with `findMany`

`findMany(ids)` reads many keys in one shot. It is available on every preset and
returns a `MultiView<E>` — a read-only view where `view[i]` corresponds to
`ids[i]`:

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

- **Order preserved, holes are absent.** `view[i]` maps positionally to `ids[i]`; a
  `nullptr` slot means the id was found neither in L1 nor in the database. Duplicate
  ids collapse to a single downstream entry and share one pointer.
- **One round-trip.** The ids L1 does not hold (all of them without L1) are batched
  into a single `WHERE pk = ANY($1)` — never N sequential lookups. The saving scales
  with the miss count, not the request size.
- **L1 hits are zero-copy.** Hits return the live L1 slot pointer; nothing is
  copied or rehydrated.
- **Lifetime is the view's.** L1 hit pointers are pinned by one batch
  `EpochGuard` held inside the `MultiView`; entities read from the database that
  L1 does not keep (all of them without L1) are moved into the view. Either way, every
  pointer stays dereferenceable for as long as the view is alive — and must not
  outlive it.
- **Fast paths.** Empty `ids` → empty view, no guard, no I/O, no coroutine
  frame. All ids hitting L1 → synchronous resolution with no coroutine frame
  (still allocates the dedup and view buffers — the batch wins on misses, not on
  the all-hit micro-path).

## Method signatures

Every repository method — reads, writes, deletes/invalidation, and the L1
maintenance calls — with exact return types and `requires` constraints lives in
[api-reference.md › Repository API](api-reference.md#repository-api). List repos
add `query()`/`listSize()` — see [lists.md](lists.md).
</content>