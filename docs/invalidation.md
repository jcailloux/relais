# Cross-invalidation

When one entity changes, dependent caches elsewhere may go stale. relais
declares those dependencies as the variadic `Invalidations...` pack on `Repo`.
The `InvalidationMixin` sits atop the chain and intercepts `insert`/`update`/
`erase`/`patch`/`invalidate` to propagate — transparently, no extra API.

```cpp
using PurchaseRepo = Repo<PurchaseEntity, "Purchase", config::Local,
    cache::Invalidate<UserStatsRepo, &Purchase::user_id>,   // entity → entity
    cache::InvalidateList<PurchaseListRepo>                  // entity → list
>;
```

> **Prerequisite:** [concepts.md](concepts.md) and [entities.md](entities.md).
> This guide teaches *what/when* to declare; the exact template parameters of the
> four descriptors, the resolver overloads, and the batch-method signatures live
> in
> [api-reference.md › Invalidation descriptors](api-reference.md#invalidation-descriptors).

## The four mechanisms

| Mechanism | What it does | Use case |
|---|---|---|
| `Invalidate<Cache, KeyExtractor>` | Extract a key from the entity, invalidate that key in the target cache | Direct foreign key |
| `InvalidateList<ListRepo>` | Pass the full entity to a target list cache | Direct list invalidation |
| `InvalidateVia<Target, KeyExtractor, Resolver>` | Async resolver maps the source to target keys | Junction tables / indirect relationships |
| `InvalidateListVia<ListRepo, KeyExtractor, Resolver>` | Async resolver drives selective list-page invalidation | Indirect list invalidation |

## Indirect invalidation via a resolver

When the source entity doesn't carry the target key directly (e.g. through a
junction table), use `InvalidateVia` with an async resolver:

```cpp
struct UserToGuildsResolver {
    static io::Task<std::vector<int64_t>> resolve(int64_t user_id) {
        auto result = co_await PgProvider::queryArgs(
            "SELECT guild_id FROM guild_members WHERE user_id = $1", user_id);
        std::vector<int64_t> guild_ids;
        for (int i = 0; i < result.rows(); ++i)
            guild_ids.push_back(result[i].get<int64_t>(0));
        co_return guild_ids;
    }
};

using UserRepo = Repo<UserEntity, "User", config::Local,
    cache::InvalidateVia<GuildDetailRepo, &User::user_id,
        &UserToGuildsResolver::resolve>
>;
```

The resolver returns `Task<iterable<Key>>` (e.g. `Task<std::vector<int64_t>>`).
If the target cache has its own `Invalidations...`, those cascade automatically.

### Raw queries — the `PgProvider` escape hatch

A resolver is the canonical escape hatch for a direct SQL query the declarative
layer can't express. The `PgProvider` query entry points, the `PgResult`/`Row`
accessors, and the error types are specified in
[api-reference.md › PgProvider](api-reference.md#runtime-and-io). One gotcha
worth repeating inline: **the row count is `result.rows()` (an `int`), not
`size()`** — there is no `size()`.

```cpp
auto r = co_await PgProvider::queryArgs(
    "SELECT role_id FROM member_roles WHERE discord_user_id = $1", uid);
std::vector<int64_t> ids;
for (int i = 0; i < r.rows(); ++i)        // rows(), not size()
    ids.push_back(r[i].get<int64_t>(0));
```

## Selective list invalidation via `InvalidateListVia`

For indirect list invalidation where a source change should selectively drop
cached list *pages*, use `InvalidateListVia` with a typed resolver. The target
list repo defines a `GroupKey` (typed filter values) and **must provide** an
`invalidateByTarget(GroupKey, optional<sort_value>)` method that the resolver
drives; the resolver returns `ListInvalidationTarget<GroupKey>`. This API is
**cache-level agnostic** — identical for L1 (RAM) and L2 (Redis).

> `invalidateByTarget` is the target repo's responsibility — the shipped mixin
> ships the lower-level `invalidateListGroupByKey` / `invalidateAllListGroups`
> primitives it builds on, not `invalidateByTarget` itself.

```cpp
using Target = cache::ListInvalidationTarget<ArticleListRepo::GroupKey>;

struct PurchaseToArticleResolver {
    static io::Task<std::vector<Target>> resolve(int64_t user_id) {
        auto result = co_await PgProvider::queryArgs(
            "SELECT category, view_count FROM articles WHERE author_id = $1", user_id);
        std::vector<Target> targets;
        for (int i = 0; i < result.rows(); ++i) {
            Target t;
            t.filters.category = result[i].get<std::string>(0);
            t.sort_value = result[i].get<int64_t>(1);
            targets.push_back(std::move(t));
        }
        co_return targets;
    }
};

using PurchaseRepo = Repo<PurchaseEntity, "Purchase", config::Local,
    cache::InvalidateListVia<ArticleListRepo, &Purchase::user_id,
        &PurchaseToArticleResolver::resolve>
>;
```

### Three granularities

| Granularity | Resolver returns | Effect |
|---|---|---|
| **Per-page** | `sort_value` present | Only pages whose sort range includes the value |
| **Per-group** | `sort_value` absent (`nullopt`) | All pages in the targeted group |
| **Full pattern** | resolver returns `std::nullopt` | All groups in the repository |

Each cached Redis page carries a 19-byte binary header (magic + sort bounds +
flags). For per-page granularity, a Lua script reads those headers atomically
(`GETRANGE`) and deletes only the affected pages — one Redis round-trip.

## Batch and predicate erase / invalidate

Beyond the per-id `erase`/`invalidate`, four methods act on a set of rows in a
single cascade. `eraseMany`/`invalidateMany` take an enumerated key set;
`eraseWhere`/`invalidateWhere` take a predicate that resolves its own set
server-side. `erase*` removes the rows from L3; `invalidate*` only evicts caches
(rows persist, a later read repopulates).

| Method | Input | L3 | Lists | Return |
|---|---|---|---|---|
| `eraseMany(span<const Key>)` | enumerated keys | `DELETE … WHERE pk = ANY` | dropped (rows gone) | `optional<size_t>` rows deleted |
| `invalidateMany(span<const Key>)` | enumerated keys | untouched | kept (rows persist) | `void` |
| `eraseWhere(pred)` | `FilterSet<E>` predicate | chunked `DELETE … RETURNING` (ctid-bounded, `K_pg`/chunk) | dropped via predicate fast-path | `optional<size_t>` rows deleted |
| `invalidateWhere(pred)` | `FilterSet<E>` predicate | untouched | kept | `void` |

`optional<size_t>` mirrors mono `erase`: `nullopt` is a DB error, `0` a valid
non-matching set. The real gates: `eraseMany` and the `erase*` family require a
writable (`!read_only`) config; `invalidateMany` is unconditional;
`eraseWhere`/`invalidateWhere` require `HasFilterSet<E>`. All four are facades on
`Repo` regardless of preset — the cascade short-circuits in tiers a preset omits.

> Exact signatures (and the `eraseWhere`/`invalidateWhere` template form) are in
> [api-reference.md › Deletes & invalidation](api-reference.md#repository-api).

### Enumerated keys — `eraseMany` / `invalidateMany`

```cpp
std::vector<int64_t> ids = {1, 2, 3, 2};   // duplicates collapse by equality
auto n = co_await UserRepo::eraseMany(ids); // *n == 3 (deduped, absent ids skip)

co_await UserRepo::invalidateMany(ids);     // evict only, rows stay in L3
```

The input is `span<const Key>` — the zero-copy, caller-owned convention shared
with `findMany`. Both dedup the input by equality first (composite/partition
keys model `==` but not `<`). Across the hierarchy the cascade collapses to one
round-trip per tier: `eraseMany` issues one `DELETE WHERE pk = ANY($1) RETURNING`
(the `RETURNING` set is the affected-entity source — no extra read) and one
batched `UNLINK`; `invalidateMany` issues one batched `UNLINK`, sub-chunked at
1000 keys per command. Cross-invalidation declared via `Invalidations...` is **deduplicated**
across the whole set before propagating.

### Predicate — `eraseWhere` / `invalidateWhere`

The predicate is the generated named aggregate `FilterSet<E>`, built with
designated initializers (field names match the entity's `filterable` columns):

```cpp
auto n = co_await ArticleRepo::eraseWhere({.author_id = 42});
co_await ArticleRepo::eraseWhere({.category = std::string("tech")});

co_await ArticleRepo::invalidateWhere({.author_id = 42});  // evict only
```

`eraseWhere` fuses resolve and delete in a chunked, ctid-bounded
`DELETE … RETURNING` loop (`K_pg` rows per chunk, looping until a chunk deletes
nothing); `invalidateWhere` resolves via one `SELECT WHERE pred`, then evicts. The entity
tier evicts per resolved row (L1 + L2 + deduped cross-inval). The **own-list**
tier is driven by the predicate, not the resolved id set: `eraseWhere` emits
exactly **one `RangeModification` (L1) + one predicate-driven `EVAL` (L2)** for
the whole deleted set — O(1)/O(groups), filter-aware, never-miss — see
[the predicate list fast-path](lists.md#predicate-erase-and-invalidate). The
oracle holds: `invalidateWhere(P)` ≡ `invalidateMany(ids resolved by P)`.

## Propagation behavior

**Modification ops** (`insert`, `update`, `erase`):

1. Fetch the old value first (for updates/deletes).
2. Run the DB operation.
3. Propagate to dependent caches with old/new entity data.

**Explicit `invalidate(id)`:**

1. Fetch the entity for propagation.
2. Propagate to dependent caches.
3. Invalidate all tiers (L1 + L2).
</content>