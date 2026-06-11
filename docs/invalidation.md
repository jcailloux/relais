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

### Raw queries — `PgProvider` / `PgResult` contract

A resolver is the canonical escape hatch for a direct SQL query the declarative
layer can't express. The contract:

- **`PgProvider::queryArgs(sql, args...)`** — parameterized query, builds and
  keeps `PgParams` alive in the coroutine frame. Use `queryParams(sql, params)`
  for a pre-built `PgParams`, `query(sql)` for no params. All return
  `io::Task<io::PgResult>`. `sql`/`params` must outlive the `co_await`.
- **Row count is `result.rows()`** (returns `int`), **not** `size()` — there is
  no `size()`. Also `empty()`, `ok()` (command succeeded), `affectedRows()`.
- **`result[i]`** → a `Row`. **`row.get<T>(col)`** reads column index `col`
  (0-based `int`); specialized for `int64_t`, `int32_t`, `double`, `bool`,
  `std::string`, `std::string_view`. Use `row.getOpt<T>(col)` / `row.isNull(col)`
  for nullable columns. `get<T>` throws `PgError` on a parse failure.

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
list repo defines a `GroupKey` (typed filter values) and an `invalidateByTarget`
method; the resolver returns `ListInvalidationTarget<GroupKey>`. This API is
**cache-level agnostic** — identical for L1 (RAM) and L2 (Redis).

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

Each cached Redis page carries a 19-byte binary header with sort-bounds
metadata. For per-page granularity, a Lua script reads those headers atomically
(`GETRANGE`) and deletes only the affected pages — one Redis round-trip.

## Propagation behavior

**Modification ops** (`insert`, `update`, `erase`):

1. Fetch the old value first (for updates/deletes).
2. Run the DB operation.
3. Propagate to dependent caches with old/new entity data.

**Explicit `invalidate(id)`:**

1. Fetch the entity for propagation.
2. Invalidate all tiers.
3. Propagate to dependent caches.
</content>