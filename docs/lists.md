# List cache — ListMixin

Paginated query results, cached in L1 with lazy validation via modification
tracking. **Auto-activated** when the entity's Mapping carries an embedded
`ListDescriptor` (detected by the `HasListDescriptor` concept) — no extra
configuration on the `Repo`.

## Declare a list entity

Annotate fields with `filterable` / `sortable`; pagination limits stay at class
level via `@relais_list`. The generator embeds a `ListDescriptor` in the
Mapping.

```cpp
// @relais table=audit_logs
// @relais_list limits=10,25,50,100
struct AuditLog {
    int64_t id = 0;            // @relais primary_key db_managed sortable:asc
    int64_t guild_id = 0;      // @relais filterable
    int64_t user_id = 0;       // @relais filterable
    std::string module;        // @relais filterable
    std::string action_type;   // @relais filterable
    std::string created_at;    // @relais timestamp sortable:desc
};
```

### `filterable` syntax

| Annotation | HTTP param | Operator |
|---|---|---|
| `filterable` | field name | EQ (default) |
| `filterable:custom_name` | `custom_name` | EQ |
| `filterable:ge` | field name | GE (known operator) |
| `filterable:date_from:ge` | `date_from` | GE |
| `filterable:in` | field name | IN (set membership) |
| `filterable:authors:in` | `authors` | IN |

Known operators: `eq`, `ne`, `gt`, `ge`/`gte`, `lt`, `le`/`lte`, `in`.

Multiple filters on one field (range queries):

```cpp
std::string created_at;  // @relais timestamp filterable:date_from:gte filterable:date_to:lte sortable:desc
```

### `in` operator (set membership)

`filterable:in` matches a column against a set of values. The HTTP param is a
comma-separated list (`?authors=1,2,3`); the set is canonicalized
(deduplicated, sorted) before use. Element types are `int64`, `int32`,
`std::string`, and `bool`; `enum` and field converters are rejected at compile
time. An empty or all-invalid set leaves the filter inactive. The set is bounded
at 256 elements.

Boolean filter values — for `in` and scalar `eq` alike — accept the standard
HTTP / HTML-form conventions, case-insensitively: `true/1/t/yes/y/on` and
`false/0/f/no/n/off`. Any other token leaves the filter inactive rather than
defaulting to `false`.

### `sortable` syntax

| Annotation | HTTP param | Direction |
|---|---|---|
| `sortable` | field name | DESC (default) |
| `sortable:asc` | field name | ASC |
| `sortable:desc` | field name | DESC |
| `sortable:custom_name:asc` | `custom_name` | ASC |

Column names are derived from the field name (override with `column=`).

## Query

```cpp
using AuditLogRepo = Repo<AuditLogEntity, "AuditLog">;   // list support auto-detected

#include <jcailloux/relais/list/spec/HttpQueryParser.h>

io::Task<std::string> handleAuditLogList(
    const std::unordered_map<std::string, std::string>& params)
{
    // Parse + validate against the ListDescriptor
    auto q = parseListQueryStrict<AuditLogRepo::ListDescriptorType>(params);
    if (!q) {
        // q.error() describes the validation failure
    }

    auto result = co_await AuditLogRepo::query(std::move(*q));  // L1 cached
    co_return *result.json();                                   // shared_ptr<const string>
}
```

`parseListQueryStrict` takes a generic map (default
`unordered_map<string, string>`) and validates every parameter against the
descriptor.

## Query from C++ code (no HTTP)

To query from C++ and get **structs back** — not JSON, not `shared_ptr` —
build the `ListQuery` directly. `Repo::ListQuery` is
`list::spec::ListDescriptorQuery<Descriptor>`; its fields are `filters`,
`sort`, `limit` (default 20), `cursor`, `offset`, `group_key`, `cache_key`.

```cpp
#include <jcailloux/relais/list/spec/HttpQueryParser.h>  // groupCacheKey / cacheKey live here
namespace ld = jcailloux::relais::list::spec;

AuditLogRepo::ListQuery q;
q.limit = 200;
q.filters.get<"user_id">() = uid;       // by name; value type is the field T

// MANDATORY: derive the cache keys before query(). Skipping this is a silent
// footgun — both keys stay empty, every call collides on the same key, and the
// cache returns stale/wrong pages with no error.
using Desc = AuditLogRepo::ListDescriptorType;
q.group_key = ld::groupCacheKey<Desc>(q);
q.cache_key = ld::cacheKey<Desc>(q);

auto view = co_await AuditLogRepo::query(q);   // CacheView<ListWrapper<Entity>>
for (const auto& e : view->items)              // items is std::vector<Entity>, BY VALUE
    use(e.user_id);                            // struct fields accessed directly
// view->size(), view->total_count, view->cursor() also available.
```

`items` is a `std::vector<Entity>` (by value). The `std::vector<EntityPtr>`
inside `CachedListResult` is the internal L2/Redis representation — not the
public `query()` API.

### Filter object model

Generated filters are `std::optional<T>` — **active** means `has_value()`.
Two accessors: `get<"field">()` (compile-time, by param name) and `get<N>()`
(by index). **Gotcha:** `get<N>()` indices follow the **alphabetical order of
param names** (the generator sorts filters for deterministic cache keys —
`generate_entities.py:1269`), *not* declaration order. Prefer `get<"name">()`
to stay immune to this.

### Behavior & gotchas

- **No active filter → the whole table** (paginated by `limit`). Relied on for
  "load this small table" — no `WHERE` clause is emitted when no filter is set.
- **`limit` is not clamped on the code path.** `parseListQueryStrict` *rejects*
  an out-of-range limit (HTTP), but a hand-built `ListQuery` passes `limit`
  straight into SQL. For "load everything", set `limit` above the row count
  yourself — exceeding `maxLimit` silently yields a truncated page, no error.
- **At least one `sortable` field is required.** The generator emits a
  `ListDescriptor` if *any* of `filterable`/`sortable`/`limits` is present, but
  `ValidListDescriptor` requires `HasSorts ≥ 1` (cursor pagination needs a
  deterministic order). `filterable` alone won't compile a usable list.
- **Composite keys are supported.** The keyset cursor spans every primary-key
  column, so an entity with a composite key (including an all-PK junction) can
  carry a `@relais_list`. Key components must be integers.

## CRUD → list notification (automatic)

ListMixin intercepts `insert`, `update`, `erase`, and `patch` to notify the list
cache of entity changes. The `ModificationTracker` records them; cached pages are
validated **lazily** on the next `query()`. No manual
`notifyCreated`/`notifyUpdated`/`notifyDeleted` for same-repo entities.

```cpp
// Done for you by ListMixin's CRUD interception:
listCache().onEntityCreated(entity);
listCache().onEntityUpdated(old_entity, new_entity);
listCache().onEntityDeleted(entity);
```

The tracker uses a monotonic generation counter (not timestamps); pages affected
by recent modifications are dropped on `get()`. "Affected" is decided by
`matchesFilters`: a cached page is dropped only if the changed entity matches
its active filter values. So `insert(uid, role)` drops the
`discord_user_id=uid` page but leaves `discord_user_id=other` pages intact.

For cross-repo list invalidation (a change in one table dropping another table's
list pages), see [`InvalidateListVia`](invalidation.md#selective-list-invalidation-via-invalidatelistvia).

## List methods (auto-detected)

| Method | Description |
|---|---|
| `query(ListQuery)` | Execute a paginated query (L1 cached) |
| `listSize()` | Current list-cache entry count |
| `trySweep()` | Non-blocking cleanup of entity + list L1 caches |
| `purge()` | Blocking full cleanup of entity + list L1 caches |
| `warmup()` | Prime both entity and list caches |
| `ListDescriptorType` | Alias for `parseListQueryStrict<>()` |
</content>