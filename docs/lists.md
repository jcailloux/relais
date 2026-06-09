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

Known operators: `eq`, `ne`, `gt`, `ge`/`gte`, `lt`, `le`/`lte`.

Multiple filters on one field (range queries):

```cpp
std::string created_at;  // @relais timestamp filterable:date_from:gte filterable:date_to:lte sortable:desc
```

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
by recent modifications are dropped on `get()`.

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