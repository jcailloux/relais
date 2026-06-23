# List cache — ListMixin

Paginated query results, cached in L1 with lazy validation via modification
tracking. **Auto-activated** when the entity's Mapping carries an embedded
`ListDescriptor` (detected by the `HasListDescriptor` concept) — no extra
configuration on the `Repo`. The generator emits a `ListDescriptor` only when the
entity has **at least one `sortable` field**; `filterable` alone gives you
`eraseWhere`/`invalidateWhere` (see [Behavior & gotchas](#behavior--gotchas)) but
no cached list.

> **Prerequisite:** [concepts.md](concepts.md) and [entities.md](entities.md).
> This guide teaches list declaration and querying; the exact builder,
> query-type, cursor, and parser signatures live in
> [api-reference.md › List and query API](api-reference.md#list-and-query-api).

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
| `filterable:nin` | field name | NIN (set anti-membership) |
| `filterable:authors:not_in` | `authors` | NIN |

Known operators: `eq`, `ne`, `gt`, `ge`/`gte`, `lt`, `le`/`lte`, `in`,
`nin`/`not_in`.

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

### `nin` operator (set anti-membership)

`filterable:nin` (alias `not_in`) is the logical inverse of `in`: it matches a
column whose value is in *none* of the set. It shares the `in` machinery
end-to-end (same param, canonicalization, element types, 256 bound, compile-time
rejections); SQL uses `!= ALL($n)`, mirroring `in`'s `= ANY($n)`. Two semantics
differ from `in`:

- **NULL is excluded, not included.** A NULL column value matches neither `in`
  nor `nin` — `value NOT IN (…)` is SQL three-valued NULL, so the row drops out.
  `nin` is *not* the negation of `in` on NULL rows; it is on every non-NULL row.
- **The empty set is the universe.** `nin {}` matches everything (`!= ALL('{}')`
  is `TRUE`), the exact opposite of `in {}` matching nothing. As with `in`, an
  empty set is unreachable over HTTP (an empty param leaves the filter inactive,
  which coincides with "match everything"); it exists only by construction.

<details><summary>Consumer note — cost and invalidation churn</summary>

`!= ALL($n)` is non-sargable on Postgres (anti-membership), so a `nin` filter is
bounded by keyset pagination, not an index seek. Invalidation churn is higher
than `in` — the complement is far less selective, so a write is more likely to
fall inside a cached `nin` group and purge its page. Per-operation cost is
identical to `in` (same `cmpin`/`skipset`, same Redis round-trip).
</details>

### `sortable` syntax

| Annotation | HTTP param | Direction |
|---|---|---|
| `sortable` | field name | DESC (default) |
| `sortable:asc` | field name | ASC |
| `sortable:desc` | field name | DESC |
| `sortable:custom_name:asc` | `custom_name` | ASC |

Column names are derived from the field name (override with `column=`).

### `limits` syntax

`@relais_list limits=…` declares the page-size grid for the model — a
class-level annotation, comma-separated, any length:

```cpp
// @relais_list limits=5,10,20,50,100
```

The generator sorts and deduplicates the grid, then emits three members on the
`ListDescriptor`: `allowedLimits` (the array), `defaultLimit` (its front),
`maxLimit` (its back). Omitting `limits=` yields the grid `{10,25,50}`.

The grid is the source of truth for the page size and feeds the canonical cache
key, so a model's accepted page sizes are fixed at compile time. The two parsers
read it differently:

| | absent `limit` param | out-of-grid `limit` |
|---|---|---|
| `parseListQuery` (tolerant) | `defaultLimit` | rounds up to the next step, caps at `maxLimit` |
| `parseListQueryStrict` (strict) | `defaultLimit` | `InvalidLimit` error |

A hand-written descriptor with no `allowedLimits` member falls back to the grid
`{10,25,50,100}`, and to a default page size of `20` (the `ListQuery` struct
default).

## Query

```cpp
using AuditLogRepo = Repo<AuditLogEntity, "AuditLog">;   // list support auto-detected

#include <jcailloux/relais/list/spec/HttpQueryParser.h>

io::Task<std::string> handleAuditLogList(
    const std::unordered_map<std::string, std::string>& params)
{
    // Parse + validate against the ListDescriptor
    auto q = parseListQueryStrict<AuditLogRepo::ListDescriptorType>(params);
    if (!q)
        co_return R"({"error":"invalid query"})";  // q.error() has the detail

    auto result = co_await AuditLogRepo::query(std::move(*q));  // L1 cached, CacheView
    co_return result->json();                                   // ListWrapper::json() → std::string
}
```

`parseListQueryStrict` takes a generic map (default
`unordered_map<string, string>`) and validates every parameter against the
descriptor. `query()` returns a guarded list view (`CacheView<ListWrapper<Entity>>`)
either way; here `result->json()` serializes it to a `std::string`, while the
[C++ section](#query-from-c-code-no-http) below reads the struct accessors
(`view->items`) on the same view. (For JSON directly, `queryJson(q)` returns an
`io::Immediate<std::string>` and skips the view.)

## Query from C++ code (no HTTP)

To query from C++ and get **structs back** — not JSON, not `shared_ptr` —
build the query with `Repo::queryBuilder()`. The builder is the primary
construction path: it sets filters and sort **by name** (compile-time verified)
and `.build()` is the single point that seals it into the immutable
`Repo::ListQuery` — the only type `query()` accepts.

```cpp
auto q = AuditLogRepo::queryBuilder()
    .filter<"user_id">(uid)        // by name; value type is the field T
    .sortDesc<"created_at">()      // name + direction checked at compile time
    .limit(200)                    // exact — trusted path, no grid normalization
    .build();                      // seals → AuditLogRepo::ListQuery

auto view = co_await AuditLogRepo::query(q);   // CacheView<ListWrapper<Entity>>
for (const auto& e : view->items)              // items is std::vector<Entity>, BY VALUE
    use(e.user_id);                            // struct fields accessed directly
// view->size(), view->total_count, view->cursor() also available.
```

The cache keys (`groupKey`/`cacheKey`) are computed **once at seal time** from
the final params — there is no manual key derivation and no way to forget it. A
query that reaches `query()` always carries keys consistent with its contents,
by construction.

The full builder surface — every setter, its exact slot type, and
`.build()`/`.params()` — is in
[api-reference.md › QueryBuilder](api-reference.md#list-and-query-api). The
example above is the common chain; `.build()` is the single sealing point.

`items` is a `std::vector<Entity>` (by value). The `std::vector<EntityPtr>`
inside `CachedListResult` is the internal L2/Redis representation — not the
public `query()` API.

### Sealed query and the params escape hatch

`Repo::ListQuery` is `list::spec::ListQuery<Descriptor>` — immutable,
constructible **only** through `seal()`, not default-constructible, with no
settable fields (const getters only; see
[api-reference.md › ListQuery](api-reference.md#list-and-query-api)).

When the builder's fixed chain doesn't fit — assembling params in a loop, from a
table, or mutating after the fact — drop to the mutable bundle and seal
explicitly:

```cpp
namespace ld = jcailloux::relais::list::spec;
using Desc = AuditLogRepo::ListDescriptorType;

AuditLogRepo::ListQueryParams p;          // mutable: filters, sort, limit, cursor, offset
p.limit = 200;
p.filters.get<"user_id">() = uid;
auto q = ld::seal<Desc>(std::move(p));    // computes both keys, yields ListQuery

// Re-seal after mutating a sealed query (e.g. advancing the cursor):
auto p2 = q.params();
p2.cursor = next;                         // next is an AuditLogRepo::Cursor
auto q2 = ld::seal<Desc>(std::move(p2));
```

### Keyset cursor

The keyset cursor is descriptor-tagged: `Repo::Cursor` is
`list::spec::TypedCursor<Descriptor>`. The wire token is unchanged — a page's
`view->cursor()` is still a base64 `std::string`; you decode the token you
previously emitted and pass the result to `.after()`:

```cpp
auto cursor = AuditLogRepo::Cursor::decode(token);   // optional<AuditLogRepo::Cursor>
if (!cursor) { /* malformed token — reject at the trust boundary */ }

auto next = AuditLogRepo::queryBuilder()
    .filter<"user_id">(uid)
    .sortDesc<"created_at">()
    .after(*cursor)
    .build();
```

`decode()` is the **only** way to obtain a cursor from a token (the raw
constructor is private). The tag makes one kind of mistake a compile error:
handing `OtherRepo::Cursor` to this repo's `.after()` (or assigning it into this
repo's params) does not compile — distinct types, no conversion.

What the tag does **not** catch: `Repo::Cursor::decode` does not verify that a
token actually originated from *this* descriptor's pages. A well-formed token
minted by another list decodes to a same-typed cursor whose keyset bytes are
meaningless for this query — provenance can't be a compile-time fact for an opaque
token off the wire. Re-emit only the `next_cursor` a page handed you.

### Filter object model

Generated filters are `std::optional<T>` — **active** means `has_value()`.
Access is by param name only: `get<"field">()` (compile-time lookup) or the
builder's `.filter<"name">()`. There is no positional accessor — filter slots
follow the **alphabetical order of param names** (the generator sorts filters by
param name for deterministic cache keys), *not* declaration order, so an index
never coincides with the declaration site and is a footgun.

### Behavior & gotchas

- **No active filter → the whole table** (paginated by `limit`). Relied on for
  "load this small table" — no `WHERE` clause is emitted when no filter is set.
- **`limit` is not clamped on the code path.** `parseListQueryStrict` *rejects*
  an out-of-range limit (HTTP), but a hand-built `ListQuery` passes `limit`
  straight into SQL. For "load everything", set `limit` above the row count
  yourself — exceeding `maxLimit` silently yields a truncated page, no error.
- **At least one `sortable` field is required.** The generator embeds a
  `ListDescriptor` **only when the entity has ≥1 sort**; `filterable` alone
  yields a `FilterSet` but no `ListDescriptor`, so `HasListDescriptor` fails and
  `ListMixin` is never activated. Cursor pagination needs a deterministic order —
  so `filterable` alone won't give you a cached list (it still powers
  `eraseWhere`/`invalidateWhere`).
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

## Predicate erase and invalidate

`eraseWhere(pred)` / `invalidateWhere(pred)` (see
[batch and predicate erase / invalidate](invalidation.md#batch-and-predicate-erase--invalidate))
delete or evict a set of rows in one call — but they treat the list tier
differently.

**`eraseWhere`** deletes the rows, so their pages must change. The entity tier is
evicted per resolved row, but the **own-list** tier never iterates that id set —
it is driven directly by the predicate, in O(1)/O(groups):

- **L1**: one `RangeModification`. The `ModificationTracker` carries a second
  track (predicate + generation) alongside the per-entity one, sharing the
  generation counter and the bitmap/drain cycle. A cached page is dropped lazily
  on the next `query()` when the predicate is *range-affecting* — group-compatible
  **and** sort-range-overlapping. The per-entity check is the `lo == hi` special
  case of this range check.
- **L2**: one predicate-driven `EVAL` over the cached groups (`pmatch` set-vs-set
  + `chk_range` overlap), one Redis round-trip.

This fast-path is **filter-aware and never-miss**: it prunes only groups the
predicate provably cannot affect (EQ-different / IN-disjoint; an absent
constraint is a wildcard that matches). It is **always** taken — there is no
`purgeAll` fallback for large sets. `eraseWhere` appends its `RangeModification`
on **every** call, even at zero rows deleted — the fast-path fires
unconditionally.

**`invalidateWhere`** does *not* touch the list tier at all. It resolves the
matching rows (`SELECT … WHERE pred`, so its entity tier is O(rows)) and evicts
only the entity caches; the rows still exist, so cached list pages stay valid
(the oracle `invalidateWhere(P) ≡ invalidateMany(ids resolved by P)`).

## List methods

A list-enabled repo adds `query()` and `listSize()`, and its L1 maintenance
calls (`sweep`/`purge`/`warmup`) cover both the entity and list caches;
`ListDescriptorType` is the alias you pass to `parseListQuery*<…>` and the
builder. Exact signatures and the full set of `Repo` list aliases are in
[api-reference.md › Repository API](api-reference.md#repository-api) and
[› List and query API](api-reference.md#list-and-query-api).
</content>