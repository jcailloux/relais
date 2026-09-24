# Conditional writes — deciding under concurrency

> **Prerequisite:** [partial updates with `patch`](caching.md#partial-updates-with-patch).
> Every signature lives in
> [api-reference.md › Conditional writes](api-reference.md#conditional-writes).

Some writes *decide*: take a seat, debit a balance, dequeue a job. Deciding with
"read, check in C++, then write" is a race — a concurrent writer can change the
row between the check and the write — and behind a cache the check may even run on
a stale value. relais moves the decision into the database: **one statement
checks the condition and writes**, under the row lock, on the committed value.

```cpp
using namespace jcailloux::relais::entity;   // set, increment, when, eq, …
using F = SeatEntity::Field;

// Take up to 10 free seats of show 7, held for 15 minutes.
auto seats = co_await SeatRepo::claim<{.mode = ClaimMode::UpTo}>(
    when(eq<F::show_id>(7), eq<F::state>(State::Free)), 10,
    set<F::state>(State::Held), set<F::holder>(cart),
    nowPlus<F::expires_at>(15min));
```

A value read with `find` is fit for display; a decision belongs in a guard.

## Which write

| | Rows targeted | How many | A row locked by a concurrent writer |
|---|---|---|---|
| `patchIf(id, when(…), updates…)` | one row, by its key | 0 or 1 | waited on, then re-checked |
| `claim(when(…), [orderBy(…)], n, updates…)` | the first n matching rows | exactly n, or at most n | skipped, or waited on |
| `patchWhere(when(…), updates…)` | every matching row | 0 to N | waited on, then re-checked |

- **You know the row** (a version check, a balance, a state transition) → `patchIf`.
- **You need some rows, any will do** (a job, a seat, a code from a batch) → `claim`.
- **You act on a whole set** (release a cart, expire orders) → `patchWhere`.

All three take the updates of `patch` and the guards below, on a generated entity
(`Field` enum) in a writable repository.

## Relative updates

When the new value depends on the current one, compute it in the database, so
concurrent writers never lose each other's change:

| Update | SQL | Field |
|---|---|---|
| `increment<F>(n)` | `col = col + n` | numeric, not `bool`, `NOT NULL` |
| `decrement<F>(n)` | `col = col - n` | same |
| `nowPlus<F>(duration)` | `col = now() + duration` | timestamp |

They mix with `set` / `setNull`, in `patch` too: `patch(id, increment<F::views>(1))`
is a counter that loses no increment.

- `decrement` does not stop at zero. Guard it —
  `patchIf(id, when(ge<F::stock>(n)), decrement<F::stock>(n))` — or declare
  `CHECK (stock >= 0)`. An overflow or a violated `CHECK` is a database error.
- `nowPlus` uses the database clock, at microsecond resolution; `nowPlus<F>(0s)`
  stamps the current time. Set deadlines with `nowPlus` and compare them with
  `dbNow` (below): instances whose clocks drift apart then never disagree on an
  expiry.
- Each call executes on its own. relais merges identical concurrent plain writes
  into one round-trip, but never a relative or conditional write: merged, two
  increments would count once, and two claims would receive the same rows.

## Guards: `when(…)`

A guard is evaluated by the database on the row being written, in the same
statement. `F` is a value of the entity's `Field` enum.

| Leaf | SQL |
|---|---|
| `eq<F>(v)`, `ne`, `gt`, `ge`, `lt`, `le` | `col = v`, `!=`, `>`, `>=`, `<`, `<=` |
| `lt<F>(dbNow)`, or any comparison with `dbNow` | `col < now()` — timestamp fields |
| `in<F>(values)`, `notIn<F>(values)` | `col = ANY(…)`, `col != ALL(…)` — a range or `{a, b}` |
| `isNull<F>()`, `isNotNull<F>()` | `col IS [NOT] NULL` — nullable fields |

Guards combine in at most two levels, checked at compile time:

- `when(a, b, …)` means `a AND b AND …`; it takes leaves or `anyOf`, at least one;
- `anyOf(a, b, …)` means `a OR b OR …`; it takes leaves or `allOf`;
- `allOf(a, b, …)` means `a AND b AND …`; it takes leaves.

```cpp
// Free, or held with an expired hold.
auto isFree = anyOf(eq<F::state>(State::Free),
                    allOf(eq<F::state>(State::Held), lt<F::expires_at>(dbNow)));
```

Any combination of column-to-value comparisons fits this form. A comparison
between two columns (`stock - reserved >= q`) does not.

- **NULL follows SQL**: a comparison on a NULL column is not true, so
  `lt<F>(x)` skips rows where `F` is NULL. Test NULL with `isNull<F>()`;
  `eq<F>(std::nullopt)` does not compile.
- **Only `Field` members** can be guarded or written. The primary key,
  `db_managed` and JSON fields are not members; `patchIf` targets the key through
  its `id` argument.
- **An empty set** makes `in` false and `notIn` true.

### Mapped enums

A field mapped with `@relais enum=free:Free,held:Held` takes, in guards as in
`set`, either the C++ value or the database string:

```cpp
when(eq<F::state>(State::Held))   // converted by the generated mapping, typo-proof
when(eq<F::state>("held"))        // as it arrives from a request or a file
```

On a `TEXT` column, an invalid string is stored as is, and `fromRow` reads it back
as the enum's default value. A PostgreSQL `ENUM` type or a `CHECK (col IN (…))`
turns it into a database error, for every writer, raw SQL included.

## `patchIf`

```cpp
auto r = co_await AccountRepo::patchIf(id,
    when(ge<F::balance>(amount)),
    decrement<F::balance>(amount));

if (!r)       { /* database error */ }
else if (!*r) { /* refused: balance too low, or no such account */ }
else          { /* (*r)->balance is the committed balance */ }
```

Returns `std::optional<CacheView<E>>`: `nullopt` on a database error, an empty
view when the guard is false **or** the row does not exist (the two are not
distinguished), otherwise the committed row.

## `patchWhere`

```cpp
auto released = co_await SeatRepo::patchWhere(
    when(eq<F::holder>(cart), eq<F::state>(State::Held)),
    set<F::state>(State::Free), setNull<F::holder>(), setNull<F::expires_at>());
// *released: how many seats the cart still held
```

Changes every matching row in one statement, and holds the lock of each changed
row until the statement ends.

## `claim`

```cpp
auto jobs = co_await JobRepo::claim<{.mode = ClaimMode::UpTo}>(
    when(eq<F::state>(JobState::Queued)),
    orderBy(desc<F::priority>(), asc<F::created_at>()), 10,
    set<F::state>(JobState::Running), set<F::worker>(me),
    nowPlus<F::lease_until>(30s));
```

Takes the first `n` matching rows and writes them, in one statement. The options
are a `ClaimOptions` template argument; name only those you change:

| Option | Value | Effect |
|---|---|---|
| `.mode` | `Exact` (default) | n rows or none: with fewer candidates, nothing is written |
| | `UpTo` | as many as available, at most n |
| `.lock` | `SkipLocked` (default) | a row locked by a concurrent writer is skipped; the next candidate is taken |
| | `Wait` | a locked row is waited on, re-checked, and skipped if it no longer matches |
| `.returns` | `After` (default), `Count`, `Changes` | see [Results](#results) |

**Order.** Without `orderBy`, rows are taken in primary-key order (oldest first
for a serial key). `orderBy` accepts:

- `asc<F>()`, `desc<F>()` — NULLs last in ascending order, first in descending
  order, as in PostgreSQL; override with `desc<F, Nulls::Last>()`;
- `first(guard)` — rows satisfying the guard first; a guard evaluating to NULL
  counts as unsatisfied.

The primary key always closes the order, so it is total and the result comes back
in it. An index matching the filter and the order —
`(state, priority DESC, created_at, id)` above — lets the database stop at the
n-th row; `first(…)` or an unindexed order sorts every candidate, which is fine
only when few rows match.

## Results

`patchWhere` and `claim` return what their `.returns` option asks for:

| `.returns` | Type | Default of |
|---|---|---|
| `Count` | `std::optional<size_t>` | `patchWhere` |
| `After` | `std::optional<std::vector<E>>` — each committed row | `claim` |
| `Changes` | `std::optional<std::vector<Change<E>>>` — `{before, after}` per row | — |

```cpp
auto log = co_await OrderRepo::patchWhere<{.returns = Returns::Changes}>(
    when(eq<F::state>(OrderState::Pending), lt<F::due_at>(dbNow)),
    set<F::state>(OrderState::Expired));
if (log) for (const auto& c : *log) audit(c.before, c.after);
```

**`nullopt` is a database error; an empty result is a refusal** — no row matched,
or too few for an `Exact` claim. Rows are returned by value. `claim` with `n == 0`
returns an empty result without querying.

On every write, `io::PgUncertainError` (timeout, connection lost once the query
was sent) propagates: the statement may or may not have committed. See
[runtime.md › Liveness & failure semantics](runtime.md#liveness--failure-semantics).

## Cache

The caller has nothing to invalidate:

- **`patchIf`** updates the cache like `patch`; the returned view is the committed
  row. A refusal or an error wrote nothing and leaves the cache as it was.
- **`patchWhere` / `claim`**: when the call returns, the changed entities are out
  of the cache, so the next `find` reads them from the database; Redis list pages
  and cross-invalidation targets are refreshed just after, in the background.
- **Uncertain outcome**: `patchIf` evicts its entity; the rows of `patchWhere` or
  `claim` cannot be known. What cannot be invalidated stays cached until
  `l1_ttl` / `l2_ttl`, and an error is logged. Decisions are unaffected: they are
  taken in the database.

## Limits

- **`SkipLocked` can refuse spuriously**: it skips a row whose writer then rolls
  back. Retry a refused claim when a candidate may have been busy.
- **`Wait` ranks before waiting**: after the wait, a row that no longer matches is
  skipped and the next candidate taken, but candidates keep the rank of their
  values before the wait.
- **Deadlocks**: statements that wait on each other's rows — `Wait` claims with
  different orders, overlapping `patchWhere` — can deadlock. PostgreSQL aborts one
  of them, a database error (`nullopt`): retry it.
- **One statement, one table.** See [Beyond one table](#beyond-one-table).

## Recipes

**Optimistic locking.** A `version` column, compared and bumped by the write:

```cpp
auto r = co_await DocRepo::patchIf(id, when(eq<F::version>(seen)),
    set<F::body>(text), increment<F::version>(1));
// empty view → saved by someone else since `seen`: reload and merge
```

**Job queue with a lease.** A job whose lease expired is claimable again, with no
sweeper; completing checks the lease is still ours:

```cpp
auto claimable = anyOf(eq<F::state>(JobState::Queued),
                       allOf(eq<F::state>(JobState::Running), lt<F::lease_until>(dbNow)));
auto jobs = co_await JobRepo::claim<{.mode = ClaimMode::UpTo}>(
    when(claimable), orderBy(desc<F::priority>()), 10,
    set<F::state>(JobState::Running), set<F::worker>(me), nowPlus<F::lease_until>(30s));

// Refused if the lease expired and another worker took the job.
co_await JobRepo::patchIf(job.id, when(eq<F::worker>(me), eq<F::state>(JobState::Running)),
    set<F::state>(JobState::Done));
```

**Scarce units with expiry** (seats, stock, licences). One row per unit is the
source of truth: a counter beside a reservation table would need both tables
written atomically. With `isFree` as defined above:

```cpp
auto units = co_await UnitRepo::claim(when(eq<F::product_id>(p), isFree), qty,
    set<F::state>(State::Held), set<F::cart_id>(cart), nowPlus<F::expires_at>(30min));

// Checkout: keep only the holds still alive. *kept < qty → an item was lost.
auto kept = co_await UnitRepo::patchWhere(
    when(eq<F::cart_id>(cart), eq<F::state>(State::Held), gt<F::expires_at>(dbNow)),
    set<F::state>(State::Paying));
```

**Preemption.** Withdraw one unit even if held: a free one first, else the latest
hold, never one being paid for. `Wait`, because skipping a unit locked by a write
in progress could miss the only candidate:

```cpp
auto r = co_await UnitRepo::claim<{.lock = Lock::Wait, .returns = Returns::Changes}>(
    when(eq<F::product_id>(p), in<F::state>({State::Free, State::Held})),
    orderBy(first(isFree), desc<F::expires_at>()), 1,
    set<F::state>(State::Withdrawn), setNull<F::cart_id>(), setNull<F::expires_at>());
// empty → every unit is being paid for; (*r)[0].before.cart_id → the cart that lost it
```

The dispossessed cart learns it at checkout, like an expired hold.

**Recovering from a crash.** Each write commits entirely or not at all. The
application stays recoverable when:

- each step is **one deciding statement**;
- **the deciding rows carry the link** (the `cart_id` set by the claim), with an
  identifier generated before the write;
- **other records are derived**: an order header written after the claim can be
  rebuilt from the rows carrying its identifier.

After an uncertain outcome, read the rows by that identifier from the database to
learn what you hold; left alone, they are released at their expiry.

## Beyond one table

The repositories write one table per statement, and relais opens no transactions:
each statement it sends may run on a different connection. A decision that writes
several tables atomically is a single raw statement whose data-modifying CTEs
write each table, sent with
[`PgProvider::queryWrite`](api-reference.md#pgprovider--pgresult--row--errors):

```cpp
static constexpr const char* kCheckout = R"(
    WITH taken AS (UPDATE stock SET qty = qty - $2 WHERE sku = $1 AND qty >= $2 RETURNING sku)
    INSERT INTO order_lines (order_id, sku, qty) SELECT $3, sku, $2 FROM taken
    RETURNING id)";
auto r = co_await PgProvider::queryWrite(kCheckout, params, io::batch::WriteMode::Exclusive);
// no row → the stock check failed, nothing was written
```

Pass `WriteMode::Exclusive`, otherwise an identical concurrent statement shares
this one's result instead of running. The repositories do not see this write:
call `invalidateMany(ids)` on each affected repository afterwards. It evicts the
entities; their list pages, and the cross-invalidation targets of their new
values, refresh at their TTL.
