# Runtime & threading model

Repository calls like `co_await UserRepo::find(123)` don't talk to PostgreSQL
directly — they route through `PgProvider`, which dispatches to a per-loop
`BatchScheduler` + connection pools running on an **event loop**. Before any repo
call works you must stand up that runtime and bind it. This page covers how.

> **Prerequisite:** [concepts.md](concepts.md) for the mental model. Exact
> signatures (`IoPool`/`IoPoolConfig`, `IoPool::create`, `PgProvider::init`,
> the `Task`/`spawnOn`/`Outcome` family) live in
> [api-reference.md › Runtime and I/O](api-reference.md#runtime-and-io).

> This page covers the **I/O runtime** (loops, pools, binding). The separate
> always-on background `RuntimeThread` (cached clock + L1 memory tick) starts
> itself — you never wire it; see
> [foreign-event-loops.md › The background runtime thread](foreign-event-loops.md#the-background-runtime-thread-you-dont-wire-it).

## The model in one picture

```
UserRepo::find(id)                     ← your code, on an event-loop thread
   └─ PgProvider::query/...            ← thread_local service locator
        └─ BatchScheduler              ← adaptive PG batching, per loop
             └─ PgPool                 ← connections owned by THIS loop
                  └─ EpollIoContext / your adapter  ← the event loop
```

`PgProvider`'s providers are **`thread_local`**: each event-loop thread binds its
own pool/batcher. A Task co_awaited on loop K routes to loop K's resources with
**no cross-thread hop** (shared-nothing). One loop = simplest; N loops scale
throughput ~linearly with cores at unchanged per-request latency.

## The one rule

> **Call `PgProvider::init(io, pool)` ON the event-loop thread it serves, once
> per loop.** Repo calls must then run on that same loop thread.

Providers are `thread_local`, so `init()` on thread A binds nothing for thread B.
In debug builds a repo call on a thread that never ran `init()` trips an `assert`
— `PgProvider::query() called before init() on this thread (providers are
thread_local — init() must run on each loop thread)`. `init()` itself also asserts
`isInLoopThread()` on adapters that expose it (relais's `EpollIoContext` does),
catching an `init()` on the wrong thread.

## Built-in standalone runtime: `IoPool`

`io::IoPool` is the reference shared-nothing runtime — N epoll loops, each pinned
to a core with its own pools/batcher, each binding its own providers
automatically, so you never call `init()` yourself. Reach for it when relais drives
the process: a standalone service, a batch job, or background workers with no other
framework owning your loops.

```cpp
#include <jcailloux/relais/io/IoPool.h>
using namespace jcailloux::relais;

io::IoPoolConfig cfg;
cfg.num_workers   = 4;                       // N loops (one per core)
cfg.pg_conninfo   = "host=localhost dbname=app user=app";  // empty → libpq PG* env
cfg.pg_max_conns_per_worker = 8;             // total PG conns = N × this — keep < max_connections

auto pool = io::IoPool::create(cfg);         // blocks until all workers connected + bound

// Run repo work on a worker loop (e.g. a job or a standalone service):
pool->workerIo(0).post([] {
    [] () -> io::DetachedTask {
        auto user = co_await UserRepo::find(123);
        // ...
    }();
});
```

`create()` returns once every worker has connected and bound its providers. Repo
calls only work **inside a coroutine running on a worker loop** (here, posted to
`workerIo(0)`); calling them from an un-bound thread asserts/throws.
`io::DetachedTask` is a fire-and-forget coroutine — you wrap the body in one
because `post()` takes a plain callable, not an awaitable (see
[api-reference.md › Runtime and I/O](api-reference.md#runtime-and-io)).

> The snippet sets the fields that matter most; the full `IoPoolConfig` field
> list with defaults is in
> [api-reference.md › Runtime and I/O](api-reference.md#runtime-and-io).

## Co-locate on your framework's loops (Drogon, asio, libuv, …)

When a web framework already owns the event loops your requests run on, that is
where relais should run too — **inline on those loops**, not bridged to a separate
`IoPool`. This is the path for a service built on an existing framework. Bridging
each call to an `IoPool` thread would instead cost a hop (~3 µs, two syscalls) on
every call, even an L1 hit; co-located, that same hit stays a pure in-process
lookup (~50 ns, zero hops).

If your framework runs one epoll loop per core, write a small `IoContext` adapter
for its loop, verify it with the conformance harness, then `init()` per loop
thread.

See **[foreign-event-loops.md](foreign-event-loops.md)** for the full recipe
(adapter sketch, the conformance harness, the per-loop bootstrap with `spawnOn`).

## Threading contract, summarized

- One `IoContext` event loop per thread; build `PgPool` **on** that loop (its
  connection I/O is async on the loop).
- `PgProvider::init(io, pgPool)` **on each loop thread** — binds that thread's
  `thread_local` providers. Mono-loop: once. N-loop: once per loop.
- Repo calls (`find`/`insert`/`patch`/`erase`/…) run **on a bound loop thread**.
- No state is shared between loops — a request stays on its loop end to end.
- Bootstrapping from another thread (the loop is busy): drive the lazy
  `PgPool::create()` onto the loop with `spawnOn` and block on a `std::promise`;
  do the `init()` in its completion callback (which runs on the loop thread).

## Write ordering

Repository writes route through one **write batch** per loop. A write with none
in flight goes straight to PostgreSQL; the writes submitted meanwhile depart
together when it completes, split by row over up to half of `max_concurrent`
connections, which commit in parallel.

- **Awaited writes run in order.** `co_await`-ing a write before issuing the next
  orders them: the second does not start until the first has committed. This is
  the one way to order dependent writes.
- **Writes not awaited in turn have no defined order.** Issued from concurrent
  coroutines or a `whenAll`, two writes on different rows may commit in either
  order: a child row inserted alongside its parent can fail its foreign key.
  Within a batch, writes on the same row keep their submission order, but nothing
  tells the caller whether two writes share one.

Reads keep read-your-writes too. A read identical to one already sent on the loop
(same statement, same parameters) takes that read's result instead of sending its
own, but only if no write has completed on the loop since that read was sent. A read
issued after an awaited write therefore always sees it. Writes made outside the loop
(another process) are not tracked: a shared result can predate them by at most the
round-trip of the read in flight.

## Liveness & failure semantics

A repo call must never hang on a dead socket, and a *failed* call must never leave a
cache holding a value the database no longer agrees with. Two mechanisms cover this.

### Bounded waits, automatic recovery

Every wait a repo call can block on is bounded by a client-side timer:

- **`acquire_timeout`** (default 5 s) — acquiring a pooled connection, including the
  warm-up connect at startup, so an unreachable database fails `create()` instead of
  hanging forever.
- **`query_timeout`** (default 30 s) — each per-connection I/O wait.

These are a **liveness backstop, not an SLA**: they bound a silently blackholed
socket — dead network with no RST, frozen VM, firewall drop — that no server-side
timeout can catch. Delegate the normal cases to the server first: `connect_timeout`,
`keepalives`, and `statement_timeout` in the PG `conninfo` are cheaper and more
precise; the client timer is the independent floor underneath them. Setting either
to `0` disables it (discouraged — it reopens the hang).

A wait that trips its timer throws, and the connection it owns is **poisoned and
rebuilt**: the next `acquire()` reconnects transparently. The server does not see
the client leave until it next reads the socket, so relais sets each session's
`client_connection_check_interval` to `client_check_interval` (`PgPoolConfig`,
`IoPoolConfig`; default 1 s): the server then aborts the abandoned statement
within that interval instead of running it to the end. `0` sends nothing.

### Failures and the cache

The rule is one line: **a failure whose database effect is unknown evicts the cache
by precaution; a failure that provably changed nothing leaves it untouched.** It
plays out in three cases:

- **Reads propagate the error — they never fake an answer.** A failed entity or list
  read *throws*; it is never reported as "not found" or an empty page (a false 404).
- **A deterministic write failure** — the DB rolled back (connection refused, query
  rejected) — returns `nullopt`, leaving the cache as-is. The call is safe to retry.
- **An uncertain write** — a timeout, or a connection dropped *after* the request was
  flushed — may have committed. relais throws `io::PgUncertainError` and **evicts the
  affected entries first**, so the next read re-fetches the truth. Because the
  outcome is an exception, not an ambiguous `nullopt`, a caller never mistakes
  "maybe committed" for "definitely failed" and double-writes.

> The error *type* is the signal: `io::PgUncertainError` (base of `PgQueryTimeout`
> and `PgConnectionLost`) is the "maybe committed" category; a plain `io::PgError`
> means the DB is unchanged. See
> [api-reference.md › errors](api-reference.md#pgprovider--pgresult--row--errors).

### Settling an uncertain write

The abandoned statement can still commit after the eviction, and a read in between
can cache the prior value. relais therefore **settles** every uncertain write: off
the request path, it waits until the write can no longer commit, then evicts the
same entries again. **No failure leaves the cache stale past its settling.**

- **Window.** Until the write is settled, a read may return the prior value.
  Settling takes a few round-trips when PostgreSQL answers (`settle_delay` under
  `Delay`); while it does not, relais retries (from 50 ms, doubling up to 2 s),
  and the window lasts until it answers.
- **Rolled back.** When the server reports the write rolled back, nothing more is
  evicted.
- **Connections.** Settling runs on a connection of its own per event loop, outside
  the pool, open only while a write waits: count one more per loop against the
  server's `max_connections`.
- **PostgreSQL 14** or later.

`uncertain_settle` (`PgPoolConfig`, `IoPoolConfig`) chooses what settling waits for:

| Mode | The write is settled once… | Use it when |
|---|---|---|
| `Terminate` (default) | relais has terminated the server backend that ran it (`pg_terminate_backend`) | relais connects to PostgreSQL directly |
| `Await` | that backend has exited on its own, once the statement ended (`client_check_interval` cuts it short) | relais must not end server sessions |
| `Delay` | `settle_delay` has elapsed; `0` takes the session's `statement_timeout` + 1 s | a transaction pooler sits in front of PostgreSQL |
| `Off` | never: the first eviction is the only one | stale reads after a failure are acceptable |

**Behind a transaction pooler** (PgBouncer `pool_mode = transaction`), a client
connection changes server backend from one transaction to the next: `Terminate` and
`Await` would watch, or end, a backend now serving another client, and a session
setting would leak to other clients. Use `Delay` (it sends no session setting), and
bound every write with a `statement_timeout` on the role or the database: a
`settle_delay` shorter than it settles too early.

**Where the promise stops.** In these cases a prior value read during the failure
stays cached until `l1_ttl`:

- `Off`;
- `Delay` with neither `settle_delay` nor a `statement_timeout` (warned at pool
  creation);
- the server refusing to settle (the role may not terminate the backend) when no
  `statement_timeout` applies; with one, settling waits it out (+ 1 s) instead. The
  refusal is logged as an error.
