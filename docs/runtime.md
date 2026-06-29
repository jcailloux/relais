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
        └─ BatchScheduler              ← adaptive PG/Redis batching, per loop
             └─ PgPool / RedisPool     ← connections owned by THIS loop
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

## Easy path: the built-in `IoPool`

`io::IoPool` is the reference shared-nothing runtime — N epoll loops, each pinned
to a core with its own pools/batcher, each binding its own providers
automatically. You don't call `init()` yourself.

```cpp
#include <jcailloux/relais/io/IoPool.h>
using namespace jcailloux::relais;

io::IoPoolConfig cfg;
cfg.num_workers   = 4;                       // N loops (one per core)
cfg.pg_conninfo   = "host=localhost dbname=app user=app";  // empty → libpq PG* env
cfg.redis_host    = "127.0.0.1";
cfg.redis_port    = 6379;
cfg.pg_max_conns_per_worker = 8;             // total PG conns = N × this — keep < max_connections

auto pool = io::IoPool::create(cfg);         // blocks until all workers connected + bound

// Run repo work on a worker loop (e.g. a job; a web server runs handlers here):
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

## Bring your own loop (Drogon, asio, libuv, …)

**Do you need this?** Only if a web framework already owns the event loops your
requests run on. Bridging each call from a framework thread to `IoPool` then costs
a thread hop (~3 µs, two syscalls) — even on an L1 hit — and co-locating relais on
those existing loops removes it. If relais drives your runtime (jobs, or a server
built on `IoPool`), skip this section: no adapter needed.

If your framework already runs one epoll loop per core, run relais **inline on
those loops** instead — an L1 cache hit is then a pure in-process lookup
(~50 ns, zero hops). Write a small `IoContext` adapter for your loop, verify it
with the conformance harness, then `init()` per loop thread.

See **[foreign-event-loops.md](foreign-event-loops.md)** for the full recipe
(adapter sketch, the conformance harness, the per-loop bootstrap with `spawnOn`).

## Threading contract, summarized

- One `IoContext` event loop per thread; build `PgPool`/`RedisPool` **on** that
  loop (their connection I/O is async on the loop).
- `PgProvider::init(io, pgPool[, redisClient])` **on each loop thread** — binds
  that thread's `thread_local` providers. The optional Redis arg is a
  `RedisClient` (not a `RedisPool`); `init` wraps it into a single-client pool.
  Mono-loop: once. N-loop: once per loop.
- Repo calls (`find`/`insert`/`patch`/`erase`/…) run **on a bound loop thread**.
- No state is shared between loops — a request stays on its loop end to end.
- Bootstrapping from another thread (the loop is busy): drive the lazy
  `PgPool::create()` onto the loop with `spawnOn` and block on a `std::promise`;
  do the `init()` in its completion callback (which runs on the loop thread).

## Write ordering

Writes (`insert`/`patch`/`update`/`erase`/`eraseMany`) route through one **write
batch** per loop. Each takes a monotonic sequence number at submit time, and the
batch fires its writes in that order. Two guarantees follow:

- **Intra-flow (read-your-writes).** Within one coroutine, `co_await`-ing a write
  before issuing the next orders them by construction — the second doesn't start
  until the first's continuation resumes. Holds for any dependent pair, same
  batch or not.
- **Intra-batch.** Writes that land in the same batch fire in submission (`seq`)
  order. An `INSERT` then an `UPDATE` of the same PK, submitted in that order,
  execute in that order: the `UPDATE` matches the freshly-inserted row.

What the framework does **not** do: track write dependencies across *concurrent*
coroutines. Two coroutines writing the same key with no `co_await` between them
race — their relative order is undefined, and no I/O layer can resolve that
meaningfully (it is a business-logic race). Order dependent writes with `co_await`.

### Exception: `eraseWhereRaw` is not seq-ordered

`eraseWhereRaw` (the predicate `DELETE … WHERE <cond>` behind `eraseWhere`) does
**not** go through the write batch. Its SQL is a per-call `std::string` (unstable
pointer → not coalescing-safe) and it runs as a chunked `DELETE` loop with a data
dependency between chunks, so it stays on the read path (`queryParams`), outside
the seq-ordered write batch. A `eraseWhere` is therefore **not** `seq`-ordered
relative to batch writes; if you need it ordered against another write, `co_await`
it explicitly. (Its cache-invalidation branch is separate and *is* batched/
pipelined — entity-tier UNLINKs share one flush, like `invalidateMany`, plus one
predicate-driven list EVAL — this exception is about the `DELETE` itself.)

## Liveness & failure semantics

A repo call must never hang on a dead socket, and a *failed* call must never leave a
cache holding a value the database no longer agrees with. Two mechanisms cover this.

### Bounded waits, automatic recovery

Every wait a repo call can block on is bounded by a client-side timer:

- **`acquire_timeout`** (default 5 s) — acquiring a pooled connection, including the
  warm-up connect at startup, so an unreachable database fails `create()` instead of
  hanging forever.
- **`query_timeout`** (default 30 s) — each per-connection I/O wait, PostgreSQL *and*
  Redis.

These are a **liveness backstop, not an SLA**: they bound a silently blackholed
socket — dead network with no RST, frozen VM, firewall drop — that no server-side
timeout can catch. Delegate the normal cases to the server first: `connect_timeout`,
`keepalives`, and `statement_timeout` in the PG `conninfo` are cheaper and more
precise; the client timer is the independent floor underneath them. Setting either
to `0` disables it (discouraged — it reopens the hang).

A wait that trips its timer throws, and the connection it owns is **poisoned and
rebuilt** — but on different schedules. PG rebuilds *eagerly*: the next `acquire()`
reconnects transparently. Redis rebuilds *lazily*, off the cache self-heal pass — the
deferred retry that a failed L2 eviction schedules, which reconnects the dead client
before replaying. A read-only workload that never evicts never schedules one; its L2
reads simply keep degrading to L3 (a plain miss) until a write triggers the heal.
(Self-heal needs a way to dial a fresh connection: `IoPool` and `RedisPool::create`
wire it; `RedisPool::fromClients` only if you pass a reconnect factory, else a dead
client stays down.)

### The cache never goes stale under failure

The rule is one line: **a failure whose database effect is unknown evicts the cache
by precaution; a failure that provably changed nothing leaves it untouched.** It
plays out in three cases:

- **Reads propagate the error — they never fake an answer.** A failed entity or list
  read *throws*; it is never reported as "not found" or an empty page (a false 404).
  The one degradation is a Redis read timeout: L2 is only a cache, so the read falls
  through to L3 (PostgreSQL) instead of throwing.
- **A deterministic write failure** — the DB rolled back (connection refused, query
  rejected) — returns `nullopt`, leaving the cache as-is. The call is safe to retry.
- **An uncertain write** — a timeout, or a connection dropped *after* the request was
  flushed — may have committed. relais throws `io::PgUncertainError` and **evicts the
  affected entries first** (L2 before L1, by construction), so the next read
  re-fetches the truth. Because the outcome is an exception, not an ambiguous
  `nullopt`, a caller never mistakes "maybe committed" for "definitely failed" and
  double-writes.

> The error *type* is the signal: `io::PgUncertainError` (base of `PgQueryTimeout`
> and `PgConnectionLost`) is the "maybe committed" category; a plain `io::PgError`
> means the DB is unchanged. See
> [api-reference.md › errors](api-reference.md#pgprovider--pgresult--row--errors).
