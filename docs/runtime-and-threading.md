# Runtime & threading model

Repository calls like `co_await UserRepo::find(123)` don't talk to PostgreSQL
directly — they route through `PgProvider`, which dispatches to a per-loop
`BatchScheduler` + connection pools running on an **event loop**. Before any repo
call works you must stand up that runtime and bind it. This page covers how.

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

Providers are `thread_local`, so init() on thread A does nothing for thread B.
Debug builds assert this for adapters that expose `isInLoopThread()` (relais's
`EpollIoContext` does); otherwise the first repo call on an un-bound thread fails
loud with "called before init() on this thread".

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

## Bring your own loop (Drogon, asio, libuv, …)

If your framework already runs one epoll loop per core, run relais **inline on
those loops** instead — an L1 cache hit is then a pure `thread_local` lookup
(~50 ns, zero hops). Write a small `IoContext` adapter for your loop, verify it
with the conformance harness, then `init()` per loop thread.

See **[io-context-adapters.md](io-context-adapters.md)** for the full recipe
(adapter sketch, the conformance harness, the per-loop bootstrap with `spawnOn`).

## Threading contract, summarized

- One `IoContext` event loop per thread; build `PgPool`/`RedisPool` **on** that
  loop (their connection I/O is async on the loop).
- `PgProvider::init(io, pool[, redis])` **on each loop thread** — binds that
  thread's `thread_local` providers. Mono-loop: once. N-loop: once per loop.
- Repo calls (`find`/`insert`/`patch`/`erase`/…) run **on a bound loop thread**.
- No state is shared between loops — a request stays on its loop end to end.
- Bootstrapping from another thread (the loop is busy): drive the lazy
  `PgPool::create()` onto the loop with `spawnOn` and block on a `std::promise`;
  do the `init()` in its completion callback (which runs on the loop thread).
