# Running relais on your event loop

relais runs its I/O on event loops: each loop has its own connection pool, and a
repository call is served by the loop whose thread makes it. When your
application already runs an event loop, attach relais to it: handlers then call
relais directly, on their own thread. An L1 hit is a lookup in memory, and a miss
sends its query from that same thread: no thread hop, no lock.

| Your application runs | Attach relais with | See |
|---|---|---|
| Drogon | `attachDrogon(conninfo)`, before `run()` | [Drogon](#drogon) |
| a trantor `EventLoop` | `attach(io, conninfo)`, `io` a `TrantorIoContext` | [trantor](#trantor) |
| an asio or Boost.Asio `io_context` | `attach(ctx, conninfo)` | [asio and Boost.Asio](#asio-and-boostasio) |
| a libuv loop | `attach(io, conninfo)`, `io` a `LibuvIoContext` | [libuv](#libuv) |
| another loop: GLib, Qt, libevent, your own | `attachHere(conninfo)`, and one descriptor to watch | [Any other loop](#any-other-loop-one-descriptor) |
| no event loop | `io::IoPool`, which runs loops of its own | [runtime.md › IoPool](runtime.md#no-event-loop-iopool) |

Each integration is one header of `jcailloux/relais/integration/`. Including it
needs the library it integrates; relais itself depends on none of them. A loop
with no integration can also get an adapter of its own:
[Writing an adapter](#writing-an-adapter).

## One pool per loop thread

These rules hold on every loop:

- **relais binds the thread that runs the loop.** Repository calls run there:
  from the loop's callbacks, handlers and coroutines. On a thread relais is not
  attached to, a call that needs PostgreSQL throws `std::logic_error`.
- **One thread per loop.** relais's state for a loop belongs to one thread, so a
  loop that several threads run cannot host it. With several threads, run one
  loop per thread and attach relais to each: each loop gets its own pool, and a
  request stays on its loop end to end.
- **Connections add up.** Each loop opens up to `max_connections`: keep their
  total under the server's `max_connections`.
- **`AttachConfig` sets up each loop**: `.pool` is its pool's `PgPoolConfig`
  (connections, timeouts, settling), `.max_concurrent` the sends it keeps in
  flight (default 8). Fields and defaults are in
  [api-reference.md › Attaching relais to a loop](api-reference.md#attaching-relais-to-a-loop);
  what the timeouts bound, in
  [runtime.md › Liveness & failure semantics](runtime.md#liveness--failure-semantics).

## Drogon

```cpp
#include <jcailloux/relais/integration/drogon.h>

int main() {
    jcailloux::relais::attachDrogon("host=localhost dbname=app user=app",
                                    {.pool = {.max_connections = 8}});
    drogon::app().addListener("0.0.0.0", 8080).setThreadNum(4).run();
}
```

relais binds each of Drogon's IO loops, where handlers run, once Drogon has
started them and before it serves a request. A coroutine handler awaits relais
directly:

```cpp
drogon::app().registerHandler("/users/{1}",
    [](drogon::HttpRequestPtr, int64_t id) -> drogon::Task<drogon::HttpResponsePtr> {
        auto user = co_await UserRepo::find(id);
        auto resp = drogon::HttpResponse::newHttpResponse();
        if (!user) resp->setStatusCode(drogon::k404NotFound);
        else resp->setBody(user->username);
        co_return resp;
    });
```

- Call `attachDrogon` once, before `run()`: it throws `AttachError` otherwise.
- If a loop cannot be bound (it fails relais's check, or PostgreSQL does not
  answer within `acquire_timeout`), Drogon quits without serving, and `run()`
  throws that error.
- When Drogon quits, relais leaves each loop before it stops, once the requests
  in flight on it have ended.
- Drogon's main loop, where beginning advices and `app().getLoop()` timers run,
  is not bound: a relais call there throws `std::logic_error`.

Tested with Drogon 1.9.13.

## Attaching to a loop: `attach` and `attachHere`

On trantor, asio, libuv and an adapter of your own, two calls bind relais to one
loop:

```cpp
// From another thread, while the loop runs: returns once relais is ready.
auto relais = jcailloux::relais::attach(io, conninfo, cfg);

// On the loop's own thread, from one of its callbacks or before it starts there:
// returns at once.
auto relais = jcailloux::relais::attachHere(io, conninfo, cfg, on_ready);
```

**`attach`** checks the loop in a few milliseconds (posts from another thread, a
watch, a timer, used as relais uses them), creates the pool on it, opens
`min_connections`, and binds the loop's thread. It waits for the loop throughout,
so the loop must be running, on another thread. It throws `AttachError` if the
loop fails the check (the message says what the loop did not do), if it is called
on the loop's own thread, or if relais is attached to that loop already. If the
pool cannot connect, it throws the connection's error: `io::PgConnectionError`,
or `io::PgPoolTimeout` after `acquire_timeout`.

**`attachHere`** binds the calling thread at once and returns: a repository call
made from then on waits for the pool's first connection. On the loop's first
turn, relais checks that the loop runs on this thread (another thread ends the
process: relais's state belongs to the thread that attached it), checks the loop
as `attach` does, within `acquire_timeout` (1 s at least), then connects the
pool. `on_ready(std::exception_ptr)` then runs once, on the loop's thread: with
a null pointer once the pool has `min_connections`, or with what failed. A
failure is also logged, and every PostgreSQL call on that thread fails with it,
the calls waiting and the later ones, until `detach()`. `on_ready` must not
throw, and never runs once `detach()` has begun. `attachHere` throws
`AttachError` if relais is attached to this thread already.

`attach` suits startup code that can wait for relais, before the server accepts
requests; `attachHere` suits code that runs on the loop's thread, or must not
block.

Both return an `Attachment`:

- `ready()` tells whether the pool has its first connections (always, once
  `attach` has returned); `error()` gives what `attachHere` could not start
  with.
- `detach()`, or the destructor, takes relais off the loop, which must still be
  running: relais waits there for the requests in flight (each bounded by its
  timeouts), unbinds the thread and closes the pool. From another thread,
  `detach()` returns once done, and the loop may then stop at once. On the
  loop's thread, it starts the teardown and returns at once: the loop must go on
  running until the teardown ends. On asio and libuv, `run()` returns once it
  has, if nothing else is pending.
- `io` must outlive the `Attachment`.

## trantor

```cpp
#include <trantor/net/EventLoopThread.h>
#include <jcailloux/relais/integration/trantor.h>

trantor::EventLoopThread thread;
thread.run();
jcailloux::relais::io::TrantorIoContext io(thread.getLoop());
auto relais = jcailloux::relais::attach(io, conninfo);
```

`io::TrantorIoContext` adapts a `trantor::EventLoop`, which must outlive it. On
the loop's thread, call `attachHere(io, conninfo)` instead. trantor has no
coroutine type of its own: a callback of the loop starts relais's work as a
coroutine, an [`io::DetachedTask`](api-reference.md#task--immediate--detachedtask)
for one whose result nobody awaits.

Tested with trantor 1.5.28.

## asio and Boost.Asio

```cpp
#include <asio.hpp>
#include <jcailloux/relais/integration/asio.h>   // boost_asio.h for Boost.Asio

asio::io_context ctx;
auto work = asio::make_work_guard(ctx);           // keeps run() from returning first
std::thread runner([&] { ctx.run(); });
auto relais = jcailloux::relais::attach(ctx, conninfo);
```

On the thread that runs the `io_context`, before `run()` or from a handler, call
`attachHere(ctx, conninfo)` instead. There is no adapter to build: relais keeps
one in the `io_context`, which must outlive the `Attachment`.

- **One thread per `io_context`.** `attach` refuses an `io_context` that several
  threads run (`AttachError`, before connecting). A thread that starts running
  it later, and runs one of relais's callbacks, ends the process. With several
  threads, run one `io_context` per thread and attach relais to each.
- After `detach()`, `run()` returns once nothing else is pending.
- asio re-arms a descriptor after each wake-up (one `epoll_ctl`): a request
  costs 5 system calls here, against 4 on the other loops.

### Awaiting relais from an asio coroutine: `relais::async`

An asio coroutine awaits asio operations only. `relais::async` runs a relais
call as one, completing with `(std::exception_ptr, T)` as `co_spawn` does,
through any completion token:

```cpp
asio::awaitable<void> show(int64_t id) {
    auto user = co_await jcailloux::relais::async(UserRepo::find(id), asio::use_awaitable);
    if (user) std::cout << user->username << '\n';
}
```

- Call it on the thread of an `io_context` relais is attached to:
  `std::logic_error` otherwise. The call starts at once.
- Any token works: `use_awaitable` (an exception is rethrown), `deferred` (the
  default from asio 1.29), `as_tuple`, a callback taking
  `(std::exception_ptr, T)`, `use_future`. The handler runs on its associated
  executor, by default the `io_context`.
- What the call refers to must outlive the operation, as an asio buffer does:
  the key passed to `find`, the SQL text and parameters of a `PgProvider` query.
  Awaited at once, as above, it does.
- A cancelled operation (`terminal` or `partial`, e.g. `||` against a timer)
  completes at once with `operation_aborted`. The request runs on, and its
  result is dropped; relais's cache stays valid.
- Each `relais::async` costs what an asio operation does: about 240 ns for a
  cache hit, which completes through a post to the `io_context`. A handler that
  makes several calls does better to group them in one relais `io::Task`, passed
  once to `relais::async`: each hit inside it costs about 2 ns.

```cpp
jcailloux::relais::io::Task<std::pair<std::string, std::string>> names(int64_t a, int64_t b) {
    auto first = co_await UserRepo::find(a);
    auto second = co_await UserRepo::find(b);
    co_return std::pair{first ? first->username : "", second ? second->username : ""};
}

auto [first, second] = co_await jcailloux::relais::async(names(a, b), asio::use_awaitable);
```

`relais::async` needs asio 1.27 or later (Boost 1.82 or later). With both
`asio.h` and `boost_asio.h` in one translation unit, every call to it is
ambiguous: call it from translation units that include only one.

Tested with asio 1.38.2 and Boost 1.83.

## libuv

```cpp
#include <jcailloux/relais/integration/libuv.h>

// On the loop's thread, before uv_run or from one of its callbacks:
jcailloux::relais::io::LibuvIoContext io(loop);
auto relais = jcailloux::relais::attachHere(io, conninfo);
```

From another thread, while the loop runs, call `attach(io, conninfo)` instead.
libuv lets only the loop's thread touch the loop, which sets the adapter's
lifecycle:

- Build and destroy the adapter on the loop's thread, or while no thread runs
  the loop.
- Destroy it after `detach()`, and before closing the loop's handles
  (`uv_walk`). It closes its own handles then, which libuv frees on its next
  turn: run the loop once more before `uv_loop_close`.
- relais's handles keep the loop running while relais has connections, as any
  handle does. Before that, and for `attach` to find the loop running, something
  of the application's must keep it running: a listening socket, a timer.

Tested with libuv 1.53.0.

## Any other loop: one descriptor

Any loop that can watch a file descriptor can host relais (Linux):

```cpp
#include <jcailloux/relais/integration/fd.h>

auto relais = jcailloux::relais::attachHere(conninfo);   // on the loop's thread
// watch relais.fd() for reading, and call relais.poll() whenever it is readable
```

relais then runs a loop of its own (an `io::EpollIoContext`), which yours drives
through `fd()`: the descriptor is readable while relais has work, and `poll()`
does what is ready, without waiting, and returns. Watch it level-triggered:
`poll()` leaves `fd()` readable while work remains, and your loop calls it
again. With GLib:

```cpp
auto relais = jcailloux::relais::attachHere(conninfo);
const guint watch = g_unix_fd_add(relais.fd(), G_IO_IN, [](int, GIOCondition, gpointer r) -> gboolean {
    static_cast<jcailloux::relais::FdAttachment*>(r)->poll();
    return G_SOURCE_CONTINUE;
}, &relais);

g_main_loop_run(loop);

g_source_remove(watch);  // stop watching fd() before detach() closes it
relais.detach();
```

On other loops, the watch is:

| Loop | Watch `relais.fd()` with |
|---|---|
| Qt | `QSocketNotifier` of type `Read`, its `activated` signal connected to `poll()` |
| libevent | an event of `EV_READ \| EV_PERSIST` whose callback calls `poll()` |
| epoll | `EPOLLIN`, without `EPOLLET` |
| `poll(2)` | `POLLIN` |

`attachHere(conninfo, cfg, on_ready)` binds the calling thread as
[`attachHere(io, …)`](#attaching-to-a-loop-attach-and-attachhere) does, and
returns an `FdAttachment`. relais checks nothing of your loop: it runs its own.

- Everything but `ready()` and `error()` runs on the thread that called
  `attachHere`: `poll()` and `detach()` throw `std::logic_error` elsewhere, and
  the destructor ends the process. Repository calls run on that thread too, and
  `on_ready` runs from `poll()`.
- `detach()` runs relais's loop until the requests in flight end, then closes
  the pool and `fd()`: stop watching `fd()` before it, and before destruction.
  It throws `std::logic_error` when called from inside `poll()`.
- A request costs what it costs on relais's own loop, plus your loop's wait on
  `fd()`: one system call more. A `poll()` with nothing to do costs about
  140 ns, and a cache hit needs none.

The GLib example runs in relais's CI (`tests/integration/test_glib.cpp`).

## Writing an adapter

An adapter makes a loop satisfy the `io::IoContext` concept
(`io/IoContext.h`): watches on file descriptors, callbacks posted to the loop,
one-shot timers. The integrations above are adapters, a hundred to three hundred
lines each (`integration/trantor.h`, `integration/libuv.h`): models for one of
your own. Compared with the descriptor, an adapter saves a system call per
request.

```cpp
struct MyIoContext {
    using WatchHandle = /* a copyable handle */;

    WatchHandle addWatch(int fd, jcailloux::relais::io::IoEvent mask,
                         std::function<void(jcailloux::relais::io::IoEvent)> cb);
    void updateWatch(WatchHandle watch, jcailloux::relais::io::IoEvent mask);
    void removeWatch(WatchHandle watch);
    void post(std::function<void()> cb);
    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb);
};
static_assert(jcailloux::relais::io::IoContext<MyIoContext>);
```

The contract of each method, what relais guarantees the adapter in return, and
three optional members are in
[api-reference.md › The `IoContext` concept](api-reference.md#the-iocontext-concept).
What an adapter most often gets wrong:

- `post()` queues the callback, and never runs it in place, even on the loop's
  thread: trantor's `runInLoop` runs it in place, its `queueInLoop` queues it.
- A watch lasts until its `removeWatch`: over one-shot waits, as asio's
  `async_wait`, wait again after each callback.
- `removeWatch` takes effect when it returns: a later watch on the same
  descriptor number receives that file's events, and nothing of the old watch.

The conformance harness checks the contract. `drive(io, pred)` runs the loop on
the calling thread until `pred()` holds, one bounded turn at a time; each check
runs it from the thread that calls `runAll`, and `runAll` throws
`testing::ConformanceError` naming the rule an adapter breaks:

```cpp
#include <jcailloux/relais/testing/IoContextConformance.h>

jcailloux::relais::testing::IoContextConformance::runAll(io, [](MyIoContext& ctx, auto pred) {
    while (!pred()) ctx.runOneTurn(std::chrono::milliseconds(10));  // your loop's bounded turn
});
```

On asio, for one, a turn is `ctx.run_one_for(10ms)`. Run the harness again when
you upgrade relais or the loop's library. Once it passes, `attach(io, …)` and
`attachHere(io, …)` bind relais to the loop as on the integrations above;
`attach` also checks the loop at startup, and names what it did not do.
