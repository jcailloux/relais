# Writing an `IoContext` adapter (running relais on a foreign event loop)

relais is generic over the [`IoContext`](../include/jcailloux/relais/io/IoContext.h)
concept. `EpollIoContext` is the bundled implementation, but `PgPool<Io>`,
`RedisPool<Io>` and the coroutine awaiter machinery accept **any** type that
satisfies the concept. This lets you run relais **inline on another framework's
event loop** instead of bridging across threads.

> **Most users don't need this.** The built-in `IoPool` runtime already delivers
> the shared-nothing N-loop model and ~50 ns L1 hits. Write an adapter only when a
> web framework already owns the loops your requests run on, and the per-request
> bridge to `IoPool` (~3 µs) is a measured cost worth removing. Read
> [runtime.md](runtime.md) first.

## Why you'd want this

If your HTTP framework already runs one epoll loop per core (Drogon/trantor,
asio, libuv, seastar…), construct the relais pools **on those same loops**:

- an **L1 cache hit** is then a pure in-process `ChunkMap` lookup (~50 ns, zero
  thread hops, zero syscalls — L1 is a process-global sharded cache, not per-loop
  state);
- only real **L2/L3 misses** do async I/O, inline on the same thread.

Bridging from the framework's threads to dedicated relais loops instead costs a
hop + two syscalls per request, even on an L1 hit. Co-locate unless you can't fit
one DB/Redis pool per front-end loop within your connection budget.

## Ownership

The adapter depends on **both** relais and your framework, so it is glue and
belongs in **your** code (or a small separate integration target), not in relais
core — relais must not take a dependency on a specific web framework, and it
cannot track that framework's API across versions. relais's responsibility ends
at the concept and the conformance harness below.

## The contract

Satisfy six methods (plus the `WatchHandle` and `TimerToken` member types). The
semantic rules (the part a concept can't express) are documented on the concept
and enforced by the harness:

| relais method | what it must do |
|---|---|
| `WatchHandle addWatch(int fd, IoEvent mask, cb)` | register `fd`; call `cb(events)` on the loop thread when ready, with matching `IoEvent` bits |
| `void updateWatch(WatchHandle, IoEvent mask)` | change the active mask on that handle |
| `void removeWatch(WatchHandle)` | stop all callbacks for that handle; **must be safe to call from inside that handle's own callback** — defer teardown of loop-owned state |
| `void post(std::function<void()>)` | run once, on the loop thread, FIFO; thread-safe; **wake a blocked loop promptly** |
| `TimerToken postDelayed(duration, cb)` | run `cb` once, on the loop thread, after the delay; return a cancellable token. Used to flush a batch on an adaptive deadline |
| `void cancelTimer(TimerToken)` | cancel a pending `postDelayed`; no-op if already fired or unknown |

> The exact concept signatures and member types, plus the
> `IoContextConformance::runAll` and `spawnOn` signatures, are in
> [api-reference.md › Runtime and I/O](api-reference.md#runtime-and-io).

## Sketch: trantor (Drogon)

```cpp
#include <jcailloux/relais/io/IoContext.h>
#include <trantor/net/EventLoop.h>
#include <trantor/net/Channel.h>
#include <chrono>
#include <memory>
#include <unordered_map>

namespace relais = jcailloux::relais;

class TrantorIoContext {
public:
    using WatchHandle = int;                 // the fd doubles as the handle
    using TimerToken  = trantor::TimerId;     // trantor's own timer id

    explicit TrantorIoContext(trantor::EventLoop* loop) : loop_(loop) {}

    WatchHandle addWatch(int fd, relais::io::IoEvent ev,
                         std::function<void(relais::io::IoEvent)> cb) {
        using relais::io::IoEvent;
        auto ch = std::make_unique<trantor::Channel>(loop_, fd);
        // trantor splits events by type; relais gets one mono-bit call per type
        // (PgConnection/RedisConnection test each bit, so that is fine).
        ch->setReadCallback ([cb] { cb(IoEvent::Read); });
        ch->setWriteCallback([cb] { cb(IoEvent::Write); });
        // EPOLLERR / EPOLLHUP → IoEvent::Error, like EpollIoContext does.
        ch->setErrorCallback([cb] { cb(IoEvent::Error); });
        ch->setCloseCallback([cb] { cb(IoEvent::Error); });
        applyMask(*ch, ev);
        channels_[fd] = std::move(ch);
        return fd;
    }

    void updateWatch(WatchHandle fd, relais::io::IoEvent ev) {
        if (auto it = channels_.find(fd); it != channels_.end())
            applyMask(*it->second, ev);
    }

    void removeWatch(WatchHandle fd) {
        if (auto it = channels_.find(fd); it != channels_.end()) {
            it->second->disableAll();
            it->second->remove();   // detach from the poller (loop thread)
            // removeWatch is often called FROM the Channel's own callback (relais
            // drops its watch when I/O state changes, e.g. EOF — permitted by the
            // concept). Destroying the Channel here is a use-after-free: trantor
            // regains control in handleEventSafely() after the callback and still
            // writes into it. Erase the map entry now (the fd is reusable again)
            // but DEFER destruction to queueInLoop — it runs after this turn's
            // events are processed, when nothing touches the Channel anymore.
            trantor::Channel* dying = it->second.release();
            channels_.erase(it);
            loop_->queueInLoop([dying] { delete dying; });
        }
    }

    // queueInLoop is thread-safe and wakes the loop via its own eventfd.
    void post(std::function<void()> cb) { loop_->queueInLoop(std::move(cb)); }

    // BatchScheduler flushes a batch on an adaptive deadline via these.
    TimerToken postDelayed(std::chrono::nanoseconds delay,
                           std::function<void()> cb) {
        double seconds = std::chrono::duration<double>(delay).count();
        return loop_->runAfter(seconds, std::move(cb));  // trantor: delay in seconds
    }

    void cancelTimer(TimerToken token) { loop_->invalidateTimer(token); }

private:
    static void applyMask(trantor::Channel& ch, relais::io::IoEvent ev) {
        using relais::io::hasEvent;
        using relais::io::IoEvent;
        hasEvent(ev, IoEvent::Read)  ? ch.enableReading()  : ch.disableReading();
        hasEvent(ev, IoEvent::Write) ? ch.enableWriting()  : ch.disableWriting();
    }

    trantor::EventLoop* loop_;
    std::unordered_map<int, std::unique_ptr<trantor::Channel>> channels_;
};

static_assert(relais::io::IoContext<TrantorIoContext>);
```

> Pinned to a specific trantor API — **validate it with the harness** before
> trusting it. The version above is the shape an adapter that passes `runAll`
> converges to.

> **Self-removal is the trap.** relais calls `removeWatch(h)` from inside watch
> `h`'s own callback (a connection drops its watch on EOF/error). The loop is
> still mid-dispatch and resumes after the callback returns, so freeing the
> watch's loop-owned state — the `trantor::Channel`, or even the `std::function`
> you are invoking through — synchronously inside `removeWatch` is a
> use-after-free. Defer the free past the current event: `queueInLoop(delete)`
> for trantor; copy the callback onto the stack before invoking for a raw epoll
> loop (see `EpollIoContext::runOnce`). The harness exercises this directly (C10),
> so a synchronous-teardown adapter fails `runAll` under ASan instead of
> corrupting memory in production.

## Verify it

The harness needs one author-supplied primitive: `drive(io, pred)`, which pumps
the loop **on the calling thread** until `pred()` is true, re-checking `pred`
between iterations. trantor has no native "run once", so you synthesize it:

```cpp
#include <jcailloux/relais/testing/IoContextConformance.h>

trantor::EventLoop loop;            // binds to the constructing thread
TrantorIoContext io(&loop);

auto drive = [&loop](TrantorIoContext&, auto pred) {
    // C7 (cross-thread post) calls drive() from ANOTHER thread; a trantor loop
    // is pinned to its constructing thread, so re-bind it or assertInLoopThread
    // (compiled out in Release → segfault) fires.
    if (!loop.isInLoopThread()) loop.moveToCurrentThread();
    while (!pred()) {
        loop.queueInLoop([&loop] { loop.quit(); });
        loop.loop();  // process ~one iteration, then quit
    }
};

jcailloux::relais::testing::IoContextConformance::runAll(io, drive);
```

Two non-obvious pitfalls the harness will catch if you get them wrong:

- **EventLoop is thread-bound.** It belongs to the thread that constructed it.
  The cross-thread check (C7) drives from a different thread, so `drive` must
  re-bind with `moveToCurrentThread()`. In Release the assert is compiled out and
  you get a silent segfault instead of a clear failure.
- **One iteration per `pred` check, not episodic polling.** A level-triggered
  watch on an un-drained eventfd refires every loop turn. If `drive` runs many
  turns between `pred` checks, the watch fires hundreds of times and the
  `fired == 1` checks (C1, C6) fail. The `queueInLoop(quit)` trick caps it to one
  turn per check.

Once `runAll` passes, construct `PgPool<TrantorIoContext>` /
`RedisPool<TrantorIoContext>` on each loop and `co_await` relais `Task`s
directly inside your coroutine handlers — no thread hop.

## Bootstrapping the pools (and scaling to N loops)

`PgProvider`'s providers are **`thread_local`**: each loop thread binds its OWN
pool/batcher by calling `PgProvider::init(io, pool)` **on that loop thread**. A
Task co_awaited on loop K then routes to loop K's pool — shared-nothing, no
cross-thread hop. Mono-loop is N=1; for N loops you run this once per loop and
throughput scales ~linearly.

There is one unavoidable cross-thread moment per loop: `PgPool::create()` is a
*lazy* coroutine that does its connection I/O **on the loop thread**, but you
kick it off at startup from another thread (the loop is already running and you
must not block it). [`spawnOn`](../include/jcailloux/relais/runtime/Spawn.h)
drives it on the loop, and its completion callback — which runs **on the loop
thread** — is exactly where `init()` must bind the thread_local providers:

```cpp
#include <jcailloux/relais/runtime/Spawn.h>

// Run this once per loop, each from a thread OTHER than that loop's (else you'd
// block the loop on work posted to itself — deadlock). e.g. from Drogon's
// registerBeginningAdvice over getIOLoop(0..N-1).
void initRelaisOnLoop(trantor::EventLoop* loop, const char* conninfo,
                      size_t min, size_t max) {
    auto io = std::make_unique<TrantorIoContext>(loop);  // kept alive program-long
    auto* io_ptr = io.get();
    std::promise<std::shared_ptr<PgPool<TrantorIoContext>>> ready;
    auto fut = ready.get_future();

    relais::spawnOn(*io_ptr,
        PgPool<TrantorIoContext>::create(*io_ptr, conninfo, min, max),
        [&ready, io_ptr](relais::Outcome<std::shared_ptr<PgPool<TrantorIoContext>>> r) {
            // ON the loop thread: bind THIS loop's thread_local providers.
            if (r) { relais::PgProvider::init(*io_ptr, *r); ready.set_value(*r); }
            else   { ready.set_exception(r.error()); }
        });

    auto pool = fut.get();  // blocks until connected + bound (or throws)
    keepAlive(std::move(io), pool);  // store per-loop runtime for the process
}
```

`spawnOn` is the *only* place it's needed in the co-located model: a one-time
startup kick per loop. Per-request reads stay inline — no spawnOn, no hop. When
sizing pools, remember total connections = N × per-loop max; keep it under the
database's `max_connections`. (relais ships `io::IoPool` as a reference N-loop
runtime for its own `EpollIoContext`; external routers follow the same
init-per-loop-thread contract shown above.)

## The background runtime thread (you don't wire it)

When your framework drives the event loops, relais's cached clock and L1 memory
budget are still ticked — by relais itself, not by anything you write.

relais owns a single `RuntimeThread` — one `jthread`, a 100 ms tick that
refreshes `CachedClock`, `CachedMemory`, and the GDSF heap accounting. The first
L1-caching `CacheTier` to come alive calls `RuntimeThread::ensureStarted()` in
its constructor; it's idempotent (`std::call_once`), so it spins up exactly once
per process regardless of how many loops or repos you have. It is **independent
of your event loops** — co-located or not, single-loop or N-loop, the clocks and
budget keep ticking. You don't start it, bind it, or tick it from your loop.