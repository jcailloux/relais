# Writing an `IoContext` adapter (running relais on a foreign event loop)

relais is generic over the [`IoContext`](../include/jcailloux/relais/io/IoContext.h)
concept. `EpollIoContext` is the bundled implementation, but `PgPool<Io>`
and the coroutine awaiter machinery accept **any** type that
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
- only real **misses** do async I/O, inline on the same thread.

Bridging from the framework's threads to dedicated relais loops instead costs a
hop + two syscalls per request, even on an L1 hit. Co-locate unless you can't fit
one DB pool per front-end loop within your connection budget.

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
| `WatchHandle addWatch(int fd, IoEvent mask, cb)` | register `fd`; call `cb(events)` on the loop thread when ready, with matching `IoEvent` bits. **A watch follows the open file, not the number**: after `fd` is closed, `removeWatch` then `addWatch` on a new fd that reuses the number must deliver the new file's events |
| `void updateWatch(WatchHandle, IoEvent mask)` | change the active mask on that handle |
| `void removeWatch(WatchHandle)` | stop all callbacks for that handle at once, **including for the rest of the event being dispatched**; **must be safe to call from inside that handle's own callback** — defer teardown of loop-owned state |
| `void post(std::function<void()>)` | run once, on the loop thread, FIFO; thread-safe; **wake a blocked loop promptly** |
| `TimerToken postDelayed(duration, cb)` | run `cb` once, on the loop thread, after the delay; return a cancellable token. Drives both the adaptive batch flush **and** the per-I/O-wait `query_timeout` — a timer that never fires means no liveness protection |
| `void cancelTimer(TimerToken)` | cancel a pending `postDelayed`; no-op if already fired or unknown |

> The exact concept signatures and member types, plus the
> `IoContextConformance::runAll` and `spawnOn` signatures, are in
> [api-reference.md › Runtime and I/O](api-reference.md#runtime-and-io).

## Sketch: trantor (Drogon)

```cpp
#include <jcailloux/relais/io/IoContext.h>
#include <trantor/net/EventLoop.h>
#include <trantor/net/Channel.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <unordered_map>

namespace relais = jcailloux::relais;

class TrantorIoContext {
public:
    using IoEvent     = relais::io::IoEvent;
    using WatchHandle = int;             // the fd doubles as the handle
    using TimerToken  = std::uint64_t;

    explicit TrantorIoContext(trantor::EventLoop* loop)
        : loop_(loop),
          timer_fd_(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)) {
        // A Channel registers on the loop thread; the adapter may be built on another.
        loop_->runInLoop([this] {
            addWatch(timer_fd_, IoEvent::Read, [this](IoEvent) { fireTimers(); });
        });
    }

    WatchHandle addWatch(int fd, IoEvent ev, std::function<void(IoEvent)> cb) {
        removeWatch(fd);  // a Channel already on fd goes through the deferred free
        auto ch = std::make_unique<trantor::Channel>(loop_, fd);
        // One callback per epoll event, with the combined mask.
        ch->setEventCallback([raw = ch.get(), cb = std::move(cb)] {
            cb(fromEpoll(raw->revents()));
        });
        applyMask(*ch, ev);
        channels_[fd] = std::move(ch);
        return fd;
    }

    void updateWatch(WatchHandle fd, IoEvent ev) {
        if (auto it = channels_.find(fd); it != channels_.end())
            applyMask(*it->second, ev);
    }

    void removeWatch(WatchHandle fd) {
        if (auto it = channels_.find(fd); it != channels_.end()) {
            it->second->disableAll();
            it->second->remove();   // detached now: no further callback
            // Freed after this turn's events: trantor still touches the Channel
            // when the callback that called removeWatch returns.
            trantor::Channel* dying = it->second.release();
            channels_.erase(it);
            loop_->queueInLoop([dying] { delete dying; });
        }
    }

    // queueInLoop is thread-safe and wakes the loop via its own eventfd.
    void post(std::function<void()> cb) { loop_->queueInLoop(std::move(cb)); }

    // Thread-safe. The timers live here, not in trantor's timer queue.
    TimerToken postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        const auto due = Clock::now() + delay;
        std::lock_guard lock(timer_mutex_);
        const TimerToken token = next_token_++;
        by_token_.emplace(token, timers_.emplace(due, Timer{token, std::move(cb)}));
        if (due < armed_) arm(due);  // a syscall only for a new earliest deadline
        return token;
    }

    void cancelTimer(TimerToken token) {
        std::lock_guard lock(timer_mutex_);
        if (auto it = by_token_.find(token); it != by_token_.end()) {
            timers_.erase(it->second);
            by_token_.erase(it);
        }  // the timerfd keeps its deadline: one empty wakeup, then fireTimers re-arms
    }

private:
    using Clock = std::chrono::steady_clock;  // CLOCK_MONOTONIC on Linux
    struct Timer { TimerToken token; std::function<void()> cb; };

    // The mapping EpollIoContext uses.
    static IoEvent fromEpoll(int revents) {
        IoEvent e = IoEvent::None;
        if (revents & (EPOLLIN | EPOLLPRI))  e |= IoEvent::Read;
        if (revents & EPOLLOUT)              e |= IoEvent::Write;
        if (revents & (EPOLLERR | EPOLLHUP)) e |= IoEvent::Error;
        return e;
    }

    static void applyMask(trantor::Channel& ch, IoEvent ev) {
        using relais::io::hasEvent;
        hasEvent(ev, IoEvent::Read)  ? ch.enableReading() : ch.disableReading();
        hasEvent(ev, IoEvent::Write) ? ch.enableWriting() : ch.disableWriting();
    }

    void fireTimers() {
        std::uint64_t expirations;
        [[maybe_unused]] auto r = ::read(timer_fd_, &expirations, sizeof expirations);
        const auto now = Clock::now();
        for (;;) {
            std::function<void()> cb;
            {
                std::lock_guard lock(timer_mutex_);
                auto head = timers_.begin();
                if (head == timers_.end() || head->first > now) {
                    armed_ = Clock::time_point::max();
                    if (head != timers_.end()) arm(head->first);
                    return;
                }
                cb = std::move(head->second.cb);
                by_token_.erase(head->second.token);
                timers_.erase(head);  // under the lock: the pool is not thread-safe
            }
            cb();  // unlocked: it may arm or cancel timers
        }
    }

    void arm(Clock::time_point due) {  // timer_mutex_ held
        armed_ = due;
        const auto ns = std::chrono::nanoseconds(due.time_since_epoch()).count();
        const itimerspec its{{0, 0}, {ns / 1'000'000'000, ns % 1'000'000'000}};
        ::timerfd_settime(timer_fd_, TFD_TIMER_ABSTIME, &its, nullptr);
    }

    trantor::EventLoop* loop_;
    std::unordered_map<int, std::unique_ptr<trantor::Channel>> channels_;

    int timer_fd_;
    std::mutex timer_mutex_;
    std::pmr::unsynchronized_pool_resource pool_;  // recycles nodes: no allocation per arm
    std::pmr::multimap<Clock::time_point, Timer> timers_{&pool_};
    std::pmr::unordered_map<TimerToken, decltype(timers_)::iterator> by_token_{&pool_};
    Clock::time_point armed_ = Clock::time_point::max();  // the timerfd's deadline
    TimerToken next_token_ = 1;
};

static_assert(relais::io::IoContext<TrantorIoContext>);
```

> Checked with `runAll`, under ASan, against trantor 1.5.28 (Drogon 1.9.13).
> trantor's API changes across versions: run the harness again when you upgrade.

Three rules of the contract shape this sketch. Each is easy to break on
trantor, and the harness checks each one.

- **Self-removal.** relais calls `removeWatch(h)` from inside watch `h`'s own
  callback (a connection drops its watch on EOF or error). The loop resumes
  after that callback returns, so freeing the watch's loop-owned state — the
  `trantor::Channel`, or the `std::function` being invoked — inside
  `removeWatch` is a use-after-free. Defer the free past the current event:
  `queueInLoop(delete)` here; on a raw epoll loop, copy the callback onto the
  stack before invoking it (see `EpollIoContext::runOnce`). Defer only the free,
  not the removal: from that same callback, relais may watch a new socket that
  reuses the fd number (libpq replaces its socket while connecting), and a
  pending removal would tear the new watch down. C10 catches a synchronous
  free under ASan; C12 catches a deferred removal.
- **One event, one callback.** With separate read, write, error and close
  callbacks, trantor calls them back to back for one epoll event without
  re-checking the watch, and one event often carries several bits (readable and
  writable; error and readable when the server drops the connection). If the
  first callback removes the watch and resumes a coroutine that frees its
  awaiter, the next one runs on freed memory. Hence a single `setEventCallback`
  with the combined mask. C13 checks that nothing follows a `removeWatch`.
- **Cancel removes the timer.** relais arms a timer per I/O wait and cancels it
  when the wait completes. trantor's `invalidateTimer` leaves the cancelled
  timer queued until its deadline — about 200 bytes held for `query_timeout`
  (30 s by default), so 6 MB per 1 000 waits/s — and does nothing while the
  loop is stopped. Hence the adapter's own queue behind one timerfd. C9 and C11
  check that a cancelled timer never fires; no check sees what it retains.

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

Once `runAll` passes, construct `PgPool<TrantorIoContext>` on each loop and
`co_await` relais `Task`s directly inside your coroutine handlers — no thread hop.

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
        PgPool<TrantorIoContext>::create(*io_ptr, conninfo,
            PgPoolConfig{.min_connections = min, .max_connections = max}),
        [&ready, io_ptr](relais::Outcome<std::shared_ptr<PgPool<TrantorIoContext>>> r) {
            // ON the loop thread: bind THIS loop's thread_local providers.
            if (r) { relais::PgProvider::init(*io_ptr, *r); ready.set_value(*r); }
            else   { ready.set_exception(r.error()); }
        });

    auto pool = fut.get();  // blocks until connected + bound (or throws)
    // illustrative: you must keep io + pool alive program-long — store them
    // somewhere (keepAlive is not a relais API).
    keepAlive(std::move(io), pool);
}
```

`spawnOn` is the *only* place it's needed in the co-located model: a one-time
startup kick per loop. Per-request reads stay inline — no spawnOn, no hop. When
sizing pools, remember total connections = N × per-loop max; keep it under the
database's `max_connections`. (relais ships `io::IoPool` as a reference N-loop
runtime for its own `EpollIoContext`; external routers follow the same
init-per-loop-thread contract shown above.)

> **Wire your timeouts here.** `PgPoolConfig` also carries `acquire_timeout`
> (default 5 s) and `query_timeout` (default 30 s), the liveness backstop of
> every wait. They are armed via your `postDelayed`/`cancelTimer`, so a correct
> timer subsystem (conformance C8/C9/C11) is the prerequisite. On trantor they
> also end a wait on a lost watch: `EpollPoller::update` ignores every
> `epoll_ctl` failure, so a watch it could not register never fires and reports
> nothing. `uncertain_settle`, `settle_delay` and `client_check_interval`
> default to a direct connection; behind a transaction pooler, set them as
> [runtime.md › Settling an uncertain write](runtime.md#settling-an-uncertain-write)
> says. See
> [runtime.md › Liveness & failure semantics](runtime.md#liveness--failure-semantics).

Expose `isInLoopThread()` on your adapter and `PgProvider::init` will assert it
runs on the loop thread — the same safety net `EpollIoContext` gets. The sketch
above omits it, silently opting out of that check.

## The background runtime thread (you don't wire it)

When your framework drives the event loops, relais's cached clock and L1 memory
budget are still ticked — by relais itself, not by anything you write.

relais owns a single `RuntimeThread` — one `jthread`, a 100 ms tick that
refreshes `CachedClock` and `CachedMemory`, and — only when GDSF eviction is
compiled in — resets its admitted-bytes counter (`on_heap_refresh`). The first
L1-caching `CacheTier` to come alive calls `RuntimeThread::ensureStarted()` in
its constructor; it's idempotent (`std::call_once`), so it spins up exactly once
per process regardless of how many loops or repos you have. It is **independent
of your event loops** — co-located or not, single-loop or N-loop, the clocks and
budget keep ticking. You don't start it, bind it, or tick it from your loop.