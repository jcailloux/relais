#ifndef JCX_RELAIS_RUNTIME_SPAWN_H
#define JCX_RELAIS_RUNTIME_SPAWN_H

#include <coroutine>
#include <exception>
#include <expected>
#include <type_traits>
#include <utility>

#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/Task.h"

namespace jcailloux::relais {

// =============================================================================
// spawnOn — drive a lazy Task to completion on an event-loop thread, from any
// thread, without blocking the caller.
//
// A Task<T> is lazy and runs on the worker thread that owns the I/O resources
// (PgPool/Redis/cache routed via thread_local). Code on *another* thread
// (e.g. a different framework's request handler, or a startup/bootstrap thread)
// cannot co_await it directly. spawnOn bridges the gap:
//
//   - a lazy driver coroutine is created (suspended) holding `task` + `on_done`;
//   - post() hands the driver's coroutine_handle to the loop thread — post() is
//     thread-safe, and a handle is a trivially-copyable 8-byte pointer that fits
//     std::function's small-buffer, so no extra heap allocation on the queue;
//   - the loop thread resumes the driver, which co_awaits the Task — nobody
//     blocks, the loop keeps spinning;
//   - on completion, `on_done` is invoked with the outcome, ON the loop thread.
//
// Primary use is the cross-thread bootstrap of co-located pools: from the main
// thread, drive a lazy `PgPool::create()` to completion on the loop and signal a
// std::promise to build a blocking wait. For per-request cache reads you should
// co_await inline on the loop instead — no spawnOn, no hop.
//
//   relais::spawnOn(io, PgPool<Io>::create(io, conninfo),
//       [&ready](relais::Outcome<PoolPtr> r) {
//           if (r) ready.set_value(*r); else ready.set_exception(r.error());
//       });
//
// Cost: exactly one coroutine-frame allocation (the driver). `task` and
// `on_done` are moved *into* that frame, not into the std::function, so the
// posted callable carries only the handle (no SBO spill) and `on_done` need NOT
// be copyable — a move-only callback (capturing a unique_ptr, etc.) is fine.
//
// `on_done` is called exactly once, on the loop thread. If it throws, the
// exception is swallowed (fire-and-forget contract). Lifetime: the driver frame
// self-destructs after `on_done` returns. Teardown caveat: if the posted resume
// is never run (io context destroyed with a non-empty queue), the suspended
// driver frame leaks — same fire-and-forget semantics as a dropped post().
// =============================================================================

/// Result of a completed Task: the value, or the exception it threw.
/// For T = void, presence of a value means success (std::expected<void, ...>).
template<typename T>
using Outcome = std::expected<T, std::exception_ptr>;

namespace detail {

// Lazy, self-destroying driver coroutine. Unlike io::DetachedTask it suspends
// at initial_suspend, so creating it allocates the frame but runs nothing —
// the work starts only when its handle is resumed on the loop thread.
struct SpawnDriver {
    struct promise_type {
        SpawnDriver get_return_object() noexcept {
            return {std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never  final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept {}  // on_done threw — swallowed
    };
    std::coroutine_handle<promise_type> handle;
};

template<typename T, typename OnDone>
SpawnDriver driveTask(io::Task<T> task, OnDone on_done) {
    // `task` and `on_done` live in this coroutine frame until completion.
    if constexpr (std::is_void_v<T>) {
        try {
            co_await task;
            on_done(Outcome<void>{});
        } catch (...) {
            on_done(Outcome<void>{std::unexpect, std::current_exception()});
        }
    } else {
        try {
            T value = co_await task;
            on_done(Outcome<T>{std::move(value)});
        } catch (...) {
            on_done(Outcome<T>{std::unexpect, std::current_exception()});
        }
    }
}

}  // namespace detail

template<io::IoContext Io, typename T, typename OnDone>
void spawnOn(Io& io, io::Task<T> task, OnDone on_done) {
    std::coroutine_handle<> driver =
        detail::driveTask<T>(std::move(task), std::move(on_done)).handle;
    // Only the 8-byte handle crosses to the loop thread → fits std::function's
    // SBO, no allocation. Resuming drives the Task on the loop thread.
    io.post([driver]() noexcept { driver.resume(); });
}

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_RUNTIME_SPAWN_H
