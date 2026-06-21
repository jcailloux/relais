#ifndef JCX_RELAIS_IO_WHENALL_H
#define JCX_RELAIS_IO_WHENALL_H

#include <coroutine>
#include <cstddef>
#include <exception>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "jcailloux/relais/io/Task.h"

namespace jcailloux::relais::io {

// =============================================================================
// whenAll — mono-thread structured gather.
//
// Starts N lazy Tasks, lets each advance to its first suspension point (its
// `submit` into the batch), then completes when all have finished. The point is
// emission shape: co_awaiting tasks one-by-one chains them (task k's `submit`
// only happens after task k-1 fully completes → N round-trips); handing them to
// whenAll fans them out (all N `submit`s enter the batch before any flush → 1
// flush, the commands co-pipeline).
//
// This is NOT concurrency in the threading sense — everything runs on the one
// loop thread, cooperatively. whenAll only changes *when* each child is started
// relative to the others; the children themselves still suspend/resume on the
// same thread with no shared mutable state. No atomics, no locks.
//
// Completion uses a plain counter latched at N+1: the extra +1 is held by the
// awaiting coroutine across the start loop so that a child completing
// synchronously (e.g. a Task::fromValue) can never drive the latch to zero
// mid-loop and resume the continuation re-entrantly. The awaiter consumes the
// +1 after starting all children; whoever brings the latch to zero resumes the
// continuation via symmetric transfer.
//
// Exceptions: a child's exception is captured and re-thrown from await_resume.
// If several children throw, the lowest-index exception is propagated and the
// others are dropped (no aggregate). Results of throwing gathers are discarded
// (no partial vector/tuple).
//
// Overloads:
//   whenAll(std::span<Task<T>>)  → vector<T>   (T != void)
//   whenAll(std::span<Task<void>>) → void
//   whenAll(Task<Ts>...)         → tuple<...>  (void members → std::monostate;
//                                               all-void → void)
//
// The returned object is the awaitable itself (not a Task) — one fewer frame.
// It is a temporary in `co_await whenAll(...)`, kept alive by the language until
// await_resume returns, which is exactly the lifetime the child frames and the
// latch need.
// =============================================================================

namespace detail {

// Completion latch. count_ starts at N+1; the +1 is the awaiting coroutine's
// guard, consumed by try_await after every child has been started.
class WhenAllCounter {
public:
    explicit WhenAllCounter(std::size_t n) noexcept : count_(n + 1) {}

    // Consume the +1 guard once all children are started. Returns true if
    // children are still pending (caller must stay suspended), false if every
    // child already completed synchronously (caller resumes immediately).
    bool try_await(std::coroutine_handle<> continuation) noexcept {
        continuation_ = continuation;
        return --count_ != 0;
    }

    // A child finished. Returns the continuation to symmetric-transfer to when
    // it is the last one, else noop_coroutine.
    std::coroutine_handle<> notify() noexcept {
        if (--count_ == 0)
            return continuation_;
        return std::noop_coroutine();
    }

private:
    std::size_t count_;
    std::coroutine_handle<> continuation_ = std::noop_coroutine();
};

// Shared promise plumbing for the per-child wrapper coroutine.
struct WhenAllPromiseBase {
    WhenAllCounter* counter_ = nullptr;

    [[nodiscard]] std::suspend_always initial_suspend() noexcept { return {}; }

    struct FinalAwaiter {
        [[nodiscard]] bool await_ready() const noexcept { return false; }

        template<typename Promise>
        [[nodiscard]] std::coroutine_handle<>
        await_suspend(std::coroutine_handle<Promise> h) noexcept {
            return h.promise().counter_->notify();
        }

        void await_resume() const noexcept {}
    };

    [[nodiscard]] FinalAwaiter final_suspend() noexcept { return {}; }

    // Reuse Task's thread-local frame pool for the wrapper frames too.
    static void* operator new(std::size_t size) {
        return FramePool::instance().alloc(size);
    }
    static void operator delete(void* ptr, std::size_t size) noexcept {
        FramePool::instance().dealloc(ptr, size);
    }
};

// Per-child wrapper coroutine: awaits the input Task, captures its outcome, and
// signals the latch from final_suspend. Lazy (initial_suspend = suspend_always)
// so the parent decides when each child starts.
template<typename T>
class WhenAllTask {
public:
    struct promise_type : WhenAllPromiseBase {
        std::variant<std::monostate, T, std::exception_ptr> result_;

        WhenAllTask get_return_object() noexcept {
            return WhenAllTask{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        void return_value(T value) noexcept(std::is_nothrow_move_constructible_v<T>) {
            result_.template emplace<1>(std::move(value));
        }
        void unhandled_exception() noexcept {
            result_.template emplace<2>(std::current_exception());
        }
    };

    explicit WhenAllTask(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}
    WhenAllTask(WhenAllTask&& o) noexcept : handle_(std::exchange(o.handle_, nullptr)) {}
    WhenAllTask& operator=(WhenAllTask&& o) noexcept {
        if (this != &o) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(o.handle_, nullptr);
        }
        return *this;
    }
    WhenAllTask(const WhenAllTask&) = delete;
    WhenAllTask& operator=(const WhenAllTask&) = delete;
    ~WhenAllTask() { if (handle_) handle_.destroy(); }

    void start(WhenAllCounter& counter) {
        handle_.promise().counter_ = &counter;
        handle_.resume();
    }
    void rethrowIfFailed() const {
        if (auto* ex = std::get_if<2>(&handle_.promise().result_))
            std::rethrow_exception(*ex);
    }
    T take() { return std::move(std::get<1>(handle_.promise().result_)); }

private:
    std::coroutine_handle<promise_type> handle_;
};

template<>
class WhenAllTask<void> {
public:
    struct promise_type : WhenAllPromiseBase {
        std::exception_ptr exception_;

        WhenAllTask get_return_object() noexcept {
            return WhenAllTask{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { exception_ = std::current_exception(); }
    };

    explicit WhenAllTask(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}
    WhenAllTask(WhenAllTask&& o) noexcept : handle_(std::exchange(o.handle_, nullptr)) {}
    WhenAllTask& operator=(WhenAllTask&& o) noexcept {
        if (this != &o) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(o.handle_, nullptr);
        }
        return *this;
    }
    WhenAllTask(const WhenAllTask&) = delete;
    WhenAllTask& operator=(const WhenAllTask&) = delete;
    ~WhenAllTask() { if (handle_) handle_.destroy(); }

    void start(WhenAllCounter& counter) {
        handle_.promise().counter_ = &counter;
        handle_.resume();
    }
    void rethrowIfFailed() const {
        if (handle_.promise().exception_)
            std::rethrow_exception(handle_.promise().exception_);
    }

private:
    std::coroutine_handle<promise_type> handle_;
};

template<typename T>
WhenAllTask<T> makeWhenAllTask(Task<T> task) {
    co_return co_await task;
}

// Awaitable over a runtime-sized homogeneous batch → vector<T> (or void).
template<typename T>
class WhenAllVector {
public:
    explicit WhenAllVector(std::vector<WhenAllTask<T>> tasks)
        : tasks_(std::move(tasks)), counter_(tasks_.size()) {}

    WhenAllVector(WhenAllVector&&) = default;
    WhenAllVector(const WhenAllVector&) = delete;
    WhenAllVector& operator=(const WhenAllVector&) = delete;

    [[nodiscard]] bool await_ready() const noexcept { return tasks_.empty(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) {
        for (auto& t : tasks_)
            t.start(counter_);
        return counter_.try_await(continuation) ? std::noop_coroutine() : continuation;
    }

    decltype(auto) await_resume() {
        for (auto& t : tasks_)
            t.rethrowIfFailed();
        if constexpr (!std::is_void_v<T>) {
            std::vector<T> out;
            out.reserve(tasks_.size());
            for (auto& t : tasks_)
                out.push_back(t.take());
            return out;
        }
    }

private:
    std::vector<WhenAllTask<T>> tasks_;
    WhenAllCounter counter_;
};

template<typename T>
auto takeOrMonostate(WhenAllTask<T>& t) {
    if constexpr (std::is_void_v<T>)
        return std::monostate{};
    else
        return t.take();
}

// Awaitable over a fixed heterogeneous pack → tuple<...> (or void if all-void).
template<typename... Ts>
class WhenAllTuple {
public:
    explicit WhenAllTuple(WhenAllTask<Ts>... tasks)
        : tasks_(std::move(tasks)...), counter_(sizeof...(Ts)) {}

    WhenAllTuple(WhenAllTuple&&) = default;
    WhenAllTuple(const WhenAllTuple&) = delete;
    WhenAllTuple& operator=(const WhenAllTuple&) = delete;

    [[nodiscard]] bool await_ready() const noexcept { return sizeof...(Ts) == 0; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) {
        std::apply([&](auto&... t) { (t.start(counter_), ...); }, tasks_);
        return counter_.try_await(continuation) ? std::noop_coroutine() : continuation;
    }

    decltype(auto) await_resume() {
        std::apply([](auto&... t) { (t.rethrowIfFailed(), ...); }, tasks_);
        if constexpr (!(std::is_void_v<Ts> && ...)) {
            return std::apply(
                [](auto&... t) { return std::make_tuple(takeOrMonostate(t)...); }, tasks_);
        }
    }

private:
    std::tuple<WhenAllTask<Ts>...> tasks_;
    WhenAllCounter counter_;
};

}  // namespace detail

template<typename T>
auto whenAll(std::span<Task<T>> tasks) {
    std::vector<detail::WhenAllTask<T>> wrappers;
    wrappers.reserve(tasks.size());
    for (auto& t : tasks)
        wrappers.push_back(detail::makeWhenAllTask<T>(std::move(t)));
    return detail::WhenAllVector<T>{std::move(wrappers)};
}

template<typename T>
auto whenAll(std::vector<Task<T>> tasks) {
    return whenAll(std::span<Task<T>>{tasks});
}

template<typename... Ts>
auto whenAll(Task<Ts>... tasks) {
    return detail::WhenAllTuple<Ts...>{detail::makeWhenAllTask<Ts>(std::move(tasks))...};
}

}  // namespace jcailloux::relais::io

#endif  // JCX_RELAIS_IO_WHENALL_H
