#ifndef JCX_RELAIS_IO_TASK_H
#define JCX_RELAIS_IO_TASK_H

#include <coroutine>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

namespace jcailloux::relais::io {

template<typename T = void>
class Task;

namespace detail {

struct PromiseBase {
    std::coroutine_handle<> continuation_ = std::noop_coroutine();

    struct FinalAwaiter {
        [[nodiscard]] bool await_ready() const noexcept { return false; }

        template<typename Promise>
        [[nodiscard]] std::coroutine_handle<>
        await_suspend(std::coroutine_handle<Promise> h) noexcept {
            return h.promise().continuation_;
        }

        void await_resume() noexcept {}
    };

    [[nodiscard]] std::suspend_always initial_suspend() noexcept { return {}; }
    [[nodiscard]] FinalAwaiter final_suspend() noexcept { return {}; }
};

} // namespace detail

// Task<T> — lazy, awaitable, move-only coroutine with symmetric transfer
//
// Two creation paths:
// 1. Coroutine: co_return / co_await → allocates coroutine frame on heap
// 2. fromValue(T): pre-resolved result → zero allocation, await_ready() = true
//
// When a caller co_awaits a fromValue Task, the coroutine machinery is
// completely bypassed: await_ready() returns true, await_suspend() is never
// called, and await_resume() returns the stored value directly.

template<typename T>
class Task {
public:
    struct promise_type : detail::PromiseBase {
        std::variant<std::monostate, T, std::exception_ptr> result_;

        Task get_return_object() noexcept {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        void return_value(T value) noexcept(std::is_nothrow_move_constructible_v<T>) {
            result_.template emplace<1>(std::move(value));
        }

        void unhandled_exception() noexcept {
            result_.template emplace<2>(std::current_exception());
        }
    };

    /// Construct a pre-resolved Task (no coroutine frame, zero heap allocation).
    /// co_await on this Task completes synchronously via await_ready() = true.
    static Task fromValue(T value) noexcept(std::is_nothrow_move_constructible_v<T>) {
        return Task{std::move(value)};
    }

    Task() noexcept = default;
    explicit Task(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}
    ~Task() { if (handle_) handle_.destroy(); }

    Task(Task&& o) noexcept
        : handle_(std::exchange(o.handle_, nullptr))
        , ready_value_(std::move(o.ready_value_)) {}
    Task& operator=(Task&& o) noexcept {
        if (this != &o) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(o.handle_, nullptr);
            ready_value_ = std::move(o.ready_value_);
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    [[nodiscard]] bool await_ready() const noexcept { return ready_value_.has_value(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
        handle_.promise().continuation_ = caller;
        return handle_;
    }

    T await_resume() {
        if (ready_value_) return std::move(*ready_value_);
        auto& result = handle_.promise().result_;
        if (auto* ex = std::get_if<2>(&result))
            std::rethrow_exception(*ex);
        return std::move(std::get<1>(result));
    }

private:
    explicit Task(T value) noexcept(std::is_nothrow_move_constructible_v<T>)
        : ready_value_(std::move(value)) {}

    std::coroutine_handle<promise_type> handle_ = nullptr;
    std::optional<T> ready_value_;
};

// Task<void> specialization

template<>
class Task<void> {
public:
    struct promise_type : detail::PromiseBase {
        std::exception_ptr exception_;

        Task get_return_object() noexcept {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        void return_void() noexcept {}

        void unhandled_exception() noexcept {
            exception_ = std::current_exception();
        }
    };

    /// Construct a pre-resolved void Task (no coroutine frame).
    static Task ready() noexcept {
        Task t;
        t.ready_ = true;
        return t;
    }

    Task() noexcept = default;
    explicit Task(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}
    ~Task() { if (handle_) handle_.destroy(); }

    Task(Task&& o) noexcept
        : handle_(std::exchange(o.handle_, nullptr))
        , ready_(o.ready_) {}
    Task& operator=(Task&& o) noexcept {
        if (this != &o) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(o.handle_, nullptr);
            ready_ = o.ready_;
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    [[nodiscard]] bool await_ready() const noexcept { return ready_; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
        handle_.promise().continuation_ = caller;
        return handle_;
    }

    void await_resume() {
        if (ready_) return;
        if (handle_.promise().exception_)
            std::rethrow_exception(handle_.promise().exception_);
    }

private:
    std::coroutine_handle<promise_type> handle_ = nullptr;
    bool ready_ = false;
};

// DetachedTask — eager, fire-and-forget coroutine (self-destroying)
//
// Starts immediately on creation, self-destructs on completion.
// Exceptions are swallowed (logged if RELAIS_LOG_ERROR is available).
// Use for async work that doesn't need to be awaited.

struct DetachedTask {
    struct promise_type {
        DetachedTask get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept {}
    };
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_TASK_H
