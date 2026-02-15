#ifndef JCX_RELAIS_IO_TASK_H
#define JCX_RELAIS_IO_TASK_H

#include <coroutine>
#include <exception>
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

    Task() noexcept = default;
    explicit Task(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}
    ~Task() { if (handle_) handle_.destroy(); }

    Task(Task&& o) noexcept : handle_(std::exchange(o.handle_, nullptr)) {}
    Task& operator=(Task&& o) noexcept {
        if (this != &o) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(o.handle_, nullptr);
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    [[nodiscard]] bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
        handle_.promise().continuation_ = caller;
        return handle_;
    }

    T await_resume() {
        auto& result = handle_.promise().result_;
        if (auto* ex = std::get_if<2>(&result))
            std::rethrow_exception(*ex);
        return std::move(std::get<1>(result));
    }

private:
    std::coroutine_handle<promise_type> handle_ = nullptr;
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

    Task() noexcept = default;
    explicit Task(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}
    ~Task() { if (handle_) handle_.destroy(); }

    Task(Task&& o) noexcept : handle_(std::exchange(o.handle_, nullptr)) {}
    Task& operator=(Task&& o) noexcept {
        if (this != &o) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(o.handle_, nullptr);
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    [[nodiscard]] bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
        handle_.promise().continuation_ = caller;
        return handle_;
    }

    void await_resume() {
        if (handle_.promise().exception_)
            std::rethrow_exception(handle_.promise().exception_);
    }

private:
    std::coroutine_handle<promise_type> handle_ = nullptr;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_TASK_H
