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

// =============================================================================
// Thread-local free-list pool for coroutine frames.
//
// Each thread keeps a singly-linked free list of recently freed blocks,
// grouped by size. On alloc: pop from free list (~3-5ns, no lock).
// On dealloc: push back. Falls through to ::operator new on cold start
// or when the pool is empty.
//
// The pool caps at kMaxCached blocks per size class to bound memory.
// Blocks larger than kMaxFrameSize bypass the pool entirely.
// =============================================================================

struct FramePool {
    static constexpr size_t kMaxFrameSize = 1024;   // frames > 1KB bypass pool
    static constexpr size_t kMaxCached    = 128;     // max blocks per size class

    struct Block {
        Block* next;
    };

    struct SizeClass {
        Block* head = nullptr;
        size_t count = 0;
    };

    // Size classes: 64, 128, 192, 256, 320, 384, 448, 512, 576, 640, 704, 768, 832, 896, 960, 1024
    static constexpr size_t kGranularity = 64;
    static constexpr size_t kNumClasses  = kMaxFrameSize / kGranularity;

    SizeClass classes[kNumClasses];

    static size_t classIndex(size_t size) noexcept {
        return (size + kGranularity - 1) / kGranularity - 1;
    }

    static size_t classSize(size_t idx) noexcept {
        return (idx + 1) * kGranularity;
    }

    static FramePool& instance() noexcept {
        static thread_local FramePool pool;
        return pool;
    }

    void* alloc(size_t size) {
        if (size > kMaxFrameSize)
            return ::operator new(size);
        auto idx = classIndex(size);
        auto& sc = classes[idx];
        if (sc.head) {
            auto* block = sc.head;
            sc.head = block->next;
            --sc.count;
            return block;
        }
        return ::operator new(classSize(idx));
    }

    void dealloc(void* ptr, size_t size) noexcept {
        if (size > kMaxFrameSize) {
            ::operator delete(ptr);
            return;
        }
        auto idx = classIndex(size);
        auto& sc = classes[idx];
        if (sc.count >= kMaxCached) {
            ::operator delete(ptr);
            return;
        }
        auto* block = static_cast<Block*>(ptr);
        block->next = sc.head;
        sc.head = block;
        ++sc.count;
    }

    ~FramePool() {
        for (auto& sc : classes) {
            while (sc.head) {
                auto* next = sc.head->next;
                ::operator delete(sc.head);
                sc.head = next;
            }
        }
    }
};

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

    // Coroutine frame allocation: thread-local pool (~3-5ns) with fallback.
    static void* operator new(size_t size) {
        return FramePool::instance().alloc(size);
    }

    static void operator delete(void* ptr, size_t size) noexcept {
        FramePool::instance().dealloc(ptr, size);
    }
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

// Immediate<T> — zero-overhead awaitable for sync/async branching
//
// Two creation paths:
// 1. Immediate(T): value ready, no Task allocated, no optional wrapping
// 2. Immediate(Task<T>): deferred, await delegates to the inner Task
//
// On co_await of a ready Immediate: await_ready() = true, no suspend,
// await_resume() moves T directly out of the variant (single move).
// sizeof = sizeof(variant<T, Task<T>>), no extra discriminant.

template<typename T>
class Immediate {
public:
    Immediate(T value) noexcept(std::is_nothrow_move_constructible_v<T>)
        : state_(std::in_place_index<0>, std::move(value)) {}

    Immediate(Task<T> task) noexcept
        : state_(std::in_place_index<1>, std::move(task)) {}

    Immediate(Immediate&&) = default;
    Immediate& operator=(Immediate&&) = default;
    Immediate(const Immediate&) = delete;
    Immediate& operator=(const Immediate&) = delete;

    [[nodiscard]] bool await_ready() const noexcept {
        return state_.index() == 0;
    }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
        return std::get<1>(state_).await_suspend(caller);
    }

    T await_resume() {
        if (state_.index() == 0)
            return std::move(std::get<0>(state_));
        return std::get<1>(state_).await_resume();
    }

    /// Extract the inner Task (only valid when !await_ready()).
    Task<T> take_task() noexcept {
        return std::move(std::get<1>(state_));
    }

private:
    std::variant<T, Task<T>> state_;
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
