#ifndef JCX_RELAIS_IO_TEST_RUNNER_H
#define JCX_RELAIS_IO_TEST_RUNNER_H

#include <jcailloux/relais/io/Task.h>
#include "EpollIoContext.h"

#include <coroutine>
#include <exception>
#include <optional>
#include <variant>

namespace jcailloux::relais::io::test {

// runTask — execute a Task<T> on an EpollIoContext event loop
//
// Bridges the gap between sync test code and async coroutines.

template<typename T>
T runTask(EpollIoContext& io, Task<T> task) {
    std::variant<std::monostate, T, std::exception_ptr> result;
    bool done = false;

    auto wrapper = [&](Task<T> t) -> Task<void> {
        try {
            result.template emplace<1>(co_await std::move(t));
        } catch (...) {
            result.template emplace<2>(std::current_exception());
        }
        done = true;
    };

    auto wrapperTask = wrapper(std::move(task));

    struct Starter {
        struct promise_type {
            std::coroutine_handle<> inner;

            Starter get_return_object() noexcept {
                return Starter{std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_never initial_suspend() noexcept { return {}; }
            std::suspend_always final_suspend() noexcept { return {}; }
            void return_void() noexcept {}
            void unhandled_exception() noexcept {}
        };
        std::coroutine_handle<promise_type> handle;
    };

    auto starter = [&](Task<void> t) -> Starter {
        co_await std::move(t);
    };

    auto s = starter(std::move(wrapperTask));

    io.runUntil([&] { return done; });

    if (s.handle) s.handle.destroy();

    if (auto* ex = std::get_if<2>(&result))
        std::rethrow_exception(*ex);
    return std::move(std::get<1>(result));
}

// Specialization for Task<void>
inline void runTask(EpollIoContext& io, Task<void> task) {
    std::exception_ptr ex;
    bool done = false;

    auto wrapper = [&](Task<void> t) -> Task<void> {
        try {
            co_await std::move(t);
        } catch (...) {
            ex = std::current_exception();
        }
        done = true;
    };

    struct Starter {
        struct promise_type {
            Starter get_return_object() noexcept {
                return Starter{std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_never initial_suspend() noexcept { return {}; }
            std::suspend_always final_suspend() noexcept { return {}; }
            void return_void() noexcept {}
            void unhandled_exception() noexcept {}
        };
        std::coroutine_handle<promise_type> handle;
    };

    auto starter = [&](Task<void> t) -> Starter {
        co_await std::move(t);
    };

    auto s = starter(wrapper(std::move(task)));

    io.runUntil([&] { return done; });

    if (s.handle) s.handle.destroy();

    if (ex) std::rethrow_exception(ex);
}

} // namespace jcailloux::relais::io::test

#endif // JCX_RELAIS_IO_TEST_RUNNER_H
