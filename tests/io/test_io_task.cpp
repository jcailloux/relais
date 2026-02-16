#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/io/Task.h>
#include <string>
#include <stdexcept>
#include <variant>

using namespace jcailloux::relais::io;

// Local sync_wait for testing pure coroutines (no IoContext needed).
// Uses an eager Starter coroutine to drive the lazy Task synchronously.

namespace {

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

template<typename T>
T sync_wait(Task<T> task) {
    std::variant<std::monostate, T, std::exception_ptr> result;

    auto wrapper = [&](Task<T> t) -> Task<void> {
        try {
            result.template emplace<1>(co_await std::move(t));
        } catch (...) {
            result.template emplace<2>(std::current_exception());
        }
    };

    auto s = [&](Task<void> t) -> Starter { co_await std::move(t); }(wrapper(std::move(task)));
    if (s.handle) s.handle.destroy();

    if (auto* ex = std::get_if<2>(&result))
        std::rethrow_exception(*ex);
    return std::move(std::get<1>(result));
}

inline void sync_wait(Task<void> task) {
    std::exception_ptr ex;

    auto wrapper = [&](Task<void> t) -> Task<void> {
        try {
            co_await std::move(t);
        } catch (...) {
            ex = std::current_exception();
        }
    };

    auto s = [&](Task<void> t) -> Starter { co_await std::move(t); }(wrapper(std::move(task)));
    if (s.handle) s.handle.destroy();

    if (ex) std::rethrow_exception(ex);
}

} // anonymous namespace

// =============================================================================
// Basic Task<T> creation and sync_wait
// =============================================================================

Task<int> returnFortyTwo() {
    co_return 42;
}

Task<std::string> returnHello() {
    co_return "hello";
}

Task<void> doNothing() {
    co_return;
}

TEST_CASE("Task<int> sync_wait returns value", "[task]") {
    auto result = sync_wait(returnFortyTwo());
    REQUIRE(result == 42);
}

TEST_CASE("Task<string> sync_wait returns value", "[task]") {
    auto result = sync_wait(returnHello());
    REQUIRE(result == "hello");
}

TEST_CASE("Task<void> sync_wait completes", "[task]") {
    REQUIRE_NOTHROW(sync_wait(doNothing()));
}

// =============================================================================
// Exception propagation
// =============================================================================

Task<int> throwRuntime() {
    throw std::runtime_error("boom");
    co_return 0;  // unreachable
}

Task<void> throwVoid() {
    throw std::logic_error("void boom");
    co_return;  // unreachable
}

TEST_CASE("Task<int> propagates exception through sync_wait", "[task]") {
    REQUIRE_THROWS_AS(sync_wait(throwRuntime()), std::runtime_error);
}

TEST_CASE("Task<void> propagates exception through sync_wait", "[task]") {
    REQUIRE_THROWS_AS(sync_wait(throwVoid()), std::logic_error);
}

// =============================================================================
// Chained co_await (symmetric transfer)
// =============================================================================

Task<int> inner() {
    co_return 10;
}

Task<int> middle() {
    auto v = co_await inner();
    co_return v + 20;
}

Task<int> outer() {
    auto v = co_await middle();
    co_return v + 30;
}

TEST_CASE("Chained co_await with symmetric transfer", "[task]") {
    auto result = sync_wait(outer());
    REQUIRE(result == 60);  // 10 + 20 + 30
}

// =============================================================================
// Deep coroutine chain (validates no stack overflow with symmetric transfer)
// =============================================================================

Task<int> recurse(int n) {
    if (n <= 0) co_return 0;
    auto v = co_await recurse(n - 1);
    co_return v + 1;
}

TEST_CASE("Deep coroutine chain does not stack overflow", "[task]") {
    // With symmetric transfer, this should not cause stack overflow
    // even with a deep chain. Without it, this would blow the stack.
    auto result = sync_wait(recurse(10000));
    REQUIRE(result == 10000);
}

// =============================================================================
// Move semantics
// =============================================================================

TEST_CASE("Task is move-constructible", "[task]") {
    auto t1 = returnFortyTwo();
    auto t2 = std::move(t1);
    REQUIRE(sync_wait(std::move(t2)) == 42);
}

TEST_CASE("Task is move-assignable", "[task]") {
    auto t1 = returnFortyTwo();
    Task<int> t2;
    t2 = std::move(t1);
    REQUIRE(sync_wait(std::move(t2)) == 42);
}

// =============================================================================
// Task<void> chaining
// =============================================================================

static int sideEffect = 0;

Task<void> setEffect(int v) {
    sideEffect = v;
    co_return;
}

Task<void> chainVoid() {
    co_await setEffect(99);
    co_return;
}

TEST_CASE("Task<void> chaining works", "[task]") {
    sideEffect = 0;
    sync_wait(chainVoid());
    REQUIRE(sideEffect == 99);
}

// =============================================================================
// Exception in chained coroutine
// =============================================================================

Task<int> failInner() {
    throw std::runtime_error("inner fail");
    co_return 0;
}

Task<int> catchInMiddle() {
    try {
        co_return co_await failInner();
    } catch (const std::runtime_error&) {
        co_return -1;
    }
}

TEST_CASE("Exception caught in middle coroutine", "[task]") {
    auto result = sync_wait(catchInMiddle());
    REQUIRE(result == -1);
}
