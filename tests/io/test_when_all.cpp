#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/io/Task.h>
#include <jcailloux/relais/io/WhenAll.h>

#include <coroutine>
#include <exception>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

using namespace jcailloux::relais::io;

// =============================================================================
// Test harness — drive lazy coroutines synchronously without an IoContext, with
// explicit control over *when* suspended children resume (to mimic the batch
// scheduler completing pipelined commands out of submission order).
//
// IMPORTANT: the Runner coroutines below are written as *captureless* lambdas
// that take everything they touch as by-reference parameters. A capturing
// (`[&]`) coroutine lambda would store those captures in the closure object,
// which is a temporary destroyed at the end of the `auto r = [...]();`
// statement — yet the coroutine outlives it (we resume it later), so accessing
// a capture would be a use-after-scope. Parameters live in the coroutine frame
// (alive until handle.destroy()) and bind to the test locals, which outlive the
// frame; that is safe.
// =============================================================================

namespace {

// Eager driver: runs the body until it suspends, keeps its handle so the body's
// frame (and the awaited temporaries it holds) stay alive across suspension.
struct Runner {
    struct promise_type {
        Runner get_return_object() noexcept {
            return Runner{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never  initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

// A suspension point under test control: parks the awaiting handle in `pending`
// until the test resumes it.
struct ManualEvent {
    std::vector<std::coroutine_handle<>>* pending;
    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const noexcept { pending->push_back(h); }
    void await_resume() const noexcept {}
};

Task<int> delayedInt(std::vector<std::coroutine_handle<>>& pending, int v) {
    co_await ManualEvent{&pending};
    co_return v;
}

Task<std::string> delayedString(std::vector<std::coroutine_handle<>>& pending, std::string v) {
    co_await ManualEvent{&pending};
    co_return v;
}

Task<void> delayedVoid(std::vector<std::coroutine_handle<>>& pending, int& sink, int v) {
    co_await ManualEvent{&pending};
    sink += v;
    co_return;
}

Task<int> throwingAfterSuspend(std::vector<std::coroutine_handle<>>& pending, const char* msg) {
    co_await ManualEvent{&pending};
    throw std::runtime_error(msg);
    co_return 0;
}

Task<int> throwingSync(const char* msg) {
    throw std::runtime_error(msg);
    co_return 0;
}

std::string messageOf(std::exception_ptr ep) {
    try {
        std::rethrow_exception(ep);
    } catch (const std::exception& e) {
        return e.what();
    } catch (...) {
        return "<non-std exception>";
    }
}

}  // namespace

TEST_CASE("whenAll gathers synchronous fromValue tasks", "[io][whenall]") {
    // All children ready → the +1 guard must keep the latch off zero until the
    // awaiter consumes it, then resume the continuation in one shot.
    std::optional<std::vector<int>> result;

    auto r = [](std::optional<std::vector<int>>& out) -> Runner {
        std::vector<Task<int>> tasks;
        tasks.push_back(Task<int>::fromValue(10));
        tasks.push_back(Task<int>::fromValue(20));
        tasks.push_back(Task<int>::fromValue(30));
        out = co_await whenAll(std::span<Task<int>>{tasks});
    }(result);
    if (r.handle) r.handle.destroy();

    REQUIRE(result.has_value());
    REQUIRE(*result == std::vector<int>{10, 20, 30});
}

TEST_CASE("whenAll result order is submission order, not completion order", "[io][whenall]") {
    std::vector<std::coroutine_handle<>> pending;
    std::optional<std::vector<int>> result;

    auto r = [](std::vector<std::coroutine_handle<>>& p,
                std::optional<std::vector<int>>& out) -> Runner {
        std::vector<Task<int>> tasks;
        tasks.push_back(delayedInt(p, 1));
        tasks.push_back(delayedInt(p, 2));
        tasks.push_back(delayedInt(p, 3));
        out = co_await whenAll(std::span<Task<int>>{tasks});
    }(pending, result);

    // Gather is suspended; every child parked itself.
    REQUIRE(pending.size() == 3);
    REQUIRE(!result.has_value());

    // Resume out of order: 2, then 0, then 1 (the last resume completes the gather).
    pending[2].resume();
    REQUIRE(!result.has_value());
    pending[0].resume();
    REQUIRE(!result.has_value());
    pending[1].resume();
    REQUIRE(result.has_value());

    if (r.handle) r.handle.destroy();
    // Order follows submission, independent of the resume order above.
    REQUIRE(*result == std::vector<int>{1, 2, 3});
}

TEST_CASE("whenAll mixes synchronous and suspended children in one gather", "[io][whenall]") {
    // Interleaves fromValue (completes inside the start loop, decrements the
    // latch early) with a suspended child (completes later). The N+1 guard must
    // still hold the latch off zero until the suspended one resumes.
    std::vector<std::coroutine_handle<>> pending;
    std::optional<std::vector<int>> result;

    auto r = [](std::vector<std::coroutine_handle<>>& p,
                std::optional<std::vector<int>>& out) -> Runner {
        std::vector<Task<int>> tasks;
        tasks.push_back(Task<int>::fromValue(10));
        tasks.push_back(delayedInt(p, 20));
        tasks.push_back(Task<int>::fromValue(30));
        out = co_await whenAll(std::span<Task<int>>{tasks});
    }(pending, result);

    // Only the suspended child is pending; the two fromValue ones already ran.
    REQUIRE(pending.size() == 1);
    REQUIRE(!result.has_value());
    pending[0].resume();
    REQUIRE(result.has_value());

    if (r.handle) r.handle.destroy();
    REQUIRE(*result == std::vector<int>{10, 20, 30});
}

TEST_CASE("whenAll over Task<void> propagates completion and side effects", "[io][whenall]") {
    std::vector<std::coroutine_handle<>> pending;
    int sink = 0;
    bool done = false;

    auto r = [](std::vector<std::coroutine_handle<>>& p, int& s, bool& d) -> Runner {
        std::vector<Task<void>> tasks;
        tasks.push_back(delayedVoid(p, s, 1));
        tasks.push_back(delayedVoid(p, s, 2));
        tasks.push_back(delayedVoid(p, s, 4));
        co_await whenAll(std::span<Task<void>>{tasks});
        d = true;
    }(pending, sink, done);

    REQUIRE(pending.size() == 3);
    for (auto& h : pending) h.resume();
    if (r.handle) r.handle.destroy();

    REQUIRE(done);
    REQUIRE(sink == 7);
}

TEST_CASE("whenAll empty span completes immediately", "[io][whenall]") {
    std::optional<std::vector<int>> result;

    auto r = [](std::optional<std::vector<int>>& out) -> Runner {
        std::vector<Task<int>> tasks;  // empty
        out = co_await whenAll(std::span<Task<int>>{tasks});
    }(result);
    if (r.handle) r.handle.destroy();

    REQUIRE(result.has_value());
    REQUIRE(result->empty());
}

TEST_CASE("whenAll propagates a child exception thrown after suspension", "[io][whenall]") {
    std::vector<std::coroutine_handle<>> pending;
    std::exception_ptr err;
    bool reached = false;

    auto r = [](std::vector<std::coroutine_handle<>>& p,
                std::exception_ptr& e, bool& reach) -> Runner {
        std::vector<Task<int>> tasks;
        tasks.push_back(delayedInt(p, 1));
        tasks.push_back(throwingAfterSuspend(p, "boom"));
        try {
            (void)co_await whenAll(std::span<Task<int>>{tasks});
            reach = true;
        } catch (...) {
            e = std::current_exception();
        }
    }(pending, err, reached);

    REQUIRE(pending.size() == 2);
    for (auto& h : pending) h.resume();
    if (r.handle) r.handle.destroy();

    REQUIRE(!reached);
    REQUIRE(err != nullptr);
    REQUIRE(messageOf(err) == "boom");
}

TEST_CASE("whenAll propagates the lowest-index exception when several throw", "[io][whenall]") {
    std::exception_ptr err;

    auto r = [](std::exception_ptr& e) -> Runner {
        std::vector<Task<int>> tasks;
        tasks.push_back(throwingSync("first"));
        tasks.push_back(throwingSync("second"));
        try {
            (void)co_await whenAll(std::span<Task<int>>{tasks});
        } catch (...) {
            e = std::current_exception();
        }
    }(err);
    if (r.handle) r.handle.destroy();

    REQUIRE(err != nullptr);
    REQUIRE(messageOf(err) == "first");
}

TEST_CASE("whenAll variadic gathers a heterogeneous tuple", "[io][whenall]") {
    std::vector<std::coroutine_handle<>> pending;
    std::optional<std::tuple<int, std::string>> result;

    auto r = [](Task<int> a, Task<std::string> b,
                std::optional<std::tuple<int, std::string>>& out) -> Runner {
        out = co_await whenAll(std::move(a), std::move(b));
    }(delayedInt(pending, 42), delayedString(pending, "hi"), result);

    REQUIRE(pending.size() == 2);
    REQUIRE(!result.has_value());
    pending[1].resume();
    pending[0].resume();
    REQUIRE(result.has_value());

    if (r.handle) r.handle.destroy();
    REQUIRE(std::get<0>(*result) == 42);
    REQUIRE(std::get<1>(*result) == "hi");
}

TEST_CASE("whenAll variadic of all-void returns void", "[io][whenall]") {
    std::vector<std::coroutine_handle<>> pending;
    int sink = 0;
    bool done = false;

    auto r = [](Task<void> x, Task<void> y, bool& d) -> Runner {
        co_await whenAll(std::move(x), std::move(y));
        d = true;
    }(delayedVoid(pending, sink, 3), delayedVoid(pending, sink, 5), done);

    REQUIRE(pending.size() == 2);
    pending[0].resume();
    REQUIRE(!done);
    pending[1].resume();
    REQUIRE(done);
    if (r.handle) r.handle.destroy();

    REQUIRE(sink == 8);
}

TEST_CASE("whenAll vector overload owns and gathers", "[io][whenall]") {
    std::vector<std::coroutine_handle<>> pending;
    std::optional<std::vector<int>> result;

    auto r = [](std::vector<std::coroutine_handle<>>& p,
                std::optional<std::vector<int>>& out) -> Runner {
        std::vector<Task<int>> tasks;
        tasks.push_back(delayedInt(p, 7));
        tasks.push_back(delayedInt(p, 8));
        out = co_await whenAll(std::move(tasks));
    }(pending, result);

    REQUIRE(pending.size() == 2);
    pending[0].resume();
    pending[1].resume();
    if (r.handle) r.handle.destroy();

    REQUIRE(result.has_value());
    REQUIRE(*result == std::vector<int>{7, 8});
}
