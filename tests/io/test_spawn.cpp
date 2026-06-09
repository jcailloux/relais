#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/runtime/Spawn.h>
#include <jcailloux/relais/io/EpollIoContext.h>
#include <jcailloux/relais/io/Task.h>

#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

using namespace jcailloux::relais;
using jcailloux::relais::io::Task;
using Io = jcailloux::relais::io::EpollIoContext;

namespace {

Task<int> makeValue(int v) { co_return v; }

Task<int> makeThrow() {
    throw std::runtime_error("boom");
    co_return 0;  // unreachable, makes this a coroutine
}

Task<std::string> makeString(std::string s) { co_return s; }

Task<void> makeVoidOk() { co_return; }

Task<void> makeVoidThrow() {
    throw std::runtime_error("void-boom");
    co_return;
}

// Runs an EpollIoContext on a dedicated thread — the realistic spawnOn target:
// a worker loop that owns the I/O resources, fed from another thread.
struct LoopThread {
    Io io;
    std::thread th{[this] { io.run(); }};
    ~LoopThread() {
        io.stop();
        th.join();
    }
};

template<typename T>
T await(std::future<T>& f) {
    REQUIRE(f.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
    return f.get();
}

}  // namespace

TEST_CASE("spawnOn: value Task delivers the value", "[io][spawn]") {
    LoopThread L;
    std::promise<Outcome<int>> p;
    auto fut = p.get_future();

    spawnOn(L.io, makeValue(42),
            [&p](Outcome<int> r) { p.set_value(std::move(r)); });

    auto r = await(fut);
    REQUIRE(r.has_value());
    REQUIRE(*r == 42);
}

TEST_CASE("spawnOn: throwing Task delivers the exception", "[io][spawn]") {
    LoopThread L;
    std::promise<Outcome<int>> p;
    auto fut = p.get_future();

    spawnOn(L.io, makeThrow(),
            [&p](Outcome<int> r) { p.set_value(std::move(r)); });

    auto r = await(fut);
    REQUIRE_FALSE(r.has_value());
    REQUIRE_THROWS_AS(std::rethrow_exception(r.error()), std::runtime_error);
}

TEST_CASE("spawnOn: void Task success carries no error", "[io][spawn]") {
    LoopThread L;
    std::promise<Outcome<void>> p;
    auto fut = p.get_future();

    spawnOn(L.io, makeVoidOk(),
            [&p](Outcome<void> r) { p.set_value(std::move(r)); });

    auto r = await(fut);
    REQUIRE(r.has_value());
}

TEST_CASE("spawnOn: void Task exception is propagated", "[io][spawn]") {
    LoopThread L;
    std::promise<Outcome<void>> p;
    auto fut = p.get_future();

    spawnOn(L.io, makeVoidThrow(),
            [&p](Outcome<void> r) { p.set_value(std::move(r)); });

    auto r = await(fut);
    REQUIRE_FALSE(r.has_value());
    REQUIRE_THROWS_AS(std::rethrow_exception(r.error()), std::runtime_error);
}

TEST_CASE("spawnOn: pre-resolved fromValue Task bypasses coroutine machinery",
          "[io][spawn]") {
    LoopThread L;
    std::promise<Outcome<int>> p;
    auto fut = p.get_future();

    spawnOn(L.io, Task<int>::fromValue(7),
            [&p](Outcome<int> r) { p.set_value(std::move(r)); });

    auto r = await(fut);
    REQUIRE(r.has_value());
    REQUIRE(*r == 7);
}

TEST_CASE("spawnOn: move-only value type round-trips", "[io][spawn]") {
    LoopThread L;
    std::promise<std::string> p;
    auto fut = p.get_future();

    spawnOn(L.io, makeString("hello"),
            [&p](Outcome<std::string> r) { p.set_value(std::move(*r)); });

    REQUIRE(await(fut) == "hello");
}

TEST_CASE("spawnOn: on_done runs on the loop thread, not the caller",
          "[io][spawn]") {
    LoopThread L;
    auto loop_tid = L.th.get_id();
    auto caller_tid = std::this_thread::get_id();
    REQUIRE(loop_tid != caller_tid);

    std::promise<std::thread::id> p;
    auto fut = p.get_future();

    spawnOn(L.io, makeValue(1),
            [&p](Outcome<int>) { p.set_value(std::this_thread::get_id()); });

    REQUIRE(await(fut) == loop_tid);
}

TEST_CASE("spawnOn: accepts a move-only on_done callback", "[io][spawn]") {
    // The callback lives in the driver frame, not the std::function, so it does
    // not need to be copyable — capturing a unique_ptr must compile and run.
    LoopThread L;
    std::promise<int> p;
    auto fut = p.get_future();
    auto held = std::make_unique<int>(99);

    spawnOn(L.io, makeValue(1),
            [up = std::move(held), &p](Outcome<int> r) {
                p.set_value(*up + *r);
            });

    REQUIRE(await(fut) == 100);
}

TEST_CASE("spawnOn: bootstrap pattern — block until a lazy Task completes",
          "[io][spawn]") {
    // Mirrors the real consumer's startup: drive a lazy Task to completion on
    // the loop from another thread, signal a promise, block on the future.
    LoopThread L;
    std::promise<int> ready;
    auto fut = ready.get_future();

    spawnOn(L.io, makeValue(7),
            [&ready](Outcome<int> r) {
                if (r) ready.set_value(*r);
                else   ready.set_exception(r.error());
            });

    REQUIRE(fut.get() == 7);
}

TEST_CASE("spawnOn: many concurrent dispatches all complete exactly once",
          "[io][spawn]") {
    LoopThread L;
    constexpr int N = 1000;
    std::atomic<int> sum{0};
    std::atomic<int> count{0};

    for (int i = 0; i < N; ++i) {
        spawnOn(L.io, makeValue(i), [&](Outcome<int> r) {
            sum.fetch_add(*r, std::memory_order_relaxed);
            count.fetch_add(1, std::memory_order_relaxed);
        });
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (count.load(std::memory_order_relaxed) < N &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }

    REQUIRE(count.load() == N);
    REQUIRE(sum.load() == N * (N - 1) / 2);
}

TEST_CASE("spawnOn: works against a single-threaded inline loop", "[io][spawn]") {
    // No background thread — drive the loop by hand. Exercises the path where
    // the same thread posts and drains.
    Io io;
    bool done = false;
    int got = 0;

    spawnOn(io, makeValue(123), [&](Outcome<int> r) {
        got = *r;
        done = true;
    });

    io.runUntil([&] { return done; });
    REQUIRE(done);
    REQUIRE(got == 123);
}