#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/EpollIoContext.h>

#include <fixtures/AllocationCounter.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace jcailloux::relais::io;
using namespace std::chrono_literals;

namespace {

// What a hundred turns allocate, each running a posted callback and a timer,
// and a hundred more with nothing to run, once two such turns have run.
std::uint64_t allocationsOfWarmTurns(EpollIoContext& io) {
    int ran = 0;
    auto turn = [&] {
        io.post([&ran] { ++ran; });
        io.postDelayed(0ns, [&ran] { ++ran; });
        io.runOnce(0);
    };
    turn();
    turn();
    const auto before = relais_test::threadAllocations().count;
    for (int i = 0; i < 100; ++i) {
        io.runOnce(0);
        turn();
    }
    const auto allocated = relais_test::threadAllocations().count - before;
    REQUIRE(ran == 2 * 102);
    return allocated;
}

}  // namespace

TEST_CASE("EpollIoContext: post() from same thread", "[io][epoll]") {
    EpollIoContext io;
    bool called = false;

    io.post([&] { called = true; });
    io.runOnce(0);

    REQUIRE(called);
}

TEST_CASE("EpollIoContext: post() from different thread", "[io][epoll]") {
    EpollIoContext io;
    std::atomic<bool> called{false};

    std::thread t([&] {
        io.post([&] { called.store(true); });
    });
    t.join();

    // Run until the callback fires
    io.runUntil([&] { return called.load(); });
    REQUIRE(called.load());
}

TEST_CASE("EpollIoContext: postDelayed fires after delay", "[io][epoll]") {
    EpollIoContext io;
    bool called = false;

    io.postDelayed(std::chrono::milliseconds(10), [&] { called = true; });

    // Should not have fired yet
    io.runOnce(0);
    REQUIRE_FALSE(called);

    // Wait enough for the timer to fire
    io.runOnce(50);
    REQUIRE(called);
}

TEST_CASE("EpollIoContext: cancelTimer prevents callback", "[io][epoll]") {
    EpollIoContext io;
    bool called = false;

    auto token = io.postDelayed(std::chrono::milliseconds(10), [&] { called = true; });
    io.cancelTimer(token);

    // Wait well past the scheduled time
    io.runOnce(50);
    REQUIRE_FALSE(called);
}

TEST_CASE("EpollIoContext: stop() exits run()", "[io][epoll]") {
    EpollIoContext io;

    // Schedule a stop after a short delay
    io.postDelayed(std::chrono::milliseconds(10), [&] { io.stop(); });

    // run() should return after stop() is called
    io.run();
    // If we reach here, stop() worked
    REQUIRE(true);
}

TEST_CASE("EpollIoContext: multiple timers fire in order", "[io][epoll]") {
    EpollIoContext io;
    std::vector<int> order;

    io.postDelayed(std::chrono::milliseconds(30), [&] { order.push_back(3); });
    io.postDelayed(std::chrono::milliseconds(10), [&] { order.push_back(1); });
    io.postDelayed(std::chrono::milliseconds(20), [&] { order.push_back(2); });

    // Run until all timers fire
    io.runUntil([&] { return order.size() >= 3; });

    REQUIRE(order == std::vector<int>{1, 2, 3});
}

TEST_CASE("EpollIoContext: loop-local timer arm issues no pipe wakeup", "[io][epoll]") {
    EpollIoContext io;
    bool done = false;
    uint64_t w_before = 0, w_after = 0;

    // This callback runs ON the loop thread (delivered by runUntil below).
    io.post([&] {
        REQUIRE(io.isInLoopThread());
        w_before = io.loopWakeups();
        io.postDelayed(std::chrono::milliseconds(1), [&] { done = true; });
        w_after = io.loopWakeups();
    });
    io.runUntil([&] { return done; });

    // Arming from the loop thread re-arms the timerfd in runOnce; no syscall.
    REQUIRE(w_after == w_before);
}

TEST_CASE("EpollIoContext: cross-thread timer arm wakes the loop", "[io][epoll]") {
    EpollIoContext io;
    std::atomic<bool> fired{false};
    uint64_t before = io.loopWakeups();

    std::thread t([&] {
        io.postDelayed(std::chrono::milliseconds(1), [&] { fired.store(true); });
    });
    t.join();

    io.runUntil([&] { return fired.load(); });
    REQUIRE(io.loopWakeups() > before);  // a blocked loop must be poked awake
}

TEST_CASE("EpollIoContext: cancel removes timers without accumulation", "[io][epoll]") {
    EpollIoContext io;
    constexpr int N = 2000;

    std::vector<EpollIoContext::TimerToken> tokens;
    tokens.reserve(N);
    for (int i = 0; i < N; ++i)
        tokens.push_back(io.postDelayed(std::chrono::seconds(60), [] {}));
    REQUIRE(io.pendingTimerCount() == N);

    for (auto t : tokens) io.cancelTimer(t);
    // Direct removal — not deferred to the (60s) deadline. The old tombstone
    // scheme would keep N entries queued plus N cancel markers.
    REQUIRE(io.pendingTimerCount() == 0);

    // The subsystem is still live after the churn.
    bool fired = false;
    io.postDelayed(std::chrono::milliseconds(1), [&] { fired = true; });
    io.runUntil([&] { return fired; });
    REQUIRE(fired);
    REQUIRE(io.pendingTimerCount() == 0);
}

TEST_CASE("EpollIoContext: thread-safe post with multiple threads", "[io][epoll]") {
    EpollIoContext io;
    std::atomic<int> count{0};
    constexpr int N = 100;

    std::vector<std::thread> threads;
    for (int i = 0; i < N; ++i) {
        threads.emplace_back([&] {
            io.post([&] { count.fetch_add(1); });
        });
    }
    for (auto& t : threads) t.join();

    // Run until all callbacks processed
    io.runUntil([&] { return count.load() >= N; });
    REQUIRE(count.load() == N);
}

TEST_CASE("EpollIoContext: a loop turn whose timers did not move earlier rearms nothing",
          "[io][epoll]") {
    EpollIoContext io;
    io.runOnce(0);
    REQUIRE(io.timerfdArms() == 0);  // nothing to arm

    io.postDelayed(std::chrono::seconds(60), [] {});
    io.runOnce(0);
    REQUIRE(io.timerfdArms() == 1);

    for (int i = 0; i < 100; ++i) {
        io.post([] {});
        io.runOnce(0);
    }
    CHECK(io.timerfdArms() == 1);

    // A later timer leaves the expiry where it is; an earlier one moves it.
    io.postDelayed(std::chrono::seconds(120), [] {});
    io.runOnce(0);
    CHECK(io.timerfdArms() == 1);
    io.postDelayed(std::chrono::seconds(30), [] {});
    io.runOnce(0);
    CHECK(io.timerfdArms() == 2);
}

TEST_CASE("EpollIoContext: an earlier timer fires on time while a later one is armed",
          "[io][epoll]") {
    EpollIoContext io;
    io.postDelayed(std::chrono::seconds(10), [] {});
    io.runOnce(0);

    bool fired = false;
    const auto t0 = std::chrono::steady_clock::now();
    io.postDelayed(std::chrono::milliseconds(5), [&] { fired = true; });
    io.runUntil([&] { return fired; });

    // Well before the loop's own 100 ms wait would have run it.
    CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(60));
}

TEST_CASE("EpollIoContext: a cancelled earliest timer lets the next one fire on time",
          "[io][epoll]") {
    EpollIoContext io;
    bool cancelled_fired = false, fired = false;
    const auto t0 = std::chrono::steady_clock::now();
    auto first = io.postDelayed(std::chrono::milliseconds(20), [&] { cancelled_fired = true; });
    io.postDelayed(std::chrono::milliseconds(40), [&] { fired = true; });
    io.runOnce(0);  // the timerfd is armed for the first
    io.cancelTimer(first);

    io.runUntil([&] { return fired; });
    CHECK_FALSE(cancelled_fired);
    CHECK(std::chrono::steady_clock::now() - t0 >= std::chrono::milliseconds(40));
    // Armed for the first, then, from its expiry for nothing, for the second.
    CHECK(io.timerfdArms() == 2);
}

TEST_CASE("EpollIoContext: a loop turn allocates nothing", "[io][epoll]") {
    EpollIoContext io;
    CHECK(allocationsOfWarmTurns(io) == 0);

    // Callbacks posted from another thread: the loop's thread runs them
    // without allocating either.
    std::atomic<int> ran{0};
    auto postFromAnotherThread = [&] {
        std::thread([&] {
            for (int i = 0; i < 8; ++i) io.post([&ran] { ran.fetch_add(1); });
        }).join();
    };
    postFromAnotherThread();
    io.runUntil([&] { return ran.load() == 8; });
    postFromAnotherThread();
    const auto before = relais_test::threadAllocations().count;
    io.runUntil([&] { return ran.load() == 16; });
    CHECK(relais_test::threadAllocations().count == before);
}

// A posted callback may run the loop itself: that turn runs what was posted
// since, then the rest of the outer batch runs, each callback once.
TEST_CASE("EpollIoContext: a posted callback that runs the loop", "[io][epoll]") {
    EpollIoContext io;
    std::string ran;
    io.post([&] {
        ran += 'A';
        io.post([&] { ran += 'D'; });
        io.runOnce(0);
    });
    io.post([&] { ran += 'B'; });
    io.post([&] { ran += 'C'; });
    io.runOnce(0);
    io.runOnce(0);
    CHECK(ran == "ADBC");
    CHECK(allocationsOfWarmTurns(io) == 0);
}

// A timer callback may run the loop itself: that turn fires the timers due
// since, then the rest of the outer batch fires, each timer once.
TEST_CASE("EpollIoContext: a timer callback that runs the loop", "[io][epoll]") {
    EpollIoContext io;
    std::vector<std::string> fired;
    // Its captures outgrow std::function's inline buffer: a timer destroyed
    // before it fires is a use after free, which ASan reports.
    const std::string second(64, '2');
    io.postDelayed(0ns, [&] {
        fired.emplace_back("first");
        io.postDelayed(0ns, [&] { fired.emplace_back("set by the first"); });
        io.runOnce(0);
        fired.emplace_back("first, after its turn");
    });
    io.postDelayed(0ns, [&fired, second] { fired.push_back(second); });
    io.runOnce(0);
    io.runOnce(0);
    CHECK(fired == std::vector<std::string>{"first", "set by the first", "first, after its turn", second});
    CHECK(allocationsOfWarmTurns(io) == 0);
}

// A callback that throws leaves runOnce; the loop stays whole: what is posted
// and armed next runs, and a turn again allocates nothing.
TEST_CASE("EpollIoContext: a callback that throws leaves the loop whole", "[io][epoll]") {
    EpollIoContext io;
    io.post([] { throw std::runtime_error("posted"); });
    CHECK_THROWS_AS(io.runOnce(0), std::runtime_error);
    io.postDelayed(0ns, [] { throw std::runtime_error("timer"); });
    CHECK_THROWS_AS(io.runOnce(0), std::runtime_error);
    CHECK(allocationsOfWarmTurns(io) == 0);
}

// The buffer that runs posted callbacks keeps its capacity from one turn to
// the next, up to a thousand callbacks: a burst's is freed once run.
TEST_CASE("EpollIoContext: a burst of posted callbacks leaves no memory held", "[io][epoll]") {
    EpollIoContext io;
    io.runOnce(0);
    const auto before = relais_test::threadAllocations().live_bytes;
    int ran = 0;
    for (int i = 0; i < 100'000; ++i) io.post([&ran] { ++ran; });
    io.runOnce(0);
    REQUIRE(ran == 100'000);
    const auto held = relais_test::threadAllocations().live_bytes - before;
    CHECK(held <= static_cast<std::int64_t>(2 * 1024 * sizeof(std::function<void()>)));
}
