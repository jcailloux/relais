#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/EpollIoContext.h>

#include <atomic>
#include <chrono>
#include <thread>

using namespace jcailloux::relais::io;

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
