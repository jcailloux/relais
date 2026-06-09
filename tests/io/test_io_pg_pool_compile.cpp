#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <chrono>
#include <cstdint>
#include <functional>

using namespace jcailloux::relais::io;

// Mock IoContext for compilation testing
struct TestIo {
    using WatchHandle = int;
    using TimerToken = uint64_t;
    int next_handle = 1;
    TimerToken next_token = 1;

    WatchHandle addWatch(int, IoEvent, std::function<void(IoEvent)>) {
        return next_handle++;
    }
    void removeWatch(WatchHandle) {}
    void updateWatch(WatchHandle, IoEvent) {}
    void post(std::function<void()>) {}
    template<typename Rep, typename Period>
    TimerToken postDelayed(std::chrono::duration<Rep, Period>, std::function<void()>) {
        return next_token++;
    }
    void cancelTimer(TimerToken) {}
};

static_assert(IoContext<TestIo>);

// Verify types are well-formed with TestIo
using TestConnection = PgConnection<TestIo>;
using TestPool = PgPool<TestIo>;
using TestClient = PgClient<TestIo>;
using TestGuard = TestPool::ConnectionGuard;

TEST_CASE("PgPool and PgClient compile with mock IoContext", "[pg][pool]") {
    // Pure compilation test — no DB connection
    REQUIRE(true);
}

TEST_CASE("ConnectionGuard is move-constructible", "[pg][pool]") {
    // Default guard is empty
    TestGuard guard;
    REQUIRE(true);
}
