#include <catch2/catch_test_macros.hpp>

#include <fixtures/EpollIoContext.h>
#include <jcailloux/relais/io/redis/RedisPool.h>
#include <jcailloux/relais/io/Task.h>

using namespace jcailloux::relais::io;
using Io = test::EpollIoContext;

TEST_CASE("RedisPool: create and round-robin", "[io][redis][pool][integration]") {
    Io io;
    bool done = false;

    auto task = [&]() -> DetachedTask {
        auto pool = co_await RedisPool<Io>::create(io, "127.0.0.1", 6379, 3);

        REQUIRE(pool.size() == 3);
        REQUIRE_FALSE(pool.empty());

        // Round-robin: each call returns a different client
        auto& c0 = pool.next();
        auto& c1 = pool.next();
        auto& c2 = pool.next();
        auto& c3 = pool.next();  // Should wrap around to c0

        REQUIRE(&c0 != &c1);
        REQUIRE(&c1 != &c2);
        REQUIRE(&c3 == &c0);  // Wrapped around

        // Each client should be connected
        REQUIRE(c0.connected());
        REQUIRE(c1.connected());
        REQUIRE(c2.connected());

        // Execute a command through the pool
        auto result = co_await pool.next().exec("PING");
        REQUIRE(result.isString());
        REQUIRE(result.asStringView() == "PONG");

        done = true;
    };

    task();
    io.runUntil([&] { return done; });
    REQUIRE(done);
}

TEST_CASE("RedisPool: concurrent commands via round-robin", "[io][redis][pool][integration]") {
    Io io;
    bool done = false;

    auto task = [&]() -> DetachedTask {
        auto pool = co_await RedisPool<Io>::create(io, "127.0.0.1", 6379, 4);

        // SET then GET via different connections
        co_await pool.next().exec("SET", "pool_test_key", "pool_test_value");
        auto result = co_await pool.next().exec("GET", "pool_test_key");

        REQUIRE(result.isString());
        REQUIRE(result.asStringView() == "pool_test_value");

        // Clean up
        co_await pool.next().exec("DEL", "pool_test_key");

        done = true;
    };

    task();
    io.runUntil([&] { return done; });
    REQUIRE(done);
}
