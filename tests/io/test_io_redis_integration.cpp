#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/io/redis/RedisError.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

#include <cstdlib>
#include <string>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::test;

static const char* redisHost() {
    const char* h = std::getenv("REDIS_HOST");
    return h ? h : "127.0.0.1";
}

static int redisPort() {
    const char* p = std::getenv("REDIS_PORT");
    return p ? std::atoi(p) : 6379;
}

// =============================================================================
// Connection
// =============================================================================

TEST_CASE("RedisClient async connect", "[redis][integration]") {
    EpollIoContext io;

    auto client = runTask(io, [](EpollIoContext& io) -> Task<std::shared_ptr<RedisClient<EpollIoContext>>> {
        co_return co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());
    }(io));

    REQUIRE(client->connected());
}

// =============================================================================
// Basic commands
// =============================================================================

TEST_CASE("RedisClient SET and GET", "[redis][integration]") {
    EpollIoContext io;

    auto result = runTask(io, [](EpollIoContext& io) -> Task<std::string> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());

        // SET
        auto setResult = co_await client->exec("SET", "relais:io:test:key1", "hello_relais");
        REQUIRE(setResult.isString());

        // GET
        auto getResult = co_await client->exec("GET", "relais:io:test:key1");
        REQUIRE(getResult.isString());

        // Cleanup
        co_await client->exec("DEL", "relais:io:test:key1");

        co_return getResult.asString();
    }(io));

    REQUIRE(result == "hello_relais");
}

TEST_CASE("RedisClient GET nonexistent returns nil", "[redis][integration]") {
    EpollIoContext io;

    auto isNil = runTask(io, [](EpollIoContext& io) -> Task<bool> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());
        auto r = co_await client->exec("GET", "relais:io:test:nonexistent_key_xyz");
        co_return r.isNil();
    }(io));

    REQUIRE(isNil);
}

TEST_CASE("RedisClient INCR", "[redis][integration]") {
    EpollIoContext io;

    auto value = runTask(io, [](EpollIoContext& io) -> Task<int64_t> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());

        co_await client->exec("DEL", "relais:io:test:counter");
        co_await client->exec("SET", "relais:io:test:counter", "10");
        auto r = co_await client->exec("INCR", "relais:io:test:counter");

        // Cleanup
        co_await client->exec("DEL", "relais:io:test:counter");

        co_return r.asInteger();
    }(io));

    REQUIRE(value == 11);
}

TEST_CASE("RedisClient TTL (SET EX)", "[redis][integration]") {
    EpollIoContext io;

    auto ttl = runTask(io, [](EpollIoContext& io) -> Task<int64_t> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());

        co_await client->exec("SET", "relais:io:test:ttl_key", "value", "EX", "300");
        auto r = co_await client->exec("TTL", "relais:io:test:ttl_key");

        // Cleanup
        co_await client->exec("DEL", "relais:io:test:ttl_key");

        co_return r.asInteger();
    }(io));

    REQUIRE(ttl > 0);
    REQUIRE(ttl <= 300);
}

TEST_CASE("RedisClient multiple sequential commands", "[redis][integration]") {
    EpollIoContext io;

    auto count = runTask(io, [](EpollIoContext& io) -> Task<int64_t> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());

        // Use a list to test multiple commands
        co_await client->exec("DEL", "relais:io:test:list");
        co_await client->exec("RPUSH", "relais:io:test:list", "a");
        co_await client->exec("RPUSH", "relais:io:test:list", "b");
        co_await client->exec("RPUSH", "relais:io:test:list", "c");
        auto r = co_await client->exec("LLEN", "relais:io:test:list");

        // Cleanup
        co_await client->exec("DEL", "relais:io:test:list");

        co_return r.asInteger();
    }(io));

    REQUIRE(count == 3);
}
