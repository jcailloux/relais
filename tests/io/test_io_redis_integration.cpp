#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/io/redis/RedisError.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

// =============================================================================
// Arguments
// =============================================================================

namespace {

template<typename T>
constexpr bool redisArg = RedisArg<T>;

enum Plain { PlainValue };
enum class Scoped : int64_t { Value };

struct ConvertibleToString {
    operator std::string() const { return "x"; }
};

}  // namespace

TEST_CASE("RedisArg accepts numbers and text only", "[redis]") {
    STATIC_REQUIRE(redisArg<int>);
    STATIC_REQUIRE(redisArg<int16_t>);
    STATIC_REQUIRE(redisArg<uint64_t>);
    STATIC_REQUIRE(redisArg<float>);
    STATIC_REQUIRE(redisArg<const double&>);
    STATIC_REQUIRE(redisArg<const char*>);
    STATIC_REQUIRE(redisArg<const char (&)[4]>);
    STATIC_REQUIRE(redisArg<std::string>);
    STATIC_REQUIRE(redisArg<const std::string&>);
    STATIC_REQUIRE(redisArg<std::string_view>);

    STATIC_REQUIRE_FALSE(redisArg<Plain>);
    STATIC_REQUIRE_FALSE(redisArg<Scoped>);
    STATIC_REQUIRE_FALSE(redisArg<bool>);
    STATIC_REQUIRE_FALSE(redisArg<char>);
    STATIC_REQUIRE_FALSE(redisArg<std::nullptr_t>);
    STATIC_REQUIRE_FALSE(redisArg<std::optional<int>>);
    STATIC_REQUIRE_FALSE(redisArg<std::vector<int>>);
    STATIC_REQUIRE_FALSE(redisArg<ConvertibleToString>);
}

TEST_CASE("RedisClient sends numbers as their exact text", "[redis][integration]") {
    EpollIoContext io;

    auto values = runTask(io, [](EpollIoContext& io) -> Task<std::vector<std::string>> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());
        std::vector<std::string> out;
        auto roundTrip = [&](auto v) -> Task<void> {
            co_await client->exec("SET", "relais:io:test:number", v);
            out.push_back((co_await client->exec("GET", "relais:io:test:number")).asString());
        };
        co_await roundTrip(0.1234567891);
        co_await roundTrip(0.1f);
        co_await roundTrip(std::numeric_limits<uint64_t>::max());
        co_await roundTrip(int16_t{-32768});

        // Redis reads the argument as a number: 1e-7 must not arrive as 0.
        co_await client->exec("SET", "relais:io:test:number", 0);
        out.push_back((co_await client->exec("INCRBYFLOAT", "relais:io:test:number", 1e-7)).asString());

        co_await client->exec("DEL", "relais:io:test:number");
        co_return out;
    }(io));

    REQUIRE(values.size() == 5);
    CHECK(values[0] == "0.1234567891");
    CHECK(values[1] == "0.1");
    CHECK(values[2] == "18446744073709551615");
    CHECK(values[3] == "-32768");
    CHECK(values[4] == "0.0000001");
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
