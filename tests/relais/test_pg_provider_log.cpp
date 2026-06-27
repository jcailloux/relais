#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/Log.h>
#include <jcailloux/relais/PgProvider.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

#include <string>
#include <vector>

using namespace jcailloux::relais;

// =============================================================================
// Log tests
// =============================================================================

namespace {
    struct CapturedLog {
        log::Level level;
        std::string message;
    };

    std::vector<CapturedLog> captured_logs;

    void testLogCallback(log::Level level, const char* msg, size_t len) {
        captured_logs.push_back({level, std::string(msg, len)});
    }
}

TEST_CASE("Log: no callback means no crash", "[log]") {
    log::setCallback(nullptr);
    // Should not crash
    RELAIS_LOG_ERROR << "test error";
    RELAIS_LOG_WARN << "test warning";
    RELAIS_LOG_DEBUG << "test debug";
}

TEST_CASE("Log: callback receives messages", "[log]") {
    captured_logs.clear();
    log::setCallback(testLogCallback);

    RELAIS_LOG_ERROR << "error message";
    REQUIRE(captured_logs.size() == 1);
    REQUIRE(captured_logs[0].level == log::Level::Error);
    REQUIRE(captured_logs[0].message == "error message");

    RELAIS_LOG_WARN << "warn message";
    REQUIRE(captured_logs.size() == 2);
    REQUIRE(captured_logs[1].level == log::Level::Warn);

    RELAIS_LOG_DEBUG << "debug message";
    REQUIRE(captured_logs.size() == 3);
    REQUIRE(captured_logs[2].level == log::Level::Debug);

    log::setCallback(nullptr);
}

TEST_CASE("Log: streaming multiple values", "[log]") {
    captured_logs.clear();
    log::setCallback(testLogCallback);

    RELAIS_LOG_ERROR << "MyRepo: found " << 42 << " rows, expected " << 1;
    REQUIRE(captured_logs.size() == 1);
    REQUIRE(captured_logs[0].message == "MyRepo: found 42 rows, expected 1");

    std::string name = "TestCache";
    RELAIS_LOG_WARN << name << ": GET error - " << "connection refused";
    REQUIRE(captured_logs[1].message == "TestCache: GET error - connection refused");

    size_t count = 100;
    RELAIS_LOG_DEBUG << "Cleaned " << count << " entries";
    REQUIRE(captured_logs[2].message == "Cleaned 100 entries");

    log::setCallback(nullptr);
}

TEST_CASE("Log: char and string_view types", "[log]") {
    captured_logs.clear();
    log::setCallback(testLogCallback);

    RELAIS_LOG_ERROR << 'X' << " = " << std::string_view("hello");
    REQUIRE(captured_logs[0].message == "X = hello");

    log::setCallback(nullptr);
}

// =============================================================================
// PgProvider tests
// =============================================================================

TEST_CASE("PgProvider: not initialized", "[dbprovider]") {
    PgProvider::reset();
    REQUIRE_FALSE(PgProvider::initialized());
    REQUIRE_FALSE(PgProvider::hasRedis());
}

TEST_CASE("PgProvider: init with PgPool", "[dbprovider][integration]") {
    using jcailloux::relais::io::test::EpollIoContext;
    using jcailloux::relais::io::test::runTask;

    EpollIoContext io;

    auto conninfo = std::string("host=localhost port=5432 dbname=relais_test "
                                "user=relais_test password=relais_test");

    runTask(io, [](EpollIoContext& io, const std::string& conninfo) -> jcailloux::relais::io::Task<void> {
        auto pool = co_await jcailloux::relais::io::PgPool<EpollIoContext>::create(
            io, conninfo, {.min_connections = 1, .max_connections = 4});
        PgProvider::init(io, pool);

        REQUIRE(PgProvider::initialized());
        REQUIRE_FALSE(PgProvider::hasRedis());

        // Test simple query
        auto result = co_await PgProvider::query("SELECT 1 AS num");
        REQUIRE(result.ok());
        REQUIRE(result.rows() == 1);
        REQUIRE(result[0].get<int32_t>(0) == 1);

        // Test queryArgs
        auto result2 = co_await PgProvider::queryArgs(
            "SELECT $1::int + $2::int AS sum", 10, 32);
        REQUIRE(result2.ok());
        REQUIRE(result2[0].get<int32_t>(0) == 42);

        // Test queryParams
        auto params = jcailloux::relais::io::PgParams::make("hello");
        auto result3 = co_await PgProvider::queryParams(
            "SELECT $1::text AS msg", params);
        REQUIRE(result3.ok());
        REQUIRE(result3[0].get<std::string>(0) == "hello");

        PgProvider::reset();
    }(io, conninfo));
}

TEST_CASE("PgProvider: init with Redis", "[dbprovider][integration]") {
    using jcailloux::relais::io::test::EpollIoContext;
    using jcailloux::relais::io::test::runTask;

    EpollIoContext io;

    auto conninfo = std::string("host=localhost port=5432 dbname=relais_test "
                                "user=relais_test password=relais_test");

    runTask(io, [](EpollIoContext& io, const std::string& conninfo) -> jcailloux::relais::io::Task<void> {
        auto pool = co_await jcailloux::relais::io::PgPool<EpollIoContext>::create(
            io, conninfo, {.min_connections = 1, .max_connections = 4});
        auto redis = co_await jcailloux::relais::io::RedisClient<EpollIoContext>::connect(io);

        PgProvider::init(io, pool, redis);
        REQUIRE(PgProvider::initialized());
        REQUIRE(PgProvider::hasRedis());

        // Test Redis SET/GET
        co_await PgProvider::redis("SET", "dbprovider_test_key", "hello_world");

        auto reply = co_await PgProvider::redis("GET", "dbprovider_test_key");
        REQUIRE(reply.isString());
        REQUIRE(reply.asString() == "hello_world");

        // Cleanup
        co_await PgProvider::redis("DEL", "dbprovider_test_key");

        // Test Redis with numeric args
        co_await PgProvider::redis("SET", "dbprovider_test_num", std::to_string(42));
        auto num_reply = co_await PgProvider::redis("GET", "dbprovider_test_num");
        REQUIRE(num_reply.asString() == "42");

        co_await PgProvider::redis("DEL", "dbprovider_test_num");

        PgProvider::reset();
    }(io, conninfo));
}
