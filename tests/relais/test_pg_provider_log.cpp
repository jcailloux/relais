#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/Log.h>
#include <jcailloux/relais/PgProvider.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>
#include <fixtures/TestConnInfo.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
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

TEST_CASE("Log: the callback can change while other threads log", "[log]") {
    static std::atomic<int> calls{0};
    auto counting = [](log::Level, const char*, size_t) {
        calls.fetch_add(1, std::memory_order_relaxed);
    };

    std::atomic<bool> stop{false};
    std::vector<std::thread> loggers;
    for (int t = 0; t < 4; ++t) {
        loggers.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) RELAIS_LOG_DEBUG << "tick";
        });
    }
    for (int i = 0; i < 1000; ++i) {
        log::setCallback(i % 2 ? +counting : nullptr);
    }
    stop.store(true, std::memory_order_relaxed);
    for (auto& th : loggers) th.join();

    log::setCallback(counting);
    const int before = calls.load();
    RELAIS_LOG_DEBUG << "after";
    CHECK(calls.load() == before + 1);
    log::setCallback(nullptr);
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

TEST_CASE("Log: numbers are exact, bool is spelled out", "[log]") {
    captured_logs.clear();
    log::setCallback(testLogCallback);

    RELAIS_LOG_ERROR << 0.1234567891 << ' ' << 1e-7 << ' ' << 0.1f << ' '
                     << int8_t{-128} << ' ' << UINT64_MAX << ' ' << true << ' ' << false;
    REQUIRE(captured_logs[0].message == "0.1234567891 1e-07 0.1 -128 18446744073709551615 true false");

    log::setCallback(nullptr);
}

template<typename T>
constexpr bool loggable = requires(log::LogStream& s, T v) { s << v; };

TEST_CASE("Log: a pointer does not log as bool", "[log]") {
    STATIC_REQUIRE(loggable<bool>);
    STATIC_REQUIRE(loggable<const char*>);
    STATIC_REQUIRE_FALSE(loggable<int*>);
}

// =============================================================================
// PgProvider tests
// =============================================================================

TEST_CASE("PgProvider: not initialized", "[dbprovider]") {
    PgProvider::reset();
    REQUIRE_FALSE(PgProvider::initialized());
}

TEST_CASE("PgProvider: init with PgPool", "[dbprovider][integration]") {
    using jcailloux::relais::io::test::EpollIoContext;
    using jcailloux::relais::io::test::runTask;

    EpollIoContext io;

    auto conninfo = std::string(relais_test::kConnInfo);

    runTask(io, [](EpollIoContext& io, const std::string& conninfo) -> jcailloux::relais::io::Task<void> {
        auto pool = co_await jcailloux::relais::io::PgPool<EpollIoContext>::create(
            io, conninfo, {.min_connections = 1, .max_connections = 4});
        PgProvider::init(io, pool);

        REQUIRE(PgProvider::initialized());
    
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
