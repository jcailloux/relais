/**
 * bench_io_redis.cpp
 *
 * Performance benchmarks for the Redis I/O layer.
 * Measures raw RedisClient latency, independent of the relais cache hierarchy.
 *
 * Run with:
 *   ./bench_io_redis                    # all benchmarks
 *   ./bench_io_redis "[ping]"           # PING only
 *   ./bench_io_redis "[payload]"        # payload size benchmarks
 *   BENCH_SAMPLES=1000 ./bench_io_redis # custom sample count
 */

#include <catch2/catch_test_macros.hpp>

#include "BenchEngine.h"

#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/io/redis/RedisError.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::test;
using namespace relais_bench;

static const char* redisHost() {
    const char* h = std::getenv("REDIS_HOST");
    return h ? h : "127.0.0.1";
}

static int redisPort() {
    const char* p = std::getenv("REDIS_PORT");
    return p ? std::atoi(p) : 6379;
}


// #############################################################################
//
//  1. PING latency (baseline round-trip)
//
// #############################################################################

TEST_CASE("Benchmark - Redis PING", "[benchmark][redis][ping]")
{
    EpollIoContext io;

    auto results = runTask(io, [](EpollIoContext& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("PING", [&]() -> Task<void> {
            co_await client->exec("PING");
        }));

        co_return results;
    }(io));

    WARN(formatTable("Redis PING", results));
}


// #############################################################################
//
//  2. SET/GET round-trip (small values)
//
// #############################################################################

TEST_CASE("Benchmark - Redis SET/GET", "[benchmark][redis][set-get]")
{
    EpollIoContext io;

    auto results = runTask(io, [](EpollIoContext& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("SET (small)", [&]() -> Task<void> {
            co_await client->exec("SET", "bench:io:key", "hello");
        }));

        // Pre-populate for GET
        co_await client->exec("SET", "bench:io:key", "hello");

        results.push_back(co_await benchAsync("GET (small)", [&]() -> Task<void> {
            co_await client->exec("GET", "bench:io:key");
        }));

        results.push_back(co_await benchAsync("SET+GET round-trip", [&]() -> Task<void> {
            co_await client->exec("SET", "bench:io:rt", "value");
            co_await client->exec("GET", "bench:io:rt");
        }));

        // Cleanup
        co_await client->exec("DEL", "bench:io:key", "bench:io:rt");

        co_return results;
    }(io));

    WARN(formatTable("Redis SET/GET (small)", results));
}


// #############################################################################
//
//  3. Payload size impact
//
// #############################################################################

TEST_CASE("Benchmark - Redis payload sizes", "[benchmark][redis][payload]")
{
    EpollIoContext io;

    auto results = runTask(io, [](EpollIoContext& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());

        std::string val_100(100, 'x');
        std::string val_1k(1024, 'x');
        std::string val_10k(10240, 'x');

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("SET 100B", [&]() -> Task<void> {
            co_await client->exec("SET", "bench:io:p100", val_100);
        }));

        results.push_back(co_await benchAsync("SET 1KB", [&]() -> Task<void> {
            co_await client->exec("SET", "bench:io:p1k", val_1k);
        }));

        results.push_back(co_await benchAsync("SET 10KB", [&]() -> Task<void> {
            co_await client->exec("SET", "bench:io:p10k", val_10k);
        }));

        // Pre-populate for GET
        co_await client->exec("SET", "bench:io:p100", val_100);
        co_await client->exec("SET", "bench:io:p1k", val_1k);
        co_await client->exec("SET", "bench:io:p10k", val_10k);

        results.push_back(co_await benchAsync("GET 100B", [&]() -> Task<void> {
            co_await client->exec("GET", "bench:io:p100");
        }));

        results.push_back(co_await benchAsync("GET 1KB", [&]() -> Task<void> {
            co_await client->exec("GET", "bench:io:p1k");
        }));

        results.push_back(co_await benchAsync("GET 10KB", [&]() -> Task<void> {
            co_await client->exec("GET", "bench:io:p10k");
        }));

        // Cleanup
        co_await client->exec("DEL", "bench:io:p100", "bench:io:p1k", "bench:io:p10k");

        co_return results;
    }(io));

    WARN(formatTable("Redis payload sizes", results));
}


// #############################################################################
//
//  4. EVAL (Lua script round-trip)
//
// #############################################################################

TEST_CASE("Benchmark - Redis EVAL", "[benchmark][redis][eval]")
{
    EpollIoContext io;

    auto results = runTask(io, [](EpollIoContext& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("EVAL return 1", [&]() -> Task<void> {
            co_await client->exec("EVAL", "return 1", "0");
        }));

        // Pre-populate for Lua GET
        co_await client->exec("SET", "bench:io:lua", "lua_value");

        results.push_back(co_await benchAsync("EVAL redis.call GET", [&]() -> Task<void> {
            co_await client->exec("EVAL",
                "return redis.call('GET', KEYS[1])", "1", "bench:io:lua");
        }));

        results.push_back(co_await benchAsync("EVAL SET+GET", [&]() -> Task<void> {
            co_await client->exec("EVAL",
                "redis.call('SET', KEYS[1], ARGV[1]) "
                "return redis.call('GET', KEYS[1])",
                "1", "bench:io:lua", "new_value");
        }));

        // Cleanup
        co_await client->exec("DEL", "bench:io:lua");

        co_return results;
    }(io));

    WARN(formatTable("Redis EVAL (Lua)", results));
}


// #############################################################################
//
//  5. INCR (atomic counter, minimal payload)
//
// #############################################################################

TEST_CASE("Benchmark - Redis INCR", "[benchmark][redis][incr]")
{
    EpollIoContext io;

    auto results = runTask(io, [](EpollIoContext& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<EpollIoContext>::connect(io, redisHost(), redisPort());

        co_await client->exec("SET", "bench:io:ctr", "0");

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("INCR", [&]() -> Task<void> {
            co_await client->exec("INCR", "bench:io:ctr");
        }));

        // Cleanup
        co_await client->exec("DEL", "bench:io:ctr");

        co_return results;
    }(io));

    WARN(formatTable("Redis INCR", results));
}
