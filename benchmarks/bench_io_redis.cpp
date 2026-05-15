/**
 * bench_io_redis.cpp
 *
 * Performance benchmarks for the Redis I/O layer.
 *
 * Two flavors:
 *   - [latency]    : single coroutine, sequential round-trips (p50/p99 RTT).
 *   - [throughput] : N concurrent DetachedTask workers on the same epoll loop,
 *                    fanning out across a RedisPool of `kPoolSize` connections.
 *                    Without these, the latency benchmarks would mislead
 *                    readers into treating 1/RTT as the system's max throughput.
 *
 * Run with:
 *   ./bench_io_redis                              # all benchmarks
 *   ./bench_io_redis "[latency]"                  # latency only
 *   ./bench_io_redis "[throughput]"               # throughput only
 *   ./bench_io_redis "[throughput][get]"          # throughput GET only
 *   BENCH_SAMPLES=1000 ./bench_io_redis "[latency]"
 *   BENCH_DURATION_S=10 ./bench_io_redis "[throughput]"
 */

#include <catch2/catch_test_macros.hpp>

#include "BenchEngine.h"

#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/io/redis/RedisPool.h>
#include <jcailloux/relais/io/redis/RedisError.h>
#include <jcailloux/relais/io/Task.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::test;
using namespace relais_bench;

using Io = EpollIoContext;

static const char* redisHost() {
    const char* h = std::getenv("REDIS_HOST");
    return h ? h : "127.0.0.1";
}

static int redisPort() {
    const char* p = std::getenv("REDIS_PORT");
    return p ? std::atoi(p) : 6379;
}

// Throughput pool sizing — per worker thread.
// Total Redis connections = num_threads × kPoolSize. Redis allows thousands
// of clients by default so we can be generous.
static constexpr int kPoolSize = 4;
// Coroutines per worker thread.
static constexpr int kLevels[] = {1, 4, 16, 64};


// #############################################################################
//
//  LATENCY: PING (baseline round-trip)
//
// #############################################################################

TEST_CASE("Benchmark - Redis PING", "[benchmark][redis][latency][ping]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<Io>::connect(io, redisHost(), redisPort());

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("PING", [&]() -> Task<void> {
            co_await client->exec("PING");
        }));

        co_return results;
    }(io));

    WARN(formatTable("Redis PING — latency", results));
}


// #############################################################################
//
//  LATENCY: SET/GET round-trip (small values)
//
// #############################################################################

TEST_CASE("Benchmark - Redis SET/GET", "[benchmark][redis][latency][set-get]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<Io>::connect(io, redisHost(), redisPort());

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("SET (small)", [&]() -> Task<void> {
            co_await client->exec("SET", "bench:io:key", "hello");
        }));

        co_await client->exec("SET", "bench:io:key", "hello");

        results.push_back(co_await benchAsync("GET (small)", [&]() -> Task<void> {
            co_await client->exec("GET", "bench:io:key");
        }));

        results.push_back(co_await benchAsync("SET+GET round-trip", [&]() -> Task<void> {
            co_await client->exec("SET", "bench:io:rt", "value");
            co_await client->exec("GET", "bench:io:rt");
        }));

        co_await client->exec("DEL", "bench:io:key", "bench:io:rt");

        co_return results;
    }(io));

    WARN(formatTable("Redis SET/GET — latency (small)", results));
}


// #############################################################################
//
//  LATENCY: Payload size impact
//
// #############################################################################

TEST_CASE("Benchmark - Redis payload sizes", "[benchmark][redis][latency][payload]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<Io>::connect(io, redisHost(), redisPort());

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

        co_await client->exec("DEL", "bench:io:p100", "bench:io:p1k", "bench:io:p10k");

        co_return results;
    }(io));

    WARN(formatTable("Redis payload sizes — latency", results));
}


// #############################################################################
//
//  LATENCY: EVAL (Lua script round-trip)
//
// #############################################################################

TEST_CASE("Benchmark - Redis EVAL", "[benchmark][redis][latency][eval]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<Io>::connect(io, redisHost(), redisPort());

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("EVAL return 1", [&]() -> Task<void> {
            co_await client->exec("EVAL", "return 1", "0");
        }));

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

        co_await client->exec("DEL", "bench:io:lua");

        co_return results;
    }(io));

    WARN(formatTable("Redis EVAL (Lua) — latency", results));
}


// #############################################################################
//
//  LATENCY: INCR (atomic counter, minimal payload)
//
// #############################################################################

TEST_CASE("Benchmark - Redis INCR", "[benchmark][redis][latency][incr]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto client = co_await RedisClient<Io>::connect(io, redisHost(), redisPort());

        co_await client->exec("SET", "bench:io:ctr", "0");

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("INCR", [&]() -> Task<void> {
            co_await client->exec("INCR", "bench:io:ctr");
        }));

        co_await client->exec("DEL", "bench:io:ctr");

        co_return results;
    }(io));

    WARN(formatTable("Redis INCR — latency", results));
}


// #############################################################################
// =============================================================================
// THROUGHPUT — multi-event-loop shared-nothing
//
// Each worker thread spins up its own EpollIoContext + RedisPool, then runs
// `concurrency_per_thread` DetachedTask workers. Workers call
// `pool->next().exec(...)` every iteration so commands fan out round-robin
// across the per-thread pool — calling `next()` once would pin the worker
// to a single RedisClient and let its coroutine mutex serialize everything.
//
// Note: Redis is mono-thread on the command-processing path (modulo
// `io-threads` in redis.conf), so multi-loop scaling will hit a server-side
// ceiling well before the client runs out of capacity.
// =============================================================================
// #############################################################################

static DetachedTask redisPingWorker(
        std::shared_ptr<RedisPool<Io>> pool,
        std::atomic<bool>& running,
        std::atomic<int64_t>& ops,
        std::atomic<int>& done_count)
{
    while (running.load(std::memory_order_relaxed)) {
        auto r = co_await pool->next().exec("PING");
        doNotOptimize(r);
        ops.fetch_add(1, std::memory_order_relaxed);
    }
    done_count.fetch_add(1, std::memory_order_relaxed);
}

static DetachedTask redisGetWorker(
        std::shared_ptr<RedisPool<Io>> pool,
        std::atomic<bool>& running,
        std::atomic<int64_t>& ops,
        std::atomic<int>& done_count)
{
    while (running.load(std::memory_order_relaxed)) {
        auto r = co_await pool->next().exec("GET", "bench:io:tp:key");
        doNotOptimize(r);
        ops.fetch_add(1, std::memory_order_relaxed);
    }
    done_count.fetch_add(1, std::memory_order_relaxed);
}

static DetachedTask redisSetWorker(
        std::shared_ptr<RedisPool<Io>> pool,
        int tid,
        std::atomic<bool>& running,
        std::atomic<int64_t>& ops,
        std::atomic<int>& done_count)
{
    int counter = 0;
    while (running.load(std::memory_order_relaxed)) {
        // tid in the key prevents cross-thread overwrites & makes cleanup deterministic.
        std::string key = "bench:io:tp:" + std::to_string(tid)
                          + ":" + std::to_string(counter & 1023);
        std::string val = std::to_string(counter);
        ++counter;
        auto r = co_await pool->next().exec("SET", key, val);
        doNotOptimize(r);
        ops.fetch_add(1, std::memory_order_relaxed);
    }
    done_count.fetch_add(1, std::memory_order_relaxed);
}

// Pre-populate the GET key (idempotent — runs once before [throughput][get]).
static void seedGetKey() {
    Io io;
    runTask(io, [](Io& io) -> Task<void> {
        auto pool = std::make_shared<RedisPool<Io>>(
            co_await RedisPool<Io>::create(io, redisHost(), redisPort(), 1));
        co_await pool->next().exec("SET", "bench:io:tp:key", "hello");
    }(io));
}

static void cleanupGetKey() {
    Io io;
    runTask(io, [](Io& io) -> Task<void> {
        auto pool = std::make_shared<RedisPool<Io>>(
            co_await RedisPool<Io>::create(io, redisHost(), redisPort(), 1));
        co_await pool->next().exec("DEL", "bench:io:tp:key");
    }(io));
}

static void cleanupSetKeys(int num_threads) {
    Io io;
    runTask(io, [num_threads](Io& io) -> Task<void> {
        auto pool = std::make_shared<RedisPool<Io>>(
            co_await RedisPool<Io>::create(io, redisHost(), redisPort(), 1));
        auto& client = pool->next();
        for (int t = 0; t < num_threads; ++t) {
            for (int i = 0; i < 1024; ++i) {
                co_await client.exec("DEL",
                    "bench:io:tp:" + std::to_string(t) + ":" + std::to_string(i));
            }
        }
    }(io));
}


// -----------------------------------------------------------------------------
//  THROUGHPUT: PING
// -----------------------------------------------------------------------------

TEST_CASE("Benchmark - Redis throughput PING", "[benchmark][redis][throughput][ping]")
{
    int num_threads = benchThreads();
    std::vector<ThroughputResult> results;
    double baseline = 0;

    auto thread_fn = [](int /*tid*/, int conc, std::latch& ready, std::latch& go,
                        std::atomic<bool>& running, std::atomic<int64_t>& ops)
    {
        Io io;
        auto pool = std::make_shared<RedisPool<Io>>(runTask(io,
            [](Io& io) -> Task<RedisPool<Io>> {
                co_return co_await RedisPool<Io>::create(
                    io, redisHost(), redisPort(), kPoolSize);
            }(io)));

        ready.count_down();
        go.wait();

        std::atomic<int> done_count{0};
        for (int i = 0; i < conc; ++i) {
            redisPingWorker(pool, running, ops, done_count);
        }

        io.runUntil([&] { return !running.load(std::memory_order_relaxed); });
        io.runUntil([&] {
            return done_count.load(std::memory_order_relaxed) >= conc;
        });
    };

    for (int conc : kLevels) {
        auto r = measureMultiLoopThroughput(num_threads, conc, baseline, thread_fn);
        if (results.empty()) baseline = r.throughput;
        r.speedup = (baseline > 0) ? r.throughput / baseline : 1.0;
        results.push_back(r);
    }

    WARN(formatThroughputTable(
        "Redis throughput — PING (per-thread pool=" + std::to_string(kPoolSize) + ")",
        num_threads, results));
}


// -----------------------------------------------------------------------------
//  THROUGHPUT: GET
// -----------------------------------------------------------------------------

TEST_CASE("Benchmark - Redis throughput GET", "[benchmark][redis][throughput][get]")
{
    int num_threads = benchThreads();
    seedGetKey();
    std::vector<ThroughputResult> results;
    double baseline = 0;

    auto thread_fn = [](int /*tid*/, int conc, std::latch& ready, std::latch& go,
                        std::atomic<bool>& running, std::atomic<int64_t>& ops)
    {
        Io io;
        auto pool = std::make_shared<RedisPool<Io>>(runTask(io,
            [](Io& io) -> Task<RedisPool<Io>> {
                co_return co_await RedisPool<Io>::create(
                    io, redisHost(), redisPort(), kPoolSize);
            }(io)));

        ready.count_down();
        go.wait();

        std::atomic<int> done_count{0};
        for (int i = 0; i < conc; ++i) {
            redisGetWorker(pool, running, ops, done_count);
        }

        io.runUntil([&] { return !running.load(std::memory_order_relaxed); });
        io.runUntil([&] {
            return done_count.load(std::memory_order_relaxed) >= conc;
        });
    };

    for (int conc : kLevels) {
        auto r = measureMultiLoopThroughput(num_threads, conc, baseline, thread_fn);
        if (results.empty()) baseline = r.throughput;
        r.speedup = (baseline > 0) ? r.throughput / baseline : 1.0;
        results.push_back(r);
    }

    cleanupGetKey();

    WARN(formatThroughputTable(
        "Redis throughput — GET (per-thread pool=" + std::to_string(kPoolSize) + ")",
        num_threads, results));
}


// -----------------------------------------------------------------------------
//  THROUGHPUT: SET (rotating keys, per-thread namespace)
// -----------------------------------------------------------------------------

TEST_CASE("Benchmark - Redis throughput SET", "[benchmark][redis][throughput][set]")
{
    int num_threads = benchThreads();
    std::vector<ThroughputResult> results;
    double baseline = 0;

    auto thread_fn = [](int tid, int conc, std::latch& ready, std::latch& go,
                        std::atomic<bool>& running, std::atomic<int64_t>& ops)
    {
        Io io;
        auto pool = std::make_shared<RedisPool<Io>>(runTask(io,
            [](Io& io) -> Task<RedisPool<Io>> {
                co_return co_await RedisPool<Io>::create(
                    io, redisHost(), redisPort(), kPoolSize);
            }(io)));

        ready.count_down();
        go.wait();

        std::atomic<int> done_count{0};
        for (int i = 0; i < conc; ++i) {
            redisSetWorker(pool, tid, running, ops, done_count);
        }

        io.runUntil([&] { return !running.load(std::memory_order_relaxed); });
        io.runUntil([&] {
            return done_count.load(std::memory_order_relaxed) >= conc;
        });
    };

    for (int conc : kLevels) {
        auto r = measureMultiLoopThroughput(num_threads, conc, baseline, thread_fn);
        if (results.empty()) baseline = r.throughput;
        r.speedup = (baseline > 0) ? r.throughput / baseline : 1.0;
        results.push_back(r);
    }

    cleanupSetKeys(num_threads);

    WARN(formatThroughputTable(
        "Redis throughput — SET (per-thread pool=" + std::to_string(kPoolSize) + ")",
        num_threads, results));
}