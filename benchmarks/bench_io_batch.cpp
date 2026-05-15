/**
 * bench_io_batch.cpp
 *
 * Stress-test benchmarks for the BatchScheduler Nagle batching strategy.
 * Measures throughput scaling as concurrency increases — super-linear speedup
 * demonstrates that multiple queries are flushed in a single pipeline batch.
 *
 * Run with:
 *   ./bench_io_batch                       # all benchmarks
 *   ./bench_io_batch "[batch][pg-read]"    # PG read only
 *   ./bench_io_batch "[batch][pg-write]"   # PG write only
 *   ./bench_io_batch "[batch][redis]"      # Redis only
 *   ./bench_io_batch "[batch][scaling]"    # summary scaling table
 *   BENCH_DURATION_S=10 ./bench_io_batch   # custom duration
 */

#include <catch2/catch_test_macros.hpp>

#include "BenchEngine.h"

#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/io/redis/RedisPool.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/Task.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

#include <atomic>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::batch;
using namespace jcailloux::relais::io::test;
using namespace relais_bench;

using Io = EpollIoContext;

// =============================================================================
// BatchBenchAccessor — reads private members via friend access
// =============================================================================

namespace relais_bench {

struct BatchBenchAccessor {
    struct Stats {
        double pg_rtt_us;
        double redis_rtt_us;
        bool pg_bootstrapping;
        bool redis_bootstrapping;
    };

    static Stats snapshot(const BatchScheduler<Io>& b) {
        return {
            b.estimator_.pg_network_time_ns / 1000.0,
            b.estimator_.redis_network_time_ns / 1000.0,
            b.estimator_.isPgBootstrapping(),
            b.estimator_.isRedisBootstrapping(),
        };
    }
};

} // namespace relais_bench

// =============================================================================
// Configuration
// =============================================================================

static constexpr const char* CONNINFO =
    "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test";

static const char* redisHost() {
    const char* h = std::getenv("REDIS_HOST");
    return h ? h : "127.0.0.1";
}

static int redisPort() {
    const char* p = std::getenv("REDIS_PORT");
    return p ? std::atoi(p) : 6379;
}

// =============================================================================
// Worker coroutines (free functions — safe for fire-and-forget)
// =============================================================================

static constexpr const char* PG_READ_SQL = "SELECT $1::int AS val";
static constexpr const char* PG_WRITE_SQL =
    "INSERT INTO batch_bench_write (id, val) VALUES ($1, $2) "
    "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val RETURNING id";

DetachedTask pgReadWorker(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<bool>& running,
    std::atomic<int64_t>& ops,
    std::atomic<int>& done_count)
{
    int counter = 0;
    while (running.load(std::memory_order_relaxed)) {
        auto params = PgParams::make(++counter);
        co_await batcher->submitQueryRead(PG_READ_SQL, std::move(params));
        ops.fetch_add(1, std::memory_order_relaxed);
    }
    done_count.fetch_add(1, std::memory_order_relaxed);
}

DetachedTask pgWriteWorker(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<bool>& running,
    std::atomic<int64_t>& ops,
    std::atomic<int>& done_count)
{
    int counter = 0;
    while (running.load(std::memory_order_relaxed)) {
        ++counter;
        auto params = PgParams::make(counter % 1000, counter);
        auto [result, coalesced] = co_await batcher->submitPgWrite(PG_WRITE_SQL, std::move(params));
        (void)result;
        (void)coalesced;
        ops.fetch_add(1, std::memory_order_relaxed);
    }
    done_count.fetch_add(1, std::memory_order_relaxed);
}

DetachedTask redisWorker(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<bool>& running,
    std::atomic<int64_t>& ops,
    std::atomic<int>& done_count)
{
    int counter = 0;
    while (running.load(std::memory_order_relaxed)) {
        ++counter;
        std::string key = "bench:batch:" + std::to_string(counter % 1000);
        std::string val = std::to_string(counter);
        std::vector<std::string> args = {"SET", key, val};
        std::vector<const char*> argv;
        std::vector<size_t> argvlen;
        for (const auto& a : args) {
            argv.push_back(a.data());
            argvlen.push_back(a.size());
        }
        co_await batcher->submitRedis(
            static_cast<int>(argv.size()), argv.data(), argvlen.data());
        ops.fetch_add(1, std::memory_order_relaxed);
    }
    done_count.fetch_add(1, std::memory_order_relaxed);
}

// =============================================================================
// Helpers
// =============================================================================

struct ConcurrencyResult {
    int concurrency;
    double throughput;  // ops/s
    double speedup;     // vs baseline
};

/// Bootstrap the estimator with sequential queries so Nagle batching activates.
static Task<void> bootstrapPg(BatchScheduler<Io>& batcher) {
    for (int i = 0; i < TimingEstimator::kBootstrapThreshold + 2; ++i) {
        auto params = PgParams::make(i);
        co_await batcher.submitQueryRead(PG_READ_SQL, std::move(params));
    }
}

static Task<void> bootstrapRedis(BatchScheduler<Io>& batcher) {
    for (int i = 0; i < TimingEstimator::kBootstrapThreshold + 2; ++i) {
        std::string key = "bench:boot:" + std::to_string(i);
        std::vector<std::string> args = {"SET", key, "x"};
        std::vector<const char*> argv;
        std::vector<size_t> argvlen;
        for (const auto& a : args) {
            argv.push_back(a.data());
            argvlen.push_back(a.size());
        }
        co_await batcher.submitRedis(
            static_cast<int>(argv.size()), argv.data(), argvlen.data());
    }
}

/// Run a concurrency level and return throughput.
static Task<ConcurrencyResult> runPgReadLevel(
    Io& io, std::shared_ptr<PgPool<Io>> pool,
    int concurrency, double baseline_throughput)
{
    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);
    co_await bootstrapPg(*batcher);

    std::atomic<bool> running{true};
    std::atomic<int64_t> ops{0};
    std::atomic<int> done_count{0};

    auto start = Clock::now();

    for (int i = 0; i < concurrency; ++i) {
        pgReadWorker(batcher, running, ops, done_count);
    }

    // Run for the configured duration
    int duration_s = benchDurationSeconds();
    auto deadline = start + std::chrono::seconds(duration_s);

    io.runUntil([&] {
        return Clock::now() >= deadline;
    });

    running.store(false, std::memory_order_relaxed);

    // Wait for all workers to finish their in-flight operation
    io.runUntil([&] {
        return done_count.load(std::memory_order_relaxed) >= concurrency;
    });

    auto elapsed = Clock::now() - start;
    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    double throughput = (elapsed_us > 0)
        ? ops.load(std::memory_order_relaxed) * 1'000'000.0 / elapsed_us
        : 0.0;
    double speedup = (baseline_throughput > 0)
        ? throughput / baseline_throughput
        : 1.0;

    co_return ConcurrencyResult{concurrency, throughput, speedup};
}

static Task<ConcurrencyResult> runPgWriteLevel(
    Io& io, std::shared_ptr<PgPool<Io>> pool,
    int concurrency, double baseline_throughput)
{
    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

    // Create temp table for writes
    co_await batcher->directQuery(
        "CREATE TEMP TABLE IF NOT EXISTS batch_bench_write "
        "(id INT PRIMARY KEY, val INT)");

    co_await bootstrapPg(*batcher);

    std::atomic<bool> running{true};
    std::atomic<int64_t> ops{0};
    std::atomic<int> done_count{0};

    auto start = Clock::now();

    for (int i = 0; i < concurrency; ++i) {
        pgWriteWorker(batcher, running, ops, done_count);
    }

    int duration_s = benchDurationSeconds();
    auto deadline = start + std::chrono::seconds(duration_s);

    io.runUntil([&] {
        return Clock::now() >= deadline;
    });

    running.store(false, std::memory_order_relaxed);

    io.runUntil([&] {
        return done_count.load(std::memory_order_relaxed) >= concurrency;
    });

    auto elapsed = Clock::now() - start;
    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    double throughput = (elapsed_us > 0)
        ? ops.load(std::memory_order_relaxed) * 1'000'000.0 / elapsed_us
        : 0.0;
    double speedup = (baseline_throughput > 0)
        ? throughput / baseline_throughput
        : 1.0;

    co_return ConcurrencyResult{concurrency, throughput, speedup};
}

static Task<ConcurrencyResult> runRedisLevel(
    Io& io, std::shared_ptr<PgPool<Io>> pg_pool,
    std::shared_ptr<RedisPool<Io>> redis_pool,
    int concurrency, double baseline_throughput)
{
    auto batcher = std::make_shared<BatchScheduler<Io>>(
        io, pg_pool, redis_pool, 8);
    co_await bootstrapRedis(*batcher);

    std::atomic<bool> running{true};
    std::atomic<int64_t> ops{0};
    std::atomic<int> done_count{0};

    auto start = Clock::now();

    for (int i = 0; i < concurrency; ++i) {
        redisWorker(batcher, running, ops, done_count);
    }

    int duration_s = benchDurationSeconds();
    auto deadline = start + std::chrono::seconds(duration_s);

    io.runUntil([&] {
        return Clock::now() >= deadline;
    });

    running.store(false, std::memory_order_relaxed);

    io.runUntil([&] {
        return done_count.load(std::memory_order_relaxed) >= concurrency;
    });

    auto elapsed = Clock::now() - start;
    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    double throughput = (elapsed_us > 0)
        ? ops.load(std::memory_order_relaxed) * 1'000'000.0 / elapsed_us
        : 0.0;
    double speedup = (baseline_throughput > 0)
        ? throughput / baseline_throughput
        : 1.0;

    co_return ConcurrencyResult{concurrency, throughput, speedup};
}

// =============================================================================
// Formatting
// =============================================================================

static std::string formatScalingTable(
    const std::string& title,
    double rtt_us,
    const std::vector<ConcurrencyResult>& results)
{
    int duration_s = benchDurationSeconds();
    std::ostringstream out;

    auto bar = std::string(52, '=');
    out << "\n  " << bar << "\n"
        << "  " << title << "\n"
        << "  " << bar << "\n"
        << "  duration:       " << duration_s << ".0s\n"
        << "  estimator RTT:  " << std::fixed << std::setprecision(1) << rtt_us << " us\n"
        << "\n"
        << "  " << std::left << std::setw(14) << "concurrency"
        << std::setw(16) << "throughput"
        << "speedup\n"
        << "  " << std::string(14, '-') << std::string(16, '-') << std::string(10, '-') << "\n";

    for (const auto& r : results) {
        std::string conc_str = std::to_string(r.concurrency) +
            (r.concurrency == 1 ? " coro" : " coros");
        out << "  " << std::right << std::setw(10) << conc_str << "    "
            << std::left << std::setw(16) << fmtOps(r.throughput)
            << std::fixed << std::setprecision(1) << r.speedup << "x\n";
    }

    out << "  " << bar;
    return out.str();
}

// #############################################################################
//
//  1. PG Read Batching — scaling with concurrency
//
// #############################################################################

TEST_CASE("Benchmark - PG Read Batching", "[benchmark][batch][pg-read]")
{
    Io io;
    static constexpr int levels[] = {1, 4, 16, 64, 128};

    auto results = runTask(io, [](Io& io) -> Task<std::vector<ConcurrencyResult>> {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, 2, 8);

        std::vector<ConcurrencyResult> results;
        double baseline = 0;

        for (int n : levels) {
            auto r = co_await runPgReadLevel(io, pool, n, baseline);
            if (n == 1) baseline = r.throughput;
            r.speedup = (baseline > 0) ? r.throughput / baseline : 1.0;
            results.push_back(r);
        }

        co_return results;
    }(io));

    // Get RTT from a fresh batcher snapshot
    double rtt_us = 0;
    {
        Io io2;
        rtt_us = runTask(io2, [](Io& io2) -> Task<double> {
            auto pool = co_await PgPool<Io>::create(io2, CONNINFO, 1, 4);
            auto batcher = std::make_shared<BatchScheduler<Io>>(io2, pool, nullptr, 8);
            co_await bootstrapPg(*batcher);
            auto stats = BatchBenchAccessor::snapshot(*batcher);
            co_return stats.pg_rtt_us;
        }(io2));
    }

    WARN(formatScalingTable("PG Read Batching", rtt_us, results));
}


// #############################################################################
//
//  2. PG Write Batching — scaling with concurrency
//
// #############################################################################

TEST_CASE("Benchmark - PG Write Batching", "[benchmark][batch][pg-write]")
{
    Io io;
    static constexpr int levels[] = {1, 4, 16, 64};

    auto results = runTask(io, [](Io& io) -> Task<std::vector<ConcurrencyResult>> {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, 2, 8);

        std::vector<ConcurrencyResult> results;
        double baseline = 0;

        for (int n : levels) {
            auto r = co_await runPgWriteLevel(io, pool, n, baseline);
            if (n == 1) baseline = r.throughput;
            r.speedup = (baseline > 0) ? r.throughput / baseline : 1.0;
            results.push_back(r);
        }

        co_return results;
    }(io));

    double rtt_us = 0;
    {
        Io io2;
        rtt_us = runTask(io2, [](Io& io2) -> Task<double> {
            auto pool = co_await PgPool<Io>::create(io2, CONNINFO, 1, 4);
            auto batcher = std::make_shared<BatchScheduler<Io>>(io2, pool, nullptr, 8);
            co_await bootstrapPg(*batcher);
            auto stats = BatchBenchAccessor::snapshot(*batcher);
            co_return stats.pg_rtt_us;
        }(io2));
    }

    WARN(formatScalingTable("PG Write Batching", rtt_us, results));
}


// #############################################################################
//
//  3. Redis Batching — scaling with concurrency
//
// #############################################################################

TEST_CASE("Benchmark - Redis Batching", "[benchmark][batch][redis]")
{
    Io io;
    static constexpr int levels[] = {1, 4, 16, 64, 128};

    auto results = runTask(io, [](Io& io) -> Task<std::vector<ConcurrencyResult>> {
        auto pg_pool = co_await PgPool<Io>::create(io, CONNINFO, 1, 4);
        auto redis_pool = std::make_shared<RedisPool<Io>>(
            co_await RedisPool<Io>::create(io, redisHost(), redisPort(), 4));

        std::vector<ConcurrencyResult> results;
        double baseline = 0;

        for (int n : levels) {
            auto r = co_await runRedisLevel(io, pg_pool, redis_pool, n, baseline);
            if (n == 1) baseline = r.throughput;
            r.speedup = (baseline > 0) ? r.throughput / baseline : 1.0;
            results.push_back(r);
        }

        // Cleanup bench keys
        auto& client = redis_pool->next();
        for (int i = 0; i < 1000; ++i) {
            std::string key = "bench:batch:" + std::to_string(i);
            co_await client.exec("DEL", key);
        }
        for (int i = 0; i < TimingEstimator::kBootstrapThreshold + 2; ++i) {
            std::string key = "bench:boot:" + std::to_string(i);
            co_await client.exec("DEL", key);
        }

        co_return results;
    }(io));

    double rtt_us = 0;
    {
        Io io2;
        rtt_us = runTask(io2, [](Io& io2) -> Task<double> {
            auto pg_pool = co_await PgPool<Io>::create(io2, CONNINFO, 1, 4);
            auto redis_pool = std::make_shared<RedisPool<Io>>(
                co_await RedisPool<Io>::create(io2, redisHost(), redisPort(), 1));
            auto batcher = std::make_shared<BatchScheduler<Io>>(
                io2, pg_pool, redis_pool, 8);
            co_await bootstrapRedis(*batcher);
            auto stats = BatchBenchAccessor::snapshot(*batcher);
            co_return stats.redis_rtt_us;
        }(io2));
    }

    WARN(formatScalingTable("Redis Batching", rtt_us, results));
}


// #############################################################################
//
//  4. Combined Scaling Summary
//
// #############################################################################

TEST_CASE("Benchmark - Batch Scaling Summary", "[benchmark][batch][scaling]")
{
    Io io;

    struct AllResults {
        std::vector<ConcurrencyResult> pg_read;
        std::vector<ConcurrencyResult> pg_write;
        std::vector<ConcurrencyResult> redis;
        double pg_rtt_us = 0;
        double redis_rtt_us = 0;
    };

    auto all = runTask(io, [](Io& io) -> Task<AllResults> {
        AllResults out;

        auto pg_pool = co_await PgPool<Io>::create(io, CONNINFO, 2, 8);
        auto redis_pool = std::make_shared<RedisPool<Io>>(
            co_await RedisPool<Io>::create(io, redisHost(), redisPort(), 4));

        // PG Read: 1, 16, 64
        {
            double baseline = 0;
            for (int n : {1, 16, 64}) {
                auto r = co_await runPgReadLevel(io, pg_pool, n, baseline);
                if (n == 1) baseline = r.throughput;
                r.speedup = (baseline > 0) ? r.throughput / baseline : 1.0;
                out.pg_read.push_back(r);
            }
        }

        // PG Write: 1, 16, 64
        {
            double baseline = 0;
            for (int n : {1, 16, 64}) {
                auto r = co_await runPgWriteLevel(io, pg_pool, n, baseline);
                if (n == 1) baseline = r.throughput;
                r.speedup = (baseline > 0) ? r.throughput / baseline : 1.0;
                out.pg_write.push_back(r);
            }
        }

        // Redis: 1, 16, 64
        {
            double baseline = 0;
            for (int n : {1, 16, 64}) {
                auto r = co_await runRedisLevel(io, pg_pool, redis_pool, n, baseline);
                if (n == 1) baseline = r.throughput;
                r.speedup = (baseline > 0) ? r.throughput / baseline : 1.0;
                out.redis.push_back(r);
            }
        }

        // Get RTT estimates
        {
            auto batcher = std::make_shared<BatchScheduler<Io>>(
                io, pg_pool, redis_pool, 8);
            co_await bootstrapPg(*batcher);
            co_await bootstrapRedis(*batcher);
            auto stats = BatchBenchAccessor::snapshot(*batcher);
            out.pg_rtt_us = stats.pg_rtt_us;
            out.redis_rtt_us = stats.redis_rtt_us;
        }

        // Cleanup
        auto& client = redis_pool->next();
        for (int i = 0; i < 1000; ++i) {
            co_await client.exec("DEL", "bench:batch:" + std::to_string(i));
        }
        for (int i = 0; i < TimingEstimator::kBootstrapThreshold + 2; ++i) {
            co_await client.exec("DEL", "bench:boot:" + std::to_string(i));
        }

        co_return out;
    }(io));

    std::ostringstream out;
    out << "\n"
        << formatScalingTable("PG Read Batching", all.pg_rtt_us, all.pg_read) << "\n\n"
        << formatScalingTable("PG Write Batching", all.pg_rtt_us, all.pg_write) << "\n\n"
        << formatScalingTable("Redis Batching", all.redis_rtt_us, all.redis);

    WARN(out.str());
}
