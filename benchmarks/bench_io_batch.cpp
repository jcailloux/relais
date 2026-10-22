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
 *   ./bench_io_batch "[batch][scaling]"    # summary scaling table
 *   BENCH_DURATION_S=10 ./bench_io_batch   # custom duration
 */

#include <catch2/catch_test_macros.hpp>

#include "BenchEngine.h"

#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/Task.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

#include <atomic>
#include <stdexcept>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::batch;
using namespace jcailloux::relais::io::batch::detail;
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
        bool pg_bootstrapping;
    };

    static Stats snapshot(const BatchScheduler<Io>& b) {
        return {
            b.estimator_.pg_network_time_ns / 1000.0,
            b.estimator_.isPgBootstrapping(),
        };
    }
};

} // namespace relais_bench

// =============================================================================
// Configuration
// =============================================================================

static constexpr const char* CONNINFO =
    "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test";

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
    // A worker always counts its end: a write that throws would otherwise
    // leave the level waiting for it forever.
    struct Done {
        std::atomic<int>& count;
        ~Done() { count.fetch_add(1, std::memory_order_relaxed); }
    } done{done_count};
    int counter = 0;
    while (running.load(std::memory_order_relaxed)) {
        ++counter;
        auto params = PgParams::make(counter % 1000, counter);
        auto [result, coalesced] = co_await batcher->submitPgWrite(PG_WRITE_SQL, std::move(params));
        if (!result.ok()) throw std::runtime_error("write failed");
        (void)coalesced;
        ops.fetch_add(1, std::memory_order_relaxed);
    }
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

/// Suspends the calling coroutine for a millisecond. A level waits with it
/// rather than running the loop itself: a loop nested in a resumed coroutine
/// runs inside the handler that resumed it, and can wait forever for input
/// that only the outer loop delivers.
struct Pause {
    Io& io;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const {
        io.postDelayed(std::chrono::milliseconds(1), [h] { h.resume(); });
    }
    void await_resume() const noexcept {}
};

/// Run a concurrency level and return throughput.
static Task<ConcurrencyResult> runPgReadLevel(
    Io& io, std::shared_ptr<PgPool<Io>> pool,
    int concurrency, double baseline_throughput)
{
    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
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

    while (Clock::now() < deadline) co_await Pause{io};

    running.store(false, std::memory_order_relaxed);

    // Wait for all workers to finish their in-flight operation
    while (done_count.load(std::memory_order_relaxed) < concurrency) co_await Pause{io};

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
    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

    // A real table: a TEMP one exists only on the connection that created
    // it, and writes run on every connection of the pool.
    co_await batcher->directQuery(
        "CREATE TABLE IF NOT EXISTS batch_bench_write "
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

    while (Clock::now() < deadline) co_await Pause{io};

    running.store(false, std::memory_order_relaxed);

    while (done_count.load(std::memory_order_relaxed) < concurrency) co_await Pause{io};

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
        csv(title + " | " + conc_str, "ops_s", r.throughput);
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
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 2, .max_connections = 8});

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
            auto pool = co_await PgPool<Io>::create(io2, CONNINFO, {.min_connections = 1, .max_connections = 4});
            auto batcher = std::make_shared<BatchScheduler<Io>>(io2, pool, 8);
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
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 2, .max_connections = 8});

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
            auto pool = co_await PgPool<Io>::create(io2, CONNINFO, {.min_connections = 1, .max_connections = 4});
            auto batcher = std::make_shared<BatchScheduler<Io>>(io2, pool, 8);
            co_await bootstrapPg(*batcher);
            auto stats = BatchBenchAccessor::snapshot(*batcher);
            co_return stats.pg_rtt_us;
        }(io2));
    }

    WARN(formatScalingTable("PG Write Batching", rtt_us, results));
}


// #############################################################################
//
//  3. Combined Scaling Summary
//
// #############################################################################

TEST_CASE("Benchmark - Batch Scaling Summary", "[benchmark][batch][scaling]")
{
    Io io;

    struct AllResults {
        std::vector<ConcurrencyResult> pg_read;
        std::vector<ConcurrencyResult> pg_write;
        double pg_rtt_us = 0;
    };

    auto all = runTask(io, [](Io& io) -> Task<AllResults> {
        AllResults out;

        auto pg_pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 2, .max_connections = 8});

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

        // Get RTT estimates
        {
            auto batcher = std::make_shared<BatchScheduler<Io>>(io, pg_pool, 8);
            co_await bootstrapPg(*batcher);
            out.pg_rtt_us = BatchBenchAccessor::snapshot(*batcher).pg_rtt_us;
        }

        co_return out;
    }(io));

    std::ostringstream out;
    out << "\n"
        << formatScalingTable("PG Read Batching", all.pg_rtt_us, all.pg_read) << "\n\n"
        << formatScalingTable("PG Write Batching", all.pg_rtt_us, all.pg_write);

    WARN(out.str());
}
