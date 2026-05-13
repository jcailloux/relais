/**
 * bench_io_pg.cpp
 *
 * Performance benchmarks for the PostgreSQL I/O layer.
 *
 * Two flavors:
 *   - [latency]    : single coroutine, sequential round-trips (p50/p99 RTT).
 *   - [throughput] : N concurrent DetachedTask workers on the same epoll loop.
 *                    Exposes real pool/pipeline utilization. Without these,
 *                    the latency benchmarks would mislead readers into
 *                    treating 1/RTT as the system's max throughput.
 *
 * Run with:
 *   ./bench_io_pg                                # all benchmarks
 *   ./bench_io_pg "[latency]"                    # latency only
 *   ./bench_io_pg "[throughput]"                 # throughput only
 *   ./bench_io_pg "[throughput][select]"         # throughput SELECT only
 *   BENCH_SAMPLES=1000 ./bench_io_pg "[latency]"
 *   BENCH_DURATION_S=10 ./bench_io_pg "[throughput]"
 */

#include <catch2/catch_test_macros.hpp>

#include "BenchEngine.h"

#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgConnection.h>
#include <jcailloux/relais/io/pg/PgResult.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/Task.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

#include <atomic>
#include <memory>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::test;
using namespace relais_bench;

using Io = EpollIoContext;

static const char* getConnInfo() {
    return "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test";
}

// Throughput pool sizing — per worker thread.
// Total PG backends = num_threads × kPoolMax. PostgreSQL default
// max_connections = 100, so keep `num_threads × kPoolMax ≤ ~80` for safety
// (override pool via env if PG is tuned higher).
static constexpr int kPoolMin = 2;
static constexpr int kPoolMax = 8;
// Coroutines per worker thread. Beyond kPoolMax we observe pool-queue waits.
static constexpr int kLevels[] = {1, 4, 16, 64};


// #############################################################################
//
//  LATENCY: Raw connection SELECT (no pool)
//
// #############################################################################

TEST_CASE("Benchmark - PG raw SELECT", "[benchmark][pg][latency][select]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto conn = co_await PgConnection<Io>::connect(io, getConnInfo());

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("SELECT 1", [&]() -> Task<void> {
            co_await conn.query("SELECT 1");
        }));

        results.push_back(co_await benchAsync("SELECT 1 (parameterized)", [&]() -> Task<void> {
            auto params = PgParams::make(1);
            co_await conn.queryParams("SELECT $1::int", params);
        }));

        results.push_back(co_await benchAsync("SELECT now()", [&]() -> Task<void> {
            co_await conn.query("SELECT now()");
        }));

        co_return results;
    }(io));

    WARN(formatTable("PG raw SELECT — latency (single connection)", results));
}


// #############################################################################
//
//  LATENCY: Pool acquire + query + release
//
// #############################################################################

TEST_CASE("Benchmark - PG pool query", "[benchmark][pg][latency][pool]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto pool = co_await PgPool<Io>::create(io, getConnInfo(), 2, 4);
        PgClient<Io> client(pool);

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("pool query SELECT 1", [&]() -> Task<void> {
            co_await client.query("SELECT 1");
        }));

        results.push_back(co_await benchAsync("pool queryArgs (1 param)", [&]() -> Task<void> {
            co_await client.queryArgs("SELECT $1::int", 42);
        }));

        results.push_back(co_await benchAsync("pool queryArgs (3 params)", [&]() -> Task<void> {
            co_await client.queryArgs(
                "SELECT $1::int, $2::text, $3::bool", 42, "hello", true);
        }));

        co_return results;
    }(io));

    WARN(formatTable("PG pool query — latency (acquire+query+release)", results));
}


// #############################################################################
//
//  LATENCY: Real table queries
//
// #############################################################################

TEST_CASE("Benchmark - PG table queries", "[benchmark][pg][latency][table]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto pool = co_await PgPool<Io>::create(io, getConnInfo(), 2, 4);
        PgClient<Io> client(pool);

        for (int i = 0; i < 20; ++i) {
            co_await client.queryArgs(
                "INSERT INTO relais_test_items (name, value, is_active) "
                "VALUES ($1, $2, true)",
                "bench_pg_" + std::to_string(i), i * 10);
        }

        auto idResult = co_await client.query(
            "SELECT id FROM relais_test_items WHERE name = 'bench_pg_0'");
        auto id = idResult[0].get<int64_t>(0);

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("SELECT by PK", [&]() -> Task<void> {
            co_await client.queryArgs(
                "SELECT id, name, value, is_active FROM relais_test_items WHERE id = $1", id);
        }));

        results.push_back(co_await benchAsync("SELECT LIMIT 10", [&]() -> Task<void> {
            co_await client.query(
                "SELECT id, name, value, is_active FROM relais_test_items "
                "ORDER BY id LIMIT 10");
        }));

        results.push_back(co_await benchAsync("SELECT COUNT(*)", [&]() -> Task<void> {
            co_await client.query("SELECT COUNT(*) FROM relais_test_items");
        }));

        co_await client.query(
            "DELETE FROM relais_test_items WHERE name LIKE 'bench_pg_%'");

        co_return results;
    }(io));

    WARN(formatTable("PG table queries — latency", results));
}


// #############################################################################
//
//  LATENCY: INSERT + DELETE round-trip
//
// #############################################################################

TEST_CASE("Benchmark - PG write operations", "[benchmark][pg][latency][write]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto pool = co_await PgPool<Io>::create(io, getConnInfo(), 2, 4);
        PgClient<Io> client(pool);

        int counter = 0;

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("INSERT RETURNING", [&]() -> Task<void> {
            ++counter;
            auto r = co_await client.queryArgs(
                "INSERT INTO relais_test_items (name, value, is_active) "
                "VALUES ($1, $2, true) RETURNING id",
                "bench_ins_" + std::to_string(counter), counter);
            doNotOptimize(r);
        }));

        results.push_back(co_await benchAsync("INSERT+DELETE", [&]() -> Task<void> {
            ++counter;
            auto r = co_await client.queryArgs(
                "INSERT INTO relais_test_items (name, value, is_active) "
                "VALUES ($1, $2, true) RETURNING id",
                "bench_del_" + std::to_string(counter), counter);
            auto id = r[0].get<int64_t>(0);
            co_await client.queryArgs(
                "DELETE FROM relais_test_items WHERE id = $1", id);
        }));

        co_await client.query(
            "DELETE FROM relais_test_items WHERE name LIKE 'bench_ins_%'");

        co_return results;
    }(io));

    WARN(formatTable("PG write operations — latency", results));
}


// #############################################################################
// =============================================================================
// THROUGHPUT — multi-event-loop shared-nothing
//
// Each worker thread spins up its own EpollIoContext + PgPool, then runs
// `concurrency_per_thread` DetachedTask workers. Bootstrap (connections)
// happens before the measurement window opens. PG forks one backend per
// connection, so num_threads × pool_max parallel PG sessions are possible.
// =============================================================================
// #############################################################################

// Workers must be free functions so the coroutine frame captures by value.

static DetachedTask pgSelect1Worker(
        std::shared_ptr<PgPool<Io>> pool,
        std::atomic<bool>& running,
        std::atomic<int64_t>& ops,
        std::atomic<int>& done_count)
{
    PgClient<Io> client(pool);
    while (running.load(std::memory_order_relaxed)) {
        auto r = co_await client.query("SELECT 1");
        doNotOptimize(r);
        ops.fetch_add(1, std::memory_order_relaxed);
    }
    done_count.fetch_add(1, std::memory_order_relaxed);
}

static DetachedTask pgSelectParamWorker(
        std::shared_ptr<PgPool<Io>> pool,
        std::atomic<bool>& running,
        std::atomic<int64_t>& ops,
        std::atomic<int>& done_count)
{
    PgClient<Io> client(pool);
    int counter = 0;
    while (running.load(std::memory_order_relaxed)) {
        auto r = co_await client.queryArgs("SELECT $1::int", ++counter);
        doNotOptimize(r);
        ops.fetch_add(1, std::memory_order_relaxed);
    }
    done_count.fetch_add(1, std::memory_order_relaxed);
}

static DetachedTask pgSelectPkWorker(
        std::shared_ptr<PgPool<Io>> pool,
        int64_t id,
        std::atomic<bool>& running,
        std::atomic<int64_t>& ops,
        std::atomic<int>& done_count)
{
    PgClient<Io> client(pool);
    while (running.load(std::memory_order_relaxed)) {
        auto r = co_await client.queryArgs(
            "SELECT id, name, value, is_active "
            "FROM relais_test_items WHERE id = $1", id);
        doNotOptimize(r);
        ops.fetch_add(1, std::memory_order_relaxed);
    }
    done_count.fetch_add(1, std::memory_order_relaxed);
}

// One row seeded globally before any [throughput][table] test runs.
// We need a stable PK across threads. Created once in a setup helper.
static int64_t seedThroughputRow() {
    Io io;
    return runTask(io,
        [](Io& io) -> Task<int64_t> {
            auto pool = co_await PgPool<Io>::create(io, getConnInfo(), 1, 1);
            PgClient<Io> client(pool);
            co_await client.query(
                "DELETE FROM relais_test_items WHERE name = 'bench_pg_tp'");
            co_await client.queryArgs(
                "INSERT INTO relais_test_items (name, value, is_active) "
                "VALUES ($1, $2, true)",
                "bench_pg_tp", 0);
            auto r = co_await client.query(
                "SELECT id FROM relais_test_items WHERE name = 'bench_pg_tp'");
            co_return r[0].get<int64_t>(0);
        }(io));
}

static void cleanupThroughputRow() {
    Io io;
    runTask(io, [](Io& io) -> Task<void> {
        auto pool = co_await PgPool<Io>::create(io, getConnInfo(), 1, 1);
        PgClient<Io> client(pool);
        co_await client.query(
            "DELETE FROM relais_test_items WHERE name = 'bench_pg_tp'");
    }(io));
}


// -----------------------------------------------------------------------------
//  THROUGHPUT: SELECT 1 (no params)
// -----------------------------------------------------------------------------

TEST_CASE("Benchmark - PG throughput SELECT 1", "[benchmark][pg][throughput][select]")
{
    int num_threads = benchThreads();
    std::vector<ThroughputResult> results;
    double baseline = 0;

    auto thread_fn = [](int /*tid*/, int conc, std::latch& ready, std::latch& go,
                        std::atomic<bool>& running, std::atomic<int64_t>& ops)
    {
        Io io;
        auto pool = runTask(io,
            [](Io& io) -> Task<std::shared_ptr<PgPool<Io>>> {
                co_return co_await PgPool<Io>::create(io, getConnInfo(),
                                                      kPoolMin, kPoolMax);
            }(io));

        ready.count_down();
        go.wait();

        std::atomic<int> done_count{0};
        for (int i = 0; i < conc; ++i) {
            pgSelect1Worker(pool, running, ops, done_count);
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

    WARN(formatThroughputTable("PG throughput — SELECT 1 (raw, no params)",
                                num_threads, results));
}


// -----------------------------------------------------------------------------
//  THROUGHPUT: SELECT $1::int (parameterized, no table)
// -----------------------------------------------------------------------------

TEST_CASE("Benchmark - PG throughput SELECT param", "[benchmark][pg][throughput][param]")
{
    int num_threads = benchThreads();
    std::vector<ThroughputResult> results;
    double baseline = 0;

    auto thread_fn = [](int /*tid*/, int conc, std::latch& ready, std::latch& go,
                        std::atomic<bool>& running, std::atomic<int64_t>& ops)
    {
        Io io;
        auto pool = runTask(io,
            [](Io& io) -> Task<std::shared_ptr<PgPool<Io>>> {
                co_return co_await PgPool<Io>::create(io, getConnInfo(),
                                                      kPoolMin, kPoolMax);
            }(io));

        ready.count_down();
        go.wait();

        std::atomic<int> done_count{0};
        for (int i = 0; i < conc; ++i) {
            pgSelectParamWorker(pool, running, ops, done_count);
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

    WARN(formatThroughputTable("PG throughput — SELECT $1::int (parameterized)",
                                num_threads, results));
}


// -----------------------------------------------------------------------------
//  THROUGHPUT: SELECT by PK on real table
// -----------------------------------------------------------------------------

TEST_CASE("Benchmark - PG throughput SELECT by PK", "[benchmark][pg][throughput][table]")
{
    int num_threads = benchThreads();
    int64_t id = seedThroughputRow();
    std::vector<ThroughputResult> results;
    double baseline = 0;

    auto thread_fn = [id](int /*tid*/, int conc, std::latch& ready, std::latch& go,
                          std::atomic<bool>& running, std::atomic<int64_t>& ops)
    {
        Io io;
        auto pool = runTask(io,
            [](Io& io) -> Task<std::shared_ptr<PgPool<Io>>> {
                co_return co_await PgPool<Io>::create(io, getConnInfo(),
                                                      kPoolMin, kPoolMax);
            }(io));

        ready.count_down();
        go.wait();

        std::atomic<int> done_count{0};
        for (int i = 0; i < conc; ++i) {
            pgSelectPkWorker(pool, id, running, ops, done_count);
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

    cleanupThroughputRow();

    WARN(formatThroughputTable("PG throughput — SELECT by PK",
                                num_threads, results));
}