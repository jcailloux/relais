/**
 * bench_io_pg.cpp
 *
 * Performance benchmarks for the PostgreSQL I/O layer.
 *
 * Three flavors:
 *   - [latency]    : single coroutine, sequential round-trips (p50/p99 RTT).
 *   - [throughput] : N concurrent DetachedTask workers on the same epoll loop.
 *                    Exposes real pool/pipeline utilization. Without these,
 *                    the latency benchmarks would mislead readers into
 *                    treating 1/RTT as the system's max throughput.
 *   - [timer]      : per-operation deadline arm/cancel cost in isolation, plus
 *                    its zero-syscall / bounded-memory invariants. The
 *                    [timeout] cases re-measure the same overhead end to end.
 *
 * Run with:
 *   ./bench_io_pg                                # all benchmarks
 *   ./bench_io_pg "[latency]"                    # latency only
 *   ./bench_io_pg "[throughput]"                 # throughput only
 *   ./bench_io_pg "[throughput][select]"         # throughput SELECT only
 *   ./bench_io_pg "[timer]"                      # timer subsystem (no DB)
 *   ./bench_io_pg "[timeout]"                    # query_timeout off vs on
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
        auto pool = co_await PgPool<Io>::create(io, getConnInfo(), {.min_connections = 2, .max_connections = 4});
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
        auto pool = co_await PgPool<Io>::create(io, getConnInfo(), {.min_connections = 2, .max_connections = 4});
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
        auto pool = co_await PgPool<Io>::create(io, getConnInfo(), {.min_connections = 2, .max_connections = 4});
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
            auto pool = co_await PgPool<Io>::create(io, getConnInfo(), {.min_connections = 1, .max_connections = 1});
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
        auto pool = co_await PgPool<Io>::create(io, getConnInfo(), {.min_connections = 1, .max_connections = 1});
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
                                                      {.min_connections = kPoolMin, .max_connections = kPoolMax});
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
                                                      {.min_connections = kPoolMin, .max_connections = kPoolMax});
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
                                                      {.min_connections = kPoolMin, .max_connections = kPoolMax});
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


// #############################################################################
// =============================================================================
// TIMEOUT / TIMER — per-operation deadline overhead and timer-subsystem health
//
// Every timed operation arms a postDelayed on registerWatch and cancels it on
// removeCurrentWatch when the result wins the race (the overwhelmingly common
// case). query_timeout=0 arms nothing. These cases quantify the cost of the
// armed-but-never-fired path and assert its two structural invariants:
//   - loop-local arm issues no pipe wakeup (zero syscall),
//   - cancel removes the node outright (no tombstone, bounded memory).
// =============================================================================
// #############################################################################

// -----------------------------------------------------------------------------
//  TIMER: isolated arm + cancel cost (no DB)
//
//  The end-to-end cases below drown this in the μs-scale round-trip; here it is
//  measured directly. Steady state runs against a warmed pmr node pool, so the
//  figure is the real per-op cost a timed query pays, with no malloc.
// -----------------------------------------------------------------------------

TEST_CASE("Benchmark - timer arm+cancel (loop-local)", "[benchmark][io][timer]")
{
    Io io;
    io.runOnce(0);  // claim the loop thread so postDelayed takes the loop-local
                    // (no-wakeup) arm path that a running event loop always uses

    constexpr auto kNever = std::chrono::hours{1};  // deadline never reached
    constexpr int kWarm = 20'000;
    constexpr int kIters = 200'000;

    for (int i = 0; i < kWarm; ++i) {               // warm the pmr node pool
        auto t = io.postDelayed(kNever, []{});
        io.cancelTimer(t);
    }

    uint64_t wake0 = io.loopWakeups();
    auto t0 = Clock::now();
    for (int i = 0; i < kIters; ++i) {
        auto t = io.postDelayed(kNever, []{});
        io.cancelTimer(t);
    }
    double arm_cancel_ns =
        std::chrono::duration<double, std::nano>(Clock::now() - t0).count() / kIters;
    uint64_t wake_delta = io.loopWakeups() - wake0;
    size_t pending_after = io.pendingTimerCount();

    // Memory stability at high QPS: N deadlines live at once (one per in-flight
    // query), then all cancelled. pendingTimerCount must return exactly to 0 —
    // erase is outright, no tombstones accumulate (validates the direct removal).
    constexpr int kLive = 50'000;
    std::vector<Io::TimerToken> tokens;
    tokens.reserve(kLive);
    for (int i = 0; i < kLive; ++i) tokens.push_back(io.postDelayed(kNever, []{}));
    size_t peak_pending = io.pendingTimerCount();
    for (auto t : tokens) io.cancelTimer(t);
    size_t pending_drained = io.pendingTimerCount();

    // Deterministic structural invariants gate; the ns figure only reports.
    CHECK(wake_delta == 0);          // loop-local arm issues no pipe write
    CHECK(pending_after == 0);       // every armed timer cancelled
    CHECK(peak_pending == kLive);    // all live deadlines tracked, none coalesced
    CHECK(pending_drained == 0);     // cancel-all leaves no residue

    std::ostringstream out;
    out << "\n  timer arm+cancel (loop-local, deadline never fires)\n"
        << "    per-op:        " << fmtDuration(arm_cancel_ns / 1000.0) << "\n"
        << "    pipe wakeups:  " << wake_delta << "  (loop-local arm → 0 syscall)\n"
        << "    peak pending:  " << peak_pending << " timers held at once\n"
        << "    after drain:   " << pending_drained << "  (outright erase, no tombstone)";
    WARN(out.str());
}


// -----------------------------------------------------------------------------
//  LATENCY: same query, query_timeout off vs armed-never-fired
//
//  p50/p99 side by side. The delta is the per-round-trip arm+cancel — expected
//  to sit inside the round-trip noise (no syscall, no measurable regression).
// -----------------------------------------------------------------------------

TEST_CASE("Benchmark - PG pool query: timeout off vs on", "[benchmark][pg][latency][timeout]")
{
    Io io;

    auto results = runTask(io, [](Io& io) -> Task<std::vector<BenchResult>> {
        auto pool_off = co_await PgPool<Io>::create(io, getConnInfo(),
            {.min_connections = 1, .max_connections = 1,
             .query_timeout = std::chrono::milliseconds{0}});
        auto pool_on = co_await PgPool<Io>::create(io, getConnInfo(),
            {.min_connections = 1, .max_connections = 1,
             .query_timeout = std::chrono::milliseconds{5000}});
        PgClient<Io> off(pool_off);
        PgClient<Io> on(pool_on);

        std::vector<BenchResult> results;

        results.push_back(co_await benchAsync("SELECT 1  (query_timeout=off)", [&]() -> Task<void> {
            co_await off.query("SELECT 1");
        }));

        results.push_back(co_await benchAsync("SELECT 1  (query_timeout=5s, armed+cancelled)", [&]() -> Task<void> {
            co_await on.query("SELECT 1");
        }));

        co_return results;
    }(io));

    WARN(formatTable("PG pool query — per-op deadline arm/cancel vs round-trip", results));
}


// -----------------------------------------------------------------------------
//  THROUGHPUT: concurrent in-flight load, query_timeout off vs on
//
//  Single loop, kConc queries in flight at once — the pipeline saturates and
//  deadlines arm/cancel back to back. Reports throughput delta plus two health
//  metrics. The wakeup rate is non-zero in BOTH configs: it comes from the pool
//  granting a queued waiter (io_.post resumes it, an unconditional pipe write) —
//  the loop is oversubscribed (kConc > pool max) on purpose, to force pipelining.
//  What matters here is that query_timeout=on matches the off baseline: the
//  deadline arming is loop-local and adds no parasitic wakeup of its own. The
//  absolute zero-syscall property of arming is proven in the [timer] case.
//  peak pending = the memory bound: one timer per in-flight deadline, no more.
// -----------------------------------------------------------------------------

TEST_CASE("Benchmark - PG throughput: timeout off vs on", "[benchmark][pg][throughput][timeout]")
{
    constexpr int kConc = 32;          // in-flight queries on one loop
    int duration_s = benchDurationSeconds();

    struct Row {
        const char* label;
        double ops_s;
        double wakeups_per_kop;
        size_t peak_pending;
    };
    std::vector<Row> rows;

    for (auto qt : {std::chrono::milliseconds{0}, std::chrono::milliseconds{5000}}) {
        Io io;
        auto pool = runTask(io, [qt](Io& io) -> Task<std::shared_ptr<PgPool<Io>>> {
            co_return co_await PgPool<Io>::create(io, getConnInfo(),
                {.min_connections = kPoolMax, .max_connections = kPoolMax,
                 .query_timeout = qt});
        }(io));

        std::atomic<bool> running{true};
        std::atomic<int64_t> ops{0};
        std::atomic<int> done_count{0};
        for (int i = 0; i < kConc; ++i)
            pgSelect1Worker(pool, running, ops, done_count);

        uint64_t wake0 = io.loopWakeups();
        size_t peak = 0;
        auto t0 = Clock::now();
        auto deadline = t0 + std::chrono::seconds(duration_s);
        io.runUntil([&] {
            peak = std::max(peak, io.pendingTimerCount());
            if (Clock::now() >= deadline)
                running.store(false, std::memory_order_relaxed);
            return done_count.load(std::memory_order_relaxed) >= kConc;
        });
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - t0).count();

        uint64_t wake_delta = io.loopWakeups() - wake0;
        int64_t total = ops.load(std::memory_order_relaxed);
        double ops_s = (elapsed_us > 0) ? total * 1'000'000.0 / elapsed_us : 0.0;
        double wk_per_kop = (total > 0) ? wake_delta * 1000.0 / total : 0.0;
        rows.push_back({qt.count() == 0 ? "query_timeout=off" : "query_timeout=5s",
                        ops_s, wk_per_kop, peak});
    }

    // Enabling query_timeout must not add wakeups over the baseline: arming is
    // loop-local. The two rates are structurally identical (~1 per granted
    // waiter); any real delta would mean the timeout re-introduced a pipe write.
    double wakeup_delta = rows[1].wakeups_per_kop - rows[0].wakeups_per_kop;
    CHECK(std::abs(wakeup_delta) < 1.0);

    std::ostringstream out;
    out << "\n  PG throughput — " << kConc << " in-flight on one loop, "
        << duration_s << "s\n";
    for (const auto& r : rows)
        out << "    " << std::left << std::setw(20) << r.label
            << std::right << std::setw(12) << fmtOps(r.ops_s)
            << "   wakeups/1k ops: " << std::fixed << std::setprecision(3)
            << r.wakeups_per_kop
            << "   peak pending: " << r.peak_pending << "\n";
    out << "    timeout adds " << std::fixed << std::setprecision(4)
        << wakeup_delta << " wakeups/1k ops (loop-local arm → no pipe write)";
    WARN(out.str());
}