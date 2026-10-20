#include <catch2/catch_test_macros.hpp>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestConnInfo.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/batch/TimingEstimator.h>
#include <jcailloux/relais/io/Task.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::batch;
using Io = test::EpollIoContext;

static constexpr const char* CONNINFO = relais_test::kConnInfo;

// =============================================================================
// TimeoutGuard — prevents tests from hanging on deadlocks
// =============================================================================

namespace {

struct TimeoutGuard {
    Io& io;
    bool timed_out = false;
    EpollIoContext::TimerToken token;

    explicit TimeoutGuard(Io& io_ref,
                          std::chrono::seconds timeout = std::chrono::seconds(10))
        : io(io_ref)
        , token(static_cast<EpollIoContext&>(io_ref).postDelayed(
              timeout, [this] { timed_out = true; }))
    {}

    ~TimeoutGuard() {
        if (!timed_out)
            static_cast<EpollIoContext&>(io).cancelTimer(token);
    }

    TimeoutGuard(const TimeoutGuard&) = delete;
    TimeoutGuard& operator=(const TimeoutGuard&) = delete;
};

// YieldAwaiter — yields to the event loop without acquiring any pool connection.
// Use this to wait for fire-and-forget coroutines (DetachedTasks) to complete
// without competing with them for shared resources like pool connections.
struct YieldAwaiter {
    Io& io;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const {
        io.post([h] { h.resume(); });
    }
    void await_resume() const noexcept {}
};

// =============================================================================
// Free-function coroutine helpers for concurrent tests.
//
// Lambda coroutines are unsafe when the lambda is a temporary: the implicit
// object parameter (this) is NOT copied into the coroutine frame [dcl.fct.def.coroutine],
// so accessing captures after the lambda is destroyed is use-after-free.
// Free functions avoid this — their parameters ARE copied into the frame.
// =============================================================================

DetachedTask concurrentRead(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    int value)
{
    auto params = PgParams::make(value);
    auto result = co_await batcher->submitQueryRead(
        "SELECT $1::int AS val", std::move(params));
    REQUIRE(result.ok());
    REQUIRE(result[0].get<int32_t>(0) == value);
    ++completed;
}

DetachedTask concurrentWrite(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    const char* sql,
    int id, int val)
{
    auto params = PgParams::make(id, val);
    auto [result, coalesced] = co_await batcher->submitPgWrite(sql, std::move(params));
    (void)result;
    (void)coalesced;
    ++completed;
}

// Write coalescing helpers — track both completion and coalesced flag
DetachedTask coalescedWrite(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    std::atomic<int>& coalesced_count,
    const char* sql,
    int id, int val)
{
    auto params = PgParams::make(id, val);
    auto [result, coalesced] = co_await batcher->submitPgWrite(sql, std::move(params));
    (void)result;
    if (coalesced) coalesced_count.fetch_add(1, std::memory_order_relaxed);
    ++completed;
}

DetachedTask coalescedExecute(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    std::atomic<int>& coalesced_count,
    const char* sql,
    PgParams params)
{
    auto [result, coalesced] = co_await batcher->submitPgWrite(sql, std::move(params));
    (void)result;
    if (coalesced) coalesced_count.fetch_add(1, std::memory_order_relaxed);
    ++completed;
}

// Mode-aware write helper — records the coalesced flag and the first returned
// int column of its own result.
DetachedTask modeWrite(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    WriteMode mode,
    bool& coalesced_out,
    int& returned_out,
    const char* sql,
    PgParams params)
{
    auto [result, coalesced] = co_await batcher->submitPgWrite(
        sql, std::move(params), mode);
    coalesced_out = coalesced;
    if (result.rows() > 0) returned_out = result[0].get<int32_t>(0);
    ++completed;
}

// Ordered-write helper — submits one write, records its affectedRows and, if it
// has a RETURNING row, the first returned int column. Used to prove intra-batch
// write→write order (INSERT-then-UPDATE same PK).
DetachedTask orderedWrite(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    std::atomic<int>& affected_out,
    std::atomic<int>& returned_val_out,
    const char* sql,
    PgParams params)
{
    auto [result, coalesced] = co_await batcher->submitPgWrite(sql, std::move(params));
    (void)coalesced;
    affected_out.store(static_cast<int>(result.affectedRows()), std::memory_order_relaxed);
    if (result.rows() > 0)
        returned_val_out.store(result[0].get<int32_t>(0), std::memory_order_relaxed);
    ++completed;
}

// Entity-read helpers for the ANY-fusion test. Each records the result it
// actually received so the test can prove fan-out (a shared key reaches several
// waiters) and isolation (no waiter sees another's rows).
DetachedTask entityFind(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    const char* batch_sql, const char* single_sql,
    int key, std::vector<int>* out_vals)
{
    auto result = co_await batcher->submitEntityRead(
        batch_sql, single_sql, PgParams::make(key));
    for (int r = 0; r < result.rows(); ++r)
        out_vals->push_back(result[r].get<int32_t>(1));  // val column
    ++completed;
}

DetachedTask entityFindMany(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    const char* batch_sql, const char* single_sql,
    std::vector<int> keys, std::vector<int>* out_ids)
{
    std::vector<PgParams> kp;
    kp.reserve(keys.size());
    for (int k : keys) kp.push_back(PgParams::make(k));
    auto result = co_await batcher->submitEntityReadMany(
        batch_sql, single_sql, std::move(kp));
    for (int r = 0; r < result.rows(); ++r)
        out_ids->push_back(result[r].get<int32_t>(0));  // id column
    ++completed;
}

} // anonymous namespace

// =============================================================================
// Basic query correctness
// =============================================================================

TEST_CASE("BatchScheduler: single query returns correct result",
          "[io][batch][integration]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        auto result = co_await batcher->submitQueryRead(
            "SELECT 42 AS val", PgParams{});

        REQUIRE(result.ok());
        REQUIRE(result.rows() == 1);
        REQUIRE(result[0].get<int32_t>(0) == 42);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

TEST_CASE("BatchScheduler: parameterized query returns correct result",
          "[io][batch][integration]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        auto params = PgParams::make(7, 35);
        auto result = co_await batcher->submitQueryRead(
            "SELECT $1::int + $2::int AS val", std::move(params));

        REQUIRE(result.ok());
        REQUIRE(result.rows() == 1);
        REQUIRE(result[0].get<int32_t>(0) == 42);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// =============================================================================
// Write path
// =============================================================================

TEST_CASE("BatchScheduler: submitPgWrite returns result with RETURNING",
          "[io][batch][integration]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS batch_test_write (id INT, val TEXT)");

        auto params = PgParams::make(1, "hello");
        auto [result, coalesced] = co_await batcher->submitPgWrite(
            "INSERT INTO batch_test_write (id, val) VALUES ($1, $2) RETURNING id, val",
            std::move(params));

        REQUIRE(result.ok());
        REQUIRE(result.rows() == 1);
        REQUIRE(result[0].get<int32_t>(0) == 1);
        REQUIRE(result[0].get<std::string>(1) == "hello");
        REQUIRE_FALSE(coalesced);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

TEST_CASE("BatchScheduler: submitPgWrite returns affected rows",
          "[io][batch][integration]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS batch_test_exec (id INT)");
        co_await batcher->directQuery(
            "INSERT INTO batch_test_exec VALUES (1), (2), (3)");

        auto params = PgParams::make(2);
        auto [result, coalesced] = co_await batcher->submitPgWrite(
            "DELETE FROM batch_test_exec WHERE id = $1", std::move(params));

        REQUIRE(result.affectedRows() == 1);
        REQUIRE_FALSE(coalesced);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// =============================================================================
// Write ordering contract — intra-batch write→write order == seq order.
//
// Two writes of the same PK submitted into the same write batch (INSERT then
// UPDATE) must land in submission (seq) order: the INSERT runs before the
// UPDATE, so the UPDATE matches the freshly-inserted row and the row's final
// value is the UPDATE's. Both are launched without yielding between them, so
// they accumulate into one pending write batch (same path the coalescing tests
// exercise). seq sorting (firePgWriteBatch) is what restores submission order.
// Contract: docs/runtime-and-threading.md ("Write ordering").
// =============================================================================
TEST_CASE("Write ordering: INSERT-then-UPDATE same PK lands in seq order",
          "[io][batch][integration]")
{
    Io io;
    std::atomic<int> completed{0};
    std::atomic<int> insert_affected{-1};
    std::atomic<int> update_affected{-1};
    std::atomic<int> insert_ret{-1};   // INSERT has no RETURNING → stays -1
    std::atomic<int> update_val{-1};    // UPDATE RETURNING val → the final value
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS ord_test (id INT PRIMARY KEY, val INT)");

        // INSERT takes seq=0, UPDATE seq=1 — submitted in this order, no yield
        // between → same batch. The UPDATE must see the inserted row.
        orderedWrite(batcher, completed, insert_affected, insert_ret,
            "INSERT INTO ord_test (id, val) VALUES ($1, $2)",
            PgParams::make(1, 100));
        orderedWrite(batcher, completed, update_affected, update_val,
            "UPDATE ord_test SET val = $2 WHERE id = $1 RETURNING val",
            PgParams::make(1, 999));
    };
    task();

    io.runUntil([&] { return completed.load() == 2 || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(completed.load() == 2);

    // INSERT created the row; UPDATE matched it (affected==1, RETURNING 1 row)
    // — proving the INSERT executed first.
    REQUIRE(insert_affected.load() == 1);
    REQUIRE(update_affected.load() == 1);
    // Final value is the UPDATE's, not the INSERT's.
    REQUIRE(update_val.load() == 999);
}

// =============================================================================
// ConcurrencyGate: concurrent queries with tight budget (deadlock detection)
//
// max_concurrent=1 forces all queries through the gate's waiter queue.
// With N=50, the double-increment bug (fixed in ConcurrencyGate::release)
// would cause inflight to accumulate phantom counts → permanent deadlock.
// =============================================================================

TEST_CASE("BatchScheduler: concurrent queries with tight budget don't deadlock",
          "[io][batch][integration]")
{
    Io io;
    std::atomic<int> completed{0};
    constexpr int N = 50;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(
            io, pool, /*max_concurrent=*/1);

        for (int i = 0; i < N; ++i) {
            concurrentRead(batcher, completed, i + 1);
        }
    };
    task();

    io.runUntil([&] { return completed.load() == N || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(completed.load() == N);
}

// =============================================================================
// Mixed reads and writes through the gate
// =============================================================================

TEST_CASE("BatchScheduler: mixed reads and writes don't deadlock",
          "[io][batch][integration]")
{
    Io io;
    std::atomic<int> completed{0};
    constexpr int N = 30;
    TimeoutGuard timeout(io);

    static constexpr const char* INSERT_SQL =
        "INSERT INTO batch_test_mixed VALUES ($1, $2)";

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(
            io, pool, /*max_concurrent=*/2);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS batch_test_mixed (id INT, val INT)");

        for (int i = 0; i < N; ++i) {
            if (i % 3 == 0) {
                concurrentWrite(batcher, completed, INSERT_SQL, i, i * 10);
            } else {
                concurrentRead(batcher, completed, i);
            }
        }
    };
    task();

    io.runUntil([&] { return completed.load() == N || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(completed.load() == N);
}

// =============================================================================
// Timing estimator integration
// =============================================================================

TEST_CASE("BatchScheduler: timing estimator updates after bootstrap",
          "[io][batch][integration]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        // Initial state
        REQUIRE(batcher->estimator().isPgBootstrapping());
        REQUIRE(batcher->estimator().pg_bootstrap_count == 0);
        REQUIRE(batcher->estimator().pg_network_time_ns == 0.0);

        static constexpr const char* SQL = "SELECT $1::int AS val";

        // Run enough queries to exit bootstrap phase
        for (int i = 0; i < TimingEstimator::kBootstrapThreshold; ++i) {
            auto params = PgParams::make(i);
            auto result = co_await batcher->submitQueryRead(SQL, std::move(params));
            REQUIRE(result.ok());
        }

        // Bootstrap should be complete
        REQUIRE_FALSE(batcher->estimator().isPgBootstrapping());
        REQUIRE(batcher->estimator().pg_bootstrap_count
                >= TimingEstimator::kBootstrapThreshold);

        // Network time should be calibrated
        REQUIRE(batcher->estimator().pg_network_time_ns > 0.0);

        // Staleness should be reset
        REQUIRE_FALSE(batcher->estimator().isPgStale());

        // Per-SQL timing should be recorded
        double sql_time = batcher->estimator().getRequestTime(SQL);
        REQUIRE(sql_time > 0.0);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// =============================================================================
// directQuery bypass (for BEGIN/COMMIT/SET etc.)
// =============================================================================

TEST_CASE("BatchScheduler: directQuery bypass works",
          "[io][batch][integration]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        // directQuery: no params
        auto r1 = co_await batcher->directQuery("SELECT 1 AS val");
        REQUIRE(r1.ok());
        REQUIRE(r1[0].get<int32_t>(0) == 1);

        // directQueryParams
        auto params = PgParams::make(42);
        auto r2 = co_await batcher->directQueryParams(
            "SELECT $1::int AS val", params);
        REQUIRE(r2.ok());
        REQUIRE(r2[0].get<int32_t>(0) == 42);

        // directExecute
        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS batch_test_direct (id INT)");
        auto params2 = PgParams::make(999);
        int affected = co_await batcher->directExecute(
            "DELETE FROM batch_test_direct WHERE id = $1", params2);
        REQUIRE(affected == 0);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// =============================================================================
// Nagle strategy: batch accumulation after bootstrap
//
// After bootstrap (5 sequential queries), submit concurrent queries.
// Verify they all complete correctly — the batch path is exercised when
// queries arrive while a batch is already accumulating.
// =============================================================================

TEST_CASE("BatchScheduler: concurrent queries after bootstrap complete correctly",
          "[io][batch][integration]")
{
    Io io;
    std::atomic<int> completed{0};
    constexpr int N_CONCURRENT = 20;
    bool bootstrap_done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(
            io, pool, /*max_concurrent=*/4);

        static constexpr const char* SQL = "SELECT $1::int AS val";

        // Phase 1: sequential bootstrap
        for (int i = 0; i < TimingEstimator::kBootstrapThreshold; ++i) {
            auto params = PgParams::make(i);
            co_await batcher->submitQueryRead(SQL, std::move(params));
        }
        REQUIRE_FALSE(batcher->estimator().isPgBootstrapping());
        bootstrap_done = true;

        // Phase 2: concurrent queries — some may go through batch path
        for (int i = 0; i < N_CONCURRENT; ++i) {
            concurrentRead(batcher, completed, i + 100);
        }
    };
    task();

    io.runUntil([&] {
        return completed.load() == N_CONCURRENT || timeout.timed_out;
    });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(bootstrap_done);
    REQUIRE(completed.load() == N_CONCURRENT);
}

// =============================================================================
// Write coalescing tests
// =============================================================================

// Helper: bootstrap the estimator past the threshold so Nagle batching activates.
// Must be co_awaited (not fire-and-forget) so the event loop can drive the queries.
Task<void> bootstrapPg(std::shared_ptr<BatchScheduler<Io>> batcher)
{
    for (int i = 0; i < TimingEstimator::kBootstrapThreshold + 2; ++i) {
        auto params = PgParams::make(i);
        co_await batcher->submitQueryRead("SELECT $1::int", std::move(params));
    }
    // Also bootstrap the write path
    for (int i = 0; i < TimingEstimator::kBootstrapThreshold + 2; ++i) {
        auto params = PgParams::make(i, i * 10);
        auto [result, coalesced] = co_await batcher->submitPgWrite(
            "SELECT $1::int, $2::int", std::move(params));
        (void)result;
        (void)coalesced;
    }
}

// =============================================================================
// Entity-read fusion (P2-B): concurrent find + findMany collapse into ONE
// deduplicated `pk = ANY` segment. Asserts the fan-out / isolation contract:
//   - a key requested by both a find and a findMany feeds BOTH (fan-out),
//   - each waiter receives EXACTLY its own rows (no cross-caller leakage),
//   - absent keys yield empty results (single → no rows, multi → empty subset).
// =============================================================================

TEST_CASE("Entity read fusion: concurrent find + findMany share one ANY",
          "[io][batch][integration][findmany][fusion]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        // Single connection: the temp table below is connection-local, while
        // the leader's direct read and the fused ANY each acquire from the pool.
        // A multi-connection pool would split them across connections — only one
        // holds the data — so the fused read can land on an empty table. The
        // fusion path (dedup + fan-out) runs entirely on one pipelined
        // connection regardless, so a 1-connection pool exercises it fully.
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        // Bootstrap so Nagle batching is active (else every read goes direct).
        co_await bootstrapPg(batcher);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS fuse_test (id INT PRIMARY KEY, val INT)");
        co_await batcher->directQuery(
            "INSERT INTO fuse_test VALUES (1,10),(2,20),(3,30),(4,40),(5,50)");

        static constexpr const char* SINGLE =
            "SELECT id, val FROM fuse_test WHERE id = $1";
        static constexpr const char* ANY =
            "SELECT id, val FROM fuse_test WHERE id = ANY($1)";

        std::atomic<int> completed{0};
        std::vector<int> leaderVals, find2Vals, find5Vals, find9Vals,
                         manyIds, manyAbsentIds;

        // The first read goes direct (Nagle leader); the rest accumulate during
        // its RTT and FUSE into one deduplicated ANY.
        entityFind(batcher, completed, ANY, SINGLE, 1, &leaderVals);
        entityFind(batcher, completed, ANY, SINGLE, 2, &find2Vals);   // shares key 2
        entityFindMany(batcher, completed, ANY, SINGLE, {2, 3, 4}, &manyIds);
        entityFind(batcher, completed, ANY, SINGLE, 5, &find5Vals);   // disjoint
        entityFind(batcher, completed, ANY, SINGLE, 9, &find9Vals);   // absent
        entityFindMany(batcher, completed, ANY, SINGLE, {7, 8}, &manyAbsentIds);

        while (completed.load() < 6) co_await YieldAwaiter{io};

        // Leader served direct → its own single row.
        REQUIRE(leaderVals == std::vector<int>{10});
        // Fan-out: key 2's row reaches both the single find and the findMany.
        REQUIRE(find2Vals == std::vector<int>{20});
        std::sort(manyIds.begin(), manyIds.end());
        REQUIRE(manyIds == std::vector<int>{2, 3, 4});  // exactly its keys
        // Disjoint find: only its row, no leakage from the findMany.
        REQUIRE(find5Vals == std::vector<int>{50});
        // Absent single key → zero rows.
        REQUIRE(find9Vals.empty());
        // Absent multi keys → empty subset.
        REQUIRE(manyAbsentIds.empty());

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// =============================================================================
// Pipelined batches whose statements are prepared mid-batch.
//
// A statement unknown to the connection is prepared inside the pipeline, just
// before its execution. Every caller of a batch mixing several such statements
// (or a known statement followed by an unknown one) must still receive the
// result of its own query. Each SQL below returns a (tag, v) signature unique
// to the caller, so a misrouted or empty result is visible.
// =============================================================================

namespace {

struct TaggedOutcome {
    int rows = -1;
    int tag = 0;
    int v = 0;
    bool error = false;
};

void recordTagged(const PgResult& result, TaggedOutcome* out) {
    out->rows = result.rows();
    if (result.rows() > 0 && result.cols() == 2) {
        out->tag = result[0].get<int32_t>(0);
        out->v = result[0].get<int32_t>(1);
    }
}

DetachedTask taggedRead(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    const char* sql, int v, TaggedOutcome* out)
{
    try {
        auto result = co_await batcher->submitQueryRead(sql, PgParams::make(v));
        recordTagged(result, out);
    } catch (...) {
        out->error = true;
    }
    ++completed;
}

DetachedTask taggedWrite(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    const char* sql, int v, TaggedOutcome* out)
{
    try {
        auto [result, coalesced] = co_await batcher->submitPgWrite(
            sql, PgParams::make(v), WriteMode::Exclusive);
        (void)coalesced;
        recordTagged(result, out);
    } catch (...) {
        out->error = true;
    }
    ++completed;
}

void checkTagged(const TaggedOutcome& o, int tag, int v) {
    INFO("expected tag " << tag << " v " << v
         << ", got rows " << o.rows << " tag " << o.tag << " v " << o.v
         << (o.error ? " (error)" : ""));
    CHECK_FALSE(o.error);
    CHECK(o.rows == 1);
    CHECK(o.tag == tag);
    CHECK(o.v == v);
}

} // anonymous namespace

TEST_CASE("Pipelined read batch: several statements prepared mid-batch",
          "[io][batch][integration][pipeline-prepare]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);
    std::array<TaggedOutcome, 4> out{};

    auto task = [&]() -> DetachedTask {
        // Single connection: the batch runs on the connection the leader
        // used, whose prepared-statement set is known.
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
        co_await bootstrapPg(batcher);

        std::atomic<int> completed{0};
        // Submitted in the same loop turn: the first goes direct (Nagle
        // leader), the other three form one batch of three unprepared SQLs.
        taggedRead(batcher, completed, "SELECT 9101::int AS tag, $1::int AS v", 1, &out[0]);
        taggedRead(batcher, completed, "SELECT 9102::int AS tag, $1::int AS v", 2, &out[1]);
        taggedRead(batcher, completed, "SELECT 9103::int AS tag, $1::int AS v", 3, &out[2]);
        taggedRead(batcher, completed, "SELECT 9104::int AS tag, $1::int AS v", 4, &out[3]);

        while (completed.load() < 4) co_await YieldAwaiter{io};
        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);

    checkTagged(out[0], 9101, 1);
    checkTagged(out[1], 9102, 2);
    checkTagged(out[2], 9103, 3);
    checkTagged(out[3], 9104, 4);
}

TEST_CASE("Pipelined read batch: prepared statement ahead of an unprepared one",
          "[io][batch][integration][pipeline-prepare]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);
    std::array<TaggedOutcome, 3> out{};

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
        co_await bootstrapPg(batcher);

        static constexpr const char* KNOWN = "SELECT 9201::int AS tag, $1::int AS v";
        static constexpr const char* UNKNOWN = "SELECT 9202::int AS tag, $1::int AS v";

        std::atomic<int> completed{0};
        // The leader prepares KNOWN on the connection; the batch then holds
        // KNOWN (already prepared) followed by UNKNOWN (prepared mid-batch).
        taggedRead(batcher, completed, KNOWN, 1, &out[0]);
        taggedRead(batcher, completed, KNOWN, 2, &out[1]);
        taggedRead(batcher, completed, UNKNOWN, 3, &out[2]);

        while (completed.load() < 3) co_await YieldAwaiter{io};
        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);

    checkTagged(out[0], 9201, 1);
    checkTagged(out[1], 9201, 2);
    checkTagged(out[2], 9202, 3);
}

TEST_CASE("Pipelined write batch: several statements prepared mid-batch",
          "[io][batch][integration][pipeline-prepare]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);
    std::array<TaggedOutcome, 4> out{};

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
        co_await bootstrapPg(batcher);

        std::atomic<int> completed{0};
        // Exclusive: no coalescing. Same Nagle as reads: one direct leader,
        // then a batch of three unprepared SQLs.
        taggedWrite(batcher, completed, "SELECT 9301::int AS tag, $1::int AS v", 1, &out[0]);
        taggedWrite(batcher, completed, "SELECT 9302::int AS tag, $1::int AS v", 2, &out[1]);
        taggedWrite(batcher, completed, "SELECT 9303::int AS tag, $1::int AS v", 3, &out[2]);
        taggedWrite(batcher, completed, "SELECT 9304::int AS tag, $1::int AS v", 4, &out[3]);

        while (completed.load() < 4) co_await YieldAwaiter{io};
        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);

    checkTagged(out[0], 9301, 1);
    checkTagged(out[1], 9302, 2);
    checkTagged(out[2], 9303, 3);
    checkTagged(out[3], 9304, 4);
}

TEST_CASE("Write coalescing: identical writes in batch are coalesced",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        // Bootstrap to activate Nagle batching
        co_await bootstrapPg(batcher);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS coal_test (id INT PRIMARY KEY, val INT)");
        co_await batcher->directQuery(
            "INSERT INTO coal_test VALUES (1, 100)");

        // Launch N identical UPDATE writes concurrently.
        // The first goes direct (Nagle), subsequent accumulate and should coalesce.
        constexpr int N = 10;
        std::atomic<int> completed{0};
        std::atomic<int> coalesced_count{0};
        static constexpr const char* UPDATE_SQL =
            "UPDATE coal_test SET val = $2 WHERE id = $1 RETURNING id";

        for (int i = 0; i < N; ++i) {
            coalescedWrite(batcher, completed, coalesced_count,
                           UPDATE_SQL, 1, 999);
        }

        while (completed.load() < N) {
            co_await YieldAwaiter{io};
        }

        REQUIRE(completed.load() == N);
        // At least some writes should have been coalesced
        // (the first goes direct, subsequent accumulate during its RTT)
        REQUIRE(coalesced_count.load() > 0);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

TEST_CASE("Write coalescing: different params are NOT coalesced",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await bootstrapPg(batcher);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS coal_diff (id INT PRIMARY KEY, val INT)");
        co_await batcher->directQuery(
            "INSERT INTO coal_diff VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)");

        // Launch writes with DIFFERENT keys — none should coalesce
        constexpr int N = 5;
        std::atomic<int> completed{0};
        std::atomic<int> coalesced_count{0};
        static constexpr const char* SQL =
            "UPDATE coal_diff SET val = $2 WHERE id = $1 RETURNING id";

        for (int i = 1; i <= N; ++i) {
            coalescedWrite(batcher, completed, coalesced_count, SQL, i, 42);
        }

        while (completed.load() < N) {
            co_await YieldAwaiter{io};
        }

        REQUIRE(completed.load() == N);
        REQUIRE(coalesced_count.load() == 0);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

TEST_CASE("Write coalescing: coalesced followers get correct result",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await bootstrapPg(batcher);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS coal_result (id INT PRIMARY KEY, val TEXT)");
        co_await batcher->directQuery(
            "INSERT INTO coal_result VALUES (1, 'old')");

        // Launch identical writes and verify all get the same valid result
        constexpr int N = 8;
        std::atomic<int> completed{0};

        struct Slot { PgResult result; bool coalesced = false; };
        std::vector<Slot> slots(N);

        static constexpr const char* SQL =
            "UPDATE coal_result SET val = $2 WHERE id = $1 RETURNING id, val";

        // Use individual DetachedTasks that write to indexed slots
        for (int i = 0; i < N; ++i) {
            [](std::shared_ptr<BatchScheduler<Io>> b,
               std::atomic<int>& comp,
               Slot& slot,
               const char* sql) -> DetachedTask
            {
                auto params = PgParams::make(1, "updated");
                auto [result, coalesced] = co_await b->submitPgWrite(sql, std::move(params));
                slot.result = std::move(result);
                slot.coalesced = coalesced;
                ++comp;
            }(batcher, completed, slots[i], SQL);
        }

        while (completed.load() < N) {
            co_await YieldAwaiter{io};
        }

        // ALL results (leader + followers) must be valid
        for (int i = 0; i < N; ++i) {
            REQUIRE(slots[i].result.ok());
            REQUIRE(slots[i].result.rows() == 1);
            REQUIRE(slots[i].result[0].get<std::string>(1) == "updated");
        }

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// Idempotence invariant behind write-coalescing: dropping N-1 identical writes
// is sound ONLY because the SET is absolute (val=$2). N coalesced absolute writes
// must leave the SAME final state as a single write. (The generator guarantees
// only absolute SETs — locked statically by test_relais_gen_sql; here we lock the
// runtime consequence.)
TEST_CASE("Write coalescing: N absolute SETs are idempotent in final state",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await bootstrapPg(batcher);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS coal_idem (id INT PRIMARY KEY, val INT)");
        co_await batcher->directQuery(
            "INSERT INTO coal_idem VALUES (1, 0)");

        // N identical absolute writes on the same key — all coalesce into one.
        constexpr int N = 8;
        constexpr int kVal = 99;
        std::atomic<int> completed{0};
        std::atomic<int> coalesced_count{0};
        static constexpr const char* SQL =
            "UPDATE coal_idem SET val = $2 WHERE id = $1 RETURNING id";

        for (int i = 0; i < N; ++i) {
            coalescedWrite(batcher, completed, coalesced_count, SQL, 1, kVal);
        }

        while (completed.load() < N) {
            co_await YieldAwaiter{io};
        }
        REQUIRE(completed.load() == N);
        // Coalescing must actually have happened, otherwise the invariant is untested.
        REQUIRE(coalesced_count.load() > 0);

        // Final state equals a single absolute write: val == kVal, exactly one row.
        auto check = co_await batcher->directQuery(
            "SELECT val FROM coal_idem WHERE id = 1");
        REQUIRE(check.ok());
        REQUIRE(check.rows() == 1);
        REQUIRE(check[0].get<int32_t>(0) == kVal);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// A relative write is not idempotent: coalescing N identical `val = val + 1`
// would apply one increment and report N successes. WriteMode::Exclusive keeps
// each write on its own entry, so the final state counts every caller.
TEST_CASE("Write coalescing: identical Exclusive writes all execute",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await bootstrapPg(batcher);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS coal_excl (id INT PRIMARY KEY, val INT)");
        co_await batcher->directQuery(
            "INSERT INTO coal_excl VALUES (1, 0)");

        constexpr int N = 10;
        std::atomic<int> completed{0};
        bool coalesced[N] = {};
        int returned[N] = {};
        static constexpr const char* SQL =
            "UPDATE coal_excl SET val = val + 1 WHERE id = $1 RETURNING val";

        for (int i = 0; i < N; ++i) {
            modeWrite(batcher, completed, WriteMode::Exclusive,
                      coalesced[i], returned[i], SQL, PgParams::make(1));
        }

        while (completed.load() < N) {
            co_await YieldAwaiter{io};
        }

        // No caller was handed another's result: each saw its own increment.
        for (int i = 0; i < N; ++i) REQUIRE_FALSE(coalesced[i]);
        std::vector<int> seen(returned, returned + N);
        std::sort(seen.begin(), seen.end());
        for (int i = 0; i < N; ++i) REQUIRE(seen[i] == i + 1);

        auto check = co_await batcher->directQuery(
            "SELECT val FROM coal_excl WHERE id = 1");
        REQUIRE(check.ok());
        REQUIRE(check[0].get<int32_t>(0) == N);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// The opt-out is two-sided: an Exclusive entry never joins an identical
// Idempotent leader, and never serves as a leader for an identical Idempotent
// write. Idempotent writes still coalesce among themselves in the same batch.
TEST_CASE("Write coalescing: Exclusive neither joins nor leads Idempotent",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await bootstrapPg(batcher);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS coal_mixed (id INT PRIMARY KEY, val INT)");
        co_await batcher->directQuery(
            "INSERT INTO coal_mixed VALUES (1, 0), (2, 0)");

        static constexpr const char* SQL =
            "UPDATE coal_mixed SET val = $2 WHERE id = $1 RETURNING id";

        // Submission order (all but the first land in one pending batch):
        //   0: distinct write, goes direct (Nagle leader)
        //   1: Exclusive X   — first X in the batch
        //   2: Idempotent X  — must not join the Exclusive entry
        //   3: Idempotent X  — coalesces onto entry 2
        //   4: Exclusive X   — must not join the Idempotent entry
        constexpr int N = 5;
        const WriteMode modes[N] = {
            WriteMode::Idempotent, WriteMode::Exclusive, WriteMode::Idempotent,
            WriteMode::Idempotent, WriteMode::Exclusive};
        std::atomic<int> completed{0};
        bool coalesced[N] = {};
        int returned[N] = {};

        for (int i = 0; i < N; ++i) {
            const int id = i == 0 ? 2 : 1;
            modeWrite(batcher, completed, modes[i], coalesced[i], returned[i],
                      SQL, PgParams::make(id, 7));
        }

        while (completed.load() < N) {
            co_await YieldAwaiter{io};
        }

        REQUIRE_FALSE(coalesced[0]);
        REQUIRE_FALSE(coalesced[1]);
        REQUIRE_FALSE(coalesced[2]);
        REQUIRE(coalesced[3]);
        REQUIRE_FALSE(coalesced[4]);
        REQUIRE(returned[0] == 2);
        for (int i = 1; i < N; ++i) REQUIRE(returned[i] == 1);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

TEST_CASE("Write coalescing: submitPgWrite coalescing returns correct affected rows",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await bootstrapPg(batcher);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS coal_exec (id INT PRIMARY KEY, val INT)");
        co_await batcher->directQuery(
            "INSERT INTO coal_exec VALUES (1, 0)");

        // Launch identical DELETEs via submitPgWrite
        constexpr int N = 6;
        std::atomic<int> completed{0};
        std::atomic<int> coalesced_count{0};
        static constexpr const char* SQL =
            "DELETE FROM coal_exec WHERE id = $1";

        for (int i = 0; i < N; ++i) {
            coalescedExecute(batcher, completed, coalesced_count,
                             SQL, PgParams::make(1));
        }

        while (completed.load() < N) {
            co_await YieldAwaiter{io};
        }
        REQUIRE(completed.load() == N);
        // At least some should be coalesced
        REQUIRE(coalesced_count.load() > 0);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

TEST_CASE("Write coalescing: mixed identical and different writes coalesce by group",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

        co_await bootstrapPg(batcher);

        co_await batcher->directQuery(
            "CREATE TEMP TABLE IF NOT EXISTS coal_mixed (id INT PRIMARY KEY, val INT)");
        co_await batcher->directQuery(
            "INSERT INTO coal_mixed VALUES (1, 0), (2, 0)");

        // Mix: 4 identical writes (id=1, val=10) + 4 identical writes (id=2, val=20)
        std::atomic<int> completed{0};
        std::atomic<int> coalesced_count{0};
        static constexpr const char* SQL =
            "UPDATE coal_mixed SET val = $2 WHERE id = $1 RETURNING id";

        // Group A: same key=1
        for (int i = 0; i < 4; ++i) {
            coalescedWrite(batcher, completed, coalesced_count, SQL, 1, 10);
        }
        // Group B: same key=2
        for (int i = 0; i < 4; ++i) {
            coalescedWrite(batcher, completed, coalesced_count, SQL, 2, 20);
        }

        // Use event loop polling (no pool connection acquired) to wait
        while (completed.load() < 8) {
            co_await YieldAwaiter{io};
        }

        REQUIRE(completed.load() == 8);
        // Each group should have coalesced followers (at least some from each group)
        REQUIRE(coalesced_count.load() > 0);
        // At most 6 coalesced (8 total - min 2 leaders, one per group)
        REQUIRE(coalesced_count.load() <= 6);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// =============================================================================
// Read in flight: a read identical to the direct one in flight (Nagle leader)
// takes its result instead of sending its own statement, unless a write
// completed on the scheduler since that statement was sent.
// =============================================================================

namespace {

struct ReadOutcome {
    int value = -1;
    bool done = false;
    bool timed_out = false;
    bool other_error = false;
};

DetachedTask readInt(std::shared_ptr<BatchScheduler<Io>> batcher,
                     const char* sql, int key, ReadOutcome* out)
{
    try {
        auto result = co_await batcher->submitQueryRead(sql, PgParams::make(key));
        if (result.rows() > 0) out->value = result[0].get<int32_t>(0);
    } catch (const PgQueryTimeout&) {
        out->timed_out = true;
    } catch (...) {
        out->other_error = true;
    }
    out->done = true;
}

// Writes `val` for row 1, on the write path or on the read path (as
// eraseWhere's DELETE does), then reads it back with `sql`.
DetachedTask writeThenRead(std::shared_ptr<BatchScheduler<Io>> batcher,
                           const char* sql, int val, bool read_path, ReadOutcome* out)
{
    static constexpr const char* UPDATE = "UPDATE lead_rw SET val = $2 WHERE id = $1";
    if (read_path)
        co_await batcher->submitQueryRead(UPDATE, PgParams::make(1, val));
    else
        co_await batcher->submitPgWrite(UPDATE, PgParams::make(1, val));
    auto result = co_await batcher->submitQueryRead(sql, PgParams::make(1));
    if (result.rows() > 0) out->value = result[0].get<int32_t>(0);
    out->done = true;
}

bool allDone(std::initializer_list<const ReadOutcome*> outs) {
    for (auto* o : outs) if (!o->done) return false;
    return true;
}

} // anonymous namespace

TEST_CASE("Read in flight: identical reads share the direct read's statement",
          "[io][batch][integration][coalescing]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
        co_await bootstrapPg(batcher);

        static constexpr const char* SQL = "SELECT $1::int * 2";
        ReadOutcome a, b, c, d;

        auto before = batcher->pgReadsSent();
        readInt(batcher, SQL, 21, &a);  // direct
        readInt(batcher, SQL, 21, &b);  // shares a's statement
        readInt(batcher, SQL, 21, &c);  // shares a's statement
        while (!allDone({&a, &b, &c})) co_await YieldAwaiter{io};
        CHECK(a.value == 42);
        CHECK(b.value == 42);
        CHECK(c.value == 42);
        CHECK(batcher->pgReadsSent() - before == 1);

        a = b = c = {};
        before = batcher->pgReadsSent();
        readInt(batcher, SQL, 1, &a);   // direct
        readInt(batcher, SQL, 1, &b);   // shares a's statement
        readInt(batcher, SQL, 2, &c);   // batch
        readInt(batcher, SQL, 2, &d);   // coalesced in the batch
        while (!allDone({&a, &b, &c, &d})) co_await YieldAwaiter{io};
        CHECK(a.value == 2);
        CHECK(b.value == 2);
        CHECK(c.value == 4);
        CHECK(d.value == 4);
        CHECK(batcher->pgReadsSent() - before == 2);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

TEST_CASE("Read in flight: not shared once a write completed since its send",
          "[io][batch][integration][coalescing]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    // The direct read sleeps on one connection while the write and the
    // second read run on the other.
    static constexpr const char* SLOW =
        "SELECT val FROM lead_rw, pg_sleep(0.3) WHERE id = $1";

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 2, .max_connections = 2});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
        co_await bootstrapPg(batcher);
        co_await batcher->directQuery(
            "CREATE TABLE IF NOT EXISTS lead_rw (id INT PRIMARY KEY, val INT)");
        co_await batcher->directQuery("DELETE FROM lead_rw");
        co_await batcher->directQuery("INSERT INTO lead_rw VALUES (1, 10)");

        ReadOutcome lead, next;
        auto before = batcher->pgReadsSent();

        SECTION("no write in between: the second read shares the first") {
            readInt(batcher, SLOW, 1, &lead);
            readInt(batcher, SLOW, 1, &next);
            while (!allDone({&lead, &next})) co_await YieldAwaiter{io};
            CHECK(lead.value == 10);
            CHECK(next.value == 10);
            CHECK(batcher->pgReadsSent() - before == 1);
        }
        SECTION("a write awaited in between: the second read sees it") {
            readInt(batcher, SLOW, 1, &lead);
            writeThenRead(batcher, SLOW, 20, false, &next);
            while (!allDone({&lead, &next})) co_await YieldAwaiter{io};
            CHECK(lead.value == 10);
            CHECK(next.value == 20);
            CHECK(batcher->pgReadsSent() - before == 2);
        }
        SECTION("a statement of the read path that wrote: the second read sees it") {
            readInt(batcher, SLOW, 1, &lead);
            writeThenRead(batcher, SLOW, 30, true, &next);
            while (!allDone({&lead, &next})) co_await YieldAwaiter{io};
            CHECK(lead.value == 10);
            CHECK(next.value == 30);
            CHECK(batcher->pgReadsSent() - before == 3);  // + the UPDATE
        }

        co_await batcher->directQuery("DROP TABLE lead_rw");
        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

TEST_CASE("Read in flight: the direct read's error reaches every follower",
          "[io][batch][integration][coalescing]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO,
            {.min_connections = 1, .max_connections = 1,
             .query_timeout = std::chrono::milliseconds(200)});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
        co_await bootstrapPg(batcher);

        static constexpr const char* SLOW = "SELECT $1::int, pg_sleep(1)";
        ReadOutcome a, b, c;
        auto before = batcher->pgReadsSent();
        readInt(batcher, SLOW, 1, &a);
        readInt(batcher, SLOW, 1, &b);
        readInt(batcher, SLOW, 1, &c);
        while (!allDone({&a, &b, &c})) co_await YieldAwaiter{io};
        CHECK(a.timed_out);
        CHECK(b.timed_out);
        CHECK(c.timed_out);
        CHECK(batcher->pgReadsSent() - before == 1);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

TEST_CASE("Read in flight: a cancelled follower leaves the others to finish",
          "[io][batch][integration][coalescing]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
        co_await bootstrapPg(batcher);

        static constexpr const char* SQL = "SELECT $1::int, pg_sleep(0.05)";
        ReadOutcome lead, kept;
        auto before = batcher->pgReadsSent();
        readInt(batcher, SQL, 7, &lead);

        // A follower started by hand, then destroyed while it waits.
        auto cancelled = std::make_unique<Task<PgResult>>(
            batcher->submitQueryRead(SQL, PgParams::make(7)));
        cancelled->await_suspend(std::noop_coroutine()).resume();

        readInt(batcher, SQL, 7, &kept);
        cancelled.reset();

        while (!allDone({&lead, &kept})) co_await YieldAwaiter{io};
        CHECK(lead.value == 7);
        CHECK(kept.value == 7);
        CHECK(batcher->pgReadsSent() - before == 1);

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// =============================================================================
// Write batch fan-out: a batch commits on several connections, one row per
// connection, in submission order.
// =============================================================================

namespace relais_bench {

struct BatchBenchAccessor {
    static size_t writeUnits(const BatchScheduler<Io>& b) { return b.pg_write_units_; }
    static size_t detachedInflight(const BatchScheduler<Io>& b) { return b.detached_inflight_; }
    static size_t writeFanout(const BatchScheduler<Io>& b) { return b.pg_write_fanout_; }
};

}  // namespace relais_bench

namespace {

using relais_bench::BatchBenchAccessor;

constexpr const char* kFanoutTable = "relais_test_fanout";

// Each statement answers (value, backend pid).
constexpr const char* kAppendDigit =
    "UPDATE relais_test_fanout SET v = v * 10 + $2 WHERE id = $1 RETURNING v, pg_backend_pid()";
constexpr const char* kSetValue =
    "UPDATE relais_test_fanout SET v = $2 WHERE id = $1 RETURNING v, pg_backend_pid()";
constexpr const char* kInsertRow =
    "INSERT INTO relais_test_fanout VALUES ($1, $2) RETURNING v, pg_backend_pid()";
constexpr const char* kKillOwnBackend =
    "SELECT $1::int + $2::int, pg_backend_pid() FROM pg_terminate_backend(pg_backend_pid())";
constexpr const char* kEcho = "SELECT $1::int + $2::int, pg_backend_pid()";

WriteAffinity rowOf() { return rowAt(tableTag(kFanoutTable), 0, 1); }

struct Outcome {
    bool done = false;
    bool failed = false;
    bool coalesced = false;
    int value = 0;
    int pid = 0;
};

DetachedTask fanoutWrite(std::shared_ptr<BatchScheduler<Io>> b, Outcome& out,
                         const char* sql, int id, int arg, WriteAffinity affinity,
                         WriteMode mode = WriteMode::Exclusive)
{
    try {
        auto [result, coalesced] = co_await b->submitPgWrite(
            sql, PgParams::make(id, arg), mode, affinity);
        out.coalesced = coalesced;
        out.failed = !result.ok();   // a failed statement answers an error result
        if (result.rows() > 0) {
            out.value = result[0].get<int32_t>(0);
            out.pid = result[0].get<int32_t>(1);
        }
    } catch (...) {
        out.failed = true;
    }
    out.done = true;
}

// A pool of 8 warm connections, so every group finds its own; the scheduler's
// budget of 8 gives a fan-out of 4. Rows 1..8 hold v = 0.
Task<std::shared_ptr<BatchScheduler<Io>>> fanoutScheduler(Io& io) {
    auto pool = co_await PgPool<Io>::create(
        io, CONNINFO, {.min_connections = 8, .max_connections = 8});
    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
    co_await bootstrapPg(batcher);
    co_await batcher->directQuery(
        "CREATE TABLE IF NOT EXISTS relais_test_fanout (id INT PRIMARY KEY, v INT NOT NULL)");
    co_await batcher->directQuery("TRUNCATE relais_test_fanout");
    co_await batcher->directQuery(
        "INSERT INTO relais_test_fanout SELECT g, 0 FROM generate_series(1, 8) g");
    co_return batcher;
}

// The leader goes direct; writes launched before it returns share one batch.
void launchLeader(std::shared_ptr<BatchScheduler<Io>> b, Outcome& lead) {
    fanoutWrite(std::move(b), lead, kEcho, 0, 0, kFree);
}

Task<void> settle(Io& io, std::span<Outcome> outs) {
    while (!std::ranges::all_of(outs, &Outcome::done)) co_await YieldAwaiter{io};
}

template<typename Body>
void runFanout(Body body) {
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);
    [](Io& io, bool& done, Body body) -> DetachedTask {
        auto batcher = co_await fanoutScheduler(io);
        co_await body(io, batcher);
        done = true;
    }(io, done, std::move(body));
    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

size_t distinctPids(std::span<const Outcome> outs) {
    std::vector<int> pids;
    for (const auto& o : outs) pids.push_back(o.pid);
    std::ranges::sort(pids);
    return static_cast<size_t>(std::ranges::unique(pids).begin() - pids.begin());
}

}  // namespace

TEST_CASE("Write fan-out: the derived fan-out is half the PG budget",
          "[io][batch][fanout]")
{
    Io io;
    auto b8 = std::make_shared<BatchScheduler<Io>>(io, nullptr, 8);
    auto b1 = std::make_shared<BatchScheduler<Io>>(io, nullptr, 1);
    auto b64 = std::make_shared<BatchScheduler<Io>>(io, nullptr, 64);
    CHECK(BatchBenchAccessor::writeFanout(*b8) == 4);
    CHECK(BatchBenchAccessor::writeFanout(*b1) == 1);
    CHECK(BatchBenchAccessor::writeFanout(*b64) == 16);
}

TEST_CASE("Write fan-out: writes on distinct rows commit on several connections",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 8> outs;
        launchLeader(b, lead);
        for (int i = 0; i < 8; ++i)
            fanoutWrite(b, outs[i], kSetValue, i + 1, 10 + i, rowOf());
        co_await settle(io, outs);
        for (int i = 0; i < 8; ++i) {
            REQUIRE_FALSE(outs[i].failed);
            CHECK(outs[i].value == 10 + i);
        }
        CHECK(distinctPids(outs) == 4);
        // Rows dealt round-robin: row i and row i + 4 share a connection.
        for (int i = 0; i < 4; ++i) CHECK(outs[i].pid == outs[i + 4].pid);
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: writes on one row keep submission order on one connection",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 10> outs;   // row 1 gets digits 1..5, interleaved
        launchLeader(b, lead);
        for (int d = 1; d <= 5; ++d) {
            fanoutWrite(b, outs[2 * (d - 1)], kAppendDigit, 1, d, rowOf());
            fanoutWrite(b, outs[2 * (d - 1) + 1], kAppendDigit, d + 1, d, rowOf());
        }
        co_await settle(io, outs);
        int expected = 0;
        for (int d = 1; d <= 5; ++d) {
            const auto& o = outs[2 * (d - 1)];
            REQUIRE_FALSE(o.failed);
            expected = expected * 10 + d;
            CHECK(o.value == expected);
            CHECK(o.pid == outs[0].pid);
        }
        CHECK(distinctPids(outs) > 1);
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: a write of unknown row keeps the batch on one connection",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 6> outs;
        launchLeader(b, lead);
        for (int i = 0; i < 5; ++i)
            fanoutWrite(b, outs[i], kSetValue, i + 1, 7, rowOf());
        fanoutWrite(b, outs[5], kSetValue, 6, 7, kOrdered);
        co_await settle(io, outs);
        for (const auto& o : outs) REQUIRE_FALSE(o.failed);
        CHECK(distinctPids(outs) == 1);
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: a key beyond the parameters keeps the batch on one connection",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 4> outs;
        launchLeader(b, lead);
        for (int i = 0; i < 3; ++i)
            fanoutWrite(b, outs[i], kSetValue, i + 1, 7, rowOf());
        fanoutWrite(b, outs[3], kSetValue, 4, 7, rowAt(tableTag(kFanoutTable), 2, 1));
        co_await settle(io, outs);
        for (const auto& o : outs) REQUIRE_FALSE(o.failed);
        CHECK(distinctPids(outs) == 1);
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: free writes are dealt across connections",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 8> outs;
        launchLeader(b, lead);
        for (int i = 0; i < 8; ++i)
            fanoutWrite(b, outs[i], kInsertRow, 100 + i, i, kFree);
        co_await settle(io, outs);
        for (const auto& o : outs) REQUIRE_FALSE(o.failed);
        CHECK(distinctPids(outs) == 4);
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: coalesced followers in several groups get their leader's result",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 6> outs;   // rows 1, 2, 3, three writes each
        launchLeader(b, lead);
        for (int i = 0; i < 6; ++i)
            fanoutWrite(b, outs[i], kSetValue, i % 3 + 1, 40 + i % 3, rowOf(),
                        WriteMode::Idempotent);
        co_await settle(io, outs);
        for (int i = 0; i < 6; ++i) {
            REQUIRE_FALSE(outs[i].failed);
            CHECK(outs[i].value == 40 + i % 3);
            CHECK(outs[i].pid == outs[i % 3].pid);
            CHECK(outs[i].coalesced == (i >= 3));
        }
        CHECK(distinctPids(outs) == 3);
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: a lost connection fails only its own group",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        // Row 1 is dealt to the first group: its first statement kills the
        // group's backend, and the write after it on the same row is lost too.
        std::array<Outcome, 4> outs;
        launchLeader(b, lead);
        fanoutWrite(b, outs[0], kKillOwnBackend, 1, 0, rowOf());
        fanoutWrite(b, outs[1], kSetValue, 2, 5, rowOf());
        fanoutWrite(b, outs[2], kSetValue, 3, 5, rowOf());
        fanoutWrite(b, outs[3], kSetValue, 1, 5, rowOf());
        co_await settle(io, outs);
        CHECK(outs[0].failed);
        CHECK(outs[3].failed);
        CHECK_FALSE(outs[1].failed);
        CHECK_FALSE(outs[2].failed);
        CHECK(outs[1].pid != outs[2].pid);
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: a failed statement fails only its own write",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 4> outs;
        launchLeader(b, lead);
        fanoutWrite(b, outs[0], kInsertRow, 1, 0, rowOf());   // row 1 exists
        fanoutWrite(b, outs[1], kSetValue, 2, 5, rowOf());
        fanoutWrite(b, outs[2], kSetValue, 1, 6, rowOf());   // same group, after
        fanoutWrite(b, outs[3], kSetValue, 3, 5, rowOf());
        co_await settle(io, outs);
        CHECK(outs[0].failed);
        CHECK_FALSE(outs[1].failed);
        CHECK_FALSE(outs[2].failed);
        CHECK(outs[2].value == 6);
        CHECK_FALSE(outs[3].failed);
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: a write whose commit fails fails alone",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        // A duplicate is refused at commit, after its INSERT succeeded.
        co_await b->directQuery("DROP TABLE IF EXISTS relais_test_deferred");
        co_await b->directQuery("CREATE TABLE relais_test_deferred "
                                "(id INT, UNIQUE (id) DEFERRABLE INITIALLY DEFERRED)");
        co_await b->directQuery("INSERT INTO relais_test_deferred VALUES (1)");
        static constexpr const char* kInsertDeferred =
            "INSERT INTO relais_test_deferred SELECT $1::int WHERE $2::int >= 0 "
            "RETURNING id, pg_backend_pid()";

        Outcome lead;   // sent alone, not pipelined
        std::array<Outcome, 3> outs;
        fanoutWrite(b, lead, kInsertDeferred, 1, 0, kOrdered);
        fanoutWrite(b, outs[0], kInsertDeferred, 10, 0, kOrdered);
        fanoutWrite(b, outs[1], kInsertDeferred, 1, 1, kOrdered);
        fanoutWrite(b, outs[2], kInsertDeferred, 11, 0, kOrdered);
        co_await settle(io, outs);
        co_await settle(io, std::span(&lead, 1));

        CHECK(lead.failed);
        CHECK_FALSE(outs[0].failed);
        CHECK(outs[0].value == 10);
        CHECK(outs[1].failed);
        CHECK_FALSE(outs[2].failed);
        CHECK(outs[2].value == 11);
        CHECK(outs[0].pid == outs[2].pid);   // one pipeline
        co_await b->directQuery("DROP TABLE relais_test_deferred");
    });
}

TEST_CASE("Write fan-out: a cancelled write in one group leaves the others whole",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 3> outs;
        launchLeader(b, lead);
        fanoutWrite(b, outs[0], kSetValue, 1, 8, rowOf());
        // Started by hand, then destroyed before the batch fires: its entry
        // is cancelled.
        auto dropped = std::make_unique<Task<PgWriteResult>>(
            b->submitPgWrite(kSetValue, PgParams::make(2, 8), WriteMode::Exclusive, rowOf()));
        dropped->await_suspend(std::noop_coroutine()).resume();
        fanoutWrite(b, outs[1], kSetValue, 3, 8, rowOf());
        fanoutWrite(b, outs[2], kSetValue, 4, 8, rowOf());
        dropped.reset();
        co_await settle(io, outs);
        for (const auto& o : outs) {
            REQUIRE_FALSE(o.failed);
            CHECK(o.value == 8);
        }
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: the wave and teardown accounting follow every group",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 8> outs;
        launchLeader(b, lead);
        CHECK(BatchBenchAccessor::writeUnits(*b) == 1);
        for (int i = 0; i < 8; ++i)
            fanoutWrite(b, outs[i], kSetValue, i + 1, 3, rowOf());
        while (BatchBenchAccessor::detachedInflight(*b) == 0) co_await YieldAwaiter{io};
        CHECK(BatchBenchAccessor::writeUnits(*b) == 4);
        CHECK_FALSE(b->quiescentForTeardown());
        co_await settle(io, outs);
        co_await settle(io, std::span(&lead, 1));
        while (BatchBenchAccessor::detachedInflight(*b) != 0) co_await YieldAwaiter{io};
        CHECK(b->quiescentForTeardown());
        CHECK(BatchBenchAccessor::writeUnits(*b) == 0);

        // Alone again: a write goes direct, not through a batch.
        Outcome alone;
        fanoutWrite(b, alone, kSetValue, 1, 4, rowOf());
        CHECK(BatchBenchAccessor::writeUnits(*b) == 1);
        co_await settle(io, std::span(&alone, 1));
        CHECK_FALSE(alone.failed);
        CHECK(BatchBenchAccessor::writeUnits(*b) == 0);
    });
}

TEST_CASE("Write fan-out: writes submitted during a wave wait for it to end",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 8> first;
        std::array<Outcome, 8> second;
        launchLeader(b, lead);
        for (int i = 0; i < 8; ++i)
            fanoutWrite(b, first[i], kSetValue, i + 1, 3, rowOf());
        while (BatchBenchAccessor::detachedInflight(*b) == 0) co_await YieldAwaiter{io};
        // The first wave is in flight: these accumulate behind it.
        for (int i = 0; i < 8; ++i)
            fanoutWrite(b, second[i], kAppendDigit, i + 1, 5, rowOf());
        size_t max_units = 0;
        while (!std::ranges::all_of(second, &Outcome::done)) {
            max_units = std::max(max_units, BatchBenchAccessor::writeUnits(*b));
            co_await YieldAwaiter{io};
        }
        CHECK(max_units <= BatchBenchAccessor::writeFanout(*b));
        for (const auto& o : first) {
            REQUIRE_FALSE(o.failed);
            CHECK(o.value == 3);
        }
        for (const auto& o : second) {
            REQUIRE_FALSE(o.failed);
            CHECK(o.value == 35);
        }
        co_await settle(io, std::span(&lead, 1));
    });
}

TEST_CASE("Write fan-out: a batch beyond the wave size departs over several waves",
          "[io][batch][fanout][integration]")
{
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::vector<Outcome> outs(600);
        launchLeader(b, lead);
        for (int i = 0; i < 600; ++i)
            fanoutWrite(b, outs[i], kEcho, i, 1, kFree);
        co_await settle(io, outs);
        for (int i = 0; i < 600; ++i) {
            REQUIRE_FALSE(outs[i].failed);
            CHECK(outs[i].value == i + 1);
        }
        co_await settle(io, std::span(&lead, 1));
        CHECK(BatchBenchAccessor::writeUnits(*b) == 0);
    });
}

TEST_CASE("Write fan-out: a fan-out of 1 keeps every batch on one connection",
          "[io][batch][fanout][integration]")
{
    BatchScheduler<Io>::write_fanout_override = 1;
    struct Reset { ~Reset() { BatchScheduler<Io>::write_fanout_override = 0; } } reset;
    runFanout([](Io& io, std::shared_ptr<BatchScheduler<Io>> b) -> Task<void> {
        Outcome lead;
        std::array<Outcome, 8> outs;
        launchLeader(b, lead);
        for (int i = 0; i < 8; ++i)
            fanoutWrite(b, outs[i], kSetValue, i + 1, 2, rowOf());
        co_await settle(io, outs);
        for (const auto& o : outs) REQUIRE_FALSE(o.failed);
        CHECK(distinctPids(outs) == 1);
        co_await settle(io, std::span(&lead, 1));
    });
}
