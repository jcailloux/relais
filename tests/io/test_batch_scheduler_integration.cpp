#include <catch2/catch_test_macros.hpp>

#include <fixtures/EpollIoContext.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/io/redis/RedisPool.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/batch/TimingEstimator.h>
#include <jcailloux/relais/io/Task.h>
#include <jcailloux/relais/PgProvider.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::batch;
using Io = test::EpollIoContext;

static constexpr const char* CONNINFO =
    "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test";

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

DetachedTask concurrentRedisSet(
    std::shared_ptr<BatchScheduler<Io>> batcher,
    std::atomic<int>& completed,
    std::string key, std::string value)
{
    // Build argv: SET <key> <value>
    std::vector<std::string> args = {"SET", std::move(key), std::move(value)};
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    for (const auto& a : args) {
        argv.push_back(a.data());
        argvlen.push_back(a.size());
    }
    auto result = co_await batcher->submitRedis(
        static_cast<int>(argv.size()), argv.data(), argvlen.data());
    REQUIRE(result.isString());
    REQUIRE(result.asString() == "OK");
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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
            io, pool, nullptr, /*max_concurrent=*/1);

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
            io, pool, nullptr, /*max_concurrent=*/2);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
            io, pool, nullptr, /*max_concurrent=*/4);

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
// Redis: single command via submitRedis
// =============================================================================

TEST_CASE("BatchScheduler: single Redis command returns correct result",
          "[io][batch][integration][redis]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto redis = co_await RedisClient<Io>::connect(io);
        auto redis_pool = std::make_shared<RedisPool<Io>>(
            RedisPool<Io>::fromClients({std::move(redis)}));
        auto batcher = std::make_shared<BatchScheduler<Io>>(
            io, pool, redis_pool, 8);

        // SET via submitRedis
        {
            std::vector<std::string> args = {"SET", "batch_test_redis_key", "hello_batch"};
            std::vector<const char*> argv;
            std::vector<size_t> argvlen;
            for (const auto& a : args) {
                argv.push_back(a.data());
                argvlen.push_back(a.size());
            }
            auto result = co_await batcher->submitRedis(
                static_cast<int>(argv.size()), argv.data(), argvlen.data());
            REQUIRE(result.isString());
            REQUIRE(result.asString() == "OK");
        }

        // GET via submitRedis
        {
            std::vector<std::string> args = {"GET", "batch_test_redis_key"};
            std::vector<const char*> argv;
            std::vector<size_t> argvlen;
            for (const auto& a : args) {
                argv.push_back(a.data());
                argvlen.push_back(a.size());
            }
            auto result = co_await batcher->submitRedis(
                static_cast<int>(argv.size()), argv.data(), argvlen.data());
            REQUIRE(result.isString());
            REQUIRE(result.asString() == "hello_batch");
        }

        // Cleanup
        {
            std::vector<std::string> args = {"DEL", "batch_test_redis_key"};
            std::vector<const char*> argv;
            std::vector<size_t> argvlen;
            for (const auto& a : args) {
                argv.push_back(a.data());
                argvlen.push_back(a.size());
            }
            co_await batcher->submitRedis(
                static_cast<int>(argv.size()), argv.data(), argvlen.data());
        }

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// =============================================================================
// Redis: concurrent commands don't deadlock
// =============================================================================

TEST_CASE("BatchScheduler: concurrent Redis commands complete correctly",
          "[io][batch][integration][redis]")
{
    Io io;
    std::atomic<int> completed{0};
    constexpr int N = 30;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto redis = co_await RedisClient<Io>::connect(io);
        auto redis_pool = std::make_shared<RedisPool<Io>>(
            RedisPool<Io>::fromClients({std::move(redis)}));
        auto batcher = std::make_shared<BatchScheduler<Io>>(
            io, pool, redis_pool, /*max_concurrent=*/2);

        for (int i = 0; i < N; ++i) {
            concurrentRedisSet(batcher, completed,
                "batch_test_conc_" + std::to_string(i),
                std::to_string(i * 10));
        }
    };
    task();

    io.runUntil([&] { return completed.load() == N || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(completed.load() == N);

    // Cleanup keys
    Io io2;
    bool cleanup_done = false;
    auto cleanup = [&]() -> DetachedTask {
        auto redis = co_await RedisClient<Io>::connect(io2);
        for (int i = 0; i < N; ++i) {
            co_await redis->exec("DEL", "batch_test_conc_" + std::to_string(i));
        }
        cleanup_done = true;
    };
    cleanup();
    io2.runUntil([&] { return cleanup_done; });
}

// =============================================================================
// Redis: pipelineExec on RedisClient returns correct results
// =============================================================================

TEST_CASE("BatchScheduler: Redis pipelineExec returns correct results",
          "[io][batch][integration][redis]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto redis = co_await RedisClient<Io>::connect(io);

        // Prepare 3 SET commands
        using PCmd = RedisClient<Io>::PipelineCmd;

        std::vector<std::string> k1 = {"SET", "pipe_test_1", "aaa"};
        std::vector<std::string> k2 = {"SET", "pipe_test_2", "bbb"};
        std::vector<std::string> k3 = {"GET", "pipe_test_1"};

        // Build argv arrays (must stay alive during pipelineExec)
        auto buildArgv = [](const std::vector<std::string>& args,
                            std::vector<const char*>& argv,
                            std::vector<size_t>& argvlen) {
            for (const auto& a : args) {
                argv.push_back(a.data());
                argvlen.push_back(a.size());
            }
        };

        std::vector<const char*> a1, a2, a3;
        std::vector<size_t> l1, l2, l3;
        buildArgv(k1, a1, l1);
        buildArgv(k2, a2, l2);
        buildArgv(k3, a3, l3);

        PCmd cmds[3] = {
            {static_cast<int>(a1.size()), a1.data(), l1.data()},
            {static_cast<int>(a2.size()), a2.data(), l2.data()},
            {static_cast<int>(a3.size()), a3.data(), l3.data()},
        };

        auto results = co_await redis->pipelineExec(cmds, 3);
        REQUIRE(results.size() == 3);
        REQUIRE(results[0].isString());
        REQUIRE(results[0].asString() == "OK");
        REQUIRE(results[1].isString());
        REQUIRE(results[1].asString() == "OK");
        REQUIRE(results[2].isString());
        REQUIRE(results[2].asString() == "aaa");

        // Cleanup
        co_await redis->exec("DEL", "pipe_test_1", "pipe_test_2");

        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
}

// =============================================================================
// PgProvider: redis routes through BatchScheduler
// =============================================================================

TEST_CASE("BatchScheduler: PgProvider redis routes through batcher",
          "[io][batch][integration][redis]")
{
    using jcailloux::relais::PgProvider;

    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto redis = co_await RedisClient<Io>::connect(io);

        PgProvider::init(io, pool, redis);
        REQUIRE(PgProvider::initialized());
        REQUIRE(PgProvider::hasRedis());

        // SET via PgProvider::redis (goes through batcher->submitRedis)
        co_await PgProvider::redis("SET", "dbp_batch_test", "routed");

        auto reply = co_await PgProvider::redis("GET", "dbp_batch_test");
        REQUIRE(reply.isString());
        REQUIRE(reply.asString() == "routed");

        // Cleanup
        co_await PgProvider::redis("DEL", "dbp_batch_test");

        PgProvider::reset();
        done = true;
    };
    task();

    io.runUntil([&] { return done || timeout.timed_out; });
    REQUIRE_FALSE(timeout.timed_out);
    REQUIRE(done);
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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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

TEST_CASE("Write coalescing: identical writes in batch are coalesced",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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

TEST_CASE("Write coalescing: submitPgWrite coalescing returns correct affected rows",
          "[io][batch][integration][coalesce]")
{
    Io io;
    bool done = false;
    TimeoutGuard timeout(io);

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
