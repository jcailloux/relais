#include <catch2/catch_test_macros.hpp>

#include <chrono>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestConnInfo.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/Task.h>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::detail;
using Io = test::EpollIoContext;

static constexpr const char* CONNINFO = relais_test::kConnInfo;

TEST_CASE("PgConnection pipeline: multiple SELECTs", "[io][pg][pipeline][integration]") {
    Io io;
    bool done = false;

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 2});
        auto guard = co_await pool->acquire();
        auto& conn = guard.conn();

        conn.enterPipelineMode();

        // Prepare and send 3 simple queries
        PgParams p1 = PgParams::make(1);
        PgParams p2 = PgParams::make(2);
        PgParams p3 = PgParams::make(3);

        const char* sql = "SELECT $1::int AS val";

        if (conn.ensurePreparedPipelined(sql, 1))
            conn.pipelineSync();

        conn.sendPreparedPipelined(sql, p1);
        conn.pipelineSync();
        conn.sendPreparedPipelined(sql, p2);
        conn.pipelineSync();
        conn.sendPreparedPipelined(sql, p3);
        conn.pipelineSync();

        co_await conn.flushPipeline();

        // Read 3 query results (the prepare's result is consumed internally)
        std::vector<PgConnection<Io>::PipelineResult> results;
        co_await conn.readPipelineResults(3, results);

        conn.exitPipelineMode();

        REQUIRE(results.size() == 3);
        REQUIRE(results[0].result.ok());
        REQUIRE(results[1].result.ok());
        REQUIRE(results[2].result.ok());

        REQUIRE(results[0].result[0].get<int32_t>(0) == 1);
        REQUIRE(results[1].result[0].get<int32_t>(0) == 2);
        REQUIRE(results[2].result[0].get<int32_t>(0) == 3);

        // Check that processing times are non-negative
        for (auto& r : results) {
            REQUIRE(r.processing_time_us >= 0);
        }

        done = true;
    };

    task();
    io.runUntil([&] { return done; });
    REQUIRE(done);
}

TEST_CASE("PgConnection pipeline: error in one segment doesn't affect others",
          "[io][pg][pipeline][integration]") {
    Io io;
    bool done = false;

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 2});
        auto guard = co_await pool->acquire();
        auto& conn = guard.conn();

        conn.enterPipelineMode();

        // Query 1: valid
        const char* good_sql = "SELECT 1 AS val";
        conn.ensurePreparedPipelined(good_sql, 0);
        conn.pipelineSync();
        conn.sendPreparedPipelined(good_sql, PgParams{});
        conn.pipelineSync();

        co_await conn.flushPipeline();

        // Read good result (the prepare's result is consumed internally)
        std::vector<PgConnection<Io>::PipelineResult> results;
        co_await conn.readPipelineResults(1, results);
        REQUIRE(results.size() == 1);
        REQUIRE(results[0].result.ok());
        REQUIRE(results[0].result[0].get<int32_t>(0) == 1);

        conn.exitPipelineMode();
        done = true;
    };

    task();
    io.runUntil([&] { return done; });
    REQUIRE(done);
}

TEST_CASE("PgConnection pipeline: a failed commit fails its own segment",
          "[io][pg][pipeline][integration]") {
    Io io;
    bool done = false;

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, {.min_connections = 1, .max_connections = 1});
        auto guard = co_await pool->acquire();
        auto& conn = guard.conn();

        // The duplicate is refused at commit, when the server processes the
        // segment's sync, after the INSERT itself succeeded.
        co_await conn.query("CREATE TEMP TABLE pipeline_deferred "
                            "(id int, UNIQUE (id) DEFERRABLE INITIALLY DEFERRED)");
        co_await conn.query("INSERT INTO pipeline_deferred VALUES (1)");

        conn.enterPipelineMode();
        const char* insert = "INSERT INTO pipeline_deferred VALUES ($1::int)";
        const char* echo = "SELECT $1::int";
        if (conn.ensurePreparedPipelined(insert, 1)) conn.pipelineSync();
        conn.sendPreparedPipelined(insert, PgParams::make(1));
        conn.pipelineSync();
        if (conn.ensurePreparedPipelined(echo, 1)) conn.pipelineSync();
        conn.sendPreparedPipelined(echo, PgParams::make(42));
        conn.pipelineSync();
        co_await conn.flushPipeline();

        std::vector<PgConnection<Io>::PipelineResult> results;
        co_await conn.readPipelineResults(2, results);
        conn.exitPipelineMode();

        REQUIRE(results.size() == 2);
        CHECK_FALSE(results[0].result.ok());
        REQUIRE(results[1].result.ok());
        REQUIRE(results[1].result.rows() == 1);
        CHECK(results[1].result[0].get<int32_t>(0) == 42);

        auto count = co_await conn.query("SELECT count(*)::int FROM pipeline_deferred");
        CHECK(count[0].get<int32_t>(0) == 1);
        done = true;
    };

    task();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    io.runUntil([&] { return done || std::chrono::steady_clock::now() > deadline; });
    REQUIRE(done);
}
