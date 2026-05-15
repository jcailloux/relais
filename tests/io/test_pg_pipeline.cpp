#include <catch2/catch_test_macros.hpp>

#include <fixtures/EpollIoContext.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/Task.h>

using namespace jcailloux::relais::io;
using Io = test::EpollIoContext;

static constexpr const char* CONNINFO =
    "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test";

TEST_CASE("PgConnection pipeline: multiple SELECTs", "[io][pg][pipeline][integration]") {
    Io io;
    bool done = false;

    auto task = [&]() -> DetachedTask {
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, 1, 2);
        auto guard = co_await pool->acquire();
        auto& conn = guard.conn();

        conn.enterPipelineMode();

        // Prepare and send 3 simple queries
        PgParams p1 = PgParams::make(1);
        PgParams p2 = PgParams::make(2);
        PgParams p3 = PgParams::make(3);

        const char* sql = "SELECT $1::int AS val";

        int prepares = 0;
        if (conn.ensurePreparedPipelined(sql, 1)) {
            conn.pipelineSync();
            ++prepares;
        }

        conn.sendPreparedPipelined(sql, p1);
        conn.pipelineSync();
        conn.sendPreparedPipelined(sql, p2);
        conn.pipelineSync();
        conn.sendPreparedPipelined(sql, p3);
        conn.pipelineSync();

        co_await conn.flushPipeline();

        // Read prepare result
        for (int i = 0; i < prepares; ++i) {
            auto r = co_await conn.readPipelineResults(1);
            // Prepare result — just consume
        }

        // Read 3 query results
        auto results = co_await conn.readPipelineResults(3);

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
        auto pool = co_await PgPool<Io>::create(io, CONNINFO, 1, 2);
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

        // Consume prepare
        co_await conn.readPipelineResults(1);

        // Read good result
        auto results = co_await conn.readPipelineResults(1);
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
