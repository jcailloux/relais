/**
 * bench_io_pg.cpp
 *
 * Performance benchmarks for the PostgreSQL I/O layer.
 * Measures raw PgConnection/PgPool/PgClient query latency,
 * independent of the relais cache hierarchy.
 *
 * Run with:
 *   ./bench_io_pg                       # all benchmarks
 *   ./bench_io_pg "[select]"            # SELECT benchmarks only
 *   ./bench_io_pg "[pool]"              # pool benchmarks only
 *   BENCH_SAMPLES=1000 ./bench_io_pg    # custom sample count
 */

#include <catch2/catch_test_macros.hpp>

#include "BenchEngine.h"

#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgConnection.h>
#include <jcailloux/relais/io/pg/PgResult.h>
#include <jcailloux/relais/io/pg/PgParams.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::test;
using namespace relais_bench;

static const char* getConnInfo() {
    return "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test";
}


// #############################################################################
//
//  1. Raw connection SELECT (no pool)
//
// #############################################################################

TEST_CASE("Benchmark - PG raw SELECT", "[benchmark][pg][select]")
{
    EpollIoContext io;

    auto results = runTask(io, [](EpollIoContext& io) -> Task<std::vector<BenchResult>> {
        auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());

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

    WARN(formatTable("PG raw SELECT (single connection)", results));
}


// #############################################################################
//
//  2. Pool acquire + query + release
//
// #############################################################################

TEST_CASE("Benchmark - PG pool query", "[benchmark][pg][pool]")
{
    EpollIoContext io;

    auto results = runTask(io, [](EpollIoContext& io) -> Task<std::vector<BenchResult>> {
        auto pool = co_await PgPool<EpollIoContext>::create(io, getConnInfo(), 2, 4);
        PgClient<EpollIoContext> client(pool);

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

    WARN(formatTable("PG pool query (acquire+query+release)", results));
}


// #############################################################################
//
//  3. Real table queries
//
// #############################################################################

TEST_CASE("Benchmark - PG table queries", "[benchmark][pg][table]")
{
    EpollIoContext io;

    auto results = runTask(io, [](EpollIoContext& io) -> Task<std::vector<BenchResult>> {
        auto pool = co_await PgPool<EpollIoContext>::create(io, getConnInfo(), 2, 4);
        PgClient<EpollIoContext> client(pool);

        // Insert test rows
        for (int i = 0; i < 20; ++i) {
            co_await client.queryArgs(
                "INSERT INTO relais_test_items (name, value, is_active) "
                "VALUES ($1, $2, true)",
                "bench_pg_" + std::to_string(i), i * 10);
        }

        // Get one ID for single-row lookups
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

        // Cleanup
        co_await client.query(
            "DELETE FROM relais_test_items WHERE name LIKE 'bench_pg_%'");

        co_return results;
    }(io));

    WARN(formatTable("PG table queries", results));
}


// #############################################################################
//
//  4. INSERT + DELETE round-trip
//
// #############################################################################

TEST_CASE("Benchmark - PG write operations", "[benchmark][pg][write]")
{
    EpollIoContext io;

    auto results = runTask(io, [](EpollIoContext& io) -> Task<std::vector<BenchResult>> {
        auto pool = co_await PgPool<EpollIoContext>::create(io, getConnInfo(), 2, 4);
        PgClient<EpollIoContext> client(pool);

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

        // Cleanup leftovers from INSERT benchmark
        co_await client.query(
            "DELETE FROM relais_test_items WHERE name LIKE 'bench_ins_%'");

        co_return results;
    }(io));

    WARN(formatTable("PG write operations", results));
}
