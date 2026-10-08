#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/pg/PgConnection.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgError.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

#include <bit>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::test;
using jcailloux::relais::detail::parseNumber;

// =============================================================================
// Connection string from environment (same as relais tests)
// =============================================================================

static const char* getConnInfo() {
    return "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test";
}

// =============================================================================
// PgConnection tests
// =============================================================================

TEST_CASE("PgConnection async connect", "[pg][integration]") {
    EpollIoContext io;
    auto conn = runTask(io, PgConnection<EpollIoContext>::connect(io, getConnInfo()));
    REQUIRE(conn.connected());
}

TEST_CASE("PgConnection simple query", "[pg][integration]") {
    EpollIoContext io;

    auto result = runTask(io, [](EpollIoContext& io) -> Task<PgResult> {
        auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
        co_return co_await conn.query("SELECT 1 AS num, 'hello' AS greeting");
    }(io));

    REQUIRE(result.ok());
    REQUIRE(result.rows() == 1);
    REQUIRE(result[0].get<int32_t>(0) == 1);
    REQUIRE(result[0].get<std::string>(1) == "hello");
}

TEST_CASE("PgConnection parameterized query", "[pg][integration]") {
    EpollIoContext io;

    auto result = runTask(io, [](EpollIoContext& io) -> Task<PgResult> {
        auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
        auto params = PgParams::make(42, "world");
        co_return co_await conn.queryParams(
            "SELECT $1::int AS num, $2::text AS txt", params);
    }(io));

    REQUIRE(result.ok());
    REQUIRE(result[0].get<int32_t>(0) == 42);
    REQUIRE(result[0].get<std::string>(1) == "world");
}

TEST_CASE("PgConnection floating-point params round-trip bit for bit", "[pg][integration]") {
    constexpr double inf = std::numeric_limits<double>::infinity();
    const std::vector<double> doubles{0.1234567891, 1e-7, 1e300, 5e-324, DBL_MAX, -0.0, inf, -inf};
    const std::vector<float> floats{0.1f, FLT_MIN, FLT_MAX, -0.0f,
                                    std::numeric_limits<float>::denorm_min()};

    struct Read {
        std::vector<double> doubles;
        std::vector<std::string> floats;
        std::vector<double> array;
        double nan = 0;
    };
    EpollIoContext io;

    auto read = runTask(io, [&](EpollIoContext& io) -> Task<Read> {
        auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
        Read out;
        for (double d : doubles) {
            auto params = PgParams::make(d);
            auto r = co_await conn.queryParams("SELECT $1::float8", params);
            out.doubles.push_back(r[0].get<double>(0));
        }
        for (float f : floats) {
            auto params = PgParams::make(f);
            auto r = co_await conn.queryParams("SELECT $1::float4", params);
            out.floats.push_back(r[0].get<std::string>(0));
        }
        auto params = PgParams::make(doubles, std::numeric_limits<double>::quiet_NaN());
        auto r = co_await conn.queryParams("SELECT $1::float8[], $2::float8", params);
        out.array = r[0].get<std::vector<double>>(0);
        out.nan = r[0].get<double>(1);
        co_return out;
    }(io));

    REQUIRE(read.doubles.size() == doubles.size());
    REQUIRE(read.array.size() == doubles.size());
    for (size_t i = 0; i < doubles.size(); ++i) {
        INFO("double " << i);
        CHECK(std::bit_cast<uint64_t>(read.doubles[i]) == std::bit_cast<uint64_t>(doubles[i]));
        CHECK(std::bit_cast<uint64_t>(read.array[i]) == std::bit_cast<uint64_t>(doubles[i]));
    }
    REQUIRE(read.floats.size() == floats.size());
    for (size_t i = 0; i < floats.size(); ++i) {
        INFO("float " << i << ": " << read.floats[i]);
        const auto f = parseNumber<float>(read.floats[i]);
        REQUIRE(f.has_value());
        CHECK(std::bit_cast<uint32_t>(*f) == std::bit_cast<uint32_t>(floats[i]));
    }
    CHECK(std::isnan(read.nan));
}

// =============================================================================
// CRUD on relais_test_items
// =============================================================================

TEST_CASE("PgConnection CRUD operations", "[pg][integration]") {
    EpollIoContext io;

    // Diagnostic: verify connection target
    auto diag = runTask(io, [](EpollIoContext& io) -> Task<PgResult> {
        auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
        co_return co_await conn.query(
            "SELECT current_database(), current_user, current_schema(), "
            "EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='relais_test_items')");
    }(io));
    WARN("conninfo: " << getConnInfo());
    WARN("database: " << diag[0].get<std::string>(0));
    WARN("user: " << diag[0].get<std::string>(1));
    WARN("schema: " << diag[0].get<std::string>(2));
    WARN("table_exists: " << diag[0].get<bool>(3));
    REQUIRE(diag[0].get<bool>(3));

    // Clean table
    runTask(io, [](EpollIoContext& io) -> Task<void> {
        auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
        co_await conn.query("DELETE FROM relais_test_items");
    }(io));

    SECTION("INSERT and SELECT") {
        auto result = runTask(io, [](EpollIoContext& io) -> Task<PgResult> {
            auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
            auto params = PgParams::make("test_item", 42, true);
            co_return co_await conn.queryParams(
                "INSERT INTO relais_test_items (name, value, is_active) "
                "VALUES ($1, $2, $3) RETURNING id, name, value, is_active",
                params);
        }(io));

        REQUIRE(result.ok());
        REQUIRE(result.rows() == 1);
        auto id = result[0].get<int64_t>(0);
        REQUIRE(id > 0);
        REQUIRE(result[0].get<std::string>(1) == "test_item");
        REQUIRE(result[0].get<int32_t>(2) == 42);
        REQUIRE(result[0].get<bool>(3) == true);

        // SELECT back
        auto select = runTask(io, [id](EpollIoContext& io) -> Task<PgResult> {
            auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
            auto params = PgParams::make(id);
            co_return co_await conn.queryParams(
                "SELECT id, name, value, is_active FROM relais_test_items WHERE id = $1",
                params);
        }(io));

        REQUIRE(select.ok());
        REQUIRE(select.rows() == 1);
        REQUIRE(select[0].get<std::string>(1) == "test_item");
    }

    SECTION("UPDATE") {
        // Insert first
        auto insert = runTask(io, [](EpollIoContext& io) -> Task<PgResult> {
            auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
            auto params = PgParams::make("to_update", 1, true);
            co_return co_await conn.queryParams(
                "INSERT INTO relais_test_items (name, value, is_active) "
                "VALUES ($1, $2, $3) RETURNING id",
                params);
        }(io));
        auto id = insert[0].get<int64_t>(0);

        // Update
        auto affected = runTask(io, [id](EpollIoContext& io) -> Task<int> {
            auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
            auto params = PgParams::make("updated", false, id);
            co_return co_await conn.execute(
                "UPDATE relais_test_items SET name = $1, is_active = $2 WHERE id = $3",
                params);
        }(io));

        REQUIRE(affected == 1);
    }

    SECTION("DELETE") {
        // Insert first
        auto insert = runTask(io, [](EpollIoContext& io) -> Task<PgResult> {
            auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
            auto params = PgParams::make("to_delete", 0, true);
            co_return co_await conn.queryParams(
                "INSERT INTO relais_test_items (name, value, is_active) "
                "VALUES ($1, $2, $3) RETURNING id",
                params);
        }(io));
        auto id = insert[0].get<int64_t>(0);

        // Delete
        auto affected = runTask(io, [id](EpollIoContext& io) -> Task<int> {
            auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
            auto params = PgParams::make(id);
            co_return co_await conn.execute(
                "DELETE FROM relais_test_items WHERE id = $1", params);
        }(io));

        REQUIRE(affected == 1);
    }

    SECTION("NULL handling") {
        auto result = runTask(io, [](EpollIoContext& io) -> Task<PgResult> {
            auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
            auto params = PgParams::make("null_desc", 0, true, nullptr);
            co_return co_await conn.queryParams(
                "INSERT INTO relais_test_items (name, value, is_active, description) "
                "VALUES ($1, $2, $3, $4) RETURNING id, description",
                params);
        }(io));

        REQUIRE(result.ok());
        REQUIRE(result[0].isNull(1));  // description column is NULL
        auto opt = result[0].getOpt<std::string>(1);
        REQUIRE_FALSE(opt.has_value());
    }
}

// =============================================================================
// Error handling
// =============================================================================

TEST_CASE("PgConnection query error throws PgError", "[pg][integration]") {
    EpollIoContext io;

    REQUIRE_THROWS_AS(
        runTask(io, [](EpollIoContext& io) -> Task<PgResult> {
            auto conn = co_await PgConnection<EpollIoContext>::connect(io, getConnInfo());
            co_return co_await conn.query("SELECT * FROM nonexistent_table_xyz");
        }(io)),
        PgError
    );
}

TEST_CASE("PgConnection bad conninfo throws PgConnectionError", "[pg][integration]") {
    EpollIoContext io;

    // Use an invalid dbname on a valid host for fast failure
    REQUIRE_THROWS_AS(
        runTask(io, [](EpollIoContext& io) -> Task<PgConnection<EpollIoContext>> {
            co_return co_await PgConnection<EpollIoContext>::connect(
                io, "host=localhost port=5432 dbname=nonexistent_db_xyz_relais "
                    "user=nonexistent_user_xyz connect_timeout=2");
        }(io)),
        PgConnectionError
    );
}

// =============================================================================
// PgPool + PgClient
// =============================================================================

TEST_CASE("PgPool create and acquire", "[pg][integration][pool]") {
    EpollIoContext io;

    auto result = runTask(io, [](EpollIoContext& io) -> Task<int32_t> {
        auto pool = co_await PgPool<EpollIoContext>::create(
            io, getConnInfo(), {.min_connections = 2, .max_connections = 4});

        {
            auto guard = co_await pool->acquire();
            auto r = co_await guard.conn().query("SELECT 42 AS answer");
            co_return r[0].get<int32_t>(0);
        }
    }(io));

    REQUIRE(result == 42);
}

TEST_CASE("PgClient query convenience", "[pg][integration][pool]") {
    EpollIoContext io;

    auto result = runTask(io, [](EpollIoContext& io) -> Task<std::string> {
        auto pool = co_await PgPool<EpollIoContext>::create(io, getConnInfo(), {.min_connections = 1, .max_connections = 4});
        PgClient<EpollIoContext> client(pool);

        auto r = co_await client.queryArgs("SELECT $1::text || ' ' || $2::text AS msg",
            "hello", "relais");
        co_return r[0].get<std::string>(0);
    }(io));

    REQUIRE(result == "hello relais");
}
