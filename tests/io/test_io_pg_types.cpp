#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/pg/PgResult.h>
#include <optional>
#include <string>
#include <vector>

using namespace jcailloux::relais::io;

// =============================================================================
// PgParam tests
// =============================================================================

TEST_CASE("PgParam null", "[pg][params]") {
    auto p = PgParam::null();
    REQUIRE(p.isNull());
    REQUIRE(p.data() == nullptr);
    REQUIRE(p.length() == 0);
}

TEST_CASE("PgParam text", "[pg][params]") {
    auto p = PgParam::text("hello");
    REQUIRE_FALSE(p.isNull());
    REQUIRE(std::string(p.data()) == "hello");
    REQUIRE(p.length() == 5);
    REQUIRE(p.format() == 0);
}

TEST_CASE("PgParam integer", "[pg][params]") {
    auto p = PgParam::integer(42);
    REQUIRE_FALSE(p.isNull());
    REQUIRE(std::string(p.data()) == "42");
}

TEST_CASE("PgParam bigint", "[pg][params]") {
    auto p = PgParam::bigint(9'000'000'000LL);
    REQUIRE(std::string(p.data()) == "9000000000");
}

TEST_CASE("PgParam boolean", "[pg][params]") {
    REQUIRE(std::string(PgParam::boolean(true).data()) == "t");
    REQUIRE(std::string(PgParam::boolean(false).data()) == "f");
}

TEST_CASE("PgParam floating", "[pg][params]") {
    auto p = PgParam::floating(3.14);
    REQUIRE_FALSE(p.isNull());
    // Just check it's a valid number string
    REQUIRE(std::stod(p.data()) == Catch::Approx(3.14));
}


TEST_CASE("PgParam fromOptional null", "[pg][params]") {
    std::optional<int32_t> empty;
    auto p = PgParam::fromOptional(empty);
    REQUIRE(p.isNull());
}

TEST_CASE("PgParam fromOptional value", "[pg][params]") {
    std::optional<int32_t> val = 99;
    auto p = PgParam::fromOptional(val);
    REQUIRE_FALSE(p.isNull());
    REQUIRE(std::string(p.data()) == "99");
}

// =============================================================================
// PgParams builder tests
// =============================================================================

TEST_CASE("PgParams::make with mixed types", "[pg][params]") {
    auto params = PgParams::make(42, "hello", true, 3.14, nullptr);
    REQUIRE(params.count() == 5);

    auto vals = params.values();
    REQUIRE(std::string(vals[0]) == "42");
    REQUIRE(std::string(vals[1]) == "hello");
    REQUIRE(std::string(vals[2]) == "t");
    // vals[3] is 3.14 as string
    REQUIRE(vals[4] == nullptr);  // null
}

TEST_CASE("PgParams::make with optional", "[pg][params]") {
    std::optional<int64_t> present = 100;
    std::optional<int64_t> absent;
    auto params = PgParams::make(present, absent);
    REQUIRE(params.count() == 2);

    auto vals = params.values();
    REQUIRE(std::string(vals[0]) == "100");
    REQUIRE(vals[1] == nullptr);
}

// =============================================================================
// PgError tests
// =============================================================================

TEST_CASE("PgError hierarchy", "[pg][error]") {
    REQUIRE_THROWS_AS(throw PgError("test"), std::runtime_error);
    REQUIRE_THROWS_AS(throw PgNoRows(), PgError);
    REQUIRE_THROWS_AS(throw PgNoRows("SELECT 1"), PgError);
    REQUIRE_THROWS_AS(throw PgConnectionError("conn lost"), PgError);
}

// =============================================================================
// PgResult with null PGresult (no DB needed)
// =============================================================================

TEST_CASE("PgResult default is empty", "[pg][result]") {
    PgResult r;
    REQUIRE_FALSE(r.valid());
    REQUIRE(r.empty());
    REQUIRE(r.rows() == 0);
}
