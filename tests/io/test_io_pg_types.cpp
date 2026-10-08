#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/pg/PgResult.h>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <utility>
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

TEST_CASE("PgParam boolean", "[pg][params]") {
    REQUIRE(std::string(PgParam::boolean(true).data()) == "t");
    REQUIRE(std::string(PgParam::boolean(false).data()) == "f");
}

TEST_CASE("PgParam number writes the exact decimal text", "[pg][params]") {
    CHECK(std::string(PgParam::number(42).data()) == "42");
    CHECK(std::string(PgParam::number(9'000'000'000LL).data()) == "9000000000");
    CHECK(std::string(PgParam::number(std::numeric_limits<uint64_t>::max()).data())
          == "18446744073709551615");
    CHECK(std::string(PgParam::number(int16_t{-32768}).data()) == "-32768");
    CHECK(std::string(PgParam::number(3.14).data()) == "3.14");
    CHECK(std::string(PgParam::number(0.1234567891).data()) == "0.1234567891");
    CHECK(std::string(PgParam::number(1e-7).data()) == "1e-07");
    CHECK(std::string(PgParam::number(0.1f).data()) == "0.1");
    CHECK(std::string(PgParam::number(std::numeric_limits<double>::quiet_NaN()).data()) == "NaN");
    CHECK(std::string(PgParam::number(-std::numeric_limits<double>::infinity()).data())
          == "-Infinity");
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
    REQUIRE(std::string(vals[3]) == "3.14");
    REQUIRE(vals[4] == nullptr);  // null
}

TEST_CASE("PgParams::make binds every number type exactly", "[pg][params]") {
    auto params = PgParams::make(uint64_t{18'446'744'073'709'551'615u}, int16_t{-7},
                                 uint8_t{255}, int8_t{-128}, 1e20, 1.5f);
    auto vals = params.values();
    CHECK(std::string(vals[0]) == "18446744073709551615");
    CHECK(std::string(vals[1]) == "-7");
    CHECK(std::string(vals[2]) == "255");
    CHECK(std::string(vals[3]) == "-128");
    CHECK(std::string(vals[4]) == "1e+20");
    CHECK(std::string(vals[5]) == "1.5");
}

TEST_CASE("PgParams::make with optional", "[pg][params]") {
    std::optional<int64_t> present = 100;
    std::optional<int64_t> absent;
    std::optional<double> real = 0.1234567891;
    std::optional<float> absent_real;
    std::optional<std::string> text = "x";
    auto params = PgParams::make(present, absent, real, absent_real, text);
    REQUIRE(params.count() == 5);

    auto vals = params.values();
    REQUIRE(std::string(vals[0]) == "100");
    REQUIRE(vals[1] == nullptr);
    REQUIRE(std::string(vals[2]) == "0.1234567891");
    REQUIRE(vals[3] == nullptr);
    REQUIRE(std::string(vals[4]) == "x");
}

TEST_CASE("PgParams array literals", "[pg][params]") {
    auto text = [](const PgParam& p) { return std::string(p.data(), static_cast<size_t>(p.length())); };

    CHECK(text(PgParams::arrayLiteral(std::vector<double>{0.1234567891, 1e-7, -0.0}))
          == "{0.1234567891,1e-07,-0}");
    CHECK(text(PgParams::arrayLiteral(std::vector<double>{
              std::numeric_limits<double>::quiet_NaN(),
              std::numeric_limits<double>::infinity(),
              -std::numeric_limits<double>::infinity()}))
          == "{NaN,Infinity,-Infinity}");
    CHECK(text(PgParams::arrayLiteral(std::vector<int64_t>{})) == "{}");
    CHECK(text(PgParams::arrayLiteral(std::vector<uint64_t>{1, 18'446'744'073'709'551'615u}))
          == "{1,18446744073709551615}");
    CHECK(text(PgParams::arrayLiteral(std::vector<bool>{true, false})) == "{t,f}");
    CHECK(text(PgParams::arrayLiteral(std::vector<std::optional<int32_t>>{1, std::nullopt}))
          == "{1,NULL}");
    CHECK(text(PgParams::arrayLiteral(std::vector<std::string>{"a", "b c", "", "q\"x"}))
          == "{a,\"b c\",\"\",\"q\\\"x\"}");
}

// =============================================================================
// Accepted argument types (compile time)
// =============================================================================

namespace {

template<typename T>
constexpr bool bindable = requires(T&& v) { PgParams::make(std::forward<T>(v)); };

enum Plain { PlainValue };
enum class Scoped : int64_t { Value };

struct ConvertibleToString {
    operator std::string() const { return "x"; }
};
struct ConvertibleToDouble {
    operator double() const { return 1.0; }
};

}  // namespace

TEST_CASE("PgParams accepts the closed argument set", "[pg][params]") {
    STATIC_REQUIRE(bindable<int>);
    STATIC_REQUIRE(bindable<int16_t>);
    STATIC_REQUIRE(bindable<uint64_t>);
    STATIC_REQUIRE(bindable<float>);
    STATIC_REQUIRE(bindable<double&>);
    STATIC_REQUIRE(bindable<const double&>);
    STATIC_REQUIRE(bindable<bool>);
    STATIC_REQUIRE(bindable<const char*>);
    STATIC_REQUIRE(bindable<const char (&)[6]>);
    STATIC_REQUIRE(bindable<std::string>);
    STATIC_REQUIRE(bindable<const std::string&>);
    STATIC_REQUIRE(bindable<std::string_view>);
    STATIC_REQUIRE(bindable<std::nullptr_t>);
    STATIC_REQUIRE(bindable<PgParam>);
    STATIC_REQUIRE(bindable<std::optional<double>>);
    STATIC_REQUIRE(bindable<const std::optional<std::string>&>);
    STATIC_REQUIRE(bindable<std::vector<double>>);
    STATIC_REQUIRE(bindable<std::vector<std::optional<int32_t>>>);
}

TEST_CASE("PgParams refuses enums and convertible classes", "[pg][params]") {
    STATIC_REQUIRE_FALSE(bindable<Plain>);
    STATIC_REQUIRE_FALSE(bindable<Scoped>);
    STATIC_REQUIRE_FALSE(bindable<std::optional<Scoped>>);
    STATIC_REQUIRE_FALSE(bindable<std::vector<Plain>>);
    STATIC_REQUIRE_FALSE(bindable<ConvertibleToString>);
    STATIC_REQUIRE_FALSE(bindable<ConvertibleToDouble>);
    STATIC_REQUIRE_FALSE(bindable<char>);
    STATIC_REQUIRE_FALSE(bindable<std::optional<char>>);
    STATIC_REQUIRE_FALSE(bindable<std::vector<std::vector<int>>>);
    STATIC_REQUIRE_FALSE(bindable<std::optional<std::optional<int>>>);

    // The documented way to bind an enum.
    auto params = PgParams::make(std::to_underlying(Scoped::Value));
    CHECK(std::string(params.values()[0]) == "0");
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
