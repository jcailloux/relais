#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/detail/NumberText.h>

#include <bit>
#include <cfloat>
#include <clocale>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>

using jcailloux::relais::detail::appendNumber;
using jcailloux::relais::detail::kMaxNumberChars;
using jcailloux::relais::detail::parseNumber;
using jcailloux::relais::detail::TextNumber;
using jcailloux::relais::detail::toText;

namespace {

/// Write `v`, parse it back, and require the very same bit pattern.
template<typename T>
void requireBitExactRoundTrip(T v) {
    const std::string text = toText(v);
    INFO("text: " << text);
    REQUIRE(text.size() <= kMaxNumberChars<T>);
    const auto back = parseNumber<T>(text);
    REQUIRE(back.has_value());
    if constexpr (std::is_floating_point_v<T>) {
        using Bits = std::conditional_t<sizeof(T) == 4, std::uint32_t, std::uint64_t>;
        REQUIRE(std::bit_cast<Bits>(*back) == std::bit_cast<Bits>(v));
    } else {
        REQUIRE(*back == v);
    }
}

struct LocaleGuard {
    ~LocaleGuard() { std::setlocale(LC_NUMERIC, "C"); }
};

}  // namespace

// =============================================================================
// Concept
// =============================================================================

TEST_CASE("TextNumber covers numbers, not bool nor characters", "[number-text]") {
    STATIC_REQUIRE(TextNumber<int>);
    STATIC_REQUIRE(TextNumber<std::int8_t>);
    STATIC_REQUIRE(TextNumber<std::uint8_t>);
    STATIC_REQUIRE(TextNumber<std::int16_t>);
    STATIC_REQUIRE(TextNumber<std::uint64_t>);
    STATIC_REQUIRE(TextNumber<float>);
    STATIC_REQUIRE(TextNumber<double>);
    STATIC_REQUIRE(TextNumber<const double>);

    STATIC_REQUIRE_FALSE(TextNumber<bool>);
    STATIC_REQUIRE_FALSE(TextNumber<const bool>);
    STATIC_REQUIRE_FALSE(TextNumber<char>);
    STATIC_REQUIRE_FALSE(TextNumber<wchar_t>);
    STATIC_REQUIRE_FALSE(TextNumber<char8_t>);
    STATIC_REQUIRE_FALSE(TextNumber<char16_t>);
    STATIC_REQUIRE_FALSE(TextNumber<char32_t>);
    STATIC_REQUIRE_FALSE(TextNumber<std::string>);

    enum Plain { A };
    enum class Scoped { B };
    STATIC_REQUIRE_FALSE(TextNumber<Plain>);
    STATIC_REQUIRE_FALSE(TextNumber<Scoped>);
}

TEST_CASE("kMaxNumberChars is the exact worst case", "[number-text]") {
    STATIC_REQUIRE(kMaxNumberChars<std::int8_t> == 4);
    STATIC_REQUIRE(kMaxNumberChars<std::uint8_t> == 3);
    STATIC_REQUIRE(kMaxNumberChars<std::int32_t> == 11);
    STATIC_REQUIRE(kMaxNumberChars<std::int64_t> == 20);
    STATIC_REQUIRE(kMaxNumberChars<std::uint64_t> == 20);
    STATIC_REQUIRE(kMaxNumberChars<float> == 15);
    STATIC_REQUIRE(kMaxNumberChars<double> == 24);

    CHECK(toText(std::numeric_limits<std::int64_t>::min()).size() == 20);
    CHECK(toText(-2.2250738585072014e-308).size() == 24);
}

// =============================================================================
// Floating point
// =============================================================================

TEST_CASE("double is written as the shortest exact text", "[number-text]") {
    CHECK(toText(0.1234567891) == "0.1234567891");
    CHECK(toText(0.1) == "0.1");
    CHECK(toText(48.8566) == "48.8566");
    CHECK(toText(1e-7) == "1e-07");
    CHECK(toText(1e20) == "1e+20");
    CHECK(toText(1e300) == "1e+300");
    CHECK(toText(5e-324) == "5e-324");
    CHECK(toText(DBL_MAX) == "1.7976931348623157e+308");
    CHECK(toText(-0.0) == "-0");
    CHECK(toText(3.0) == "3");
}

TEST_CASE("double round-trips bit for bit", "[number-text]") {
    for (double v : {0.1234567891, 0.1, 1e-7, 1e300, 5e-324, DBL_MIN, DBL_MAX,
                     -DBL_MAX, -0.0, 0.0, 1.0 / 3.0, -2.2250738585072014e-308}) {
        requireBitExactRoundTrip(v);
    }
}

TEST_CASE("float is written from its own precision, not promoted", "[number-text]") {
    CHECK(toText(0.1f) == "0.1");
    CHECK(toText(FLT_MIN) == "1.1754944e-38");
    CHECK(toText(FLT_MAX) == "3.4028235e+38");

    for (float v : {0.1f, FLT_MIN, FLT_MAX, -0.0f, 1.0f / 3.0f,
                    std::numeric_limits<float>::denorm_min()}) {
        requireBitExactRoundTrip(v);
    }
}

TEST_CASE("non-finite values use PostgreSQL's spelling", "[number-text]") {
    constexpr double inf = std::numeric_limits<double>::infinity();
    constexpr double nan = std::numeric_limits<double>::quiet_NaN();

    CHECK(toText(nan) == "NaN");
    CHECK(toText(-nan) == "NaN");
    CHECK(toText(std::numeric_limits<float>::quiet_NaN()) == "NaN");
    CHECK(toText(inf) == "Infinity");
    CHECK(toText(-inf) == "-Infinity");
    CHECK(toText(-std::numeric_limits<float>::infinity()) == "-Infinity");

    SECTION("and parse back") {
        CHECK(std::isnan(parseNumber<double>("NaN").value()));
        CHECK(std::isnan(parseNumber<float>("NaN").value()));
        CHECK(parseNumber<double>("Infinity") == inf);
        CHECK(parseNumber<double>("-Infinity") == -inf);
        CHECK(parseNumber<float>("-Infinity") == -std::numeric_limits<float>::infinity());
    }
}

// =============================================================================
// Integers
// =============================================================================

TEST_CASE("integers round-trip at their bounds", "[number-text]") {
    using std::numeric_limits;

    CHECK(toText(std::int8_t{-128}) == "-128");
    CHECK(toText(std::uint8_t{255}) == "255");
    CHECK(toText(numeric_limits<std::int64_t>::min()) == "-9223372036854775808");
    CHECK(toText(numeric_limits<std::uint64_t>::max()) == "18446744073709551615");

    requireBitExactRoundTrip(numeric_limits<std::int8_t>::min());
    requireBitExactRoundTrip(numeric_limits<std::int8_t>::max());
    requireBitExactRoundTrip(numeric_limits<std::uint8_t>::max());
    requireBitExactRoundTrip(numeric_limits<std::int16_t>::min());
    requireBitExactRoundTrip(numeric_limits<std::int16_t>::max());
    requireBitExactRoundTrip(numeric_limits<std::int32_t>::min());
    requireBitExactRoundTrip(numeric_limits<std::int32_t>::max());
    requireBitExactRoundTrip(numeric_limits<std::int64_t>::min());
    requireBitExactRoundTrip(numeric_limits<std::int64_t>::max());
    requireBitExactRoundTrip(numeric_limits<std::uint64_t>::max());
    requireBitExactRoundTrip(std::int64_t{0});
}

// =============================================================================
// appendNumber / parseNumber contracts
// =============================================================================

TEST_CASE("appendNumber appends after existing content", "[number-text]") {
    std::string out = "{";
    appendNumber(out, 1.5);
    out += ',';
    appendNumber(out, std::int64_t{-7});
    CHECK(out == "{1.5,-7");
}

TEST_CASE("parseNumber is strict", "[number-text]") {
    CHECK_FALSE(parseNumber<double>("1.5x").has_value());
    CHECK_FALSE(parseNumber<double>("").has_value());
    CHECK_FALSE(parseNumber<double>(" 1").has_value());
    CHECK_FALSE(parseNumber<double>("1e400").has_value());
    CHECK_FALSE(parseNumber<std::int64_t>("12.5").has_value());
    CHECK_FALSE(parseNumber<std::int64_t>("").has_value());
    CHECK_FALSE(parseNumber<std::int8_t>("128").has_value());
    CHECK_FALSE(parseNumber<std::uint64_t>("-1").has_value());
    CHECK_FALSE(parseNumber<std::int32_t>("1,5").has_value());

    CHECK(parseNumber<std::int64_t>("-42") == -42);
    CHECK(parseNumber<double>("12.5") == 12.5);
}

// =============================================================================
// Locale independence
// =============================================================================

TEST_CASE("number text ignores LC_NUMERIC", "[number-text]") {
    LocaleGuard guard;
    if (!std::setlocale(LC_NUMERIC, "fr_FR.UTF-8") && !std::setlocale(LC_NUMERIC, "fr_FR.utf8")) {
        SKIP("fr_FR locale not installed");
    }
    // The locale is really active: printf-style formatting now uses a comma.
    REQUIRE(std::string(std::localeconv()->decimal_point) == ",");

    CHECK(toText(0.5) == "0.5");
    CHECK(toText(0.1234567891) == "0.1234567891");
    CHECK(parseNumber<double>("0.5") == 0.5);
    CHECK_FALSE(parseNumber<double>("0,5").has_value());
}
