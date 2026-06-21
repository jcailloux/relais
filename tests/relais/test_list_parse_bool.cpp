/**
 * test_list_parse_bool.cpp
 * HTTP query parsing of boolean filter values (parse::toBool).
 *
 * Before this, `parseValue<bool>` fell through to the catch-all nullopt: a
 * `filterable` bool field produced a filter that could never activate via HTTP,
 * silently serving an unfiltered list. parse::toBool now accepts the standard
 * HTTP / HTML-form boolean conventions (case-insensitive), feeding both the
 * scalar EQ path (parseValue) and the IN-list path (parseInElement).
 *
 * Pure unit test (no DB / Redis): hand-built descriptors on the committed
 * TestArticleEntity fixture, independent of the generator work.
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "fixtures/generated/TestArticleEntity.h"
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/HttpQueryParser.h>
#include <jcailloux/relais/list/spec/ParseUtils.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>

namespace decl = jcailloux::relais::list::spec;
namespace parse = jcailloux::relais::list::spec::parse;
using relais_test::TestArticle;
using Entity = entity::generated::TestArticleEntity;

namespace {

struct BoolEqDesc {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"is_published", &TestArticle::is_published, "is_published", decl::Op::EQ>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

struct BoolInDesc {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"is_published", &TestArticle::is_published, "is_published", decl::Op::IN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

Entity makeArticle(bool is_published) {
    Entity e;
    e.id = 1;
    e.is_published = is_published;
    return e;
}

template<typename Desc>
auto parseOne(std::string value) {
    return decl::parseListQuery<Desc>(
        std::unordered_map<std::string, std::string>{{"is_published", std::move(value)}});
}

}  // namespace

TEST_CASE("parse::toBool accepts the standard HTTP boolean conventions",
          "[list][parse][bool]") {
    for (const char* s : {"true", "True", "TRUE", "1", "t", "T",
                          "yes", "Yes", "y", "Y", "on", "ON"})
        CHECK(parse::toBool(s) == std::optional<bool>{true});

    for (const char* s : {"false", "False", "FALSE", "0", "f", "F",
                          "no", "No", "n", "N", "off", "Off"})
        CHECK(parse::toBool(s) == std::optional<bool>{false});

    // Anything outside the convention sets -> nullopt (no false-as-default).
    for (const char* s : {"", "2", "-1", "tru", "truee", "ye", "onn",
                          "maybe", " 1", "1 ", "t rue"})
        CHECK(parse::toBool(s) == std::nullopt);
}

TEST_CASE("HTTP scalar bool EQ filter activates via parseValue",
          "[list][parse][bool]") {
    auto q = parseOne<BoolEqDesc>("true");
    REQUIRE(q.filters().template get<0>().has_value());
    CHECK(*q.filters().template get<0>() == true);
    CHECK(decl::matchesFilters<BoolEqDesc>(makeArticle(true), q.filters()));
    CHECK_FALSE(decl::matchesFilters<BoolEqDesc>(makeArticle(false), q.filters()));

    auto qOff = parseOne<BoolEqDesc>("off");
    REQUIRE(qOff.filters().template get<0>().has_value());
    CHECK(*qOff.filters().template get<0>() == false);
    CHECK(decl::matchesFilters<BoolEqDesc>(makeArticle(false), qOff.filters()));

    // Junk leaves the filter inactive rather than defaulting to false.
    auto qJunk = parseOne<BoolEqDesc>("maybe");
    CHECK_FALSE(qJunk.filters().template get<0>().has_value());
}

TEST_CASE("HTTP bool IN list parses, dedups and canonicalizes",
          "[list][parse][bool]") {
    // Mixed conventions, duplicates collapsed, sorted (false < true).
    auto q = parseOne<BoolInDesc>("yes,no,1,off");
    REQUIRE(q.filters().template get<0>().has_value());
    CHECK(*q.filters().template get<0>() == std::vector<bool>{false, true});

    auto qSingleton = parseOne<BoolInDesc>("t");
    REQUIRE(qSingleton.filters().template get<0>().has_value());
    CHECK(*qSingleton.filters().template get<0>() == std::vector<bool>{true});
    CHECK(decl::matchesFilters<BoolInDesc>(makeArticle(true), qSingleton.filters()));
    CHECK_FALSE(decl::matchesFilters<BoolInDesc>(makeArticle(false), qSingleton.filters()));

    // Invalid elements dropped; an all-invalid list leaves the filter inactive.
    auto qBad = parseOne<BoolInDesc>("maybe,perhaps");
    CHECK_FALSE(qBad.filters().template get<0>().has_value());
}