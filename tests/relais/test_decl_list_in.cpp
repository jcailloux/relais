/**
 * test_decl_list_in.cpp
 * Unit tests for the IN filter operator at the L1 matching layer.
 *
 * Scope (commit 3): the free `matchesFilters<Descriptor>(entity, filters)` —
 * the real L1 invalidation path (ListCache.h:529/536). Tested in isolation from
 * transport (no DB, no Redis): hand-built descriptors with IN filters and a
 * directly-populated Filters set. Validates the §1 invariant on the L1 tier:
 * "entity scalar ∈ query set". The L3 SQL `= ANY` and L2 Lua `cmpin` tiers, plus
 * the binary canonicalization and HTTP parsing, are covered in later commits.
 *
 * Covers §7.6 (L1 memory matching):
 * - IN match / mismatch per element type: string, int64, int32, bool
 * - singleton {x} ≡ EQ x
 * - empty set {} ⇒ no entity matches
 * - combinations EQ + IN + range
 * - optional member null ∉ any set
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <vector>

#include "fixtures/generated/TestArticleEntity.h"
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>

namespace decl = jcailloux::relais::list::spec;
using relais_test::TestArticle;
using Entity = entity::generated::TestArticleEntity;

// =============================================================================
// Hand-built descriptors with IN filters (independent of the generated fixture,
// whose embedded ListDescriptor carries only EQ filters until commit 8).
// =============================================================================

namespace {

// Single IN filter, one per element type ---------------------------------------

struct DescInString {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::IN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

struct DescInInt64 {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::IN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

// view_count is std::optional<int32_t> — covers IN on int32 AND optional member.
struct DescInOptInt32 {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"view_count", &TestArticle::view_count, "view_count", decl::Op::IN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

struct DescInBool {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"is_published", &TestArticle::is_published, "is_published", decl::Op::IN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

// Combination: EQ author_id [0], IN category [1], range (GE) view_count [2].
struct DescCombo {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::EQ>{},
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::IN>{},
        decl::Filter<"view_count", &TestArticle::view_count, "view_count", decl::Op::GE>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

Entity makeArticle(int64_t id, std::string category, int64_t author_id,
                   std::optional<int32_t> view_count = std::nullopt,
                   bool is_published = false) {
    Entity e;
    e.id = id;
    e.category = std::move(category);
    e.author_id = author_id;
    e.view_count = view_count;
    e.is_published = is_published;
    return e;
}

}  // namespace

// =============================================================================
// IN match / mismatch per element type
// =============================================================================

TEST_CASE("[DeclListIn] IN string membership", "[list][in][unit]") {
    decl::Filters<DescInString> f;
    f.get<0>() = std::vector<std::string>{"science", "tech"};

    CHECK(decl::matchesFilters<DescInString>(makeArticle(1, "tech", 0), f));
    CHECK(decl::matchesFilters<DescInString>(makeArticle(2, "science", 0), f));
    CHECK_FALSE(decl::matchesFilters<DescInString>(makeArticle(3, "news", 0), f));
    CHECK_FALSE(decl::matchesFilters<DescInString>(makeArticle(4, "", 0), f));
}

TEST_CASE("[DeclListIn] IN int64 membership", "[list][in][unit]") {
    decl::Filters<DescInInt64> f;
    f.get<0>() = std::vector<int64_t>{1, 2, 3};

    CHECK(decl::matchesFilters<DescInInt64>(makeArticle(1, "x", 2), f));
    CHECK_FALSE(decl::matchesFilters<DescInInt64>(makeArticle(2, "x", 9), f));

    SECTION("negatives and extremes") {
        f.get<0>() = std::vector<int64_t>{INT64_MIN, -1, 0, INT64_MAX};
        CHECK(decl::matchesFilters<DescInInt64>(makeArticle(1, "x", INT64_MIN), f));
        CHECK(decl::matchesFilters<DescInInt64>(makeArticle(2, "x", -1), f));
        CHECK(decl::matchesFilters<DescInInt64>(makeArticle(3, "x", 0), f));
        CHECK(decl::matchesFilters<DescInInt64>(makeArticle(4, "x", INT64_MAX), f));
        CHECK_FALSE(decl::matchesFilters<DescInInt64>(makeArticle(5, "x", 1), f));
    }
}

TEST_CASE("[DeclListIn] IN int32 on optional member", "[list][in][unit]") {
    decl::Filters<DescInOptInt32> f;
    f.get<0>() = std::vector<int32_t>{10, 20, INT32_MIN, INT32_MAX};

    CHECK(decl::matchesFilters<DescInOptInt32>(makeArticle(1, "x", 0, 20), f));
    CHECK(decl::matchesFilters<DescInOptInt32>(makeArticle(2, "x", 0, INT32_MIN), f));
    CHECK_FALSE(decl::matchesFilters<DescInOptInt32>(makeArticle(3, "x", 0, 5), f));

    SECTION("null member is in no set") {
        CHECK_FALSE(decl::matchesFilters<DescInOptInt32>(
            makeArticle(4, "x", 0, std::nullopt), f));
    }
}

TEST_CASE("[DeclListIn] IN bool membership", "[list][in][unit]") {
    decl::Filters<DescInBool> f;

    SECTION("set {true}") {
        f.get<0>() = std::vector<bool>{true};
        CHECK(decl::matchesFilters<DescInBool>(makeArticle(1, "x", 0, std::nullopt, true), f));
        CHECK_FALSE(decl::matchesFilters<DescInBool>(makeArticle(2, "x", 0, std::nullopt, false), f));
    }
    SECTION("set {true,false} matches both") {
        f.get<0>() = std::vector<bool>{false, true};
        CHECK(decl::matchesFilters<DescInBool>(makeArticle(1, "x", 0, std::nullopt, true), f));
        CHECK(decl::matchesFilters<DescInBool>(makeArticle(2, "x", 0, std::nullopt, false), f));
    }
}

// =============================================================================
// Singleton ≡ EQ ; empty set ; inactive filter
// =============================================================================

TEST_CASE("[DeclListIn] singleton set behaves like EQ", "[list][in][unit]") {
    decl::Filters<DescInString> f;
    f.get<0>() = std::vector<std::string>{"tech"};

    CHECK(decl::matchesFilters<DescInString>(makeArticle(1, "tech", 0), f));
    CHECK_FALSE(decl::matchesFilters<DescInString>(makeArticle(2, "science", 0), f));
    CHECK_FALSE(decl::matchesFilters<DescInString>(makeArticle(3, "news", 0), f));
}

TEST_CASE("[DeclListIn] empty set matches nothing", "[list][in][unit]") {
    decl::Filters<DescInString> f;
    // Programmatic empty set: present-but-empty optional<vector>.
    f.get<0>() = std::vector<std::string>{};
    REQUIRE(f.get<0>().has_value());

    CHECK_FALSE(decl::matchesFilters<DescInString>(makeArticle(1, "tech", 0), f));
    CHECK_FALSE(decl::matchesFilters<DescInString>(makeArticle(2, "", 0), f));
}

TEST_CASE("[DeclListIn] inactive IN filter matches every entity", "[list][in][unit]") {
    decl::Filters<DescInString> f;  // filter left unset (nullopt)
    REQUIRE_FALSE(f.get<0>().has_value());

    CHECK(decl::matchesFilters<DescInString>(makeArticle(1, "tech", 0), f));
    CHECK(decl::matchesFilters<DescInString>(makeArticle(2, "anything", 0), f));
}

// =============================================================================
// Combination EQ + IN + range
// =============================================================================

TEST_CASE("[DeclListIn] EQ + IN + range combination", "[list][in][unit]") {
    decl::Filters<DescCombo> f;
    f.get<0>() = int64_t{42};                              // EQ author_id == 42
    f.get<1>() = std::vector<std::string>{"news", "tech"};  // IN category
    f.get<2>() = int32_t{10};                              // GE view_count >= 10

    SECTION("all three satisfied") {
        CHECK(decl::matchesFilters<DescCombo>(makeArticle(1, "tech", 42, 15), f));
        CHECK(decl::matchesFilters<DescCombo>(makeArticle(2, "news", 42, 10), f));
    }
    SECTION("EQ fails") {
        CHECK_FALSE(decl::matchesFilters<DescCombo>(makeArticle(3, "tech", 7, 15), f));
    }
    SECTION("IN fails") {
        CHECK_FALSE(decl::matchesFilters<DescCombo>(makeArticle(4, "sports", 42, 15), f));
    }
    SECTION("range fails") {
        CHECK_FALSE(decl::matchesFilters<DescCombo>(makeArticle(5, "tech", 42, 5), f));
    }
    SECTION("range filter on null member excludes (range, not IN)") {
        CHECK_FALSE(decl::matchesFilters<DescCombo>(makeArticle(6, "tech", 42, std::nullopt), f));
    }
    SECTION("only IN active — EQ and range inactive") {
        decl::Filters<DescCombo> g;
        g.get<1>() = std::vector<std::string>{"tech"};
        CHECK(decl::matchesFilters<DescCombo>(makeArticle(7, "tech", 999, std::nullopt), g));
        CHECK_FALSE(decl::matchesFilters<DescCombo>(makeArticle(8, "other", 999, std::nullopt), g));
    }
}
