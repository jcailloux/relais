/**
 * test_decl_list_not_in.cpp
 * Unit tests for the NOT IN (anti-set) filter operator at the L1 matching layer.
 *
 * Scope (commit 2): the two L1 verdict paths that gained an Op::NIN branch —
 *   - free `matchesFilters<Descriptor>(entity, filters)` (GeneratedTraits.h),
 *     the real L1 invalidation path (ListCache.h:529/536);
 *   - `Filters::matchesFilters(extractTags(entity))` (GeneratedFilters.h),
 *     the FilterTags scalar-tag matching path.
 * Both are tested transport-free (no DB, no Redis): hand-built descriptors with
 * NIN filters and a directly-populated Filters set.
 *
 * Validates the §1 invariant on the L1 tier — "entity scalar ∉ query set" — and
 * the two NOT IN pitfalls:
 *   §1.1 NULL: a null optional member is excluded from BOTH IN and NOT IN
 *        results → NIN(null) == false (NOT the negation of IN).
 *   §1.2 empty set: NOT IN {} = universe (everything matches), the exact
 *        opposite of IN {} = ∅.
 *
 * The L3 SQL `!= ALL`, L2 Lua, HTTP parsing/canonicalization, and cross-tier
 * dualité tests land in later commits (§7.2–§7.7).
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <vector>

#include "fixtures/generated/TestArticleEntity.h"
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/GeneratedFilters.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>

namespace decl = jcailloux::relais::list::spec;
using relais_test::TestArticle;
using Entity = entity::generated::TestArticleEntity;

// =============================================================================
// Hand-built descriptors with NIN filters, one per element type, plus a
// combination descriptor and an IN+NIN coexistence descriptor.
// =============================================================================

namespace {

struct DescNinString {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::NIN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

struct DescNinInt64 {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::NIN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

// view_count is std::optional<int32_t> — covers NIN on int32 AND optional member.
struct DescNinOptInt32 {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"view_count", &TestArticle::view_count, "view_count", decl::Op::NIN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

struct DescNinBool {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"is_published", &TestArticle::is_published, "is_published", decl::Op::NIN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

// Combination: EQ author_id [0], NIN category [1], range (GE) view_count [2].
// NIN in the middle position — the filter after it (range) must stay aligned.
struct DescCombo {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::EQ>{},
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::NIN>{},
        decl::Filter<"view_count", &TestArticle::view_count, "view_count", decl::Op::GE>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

// IN [0] and NIN [1] on distinct columns — coexistence of both set ops in one
// descriptor (proves the set-op slots stay independent at the L1 verdict layer).
struct DescInNin {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::IN>{},
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::NIN>{}
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

// Run an entity through BOTH L1 verdict paths and assert they agree, returning
// the shared verdict. extractTags + Filters::matchesFilters is the FilterTags
// path (GeneratedFilters.h); the free matchesFilters is the GeneratedTraits.h
// path. Both gained the NIN branch in this commit and must never disagree.
template<typename Desc>
bool l1Match(const Entity& e, const decl::Filters<Desc>& f) {
    const bool viaFree = decl::matchesFilters<Desc>(e, f);
    const bool viaTags = f.matchesFilters(decl::extractTags<Desc>(e));
    CHECK(viaFree == viaTags);
    return viaFree;
}

}  // namespace

// =============================================================================
// NIN match / mismatch per element type (verdict inverted from IN on non-null)
// =============================================================================

TEST_CASE("[DeclListNin] NIN string anti-membership", "[list][nin][unit]") {
    decl::Filters<DescNinString> f;
    f.get<0>() = std::vector<std::string>{"science", "tech"};

    CHECK_FALSE(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));     // ∈ set
    CHECK_FALSE(l1Match<DescNinString>(makeArticle(2, "science", 0), f));  // ∈ set
    CHECK(l1Match<DescNinString>(makeArticle(3, "news", 0), f));           // ∉ set
    CHECK(l1Match<DescNinString>(makeArticle(4, "", 0), f));               // "" ∉ set
}

TEST_CASE("[DeclListNin] NIN int64 anti-membership", "[list][nin][unit]") {
    decl::Filters<DescNinInt64> f;
    f.get<0>() = std::vector<int64_t>{1, 2, 3};

    CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(1, "x", 2), f));  // ∈ set
    CHECK(l1Match<DescNinInt64>(makeArticle(2, "x", 9), f));        // ∉ set

    SECTION("negatives and extremes") {
        f.get<0>() = std::vector<int64_t>{INT64_MIN, -1, 0, INT64_MAX};
        CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(1, "x", INT64_MIN), f));
        CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(2, "x", -1), f));
        CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(3, "x", 0), f));
        CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(4, "x", INT64_MAX), f));
        CHECK(l1Match<DescNinInt64>(makeArticle(5, "x", 1), f));  // ∉ set
    }
}

TEST_CASE("[DeclListNin] NIN int32 on optional member", "[list][nin][unit]") {
    decl::Filters<DescNinOptInt32> f;
    f.get<0>() = std::vector<int32_t>{10, 20, INT32_MIN, INT32_MAX};

    CHECK_FALSE(l1Match<DescNinOptInt32>(makeArticle(1, "x", 0, 20), f));        // ∈ set
    CHECK_FALSE(l1Match<DescNinOptInt32>(makeArticle(2, "x", 0, INT32_MIN), f)); // ∈ set
    CHECK(l1Match<DescNinOptInt32>(makeArticle(3, "x", 0, 5), f));               // ∉ set

    SECTION("null member is excluded from NOT IN (NOT the negation of IN, §1.1)") {
        // The natural-but-wrong intuition is "null is not in the set, so it
        // matches NOT IN" → false. SQL three-valued logic excludes it.
        CHECK_FALSE(l1Match<DescNinOptInt32>(makeArticle(4, "x", 0, std::nullopt), f));
    }
}

TEST_CASE("[DeclListNin] NIN bool anti-membership", "[list][nin][unit]") {
    decl::Filters<DescNinBool> f;

    SECTION("set {true} excludes true, keeps false") {
        f.get<0>() = std::vector<bool>{true};
        CHECK_FALSE(l1Match<DescNinBool>(makeArticle(1, "x", 0, std::nullopt, true), f));
        CHECK(l1Match<DescNinBool>(makeArticle(2, "x", 0, std::nullopt, false), f));
    }
    SECTION("set {true,false} excludes both") {
        f.get<0>() = std::vector<bool>{false, true};
        CHECK_FALSE(l1Match<DescNinBool>(makeArticle(1, "x", 0, std::nullopt, true), f));
        CHECK_FALSE(l1Match<DescNinBool>(makeArticle(2, "x", 0, std::nullopt, false), f));
    }
}

// =============================================================================
// Singleton ≡ NE ; empty set = universe (§1.2) ; inactive filter
// =============================================================================

TEST_CASE("[DeclListNin] singleton set behaves like NE", "[list][nin][unit]") {
    decl::Filters<DescNinString> f;
    f.get<0>() = std::vector<std::string>{"tech"};

    CHECK_FALSE(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));  // == tech
    CHECK(l1Match<DescNinString>(makeArticle(2, "science", 0), f));     // != tech
    CHECK(l1Match<DescNinString>(makeArticle(3, "news", 0), f));        // != tech
}

TEST_CASE("[DeclListNin] empty set matches everything (NOT IN {} = universe, §1.2)",
          "[list][nin][unit]") {
    decl::Filters<DescNinString> f;
    // Programmatic empty set: present-but-empty optional<vector>. The exact
    // opposite of IN {} (which matches nothing).
    f.get<0>() = std::vector<std::string>{};
    REQUIRE(f.get<0>().has_value());

    CHECK(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));
    CHECK(l1Match<DescNinString>(makeArticle(2, "", 0), f));
}

TEST_CASE("[DeclListNin] empty set still excludes a null optional member (§1.1)",
          "[list][nin][unit]") {
    // NOT IN {} = universe, but a NULL member is excluded from every set result,
    // empty included — the null guard wins over the empty-set universe.
    decl::Filters<DescNinOptInt32> f;
    f.get<0>() = std::vector<int32_t>{};
    REQUIRE(f.get<0>().has_value());

    CHECK(l1Match<DescNinOptInt32>(makeArticle(1, "x", 0, 5), f));               // non-null ∉ {}
    CHECK_FALSE(l1Match<DescNinOptInt32>(makeArticle(2, "x", 0, std::nullopt), f)); // null excluded
}

TEST_CASE("[DeclListNin] inactive NIN filter matches every entity", "[list][nin][unit]") {
    decl::Filters<DescNinString> f;  // filter left unset (nullopt)
    REQUIRE_FALSE(f.get<0>().has_value());

    CHECK(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));
    CHECK(l1Match<DescNinString>(makeArticle(2, "anything", 0), f));
}

// =============================================================================
// Combination EQ + NIN + range (NIN in middle — alignment of the range after it)
// =============================================================================

TEST_CASE("[DeclListNin] EQ + NIN + range combination", "[list][nin][unit]") {
    decl::Filters<DescCombo> f;
    f.get<0>() = int64_t{42};                                  // EQ author_id == 42
    f.get<1>() = std::vector<std::string>{"news", "spam"};     // NIN category
    f.get<2>() = int32_t{10};                                  // GE view_count >= 10

    SECTION("all three satisfied (category ∉ {news,spam})") {
        CHECK(l1Match<DescCombo>(makeArticle(1, "tech", 42, 15), f));
        CHECK(l1Match<DescCombo>(makeArticle(2, "science", 42, 10), f));
    }
    SECTION("EQ fails") {
        CHECK_FALSE(l1Match<DescCombo>(makeArticle(3, "tech", 7, 15), f));
    }
    SECTION("NIN fails (category ∈ set)") {
        CHECK_FALSE(l1Match<DescCombo>(makeArticle(4, "news", 42, 15), f));
    }
    SECTION("range AFTER the NIN set still evaluated (alignment)") {
        CHECK_FALSE(l1Match<DescCombo>(makeArticle(5, "tech", 42, 5), f));  // 5 < 10
    }
    SECTION("range filter on null member excludes (range null → NE-only rule)") {
        CHECK_FALSE(l1Match<DescCombo>(makeArticle(6, "tech", 42, std::nullopt), f));
    }
    SECTION("only NIN active — EQ and range inactive") {
        decl::Filters<DescCombo> g;
        g.get<1>() = std::vector<std::string>{"spam"};
        CHECK(l1Match<DescCombo>(makeArticle(7, "tech", 999, std::nullopt), g));
        CHECK_FALSE(l1Match<DescCombo>(makeArticle(8, "spam", 999, std::nullopt), g));
    }
}

// =============================================================================
// Coexistence IN + NIN in one descriptor (both set-op slots independent)
// =============================================================================

TEST_CASE("[DeclListNin] IN and NIN coexist on distinct columns", "[list][nin][unit]") {
    decl::Filters<DescInNin> f;
    f.get<0>() = std::vector<int64_t>{1, 2};                    // author_id IN {1,2}
    f.get<1>() = std::vector<std::string>{"spam", "draft"};     // category NOT IN {spam,draft}

    // author ∈ {1,2} AND category ∉ {spam,draft}
    CHECK(l1Match<DescInNin>(makeArticle(1, "tech", 1), f));
    CHECK(l1Match<DescInNin>(makeArticle(2, "news", 2), f));
    // author ∉ {1,2} → IN fails
    CHECK_FALSE(l1Match<DescInNin>(makeArticle(3, "tech", 9), f));
    // category ∈ {spam,draft} → NIN fails
    CHECK_FALSE(l1Match<DescInNin>(makeArticle(4, "spam", 1), f));
    // both fail
    CHECK_FALSE(l1Match<DescInNin>(makeArticle(5, "draft", 9), f));
}
