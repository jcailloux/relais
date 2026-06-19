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
 * Commit 3 adds the L3 SQL generation block (§7.3, transport-free): the
 * `buildWhereClause` NIN branch emits `!= ALL($n)` (mirror of IN's `= ANY($n)`)
 * with exactly one array param, preserving the $n numbering invariant — verified
 * against EQ/range combinations and IN+NIN coexistence. Live-DB round-trip and
 * cross-tier dualité land with the cross-tier suite (§7.7).
 *
 * Commit 4 adds HTTP parsing + canonical key + schema (§7.2, transport-free): the
 * `is_set_op` switch routes NIN through the shared `parseInList`/`groupCacheKey`
 * set path (byte-identical encoding to IN — only the verdict differs), and
 * `filterSchema` emits '#' for NIN. Both parsers (`parseListQuery` and
 * `parseListQueryStrict`) are covered.
 *
 * The L2 Lua invalidation tests land in later commits (§7.4, §7.5).
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <vector>

#include "fixtures/generated/TestArticleEntity.h"
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/GeneratedFilters.h>
#include <jcailloux/relais/list/spec/GeneratedCriteria.h>
#include <jcailloux/relais/list/spec/HttpQueryParser.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>

#include <unordered_map>

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

// =============================================================================
// L3 SQL generation — buildWhereClause NIN branch (§7.3, transport-free).
// Validates `!= ALL($n)` (mirror of IN's `= ANY($n)`), the single-array-param
// invariant (NIN contributes exactly one $n like IN), $n numbering with EQ/range,
// the empty-set literal (`!= ALL('{}')` → universe, §1.2), and IN+NIN coexistence
// with two array params. Live-DB round-trip lands with the cross-tier suite.
// =============================================================================

namespace {

std::string paramStr(const jcailloux::relais::io::PgParam& p) {
    return p.isNull() ? std::string("<null>")
                      : std::string(p.data(), static_cast<size_t>(p.length()));
}

}  // namespace

TEST_CASE("[DeclListNin] SQL: NIN emits != ALL with one array param",
          "[list][nin][unit][sql]") {
    decl::Filters<DescNinString> f;
    f.get<0>() = std::vector<std::string>{"science", "tech"};

    auto wc = decl::buildWhereClause<DescNinString>(f);
    CHECK(wc.sql == "\"category\" != ALL($1)");
    REQUIRE(wc.params.params.size() == 1);          // exactly one param — the array
    CHECK(paramStr(wc.params.params[0]) == "{science,tech}");
    CHECK(wc.next_param == 2);
}

TEST_CASE("[DeclListNin] SQL: NIN int64 array literal", "[list][nin][unit][sql]") {
    decl::Filters<DescNinInt64> f;
    f.get<0>() = std::vector<int64_t>{1, 2, 3};

    auto wc = decl::buildWhereClause<DescNinInt64>(f);
    CHECK(wc.sql == "\"author_id\" != ALL($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{1,2,3}");
}

TEST_CASE("[DeclListNin] SQL: singleton NIN behaves like != x", "[list][nin][unit][sql]") {
    decl::Filters<DescNinString> f;
    f.get<0>() = std::vector<std::string>{"tech"};

    auto wc = decl::buildWhereClause<DescNinString>(f);
    CHECK(wc.sql == "\"category\" != ALL($1)");      // != ALL('{tech}') ≡ != 'tech'
    CHECK(paramStr(wc.params.params[0]) == "{tech}");
}

TEST_CASE("[DeclListNin] SQL: empty set yields != ALL('{}') (universe, §1.2)",
          "[list][nin][unit][sql]") {
    decl::Filters<DescNinString> f;
    f.get<0>() = std::vector<std::string>{};  // present-but-empty
    REQUIRE(f.get<0>().has_value());

    auto wc = decl::buildWhereClause<DescNinString>(f);
    CHECK(wc.sql == "\"category\" != ALL($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{}");    // != ALL('{}') → TRUE → all rows
}

TEST_CASE("[DeclListNin] SQL: EQ + NIN + range $n numbering", "[list][nin][unit][sql]") {
    decl::Filters<DescCombo> f;
    f.get<0>() = int64_t{42};                                  // EQ author_id
    f.get<1>() = std::vector<std::string>{"news", "spam"};     // NIN category
    f.get<2>() = int32_t{10};                                  // GE view_count

    auto wc = decl::buildWhereClause<DescCombo>(f);
    CHECK(wc.sql == "\"author_id\"=$1 AND \"category\" != ALL($2) AND \"view_count\">=$3");
    REQUIRE(wc.params.params.size() == 3);     // NIN contributes exactly one param
    CHECK(paramStr(wc.params.params[0]) == "42");
    CHECK(paramStr(wc.params.params[1]) == "{news,spam}");
    CHECK(paramStr(wc.params.params[2]) == "10");
    CHECK(wc.next_param == 4);                  // cursor/offset params would start at $4

    SECTION("only NIN active — EQ/range skipped, NIN takes $1") {
        decl::Filters<DescCombo> g;
        g.get<1>() = std::vector<std::string>{"spam"};
        auto wc2 = decl::buildWhereClause<DescCombo>(g);
        CHECK(wc2.sql == "\"category\" != ALL($1)");
        REQUIRE(wc2.params.params.size() == 1);
        CHECK(wc2.next_param == 2);
    }
}

TEST_CASE("[DeclListNin] SQL: IN and NIN coexist — two array params, $n correct",
          "[list][nin][unit][sql]") {
    decl::Filters<DescInNin> f;
    f.get<0>() = std::vector<int64_t>{1, 2};                   // author_id IN {1,2}
    f.get<1>() = std::vector<std::string>{"spam", "draft"};    // category NOT IN {spam,draft}

    auto wc = decl::buildWhereClause<DescInNin>(f);
    CHECK(wc.sql == "\"author_id\" = ANY($1) AND \"category\" != ALL($2)");
    REQUIRE(wc.params.params.size() == 2);     // each set op contributes one array param
    CHECK(paramStr(wc.params.params[0]) == "{1,2}");
    CHECK(paramStr(wc.params.params[1]) == "{spam,draft}");  // raw order — canonicalization is groupCacheKey's job, not the WHERE clause
    CHECK(wc.next_param == 3);
}

// =============================================================================
// HTTP parsing + canonical key + schema (§7.2, transport-free). NIN shares the
// IN set path through `is_set_op`: same `parseInList` (CSV → sort → unique →
// cap), same `groupCacheKey` byte layout. Only the schema char ('#') is NIN's.
// We re-test it here because `is_set_op` is a new branch point — a missed
// `== Op::IN` site would silently route NIN to the scalar `parseValue`.
// =============================================================================

namespace {

using Params = std::unordered_map<std::string, std::string>;

}  // namespace

TEST_CASE("[DeclListNin] parse: CSV set sorted and deduped into NIN slot",
          "[list][nin][unit][parse]") {
    auto q = decl::parseListQuery<DescNinString>(Params{{"category", "spam,draft"}});
    REQUIRE(q.filters.get<0>().has_value());
    CHECK(*q.filters.get<0>() == std::vector<std::string>{"draft", "spam"});  // sorted
}

TEST_CASE("[DeclListNin] parse: order/dups → identical group key (shared canon)",
          "[list][nin][unit][parse]") {
    auto a = decl::parseListQuery<DescNinString>(Params{{"category", "draft,spam"}});
    auto b = decl::parseListQuery<DescNinString>(Params{{"category", "spam,draft,spam"}});
    CHECK(a.group_key == b.group_key);
}

TEST_CASE("[DeclListNin] parse: invalid int elements dropped", "[list][nin][unit][parse]") {
    auto q = decl::parseListQuery<DescNinInt64>(Params{{"author_id", "1,abc,3"}});
    REQUIRE(q.filters.get<0>().has_value());
    CHECK(*q.filters.get<0>() == std::vector<int64_t>{1, 3});
}

TEST_CASE("[DeclListNin] parse: empty value leaves NIN inactive (≡ NOT IN {} = universe, §1.2)",
          "[list][nin][unit][parse]") {
    // No valid element → filter inactive. For NIN this is exactly the desired
    // semantics: inactive ≡ unfiltered ≡ NOT IN {} = universe (unlike IN, where
    // inactive is a compromise vs the empty-set = ∅).
    auto q = decl::parseListQuery<DescNinInt64>(Params{{"author_id", "abc,xyz"}});
    CHECK_FALSE(q.filters.get<0>().has_value());
}

TEST_CASE("[DeclListNin] parse: element count capped at 256", "[list][nin][unit][parse]") {
    std::string csv;
    for (int i = 0; i < 300; ++i) {
        if (i) csv += ',';
        csv += std::to_string(i);
    }
    auto q = decl::parseListQuery<DescNinInt64>(Params{{"author_id", csv}});
    REQUIRE(q.filters.get<0>().has_value());
    CHECK(q.filters.get<0>()->size() == 256);
}

TEST_CASE("[DeclListNin] parse: strict rejects undeclared, accepts well-formed NIN",
          "[list][nin][unit][parse]") {
    SECTION("undeclared param → error") {
        auto r = decl::parseListQueryStrict<DescNinString>(Params{{"bogus", "x"}});
        REQUIRE_FALSE(r.has_value());
    }
    SECTION("well-formed NIN → ok, canonical set in slot") {
        auto r = decl::parseListQueryStrict<DescNinString>(
            Params{{"category", "spam,draft,spam"}});
        REQUIRE(r.has_value());
        REQUIRE(r->filters.get<0>().has_value());
        CHECK(*r->filters.get<0>() == std::vector<std::string>{"draft", "spam"});
    }
}

TEST_CASE("[DeclListNin] parse: HTTP IN and NIN produce the same canonical set",
          "[list][nin][unit][parse]") {
    // Same CSV parsed under IN[0] vs NIN[1] of the coexistence descriptor: the
    // set payload is byte-identical (encoding is shared); the ops differ only at
    // verdict time. Here author_id is IN, category is NIN — parse both.
    auto q = decl::parseListQuery<DescInNin>(
        Params{{"author_id", "2,1,2"}, {"category", "spam,draft,spam"}});
    REQUIRE(q.filters.get<0>().has_value());
    REQUIRE(q.filters.get<1>().has_value());
    CHECK(*q.filters.get<0>() == std::vector<int64_t>{1, 2});            // IN, canonical
    CHECK(*q.filters.get<1>() == std::vector<std::string>{"draft", "spam"});  // NIN, canonical
}

// =============================================================================
// Compact filter schema (§4.5) — NIN's operator char is '#' (ASCII 35), free of
// every other op code (=61 !33 >62 G71 <60 L76 @64). Type char is the element
// type, shared with IN. The schema drives the L2 Lua binary parser (commit 5).
// =============================================================================

TEST_CASE("[DeclListNin] schema: NIN operator char is '#'", "[list][nin][unit][parse]") {
    CHECK(decl::filterSchema<DescNinString>() == "s#");   // string element, NIN
    CHECK(decl::filterSchema<DescNinInt64>() == "8#");    // int64 element, NIN
    CHECK(decl::filterSchema<DescNinBool>() == "1#");     // bool → '1'
}

TEST_CASE("[DeclListNin] schema: NIN coexists with scalar and IN ops",
          "[list][nin][unit][parse]") {
    // EQ author_id(int64) '8=' | NIN category(string) 's#' | GE view_count(int32) '4G'
    CHECK(decl::filterSchema<DescCombo>() == "8=s#4G");
    // IN author_id(int64) '8@' | NIN category(string) 's#'
    CHECK(decl::filterSchema<DescInNin>() == "8@s#");
}
