/**
 * test_decl_list_not_in.cpp
 * The NOT IN (anti-set) filter operator on every tier. Invariant: L1 and PostgreSQL
 * give the same verdict — "entity scalar ∉ query set" — including on the two
 * NOT IN pitfalls:
 *   NULL: a null optional member is excluded from IN and from a non-empty
 *        NOT IN (NOT the negation of IN); NOT IN {} holds it, as SQL
 *        `NULL <> ALL('{}')` is TRUE.
 *   Empty set: NOT IN {} = universe (everything matches), the exact opposite
 *        of IN {} = ∅.
 *
 * L1 matching (transport-free, hand-built descriptors with NIN filters and a
 * directly-populated Filters set): the free `matchesFilters<Descriptor>(entity,
 * filters)` (GeneratedTraits.h), the real L1 invalidation path, and
 * `Filters::matchesFilters(extractTags(entity))` (GeneratedFilters.h), the
 * FilterTags scalar-tag matching path.
 *
 * SQL generation (transport-free): the `buildWhereClause` NIN branch emits
 * `!= ALL($n)` (mirror of IN's `= ANY($n)`) with exactly one array param,
 * preserving the $n numbering — checked against EQ/range combinations and
 * IN+NIN coexistence.
 *
 * HTTP parsing and canonical key (transport-free): the `is_set_op` switch
 * routes NIN through the shared `parseInList`/`groupCacheKey` set path
 * (byte-identical encoding to IN — only the verdict differs). Both
 * `parseListQuery` and `parseListQueryStrict` are covered.
 *
 * Update verdict (transport-free): a cached group is reached when the old OR the
 * new state matches, so an update between two values of the set reaches nothing.
 *
 * Cross-tier property suite (DB): a fixed-seed sweep of ~25 entities (some
 * with NULL members) × ~10 varied sets per element type. (1) The NIN verdict from
 * L1 `matchesFilters` and PostgreSQL `!= ALL` is identical.
 * (2) For a present member `NIN(S,v) == !IN(S,v)`.
 * (3) For a null member `IN(S,NULL)` is false and `NIN(S,NULL)` holds only for an
 * empty S — the duality does NOT hold, the most natural implementer error
 * (`NIN = !IN` everywhere).
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/generated/TestArticleEntity.h"
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/GeneratedFilters.h>
#include <jcailloux/relais/list/spec/GeneratedCriteria.h>
#include <jcailloux/relais/list/spec/HttpQueryParser.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>

#include <unordered_map>

namespace decl = jcailloux::relais::list::spec;
using relais_test::TestArticle;
using relais_test::sync;              // relais_test::sync, not POSIX ::sync from <unistd.h>
using relais_test::TransactionGuard;
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
    const bool viaFree = decl::detail::matchesFilters<Desc>(e, f);
    const bool viaTags = f.matchesFilters(decl::detail::extractTags<Desc>(e));
    CHECK(viaFree == viaTags);
    return viaFree;
}

}  // namespace

// =============================================================================
// NIN match / mismatch per element type (verdict inverted from IN on non-null)
// =============================================================================

TEST_CASE("[DeclListNin] NIN string anti-membership", "[list][nin][unit]") {
    decl::Filters<DescNinString> f;
    f.get<"category">() = std::vector<std::string>{"science", "tech"};

    CHECK_FALSE(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));     // ∈ set
    CHECK_FALSE(l1Match<DescNinString>(makeArticle(2, "science", 0), f));  // ∈ set
    CHECK(l1Match<DescNinString>(makeArticle(3, "news", 0), f));           // ∉ set
    CHECK(l1Match<DescNinString>(makeArticle(4, "", 0), f));               // "" ∉ set
}

TEST_CASE("[DeclListNin] NIN int64 anti-membership", "[list][nin][unit]") {
    decl::Filters<DescNinInt64> f;
    f.get<"author_id">() = std::vector<int64_t>{1, 2, 3};

    CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(1, "x", 2), f));  // ∈ set
    CHECK(l1Match<DescNinInt64>(makeArticle(2, "x", 9), f));        // ∉ set

    SECTION("negatives and extremes") {
        f.get<"author_id">() = std::vector<int64_t>{INT64_MIN, -1, 0, INT64_MAX};
        CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(1, "x", INT64_MIN), f));
        CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(2, "x", -1), f));
        CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(3, "x", 0), f));
        CHECK_FALSE(l1Match<DescNinInt64>(makeArticle(4, "x", INT64_MAX), f));
        CHECK(l1Match<DescNinInt64>(makeArticle(5, "x", 1), f));  // ∉ set
    }
}

TEST_CASE("[DeclListNin] NIN int32 on optional member", "[list][nin][unit]") {
    decl::Filters<DescNinOptInt32> f;
    f.get<"view_count">() = std::vector<int32_t>{10, 20, INT32_MIN, INT32_MAX};

    CHECK_FALSE(l1Match<DescNinOptInt32>(makeArticle(1, "x", 0, 20), f));        // ∈ set
    CHECK_FALSE(l1Match<DescNinOptInt32>(makeArticle(2, "x", 0, INT32_MIN), f)); // ∈ set
    CHECK(l1Match<DescNinOptInt32>(makeArticle(3, "x", 0, 5), f));               // ∉ set

    SECTION("null member is excluded from a non-empty NOT IN (NOT the negation of IN)") {
        // The natural-but-wrong intuition is "null is not in the set, so it
        // matches NOT IN" → false. SQL three-valued logic excludes it.
        CHECK_FALSE(l1Match<DescNinOptInt32>(makeArticle(4, "x", 0, std::nullopt), f));
    }
}

TEST_CASE("[DeclListNin] NIN bool anti-membership", "[list][nin][unit]") {
    decl::Filters<DescNinBool> f;

    SECTION("set {true} excludes true, keeps false") {
        f.get<"is_published">() = std::vector<bool>{true};
        CHECK_FALSE(l1Match<DescNinBool>(makeArticle(1, "x", 0, std::nullopt, true), f));
        CHECK(l1Match<DescNinBool>(makeArticle(2, "x", 0, std::nullopt, false), f));
    }
    SECTION("set {true,false} excludes both") {
        f.get<"is_published">() = std::vector<bool>{false, true};
        CHECK_FALSE(l1Match<DescNinBool>(makeArticle(1, "x", 0, std::nullopt, true), f));
        CHECK_FALSE(l1Match<DescNinBool>(makeArticle(2, "x", 0, std::nullopt, false), f));
    }
}

// =============================================================================
// Singleton ≡ NE ; empty set = universe ; inactive filter
// =============================================================================

TEST_CASE("[DeclListNin] singleton set behaves like NE", "[list][nin][unit]") {
    decl::Filters<DescNinString> f;
    f.get<"category">() = std::vector<std::string>{"tech"};

    CHECK_FALSE(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));  // == tech
    CHECK(l1Match<DescNinString>(makeArticle(2, "science", 0), f));     // != tech
    CHECK(l1Match<DescNinString>(makeArticle(3, "news", 0), f));        // != tech
}

TEST_CASE("[DeclListNin] empty set matches everything (NOT IN {} = universe)",
          "[list][nin][unit]") {
    decl::Filters<DescNinString> f;
    // Programmatic empty set: present-but-empty optional<vector>. The exact
    // opposite of IN {} (which matches nothing).
    f.get<"category">() = std::vector<std::string>{};
    REQUIRE(f.get<"category">().has_value());

    CHECK(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));
    CHECK(l1Match<DescNinString>(makeArticle(2, "", 0), f));
}

TEST_CASE("[DeclListNin] empty set holds a null optional member",
          "[list][nin][unit]") {
    // SQL `NULL <> ALL('{}')` is TRUE: NOT IN {} holds every row, NULL included.
    decl::Filters<DescNinOptInt32> f;
    f.get<"view_count">() = std::vector<int32_t>{};
    REQUIRE(f.get<"view_count">().has_value());

    CHECK(l1Match<DescNinOptInt32>(makeArticle(1, "x", 0, 5), f));               // non-null ∉ {}
    CHECK(l1Match<DescNinOptInt32>(makeArticle(2, "x", 0, std::nullopt), f));       // null ∉ {}
}

TEST_CASE("[DeclListNin] inactive NIN filter matches every entity", "[list][nin][unit]") {
    decl::Filters<DescNinString> f;  // filter left unset (nullopt)
    REQUIRE_FALSE(f.get<"category">().has_value());

    CHECK(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));
    CHECK(l1Match<DescNinString>(makeArticle(2, "anything", 0), f));
}

// =============================================================================
// Combination EQ + NIN + range (NIN in middle — alignment of the range after it)
// =============================================================================

TEST_CASE("[DeclListNin] EQ + NIN + range combination", "[list][nin][unit]") {
    decl::Filters<DescCombo> f;
    f.get<"author_id">() = int64_t{42};                                  // EQ author_id == 42
    f.get<"category">() = std::vector<std::string>{"news", "spam"};     // NIN category
    f.get<"view_count">() = int32_t{10};                                  // GE view_count >= 10

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
    SECTION("range filter on null member excludes") {
        CHECK_FALSE(l1Match<DescCombo>(makeArticle(6, "tech", 42, std::nullopt), f));
    }
    SECTION("only NIN active — EQ and range inactive") {
        decl::Filters<DescCombo> g;
        g.get<"category">() = std::vector<std::string>{"spam"};
        CHECK(l1Match<DescCombo>(makeArticle(7, "tech", 999, std::nullopt), g));
        CHECK_FALSE(l1Match<DescCombo>(makeArticle(8, "spam", 999, std::nullopt), g));
    }
}

// =============================================================================
// Coexistence IN + NIN in one descriptor (both set-op slots independent)
// =============================================================================

TEST_CASE("[DeclListNin] IN and NIN coexist on distinct columns", "[list][nin][unit]") {
    decl::Filters<DescInNin> f;
    f.get<"author_id">() = std::vector<int64_t>{1, 2};                    // author_id IN {1,2}
    f.get<"category">() = std::vector<std::string>{"spam", "draft"};     // category NOT IN {spam,draft}

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
// SQL generation — buildWhereClause NIN branch (transport-free).
// Validates `!= ALL($n)` (mirror of IN's `= ANY($n)`), the single-array-param
// invariant (NIN contributes exactly one $n like IN), $n numbering with EQ/range,
// the empty-set literal (`!= ALL('{}')` → universe), and IN+NIN coexistence
// with two array params. Live-DB round-trip lands with the cross-tier suite.
// =============================================================================

namespace {

std::string paramStr(const jcailloux::relais::io::detail::PgParam& p) {
    return p.isNull() ? std::string("<null>")
                      : std::string(p.data(), static_cast<size_t>(p.length()));
}

}  // namespace

TEST_CASE("[DeclListNin] SQL: NIN emits != ALL with one array param",
          "[list][nin][unit][sql]") {
    decl::Filters<DescNinString> f;
    f.get<"category">() = std::vector<std::string>{"science", "tech"};

    auto wc = decl::detail::buildWhereClause<DescNinString>(f);
    CHECK(wc.sql == "\"category\" != ALL($1)");
    REQUIRE(wc.params.params.size() == 1);          // exactly one param — the array
    CHECK(paramStr(wc.params.params[0]) == "{science,tech}");
    CHECK(wc.next_param == 2);
}

TEST_CASE("[DeclListNin] SQL: NIN int64 array literal", "[list][nin][unit][sql]") {
    decl::Filters<DescNinInt64> f;
    f.get<"author_id">() = std::vector<int64_t>{1, 2, 3};

    auto wc = decl::detail::buildWhereClause<DescNinInt64>(f);
    CHECK(wc.sql == "\"author_id\" != ALL($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{1,2,3}");
}

TEST_CASE("[DeclListNin] SQL: singleton NIN behaves like != x", "[list][nin][unit][sql]") {
    decl::Filters<DescNinString> f;
    f.get<"category">() = std::vector<std::string>{"tech"};

    auto wc = decl::detail::buildWhereClause<DescNinString>(f);
    CHECK(wc.sql == "\"category\" != ALL($1)");      // != ALL('{tech}') ≡ != 'tech'
    CHECK(paramStr(wc.params.params[0]) == "{tech}");
}

TEST_CASE("[DeclListNin] SQL: empty set yields != ALL('{}') (universe)",
          "[list][nin][unit][sql]") {
    decl::Filters<DescNinString> f;
    f.get<"category">() = std::vector<std::string>{};  // present-but-empty
    REQUIRE(f.get<"category">().has_value());

    auto wc = decl::detail::buildWhereClause<DescNinString>(f);
    CHECK(wc.sql == "\"category\" != ALL($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{}");    // != ALL('{}') → TRUE → all rows
}

TEST_CASE("[DeclListNin] SQL: EQ + NIN + range $n numbering", "[list][nin][unit][sql]") {
    decl::Filters<DescCombo> f;
    f.get<"author_id">() = int64_t{42};                                  // EQ author_id
    f.get<"category">() = std::vector<std::string>{"news", "spam"};     // NIN category
    f.get<"view_count">() = int32_t{10};                                  // GE view_count

    auto wc = decl::detail::buildWhereClause<DescCombo>(f);
    CHECK(wc.sql == "\"author_id\"=$1 AND \"category\" != ALL($2) AND \"view_count\">=$3");
    REQUIRE(wc.params.params.size() == 3);     // NIN contributes exactly one param
    CHECK(paramStr(wc.params.params[0]) == "42");
    CHECK(paramStr(wc.params.params[1]) == "{news,spam}");
    CHECK(paramStr(wc.params.params[2]) == "10");
    CHECK(wc.next_param == 4);                  // cursor/offset params would start at $4

    SECTION("only NIN active — EQ/range skipped, NIN takes $1") {
        decl::Filters<DescCombo> g;
        g.get<"category">() = std::vector<std::string>{"spam"};
        auto wc2 = decl::detail::buildWhereClause<DescCombo>(g);
        CHECK(wc2.sql == "\"category\" != ALL($1)");
        REQUIRE(wc2.params.params.size() == 1);
        CHECK(wc2.next_param == 2);
    }
}

TEST_CASE("[DeclListNin] SQL: IN and NIN coexist — two array params, $n correct",
          "[list][nin][unit][sql]") {
    decl::Filters<DescInNin> f;
    f.get<"author_id">() = std::vector<int64_t>{1, 2};                   // author_id IN {1,2}
    f.get<"category">() = std::vector<std::string>{"spam", "draft"};    // category NOT IN {spam,draft}

    auto wc = decl::detail::buildWhereClause<DescInNin>(f);
    CHECK(wc.sql == "\"author_id\" = ANY($1) AND \"category\" != ALL($2)");
    REQUIRE(wc.params.params.size() == 2);     // each set op contributes one array param
    CHECK(paramStr(wc.params.params[0]) == "{1,2}");
    CHECK(paramStr(wc.params.params[1]) == "{spam,draft}");  // raw order — canonicalization is groupCacheKey's job, not the WHERE clause
    CHECK(wc.next_param == 3);
}

// =============================================================================
// HTTP parsing + canonical key (transport-free). NIN shares the IN set path
// through `is_set_op`: same `parseInList` (CSV → sort → unique → cap), same
// `groupCacheKey` byte layout. We re-test it here because `is_set_op` is a new branch point — a missed
// `== Op::IN` site would silently route NIN to the scalar `parseValue`.
// =============================================================================

namespace {

using Params = std::unordered_map<std::string, std::string>;

}  // namespace

TEST_CASE("[DeclListNin] parse: CSV set sorted and deduped into NIN slot",
          "[list][nin][unit][parse]") {
    auto q = decl::parseListQuery<DescNinString>(Params{{"category", "spam,draft"}});
    REQUIRE(q.filters().get<"category">().has_value());
    CHECK(*q.filters().get<"category">() == std::vector<std::string>{"draft", "spam"});  // sorted
}

TEST_CASE("[DeclListNin] parse: order/dups → identical group key (shared canon)",
          "[list][nin][unit][parse]") {
    auto a = decl::parseListQuery<DescNinString>(Params{{"category", "draft,spam"}});
    auto b = decl::parseListQuery<DescNinString>(Params{{"category", "spam,draft,spam"}});
    CHECK(a.groupKey() == b.groupKey());
}

TEST_CASE("[DeclListNin] parse: invalid int elements dropped", "[list][nin][unit][parse]") {
    auto q = decl::parseListQuery<DescNinInt64>(Params{{"author_id", "1,abc,3"}});
    REQUIRE(q.filters().get<"author_id">().has_value());
    CHECK(*q.filters().get<"author_id">() == std::vector<int64_t>{1, 3});
}

TEST_CASE("[DeclListNin] parse: empty value leaves NIN inactive (≡ NOT IN {} = universe)",
          "[list][nin][unit][parse]") {
    // No valid element → filter inactive. For NIN this is exactly the desired
    // semantics: inactive ≡ unfiltered ≡ NOT IN {} = universe (unlike IN, where
    // inactive is a compromise vs the empty-set = ∅).
    auto q = decl::parseListQuery<DescNinInt64>(Params{{"author_id", "abc,xyz"}});
    CHECK_FALSE(q.filters().get<"author_id">().has_value());
}

TEST_CASE("[DeclListNin] parse: element count capped at 256", "[list][nin][unit][parse]") {
    std::string csv;
    for (int i = 0; i < 300; ++i) {
        if (i) csv += ',';
        csv += std::to_string(i);
    }
    auto q = decl::parseListQuery<DescNinInt64>(Params{{"author_id", csv}});
    REQUIRE(q.filters().get<"author_id">().has_value());
    CHECK(q.filters().get<"author_id">()->size() == 256);
}


namespace {

std::string intCsv(int count, int distinct) {
    std::string csv;
    for (int i = 0; i < count; ++i) {
        if (i) csv += ',';
        csv += std::to_string(i % distinct);
    }
    return csv;
}

}  // namespace

TEST_CASE("[DeclListNin] parse: strict rejects a set above 256 distinct elements",
          "[list][nin][unit][parse]") {
    // Truncating a NOT IN set would return rows the caller excluded.
    auto r = decl::parseListQueryStrict<DescNinInt64>(Params{{"author_id", intCsv(257, 257)}});
    REQUIRE_FALSE(r.has_value());
    CHECK(r.error().type == decl::QueryValidationError::Type::InvalidValue);
    CHECK(r.error().field == "author_id");
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
        REQUIRE(r->filters().get<"category">().has_value());
        CHECK(*r->filters().get<"category">() == std::vector<std::string>{"draft", "spam"});
    }
}

TEST_CASE("[DeclListNin] parse: HTTP IN and NIN produce the same canonical set",
          "[list][nin][unit][parse]") {
    // Same CSV parsed under IN[0] vs NIN[1] of the coexistence descriptor: the
    // set payload is byte-identical (encoding is shared); the ops differ only at
    // verdict time. Here author_id is IN, category is NIN — parse both.
    auto q = decl::parseListQuery<DescInNin>(
        Params{{"author_id", "2,1,2"}, {"category", "spam,draft,spam"}});
    REQUIRE(q.filters().get<"author_id">().has_value());
    REQUIRE(q.filters().get<"category">().has_value());
    CHECK(*q.filters().get<"author_id">() == std::vector<int64_t>{1, 2});            // IN, canonical
    CHECK(*q.filters().get<"category">() == std::vector<std::string>{"draft", "spam"});  // NIN, canonical
}

// =============================================================================
// Update verdict — a cached group is reached when the entity matches it before
// OR after the update (the L1 list cache checks both states).
// =============================================================================

TEST_CASE("[DeclListNin] update reaches a group when old or new is excluded from the set",
          "[list][nin][unit]") {
    decl::Filters<DescNinString> f;
    auto reached = [&f](const Entity& oldE, const Entity& newE) {
        return l1Match<DescNinString>(oldE, f) || l1Match<DescNinString>(newE, f);
    };

    SECTION("old ∈ set, new ∉ set: the entity enters the result → reached") {
        f.get<"category">() = std::vector<std::string>{"spam"};
        CHECK(reached(makeArticle(1, "spam", 0), makeArticle(1, "news", 0)));
    }
    SECTION("both ∉ set: the entity stays in the result, its position may move → reached") {
        f.get<"category">() = std::vector<std::string>{"spam"};
        CHECK(reached(makeArticle(1, "news", 0), makeArticle(1, "tech", 0)));
    }
    SECTION("both ∈ set: the entity is out of the result in both states → not reached") {
        f.get<"category">() = std::vector<std::string>{"spam", "draft"};
        CHECK_FALSE(reached(makeArticle(1, "spam", 0), makeArticle(1, "draft", 0)));
    }
}

// =============================================================================
// Cross-tier consistency + duality — L1 and PostgreSQL agree under load.
//
// For each (entity, set) both tiers must agree on the NIN verdict "entity
// scalar ∉ set": L1 `matchesFilters` (pure C++) and PostgreSQL `!= ALL` (the
// real array parser, fed buildWhereClause's literal so escaping round-trips
// through PG). Plus the two dualité properties:
//   (2) non-null member: NIN(S,v) == !IN(S,v) — the negation holds;
//   (3) NULL member: IN(S,NULL) == false, NIN(S,NULL) == S.empty() — the negation
//       BREAKS (SQL 3-valued logic; `NULL <> ALL('{}')` alone is TRUE).
// Fixed seed (deterministic, no Math.random). IN verdict is the L1 reference.
// =============================================================================

namespace {

// IN companions of the NIN descriptors, same column/type — the dualité reference.
struct DualInString {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::IN>{}};
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}};
};
struct DualInInt64 {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::IN>{}};
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}};
};
struct DualInOptInt32 {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"view_count", &TestArticle::view_count, "view_count", decl::Op::IN>{}};
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}};
};
struct DualInBool {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"is_published", &TestArticle::is_published, "is_published", decl::Op::IN>{}};
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}};
};

// Real PostgreSQL NIN verdict: `$1 != ALL($2)`, $1 NULL for an absent member.
template<typename V>
bool sqlAllVerdict(const std::optional<V>& val, const std::string& arrayLiteral,
                   const char* cast) {
    std::string sql = "SELECT CASE WHEN $1::";
    sql += cast; sql += " != ALL($2::"; sql += cast; sql += "[]) THEN 1 ELSE 0 END";
    auto r = relais_test::execQueryArgs(sql.c_str(), val, arrayLiteral);
    return r[0].template get<int32_t>(0) == 1;
}

// getField : Entity -> std::optional<Elem> (null for an absent optional member).
template<typename DescNin, typename DescIn, typename Elem, typename Getter>
void crossTierNinProperty(const std::vector<Entity>& entities,
                          const std::vector<std::vector<Elem>>& sets,
                          const char* cast, Getter getField) {
    for (const auto& e : entities) {
        for (size_t i = 0; i < sets.size(); ++i) {
            decl::Filters<DescNin> fn;
            std::get<0>(fn.values) = sets[i];
            const bool ninL1 = decl::detail::matchesFilters<DescNin>(e, fn);

            auto wc = decl::detail::buildWhereClause<DescNin>(fn);
            const std::string arr = paramStr(wc.params.params[0]);
            const auto fld = getField(e);
            const bool ninSql = sqlAllVerdict(fld, arr, cast);

            decl::Filters<DescIn> fi;
            std::get<0>(fi.values) = sets[i];
            const bool inL1 = decl::detail::matchesFilters<DescIn>(e, fi);

            CAPTURE(e.id, i, arr, ninL1, ninSql, inL1, fld.has_value());
            CHECK(ninL1 == ninSql);          // cross-tier: L1 == SQL
            if (fld.has_value())
                CHECK(ninL1 == !inL1);      // duality holds for a present member
            else {
                CHECK(ninL1 == sets[i].empty());  // NIN(NULL) holds for {} only
                CHECK_FALSE(inL1);                // IN(NULL) == false — negation BREAKS
            }
        }
    }
}

// Distinct sets (size 0..5) from `pool`, deduped so each group key is unique. The
// empty set is reachable (NOT IN {} = universe).
template<typename Elem>
std::vector<std::vector<Elem>> makeSets(std::mt19937& gen,
                                        const std::vector<Elem>& pool, size_t want) {
    std::set<std::vector<Elem>> uniq;
    for (int guard = 0; uniq.size() < want && guard < 2000; ++guard) {
        size_t sz = gen() % 6;
        std::set<Elem> s;
        for (size_t k = 0; k < sz; ++k) s.insert(pool[gen() % pool.size()]);
        uniq.emplace(s.begin(), s.end());
    }
    return {uniq.begin(), uniq.end()};
}

constexpr size_t kXtEntities = 25;
constexpr size_t kXtSets = 10;

const std::vector<std::string> kCatPool = {
    "tech", "science", "news", "sports", "games", "music",
    "", "a,b", "x{y}", "q\"z"  // array-literal escaping must round-trip through PG
};
const std::vector<int64_t> kAuthorPool = {
    INT64_MIN, -1000, -1, 0, 1, 42, 1000, INT64_MAX
};
const std::vector<int32_t> kViewPool = {
    INT32_MIN, -5, 0, 10, 20, INT32_MAX
};

std::vector<Entity> makeEntities(std::mt19937& gen, size_t n) {
    std::vector<Entity> out;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        std::optional<int32_t> vc;
        if (gen() % 5 != 0)  // ~20% null view_count → exercises the NULL break
            vc = kViewPool[gen() % kViewPool.size()];
        out.push_back(makeArticle(
            static_cast<int64_t>(i + 1),
            kCatPool[gen() % kCatPool.size()],
            kAuthorPool[gen() % kAuthorPool.size()],
            vc,
            (gen() % 2) == 0));
    }
    return out;
}

}  // namespace

TEST_CASE("[DeclListNin][xtier] L1/PG agree + dualité on string anti-membership",
          "[integration][db][list][nin][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE01);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<std::string>(gen, kCatPool, kXtSets);
    crossTierNinProperty<DescNinString, DualInString, std::string>(
        entities, sets, "text",
        [](const Entity& e) { return std::optional<std::string>(e.category); });
}

TEST_CASE("[DeclListNin][xtier] L1/PG agree + dualité on int64 anti-membership",
          "[integration][db][list][nin][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE02);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<int64_t>(gen, kAuthorPool, kXtSets);
    crossTierNinProperty<DescNinInt64, DualInInt64, int64_t>(
        entities, sets, "int8",
        [](const Entity& e) { return std::optional<int64_t>(e.author_id); });
}

TEST_CASE("[DeclListNin][xtier] L1/PG agree + NULL break on int32 optional anti-membership",
          "[integration][db][list][nin][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE03);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<int32_t>(gen, kViewPool, kXtSets);
    // view_count is std::optional<int32_t> — ~20% null entities: a null member
    // matches neither IN nor a non-empty NIN, so the negation dualité does not hold.
    crossTierNinProperty<DescNinOptInt32, DualInOptInt32, int32_t>(
        entities, sets, "int4",
        [](const Entity& e) { return e.view_count; });
}

TEST_CASE("[DeclListNin][xtier] L1/PG agree + dualité on bool anti-membership",
          "[integration][db][list][nin][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE04);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<bool>(gen, std::vector<bool>{false, true}, kXtSets);
    crossTierNinProperty<DescNinBool, DualInBool, bool>(
        entities, sets, "bool",
        [](const Entity& e) { return std::optional<bool>(e.is_published); });
}
