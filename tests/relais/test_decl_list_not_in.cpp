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
 * Commit 5 adds the L2 selective-invalidation tests (§7.4 create, §7.5 update),
 * exercising the factored Lua `fmatch` (`is_set` shared cursor advance, NIN's only
 * own line `(fo==35 and hit)`). These require DB+Redis (TransactionGuard). NIN
 * placed first/middle/last proves `skipset` keeps the following filter aligned;
 * IN+NIN coexistence proves both set ops route through `skipset` without cursor
 * collision; the update cases replay middle-alignment against the SECOND duplicated
 * Lua script. NULL and empty-set (§1.1/§1.2) verdicts are checked at the L2 tier.
 *
 * Commit 6 adds the cross-tier consistency + dualité property suite (§7.7): a
 * deterministic (fixed-seed) sweep of ~25 entities (some with NULL members) ×
 * ~10 varied sets per element type, asserting the §1 invariant. (1) Tri-tier:
 * the NIN verdict from L1 `matchesFilters`, L2 Lua (page deleted ⇔ matched), and
 * L3 PostgreSQL `!= ALL` is identical. (2) Dualité non-null: for a present member
 * `NIN(S,v) == !IN(S,v)`. (3) Rupture sur NULL: for a null member BOTH
 * `NIN(S,NULL)` and `IN(S,NULL)` are false — the dualité does NOT hold (§1.1),
 * the single most natural implementer error (`NIN = !IN` everywhere). Requires
 * DB+Redis.
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/generated/TestArticleEntity.h"
#include <jcailloux/relais/cache/RedisCache.h>
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

    SECTION("null member is excluded from NOT IN (NOT the negation of IN, §1.1)") {
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
// Singleton ≡ NE ; empty set = universe (§1.2) ; inactive filter
// =============================================================================

TEST_CASE("[DeclListNin] singleton set behaves like NE", "[list][nin][unit]") {
    decl::Filters<DescNinString> f;
    f.get<"category">() = std::vector<std::string>{"tech"};

    CHECK_FALSE(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));  // == tech
    CHECK(l1Match<DescNinString>(makeArticle(2, "science", 0), f));     // != tech
    CHECK(l1Match<DescNinString>(makeArticle(3, "news", 0), f));        // != tech
}

TEST_CASE("[DeclListNin] empty set matches everything (NOT IN {} = universe, §1.2)",
          "[list][nin][unit]") {
    decl::Filters<DescNinString> f;
    // Programmatic empty set: present-but-empty optional<vector>. The exact
    // opposite of IN {} (which matches nothing).
    f.get<"category">() = std::vector<std::string>{};
    REQUIRE(f.get<"category">().has_value());

    CHECK(l1Match<DescNinString>(makeArticle(1, "tech", 0), f));
    CHECK(l1Match<DescNinString>(makeArticle(2, "", 0), f));
}

TEST_CASE("[DeclListNin] empty set still excludes a null optional member (§1.1)",
          "[list][nin][unit]") {
    // NOT IN {} = universe, but a NULL member is excluded from every set result,
    // empty included — the null guard wins over the empty-set universe.
    decl::Filters<DescNinOptInt32> f;
    f.get<"view_count">() = std::vector<int32_t>{};
    REQUIRE(f.get<"view_count">().has_value());

    CHECK(l1Match<DescNinOptInt32>(makeArticle(1, "x", 0, 5), f));               // non-null ∉ {}
    CHECK_FALSE(l1Match<DescNinOptInt32>(makeArticle(2, "x", 0, std::nullopt), f)); // null excluded
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
    SECTION("range filter on null member excludes (range null → NE-only rule)") {
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
    f.get<"category">() = std::vector<std::string>{"science", "tech"};

    auto wc = decl::buildWhereClause<DescNinString>(f);
    CHECK(wc.sql == "\"category\" != ALL($1)");
    REQUIRE(wc.params.params.size() == 1);          // exactly one param — the array
    CHECK(paramStr(wc.params.params[0]) == "{science,tech}");
    CHECK(wc.next_param == 2);
}

TEST_CASE("[DeclListNin] SQL: NIN int64 array literal", "[list][nin][unit][sql]") {
    decl::Filters<DescNinInt64> f;
    f.get<"author_id">() = std::vector<int64_t>{1, 2, 3};

    auto wc = decl::buildWhereClause<DescNinInt64>(f);
    CHECK(wc.sql == "\"author_id\" != ALL($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{1,2,3}");
}

TEST_CASE("[DeclListNin] SQL: singleton NIN behaves like != x", "[list][nin][unit][sql]") {
    decl::Filters<DescNinString> f;
    f.get<"category">() = std::vector<std::string>{"tech"};

    auto wc = decl::buildWhereClause<DescNinString>(f);
    CHECK(wc.sql == "\"category\" != ALL($1)");      // != ALL('{tech}') ≡ != 'tech'
    CHECK(paramStr(wc.params.params[0]) == "{tech}");
}

TEST_CASE("[DeclListNin] SQL: empty set yields != ALL('{}') (universe, §1.2)",
          "[list][nin][unit][sql]") {
    decl::Filters<DescNinString> f;
    f.get<"category">() = std::vector<std::string>{};  // present-but-empty
    REQUIRE(f.get<"category">().has_value());

    auto wc = decl::buildWhereClause<DescNinString>(f);
    CHECK(wc.sql == "\"category\" != ALL($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{}");    // != ALL('{}') → TRUE → all rows
}

TEST_CASE("[DeclListNin] SQL: EQ + NIN + range $n numbering", "[list][nin][unit][sql]") {
    decl::Filters<DescCombo> f;
    f.get<"author_id">() = int64_t{42};                                  // EQ author_id
    f.get<"category">() = std::vector<std::string>{"news", "spam"};     // NIN category
    f.get<"view_count">() = int32_t{10};                                  // GE view_count

    auto wc = decl::buildWhereClause<DescCombo>(f);
    CHECK(wc.sql == "\"author_id\"=$1 AND \"category\" != ALL($2) AND \"view_count\">=$3");
    REQUIRE(wc.params.params.size() == 3);     // NIN contributes exactly one param
    CHECK(paramStr(wc.params.params[0]) == "42");
    CHECK(paramStr(wc.params.params[1]) == "{news,spam}");
    CHECK(paramStr(wc.params.params[2]) == "10");
    CHECK(wc.next_param == 4);                  // cursor/offset params would start at $4

    SECTION("only NIN active — EQ/range skipped, NIN takes $1") {
        decl::Filters<DescCombo> g;
        g.get<"category">() = std::vector<std::string>{"spam"};
        auto wc2 = decl::buildWhereClause<DescCombo>(g);
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

TEST_CASE("[DeclListNin] parse: empty value leaves NIN inactive (≡ NOT IN {} = universe, §1.2)",
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

// =============================================================================
// L2 selective invalidation (§7.4 create, §7.5 update) — the Lua `fmatch` factored
// for IN(@=64)/NIN(#=35). A group's cached page is invalidated iff the created/
// updated entity WOULD belong to that group's filter. For a NIN filter that means
// the entity's scalar is NOT in the group's set (verdict inverted from IN).
//
// Each page is registered as a 1-byte value "x" — shorter than the bounds header,
// so the Lua `chk*` short-circuits to "delete". The surviving variable is therefore
// purely whether `fmatch` matched. Membership is read back via EXISTS. Each
// TEST_CASE/SECTION starts on a flushed Redis (TransactionGuard).
// =============================================================================

namespace {

// NIN in FIRST position (EQ after it) — alignment: the filter following the set
// must read at the right cursor once skipset has consumed the set.
struct DescNinFirst {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::NIN>{},
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::EQ>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

// NIN in LAST position.
struct DescNinLast {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::EQ>{},
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::NIN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

namespace cache_ns = jcailloux::relais::cache;
using jcailloux::relais::PgProvider;

// Any prefix works — the Lua only strips prefixLen bytes; the suffix must equal
// groupCacheKey's canonical blob. Mirror ListMixin's "<name>:dlist:g:" shape.
constexpr std::string_view kNinPrefix = "T:dlist:g:";  // 10 bytes
constexpr size_t kNinPrefixLen = 10;
const std::string kNinMaster = "test:nin:l2:master";

template<typename Desc>
std::string registerNinGroup(const decl::ListQueryParams<Desc>& q) {
    std::string groupKey = std::string(kNinPrefix) + decl::groupKey<Desc>(q.filters, q.sort);
    std::string pageKey = groupKey + ":p";
    sync(PgProvider::redis("SET", pageKey, "x"));               // < header → chk true
    sync(PgProvider::redis("SADD", groupKey + ":_keys", pageKey));
    sync(PgProvider::redis("HSET", kNinMaster, groupKey, "0")); // sort field index 0
    return pageKey;
}

bool alive(const std::string& pageKey) {
    return sync(PgProvider::redis("EXISTS", pageKey)).asInteger() == 1;
}

template<typename Desc>
size_t fireCreate(const Entity& e) {
    return sync(cache_ns::RedisCache::invalidateListGroupsSelective(
        kNinMaster, kNinPrefixLen, decl::filterSchema<Desc>(),
        decl::encodeEntityFilterBlob<Desc>(e), "0"));
}

template<typename Desc>
size_t fireUpdate(const Entity& oldE, const Entity& newE) {
    return sync(cache_ns::RedisCache::invalidateListGroupsSelectiveUpdate(
        kNinMaster, kNinPrefixLen, decl::filterSchema<Desc>(),
        decl::encodeEntityFilterBlob<Desc>(newE), "0",
        decl::encodeEntityFilterBlob<Desc>(oldE), "0"));
}

}  // namespace

TEST_CASE("[DeclListNin][L2] create invalidates groups whose set EXCLUDES the value",
          "[integration][db][redis][list][nin][l2]") {
    TransactionGuard tx;

    auto mk = [](std::vector<std::string> set) {
        decl::ListQueryParams<DescNinString> q;
        q.filters.get<"category">() = std::move(set);
        return registerNinGroup<DescNinString>(q);
    };
    auto g_sd = mk({"spam", "draft"});
    auto g_n  = mk({"news"});
    auto g_ns = mk({"news", "sports"});

    SECTION("category=news") {
        fireCreate<DescNinString>(makeArticle(1, "news", 0));
        CHECK_FALSE(alive(g_sd));  // news ∉ {spam,draft} → matches NOT IN → invalidated
        CHECK(alive(g_n));         // news ∈ {news} → no match → kept
        CHECK(alive(g_ns));        // news ∈ {news,sports} → no match → kept
    }
    SECTION("category=spam") {
        fireCreate<DescNinString>(makeArticle(2, "spam", 0));
        CHECK(alive(g_sd));        // spam ∈ {spam,draft} → no match → kept
        CHECK_FALSE(alive(g_n));   // spam ∉ {news} → matches → invalidated
        CHECK_FALSE(alive(g_ns));  // spam ∉ {news,sports} → matches → invalidated
    }
}

TEST_CASE("[DeclListNin][L2] alignment — NIN in middle position (create)",
          "[integration][db][redis][list][nin][l2]") {
    TransactionGuard tx;
    // DescCombo: [EQ author_id][NIN category][GE view_count]
    auto mk = [](int64_t author, std::vector<std::string> cats, int32_t viewGe) {
        decl::ListQueryParams<DescCombo> q;
        q.filters.get<"author_id">() = author;
        q.filters.get<"category">() = std::move(cats);
        q.filters.get<"view_count">() = viewGe;
        return registerNinGroup<DescCombo>(q);
    };
    auto gAll = mk(42, {"spam"},        10);   // EQ ok, NIN ok (science∉), range ok
    auto gNin = mk(42, {"science"},     10);   // NIN fails (science∈set)
    auto gRng = mk(42, {"spam"},        100);  // range AFTER the set fails
    auto gEq  = mk(99, {"spam"},        10);   // EQ BEFORE the set fails

    fireCreate<DescCombo>(makeArticle(1, "science", 42, 50));
    CHECK_FALSE(alive(gAll));
    CHECK(alive(gNin));  // science ∈ {science} → no NIN match
    CHECK(alive(gRng));  // 50 < 100 — proves skipset left view_count aligned
    CHECK(alive(gEq));   // author 42 ≠ 99
}

TEST_CASE("[DeclListNin][L2] alignment — NIN in first position (create)",
          "[integration][db][redis][list][nin][l2]") {
    TransactionGuard tx;
    // DescNinFirst: [NIN category][EQ author_id]
    auto mk = [](std::vector<std::string> cats, int64_t author) {
        decl::ListQueryParams<DescNinFirst> q;
        q.filters.get<"category">() = std::move(cats);
        q.filters.get<"author_id">() = author;
        return registerNinGroup<DescNinFirst>(q);
    };
    auto gMatch  = mk({"spam"}, 42);  // NIN ok (tech∉) + EQ ok
    auto gEqFail = mk({"spam"}, 99);  // EQ AFTER the set fails

    fireCreate<DescNinFirst>(makeArticle(1, "tech", 42));
    CHECK_FALSE(alive(gMatch));
    CHECK(alive(gEqFail));  // author after the set correctly mismatched
}

TEST_CASE("[DeclListNin][L2] alignment — NIN in last position (create)",
          "[integration][db][redis][list][nin][l2]") {
    TransactionGuard tx;
    // DescNinLast: [EQ author_id][NIN category]
    auto mk = [](int64_t author, std::vector<std::string> cats) {
        decl::ListQueryParams<DescNinLast> q;
        q.filters.get<"author_id">() = author;
        q.filters.get<"category">() = std::move(cats);
        return registerNinGroup<DescNinLast>(q);
    };
    auto gMatch   = mk(42, {"spam"});
    auto gNinFail = mk(42, {"tech"});
    auto gEqFail  = mk(99, {"spam"});

    fireCreate<DescNinLast>(makeArticle(1, "tech", 42));
    CHECK_FALSE(alive(gMatch));
    CHECK(alive(gNinFail));  // tech ∈ {tech} → no NIN match
    CHECK(alive(gEqFail));   // author 42 ≠ 99
}

TEST_CASE("[DeclListNin][L2] IN and NIN coexist — both route through skipset (create)",
          "[integration][db][redis][list][nin][l2]") {
    TransactionGuard tx;
    // DescInNin: [IN author_id][NIN category]. Proves the two set ops both advance
    // via skipset without colliding on the cursor.
    auto mk = [](std::vector<int64_t> authors, std::vector<std::string> cats) {
        decl::ListQueryParams<DescInNin> q;
        q.filters.get<"author_id">() = std::move(authors);
        q.filters.get<"category">() = std::move(cats);
        return registerNinGroup<DescInNin>(q);
    };
    auto gMatch  = mk({1, 2}, {"spam", "draft"});  // IN ok (1∈) AND NIN ok (tech∉)
    auto gNinNo  = mk({1, 2}, {"tech", "news"});   // NIN fails (tech∈)
    auto gInNo   = mk({8, 9}, {"spam"});           // IN fails (1∉) — NIN would pass

    fireCreate<DescInNin>(makeArticle(1, "tech", 1));
    CHECK_FALSE(alive(gMatch));
    CHECK(alive(gNinNo));  // tech ∈ {tech,news} → NIN excludes
    CHECK(alive(gInNo));   // author 1 ∉ {8,9} → IN excludes (and curseur still aligned to NIN)
}

TEST_CASE("[DeclListNin][L2] optional-null entity matches no NIN group; empty set = universe",
          "[integration][db][redis][list][nin][l2]") {
    TransactionGuard tx;
    // DescNinOptInt32: [NIN view_count] on a std::optional<int32_t> member.
    auto mk = [](std::optional<std::vector<int32_t>> set) {
        decl::ListQueryParams<DescNinOptInt32> q;
        if (set) q.filters.get<"view_count">() = std::move(*set);
        return registerNinGroup<DescNinOptInt32>(q);
    };
    auto gSet   = mk(std::vector<int32_t>{10, 20});
    auto gEmpty = mk(std::vector<int32_t>{});  // NOT IN {} = universe (§1.2)

    SECTION("non-null value excluded from set matches NIN; empty set matches all") {
        fireCreate<DescNinOptInt32>(makeArticle(1, "x", 0, 5));
        CHECK_FALSE(alive(gSet));    // 5 ∉ {10,20} → matches → invalidated
        CHECK_FALSE(alive(gEmpty));  // 5 ∉ {} → matches universe → invalidated
    }
    SECTION("null member matches no NIN group, empty included (§1.1 at L2)") {
        fireCreate<DescNinOptInt32>(makeArticle(2, "x", 0, std::nullopt));
        CHECK(alive(gSet));    // null excluded from every set result
        CHECK(alive(gEmpty));  // null excluded even from NOT IN {} (guard wins)
    }
    SECTION("value IN the set does NOT match NIN") {
        fireCreate<DescNinOptInt32>(makeArticle(3, "x", 0, 20));
        CHECK(alive(gSet));          // 20 ∈ {10,20} → no NIN match → kept
        CHECK_FALSE(alive(gEmpty));  // 20 ∉ {} → matches universe → invalidated
    }
}

TEST_CASE("[DeclListNin][L2] update invalidates when old XOR new is excluded from the set",
          "[integration][db][redis][list][nin][l2]") {
    TransactionGuard tx;
    auto mk = [](std::vector<std::string> set) {
        decl::ListQueryParams<DescNinString> q;
        q.filters.get<"category">() = std::move(set);
        return registerNinGroup<DescNinString>(q);
    };

    SECTION("asymmetric: old ∈ set (absent), new ∉ set (enters) → invalidated") {
        auto g = mk({"spam"});
        // old=spam ∈ {spam} → NIN no-match; new=news ∉ {spam} → NIN match → nm true
        fireUpdate<DescNinString>(makeArticle(1, "spam", 0), makeArticle(1, "news", 0));
        CHECK_FALSE(alive(g));
    }
    SECTION("both ∉ set → entity stays in result, position may move → invalidated") {
        auto g = mk({"spam"});
        // old=news, new=tech: both ∉ {spam} → both NIN-match → invalidated via chk_range
        fireUpdate<DescNinString>(makeArticle(1, "news", 0), makeArticle(1, "tech", 0));
        CHECK_FALSE(alive(g));
    }
    SECTION("both ∈ set → entity out of result in both states → NOT invalidated") {
        auto g = mk({"spam", "draft"});
        // old=spam, new=draft: both ∈ {spam,draft} → neither NIN-matches → no over-invalidation
        fireUpdate<DescNinString>(makeArticle(1, "spam", 0), makeArticle(1, "draft", 0));
        CHECK(alive(g));
    }
}

TEST_CASE("[DeclListNin][L2] alignment — NIN in middle position (update, 2nd Lua script)",
          "[integration][db][redis][list][nin][l2]") {
    TransactionGuard tx;
    // Replays NIN-middle alignment against the SECOND, duplicated Lua script
    // (`...Update`) to catch any copy-paste divergence from the create path.
    auto mk = [](int64_t author, std::vector<std::string> cats, int32_t viewGe) {
        decl::ListQueryParams<DescCombo> q;
        q.filters.get<"author_id">() = author;
        q.filters.get<"category">() = std::move(cats);
        q.filters.get<"view_count">() = viewGe;
        return registerNinGroup<DescCombo>(q);
    };
    auto gAll = mk(42, {"spam"}, 10);   // both old/new NIN-match (tech∉spam), range ok
    auto gRng = mk(42, {"spam"}, 100);  // range AFTER the set fails for both
    auto gEq  = mk(99, {"spam"}, 10);   // EQ BEFORE the set fails

    fireUpdate<DescCombo>(makeArticle(1, "tech", 42, 50), makeArticle(1, "tech", 42, 60));
    CHECK_FALSE(alive(gAll));
    CHECK(alive(gRng));  // 50,60 both < 100 — UPDATE skipset matches CREATE skipset
    CHECK(alive(gEq));   // author 42 ≠ 99
}

// =============================================================================
// Cross-tier consistency + dualité (§7.7) — the §1 invariant under load.
//
// For each (entity, set) the three tiers must agree on the NIN verdict "entity
// scalar ∉ set": L1 `matchesFilters` (pure C++), L3 PostgreSQL `!= ALL` (the real
// array parser, fed buildWhereClause's literal so escaping round-trips through
// PG), L2 Lua (page deleted ⇔ matched). Plus the two dualité properties:
//   (2) non-null member: NIN(S,v) == !IN(S,v) — the negation holds;
//   (3) NULL member: NIN(S,NULL) == false AND IN(S,NULL) == false — the negation
//       BREAKS (SQL 3-valued logic excludes NULL from BOTH, §1.1).
// All groups for one entity are registered into the master hash at once, then a
// single create fires and the Lua scans them — the production multi-group scan.
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

// Real PostgreSQL NIN verdict: `$1 != ALL($2)`. Only called for a present member
// (the null break is handled in C++ via has_value()), so 3-valued NULL never
// reaches PG here — `!= ALL('{}')` correctly yields TRUE (universe, §1.2).
template<typename V>
bool sqlAllVerdict(const V& val, const std::string& arrayLiteral, const char* cast) {
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
        relais_test::flushRedis();
        std::vector<std::string> pages;
        pages.reserve(sets.size());
        for (const auto& s : sets) {
            decl::ListQueryParams<DescNin> q;
            std::get<0>(q.filters.values) = s;
            pages.push_back(registerNinGroup<DescNin>(q));
        }
        fireCreate<DescNin>(e);  // one NIN create, Lua scans every registered group

        for (size_t i = 0; i < sets.size(); ++i) {
            decl::Filters<DescNin> fn;
            std::get<0>(fn.values) = sets[i];
            const bool ninL1 = decl::matchesFilters<DescNin>(e, fn);
            const bool ninL2 = !alive(pages[i]);           // deleted ⇔ matched

            auto wc = decl::buildWhereClause<DescNin>(fn);
            const std::string arr = paramStr(wc.params.params[0]);
            const auto fld = getField(e);
            const bool ninL3 = fld.has_value() && sqlAllVerdict(*fld, arr, cast);

            decl::Filters<DescIn> fi;
            std::get<0>(fi.values) = sets[i];
            const bool inL1 = decl::matchesFilters<DescIn>(e, fi);

            CAPTURE(e.id, i, arr, ninL1, ninL2, ninL3, inL1, fld.has_value());
            CHECK(ninL1 == ninL2);          // tri-tier: L1 == L2 Lua
            CHECK(ninL1 == ninL3);          // tri-tier: L1 == L3 SQL
            if (fld.has_value())
                CHECK(ninL1 == !inL1);      // §7.7.2 dualité holds for a present member
            else {
                CHECK_FALSE(ninL1);         // §7.7.3 NIN(NULL) == false
                CHECK_FALSE(inL1);          // §7.7.3 IN(NULL) == false — negation BREAKS
            }
        }
    }
}

// Distinct sets (size 0..5) from `pool`, deduped so each group key is unique. The
// empty set is reachable (NOT IN {} = universe, §1.2).
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
        if (gen() % 5 != 0)  // ~20% null view_count → exercises the NULL break (§7.7.3)
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

TEST_CASE("[DeclListNin][xtier] L1/L2/L3 agree + dualité on string anti-membership",
          "[integration][db][redis][list][nin][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE01);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<std::string>(gen, kCatPool, kXtSets);
    crossTierNinProperty<DescNinString, DualInString, std::string>(
        entities, sets, "text",
        [](const Entity& e) { return std::optional<std::string>(e.category); });
}

TEST_CASE("[DeclListNin][xtier] L1/L2/L3 agree + dualité on int64 anti-membership",
          "[integration][db][redis][list][nin][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE02);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<int64_t>(gen, kAuthorPool, kXtSets);
    crossTierNinProperty<DescNinInt64, DualInInt64, int64_t>(
        entities, sets, "int8",
        [](const Entity& e) { return std::optional<int64_t>(e.author_id); });
}

TEST_CASE("[DeclListNin][xtier] L1/L2/L3 agree + NULL break on int32 optional anti-membership",
          "[integration][db][redis][list][nin][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE03);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<int32_t>(gen, kViewPool, kXtSets);
    // view_count is std::optional<int32_t> — ~20% null entities drive §7.7.3: a null
    // member matches NEITHER NIN nor IN, so the negation dualité does not hold.
    crossTierNinProperty<DescNinOptInt32, DualInOptInt32, int32_t>(
        entities, sets, "int4",
        [](const Entity& e) { return e.view_count; });
}

TEST_CASE("[DeclListNin][xtier] L1/L2/L3 agree + dualité on bool anti-membership",
          "[integration][db][redis][list][nin][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE04);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<bool>(gen, std::vector<bool>{false, true}, kXtSets);
    crossTierNinProperty<DescNinBool, DualInBool, bool>(
        entities, sets, "bool",
        [](const Entity& e) { return std::optional<bool>(e.is_published); });
}
