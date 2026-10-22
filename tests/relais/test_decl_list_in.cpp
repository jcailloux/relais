/**
 * test_decl_list_in.cpp
 * The IN filter operator on every tier. Invariant: L1 and L3 give the same
 * verdict — "entity scalar ∈ query set".
 *
 * L1 matching (transport-free, hand-built descriptors with IN filters and a
 * directly-populated Filters set) through the free `matchesFilters<Descriptor>(
 * entity, filters)`, the real L1 invalidation path:
 * - IN match / mismatch per element type: string, int64, int32, bool
 * - singleton {x} ≡ EQ x
 * - empty set {} ⇒ no entity matches
 * - combinations EQ + IN + range
 * - optional member null ∉ any set
 *
 * L3 SQL generation, binary group key and HTTP parsing (transport-free).
 *
 * Cross-tier property suite: a fixed-seed test forces L1 `matchesFilters` and
 * L3 PostgreSQL `= ANY` through the same (entity, set) pairs and asserts
 * identical verdicts. Covers all four element encodings
 * (string/int64/int32-optional/bool), negatives/extremes, empty sets, null
 * members, and array-literal escaping that round-trips through PG.
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <unordered_map>

#include "fixtures/test_helper.h"
#include "fixtures/generated/TestArticleEntity.h"
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/GeneratedCriteria.h>
#include <jcailloux/relais/list/spec/HttpQueryParser.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>

namespace decl = jcailloux::relais::list::spec;
using relais_test::TestArticle;
using relais_test::sync;              // relais_test::sync, not POSIX ::sync from <unistd.h>
using relais_test::TransactionGuard;
using Entity = entity::generated::TestArticleEntity;

// =============================================================================
// Hand-built descriptors with IN filters, independent of the generated fixture.
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
    f.get<"category">() = std::vector<std::string>{"science", "tech"};

    CHECK(decl::detail::matchesFilters<DescInString>(makeArticle(1, "tech", 0), f));
    CHECK(decl::detail::matchesFilters<DescInString>(makeArticle(2, "science", 0), f));
    CHECK_FALSE(decl::detail::matchesFilters<DescInString>(makeArticle(3, "news", 0), f));
    CHECK_FALSE(decl::detail::matchesFilters<DescInString>(makeArticle(4, "", 0), f));
}

TEST_CASE("[DeclListIn] IN int64 membership", "[list][in][unit]") {
    decl::Filters<DescInInt64> f;
    f.get<"author_id">() = std::vector<int64_t>{1, 2, 3};

    CHECK(decl::detail::matchesFilters<DescInInt64>(makeArticle(1, "x", 2), f));
    CHECK_FALSE(decl::detail::matchesFilters<DescInInt64>(makeArticle(2, "x", 9), f));

    SECTION("negatives and extremes") {
        f.get<"author_id">() = std::vector<int64_t>{INT64_MIN, -1, 0, INT64_MAX};
        CHECK(decl::detail::matchesFilters<DescInInt64>(makeArticle(1, "x", INT64_MIN), f));
        CHECK(decl::detail::matchesFilters<DescInInt64>(makeArticle(2, "x", -1), f));
        CHECK(decl::detail::matchesFilters<DescInInt64>(makeArticle(3, "x", 0), f));
        CHECK(decl::detail::matchesFilters<DescInInt64>(makeArticle(4, "x", INT64_MAX), f));
        CHECK_FALSE(decl::detail::matchesFilters<DescInInt64>(makeArticle(5, "x", 1), f));
    }
}

TEST_CASE("[DeclListIn] IN int32 on optional member", "[list][in][unit]") {
    decl::Filters<DescInOptInt32> f;
    f.get<"view_count">() = std::vector<int32_t>{10, 20, INT32_MIN, INT32_MAX};

    CHECK(decl::detail::matchesFilters<DescInOptInt32>(makeArticle(1, "x", 0, 20), f));
    CHECK(decl::detail::matchesFilters<DescInOptInt32>(makeArticle(2, "x", 0, INT32_MIN), f));
    CHECK_FALSE(decl::detail::matchesFilters<DescInOptInt32>(makeArticle(3, "x", 0, 5), f));

    SECTION("null member is in no set") {
        CHECK_FALSE(decl::detail::matchesFilters<DescInOptInt32>(
            makeArticle(4, "x", 0, std::nullopt), f));
    }
}

TEST_CASE("[DeclListIn] IN bool membership", "[list][in][unit]") {
    decl::Filters<DescInBool> f;

    SECTION("set {true}") {
        f.get<"is_published">() = std::vector<bool>{true};
        CHECK(decl::detail::matchesFilters<DescInBool>(makeArticle(1, "x", 0, std::nullopt, true), f));
        CHECK_FALSE(decl::detail::matchesFilters<DescInBool>(makeArticle(2, "x", 0, std::nullopt, false), f));
    }
    SECTION("set {true,false} matches both") {
        f.get<"is_published">() = std::vector<bool>{false, true};
        CHECK(decl::detail::matchesFilters<DescInBool>(makeArticle(1, "x", 0, std::nullopt, true), f));
        CHECK(decl::detail::matchesFilters<DescInBool>(makeArticle(2, "x", 0, std::nullopt, false), f));
    }
}

// =============================================================================
// Singleton ≡ EQ ; empty set ; inactive filter
// =============================================================================

TEST_CASE("[DeclListIn] singleton set behaves like EQ", "[list][in][unit]") {
    decl::Filters<DescInString> f;
    f.get<"category">() = std::vector<std::string>{"tech"};

    CHECK(decl::detail::matchesFilters<DescInString>(makeArticle(1, "tech", 0), f));
    CHECK_FALSE(decl::detail::matchesFilters<DescInString>(makeArticle(2, "science", 0), f));
    CHECK_FALSE(decl::detail::matchesFilters<DescInString>(makeArticle(3, "news", 0), f));
}

TEST_CASE("[DeclListIn] empty set matches nothing", "[list][in][unit]") {
    decl::Filters<DescInString> f;
    // Programmatic empty set: present-but-empty optional<vector>.
    f.get<"category">() = std::vector<std::string>{};
    REQUIRE(f.get<"category">().has_value());

    CHECK_FALSE(decl::detail::matchesFilters<DescInString>(makeArticle(1, "tech", 0), f));
    CHECK_FALSE(decl::detail::matchesFilters<DescInString>(makeArticle(2, "", 0), f));
}

TEST_CASE("[DeclListIn] inactive IN filter matches every entity", "[list][in][unit]") {
    decl::Filters<DescInString> f;  // filter left unset (nullopt)
    REQUIRE_FALSE(f.get<"category">().has_value());

    CHECK(decl::detail::matchesFilters<DescInString>(makeArticle(1, "tech", 0), f));
    CHECK(decl::detail::matchesFilters<DescInString>(makeArticle(2, "anything", 0), f));
}

// =============================================================================
// Combination EQ + IN + range
// =============================================================================

TEST_CASE("[DeclListIn] EQ + IN + range combination", "[list][in][unit]") {
    decl::Filters<DescCombo> f;
    f.get<"author_id">() = int64_t{42};                              // EQ author_id == 42
    f.get<"category">() = std::vector<std::string>{"news", "tech"};  // IN category
    f.get<"view_count">() = int32_t{10};                              // GE view_count >= 10

    SECTION("all three satisfied") {
        CHECK(decl::detail::matchesFilters<DescCombo>(makeArticle(1, "tech", 42, 15), f));
        CHECK(decl::detail::matchesFilters<DescCombo>(makeArticle(2, "news", 42, 10), f));
    }
    SECTION("EQ fails") {
        CHECK_FALSE(decl::detail::matchesFilters<DescCombo>(makeArticle(3, "tech", 7, 15), f));
    }
    SECTION("IN fails") {
        CHECK_FALSE(decl::detail::matchesFilters<DescCombo>(makeArticle(4, "sports", 42, 15), f));
    }
    SECTION("range fails") {
        CHECK_FALSE(decl::detail::matchesFilters<DescCombo>(makeArticle(5, "tech", 42, 5), f));
    }
    SECTION("range filter on null member excludes (range, not IN)") {
        CHECK_FALSE(decl::detail::matchesFilters<DescCombo>(makeArticle(6, "tech", 42, std::nullopt), f));
    }
    SECTION("only IN active — EQ and range inactive") {
        decl::Filters<DescCombo> g;
        g.get<"category">() = std::vector<std::string>{"tech"};
        CHECK(decl::detail::matchesFilters<DescCombo>(makeArticle(7, "tech", 999, std::nullopt), g));
        CHECK_FALSE(decl::detail::matchesFilters<DescCombo>(makeArticle(8, "other", 999, std::nullopt), g));
    }
}

// =============================================================================
// L3 SQL generation — buildWhereClause IN branch (transport-free).
// Validates the `= ANY($n)` clause, the single-array-param invariant, $n
// numbering when combined with EQ/range, empty-set literal, and escaping.
// (Live-DB round-trip + cursor/offset coverage lands with the IN fixtures.)
// =============================================================================

namespace {

std::string paramStr(const jcailloux::relais::io::detail::PgParam& p) {
    return p.isNull() ? std::string("<null>")
                      : std::string(p.data(), static_cast<size_t>(p.length()));
}

}  // namespace

TEST_CASE("[DeclListIn] SQL: IN emits = ANY with one array param", "[list][in][unit][sql]") {
    decl::Filters<DescInString> f;
    f.get<"category">() = std::vector<std::string>{"science", "tech"};

    auto wc = decl::detail::buildWhereClause<DescInString>(f);
    CHECK(wc.sql == "\"category\" = ANY($1)");
    REQUIRE(wc.params.params.size() == 1);          // exactly one param — the array
    CHECK(paramStr(wc.params.params[0]) == "{science,tech}");  // order preserved here
    CHECK(wc.next_param == 2);
}

TEST_CASE("[DeclListIn] SQL: IN int64 array literal", "[list][in][unit][sql]") {
    decl::Filters<DescInInt64> f;
    f.get<"author_id">() = std::vector<int64_t>{1, 2, 3};

    auto wc = decl::detail::buildWhereClause<DescInInt64>(f);
    CHECK(wc.sql == "\"author_id\" = ANY($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{1,2,3}");

    SECTION("negatives are not quoted") {
        f.get<"author_id">() = std::vector<int64_t>{-2, -1, 0};
        auto wc2 = decl::detail::buildWhereClause<DescInInt64>(f);
        CHECK(paramStr(wc2.params.params[0]) == "{-2,-1,0}");
    }
}

TEST_CASE("[DeclListIn] SQL: singleton set", "[list][in][unit][sql]") {
    decl::Filters<DescInString> f;
    f.get<"category">() = std::vector<std::string>{"tech"};

    auto wc = decl::detail::buildWhereClause<DescInString>(f);
    CHECK(wc.sql == "\"category\" = ANY($1)");
    CHECK(paramStr(wc.params.params[0]) == "{tech}");
}

TEST_CASE("[DeclListIn] SQL: empty set yields = ANY('{}')", "[list][in][unit][sql]") {
    decl::Filters<DescInString> f;
    f.get<"category">() = std::vector<std::string>{};  // present-but-empty
    REQUIRE(f.get<"category">().has_value());

    auto wc = decl::detail::buildWhereClause<DescInString>(f);
    CHECK(wc.sql == "\"category\" = ANY($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{}");  // ANY('{}') → zero rows
}

TEST_CASE("[DeclListIn] SQL: strings with array delimiters are quoted/escaped",
          "[list][in][unit][sql]") {
    decl::Filters<DescInString> f;
    f.get<"category">() = std::vector<std::string>{"a,b", "x{y}", "q\"z", ""};

    auto wc = decl::detail::buildWhereClause<DescInString>(f);
    // Each special element is quoted; embedded quote is backslash-escaped; empty
    // string is quoted. Plain elements would stay bare (none here).
    CHECK(paramStr(wc.params.params[0]) == R"({"a,b","x{y}","q\"z",""})");
}

TEST_CASE("[DeclListIn] SQL: EQ + IN + range $n numbering", "[list][in][unit][sql]") {
    decl::Filters<DescCombo> f;
    f.get<"author_id">() = int64_t{42};                               // EQ author_id
    f.get<"category">() = std::vector<std::string>{"news", "tech"};  // IN category
    f.get<"view_count">() = int32_t{10};                               // GE view_count

    auto wc = decl::detail::buildWhereClause<DescCombo>(f);
    CHECK(wc.sql == "\"author_id\"=$1 AND \"category\" = ANY($2) AND \"view_count\">=$3");
    REQUIRE(wc.params.params.size() == 3);     // IN contributes exactly one param
    CHECK(paramStr(wc.params.params[0]) == "42");
    CHECK(paramStr(wc.params.params[1]) == "{news,tech}");
    CHECK(paramStr(wc.params.params[2]) == "10");
    CHECK(wc.next_param == 4);                  // cursor/offset params would start at $4

    SECTION("only IN active — EQ/range skipped, IN takes $1") {
        decl::Filters<DescCombo> g;
        g.get<"category">() = std::vector<std::string>{"tech"};
        auto wc2 = decl::detail::buildWhereClause<DescCombo>(g);
        CHECK(wc2.sql == "\"category\" = ANY($1)");
        REQUIRE(wc2.params.params.size() == 1);
        CHECK(wc2.next_param == 2);
    }
}

// =============================================================================
// Binary group key — groupCacheKey IN canonicalization, transport-free.
// The group_key is the cache identity of a filtered list: {a,b}, {b,a}, {a,a,b}
// MUST produce byte-identical keys or the cache fragments and the master hash
// bloats. Canonicalization (sort+unique) lives in groupCacheKey itself so even
// programmatically-built filters hash stably.
// =============================================================================

namespace {

template<typename Desc, typename Vec>
std::string groupKeyForSet(Vec v) {
    decl::ListQueryParams<Desc> q;
    std::get<0>(q.filters.values) = std::move(v);
    return decl::detail::groupKey<Desc>(q.filters, q.sort);
}

}  // namespace

TEST_CASE("[DeclListIn] binary: group key is canonical under order/dups",
          "[list][in][unit][binary]") {
    using V = std::vector<std::string>;
    const std::string ab  = groupKeyForSet<DescInString>(V{"a", "b"});
    const std::string ba  = groupKeyForSet<DescInString>(V{"b", "a"});
    const std::string aab = groupKeyForSet<DescInString>(V{"a", "a", "b"});

    CHECK(ab == ba);   // order-independent
    CHECK(ab == aab);  // duplicate-independent
}

TEST_CASE("[DeclListIn] binary: int64 set canonicalization", "[list][in][unit][binary]") {
    using V = std::vector<int64_t>;
    CHECK(groupKeyForSet<DescInInt64>(V{1, 2, 3})
          == groupKeyForSet<DescInInt64>(V{3, 1, 2, 1}));
    // A different set must NOT collide.
    CHECK(groupKeyForSet<DescInInt64>(V{1, 2, 3})
          != groupKeyForSet<DescInInt64>(V{1, 2, 4}));
}

TEST_CASE("[DeclListIn] binary: presence byte and count layout", "[list][in][unit][binary]") {
    // Inactive filter: a single 0x00 presence byte, no count, no elements.
    decl::ListQueryParams<DescInInt64> q_inactive;
    const std::string key_inactive = decl::detail::groupKey<DescInInt64>(q_inactive.filters, q_inactive.sort);

    // Present-but-empty: presence 0x01 + count 0 (4 bytes) + no elements.
    const std::string key_empty = groupKeyForSet<DescInInt64>(std::vector<int64_t>{});

    // Singleton: presence 0x01 + count 1 + 8-byte element.
    const std::string key_one = groupKeyForSet<DescInInt64>(std::vector<int64_t>{7});

    CHECK(key_inactive != key_empty);  // absent ≠ present-but-empty
    CHECK(key_empty != key_one);
    CHECK(key_empty.size() < key_one.size());  // empty set carries no element bytes
}

// =============================================================================
// HTTP parsing — parseListQuery / parseListQueryStrict IN branch.
// =============================================================================

namespace {

using Params = std::unordered_map<std::string, std::string>;

}  // namespace

TEST_CASE("[DeclListIn] parse: CSV set, sorted and deduped", "[list][in][unit][parse]") {
    auto q = decl::parseListQuery<DescInString>(Params{{"category", "tech,science"}});
    REQUIRE(q.filters().get<"category">().has_value());
    CHECK(*q.filters().get<"category">() == std::vector<std::string>{"science", "tech"});  // sorted
}

TEST_CASE("[DeclListIn] parse: duplicates collapse", "[list][in][unit][parse]") {
    auto q = decl::parseListQuery<DescInString>(Params{{"category", "tech,tech,tech"}});
    REQUIRE(q.filters().get<"category">().has_value());
    CHECK(*q.filters().get<"category">() == std::vector<std::string>{"tech"});
}

TEST_CASE("[DeclListIn] parse: order-independent group key", "[list][in][unit][parse]") {
    auto a = decl::parseListQuery<DescInString>(Params{{"category", "science,tech"}});
    auto b = decl::parseListQuery<DescInString>(Params{{"category", "tech,science"}});
    CHECK(a.groupKey() == b.groupKey());
}

TEST_CASE("[DeclListIn] parse: invalid int elements are dropped", "[list][in][unit][parse]") {
    auto q = decl::parseListQuery<DescInInt64>(Params{{"author_id", "1,abc,3"}});
    REQUIRE(q.filters().get<"author_id">().has_value());
    CHECK(*q.filters().get<"author_id">() == std::vector<int64_t>{1, 3});
}

TEST_CASE("[DeclListIn] parse: no valid element leaves filter inactive (no HTTP empty set)",
          "[list][in][unit][parse]") {
    auto q = decl::parseListQuery<DescInInt64>(Params{{"author_id", "abc,xyz"}});
    CHECK_FALSE(q.filters().get<"author_id">().has_value());  // unfiltered, not empty-set
}

TEST_CASE("[DeclListIn] parse: element count capped at 256", "[list][in][unit][parse]") {
    std::string csv;
    for (int i = 0; i < 300; ++i) {
        if (i) csv += ',';
        csv += std::to_string(i);
    }
    auto q = decl::parseListQuery<DescInInt64>(Params{{"author_id", csv}});
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

TEST_CASE("[DeclListIn] parse: strict rejects a set above 256 distinct elements",
          "[list][in][unit][parse]") {
    SECTION("257 distinct → InvalidValue on the filter") {
        auto r = decl::parseListQueryStrict<DescInInt64>(Params{{"author_id", intCsv(257, 257)}});
        REQUIRE_FALSE(r.has_value());
        CHECK(r.error().type == decl::QueryValidationError::Type::InvalidValue);
        CHECK(r.error().field == "author_id");
    }
    SECTION("256 distinct → accepted whole") {
        auto r = decl::parseListQueryStrict<DescInInt64>(Params{{"author_id", intCsv(256, 256)}});
        REQUIRE(r.has_value());
        CHECK(r->filters().get<"author_id">()->size() == 256);
    }
    SECTION("duplicates do not count toward the cap") {
        auto r = decl::parseListQueryStrict<DescInInt64>(Params{{"author_id", intCsv(400, 200)}});
        REQUIRE(r.has_value());
        CHECK(r->filters().get<"author_id">()->size() == 200);
    }
}

TEST_CASE("[DeclListIn] parse: strict rejects undeclared filter, accepts IN",
          "[list][in][unit][parse]") {
    SECTION("undeclared param → error") {
        auto r = decl::parseListQueryStrict<DescInString>(Params{{"bogus", "x"}});
        REQUIRE_FALSE(r.has_value());
    }
    SECTION("well-formed IN → ok, canonical set") {
        auto r = decl::parseListQueryStrict<DescInString>(Params{{"category", "tech,science,tech"}});
        REQUIRE(r.has_value());
        REQUIRE(r->filters().get<"category">().has_value());
        CHECK(*r->filters().get<"category">() == std::vector<std::string>{"science", "tech"});
    }
}

// =============================================================================
// Cross-tier consistency — L1 and L3 agree under load.
//
// For each (entity, set) pair both tiers must agree on "entity scalar ∈ set":
// L1 `matchesFilters` (pure C++) and L3 PostgreSQL `= ANY` (the real array
// parser, fed buildWhereClause's array literal so escaping round-trips through
// PG). Data is generated from a fixed seed (deterministic, reproducible).
// =============================================================================

namespace {

// Real PostgreSQL membership verdict. `cast` is the element's pg type; the array
// literal is exactly what buildWhereClause emits, so PG's array parser sees the
// production escaping. CASE collapses NULL/false to 0 (null member ∉ any set).
template<typename V>
bool sqlAnyVerdict(const V& val, const std::string& arrayLiteral, const char* cast) {
    std::string sql = "SELECT CASE WHEN $1::";
    sql += cast; sql += " = ANY($2::"; sql += cast; sql += "[]) THEN 1 ELSE 0 END";
    auto r = relais_test::execQueryArgs(sql.c_str(), val, arrayLiteral);
    return r[0].template get<int32_t>(0) == 1;
}

// getField : Entity -> std::optional<Elem> (null for an absent optional member).
template<typename Desc, typename Elem, typename Getter>
void crossTierProperty(const std::vector<Entity>& entities,
                       const std::vector<std::vector<Elem>>& sets,
                       const char* cast, Getter getField) {
    for (const auto& e : entities) {
        for (size_t i = 0; i < sets.size(); ++i) {
            decl::Filters<Desc> f;
            std::get<0>(f.values) = sets[i];

            const bool l1 = decl::detail::matchesFilters<Desc>(e, f);

            auto wc = decl::detail::buildWhereClause<Desc>(f);
            const std::string arr = paramStr(wc.params.params[0]);
            const auto fld = getField(e);
            const bool l3 = fld.has_value() && sqlAnyVerdict(*fld, arr, cast);

            CAPTURE(e.id, i, arr, l1, l3);
            CHECK(l1 == l3);   // L1 vs L3 SQL
        }
    }
}

// Distinct sets (size 0..5) sampled from `pool`; built via std::set so each set
// is sorted+unique, then deduped across sets to avoid group-key collisions. The
// empty set (size 0) is reachable and exercises the present-but-empty group.
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
        if (gen() % 5 != 0)  // ~20% null view_count
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

TEST_CASE("[DeclListIn][xtier] L1/L3 agree on string membership",
          "[integration][db][list][in][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE01);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<std::string>(gen, kCatPool, kXtSets);
    crossTierProperty<DescInString, std::string>(
        entities, sets, "text",
        [](const Entity& e) { return std::optional<std::string>(e.category); });
}

TEST_CASE("[DeclListIn][xtier] L1/L3 agree on int64 membership",
          "[integration][db][list][in][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE02);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<int64_t>(gen, kAuthorPool, kXtSets);
    crossTierProperty<DescInInt64, int64_t>(
        entities, sets, "int8",
        [](const Entity& e) { return std::optional<int64_t>(e.author_id); });
}

TEST_CASE("[DeclListIn][xtier] L1/L3 agree on int32 optional membership",
          "[integration][db][list][in][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE03);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<int32_t>(gen, kViewPool, kXtSets);
    // view_count is std::optional<int32_t> — null members must read false in both tiers.
    crossTierProperty<DescInOptInt32, int32_t>(
        entities, sets, "int4",
        [](const Entity& e) { return e.view_count; });
}

TEST_CASE("[DeclListIn][xtier] L1/L3 agree on bool membership",
          "[integration][db][list][in][xtier]") {
    TransactionGuard tx;
    std::mt19937 gen(0xC0FFEE04);
    auto entities = makeEntities(gen, kXtEntities);
    auto sets = makeSets<bool>(gen, std::vector<bool>{false, true}, kXtSets);
    crossTierProperty<DescInBool, bool>(
        entities, sets, "bool",
        [](const Entity& e) { return std::optional<bool>(e.is_published); });
}
