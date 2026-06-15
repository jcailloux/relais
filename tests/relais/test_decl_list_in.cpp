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
 *
 * Plus (commit 6) §7.4/§7.5 L2 selective invalidation via the Redis Lua scripts
 * (`invalidateListGroupsSelective` + `...Update`): the new `skipset`/`cmpin`/
 * `fmatch` IN path tested directly against Redis — a page stored without an SR
 * SortBounds header makes `chk` unconditionally true, so the delete decision
 * reduces to pure filter matching. Multi-filter alignment (IN in first/middle/
 * last position) is the anti-regression for `skipset` cursor desync.
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <vector>

#include <unordered_map>

#include "fixtures/test_helper.h"
#include "fixtures/generated/TestArticleEntity.h"
#include <jcailloux/relais/cache/RedisCache.h>
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

// =============================================================================
// L3 SQL generation — buildWhereClause IN branch (§7.3, transport-free).
// Validates the `= ANY($n)` clause, the single-array-param invariant, $n
// numbering when combined with EQ/range, empty-set literal, and escaping.
// (Live-DB round-trip + cursor/offset coverage lands with the IN fixtures.)
// =============================================================================

namespace {

std::string paramStr(const jcailloux::relais::io::PgParam& p) {
    return p.isNull() ? std::string("<null>")
                      : std::string(p.data(), static_cast<size_t>(p.length()));
}

}  // namespace

TEST_CASE("[DeclListIn] SQL: IN emits = ANY with one array param", "[list][in][unit][sql]") {
    decl::Filters<DescInString> f;
    f.get<0>() = std::vector<std::string>{"science", "tech"};

    auto wc = decl::buildWhereClause<DescInString>(f);
    CHECK(wc.sql == "\"category\" = ANY($1)");
    REQUIRE(wc.params.params.size() == 1);          // exactly one param — the array
    CHECK(paramStr(wc.params.params[0]) == "{science,tech}");  // order preserved here
    CHECK(wc.next_param == 2);
}

TEST_CASE("[DeclListIn] SQL: IN int64 array literal", "[list][in][unit][sql]") {
    decl::Filters<DescInInt64> f;
    f.get<0>() = std::vector<int64_t>{1, 2, 3};

    auto wc = decl::buildWhereClause<DescInInt64>(f);
    CHECK(wc.sql == "\"author_id\" = ANY($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{1,2,3}");

    SECTION("negatives are not quoted") {
        f.get<0>() = std::vector<int64_t>{-2, -1, 0};
        auto wc2 = decl::buildWhereClause<DescInInt64>(f);
        CHECK(paramStr(wc2.params.params[0]) == "{-2,-1,0}");
    }
}

TEST_CASE("[DeclListIn] SQL: singleton set", "[list][in][unit][sql]") {
    decl::Filters<DescInString> f;
    f.get<0>() = std::vector<std::string>{"tech"};

    auto wc = decl::buildWhereClause<DescInString>(f);
    CHECK(wc.sql == "\"category\" = ANY($1)");
    CHECK(paramStr(wc.params.params[0]) == "{tech}");
}

TEST_CASE("[DeclListIn] SQL: empty set yields = ANY('{}')", "[list][in][unit][sql]") {
    decl::Filters<DescInString> f;
    f.get<0>() = std::vector<std::string>{};  // present-but-empty
    REQUIRE(f.get<0>().has_value());

    auto wc = decl::buildWhereClause<DescInString>(f);
    CHECK(wc.sql == "\"category\" = ANY($1)");
    REQUIRE(wc.params.params.size() == 1);
    CHECK(paramStr(wc.params.params[0]) == "{}");  // ANY('{}') → zero rows
}

TEST_CASE("[DeclListIn] SQL: strings with array delimiters are quoted/escaped",
          "[list][in][unit][sql]") {
    decl::Filters<DescInString> f;
    f.get<0>() = std::vector<std::string>{"a,b", "x{y}", "q\"z", ""};

    auto wc = decl::buildWhereClause<DescInString>(f);
    // Each special element is quoted; embedded quote is backslash-escaped; empty
    // string is quoted. Plain elements would stay bare (none here).
    CHECK(paramStr(wc.params.params[0]) == R"({"a,b","x{y}","q\"z",""})");
}

TEST_CASE("[DeclListIn] SQL: EQ + IN + range $n numbering", "[list][in][unit][sql]") {
    decl::Filters<DescCombo> f;
    f.get<0>() = int64_t{42};                               // EQ author_id
    f.get<1>() = std::vector<std::string>{"news", "tech"};  // IN category
    f.get<2>() = int32_t{10};                               // GE view_count

    auto wc = decl::buildWhereClause<DescCombo>(f);
    CHECK(wc.sql == "\"author_id\"=$1 AND \"category\" = ANY($2) AND \"view_count\">=$3");
    REQUIRE(wc.params.params.size() == 3);     // IN contributes exactly one param
    CHECK(paramStr(wc.params.params[0]) == "42");
    CHECK(paramStr(wc.params.params[1]) == "{news,tech}");
    CHECK(paramStr(wc.params.params[2]) == "10");
    CHECK(wc.next_param == 4);                  // cursor/offset params would start at $4

    SECTION("only IN active — EQ/range skipped, IN takes $1") {
        decl::Filters<DescCombo> g;
        g.get<1>() = std::vector<std::string>{"tech"};
        auto wc2 = decl::buildWhereClause<DescCombo>(g);
        CHECK(wc2.sql == "\"category\" = ANY($1)");
        REQUIRE(wc2.params.params.size() == 1);
        CHECK(wc2.next_param == 2);
    }
}

// =============================================================================
// Binary group key (§7.1) — groupCacheKey IN canonicalization, transport-free.
// The group_key is the cache identity of a filtered list: {a,b}, {b,a}, {a,a,b}
// MUST produce byte-identical keys or the cache fragments and the master hash
// bloats. Canonicalization (sort+unique) lives in groupCacheKey itself so even
// programmatically-built filters hash stably.
// =============================================================================

namespace {

template<typename Desc, typename Vec>
std::string groupKeyForSet(Vec v) {
    decl::ListDescriptorQuery<Desc> q;
    q.filters.template get<0>() = std::move(v);
    return decl::groupCacheKey<Desc>(q);
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
    decl::ListDescriptorQuery<DescInInt64> q_inactive;
    const std::string key_inactive = decl::groupCacheKey<DescInInt64>(q_inactive);

    // Present-but-empty: presence 0x01 + count 0 (4 bytes) + no elements.
    const std::string key_empty = groupKeyForSet<DescInInt64>(std::vector<int64_t>{});

    // Singleton: presence 0x01 + count 1 + 8-byte element.
    const std::string key_one = groupKeyForSet<DescInInt64>(std::vector<int64_t>{7});

    CHECK(key_inactive != key_empty);  // absent ≠ present-but-empty
    CHECK(key_empty != key_one);
    CHECK(key_empty.size() < key_one.size());  // empty set carries no element bytes
}

// =============================================================================
// HTTP parsing (§7.2) — parseListQuery / parseListQueryStrict IN branch.
// =============================================================================

namespace {

using Params = std::unordered_map<std::string, std::string>;

}  // namespace

TEST_CASE("[DeclListIn] parse: CSV set, sorted and deduped", "[list][in][unit][parse]") {
    auto q = decl::parseListQuery<DescInString>(Params{{"category", "tech,science"}});
    REQUIRE(q.filters.get<0>().has_value());
    CHECK(*q.filters.get<0>() == std::vector<std::string>{"science", "tech"});  // sorted
}

TEST_CASE("[DeclListIn] parse: duplicates collapse", "[list][in][unit][parse]") {
    auto q = decl::parseListQuery<DescInString>(Params{{"category", "tech,tech,tech"}});
    REQUIRE(q.filters.get<0>().has_value());
    CHECK(*q.filters.get<0>() == std::vector<std::string>{"tech"});
}

TEST_CASE("[DeclListIn] parse: order-independent group key", "[list][in][unit][parse]") {
    auto a = decl::parseListQuery<DescInString>(Params{{"category", "science,tech"}});
    auto b = decl::parseListQuery<DescInString>(Params{{"category", "tech,science"}});
    CHECK(a.group_key == b.group_key);
}

TEST_CASE("[DeclListIn] parse: invalid int elements are dropped", "[list][in][unit][parse]") {
    auto q = decl::parseListQuery<DescInInt64>(Params{{"author_id", "1,abc,3"}});
    REQUIRE(q.filters.get<0>().has_value());
    CHECK(*q.filters.get<0>() == std::vector<int64_t>{1, 3});
}

TEST_CASE("[DeclListIn] parse: no valid element leaves filter inactive (no HTTP empty set)",
          "[list][in][unit][parse]") {
    auto q = decl::parseListQuery<DescInInt64>(Params{{"author_id", "abc,xyz"}});
    CHECK_FALSE(q.filters.get<0>().has_value());  // unfiltered, not empty-set
}

TEST_CASE("[DeclListIn] parse: element count capped at 256", "[list][in][unit][parse]") {
    std::string csv;
    for (int i = 0; i < 300; ++i) {
        if (i) csv += ',';
        csv += std::to_string(i);
    }
    auto q = decl::parseListQuery<DescInInt64>(Params{{"author_id", csv}});
    REQUIRE(q.filters.get<0>().has_value());
    CHECK(q.filters.get<0>()->size() == 256);
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
        REQUIRE(r->filters.get<0>().has_value());
        CHECK(*r->filters.get<0>() == std::vector<std::string>{"science", "tech"});
    }
}

// =============================================================================
// L2 selective invalidation (§7.4/§7.5) — the Redis Lua `skipset`/`cmpin`/
// `fmatch` IN path, exercised directly. A group is registered by HSET-ing its
// canonical key (prefix + groupCacheKey) into a master hash and SADD-ing one
// page into `<group>:_keys`. The page value is shorter than the SortBounds
// header, so the Lua `chk` short-circuits to "delete" — the surviving variable
// is whether `fmatch` matched. Membership of the deleted page is read back via
// EXISTS. Each TEST_CASE/SECTION starts on a flushed Redis (TransactionGuard).
// =============================================================================

namespace {

// IN in FIRST position (EQ after it) — alignment: the filter following the set
// must still read at the right cursor once skipset has consumed the set.
struct DescInFirst {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::IN>{},
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::EQ>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

// IN in LAST position.
struct DescInLast {
    using Entity = ::Entity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::EQ>{},
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::IN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

namespace cache_ns = jcailloux::relais::cache;
using jcailloux::relais::PgProvider;

// Any prefix works — the Lua only strips prefixLen bytes; the suffix must equal
// groupCacheKey's canonical blob. Mirror ListMixin's "<name>:dlist:g:" shape.
constexpr std::string_view kInPrefix = "T:dlist:g:";  // 10 bytes
constexpr size_t kInPrefixLen = 10;
const std::string kInMaster = "test:in:l2:master";

template<typename Desc>
std::string registerInGroup(const decl::ListDescriptorQuery<Desc>& q) {
    std::string groupKey = std::string(kInPrefix) + decl::groupCacheKey<Desc>(q);
    std::string pageKey = groupKey + ":p";
    sync(PgProvider::redis("SET", pageKey, "x"));               // < header → chk true
    sync(PgProvider::redis("SADD", groupKey + ":_keys", pageKey));
    sync(PgProvider::redis("HSET", kInMaster, groupKey, "0"));  // sort field index 0
    return pageKey;
}

bool alive(const std::string& pageKey) {
    return sync(PgProvider::redis("EXISTS", pageKey)).asInteger() == 1;
}

template<typename Desc>
size_t fireCreate(const Entity& e) {
    return sync(cache_ns::RedisCache::invalidateListGroupsSelective(
        kInMaster, kInPrefixLen, decl::filterSchema<Desc>(),
        decl::encodeEntityFilterBlob<Desc>(e), "0"));
}

template<typename Desc>
size_t fireUpdate(const Entity& oldE, const Entity& newE) {
    return sync(cache_ns::RedisCache::invalidateListGroupsSelectiveUpdate(
        kInMaster, kInPrefixLen, decl::filterSchema<Desc>(),
        decl::encodeEntityFilterBlob<Desc>(newE), "0",
        decl::encodeEntityFilterBlob<Desc>(oldE), "0"));
}

}  // namespace

TEST_CASE("[DeclListIn][L2] create invalidates only groups whose set contains the value",
          "[integration][db][redis][list][in][l2]") {
    TransactionGuard tx;

    auto mk = [](std::vector<std::string> set) {
        decl::ListDescriptorQuery<DescInString> q;
        q.filters.get<0>() = std::move(set);
        return registerInGroup<DescInString>(q);
    };
    auto g_ts = mk({"science", "tech"});
    auto g_t  = mk({"tech"});
    auto g_ns = mk({"news", "sports"});

    SECTION("category=tech") {
        fireCreate<DescInString>(makeArticle(1, "tech", 0));
        CHECK_FALSE(alive(g_ts));  // tech ∈ {science,tech}
        CHECK_FALSE(alive(g_t));   // tech ∈ {tech}
        CHECK(alive(g_ns));        // tech ∉ {news,sports}
    }
    SECTION("category=science") {
        fireCreate<DescInString>(makeArticle(2, "science", 0));
        CHECK_FALSE(alive(g_ts));  // science ∈ {science,tech}
        CHECK(alive(g_t));         // science ∉ {tech}
        CHECK(alive(g_ns));
    }
}

TEST_CASE("[DeclListIn][L2] alignment — IN in middle position (create)",
          "[integration][db][redis][list][in][l2]") {
    TransactionGuard tx;
    // DescCombo: [EQ author_id][IN category][GE view_count]
    auto mk = [](int64_t author, std::vector<std::string> cats, int32_t viewGe) {
        decl::ListDescriptorQuery<DescCombo> q;
        q.filters.get<0>() = author;
        q.filters.get<1>() = std::move(cats);
        q.filters.get<2>() = viewGe;
        return registerInGroup<DescCombo>(q);
    };
    auto gAll  = mk(42, {"news", "tech"}, 10);   // EQ ok, IN ok, range ok
    auto gIn   = mk(42, {"sports"},       10);   // IN fails
    auto gRng  = mk(42, {"tech"},         100);  // range AFTER the set fails
    auto gEq   = mk(99, {"tech"},         10);   // EQ BEFORE the set fails

    fireCreate<DescCombo>(makeArticle(1, "tech", 42, 50));
    CHECK_FALSE(alive(gAll));
    CHECK(alive(gIn));   // sports ∌ tech
    CHECK(alive(gRng));  // 50 < 100 — proves skipset left view_count aligned
    CHECK(alive(gEq));   // author 42 ≠ 99
}

TEST_CASE("[DeclListIn][L2] alignment — IN in first position (create)",
          "[integration][db][redis][list][in][l2]") {
    TransactionGuard tx;
    // DescInFirst: [IN category][EQ author_id]
    auto mk = [](std::vector<std::string> cats, int64_t author) {
        decl::ListDescriptorQuery<DescInFirst> q;
        q.filters.get<0>() = std::move(cats);
        q.filters.get<1>() = author;
        return registerInGroup<DescInFirst>(q);
    };
    auto gMatch = mk({"news", "tech"}, 42);  // IN ok + EQ ok
    auto gEqFail = mk({"tech"},        99);  // EQ AFTER the set fails

    fireCreate<DescInFirst>(makeArticle(1, "tech", 42));
    CHECK_FALSE(alive(gMatch));
    CHECK(alive(gEqFail));  // author after the set correctly mismatched
}

TEST_CASE("[DeclListIn][L2] alignment — IN in last position (create)",
          "[integration][db][redis][list][in][l2]") {
    TransactionGuard tx;
    // DescInLast: [EQ author_id][IN category]
    auto mk = [](int64_t author, std::vector<std::string> cats) {
        decl::ListDescriptorQuery<DescInLast> q;
        q.filters.get<0>() = author;
        q.filters.get<1>() = std::move(cats);
        return registerInGroup<DescInLast>(q);
    };
    auto gMatch  = mk(42, {"tech"});
    auto gInFail = mk(42, {"news"});
    auto gEqFail = mk(99, {"tech"});

    fireCreate<DescInLast>(makeArticle(1, "tech", 42));
    CHECK_FALSE(alive(gMatch));
    CHECK(alive(gInFail));   // news ∌ tech
    CHECK(alive(gEqFail));   // author 42 ≠ 99
}

TEST_CASE("[DeclListIn][L2] optional-null entity and empty-set group never match (create)",
          "[integration][db][redis][list][in][l2]") {
    TransactionGuard tx;
    // DescInOptInt32: [IN view_count] on a std::optional<int32_t> member.
    auto mk = [](std::optional<std::vector<int32_t>> set) {
        decl::ListDescriptorQuery<DescInOptInt32> q;
        if (set) q.filters.get<0>() = std::move(*set);
        return registerInGroup<DescInOptInt32>(q);
    };
    auto gSet   = mk(std::vector<int32_t>{10, 20});
    auto gEmpty = mk(std::vector<int32_t>{});  // present-but-empty set

    SECTION("matching value invalidates the set group, never the empty one") {
        fireCreate<DescInOptInt32>(makeArticle(1, "x", 0, 20));
        CHECK_FALSE(alive(gSet));  // 20 ∈ {10,20}
        CHECK(alive(gEmpty));      // 20 ∉ ∅
    }
    SECTION("null IN member matches no set") {
        fireCreate<DescInOptInt32>(makeArticle(2, "x", 0, std::nullopt));
        CHECK(alive(gSet));    // null ∉ {10,20}
        CHECK(alive(gEmpty));
    }
}

TEST_CASE("[DeclListIn][L2] update invalidates groups whose set holds old OR new value",
          "[integration][db][redis][list][in][l2]") {
    TransactionGuard tx;
    auto mk = [](std::vector<std::string> set) {
        decl::ListDescriptorQuery<DescInString> q;
        q.filters.get<0>() = std::move(set);
        return registerInGroup<DescInString>(q);
    };
    auto gOld   = mk({"news", "tech"});       // holds old=tech
    auto gNew   = mk({"science", "sports"});  // holds new=science
    auto gBoth  = mk({"science", "tech"});    // holds both
    auto gOther = mk({"games", "music"});     // neither

    fireUpdate<DescInString>(makeArticle(1, "tech", 0), makeArticle(1, "science", 0));
    CHECK_FALSE(alive(gOld));   // old=tech matched
    CHECK_FALSE(alive(gNew));   // new=science matched
    CHECK_FALSE(alive(gBoth));  // both matched (invalidated once)
    CHECK(alive(gOther));       // neither value in the set
}

TEST_CASE("[DeclListIn][L2] alignment — IN in middle position (update, revue C8)",
          "[integration][db][redis][list][in][l2]") {
    TransactionGuard tx;
    // Replays the IN-middle alignment against the SECOND, duplicated Lua script
    // (`...Update`) to catch any copy-paste divergence from the create path.
    auto mk = [](int64_t author, std::vector<std::string> cats, int32_t viewGe) {
        decl::ListDescriptorQuery<DescCombo> q;
        q.filters.get<0>() = author;
        q.filters.get<1>() = std::move(cats);
        q.filters.get<2>() = viewGe;
        return registerInGroup<DescCombo>(q);
    };
    auto gAll = mk(42, {"tech"}, 10);   // both old/new match
    auto gRng = mk(42, {"tech"}, 100);  // range AFTER the set fails for both
    auto gEq  = mk(99, {"tech"}, 10);   // EQ BEFORE the set fails

    fireUpdate<DescCombo>(makeArticle(1, "tech", 42, 50), makeArticle(1, "tech", 42, 60));
    CHECK_FALSE(alive(gAll));
    CHECK(alive(gRng));  // 50,60 both < 100 — UPDATE skipset matches CREATE skipset
    CHECK(alive(gEq));   // author 42 ≠ 99
}
