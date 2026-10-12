/**
 * test_decl_list_is_null.cpp
 *
 * List invalidation under an IS_NULL filter, on the L2 Lua matchers and through
 * the repos (L1, L2, L1+L2), against PostgreSQL and Redis.
 *
 * Fixture TestArticleNull: category (EQ) -> published_null (IS_NULL) ->
 * views_in (IN). The IS_NULL entity slot carries no value, so a cursor slip in
 * a Lua matcher misreads the IN filter that follows it: every case keeps a
 * filter active after the IS_NULL slot.
 *
 * Repo cases use stale sentinels: a row inserted straight into PostgreSQL is
 * visible only on a page that the write under test invalidated.
 */

#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"
#include "fixtures/generated/TestArticleNullEntity.h"
#include <jcailloux/relais/cache/RedisCache.h>
#include <jcailloux/relais/list/spec/CanonicalEncoding.h>
#include <jcailloux/relais/list/spec/TypedCursor.h>

namespace decl = jcailloux::relais::list::spec;
using namespace relais_test;
using GenEntity = entity::generated::TestArticleNullEntity;
using jcailloux::relais::PgProvider;
namespace cache_ns = jcailloux::relais::cache;

namespace {

struct GenDesc : entity::generated::TestArticleNullMapping::ListDescriptor {
    using Entity = GenEntity;
};

const std::optional<std::string> kNull = std::nullopt;
const std::optional<std::string> kSet = std::string("2026-01-01 00:00:00+00");

GenEntity makeArticle(std::string category, std::optional<std::string> published_at,
                      std::optional<int32_t> view_count, int64_t author_id = 0) {
    GenEntity e;
    e.category = std::move(category);
    e.author_id = author_id;
    e.title = "is_null";
    e.view_count = view_count;
    e.published_at = std::move(published_at);
    return e;
}

// =============================================================================
// Lua matchers, driven directly. Each page is registered as a 1-byte value,
// shorter than the bounds header, so the range check always deletes: a page
// survives only if the filter match rejected its group.
// =============================================================================

constexpr std::string_view kPrefix = "T:dlist:g:";
const std::string kMaster = "test:is_null:l2:master";

struct Group {
    std::optional<std::string> category;
    std::optional<bool> published_null;
    std::optional<std::vector<int32_t>> views_in;
};

std::string registerGroup(const Group& g) {
    decl::ListQueryParams<GenDesc> q;
    q.filters.get<"category">() = g.category;
    q.filters.get<"published_null">() = g.published_null;
    q.filters.get<"views_in">() = g.views_in;
    std::string groupKey = std::string(kPrefix) + decl::groupKey<GenDesc>(q.filters, q.sort);
    std::string pageKey = groupKey + ":p";
    sync(PgProvider::redis("SET", pageKey, "x"));
    sync(PgProvider::redis("SADD", groupKey + ":_keys", pageKey));
    sync(PgProvider::redis("HSET", kMaster, groupKey, "0"));
    return pageKey;
}

bool alive(const std::string& pageKey) {
    return sync(PgProvider::redis("EXISTS", pageKey)).asInteger() == 1;
}

void fireCreate(const GenEntity& e) {
    sync(cache_ns::RedisCache::invalidateListGroupsSelective(
        kMaster, kPrefix.size(), decl::filterSchema<GenDesc>(),
        decl::encodeEntityFilterBlob<GenDesc>(e), "0"));
}

void fireUpdate(const GenEntity& oldE, const GenEntity& newE) {
    sync(cache_ns::RedisCache::invalidateListGroupsSelectiveUpdate(
        kMaster, kPrefix.size(), decl::filterSchema<GenDesc>(),
        decl::encodeEntityFilterBlob<GenDesc>(newE), "0",
        decl::encodeEntityFilterBlob<GenDesc>(oldE), "0"));
}

void firePredicate(const Group& p) {
    decl::Filters<GenDesc> f;
    f.get<"category">() = p.category;
    f.get<"published_null">() = p.published_null;
    f.get<"views_in">() = p.views_in;
    sync(cache_ns::RedisCache::invalidateListGroupsByPredicate(
        kMaster, kPrefix.size(), decl::filterSchema<GenDesc>(),
        decl::encodeFilterSet<GenDesc>(f),
        std::to_string(std::numeric_limits<int64_t>::min()),
        std::to_string(std::numeric_limits<int64_t>::max())));
}

const std::vector<int32_t> kV5{5};
const std::vector<int32_t> kV6{6};

}  // namespace

TEST_CASE("[ListIsNull][L2] create matches on nullity, IN after the slot stays aligned",
          "[integration][db][redis][list][is_null][l2]") {
    TransactionGuard tx;
    auto gNull   = registerGroup({.published_null = true, .views_in = kV5});
    auto gSet    = registerGroup({.published_null = false, .views_in = kV5});
    auto gOpen   = registerGroup({.views_in = kV5});
    auto gNullV6 = registerGroup({.published_null = true, .views_in = kV6});
    auto gNews   = registerGroup({.category = "news", .published_null = true});

    SECTION("null entity") {
        fireCreate(makeArticle("tech", kNull, 5));
        CHECK_FALSE(alive(gNull));
        CHECK(alive(gSet));
        CHECK_FALSE(alive(gOpen));
        CHECK(alive(gNullV6));  // IN after the IS_NULL slot read at the right offset
        CHECK(alive(gNews));    // EQ before it
    }
    SECTION("non-null entity") {
        fireCreate(makeArticle("tech", kSet, 5));
        CHECK(alive(gNull));
        CHECK_FALSE(alive(gSet));
        CHECK_FALSE(alive(gOpen));
        CHECK(alive(gNullV6));
        CHECK(alive(gNews));
    }
    SECTION("null view_count after the slot") {
        fireCreate(makeArticle("tech", kNull, std::nullopt));
        CHECK(alive(gNull));  // IN never matches a null member
        CHECK(alive(gSet));
        CHECK(alive(gOpen));
    }
}

TEST_CASE("[ListIsNull][L2] update matches the old and the new nullity",
          "[integration][db][redis][list][is_null][l2]") {
    TransactionGuard tx;
    auto gNull   = registerGroup({.published_null = true, .views_in = kV5});
    auto gSet    = registerGroup({.published_null = false, .views_in = kV5});
    auto gNullV6 = registerGroup({.published_null = true, .views_in = kV6});

    SECTION("null to non-null touches both") {
        fireUpdate(makeArticle("tech", kNull, 5), makeArticle("tech", kSet, 5));
        CHECK_FALSE(alive(gNull));
        CHECK_FALSE(alive(gSet));
        CHECK(alive(gNullV6));
    }
    SECTION("non-null to null touches both") {
        fireUpdate(makeArticle("tech", kSet, 5), makeArticle("tech", kNull, 5));
        CHECK_FALSE(alive(gNull));
        CHECK_FALSE(alive(gSet));
        CHECK(alive(gNullV6));
    }
    SECTION("nullity unchanged touches its side only") {
        fireUpdate(makeArticle("tech", kNull, 5), makeArticle("news", kNull, 5));
        CHECK_FALSE(alive(gNull));
        CHECK(alive(gSet));
        CHECK(alive(gNullV6));
    }
}

TEST_CASE("[ListIsNull][L2] predicate prunes the opposite nullity",
          "[integration][db][redis][list][is_null][l2]") {
    TransactionGuard tx;
    auto gNull   = registerGroup({.published_null = true, .views_in = kV5});
    auto gSet    = registerGroup({.published_null = false, .views_in = kV5});
    auto gOpen   = registerGroup({.views_in = kV5});
    auto gNullV6 = registerGroup({.published_null = true, .views_in = kV6});

    firePredicate({.published_null = true, .views_in = kV5});
    CHECK_FALSE(alive(gNull));
    CHECK(alive(gSet));
    CHECK_FALSE(alive(gOpen));
    CHECK(alive(gNullV6));  // IN after the slot: disjoint sets prune
}

// =============================================================================
// Repos: L1, L2 and L1+L2 run the same scenarios.
// =============================================================================

namespace {

template<typename R, bool HasL1>
struct Tier {
    using Repo = R;
    static void resetL1() {
        if constexpr (HasL1) TestInternals::resetListCacheState<R>();
    }
};

using L1Tier   = Tier<Repo<GenEntity, "test:article_null:l1", cfg::Local>, true>;
using L2Tier   = Tier<Repo<GenEntity, "test:article_null:l2", cfg::Redis>, false>;
using BothTier = Tier<Repo<GenEntity, "test:article_null:both", cfg::Both>, true>;

// Every repo page keeps views_in active after the IS_NULL slot.
template<typename R>
auto page(std::optional<bool> published_null) {
    using Desc = typename R::ListDescriptorType;
    decl::ListQueryParams<Desc> p;
    p.limit = 10;
    p.filters.template get<"published_null">() = published_null;
    p.filters.template get<"views_in">() = kV5;
    return decl::seal<Desc>(std::move(p));
}

template<typename R>
size_t count(std::optional<bool> published_null) {
    return sync(R::query(page<R>(published_null)))->size();
}

int64_t insertRow(int64_t author, bool null) {
    return insertTestArticle("tech", author, "row", 5, /*is_published=*/!null);
}

template<typename R>
GenEntity load(int64_t id) {
    auto v = sync(R::find(id));
    REQUIRE(v);
    return *v;
}

}  // namespace

TEMPLATE_TEST_CASE("[ListIsNull] repo writes invalidate the pages of their nullity",
                   "[integration][db][redis][list][is_null]", L1Tier, L2Tier, BothTier) {
    using R = typename TestType::Repo;
    TransactionGuard tx;
    TestType::resetL1();

    auto author = insertTestUser("is_null", "is_null@x", 0);
    auto n1 = insertRow(author, true);
    auto s1 = insertRow(author, false);

    REQUIRE(count<R>(true) == 1);
    REQUIRE(count<R>(false) == 1);
    REQUIRE(count<R>(std::nullopt) == 2);

    // Stale sentinels: visible only on a refetched page.
    insertRow(author, true);
    insertRow(author, false);

    SECTION("create null") {
        sync(R::insert(makeArticle("tech", kNull, 5, author)));
        CHECK(count<R>(true) == 3);
        CHECK(count<R>(false) == 1);  // kept
        CHECK(count<R>(std::nullopt) == 5);
    }
    SECTION("create non-null") {
        sync(R::insert(makeArticle("tech", kSet, 5, author)));
        CHECK(count<R>(true) == 1);   // kept
        CHECK(count<R>(false) == 3);
        CHECK(count<R>(std::nullopt) == 5);
    }
    SECTION("erase null") {
        sync(R::erase(n1));
        CHECK(count<R>(true) == 1);
        CHECK(count<R>(false) == 1);  // kept
        CHECK(count<R>(std::nullopt) == 3);
    }
    SECTION("update null to non-null") {
        auto e = load<R>(n1);
        e.published_at = kSet;
        sync(R::update(n1, e));
        CHECK(count<R>(true) == 1);
        CHECK(count<R>(false) == 3);
    }
    SECTION("update non-null to null") {
        auto e = load<R>(s1);
        e.published_at.reset();
        sync(R::update(s1, e));
        CHECK(count<R>(true) == 3);
        CHECK(count<R>(false) == 1);
    }
    SECTION("update that leaves the column null") {
        auto e = load<R>(n1);
        e.title = "renamed";
        sync(R::update(n1, e));
        CHECK(count<R>(true) == 2);
        CHECK(count<R>(false) == 1);  // kept
    }
}

TEMPLATE_TEST_CASE("[ListIsNull] eraseWhere on nullity prunes the opposite pages",
                   "[integration][db][redis][list][is_null][where]", L1Tier, L2Tier, BothTier) {
    using R = typename TestType::Repo;
    TransactionGuard tx;
    TestType::resetL1();

    auto author = insertTestUser("is_null_w", "is_null_w@x", 0);
    insertRow(author, true);
    insertRow(author, true);
    insertRow(author, false);

    REQUIRE(count<R>(true) == 2);
    REQUIRE(count<R>(false) == 1);
    REQUIRE(count<R>(std::nullopt) == 3);

    insertRow(author, false);  // sentinel

    auto n = sync(R::eraseWhere({.published_null = true}));
    REQUIRE(n == std::optional<size_t>(2));

    CHECK(count<R>(true) == 0);
    CHECK(count<R>(false) == 1);  // predicate-incompatible: kept
    CHECK(count<R>(std::nullopt) == 2);
}

TEST_CASE("[ListIsNull] keyset pagination binds the cursor after an IS_NULL filter",
          "[integration][db][list][is_null]") {
    using R = L1Tier::Repo;
    using Desc = R::ListDescriptorType;
    TransactionGuard tx;
    L1Tier::resetL1();

    auto author = insertTestUser("is_null_c", "is_null_c@x", 0);
    for (int i = 0; i < 12; ++i) insertRow(author, true);
    insertRow(author, false);
    insertTestArticle("news", author, "other", 5, false);

    decl::ListQueryParams<Desc> p;
    p.limit = 10;
    p.filters.get<"category">() = std::string("tech");
    p.filters.get<"published_null">() = true;
    p.filters.get<"views_in">() = kV5;

    auto first = sync(R::query(decl::seal<Desc>(p)));
    REQUIRE(first->size() == 10);
    REQUIRE_FALSE(first->cursor().empty());

    auto cursor = decl::TypedCursor<Desc>::decode(first->cursor());
    REQUIRE(cursor.has_value());
    p.cursor = *cursor;
    auto second = sync(R::query(decl::seal<Desc>(p)));
    CHECK(second->size() == 2);
    for (const auto& e : second->items) {
        CHECK_FALSE(e.published_at.has_value());
        CHECK(e.id < first->items.back().id);
    }
}

TEST_CASE("[ListIsNull] L1+L2 parity: after each write, L2 serves what PostgreSQL holds",
          "[integration][db][redis][list][is_null][parity]") {
    using R = BothTier::Repo;
    TransactionGuard tx;
    BothTier::resetL1();

    auto author = insertTestUser("is_null_p", "is_null_p@x", 0);
    auto n1 = insertRow(author, true);
    auto s1 = insertRow(author, false);

    auto truth = [](std::optional<bool> published_null) {
        std::string sql = "SELECT id FROM relais_test_articles WHERE view_count = 5";
        if (published_null) sql += *published_null ? " AND published_at IS NULL"
                                                   : " AND published_at IS NOT NULL";
        sql += " ORDER BY id DESC LIMIT 10";
        auto r = execQuery(sql.c_str());
        std::vector<int64_t> ids;
        for (int i = 0; i < r.rows(); ++i) ids.push_back(r[i].template get<int64_t>(0));
        return ids;
    };
    auto served = [](std::optional<bool> published_null) {
        std::vector<int64_t> ids;
        for (const auto& e : sync(R::query(page<R>(published_null)))->items)
            ids.push_back(e.id);
        return ids;
    };
    const std::optional<bool> kPages[] = {true, false, std::nullopt};
    // Each check re-warms L1 and L2, so the next write meets cached pages.
    auto check = [&](std::string_view step) {
        BothTier::resetL1();  // serve from L2 alone
        for (auto v : kPages) {
            CAPTURE(step, v);
            CHECK(served(v) == truth(v));
        }
    };
    for (auto v : kPages) (void)served(v);

    sync(R::insert(makeArticle("tech", kNull, 5, author)));
    check("insert null");

    sync(R::insert(makeArticle("tech", kSet, 5, author)));
    check("insert non-null");

    auto e = load<R>(n1);
    e.published_at = kSet;
    sync(R::update(n1, e));
    check("update null to non-null");

    e = load<R>(s1);
    e.published_at.reset();
    sync(R::update(s1, e));
    check("update non-null to null");

    e = load<R>(s1);
    e.title = "renamed";
    sync(R::update(s1, e));
    check("update title");

    sync(R::erase(n1));
    check("erase");

    sync(R::eraseWhere({.published_null = true}));
    check("eraseWhere");
}
