/**
 * test_list_null_match.cpp
 *
 * List invalidation for rows whose filtered column is NULL. SQL decides what a
 * page holds: a NULL column satisfies no comparison and no membership, except
 * NOT IN an empty set (`NULL <> ALL('{}')` is TRUE). Both L1 matchers and the
 * L2 Lua matcher must give PostgreSQL's verdict, or a write leaves a stale page
 * (a NULL row missing from `nin={}`) or drops pages it cannot change.
 *
 * Fixture TestNullableList: one filter per value operator on the nullable
 * `rank`, NE and NIN on the nullable `label`, then `owner_id`, read after the
 * NOT IN slot of `label`.
 */

#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"
#include "fixtures/generated/TestNullableListEntity.h"
#include <jcailloux/relais/cache/RedisCache.h>
#include <jcailloux/relais/list/spec/CanonicalEncoding.h>

namespace decl = jcailloux::relais::list::spec;
using namespace relais_test;
using GenEntity = entity::generated::TestNullableListEntity;
using jcailloux::relais::PgProvider;
namespace cache_ns = jcailloux::relais::cache;

namespace {

struct GenDesc : entity::generated::TestNullableListMapping::ListDescriptor {
    using Entity = GenEntity;
};
using Filters = decl::Filters<GenDesc>;

GenEntity makeRow(int64_t owner, std::optional<int32_t> rank,
                  std::optional<std::string> label) {
    GenEntity e;
    e.owner_id = owner;
    e.rank = rank;
    e.label = std::move(label);
    return e;
}

int64_t insertRow(int64_t owner, std::optional<int32_t> rank,
                  std::optional<std::string> label) {
    return execQueryArgs("INSERT INTO relais_test_nullable_list (owner_id, rank, label) "
                         "VALUES ($1, $2, $3) RETURNING id", owner, rank, label)[0]
        .get<int64_t>(0);
}

// =============================================================================
// Verdict oracle: each filter set alone, against rows NULL or not in each
// column. PostgreSQL evaluates the generated WHERE clause on the stored row; the
// L2 verdict is read back from Redis (a page registered as a 1-byte value is
// shorter than the bounds header, so it survives only if its group did not match).
// =============================================================================

struct Case {
    std::string name;
    Filters filters;
};

std::vector<Case> cases() {
    std::vector<Case> out;
    auto add = [&](std::string name, const std::function<void(Filters&)>& set) {
        Filters f;
        set(f);
        out.push_back({std::move(name), std::move(f)});
    };
    for (int32_t v : {3, 5, 7}) {
        const auto s = std::to_string(v);
        add("rank_eq=" + s, [v](Filters& f) { f.get<"rank_eq">() = v; });
        add("rank_ne=" + s, [v](Filters& f) { f.get<"rank_ne">() = v; });
        add("rank_gt=" + s, [v](Filters& f) { f.get<"rank_gt">() = v; });
        add("rank_ge=" + s, [v](Filters& f) { f.get<"rank_ge">() = v; });
        add("rank_lt=" + s, [v](Filters& f) { f.get<"rank_lt">() = v; });
        add("rank_le=" + s, [v](Filters& f) { f.get<"rank_le">() = v; });
    }
    for (const auto& set : std::vector<std::vector<int32_t>>{{}, {5}, {3, 7}}) {
        const auto s = std::to_string(set.size());
        add("rank_in#" + s, [set](Filters& f) { f.get<"rank_in">() = set; });
        add("rank_nin#" + s, [set](Filters& f) { f.get<"rank_nin">() = set; });
    }
    for (const std::string v : {"a", "b"})
        add("label_ne=" + v, [v](Filters& f) { f.get<"label_ne">() = v; });
    for (const auto& set : std::vector<std::vector<std::string>>{{}, {"a"}, {"b"}})
        add("label_nin#" + std::to_string(set.size()) + (set.empty() ? "" : set[0]),
            [set](Filters& f) { f.get<"label_nin">() = set; });
    for (int64_t owner : {1, 2})
        add("label_nin={},owner_id=" + std::to_string(owner), [owner](Filters& f) {
            f.get<"label_nin">() = std::vector<std::string>{};
            f.get<"owner_id">() = owner;
        });
    return out;
}

constexpr std::string_view kPrefix = "T:dlist:g:";
const std::string kMaster = "test:null_match:l2:master";

std::string registerGroup(const Filters& f) {
    decl::ListQueryParams<GenDesc> q;
    q.filters = f;
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

bool pgVerdict(int64_t id, const Filters& f) {
    auto where = decl::buildWhereClause<GenDesc>(f);
    const auto sql = "SELECT count(*) FROM relais_test_nullable_list WHERE id = "
                   + std::to_string(id) + " AND " + where.sql;
    return sync(relais_test::detail::testPg()->queryParams(sql.c_str(), where.params))[0].get<int64_t>(0) == 1;
}

}  // namespace

TEST_CASE("[ListNullMatch] L1, L2 and PostgreSQL agree on NULL and non-NULL rows",
          "[integration][db][redis][list][null]") {
    TransactionGuard tx;
    const auto all = cases();

    for (const auto& rank : {std::optional<int32_t>{}, std::optional<int32_t>{5}}) {
        for (const auto& label : {std::optional<std::string>{}, std::optional<std::string>{"a"}}) {
            const auto row = makeRow(1, rank, label);
            const auto id = insertRow(1, rank, label);

            flushRedis();
            std::vector<std::string> pages;
            for (const auto& c : all) pages.push_back(registerGroup(c.filters));
            sync(cache_ns::RedisCache::invalidateListGroupsSelective(
                kMaster, kPrefix.size(), decl::filterSchema<GenDesc>(),
                decl::encodeEntityFilterBlob<GenDesc>(row), "0"));

            for (size_t i = 0; i < all.size(); ++i) {
                const auto& f = all[i].filters;
                const bool pg = pgVerdict(id, f);
                const bool viaFree = decl::matchesFilters<GenDesc>(row, f);
                const bool viaTags = f.matchesFilters(decl::extractTags<GenDesc>(row));
                const bool lua = !alive(pages[i]);
                CAPTURE(all[i].name, rank.has_value(), label.has_value());
                CHECK(viaFree == pg);
                CHECK(viaTags == pg);
                CHECK(lua == pg);
            }
        }
    }
}

TEST_CASE("[ListNullMatch] NOT IN an empty set holds a NULL row, a non-empty one does not",
          "[list][null][unit]") {
    const auto nullRow = makeRow(1, std::nullopt, std::nullopt);
    Filters empty;
    empty.get<"rank_nin">() = std::vector<int32_t>{};
    Filters some;
    some.get<"rank_nin">() = std::vector<int32_t>{5};

    CHECK(decl::matchesFilters<GenDesc>(nullRow, empty));
    CHECK(empty.matchesFilters(decl::extractTags<GenDesc>(nullRow)));
    CHECK_FALSE(decl::matchesFilters<GenDesc>(nullRow, some));
    CHECK_FALSE(some.matchesFilters(decl::extractTags<GenDesc>(nullRow)));
}

// =============================================================================
// Through the repos. Stale sentinels: a row inserted straight into PostgreSQL
// is visible only on a page that the write under test invalidated.
// =============================================================================

namespace {

template<typename R, bool HasL1>
struct Tier {
    using Repo = R;
    static void resetL1() {
        if constexpr (HasL1) TestInternals::resetListCacheState<R>();
    }
};

using L1Tier   = Tier<Repo<GenEntity, "test:nullable_list:l1", cfg::Local>, true>;
using L2Tier   = Tier<Repo<GenEntity, "test:nullable_list:l2", cfg::Redis>, false>;
using BothTier = Tier<Repo<GenEntity, "test:nullable_list:both", cfg::Both>, true>;

template<typename R, typename Set>
size_t count(Set set) {
    using Desc = typename R::ListDescriptorType;
    decl::ListQueryParams<Desc> p;
    p.limit = 10;
    set(p.filters);
    return sync(R::query(decl::seal<Desc>(std::move(p))))->size();
}

constexpr auto ninEmpty = [](auto& f) { f.template get<"label_nin">() = std::vector<std::string>{}; };
constexpr auto ninB = [](auto& f) { f.template get<"label_nin">() = std::vector<std::string>{"b"}; };
constexpr auto ne7 = [](auto& f) { f.template get<"rank_ne">() = 7; };
constexpr auto ge0 = [](auto& f) { f.template get<"rank_ge">() = 0; };

}  // namespace

TEMPLATE_TEST_CASE("[ListNullMatch] a NULL row invalidates the pages SQL puts it on, only those",
                   "[integration][db][redis][list][null]", L1Tier, L2Tier, BothTier) {
    using R = typename TestType::Repo;
    TransactionGuard tx;
    TestType::resetL1();

    insertRow(1, 5, "a");
    insertRow(1, std::nullopt, std::nullopt);
    REQUIRE(count<R>(ninEmpty) == 2);
    REQUIRE(count<R>(ninB) == 1);
    REQUIRE(count<R>(ne7) == 1);
    REQUIRE(count<R>(ge0) == 1);

    // Stale sentinels, one per kind of page.
    insertRow(1, std::nullopt, std::nullopt);
    insertRow(1, 6, "c");

    SECTION("insert") {
        REQUIRE(sync(R::insert(makeRow(1, std::nullopt, std::nullopt))));
    }
    SECTION("update that keeps the columns NULL") {
        const auto id = insertRow(2, std::nullopt, std::nullopt);
        REQUIRE(sync(R::update(id, makeRow(1, std::nullopt, std::nullopt))));
    }

    CHECK(count<R>(ninEmpty) == 5);  // NULL label <> ALL('{}') holds
    CHECK(count<R>(ninB) == 1);      // kept
    CHECK(count<R>(ne7) == 1);       // kept
    CHECK(count<R>(ge0) == 1);       // kept
}
