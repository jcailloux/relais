/**
 * test_list_null_match.cpp
 *
 * List invalidation for rows whose filtered column is NULL. SQL decides what a
 * page holds: a NULL column satisfies no comparison and no membership, except
 * NOT IN an empty set (`NULL <> ALL('{}')` is TRUE). Both L1 matchers must give
 * PostgreSQL's verdict, or a write leaves a stale page (a NULL row missing from
 * `nin={}`) or drops pages it cannot change.
 *
 * Fixture TestNullableList: one filter per value operator on the nullable
 * `rank`, NE and NIN on the nullable `label`, then `owner_id`.
 */

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"
#include "fixtures/generated/TestNullableListEntity.h"
#include <jcailloux/relais/list/spec/GeneratedCriteria.h>
#include <jcailloux/relais/list/spec/GeneratedFilters.h>

namespace decl = jcailloux::relais::list::spec;
using namespace relais_test;
using GenEntity = entity::generated::TestNullableListEntity;

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
// column. PostgreSQL evaluates the generated WHERE clause on the stored row.
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

bool pgVerdict(int64_t id, const Filters& f) {
    auto where = decl::detail::buildWhereClause<GenDesc>(f);
    const auto sql = "SELECT count(*) FROM relais_test_nullable_list WHERE id = "
                   + std::to_string(id) + " AND " + where.sql;
    return sync(relais_test::detail::testPg()->queryParams(sql.c_str(), where.params))[0].get<int64_t>(0) == 1;
}

}  // namespace

TEST_CASE("[ListNullMatch] L1 and PostgreSQL agree on NULL and non-NULL rows",
          "[integration][db][list][null]") {
    TransactionGuard tx;
    const auto all = cases();

    for (const auto& rank : {std::optional<int32_t>{}, std::optional<int32_t>{5}}) {
        for (const auto& label : {std::optional<std::string>{}, std::optional<std::string>{"a"}}) {
            const auto row = makeRow(1, rank, label);
            const auto id = insertRow(1, rank, label);

            for (const auto& c : all) {
                const auto& f = c.filters;
                const bool pg = pgVerdict(id, f);
                const bool viaFree = decl::detail::matchesFilters<GenDesc>(row, f);
                const bool viaTags = f.matchesFilters(decl::detail::extractTags<GenDesc>(row));
                CAPTURE(c.name, rank.has_value(), label.has_value());
                CHECK(viaFree == pg);
                CHECK(viaTags == pg);
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

    CHECK(decl::detail::matchesFilters<GenDesc>(nullRow, empty));
    CHECK(empty.matchesFilters(decl::detail::extractTags<GenDesc>(nullRow)));
    CHECK_FALSE(decl::detail::matchesFilters<GenDesc>(nullRow, some));
    CHECK_FALSE(some.matchesFilters(decl::detail::extractTags<GenDesc>(nullRow)));
}

// =============================================================================
// Through the repo. Stale sentinels: a row inserted straight into PostgreSQL
// is visible only on a page that the write under test invalidated.
// =============================================================================

namespace {

using R = Repo<GenEntity, "test:nullable_list:l1", cfg::Local>;

template<typename Set>
size_t count(Set set) {
    using Desc = R::ListDescriptorType;
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

TEST_CASE("[ListNullMatch] a NULL row invalidates the pages SQL puts it on, only those",
          "[integration][db][list][null]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<R>();

    insertRow(1, 5, "a");
    insertRow(1, std::nullopt, std::nullopt);
    REQUIRE(count(ninEmpty) == 2);
    REQUIRE(count(ninB) == 1);
    REQUIRE(count(ne7) == 1);
    REQUIRE(count(ge0) == 1);

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

    CHECK(count(ninEmpty) == 5);  // NULL label <> ALL('{}') holds
    CHECK(count(ninB) == 1);      // kept
    CHECK(count(ne7) == 1);       // kept
    CHECK(count(ge0) == 1);       // kept
}
