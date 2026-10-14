/**
 * test_decl_list_is_null.cpp
 *
 * List invalidation under an IS_NULL filter, through an L1 list repo, against
 * PostgreSQL.
 *
 * Fixture TestArticleNull: category (EQ) -> published_null (IS_NULL) ->
 * views_in (IN). Every repo page keeps the IN filter active after the IS_NULL
 * slot.
 *
 * Repo cases use stale sentinels: a row inserted straight into PostgreSQL is
 * visible only on a page that the write under test invalidated.
 */

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"
#include "fixtures/generated/TestArticleNullEntity.h"
#include <jcailloux/relais/list/spec/TypedCursor.h>

namespace decl = jcailloux::relais::list::spec;
using namespace relais_test;
using GenEntity = entity::generated::TestArticleNullEntity;

namespace {

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

const std::vector<int32_t> kV5{5};

using R = Repo<GenEntity, "test:article_null:l1", cfg::Local>;

// Every repo page keeps views_in active after the IS_NULL slot.
R::ListQuery page(std::optional<bool> published_null) {
    using Desc = R::ListDescriptorType;
    decl::ListQueryParams<Desc> p;
    p.limit = 10;
    p.filters.get<"published_null">() = published_null;
    p.filters.get<"views_in">() = kV5;
    return decl::seal<Desc>(std::move(p));
}

size_t count(std::optional<bool> published_null) {
    return sync(R::query(page(published_null)))->size();
}

int64_t insertRow(int64_t author, bool null) {
    return insertTestArticle("tech", author, "row", 5, /*is_published=*/!null);
}

GenEntity load(int64_t id) {
    auto v = sync(R::find(id));
    REQUIRE(v);
    return *v;
}

}  // namespace

TEST_CASE("[ListIsNull] repo writes invalidate the pages of their nullity",
          "[integration][db][list][is_null]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<R>();

    auto author = insertTestUser("is_null", "is_null@x", 0);
    auto n1 = insertRow(author, true);
    auto s1 = insertRow(author, false);

    REQUIRE(count(true) == 1);
    REQUIRE(count(false) == 1);
    REQUIRE(count(std::nullopt) == 2);

    // Stale sentinels: visible only on a refetched page.
    insertRow(author, true);
    insertRow(author, false);

    SECTION("create null") {
        sync(R::insert(makeArticle("tech", kNull, 5, author)));
        CHECK(count(true) == 3);
        CHECK(count(false) == 1);  // kept
        CHECK(count(std::nullopt) == 5);
    }
    SECTION("create non-null") {
        sync(R::insert(makeArticle("tech", kSet, 5, author)));
        CHECK(count(true) == 1);   // kept
        CHECK(count(false) == 3);
        CHECK(count(std::nullopt) == 5);
    }
    SECTION("erase null") {
        sync(R::erase(n1));
        CHECK(count(true) == 1);
        CHECK(count(false) == 1);  // kept
        CHECK(count(std::nullopt) == 3);
    }
    SECTION("update null to non-null") {
        auto e = load(n1);
        e.published_at = kSet;
        sync(R::update(n1, e));
        CHECK(count(true) == 1);
        CHECK(count(false) == 3);
    }
    SECTION("update non-null to null") {
        auto e = load(s1);
        e.published_at.reset();
        sync(R::update(s1, e));
        CHECK(count(true) == 3);
        CHECK(count(false) == 1);
    }
    SECTION("update that leaves the column null") {
        auto e = load(n1);
        e.title = "renamed";
        sync(R::update(n1, e));
        CHECK(count(true) == 2);
        CHECK(count(false) == 1);  // kept
    }
}

TEST_CASE("[ListIsNull] eraseWhere on nullity prunes the opposite pages",
          "[integration][db][list][is_null][where]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<R>();

    auto author = insertTestUser("is_null_w", "is_null_w@x", 0);
    insertRow(author, true);
    insertRow(author, true);
    insertRow(author, false);

    REQUIRE(count(true) == 2);
    REQUIRE(count(false) == 1);
    REQUIRE(count(std::nullopt) == 3);

    insertRow(author, false);  // sentinel

    auto n = sync(R::eraseWhere({.published_null = true}));
    REQUIRE(n == std::optional<size_t>(2));

    CHECK(count(true) == 0);
    CHECK(count(false) == 1);  // predicate-incompatible: kept
    CHECK(count(std::nullopt) == 2);
}

TEST_CASE("[ListIsNull] keyset pagination binds the cursor after an IS_NULL filter",
          "[integration][db][list][is_null]") {
    using Desc = R::ListDescriptorType;
    TransactionGuard tx;
    TestInternals::resetListCacheState<R>();

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

TEST_CASE("[ListIsNull] parity: after each write, L1 serves what PostgreSQL holds",
          "[integration][db][list][is_null][parity]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<R>();

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
        for (const auto& e : sync(R::query(page(published_null)))->items)
            ids.push_back(e.id);
        return ids;
    };
    const std::optional<bool> kPages[] = {true, false, std::nullopt};
    // Each check re-warms L1, so the next write meets cached pages: a page the
    // write wrongly kept would still serve the rows before it.
    auto check = [&](std::string_view step) {
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

    auto e = load(n1);
    e.published_at = kSet;
    sync(R::update(n1, e));
    check("update null to non-null");

    e = load(s1);
    e.published_at.reset();
    sync(R::update(s1, e));
    check("update non-null to null");

    e = load(s1);
    e.title = "renamed";
    sync(R::update(s1, e));
    check("update title");

    sync(R::erase(n1));
    check("erase");

    sync(R::eraseWhere({.published_null = true}));
    check("eraseWhere");
}
