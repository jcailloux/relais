/**
 * test_decl_list_full.cpp
 *
 * Tests for declarative list caching at L1+L2 (Both).
 * Verifies list cache interaction between RAM and Redis layers.
 *
 * Covers:
 *   1. Article list query at L1+L2
 *   2. Cascade invalidation (list + entity in both layers)
 *   3. Entity and list on same repo (cross-interaction)
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"
using namespace relais_test;

// #############################################################################
//
//  Local L1+L2 list repos
//
// #############################################################################

namespace relais_test {

using FullCacheArticleListRepo = Repo<TestArticleWrapper, "test:article:list:both", cfg::Both>;
using FullCachePurchaseListRepo = Repo<TestPurchaseWrapper, "test:purchase:list:both", cfg::Both>;

using FullArticleListQuery = FullCacheArticleListRepo::ListQuery;
using FullPurchaseListQuery = FullCachePurchaseListRepo::ListQuery;

} // namespace relais_test

// =============================================================================
// L1+L2 query helpers
// =============================================================================

namespace decl = jcailloux::relais::cache::list::decl;

static FullArticleListQuery makeFullArticleQuery(
    std::optional<std::string> category = std::nullopt,
    std::optional<int64_t> author_id = std::nullopt,
    uint16_t limit = 10
) {
    FullArticleListQuery q;
    q.limit = limit;
    if (category) q.filters.template get<0>() = std::move(*category);
    if (author_id) q.filters.template get<1>() = *author_id;

    using Desc = FullCacheArticleListRepo::ListDescriptorType;
    q.group_key = decl::groupCacheKey<Desc>(q);
    q.cache_key = decl::cacheKey<Desc>(q);
    return q;
}

static FullPurchaseListQuery makeFullPurchaseQuery(
    std::optional<int64_t> user_id = std::nullopt,
    std::optional<std::string> status = std::nullopt,
    uint16_t limit = 10
) {
    FullPurchaseListQuery q;
    q.limit = limit;
    if (user_id) q.filters.template get<0>() = *user_id;
    if (status) q.filters.template get<1>() = std::move(*status);

    using Desc = FullCachePurchaseListRepo::ListDescriptorType;
    q.group_key = decl::groupCacheKey<Desc>(q);
    q.cache_key = decl::cacheKey<Desc>(q);
    return q;
}


// #############################################################################
//
//  1. Article list query at L1+L2
//
// #############################################################################

TEST_CASE("[DeclList L1+L2] Article list query",
          "[integration][db][list][full-cache][query]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<FullCacheArticleListRepo>();

    SECTION("[query] list cached in L1 after first query") {
        auto userId = insertTestUser("author", "author@both.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("tech", userId, "Tech 2", 20);

        auto result = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(result->size() == 2);

        // Verify L1 caching: insert directly, cache should return stale
        insertTestArticle("tech", userId, "Tech 3", 30);

        auto cached = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(cached->size() == 2);  // Stale from L1
    }

    SECTION("[query] L1 hit prevents L2 query") {
        auto userId = insertTestUser("author", "author@both.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // First query populates both L1 and L2
        sync(FullCacheArticleListRepo::query(makeFullArticleQuery("tech")));

        // Insert directly in DB
        insertTestArticle("tech", userId, "Tech 2", 20);

        // L1 hit returns stale (2nd article not visible)
        auto cached = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(cached->size() == 1);
    }

    SECTION("[query] combined filters work at L1+L2") {
        auto user1 = insertTestUser("alice", "alice@both.com", 0);
        auto user2 = insertTestUser("bob", "bob@both.com", 0);
        insertTestArticle("tech", user1, "Alice Tech", 10);
        insertTestArticle("news", user1, "Alice News", 20);
        insertTestArticle("tech", user2, "Bob Tech", 30);
        insertTestArticle("news", user2, "Bob News", 40);

        auto result = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech", user2)));
        REQUIRE(result->size() == 1);
    }
}


// #############################################################################
//
//  2. Cascade invalidation
//
// #############################################################################

TEST_CASE("[DeclList L1+L2] Cascade invalidation",
          "[integration][db][list][full-cache][invalidation]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<FullCacheArticleListRepo>();

    SECTION("[invalidation] insert invalidates list in both L1 and L2") {
        auto userId = insertTestUser("author", "author@both.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate cache
        auto r1 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r1->size() == 1);

        // insert via repo → should invalidate list
        auto newArticle = makeTestArticle("tech", userId, "Tech 2", 20);
        sync(FullCacheArticleListRepo::insert(newArticle));

        // Next query should reflect the new article
        auto r2 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r2->size() == 2);
    }

    SECTION("[invalidation] update invalidates both layers") {
        auto userId = insertTestUser("author", "author@both.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Before", 10);

        // Populate list cache
        sync(FullCacheArticleListRepo::query(makeFullArticleQuery("tech")));

        // Update via repo
        auto updated = makeTestArticle("tech", userId, "After", 20, false, articleId);
        sync(FullCacheArticleListRepo::update(articleId, updated));

        // List should be refreshed
        auto result = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(result->size() == 1);
        REQUIRE(result->items.front().title == "After");
    }

    SECTION("[invalidation] delete invalidates both layers") {
        auto userId = insertTestUser("author", "author@both.com", 0);
        auto articleId = insertTestArticle("tech", userId, "To Delete", 10);
        insertTestArticle("tech", userId, "To Keep", 20);

        // Populate list cache
        auto r1 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r1->size() == 2);

        // Delete via repo
        sync(FullCacheArticleListRepo::erase(articleId));

        // List should show only the remaining article
        auto r2 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r2->size() == 1);
    }
}


// #############################################################################
//
//  3. Entity and list on same repo
//
// #############################################################################

TEST_CASE("[DeclList L1+L2] Entity and list on same repo",
          "[integration][db][list][full-cache][cross-inv]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<FullCacheArticleListRepo>();

    SECTION("[cross-inv] entity update invalidates list but entity cache reflects update") {
        auto userId = insertTestUser("author", "author@both.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Original", 10);

        // Cache both entity and list
        sync(FullCacheArticleListRepo::find(articleId));
        sync(FullCacheArticleListRepo::query(makeFullArticleQuery("tech")));

        // Update entity via repo
        auto updated = makeTestArticle("tech", userId, "Updated", 20, false, articleId);
        sync(FullCacheArticleListRepo::update(articleId, updated));

        // Entity cache should reflect the update
        auto entity = sync(FullCacheArticleListRepo::find(articleId));
        REQUIRE(entity != nullptr);
        REQUIRE(entity->title == "Updated");

        // List should also reflect the update (invalidated and re-fetched)
        auto list = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(list->size() == 1);
        REQUIRE(list->items.front().title == "Updated");
    }

    SECTION("[cross-inv] list query repopulates after entity-triggered invalidation") {
        auto userId = insertTestUser("author", "author@both.com", 0);
        insertTestArticle("tech", userId, "Article 1", 10);

        // Populate list cache
        auto r1 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r1->size() == 1);

        // insert a new entity via repo → invalidates list
        auto newArticle = makeTestArticle("tech", userId, "Article 2", 20);
        auto created = sync(FullCacheArticleListRepo::insert(newArticle));
        REQUIRE(created != nullptr);

        // List re-fetches from DB, includes new article
        auto r2 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r2->size() == 2);

        // Further DB-direct insert not visible (list is now cached)
        insertTestArticle("tech", userId, "Article 3", 30);
        auto r3 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r3->size() == 2);  // Stale from cache
    }
}
