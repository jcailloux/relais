/**
 * test_decl_list_redis.cpp
 *
 * Tests for declarative list caching at L2 (Redis).
 * Mirrors test_decl_list_cache.cpp patterns but uses L2-only repos.
 *
 * Covers:
 *   1. Article list query (filters, limit, empty)
 *   2. Purchase list query (filters, combined)
 *   3. L2 CRUD invalidation (active invalidation via Redis Lua scripts)
 *   4. L2 cache lifecycle (Redis store/hit verification)
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"
using namespace relais_test;

namespace decl = jcailloux::relais::cache::list::decl;

// #############################################################################
//
//  Local L2 list repos
//
// #############################################################################

namespace relais_test {

using L2DeclArticleListRepo = Repo<TestArticleWrapper, "test:article:list:l2:decl", cfg::Redis>;
using L2DeclPurchaseListRepo = Repo<TestPurchaseWrapper, "test:purchase:list:l2:decl", cfg::Redis>;

// Type aliases for L2 list queries
using L2ArticleListQuery = L2DeclArticleListRepo::ListQuery;
using L2PurchaseListQuery = L2DeclPurchaseListRepo::ListQuery;

// Type aliases for L2 declarative query types
using L2ArticleDecl = L2DeclArticleListRepo::ListDescriptorType;
using L2ArticleDescQuery = decl::ListDescriptorQuery<L2ArticleDecl>;

} // namespace relais_test

// =============================================================================
// Helper: build a TestArticleWrapper from raw values (for SortBounds tests)
// =============================================================================

static std::shared_ptr<const TestArticleWrapper> makeArticle(
    int64_t id,
    const std::string& category,
    int64_t author_id,
    const std::string& title,
    int32_t view_count
) {
    return makeTestArticle(category, author_id, title, view_count, false, id);
}

// =============================================================================
// L2 query helpers (using L2 repo types)
// =============================================================================

static L2ArticleListQuery makeL2ArticleQuery(
    std::optional<std::string> category = std::nullopt,
    std::optional<int64_t> author_id = std::nullopt,
    uint16_t limit = 10
) {
    L2ArticleListQuery q;
    q.limit = limit;
    if (category) q.filters.template get<0>() = std::move(*category);
    if (author_id) q.filters.template get<1>() = *author_id;

    using Desc = L2DeclArticleListRepo::ListDescriptorType;
    q.group_key = decl::groupCacheKey<Desc>(q);
    q.cache_key = decl::cacheKey<Desc>(q);
    return q;
}

static L2PurchaseListQuery makeL2PurchaseQuery(
    std::optional<int64_t> user_id = std::nullopt,
    std::optional<std::string> status = std::nullopt,
    uint16_t limit = 10
) {
    L2PurchaseListQuery q;
    q.limit = limit;
    if (user_id) q.filters.template get<0>() = *user_id;
    if (status) q.filters.template get<1>() = std::move(*status);

    using Desc = L2DeclPurchaseListRepo::ListDescriptorType;
    q.group_key = decl::groupCacheKey<Desc>(q);
    q.cache_key = decl::cacheKey<Desc>(q);
    return q;
}

/// Build a ListDescriptorQuery for articles sorted by view_count DESC (L2 variant).
static L2ArticleDescQuery makeL2ViewCountQuery(std::string_view category, uint16_t limit) {
    L2ArticleDescQuery q;
    q.limit = limit;
    q.filters.get<0>() = category;
    q.sort = jcailloux::relais::cache::list::SortSpec<size_t>{1, jcailloux::relais::cache::list::SortDirection::Desc};

    q.group_key = decl::groupCacheKey<L2ArticleDecl>(q);
    q.cache_key = decl::cacheKey<L2ArticleDecl>(q);
    return q;
}


// #############################################################################
//
//  1. Article list query at L2
//
// #############################################################################

TEST_CASE("[DeclList L2] Article list query",
          "[integration][db][redis][list][query]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[query] returns all articles when no filter") {
        auto userId = insertTestUser("author", "author@l2.com", 0);
        insertTestArticle("tech", userId, "Article A", 10);
        insertTestArticle("news", userId, "Article B", 20);
        insertTestArticle("tech", userId, "Article C", 30);

        auto result = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery()));
        REQUIRE(result->size() == 3);
    }

    SECTION("[query] filters by category") {
        auto userId = insertTestUser("author", "author@l2.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);
        insertTestArticle("tech", userId, "Tech 2", 30);

        auto result = sync(L2DeclArticleListRepo::query(
            makeL2ArticleQuery("tech")));
        REQUIRE(result->size() == 2);
    }

    SECTION("[query] combined filters") {
        auto user1 = insertTestUser("alice", "alice@l2.com", 0);
        auto user2 = insertTestUser("bob", "bob@l2.com", 0);
        insertTestArticle("tech", user1, "Alice Tech", 10);
        insertTestArticle("news", user1, "Alice News", 20);
        insertTestArticle("tech", user2, "Bob Tech", 30);

        auto result = sync(L2DeclArticleListRepo::query(
            makeL2ArticleQuery("tech", user2)));
        REQUIRE(result->size() == 1);
    }

    SECTION("[query] returns empty for non-matching filter") {
        auto userId = insertTestUser("author", "author@l2.com", 0);
        insertTestArticle("tech", userId, "Tech Article", 10);

        auto result = sync(L2DeclArticleListRepo::query(
            makeL2ArticleQuery("nonexistent")));
        REQUIRE(result->size() == 0);
        REQUIRE(result->empty());
    }

    SECTION("[query] respects limit") {
        auto userId = insertTestUser("author", "author@l2.com", 0);
        for (int i = 0; i < 5; ++i) {
            insertTestArticle("tech", userId, "Article " + std::to_string(i), i * 10);
        }

        auto result = sync(L2DeclArticleListRepo::query(
            makeL2ArticleQuery(std::nullopt, std::nullopt, 10)));
        REQUIRE(result->size() == 5);
    }
}


// #############################################################################
//
//  2. Purchase list query at L2
//
// #############################################################################

TEST_CASE("[DeclList L2] Purchase list query",
          "[integration][db][redis][list][query]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclPurchaseListRepo>();

    SECTION("[query] filters by user_id") {
        auto user1 = insertTestUser("alice", "alice@l2.com", 500);
        auto user2 = insertTestUser("bob", "bob@l2.com", 500);
        insertTestPurchase(user1, "Widget", 100);
        insertTestPurchase(user2, "Gadget", 200);
        insertTestPurchase(user2, "Doohickey", 300);

        auto result = sync(L2DeclPurchaseListRepo::query(
            makeL2PurchaseQuery(user2)));
        REQUIRE(result->size() == 2);
    }

    SECTION("[query] filters by status") {
        auto userId = insertTestUser("buyer", "buyer@l2.com", 1000);
        insertTestPurchase(userId, "Item A", 100, "completed");
        insertTestPurchase(userId, "Item B", 200, "pending");
        insertTestPurchase(userId, "Item C", 300, "completed");

        auto result = sync(L2DeclPurchaseListRepo::query(
            makeL2PurchaseQuery(std::nullopt, std::string("completed"))));
        REQUIRE(result->size() == 2);
    }

    SECTION("[query] combined user_id and status filter") {
        auto user1 = insertTestUser("alice", "alice@l2.com", 500);
        auto user2 = insertTestUser("bob", "bob@l2.com", 500);
        insertTestPurchase(user1, "A", 100, "completed");
        insertTestPurchase(user1, "B", 200, "pending");
        insertTestPurchase(user2, "C", 300, "completed");

        auto result = sync(L2DeclPurchaseListRepo::query(
            makeL2PurchaseQuery(user1, std::string("pending"))));
        REQUIRE(result->size() == 1);
    }
}


// #############################################################################
//
//  3. L2 CRUD invalidation (active invalidation via Redis Lua scripts)
//
// #############################################################################

TEST_CASE("[DeclList L2] CRUD invalidation",
          "[integration][db][redis][list][invalidation]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[invalidation] insert via repo invalidates L2 cache") {
        auto userId = insertTestUser("author", "author@l2inv.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate L2 cache
        auto r1 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        REQUIRE(r1->size() == 1);

        // Insert via repo → triggers invalidateRedisListGroups()
        auto newArticle = makeTestArticle("tech", userId, "Tech 2", 20);
        sync(L2DeclArticleListRepo::insert(newArticle));

        // Next query should hit DB and see the new article
        auto r2 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r2->size() == 2);
    }

    SECTION("[invalidation] update via repo invalidates L2 cache") {
        auto userId = insertTestUser("author", "author@l2inv.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Before", 10);

        // Populate L2 cache
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));

        // Update via repo
        auto updated = makeTestArticle("tech", userId, "After", 20, false, articleId);
        sync(L2DeclArticleListRepo::update(articleId, updated));

        // List should reflect the update
        auto result = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(result->size() == 1);
        CHECK(result->items.front().title == "After");
    }

    SECTION("[invalidation] delete via repo invalidates L2 cache") {
        auto userId = insertTestUser("author", "author@l2inv.com", 0);
        auto articleId = insertTestArticle("tech", userId, "To Delete", 10);
        insertTestArticle("tech", userId, "To Keep", 20);

        // Populate L2 cache
        auto r1 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        REQUIRE(r1->size() == 2);

        // Delete via repo
        sync(L2DeclArticleListRepo::erase(articleId));

        // List should show only the remaining article
        auto r2 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r2->size() == 1);
    }

    SECTION("[invalidation] invalidation clears all groups for this repo") {
        auto userId = insertTestUser("author", "author@l2inv.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);

        // Populate two different filter groups in L2
        auto r_tech = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        auto r_news = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));
        REQUIRE(r_tech->size() == 1);
        REQUIRE(r_news->size() == 1);

        // Insert a "tech" article via repo → all groups invalidated (non-selective)
        auto newArticle = makeTestArticle("tech", userId, "Tech 2", 30);
        sync(L2DeclArticleListRepo::insert(newArticle));

        // Both groups are invalidated (coarse-grained), re-fetched from DB
        auto r_tech_after = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r_tech_after->size() == 2);

        auto r_news_after = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));
        CHECK(r_news_after->size() == 1);
    }
}


// #############################################################################
//
//  4. L2 cache lifecycle (Redis store/hit verification)
//
// #############################################################################

TEST_CASE("[DeclList L2] Cache lifecycle",
          "[integration][db][redis][list][lifecycle]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[lifecycle] second query hits Redis (stale check)") {
        auto userId = insertTestUser("author", "author@l2life.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // First query → DB → store Redis
        auto r1 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        REQUIRE(r1->size() == 1);

        // Insert directly in DB (bypasses repo, no invalidation)
        insertTestArticle("tech", userId, "Tech 2", 20);

        // Second query → Redis hit → returns stale data (1, not 2)
        auto r2 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r2->size() == 1);
    }

    SECTION("[lifecycle] CRUD clears Redis, next query hits DB") {
        auto userId = insertTestUser("author", "author@l2life.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate L2 cache
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));

        // Insert directly in DB (not through repo)
        insertTestArticle("tech", userId, "Tech 2", 20);

        // Still stale from Redis
        auto r_stale = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r_stale->size() == 1);

        // Now insert via repo → triggers L2 invalidation
        auto newArticle = makeTestArticle("tech", userId, "Tech 3", 30);
        sync(L2DeclArticleListRepo::insert(newArticle));

        // Query now hits DB → sees all 3 articles
        auto r_fresh = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r_fresh->size() == 3);
    }

    SECTION("[lifecycle] different queries are cached independently") {
        auto userId = insertTestUser("author", "author@l2life.com", 0);
        insertTestArticle("tech", userId, "Tech", 10);
        insertTestArticle("news", userId, "News", 20);

        // Cache both queries
        auto r_tech = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        auto r_news = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));
        REQUIRE(r_tech->size() == 1);
        REQUIRE(r_news->size() == 1);

        // Insert directly in DB
        insertTestArticle("tech", userId, "Tech 2", 30);
        insertTestArticle("news", userId, "News 2", 40);

        // Both return stale from Redis
        auto r_tech_stale = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        auto r_news_stale = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));
        CHECK(r_tech_stale->size() == 1);
        CHECK(r_news_stale->size() == 1);
    }
}
