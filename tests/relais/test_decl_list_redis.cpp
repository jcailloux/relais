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
    if (author_id) q.filters.template get<0>() = *author_id;
    if (category) q.filters.template get<1>() = std::move(*category);

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
    if (status) q.filters.template get<0>() = std::move(*status);
    if (user_id) q.filters.template get<1>() = *user_id;

    using Desc = L2DeclPurchaseListRepo::ListDescriptorType;
    q.group_key = decl::groupCacheKey<Desc>(q);
    q.cache_key = decl::cacheKey<Desc>(q);
    return q;
}

/// Build a ListDescriptorQuery for articles sorted by view_count DESC (L2 variant).
static L2ArticleDescQuery makeL2ViewCountQuery(std::string_view category, uint16_t limit) {
    L2ArticleDescQuery q;
    q.limit = limit;
    q.filters.get<1>() = category;
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

        // Insert via repo → triggers selective L2 invalidation
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

        // Insert a "tech" article via repo → selective L2 invalidation on all groups
        auto newArticle = makeTestArticle("tech", userId, "Tech 2", 30);
        sync(L2DeclArticleListRepo::insert(newArticle));

        // Both groups are invalidated (small pages, first+incomplete → always hit)
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


// #############################################################################
//
//  5. L2 notify* path (exercising fireL2* synchronously)
//
// #############################################################################

TEST_CASE("[DeclList L2] notify* path (L2 invalidation)",
          "[integration][db][redis][list][notify]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[notify] notifyCreated invalidates L2 cache") {
        auto userId = insertTestUser("author", "author@l2notify.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate L2 cache
        auto r1 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        REQUIRE(r1->size() == 1);

        // Insert sentinel directly in DB (invisible to cache)
        insertTestArticle("tech", userId, "Sentinel", 20);

        // Verify cache still returns stale data
        auto r_stale = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        REQUIRE(r_stale->size() == 1);

        // notifyCreatedSync → L2 invalidation (synchronous)
        auto entity = makeTestArticle("tech", userId, "Notified", 30);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // L2 invalidated → DB hit → sees original + sentinel
        auto r2 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r2->size() == 2);
    }

    SECTION("[notify] notifyUpdated invalidates L2 cache") {
        auto userId = insertTestUser("author", "author@l2notify.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Before", 10);

        // Populate L2 cache
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));

        // Update directly in DB
        updateTestArticle(articleId, "After", 20);

        // notifyUpdatedSync → L2 invalidation
        auto oldEntity = makeTestArticle("tech", userId, "Before", 10, false, articleId);
        auto newEntity = makeTestArticle("tech", userId, "After", 20, false, articleId);
        TestInternals::notifyUpdatedSync<L2DeclArticleListRepo>(oldEntity, newEntity);

        // L2 invalidated → DB hit → sees updated title
        auto result = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(result->size() == 1);
        CHECK(result->items.front().title == "After");
    }

    SECTION("[notify] notifyDeleted invalidates L2 cache") {
        auto userId = insertTestUser("author", "author@l2notify.com", 0);
        auto articleId = insertTestArticle("tech", userId, "To Delete", 10);
        insertTestArticle("tech", userId, "To Keep", 20);

        // Populate L2 cache
        auto r1 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        REQUIRE(r1->size() == 2);

        // Delete directly in DB
        deleteTestArticle(articleId);

        // notifyDeletedSync → L2 invalidation
        auto entity = makeTestArticle("tech", userId, "To Delete", 10, false, articleId);
        TestInternals::notifyDeletedSync<L2DeclArticleListRepo>(entity);

        // L2 invalidated → DB hit → only remaining article
        auto r2 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r2->size() == 1);
    }
}


// #############################################################################
//
//  6. L2 Filter-based selective invalidation
//
// #############################################################################

TEST_CASE("[DeclList L2] Filter-based selective invalidation",
          "[integration][db][redis][list][filter-match]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[filter-match] insert tech does NOT invalidate news cache") {
        auto userId = insertTestUser("author", "author@l2fm.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);

        // Populate both groups in L2
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));

        // Insert sentinels (bypass repo)
        insertTestArticle("tech", userId, "Tech Sentinel", 30);
        insertTestArticle("news", userId, "News Sentinel", 40);

        // notifyCreatedSync with tech entity → only tech group invalidated
        auto entity = makeTestArticle("tech", userId, "Tech Notify", 50);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // tech SUPPRIMÉ: DB hit → original + sentinel = 2
        auto r_tech = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r_tech->size() == 2);

        // news CONSERVÉ: cache hit → sentinel invisible = 1
        auto r_news = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));
        CHECK(r_news->size() == 1);
    }

    SECTION("[filter-match] insert invalidates unfiltered group") {
        auto userId = insertTestUser("author", "author@l2fm.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate unfiltered group in L2
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery()));

        // Insert sentinel
        insertTestArticle("news", userId, "Sentinel", 20);

        // notifyCreatedSync with any entity → unfiltered group always matches
        auto entity = makeTestArticle("sports", userId, "Sports", 30);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // Unfiltered SUPPRIMÉ: DB hit → sees all articles
        auto r = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery()));
        CHECK(r->size() == 2);
    }

    SECTION("[filter-match] update category tech→news invalidates both groups") {
        auto userId = insertTestUser("author", "author@l2fm.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Migrating", 10);
        insertTestArticle("news", userId, "Existing News", 20);

        // Populate both groups
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));

        // Insert sentinels
        insertTestArticle("tech", userId, "Tech Sentinel", 30);
        insertTestArticle("news", userId, "News Sentinel", 40);

        // Update DB directly (move tech→news)
        updateTestArticleCategory(articleId, "news");

        // notifyUpdatedSync: old=tech, new=news → invalidates BOTH
        auto oldEntity = makeTestArticle("tech", userId, "Migrating", 10, false, articleId);
        auto newEntity = makeTestArticle("news", userId, "Migrating", 10, false, articleId);
        TestInternals::notifyUpdatedSync<L2DeclArticleListRepo>(oldEntity, newEntity);

        // tech SUPPRIMÉ: only sentinel left (original moved out)
        auto r_tech = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(r_tech->size() == 1);

        // news SUPPRIMÉ: existing + migrated + sentinel
        auto r_news = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));
        CHECK(r_news->size() == 3);
    }

    SECTION("[filter-match] notifyCreated return value reflects pages deleted") {
        auto userId = insertTestUser("author", "author@l2fm.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);

        // Populate both groups
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));

        // notifyCreatedSync with tech → returns pages deleted (tech group only)
        auto entity = makeTestArticle("tech", userId, "Tech 2", 30);
        auto pages_deleted = TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);
        CHECK(pages_deleted == 1);
    }
}


// #############################################################################
//
//  7. Lua filter matching — binary parsing
//
// #############################################################################

TEST_CASE("[DeclList L2] Lua filter matching — binary parsing",
          "[integration][db][redis][list][lua-filter]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[lua-filter] string EQ match + mismatch") {
        auto userId = insertTestUser("author", "author@lua.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);

        // Populate both groups
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));

        // Insert sentinels
        insertTestArticle("tech", userId, "Tech Sentinel", 30);
        insertTestArticle("news", userId, "News Sentinel", 40);

        // Notify with tech entity → string EQ match on category="tech"
        auto entity = makeTestArticle("tech", userId, "Notify", 50);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // tech SUPPRIMÉ: DB hit → original + sentinel
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")))->size() == 2);
        // news CONSERVÉ: cache hit → sentinel invisible
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")))->size() == 1);
    }

    SECTION("[lua-filter] int64 EQ match + mismatch") {
        auto user42 = insertTestUser("user42", "user42@lua.com", 0);
        auto user99 = insertTestUser("user99", "user99@lua.com", 0);
        insertTestArticle("tech", user42, "By 42", 10);
        insertTestArticle("tech", user99, "By 99", 20);

        // Populate groups by author_id
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery(std::nullopt, user42)));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery(std::nullopt, user99)));

        // Insert sentinels
        insertTestArticle("tech", user42, "Sentinel 42", 30);
        insertTestArticle("tech", user99, "Sentinel 99", 40);

        // Notify with author_id=user42
        auto entity = makeTestArticle("tech", user42, "Notify", 50);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // author=42 SUPPRIMÉ
        CHECK(sync(L2DeclArticleListRepo::query(
            makeL2ArticleQuery(std::nullopt, user42)))->size() == 2);
        // author=99 CONSERVÉ
        CHECK(sync(L2DeclArticleListRepo::query(
            makeL2ArticleQuery(std::nullopt, user99)))->size() == 1);
    }

    SECTION("[lua-filter] combined filters partial mismatch") {
        auto user42 = insertTestUser("user42", "user42@lua.com", 0);
        auto user99 = insertTestUser("user99", "user99@lua.com", 0);
        insertTestArticle("tech", user42, "Tech by 42", 10);

        // Populate group (category=tech, author=42)
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech", user42)));

        // Insert sentinel
        insertTestArticle("tech", user42, "Sentinel", 20);

        // Notify with (category=tech, author=99) → mismatch on author_id
        auto entity = makeTestArticle("tech", user99, "Tech by 99", 30);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // (tech,42) CONSERVÉ: entity author doesn't match
        CHECK(sync(L2DeclArticleListRepo::query(
            makeL2ArticleQuery("tech", user42)))->size() == 1);
    }

    SECTION("[lua-filter] group no filter matches any entity") {
        auto userId = insertTestUser("author", "author@lua.com", 0);
        insertTestArticle("tech", userId, "Article 1", 10);

        // Populate unfiltered group
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery()));

        // Insert sentinel
        insertTestArticle("news", userId, "Sentinel", 20);

        // Notify with any entity → unfiltered always matches
        auto entity = makeTestArticle("sports", userId, "Sports", 30);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // Unfiltered SUPPRIMÉ
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery()))->size() == 2);
    }

    SECTION("[lua-filter] group partial filter (one active, one inactive)") {
        auto user42 = insertTestUser("user42", "user42@lua.com", 0);
        auto user99 = insertTestUser("user99", "user99@lua.com", 0);
        insertTestArticle("tech", user42, "Tech 1", 10);

        // Populate group (category=tech, author=∅) — only category filter active
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));

        // Insert sentinel
        insertTestArticle("tech", user99, "Sentinel", 20);

        // Notify with (tech, author=99) → category matches, author filter inactive
        auto entity = makeTestArticle("tech", user99, "Tech by 99", 30);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // (tech, ∅) SUPPRIMÉ: inactive filter = no constraint on author
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")))->size() == 2);
    }
}


// #############################################################################
//
//  8. Lua SortBounds — per-page precision
//
// #############################################################################

TEST_CASE("[DeclList L2] Lua SortBounds — per-page precision",
          "[integration][db][redis][list][lua-sort]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[lua-sort] first+incomplete page always invalidated") {
        auto userId = insertTestUser("author", "author@sort.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate with sorted query (first+incomplete: 1 item, limit=2)
        auto q = makeL2ViewCountQuery("tech", 2);
        auto r1 = sync(L2DeclArticleListRepo::query(q));
        REQUIRE(r1->size() == 1);

        // Insert sentinel
        insertTestArticle("tech", userId, "Sentinel", 20);

        // Notify with entity having any sort value → first+incomplete always hit
        auto entity = makeTestArticle("tech", userId, "Notify", 999);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // Page SUPPRIMÉ
        CHECK(sync(L2DeclArticleListRepo::query(q))->size() == 2);
    }

    SECTION("[lua-sort] in-range invalidated, out-of-range preserved") {
        auto userId = insertTestUser("author", "author@sort.com", 0);
        // 4 articles: [100, 80, 60, 40] sorted by view_count DESC, limit=2
        auto a100 = insertTestArticle("tech", userId, "A100", 100);
        auto a80  = insertTestArticle("tech", userId, "A80", 80);
        auto a60  = insertTestArticle("tech", userId, "A60", 60);
        auto a40  = insertTestArticle("tech", userId, "A40", 40);

        // Page 1: [100, 80] (fp=true, cursor mode, complete)
        auto q1 = makeL2ViewCountQuery("tech", 2);
        auto p1 = sync(L2DeclArticleListRepo::query(q1));
        REQUIRE(p1->size() == 2);
        REQUIRE(p1->items[0].view_count.value() == 100);
        REQUIRE(p1->items[1].view_count.value() == 80);

        // Page 2: [60, 40] via cursor (fp=false, cursor mode, complete)
        auto q2 = makeL2ViewCountQuery("tech", 2);
        q2.cursor = jcailloux::relais::cache::list::Cursor::decode(
            std::string(p1->cursor())).value();
        q2.cache_key = decl::cacheKey<L2ArticleDecl>(q2);
        auto p2 = sync(L2DeclArticleListRepo::query(q2));
        REQUIRE(p2->size() == 2);
        REQUIRE(p2->items[0].view_count.value() == 60);
        REQUIRE(p2->items[1].view_count.value() == 40);

        // Insert sentinel in page 2 range
        insertTestArticle("tech", userId, "Sentinel55", 55);

        // Notify with view_count=55
        // Page 1 bounds [100,80]: fp=true → always invalidated
        // Page 2 bounds [60,40]: 55 <= 60 AND 55 >= 40 → INVALIDATED
        auto entity = makeTestArticle("tech", userId, "Notify55", 55);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // Page 2 SUPPRIMÉ: DB hit → sentinel visible
        auto p2_fresh = sync(L2DeclArticleListRepo::query(q2));
        bool sentinel_visible = false;
        for (const auto& item : p2_fresh->items) {
            if (item.view_count.value_or(0) == 55) { sentinel_visible = true; break; }
        }
        CHECK(sentinel_visible);
    }

    SECTION("[lua-sort] update with sort change — range check") {
        auto userId = insertTestUser("author", "author@sort.com", 0);
        // 4 articles: [100, 80, 60, 40] sorted by view_count DESC, limit=2
        auto a100 = insertTestArticle("tech", userId, "A100", 100);
        auto a80  = insertTestArticle("tech", userId, "A80", 80);
        auto a60  = insertTestArticle("tech", userId, "A60", 60);
        auto a40  = insertTestArticle("tech", userId, "A40", 40);

        // Page 1: [100, 80]
        auto q1 = makeL2ViewCountQuery("tech", 2);
        auto p1 = sync(L2DeclArticleListRepo::query(q1));
        REQUIRE(p1->size() == 2);

        // Page 2: [60, 40] via cursor
        auto q2 = makeL2ViewCountQuery("tech", 2);
        q2.cursor = jcailloux::relais::cache::list::Cursor::decode(
            std::string(p1->cursor())).value();
        q2.cache_key = decl::cacheKey<L2ArticleDecl>(q2);
        auto p2 = sync(L2DeclArticleListRepo::query(q2));
        REQUIRE(p2->size() == 2);

        // Insert sentinel in page 2 range
        insertTestArticle("tech", userId, "Sentinel105", 105);

        // Update a80: view_count 80→110 in DB
        updateTestArticle(a80, "A80-updated", 110);

        // notifyUpdatedSync: old=80, new=110
        // Page 1 (fp=true): inr always returns true → INVALIDATED
        // Page 2 (fp=false, bounds [60,40]):
        //   inr(80): 80<=60? no → false
        //   inr(110): 110<=60? no → false
        //   → PRESERVED
        auto oldEntity = makeTestArticle("tech", userId, "A80", 80, false, a80);
        auto newEntity = makeTestArticle("tech", userId, "A80-updated", 110, false, a80);
        TestInternals::notifyUpdatedSync<L2DeclArticleListRepo>(oldEntity, newEntity);

        // Page 2 CONSERVÉ: cache hit → still [60, 40]
        auto p2_cached = sync(L2DeclArticleListRepo::query(q2));
        CHECK(p2_cached->items[0].view_count.value() == 60);
        CHECK(p2_cached->items[1].view_count.value() == 40);
    }
}


// #############################################################################
//
//  9. Lua all-in-one — multi-group correctness
//
// #############################################################################

TEST_CASE("[DeclList L2] Lua all-in-one — multi-group correctness",
          "[integration][db][redis][list][lua-multi]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[lua-multi] 3 groups, only matching one is invalidated") {
        auto userId = insertTestUser("author", "author@multi.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);
        insertTestArticle("sports", userId, "Sports 1", 30);

        // Populate 3 groups
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("sports")));

        // Insert sentinels
        insertTestArticle("tech", userId, "Tech Sentinel", 40);
        insertTestArticle("news", userId, "News Sentinel", 50);
        insertTestArticle("sports", userId, "Sports Sentinel", 60);

        // notifyCreatedSync with tech → only tech invalidated
        auto entity = makeTestArticle("tech", userId, "Tech Notify", 70);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // tech SUPPRIMÉ
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")))->size() == 2);
        // news CONSERVÉ
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")))->size() == 1);
        // sports CONSERVÉ
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("sports")))->size() == 1);
    }

    SECTION("[lua-multi] unfiltered + filtered, insert invalidates correctly") {
        auto userId = insertTestUser("author", "author@multi.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate unfiltered + tech groups
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery()));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));

        // Insert sentinel
        insertTestArticle("tech", userId, "Sentinel", 20);

        // notifyCreatedSync with tech → both groups invalidated
        auto entity = makeTestArticle("tech", userId, "Tech Notify", 30);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // tech SUPPRIMÉ
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")))->size() == 2);
        // unfiltered SUPPRIMÉ
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery()))->size() == 2);
    }

    SECTION("[lua-multi] update cross-category invalidates old and new only") {
        auto userId = insertTestUser("author", "author@multi.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Migrating", 10);
        insertTestArticle("news", userId, "News 1", 20);
        insertTestArticle("sports", userId, "Sports 1", 30);

        // Populate 3 groups
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("sports")));

        // Insert sentinels
        insertTestArticle("tech", userId, "Tech Sentinel", 40);
        insertTestArticle("news", userId, "News Sentinel", 50);
        insertTestArticle("sports", userId, "Sports Sentinel", 60);

        // Update DB: move tech→news
        updateTestArticleCategory(articleId, "news");

        // notifyUpdatedSync: old=tech, new=news
        auto oldEntity = makeTestArticle("tech", userId, "Migrating", 10, false, articleId);
        auto newEntity = makeTestArticle("news", userId, "Migrating", 10, false, articleId);
        TestInternals::notifyUpdatedSync<L2DeclArticleListRepo>(oldEntity, newEntity);

        // tech SUPPRIMÉ (old group): sentinel only
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")))->size() == 1);
        // news SUPPRIMÉ (new group): existing + migrated + sentinel
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")))->size() == 3);
        // sports CONSERVÉ
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("sports")))->size() == 1);
    }

    SECTION("[lua-multi] delete invalidates only matching group") {
        auto userId = insertTestUser("author", "author@multi.com", 0);
        auto articleId = insertTestArticle("tech", userId, "To Delete", 10);
        insertTestArticle("news", userId, "News 1", 20);

        // Populate 2 groups
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));

        // Insert sentinels
        insertTestArticle("tech", userId, "Tech Sentinel", 30);
        insertTestArticle("news", userId, "News Sentinel", 40);

        // Delete from DB
        deleteTestArticle(articleId);

        // notifyDeletedSync with tech entity
        auto entity = makeTestArticle("tech", userId, "To Delete", 10, false, articleId);
        TestInternals::notifyDeletedSync<L2DeclArticleListRepo>(entity);

        // tech SUPPRIMÉ: sentinel only
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")))->size() == 1);
        // news CONSERVÉ
        CHECK(sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")))->size() == 1);
    }

    SECTION("[lua-multi] return value counts total pages deleted") {
        auto userId = insertTestUser("author", "author@multi.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);

        // Populate 2 groups (1 page each)
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("news")));

        // notifyCreatedSync → returns pages deleted (tech group only = 1 page)
        auto entity = makeTestArticle("tech", userId, "Notify", 30);
        auto deleted = TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);
        CHECK(deleted == 1);
    }
}


// #############################################################################
//
//  10. ListBoundsHeader binary verification
//
// #############################################################################

namespace {
namespace list_ns = jcailloux::relais::cache::list;

/// Build the Redis page key for a declarative list query (reproduces ListMixin::redisPageKey).
template<typename RepoT>
std::string buildRedisPageKey(const std::string& cache_key) {
    std::string key(RepoT::name());
    key += ":dlist:p:";
    key.append(cache_key);
    return key;
}
} // anonymous namespace

TEST_CASE("[DeclList L2] ListBoundsHeader binary verification",
          "[integration][db][redis][list][header]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[header] sorted query stores correct bounds and flags") {
        auto userId = insertTestUser("author", "author@header.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A80", 80);
        insertTestArticle("tech", userId, "A60", 60);

        // Sorted query: view_count DESC, limit=10
        auto q = makeL2ViewCountQuery("tech", 10);
        auto result = sync(L2DeclArticleListRepo::query(q));
        REQUIRE(result->size() == 3);

        // Read raw binary from Redis (includes 19-byte header)
        auto redisKey = buildRedisPageKey<L2DeclArticleListRepo>(q.cache_key);
        auto raw = sync(jcailloux::relais::cache::RedisCache::getRawBinary(redisKey));
        REQUIRE(raw.has_value());
        REQUIRE(raw->size() >= list_ns::kListBoundsHeaderSize);

        // Verify magic bytes
        CHECK((*raw)[0] == list_ns::kListBoundsHeaderMagic[0]);  // 0x53
        CHECK((*raw)[1] == list_ns::kListBoundsHeaderMagic[1]);  // 0x52

        // Parse header
        auto header = list_ns::ListBoundsHeader::readFrom(raw->data(), raw->size());
        REQUIRE(header.has_value());

        // Verify sort bounds
        CHECK(header->bounds.first_value == 100);
        CHECK(header->bounds.last_value == 60);
        CHECK(header->bounds.is_valid == true);

        // Verify flags
        CHECK(header->sort_direction == list_ns::SortDirection::Desc);
        CHECK(header->is_first_page == true);
        CHECK(header->is_incomplete == true);   // 3 items < limit 10
        CHECK(header->pagination_mode == list_ns::PaginationMode::Offset);
    }

    SECTION("[header] page 2 via cursor has correct bounds and flags") {
        auto userId = insertTestUser("author", "author@header.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A80", 80);
        insertTestArticle("tech", userId, "A60", 60);
        insertTestArticle("tech", userId, "A40", 40);

        // Page 1: [100, 80] — limit=2, sorted by view_count DESC
        auto q1 = makeL2ViewCountQuery("tech", 2);
        auto p1 = sync(L2DeclArticleListRepo::query(q1));
        REQUIRE(p1->size() == 2);
        REQUIRE(p1->items[0].view_count.value() == 100);
        REQUIRE(p1->items[1].view_count.value() == 80);

        // Page 2 via cursor: [60, 40]
        auto q2 = makeL2ViewCountQuery("tech", 2);
        q2.cursor = list_ns::Cursor::decode(std::string(p1->cursor())).value();
        q2.cache_key = decl::cacheKey<L2ArticleDecl>(q2);
        auto p2 = sync(L2DeclArticleListRepo::query(q2));
        REQUIRE(p2->size() == 2);

        // Read raw binary for page 2
        auto redisKey = buildRedisPageKey<L2DeclArticleListRepo>(q2.cache_key);
        auto raw = sync(jcailloux::relais::cache::RedisCache::getRawBinary(redisKey));
        REQUIRE(raw.has_value());

        auto header = list_ns::ListBoundsHeader::readFrom(raw->data(), raw->size());
        REQUIRE(header.has_value());

        CHECK(header->bounds.first_value == 60);
        CHECK(header->bounds.last_value == 40);
        CHECK(header->is_first_page == false);
        CHECK(header->is_incomplete == false);   // 2 items == limit 2
        CHECK(header->pagination_mode == list_ns::PaginationMode::Cursor);
        CHECK(header->sort_direction == list_ns::SortDirection::Desc);
    }

    SECTION("[header] default sort (id DESC) stores article IDs as bounds") {
        auto userId = insertTestUser("author", "author@header.com", 0);
        auto id1 = insertTestArticle("tech", userId, "First", 10);
        auto id2 = insertTestArticle("tech", userId, "Second", 20);

        // Default query — no explicit sort, uses default (id DESC)
        auto q = makeL2ArticleQuery("tech");
        auto result = sync(L2DeclArticleListRepo::query(q));
        REQUIRE(result->size() == 2);

        auto redisKey = buildRedisPageKey<L2DeclArticleListRepo>(q.cache_key);
        auto raw = sync(jcailloux::relais::cache::RedisCache::getRawBinary(redisKey));
        REQUIRE(raw.has_value());

        auto header = list_ns::ListBoundsHeader::readFrom(raw->data(), raw->size());
        REQUIRE(header.has_value());

        // Default sort is id DESC → first_value = max(id), last_value = min(id)
        CHECK(header->bounds.first_value == std::max(id1, id2));
        CHECK(header->bounds.last_value == std::min(id1, id2));
        CHECK(header->sort_direction == list_ns::SortDirection::Desc);
        CHECK(header->is_first_page == true);
    }
}


// #############################################################################
//
//  11. Insertion invalidation edge cases (L2)
//
// #############################################################################

/// Build a L2 sorted query with explicit offset (offset-based pagination, no cursor).
static L2ArticleDescQuery makeL2ViewCountQueryOffset(
    std::string_view category, uint16_t limit, uint32_t offset)
{
    L2ArticleDescQuery q;
    q.limit = limit;
    q.offset = offset;
    q.filters.get<1>() = category;
    q.sort = jcailloux::relais::cache::list::SortSpec<size_t>{
        1, jcailloux::relais::cache::list::SortDirection::Desc};
    q.group_key = decl::groupCacheKey<L2ArticleDecl>(q);
    q.cache_key = decl::cacheKey<L2ArticleDecl>(q);
    return q;
}

TEST_CASE("[DeclList L2] Insertion invalidation edge cases",
          "[integration][db][redis][list][edge-invalidation]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[edge] insert when no list queries are cached") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // No query executed — no cache populated
        // Insert via repo should NOT error even with no groups/pages in Redis
        auto newArticle = makeTestArticle("tech", userId, "Tech 2", 20);
        auto created = sync(L2DeclArticleListRepo::insert(newArticle));
        CHECK(created != nullptr);

        // Query now sees both articles
        auto result = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(result->size() == 2);
    }

    SECTION("[edge] rapid sequential inserts each invalidate L2") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate L2 cache
        auto r1 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        REQUIRE(r1->size() == 1);

        // 3 rapid sequential inserts via repo
        sync(L2DeclArticleListRepo::insert(
            makeTestArticle("tech", userId, "Tech 2", 20)));
        sync(L2DeclArticleListRepo::insert(
            makeTestArticle("tech", userId, "Tech 3", 30)));
        sync(L2DeclArticleListRepo::insert(
            makeTestArticle("tech", userId, "Tech 4", 40)));

        // Each insert invalidated L2 → query hits DB → sees all 4
        auto result = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("tech")));
        CHECK(result->size() == 4);
    }

    SECTION("[edge] insert with sort value at exact page boundary") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A80", 80);
        insertTestArticle("tech", userId, "A60", 60);
        insertTestArticle("tech", userId, "A40", 40);

        // Page 1 [100, 80]: first_page=true, offset, complete (2==limit)
        auto q1 = makeL2ViewCountQuery("tech", 2);
        auto p1 = sync(L2DeclArticleListRepo::query(q1));
        REQUIRE(p1->size() == 2);
        REQUIRE(p1->items[0].view_count.value() == 100);

        // Page 2 [60, 40] via cursor: first_page=false, cursor, complete
        auto q2 = makeL2ViewCountQuery("tech", 2);
        q2.cursor = list_ns::Cursor::decode(std::string(p1->cursor())).value();
        q2.cache_key = decl::cacheKey<L2ArticleDecl>(q2);
        auto p2 = sync(L2DeclArticleListRepo::query(q2));
        REQUIRE(p2->size() == 2);

        // Insert sentinel in DB with view_count=80 (exact last_value of page 1)
        insertTestArticle("tech", userId, "Boundary80", 80);

        // notifyCreated with sort value = 80
        // Page 1 (first_page, offset): DESC → 80 >= 80? YES → INVALIDATED
        // Page 2 (cursor, [60,40], complete): 80 <= 60? NO → PRESERVED
        auto entity = makeTestArticle("tech", userId, "Boundary80", 80);
        auto deleted = TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);
        CHECK(deleted >= 1);

        // Page 2 PRESERVED: stale data (sentinel not visible)
        auto p2_cached = sync(L2DeclArticleListRepo::query(q2));
        CHECK(p2_cached->items[0].view_count.value() == 60);
        CHECK(p2_cached->items[1].view_count.value() == 40);
    }

    SECTION("[edge] L2 offset incomplete page always invalidated (contrast with cursor)") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A80", 80);
        insertTestArticle("tech", userId, "A60", 60);

        // -- Part A: Offset mode — incomplete page IS always invalidated --

        // Page 1 (offset=0, first, complete): [100, 80]
        auto q_off1 = makeL2ViewCountQueryOffset("tech", 2, 0);
        auto p_off1 = sync(L2DeclArticleListRepo::query(q_off1));
        REQUIRE(p_off1->size() == 2);

        // Page 2 (offset=2, NOT first, incomplete): [60]
        auto q_off2 = makeL2ViewCountQueryOffset("tech", 2, 2);
        auto p_off2 = sync(L2DeclArticleListRepo::query(q_off2));
        REQUIRE(p_off2->size() == 1);  // 1 < limit 2 → incomplete

        // Insert sentinel in DB
        insertTestArticle("tech", userId, "Sentinel1", 1);

        // notifyCreated with sort value = 1 (below all ranges)
        // Page 1 (offset, first, complete, [100,80]): DESC → 1 >= 80? NO → PRESERVED
        // Page 2 (offset, NOT first, incomplete): is_incomplete → return true → INVALIDATED
        auto entity1 = makeTestArticle("tech", userId, "E1", 1);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity1);

        // Page 1 PRESERVED: cache hit → stale
        auto p_off1_after = sync(L2DeclArticleListRepo::query(q_off1));
        CHECK(p_off1_after->size() == 2);

        // Page 2 INVALIDATED: DB hit → sees [60, 1] (sentinel visible)
        auto p_off2_after = sync(L2DeclArticleListRepo::query(q_off2));
        CHECK(p_off2_after->size() == 2);  // was 1, now 2 (sentinel visible)
    }

    SECTION("[edge] cursor incomplete page NOT always invalidated (contrast with offset)") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A80", 80);
        insertTestArticle("tech", userId, "A60", 60);
        insertTestArticle("tech", userId, "A40", 40);
        insertTestArticle("tech", userId, "A20", 20);

        // Page 1 [100, 80]: first, offset, complete
        auto q1 = makeL2ViewCountQuery("tech", 2);
        auto p1 = sync(L2DeclArticleListRepo::query(q1));
        REQUIRE(p1->size() == 2);

        // Page 2 [60, 40] via cursor: complete
        auto q2 = makeL2ViewCountQuery("tech", 2);
        q2.cursor = list_ns::Cursor::decode(std::string(p1->cursor())).value();
        q2.cache_key = decl::cacheKey<L2ArticleDecl>(q2);
        auto p2 = sync(L2DeclArticleListRepo::query(q2));
        REQUIRE(p2->size() == 2);

        // Page 3 [20] via cursor: incomplete (1 < limit 2)
        auto q3 = makeL2ViewCountQuery("tech", 2);
        q3.cursor = list_ns::Cursor::decode(std::string(p2->cursor())).value();
        q3.cache_key = decl::cacheKey<L2ArticleDecl>(q3);
        auto p3 = sync(L2DeclArticleListRepo::query(q3));
        REQUIRE(p3->size() == 1);

        // Insert sentinel in DB
        insertTestArticle("tech", userId, "Sentinel999", 999);

        // notifyCreated with sort value = 999 (above all ranges, DESC)
        // Page 1 (first, offset): DESC → 999 >= 80? YES → INVALIDATED
        // Page 2 (cursor, [60,40], complete): 999 <= 60? NO → PRESERVED
        // Page 3 (cursor, [20], incomplete): isValueInRange(999, false, true, true)
        //   → 999 <= 20? NO → PRESERVED (cursor mode does range check for incomplete)
        auto entity = makeTestArticle("tech", userId, "E999", 999);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // Page 2 PRESERVED: cache hit → stale
        auto p2_cached = sync(L2DeclArticleListRepo::query(q2));
        CHECK(p2_cached->size() == 2);
        CHECK(p2_cached->items[0].view_count.value() == 60);

        // Page 3 PRESERVED: cache hit → stale (cursor incomplete NOT always invalidated)
        auto p3_cached = sync(L2DeclArticleListRepo::query(q3));
        CHECK(p3_cached->size() == 1);
        CHECK(p3_cached->items[0].view_count.value() == 20);
    }

    SECTION("[edge] insert into empty cached list") {
        auto userId = insertTestUser("author", "author@edge.com", 0);

        // Query empty category → cache empty result
        auto r1 = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("empty_cat")));
        REQUIRE(r1->size() == 0);
        REQUIRE(r1->empty());

        // Insert sentinel directly in DB
        insertTestArticle("empty_cat", userId, "First", 10);

        // Cache still returns empty (stale from L2)
        auto r_stale = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("empty_cat")));
        CHECK(r_stale->size() == 0);

        // notifyCreated → page with is_valid=false → always invalidated
        auto entity = makeTestArticle("empty_cat", userId, "Notify", 20);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // L2 invalidated → DB hit → sentinel visible
        auto r_fresh = sync(L2DeclArticleListRepo::query(makeL2ArticleQuery("empty_cat")));
        CHECK(r_fresh->size() == 1);
    }

    SECTION("[edge] insert with nullopt sort value") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A50", 50);

        // Sorted query: first+incomplete (2 items, limit=10) → always invalidated
        auto q = makeL2ViewCountQuery("tech", 10);
        auto r1 = sync(L2DeclArticleListRepo::query(q));
        REQUIRE(r1->size() == 2);

        // Insert sentinel in DB
        insertTestArticle("tech", userId, "Sentinel", 30);

        // notifyCreated with nullopt view_count → sort_value = 0
        // Page is first+incomplete → always invalidated regardless
        auto entity = makeTestArticle("tech", userId, "NullSort", std::nullopt);
        TestInternals::notifyCreatedSync<L2DeclArticleListRepo>(entity);

        // L2 invalidated → DB hit → sentinel visible
        auto r_fresh = sync(L2DeclArticleListRepo::query(q));
        CHECK(r_fresh->size() == 3);
    }
}

// #############################################################################
//
//  queryJson / queryBinary — direct serialization from L2 list cache
//
// #############################################################################

TEST_CASE("[DeclList L2] queryJson",
          "[integration][db][redis][list][queryJson][rowview]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[queryJson] returns valid JSON on L2 miss (delegates to entity path)") {
        auto userId = insertTestUser("author", "author@qj.com", 0);
        insertTestArticle("tech", userId, "QJ Article 1", 10);
        insertTestArticle("tech", userId, "QJ Article 2", 20);

        auto q = makeL2ArticleQuery("tech");
        auto json = sync(L2DeclArticleListRepo::queryJson(q));

        REQUIRE(json != nullptr);
        REQUIRE(!json->empty());
        REQUIRE(json->find("QJ Article 1") != std::string::npos);
        REQUIRE(json->find("QJ Article 2") != std::string::npos);
    }

    SECTION("[queryJson] L2 hit transcodes BEVE to JSON") {
        auto userId = insertTestUser("author", "author@qj2.com", 0);
        insertTestArticle("tech", userId, "L2H Article 1", 10);
        insertTestArticle("tech", userId, "L2H Article 2", 20);

        auto q = makeL2ArticleQuery("tech");

        // First call: populate L2 via entity path (query() stores BEVE in Redis)
        auto wrapper = sync(L2DeclArticleListRepo::query(q));
        REQUIRE(wrapper->size() == 2);

        // Insert directly in DB (bypass repo) to detect stale cache
        insertTestArticle("tech", userId, "L2H Article 3", 30);

        // queryJson should hit L2 → BEVE→JSON transcode (still 2 articles)
        auto json = sync(L2DeclArticleListRepo::queryJson(q));
        REQUIRE(json != nullptr);
        REQUIRE(json->find("L2H Article 1") != std::string::npos);
        REQUIRE(json->find("L2H Article 2") != std::string::npos);
        // Article 3 NOT in cached result
        REQUIRE(json->find("L2H Article 3") == std::string::npos);
    }

    SECTION("[queryJson] returns nullptr for empty result") {
        auto q = makeL2ArticleQuery("nonexistent_qj");
        auto json = sync(L2DeclArticleListRepo::queryJson(q));

        // Empty list should still return valid JSON (empty array)
        REQUIRE(json != nullptr);
    }

    SECTION("[queryJson] matches query().json() byte-for-byte") {
        auto userId = insertTestUser("author", "author@qj3.com", 0);
        insertTestArticle("news", userId, "Byte Article", 42);

        auto q = makeL2ArticleQuery("news");

        // Entity path
        auto wrapper = sync(L2DeclArticleListRepo::query(q));
        REQUIRE(wrapper != nullptr);
        auto entityJson = wrapper->json();

        // Evict L2 and re-query via queryJson (entity path on miss)
        TestInternals::resetListCacheState<L2DeclArticleListRepo>();
        auto rowJson = sync(L2DeclArticleListRepo::queryJson(q));
        REQUIRE(rowJson != nullptr);

        // Both should produce the same JSON content
        REQUIRE(*rowJson == *entityJson);
    }
}

TEST_CASE("[DeclList L2] queryBinary",
          "[integration][db][redis][list][queryBinary][rowview]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    SECTION("[queryBinary] returns valid BEVE on L2 miss") {
        auto userId = insertTestUser("author", "author@qb.com", 0);
        insertTestArticle("tech", userId, "QB Article 1", 10);

        auto q = makeL2ArticleQuery("tech");
        auto beve = sync(L2DeclArticleListRepo::queryBinary(q));

        REQUIRE(beve != nullptr);
        REQUIRE(!beve->empty());
    }

    SECTION("[queryBinary] L2 hit returns raw binary (skips header)") {
        auto userId = insertTestUser("author", "author@qb2.com", 0);
        insertTestArticle("tech", userId, "BinH Article 1", 10);
        insertTestArticle("tech", userId, "BinH Article 2", 20);

        auto q = makeL2ArticleQuery("tech");

        // Populate L2
        sync(L2DeclArticleListRepo::query(q));

        // Insert directly (bypass repo)
        insertTestArticle("tech", userId, "BinH Article 3", 30);

        // queryBinary should hit L2 (still 2 articles from cache)
        auto beve = sync(L2DeclArticleListRepo::queryBinary(q));
        REQUIRE(beve != nullptr);
        REQUIRE(!beve->empty());

        // Verify content by transcoding to JSON
        std::string json;
        auto err = glz::beve_to_json(*beve, json);
        REQUIRE(!err);
        REQUIRE(json.find("BinH Article 1") != std::string::npos);
        REQUIRE(json.find("BinH Article 2") != std::string::npos);
        REQUIRE(json.find("BinH Article 3") == std::string::npos);
    }
}
