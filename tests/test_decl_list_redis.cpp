/**
 * test_decl_list_redis.cpp
 *
 * Tests for declarative list caching at L2 (Redis).
 * Mirrors test_decl_list_cache.cpp patterns but uses L2-only repos.
 *
 * Covers:
 *   1. Article list query (filters, limit, empty)
 *   2. Purchase list query (filters, combined)
 *   3. SortBounds invalidation precision at L2
 *   4. ModificationTracker cleanup at L2
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"
using namespace relais_test;

namespace decl = jcailloux::drogon::cache::list::decl;

// #############################################################################
//
//  Local L2 list repos
//
// #############################################################################

namespace relais_test {

using L2DeclArticleListRepo = Repository<TestArticleWrapper, "test:article:list:l2:decl", cfg::Redis>;
using L2DeclPurchaseListRepo = Repository<TestPurchaseWrapper, "test:purchase:list:l2:decl", cfg::Redis>;

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
    TestArticleModel model;
    model.setId(id);
    model.setCategory(category);
    model.setAuthorId(author_id);
    model.setTitle(title);
    model.setViewCount(view_count);
    model.setIsPublished(false);
    model.setPublishedAt(trantor::Date::now());
    model.setCreatedAt(trantor::Date::now());

    auto opt = TestArticleWrapper::fromModel(model);
    REQUIRE(opt.has_value());
    return std::make_shared<const TestArticleWrapper>(std::move(*opt));
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

    size_t h = std::hash<uint16_t>{}(limit) ^ 0xCAFE;
    if (q.filters.template get<0>()) h ^= std::hash<std::string_view>{}(*q.filters.template get<0>()) << 1;
    if (q.filters.template get<1>()) h ^= std::hash<int64_t>{}(*q.filters.template get<1>()) << 2;
    q.query_hash = h;
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

    size_t h = std::hash<uint16_t>{}(limit) ^ 0xFACE;
    if (q.filters.template get<0>()) h ^= std::hash<int64_t>{}(*q.filters.template get<0>()) << 1;
    if (q.filters.template get<1>()) h ^= std::hash<std::string_view>{}(*q.filters.template get<1>()) << 2;
    q.query_hash = h;
    return q;
}

/// Build a ListDescriptorQuery for articles sorted by view_count DESC (L2 variant).
static L2ArticleDescQuery makeL2ViewCountQuery(std::string_view category, uint16_t limit) {
    L2ArticleDescQuery q;
    q.limit = limit;
    q.filters.get<0>() = category;
    q.sort = jcailloux::drogon::cache::list::SortSpec<size_t>{1, jcailloux::drogon::cache::list::SortDirection::Desc};
    q.query_hash = std::hash<std::string_view>{}(category)
                 ^ (static_cast<size_t>(limit) * 0x9e3779b9)
                 ^ 0xFEED;
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
//  3. SortBounds invalidation at L2
//
// #############################################################################

TEST_CASE("[DeclList L2] SortBounds invalidation",
          "[integration][db][redis][list][invalidation]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    auto alice_id = insertTestUser("alice_l2", "alice_l2@test.com", 0);

    // 8 "tech" articles with view_count 10..80
    for (int vc = 10; vc <= 80; vc += 10) {
        insertTestArticle("tech", alice_id, "tech_" + std::to_string(vc), vc);
    }
    // 3 "news" articles with view_count 100..300
    for (int vc = 100; vc <= 300; vc += 100) {
        insertTestArticle("news", alice_id, "news_" + std::to_string(vc), vc);
    }

    SECTION("[sortbounds] create invalidates only affected range") {
        // Add 7 more tech articles (90..150) to get 15 total
        for (int vc = 90; vc <= 150; vc += 10) {
            insertTestArticle("tech", alice_id, "tech_high_" + std::to_string(vc), vc);
        }

        auto q1 = makeL2ViewCountQuery("tech", 10);  // bounds(150, 60)
        auto q2 = makeL2ViewCountQuery("tech", 25);  // bounds(150, 10)
        auto q3 = makeL2ViewCountQuery("news", 10);  // filter mismatch

        auto r1 = sync(L2DeclArticleListRepo::query(q1));
        auto r2 = sync(L2DeclArticleListRepo::query(q2));
        auto r3 = sync(L2DeclArticleListRepo::query(q3));

        REQUIRE(r1->size() == 10);
        REQUIRE(r2->size() == 15);
        REQUIRE(r3->size() == 3);

        // Insert tech article with view_count=45
        insertTestArticle("tech", alice_id, "tech_new_45", 45);
        auto trigger_entity = makeArticle(999, "tech", alice_id, "tech_trigger_45", 45);
        L2DeclArticleListRepo::notifyCreated(trigger_entity);

        // q1: 45 < 60 → PRESERVED
        auto r1_after = sync(L2DeclArticleListRepo::query(q1));
        CHECK(r1_after->size() == 10);

        // q2: 45 >= 10 → INVALIDATED
        auto r2_after = sync(L2DeclArticleListRepo::query(q2));
        CHECK(r2_after->size() == 16);

        // q3: filter mismatch → PRESERVED
        auto r3_after = sync(L2DeclArticleListRepo::query(q3));
        CHECK(r3_after->size() == 3);
    }

    SECTION("[sortbounds] update invalidates ranges containing old OR new value") {
        for (int vc = 90; vc <= 150; vc += 10) {
            insertTestArticle("tech", alice_id, "tech_high_" + std::to_string(vc), vc);
        }

        auto q1 = makeL2ViewCountQuery("tech", 10);  // bounds(150, 60)
        auto r1 = sync(L2DeclArticleListRepo::query(q1));
        REQUIRE(r1->size() == 10);

        auto result_70 = getDb()->execSqlSync(
            "SELECT id FROM relais_test_articles WHERE view_count = 70 AND author_id = $1 LIMIT 1",
            alice_id);
        REQUIRE(result_70.size() > 0);
        auto article_70_id = result_70[0]["id"].as<int64_t>();

        auto old_entity = makeArticle(article_70_id, "tech", alice_id, "tech_70", 70);
        updateTestArticle(article_70_id, "tech_70_updated", 25);
        auto new_entity = makeArticle(article_70_id, "tech", alice_id, "tech_70_updated", 25);

        L2DeclArticleListRepo::notifyUpdated(old_entity, new_entity);

        auto r1_after = sync(L2DeclArticleListRepo::query(q1));
        CHECK(r1_after->size() == 10);
    }

    SECTION("[sortbounds] delete invalidates affected range") {
        auto q1 = makeL2ViewCountQuery("tech", 10);
        auto r1 = sync(L2DeclArticleListRepo::query(q1));
        REQUIRE(r1->size() == 8);

        auto result_40 = getDb()->execSqlSync(
            "SELECT id FROM relais_test_articles WHERE view_count = 40 AND author_id = $1 LIMIT 1",
            alice_id);
        REQUIRE(result_40.size() > 0);
        auto article_40_id = result_40[0]["id"].as<int64_t>();

        auto deleted_entity = makeArticle(article_40_id, "tech", alice_id, "tech_40", 40);
        deleteTestArticle(article_40_id);

        L2DeclArticleListRepo::notifyDeleted(deleted_entity);

        auto r1_after = sync(L2DeclArticleListRepo::query(q1));
        CHECK(r1_after->size() == 7);
    }

    SECTION("[sortbounds] filter mismatch preserves cache across categories") {
        auto q_tech = makeL2ViewCountQuery("tech", 10);
        auto q_news = makeL2ViewCountQuery("news", 10);

        auto r_tech = sync(L2DeclArticleListRepo::query(q_tech));
        auto r_news = sync(L2DeclArticleListRepo::query(q_news));
        REQUIRE(r_tech->size() == 8);
        REQUIRE(r_news->size() == 3);

        auto new_tech_id = insertTestArticle("tech", alice_id, "tech_new", 55);
        auto tech_entity = makeArticle(new_tech_id, "tech", alice_id, "tech_new", 55);
        L2DeclArticleListRepo::notifyCreated(tech_entity);

        auto r_tech_after = sync(L2DeclArticleListRepo::query(q_tech));
        CHECK(r_tech_after->size() == 9);

        auto r_news_after = sync(L2DeclArticleListRepo::query(q_news));
        CHECK(r_news_after->size() == 3);
    }
}


// #############################################################################
//
//  4. ModificationTracker cleanup at L2
//
// #############################################################################

TEST_CASE("[DeclList L2] ModificationTracker cleanup",
          "[integration][db][redis][list][cleanup]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<L2DeclArticleListRepo>();

    auto alice_id = insertTestUser("alice_l2_cleanup", "alice_l2_cleanup@test.com", 0);
    for (int vc = 10; vc <= 50; vc += 10) {
        insertTestArticle("tech", alice_id, "cleanup_" + std::to_string(vc), vc);
    }

    SECTION("[tracker-cleanup] old modifications removed after enough cycles") {
        constexpr auto N = TestInternals::listCacheShardCount<L2DeclArticleListRepo>();
        auto entity1 = makeArticle(9001, "tech", alice_id, "cleanup_new", 35);
        L2DeclArticleListRepo::notifyCreated(entity1);
        CHECK(TestInternals::pendingModificationCount<L2DeclArticleListRepo>() == 1);

        for (size_t i = 0; i < N; ++i) {
            TestInternals::forceModificationTrackerCleanup<L2DeclArticleListRepo>();
        }

        CHECK(TestInternals::pendingModificationCount<L2DeclArticleListRepo>() == 0);
    }

    SECTION("[tracker-cleanup] recent modifications survive cleanup") {
        constexpr auto N = TestInternals::listCacheShardCount<L2DeclArticleListRepo>();
        auto entity1 = makeArticle(9001, "tech", alice_id, "cleanup_a", 15);
        L2DeclArticleListRepo::notifyCreated(entity1);

        TestInternals::forceModificationTrackerCleanup<L2DeclArticleListRepo>();
        CHECK(TestInternals::pendingModificationCount<L2DeclArticleListRepo>() == 1);

        auto entity2 = makeArticle(9002, "tech", alice_id, "cleanup_b", 25);
        L2DeclArticleListRepo::notifyCreated(entity2);
        CHECK(TestInternals::pendingModificationCount<L2DeclArticleListRepo>() == 2);

        for (size_t i = 0; i < N - 1; ++i) {
            TestInternals::forceModificationTrackerCleanup<L2DeclArticleListRepo>();
        }

        CHECK(TestInternals::pendingModificationCount<L2DeclArticleListRepo>() == 1);

        TestInternals::forceModificationTrackerCleanup<L2DeclArticleListRepo>();
        CHECK(TestInternals::pendingModificationCount<L2DeclArticleListRepo>() == 0);
    }
}
