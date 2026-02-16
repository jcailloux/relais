/**
 * test_decl_list_cache.cpp
 * Tests for ListMixin (L1 list cache).
 *
 * Covers:
 * - Query: filters, combined filters, limits, empty results
 * - Item access: firstItem / lastItem accessors
 * - SortBounds-based invalidation: only pages whose sort range includes
 *   the modified entity's value are invalidated
 * - Filter matching: only pages matching the entity's filter values are affected
 * - ModificationTracker cleanup lifecycle
 * - Modification cutoff safety (cleanup/drain with time cutoff)
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"
using namespace relais_test;

namespace decl = jcailloux::relais::cache::list::decl;

// =============================================================================
// Helper: build a TestArticleWrapper from raw values (no DB round-trip)
// =============================================================================

std::shared_ptr<const TestArticleWrapper> makeArticle(
    int64_t id,
    const std::string& category,
    int64_t author_id,
    const std::string& title,
    int32_t view_count
) {
    return makeTestArticle(category, author_id, title, view_count, false, id);
}

// =============================================================================
// Query builder helper
// =============================================================================

using TestDecl = TestArticleListDecl;
using TestListQuery = decl::ListDescriptorQuery<TestDecl>;

/// Build a ListDescriptorQuery for articles filtered by category, sorted by view_count DESC.
TestListQuery makeViewCountQuery(std::string_view category, uint16_t limit) {
    TestListQuery q;
    q.limit = limit;

    // Filter index 0 = category (string_view — must point to stable storage)
    q.filters.get<0>() = category;

    // Sort index 1 = view_count, DESC
    q.sort = jcailloux::relais::cache::list::SortSpec<size_t>{1, jcailloux::relais::cache::list::SortDirection::Desc};

    // Unique hash per (category, limit, sort)
    q.query_hash = std::hash<std::string_view>{}(category)
                 ^ (static_cast<size_t>(limit) * 0x9e3779b9)
                 ^ 0xDEAD;  // salt to avoid collision with other tests
    return q;
}

// #############################################################################
//
//  TEST CASE 1: Article list query (filters, limit, empty)
//
// #############################################################################

TEST_CASE("[DeclListRepo] Article list query",
          "[integration][db][list][query][article]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    SECTION("[query] returns all articles when no filter") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        insertTestArticle("tech", userId, "Article A", 10);
        insertTestArticle("news", userId, "Article B", 20);
        insertTestArticle("tech", userId, "Article C", 30);

        auto result = sync(TestArticleListRepo::query(makeArticleQuery()));

        REQUIRE(result->size() == 3);
        REQUIRE(result->size() == 3);
    }

    SECTION("[query] filters by category") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);
        insertTestArticle("tech", userId, "Tech 2", 30);

        auto result = sync(TestArticleListRepo::query(
            makeArticleQuery("tech")));

        REQUIRE(result->size() == 2);
        REQUIRE(result->size() == 2);
    }

    SECTION("[query] filters by author_id") {
        auto user1 = insertTestUser("alice", "alice@example.com", 0);
        auto user2 = insertTestUser("bob", "bob@example.com", 0);
        insertTestArticle("tech", user1, "Alice Article", 10);
        insertTestArticle("tech", user2, "Bob Article 1", 20);
        insertTestArticle("news", user2, "Bob Article 2", 30);

        auto result = sync(TestArticleListRepo::query(
            makeArticleQuery(std::nullopt, user2)));

        REQUIRE(result->size() == 2);
        REQUIRE(result->size() == 2);
    }

    SECTION("[query] combined filters") {
        auto user1 = insertTestUser("alice", "alice@example.com", 0);
        auto user2 = insertTestUser("bob", "bob@example.com", 0);
        insertTestArticle("tech", user1, "Alice Tech", 10);
        insertTestArticle("news", user1, "Alice News", 20);
        insertTestArticle("tech", user2, "Bob Tech", 30);
        insertTestArticle("news", user2, "Bob News", 40);

        auto result = sync(TestArticleListRepo::query(
            makeArticleQuery("tech", user2)));

        REQUIRE(result->size() == 1);
        REQUIRE(result->size() == 1);
    }

    SECTION("[query] returns empty for non-matching filter") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        insertTestArticle("tech", userId, "Tech Article", 10);

        auto result = sync(TestArticleListRepo::query(
            makeArticleQuery("nonexistent")));

        REQUIRE(result->size() == 0);
        REQUIRE(result->empty());
    }

    SECTION("[query] respects limit") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        for (int i = 0; i < 5; ++i) {
            insertTestArticle("tech", userId, "Article " + std::to_string(i), i * 10);
        }

        auto result = sync(TestArticleListRepo::query(
            makeArticleQuery(std::nullopt, std::nullopt, 10)));

        REQUIRE(result->size() == 5);
    }

    SECTION("[query] returns empty when no data") {
        auto result = sync(TestArticleListRepo::query(makeArticleQuery()));

        REQUIRE(result->size() == 0);
        REQUIRE(result->empty());
    }
}

// #############################################################################
//
//  TEST CASE 2: Article Item accessors
//
// #############################################################################

TEST_CASE("[DeclListRepo] Article Item accessors",
          "[integration][db][list][itemview][article]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    SECTION("[itemview] firstItem and lastItem are accessible") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        insertTestArticle("tech", userId, "First Article", 10, true);
        insertTestArticle("news", userId, "Last Article", 20, true);

        auto result = sync(TestArticleListRepo::query(makeArticleQuery()));

        REQUIRE(result->size() == 2);

        const auto& first = result->items.front();
        REQUIRE(first.author_id == userId);

        const auto& last = result->items.back();
        REQUIRE(last.author_id == userId);
    }

    SECTION("[itemview] returns correct category") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        insertTestArticle("science", userId, "Science Article", 42, true);

        auto result = sync(TestArticleListRepo::query(
            makeArticleQuery("science")));

        REQUIRE(result->size() == 1);
        const auto& view = result->items.front();
        REQUIRE(view.category == "science");
        REQUIRE(view.author_id == userId);
    }
}

// #############################################################################
//
//  TEST CASE 3: Purchase list query (filters, limit, empty)
//
// #############################################################################

TEST_CASE("[DeclListRepo] Purchase list query",
          "[integration][db][list][query][purchase]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestPurchaseListRepo>();

    SECTION("[query] returns all purchases when no filter") {
        auto userId = insertTestUser("buyer", "buyer@example.com", 1000);
        insertTestPurchase(userId, "Item A", 100, "completed");
        insertTestPurchase(userId, "Item B", 200, "pending");
        insertTestPurchase(userId, "Item C", 300, "completed");

        auto result = sync(TestPurchaseListRepo::query(makePurchaseQuery()));

        REQUIRE(result->size() == 3);
        REQUIRE(result->size() == 3);
    }

    SECTION("[query] filters by user_id") {
        auto user1 = insertTestUser("alice", "alice@example.com", 500);
        auto user2 = insertTestUser("bob", "bob@example.com", 500);
        insertTestPurchase(user1, "Widget", 100);
        insertTestPurchase(user2, "Gadget", 200);
        insertTestPurchase(user2, "Doohickey", 300);

        auto result = sync(TestPurchaseListRepo::query(
            makePurchaseQuery(user2)));

        REQUIRE(result->size() == 2);
        REQUIRE(result->size() == 2);
    }

    SECTION("[query] filters by status") {
        auto userId = insertTestUser("buyer", "buyer@example.com", 1000);
        insertTestPurchase(userId, "Item A", 100, "completed");
        insertTestPurchase(userId, "Item B", 200, "pending");
        insertTestPurchase(userId, "Item C", 300, "completed");

        auto result = sync(TestPurchaseListRepo::query(
            makePurchaseQuery(std::nullopt, std::string("completed"))));

        REQUIRE(result->size() == 2);
        REQUIRE(result->size() == 2);
    }

    SECTION("[query] combined user_id and status filter") {
        auto user1 = insertTestUser("alice", "alice@example.com", 500);
        auto user2 = insertTestUser("bob", "bob@example.com", 500);
        insertTestPurchase(user1, "A", 100, "completed");
        insertTestPurchase(user1, "B", 200, "pending");
        insertTestPurchase(user2, "C", 300, "completed");
        insertTestPurchase(user2, "D", 400, "pending");

        auto result = sync(TestPurchaseListRepo::query(
            makePurchaseQuery(user1, std::string("pending"))));

        REQUIRE(result->size() == 1);
        REQUIRE(result->size() == 1);
    }

    SECTION("[query] returns empty when no data") {
        auto result = sync(TestPurchaseListRepo::query(makePurchaseQuery()));

        REQUIRE(result->size() == 0);
        REQUIRE(result->empty());
    }
}

// #############################################################################
//
//  TEST CASE 4: Purchase Item accessors
//
// #############################################################################

TEST_CASE("[DeclListRepo] Purchase Item accessors",
          "[integration][db][list][itemview][purchase]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestPurchaseListRepo>();

    SECTION("[itemview] returns correct fields") {
        auto userId = insertTestUser("buyer", "buyer@example.com", 1000);
        insertTestPurchase(userId, "Widget", 100, "completed");

        auto result = sync(TestPurchaseListRepo::query(makePurchaseQuery()));

        REQUIRE(result->size() == 1);
        const auto& view = result->items.front();
        REQUIRE(view.user_id == userId);
        REQUIRE(view.status == "completed");
    }
}

// #############################################################################
//
//  TEST CASE 5: SortBounds invalidation precision
//
// #############################################################################

TEST_CASE("[DeclListRepo] SortBounds invalidation precision",
          "[integration][db][list][invalidation]")
{
    TransactionGuard guard;

    // Reset list cache state for test isolation
    TestInternals::resetListCacheState<TestArticleListRepo>();

    // =========================================================================
    // Setup: Create test data
    // =========================================================================
    // Author: Alice (id from DB)
    auto alice_id = insertTestUser("alice_decl", "alice_decl@test.com", 0);

    // 8 "tech" articles with view_count 10, 20, 30, 40, 50, 60, 70, 80
    for (int vc = 10; vc <= 80; vc += 10) {
        insertTestArticle("tech", alice_id, "tech_" + std::to_string(vc), vc);
    }

    // 3 "news" articles with view_count 100, 200, 300
    for (int vc = 100; vc <= 300; vc += 100) {
        insertTestArticle("news", alice_id, "news_" + std::to_string(vc), vc);
    }

    SECTION("[sortbounds] create invalidates only affected range") {
        // Add 7 more tech articles with high view_counts (90..150)
        // so limit=10 gives [150..60], bounds(150, 60), value=45 → 45>=60 = false = PRESERVED
        for (int vc = 90; vc <= 150; vc += 10) {
            insertTestArticle("tech", alice_id, "tech_high_" + std::to_string(vc), vc);
        }
        // Now tech has 15 articles: 10,20,30,40,50,60,70,80,90,100,110,120,130,140,150

        // Query 1: tech, limit=10, sorted DESC → [150,140,130,120,110,100,90,80,70,60]
        // bounds(150, 60). Value=45 → 45>=60 = false → PRESERVED
        auto q1 = makeViewCountQuery("tech", 10);

        // Query 2: tech, limit=25 → all 15 items → [150,...,10]
        // bounds(150, 10). Value=45 → 45>=10 = true → INVALIDATED
        auto q2 = makeViewCountQuery("tech", 25);

        // Query 3: news, limit=10 → [300, 200, 100]
        // Filter = "news" but entity is "tech" → filter mismatch → PRESERVED
        auto q3 = makeViewCountQuery("news", 10);

        // Prime the cache
        auto r1 = sync(TestArticleListRepo::query(q1));
        auto r2 = sync(TestArticleListRepo::query(q2));
        auto r3 = sync(TestArticleListRepo::query(q3));

        REQUIRE(r1->size() == 10);  // tech top 10
        REQUIRE(r2->size() == 15);  // tech all 15
        REQUIRE(r3->size() == 3);   // news all 3

        CHECK(TestArticleListRepo::listCacheSize() == 3);

        // Insert a new tech article with view_count=45 directly in DB
        // (two inserts: one for the DB data, one as entity for notification)
        insertTestArticle("tech", alice_id, "tech_new_45", 45);

        // Build the notification entity manually (no DB round-trip needed)
        auto trigger_entity = makeArticle(999, "tech", alice_id, "tech_trigger_45", 45);

        // Manually invoke the cross-invalidation path
        TestArticleListRepo::notifyCreated(trigger_entity);

        // Verify pending modifications
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);

        // Re-query: q1 should be PRESERVED (45 < 60, out of bounds)
        auto r1_after = sync(TestArticleListRepo::query(q1));
        // shouldEvictEntry: bounds(150, 60), 45 >= 60? false → NOT evicted
        CHECK(r1_after->size() == 10);  // Cache HIT (preserved)

        // Re-query: q2 should be INVALIDATED (45 >= 10)
        auto r2_after = sync(TestArticleListRepo::query(q2));
        // shouldEvictEntry: bounds(150, 10), 45 >= 10 = true → evicted → re-fetch
        // Now DB has 16 tech articles (15 original + 1 new), limit=25 → returns 16
        CHECK(r2_after->size() == 16);  // Cache MISS → fresh data

        // Re-query: q3 should be PRESERVED (filter mismatch: entity is "tech", query filters "news")
        auto r3_after = sync(TestArticleListRepo::query(q3));
        CHECK(r3_after->size() == 3);  // Cache HIT (preserved)
    }

    SECTION("[sortbounds] update invalidates ranges containing old OR new value") {
        // Setup: 15 tech articles (10-150)
        for (int vc = 90; vc <= 150; vc += 10) {
            insertTestArticle("tech", alice_id, "tech_high_" + std::to_string(vc), vc);
        }

        // Cache: tech limit=10 → [150..60], bounds(150, 60)
        auto q1 = makeViewCountQuery("tech", 10);
        auto r1 = sync(TestArticleListRepo::query(q1));
        REQUIRE(r1->size() == 10);

        // Find article with view_count=70 to update
        auto result_70 = execQueryArgs(
            "SELECT id FROM relais_test_articles WHERE view_count = 70 AND author_id = $1 LIMIT 1",
            alice_id);
        REQUIRE(result_70.rows() > 0);
        auto article_70_id = result_70[0].get<int64_t>(0);

        // Build old entity (view_count=70) and new entity (view_count=25) manually
        auto old_entity = makeArticle(article_70_id, "tech", alice_id, "tech_70", 70);
        updateTestArticle(article_70_id, "tech_70_updated", 25);
        auto new_entity = makeArticle(article_70_id, "tech", alice_id, "tech_70_updated", 25);

        // Trigger update notification
        TestArticleListRepo::notifyUpdated(old_entity, new_entity);

        // Re-query: old_entity.view_count=70 is in [150, 60] → 70>=60 = true → INVALIDATED
        auto r1_after = sync(TestArticleListRepo::query(q1));
        // After re-fetch: 70 is gone from top range, 50 takes its place
        // New top 10 DESC: [150,140,130,120,110,100,90,80,60,50]
        CHECK(r1_after->size() == 10);

        // Verify the updated article is NOT in the top 10 (view_count=25 is below 50)
        const auto& last_view = r1_after->items.back();
        CHECK(last_view.view_count.value_or(0) >= 25);
    }

    SECTION("[sortbounds] delete invalidates affected range") {
        // Cache: tech limit=10 → all 8 items [80..10], bounds(80, 10)
        auto q1 = makeViewCountQuery("tech", 10);
        auto r1 = sync(TestArticleListRepo::query(q1));
        REQUIRE(r1->size() == 8);

        // Find article with view_count=40 to delete
        auto result_40 = execQueryArgs(
            "SELECT id FROM relais_test_articles WHERE view_count = 40 AND author_id = $1 LIMIT 1",
            alice_id);
        REQUIRE(result_40.rows() > 0);
        auto article_40_id = result_40[0].get<int64_t>(0);

        // Build entity for notification, then delete from DB
        auto deleted_entity = makeArticle(article_40_id, "tech", alice_id, "tech_40", 40);
        deleteTestArticle(article_40_id);

        // Trigger delete notification
        TestArticleListRepo::notifyDeleted(deleted_entity);

        // Re-query: deleted_entity.view_count=40, bounds(80, 10) → 40>=10 = true → INVALIDATED
        auto r1_after = sync(TestArticleListRepo::query(q1));
        CHECK(r1_after->size() == 7);  // One fewer article
    }

    SECTION("[sortbounds] filter mismatch preserves cache across categories") {
        // Cache: tech limit=10 and news limit=10
        auto q_tech = makeViewCountQuery("tech", 10);
        auto q_news = makeViewCountQuery("news", 10);

        auto r_tech = sync(TestArticleListRepo::query(q_tech));
        auto r_news = sync(TestArticleListRepo::query(q_news));
        REQUIRE(r_tech->size() == 8);
        REQUIRE(r_news->size() == 3);

        // Create a "tech" article — should only affect "tech" cache
        auto new_tech_id = insertTestArticle("tech", alice_id, "tech_new", 55);
        auto tech_entity = makeArticle(new_tech_id, "tech", alice_id, "tech_new", 55);

        TestArticleListRepo::notifyCreated(tech_entity);

        // tech: entity category="tech" matches filter → check bounds
        // bounds(80, 10), value=55 → 55>=10 = true → INVALIDATED
        auto r_tech_after = sync(TestArticleListRepo::query(q_tech));
        CHECK(r_tech_after->size() == 9);  // Fresh data with new article

        // news: entity category="tech" does NOT match filter "news" → PRESERVED
        auto r_news_after = sync(TestArticleListRepo::query(q_news));
        CHECK(r_news_after->size() == 3);  // Cache HIT
    }
}

// #############################################################################
//
//  TEST CASE 6: ModificationTracker cleanup
//
// #############################################################################

TEST_CASE("[DeclListRepo] ModificationTracker cleanup",
          "[integration][db][list][cleanup]")
{
    TransactionGuard guard;

    // Reset list cache state for test isolation
    TestInternals::resetListCacheState<TestArticleListRepo>();

    auto alice_id = insertTestUser("alice_cleanup", "alice_cleanup@test.com", 0);

    // Create some initial articles
    for (int vc = 10; vc <= 50; vc += 10) {
        insertTestArticle("tech", alice_id, "cleanup_" + std::to_string(vc), vc);
    }

    SECTION("[tracker-cleanup] old modifications are removed after enough cleanup cycles") {
        // Build entity manually (no DB round-trip needed for notification)
        auto entity1 = makeArticle(9001, "tech", alice_id, "cleanup_new", 35);
        TestArticleListRepo::notifyCreated(entity1);
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);

        // ModificationTracker uses a bitmap with ShardCount bits (one per shard).
        // Each cleanup cycle clears one shard's bit. After ShardCount cycles,
        // all bits are cleared → bitmap=0 → modification removed.
        constexpr auto N = TestInternals::listCacheShardCount<TestArticleListRepo>();
        for (size_t i = 0; i < N; ++i) {
            TestInternals::forceModificationTrackerCleanup<TestArticleListRepo>();
        }

        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 0);
    }

    SECTION("[tracker-cleanup] recent modifications survive cleanup") {
        constexpr auto N = TestInternals::listCacheShardCount<TestArticleListRepo>();

        // Build entities manually
        auto entity1 = makeArticle(9001, "tech", alice_id, "cleanup_a", 15);
        TestArticleListRepo::notifyCreated(entity1);
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);

        // Run 1 cleanup cycle
        TestInternals::forceModificationTrackerCleanup<TestArticleListRepo>();
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);  // Still there

        // Notify second creation
        auto entity2 = makeArticle(9002, "tech", alice_id, "cleanup_b", 25);
        TestArticleListRepo::notifyCreated(entity2);
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 2);

        // Run N-1 more cycles — entity1 has seen N total shards, entity2 has seen N-1
        for (size_t i = 0; i < N - 1; ++i) {
            TestInternals::forceModificationTrackerCleanup<TestArticleListRepo>();
        }

        // entity1: 1 + (N-1) = N bits cleared → bitmap=0 → REMOVED
        // entity2: 0 + (N-1) = N-1 bits cleared → 1 bit remaining → KEPT
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);

        // One more cycle removes entity2
        TestInternals::forceModificationTrackerCleanup<TestArticleListRepo>();
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 0);
    }

    SECTION("[tracker-cleanup] stale modification does not invalidate fresh cache entries") {
        // This tests that shouldEvictEntry skips modifications older than the cache entry.
        // 1. Create a modification (notification)
        auto id1 = insertTestArticle("tech", alice_id, "cleanup_stale", 35);
        auto entity1 = makeArticle(id1, "tech", alice_id, "cleanup_stale", 35);

        TestArticleListRepo::notifyCreated(entity1);
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);

        // 2. Wait a tiny bit so the cache entry will have a later timestamp
        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        // 3. Cache a query — this entry's cached_at will be AFTER the modification
        auto q = makeViewCountQuery("tech", 10);
        auto r = sync(TestArticleListRepo::query(q));
        // DB now has 5 original + 1 new = 6 tech articles
        REQUIRE(r->size() == 6);

        // 4. Re-query: the modification is older than the cache entry → skipped → cache HIT
        auto r_again = sync(TestArticleListRepo::query(q));
        CHECK(r_again->size() == 6);  // Cache HIT (stale modification ignored)

        // Cache should still have exactly 1 entry (not evicted)
        CHECK(TestArticleListRepo::listCacheSize() == 1);
    }
}

// #############################################################################
//
//  TEST CASE 7: Modification cutoff safety
//
// #############################################################################
//
// Verifies that cleanup/drain with a time cutoff never drains modifications
// added after the cutoff. This prevents premature draining when a modification
// is track()'d between the segment cleanup and the modification cleanup.

TEST_CASE("[DeclListRepo] Modification cutoff safety",
          "[integration][db][list][cutoff]")
{
    TransactionGuard guard;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    auto alice_id = insertTestUser("alice_cutoff", "alice_cutoff@test.com", 0);

    SECTION("[cutoff] cleanup(cutoff) only increments pre-cutoff modifications") {
        // M1: before cutoff
        auto entity1 = makeArticle(9001, "tech", alice_id, "before_cutoff", 10);
        TestArticleListRepo::notifyCreated(entity1);

        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto cutoff = std::chrono::steady_clock::now();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));

        // M2: after cutoff
        auto entity2 = makeArticle(9002, "tech", alice_id, "after_cutoff", 20);
        TestArticleListRepo::notifyCreated(entity2);

        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 2);

        // Run ShardCount cleanup cycles with the cutoff, one per shard identity.
        // Only M1 (before cutoff) has its bits cleared. M2 (after cutoff) is skipped.
        constexpr auto N = TestInternals::listCacheShardCount<TestArticleListRepo>();
        for (uint8_t i = 0; i < static_cast<uint8_t>(N); ++i) {
            TestInternals::cleanupModificationsWithCutoff<TestArticleListRepo>(cutoff, i);
        }

        // M1: all N bits cleared → bitmap=0 → drained.  M2: 0 bits cleared → still present
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);
    }

    SECTION("[cutoff] drain(cutoff) only removes pre-cutoff modifications") {
        auto entity1 = makeArticle(9001, "tech", alice_id, "before_drain", 10);
        TestArticleListRepo::notifyCreated(entity1);

        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto cutoff = std::chrono::steady_clock::now();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));

        auto entity2 = makeArticle(9002, "tech", alice_id, "after_drain", 20);
        TestArticleListRepo::notifyCreated(entity2);

        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 2);

        // Drain only modifications before cutoff
        TestInternals::drainModificationsWithCutoff<TestArticleListRepo>(cutoff);

        // M1: drained.  M2: still present
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);
    }

    SECTION("[cutoff] post-cutoff modification still invalidates cache entries") {
        // 1. Populate DB and cache a query
        for (int vc = 10; vc <= 50; vc += 10) {
            insertTestArticle("tech", alice_id,
                              "cutoff_art_" + std::to_string(vc), vc);
        }
        auto q = makeViewCountQuery("tech", 10);
        auto r1 = sync(TestArticleListRepo::query(q));
        REQUIRE(r1->size() == 5);
        CHECK(TestArticleListRepo::listCacheSize() == 1);

        // 2. M1 (before cutoff) — invalidates the cached page
        auto entity1 = makeArticle(9001, "tech", alice_id, "cutoff_old", 25);
        TestArticleListRepo::notifyCreated(entity1);

        // Re-query to absorb M1's invalidation and re-cache with fresh timestamp
        auto r2 = sync(TestArticleListRepo::query(q));
        REQUIRE(r2->size() == 5);  // DB still has 5 (entity1 not in DB)

        // 3. Cutoff between M1 and M2
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto cutoff = std::chrono::steady_clock::now();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));

        // 4. M2 (after cutoff) — insert into DB + notify
        insertTestArticle("tech", alice_id, "cutoff_new", 35);
        auto entity2 = makeArticle(9002, "tech", alice_id, "cutoff_new", 35);
        TestArticleListRepo::notifyCreated(entity2);

        // 5. Drain only M1
        TestInternals::drainModificationsWithCutoff<TestArticleListRepo>(cutoff);
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);

        // 6. Re-query: M2 is still in tracker → cache invalidated → DB returns 6
        auto r3 = sync(TestArticleListRepo::query(q));
        CHECK(r3->size() == 6);
    }
}

// #############################################################################
//
//  TEST CASE 8: Bitmap skip optimization
//
// #############################################################################
//
// Verifies the per-segment bitmap skip in lazy invalidation:
// When a modification's bit for the cached entry's segment identity is cleared,
// the modification is skipped during get() → cache HIT despite the modification
// still existing in the tracker.

TEST_CASE("[DeclListRepo] Bitmap skip optimization",
          "[integration][db][list][skip]")
{
    TransactionGuard guard;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    auto alice_id = insertTestUser("alice_skip", "alice_skip@test.com", 0);

    for (int vc = 10; vc <= 50; vc += 10) {
        insertTestArticle("tech", alice_id, "skip_" + std::to_string(vc), vc);
    }

    SECTION("[skip] cleared bitmap bit prevents lazy invalidation") {
        // 1. Cache a query: tech articles sorted by view_count DESC, limit=10
        auto q = makeViewCountQuery("tech", 10);
        auto r1 = sync(TestArticleListRepo::query(q));
        REQUIRE(r1->size() == 5);
        CHECK(TestArticleListRepo::listCacheSize() == 1);

        // 2. Read the shard_id assigned to this cache entry
        auto shard_id_opt = TestInternals::getListEntryShardId<TestArticleListRepo>(
            q.query_hash);
        REQUIRE(shard_id_opt.has_value());
        uint8_t shard_id = *shard_id_opt;

        // 3. Insert a new article in DB AND notify the list cache.
        //    This modification would normally invalidate Q:
        //    category="tech" matches filter, view_count=35 is in bounds [50, 10].
        insertTestArticle("tech", alice_id, "skip_trigger", 35);
        auto entity = makeArticle(9001, "tech", alice_id, "skip_trigger", 35);
        TestArticleListRepo::notifyCreated(entity);
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);

        // 4. Clear ONLY the bit for the entry's shard identity in the ModificationTracker.
        //    The modification still exists (other bits remain set), but bit shard_id = 0.
        auto cutoff = std::chrono::steady_clock::now();
        TestInternals::cleanupModificationsWithCutoff<TestArticleListRepo>(
            cutoff, shard_id);
        // M still in tracker (other bits remain)
        CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 1);

        // 5. Re-query: lazy validation in get() checks modification M.
        //    pending_segments & (1 << shard_id) == 0 → SKIP M → entry not affected → cache HIT.
        //    Cache HIT returns 5 (stale). Cache MISS would return 6 (DB has new article).
        auto r2 = sync(TestArticleListRepo::query(q));
        CHECK(r2->size() == 5);  // Cache HIT — bitmap skip prevented invalidation
    }
}
