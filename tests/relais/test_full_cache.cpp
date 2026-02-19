/**
 * test_full_cache.cpp
 *
 * Tests for L1+L2 (Both) cache hierarchy.
 * Verifies the interaction between RAM (L1) and Redis (L2) cache layers.
 *
 * Covers:
 *   1. L1 to L2 promotion (cache miss → L2 populate → L1 populate)
 *   2. Cascade invalidation (invalidate both layers, evict only)
 *   3. L1 expiration with L2 fallback
 *   4. Write-through at L1+L2
 *   5. Binary entity at L1+L2
 *   6. Cross-invalidation at L1+L2
 *   7. Hierarchy verification (short-circuit behavior)
 *   8. findJson at L1+L2
 *   9. Patch at L1+L2 (verifies both layers invalidated)
 *  10. InvalidateVia custom cross-invalidation at L1+L2
 *  11. InvalidateList cross-invalidation at L1+L2
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;

namespace decl = jcailloux::relais::cache::list::decl;

// #############################################################################
//
//  Local L1+L2 configs, repos, and helpers
//
// #############################################################################

namespace relais_test {

// Config presets for L1+L2 tests
namespace test_both {
using namespace jcailloux::relais::config;

/// Short L1 TTL for expiration tests — L1 expires quickly, L2 stays
inline constexpr auto ShortL1 = Both
    .with_l1_ttl(std::chrono::milliseconds{150});

/// Write-through strategy at L1+L2
inline constexpr auto WriteThrough = Both
    .with_update_strategy(UpdateStrategy::PopulateImmediately);

} // namespace test_both

// L1+L2 repos using existing FullCacheTestItemRepo and FullCacheTestUserRepo
// (already defined in TestRepositories.h)

// Short L1 TTL + L2: for expiration fallback tests
using ShortL1BothItemRepo = Repo<TestItemWrapper, "test:both:short", test_both::ShortL1>;

// Write-through at L1+L2
using WriteThroughBothItemRepo = Repo<TestItemWrapper, "test:both:wt", test_both::WriteThrough>;

// L1+L2 user repo for cross-invalidation target
using FullCacheInvUserRepo = Repo<TestUserWrapper, "test:user:both:inv", cfg::Both>;

// L1+L2 purchase repo with cross-invalidation → user
using FullCachePurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:both",
    cfg::Both,
    cache::Invalidate<FullCacheInvUserRepo, purchaseUserId>>;

using jcailloux::relais::wrapper::set;
using F = TestUserWrapper::Field;

// L1+L2 article repo for InvalidateVia target
using FullCacheInvArticleRepo = Repo<TestArticleWrapper, "test:article:both:inv", cfg::Both>;

// Resolver: Purchase user_id → Article IDs by same author
struct BothUserArticleResolver {
    static io::Task<std::vector<int64_t>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT id FROM relais_test_articles WHERE author_id = $1", user_id);
        std::vector<int64_t> ids;
        for (size_t i = 0; i < result.rows(); ++i)
            ids.push_back(result[i].get<int64_t>(0));
        co_return ids;
    }
};

// Purchase repo with Invalidate<User> + InvalidateVia<Article> at cfg::Both
using FullCacheCustomPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:both:custom",
    cfg::Both,
    cache::Invalidate<FullCacheInvUserRepo, purchaseUserId>,
    cache::InvalidateVia<FullCacheInvArticleRepo, purchaseUserId, &BothUserArticleResolver::resolve>>;

// L1+L2 purchase list repo (target of InvalidateList cross-invalidation)
using BothPurchaseListRepo = Repo<TestPurchaseWrapper, "test:purchase:list:both:forinv", cfg::Both>;
using BothPurchaseListQuery = BothPurchaseListRepo::ListQuery;

// Invalidator that clears both L1 and L2 for the purchase list
class BothPurchaseListInvalidator {
public:
    static io::Task<void> onEntityModified(
        std::shared_ptr<const TestPurchaseWrapper> entity)
    {
        if (entity) {
            TestInternals::resetListCacheState<BothPurchaseListRepo>();
            co_await BothPurchaseListRepo::invalidateAllListGroups();
        }
    }
};

// Purchase repo with InvalidateList at cfg::Both
using FullCacheListInvPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:both:listinv",
    cfg::Both,
    cache::InvalidateList<BothPurchaseListInvalidator>>;

} // namespace relais_test


// #############################################################################
//
//  1. L1 to L2 promotion
//
// #############################################################################

TEST_CASE("FullCache<TestItem> - L1 to L2 promotion",
          "[integration][db][full-cache][item]")
{
    TransactionGuard tx;

    SECTION("[find] cache miss populates L2 then L1") {
        auto id = insertTestItem("both_item", 100);

        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item != nullptr);
        REQUIRE(item->name == "both_item");
        REQUIRE(item->value == 100);

        // L1 should now have the item
        REQUIRE(getCacheSize<FullCacheTestItemRepo>() > 0);
    }

    SECTION("[find] L1 hit does not query DB (staleness test)") {
        auto id = insertTestItem("stale_both", 10);

        // Populate L1+L2
        sync(FullCacheTestItemRepo::find(id));

        // Modify DB directly
        updateTestItem(id, "modified_both", 999);

        // L1 hit returns stale value
        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item->name == "stale_both");
        REQUIRE(item->value == 10);
    }

    SECTION("[find] L1 miss with L2 hit promotes to L1") {
        auto id = insertTestItem("promote_item", 42);

        // Populate both L1 and L2
        sync(FullCacheTestItemRepo::find(id));

        // Clear L1 only
        FullCacheTestItemRepo::evict(id);

        // Modify DB directly — L2 still has old value
        updateTestItem(id, "db_only_value", 999);

        // Should read from L2 (not DB), promoting back to L1
        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item->name == "promote_item");
        REQUIRE(item->value == 42);
    }

    SECTION("[find] returns nullptr for non-existent id") {
        auto item = sync(FullCacheTestItemRepo::find(999999));
        REQUIRE(item == nullptr);
    }
}


// #############################################################################
//
//  2. Cascade invalidation
//
// #############################################################################

TEST_CASE("FullCache<TestItem> - cascade invalidation",
          "[integration][db][full-cache][invalidation]")
{
    TransactionGuard tx;

    SECTION("[invalidate] invalidate() clears both L1 and L2") {
        auto id = insertTestItem("inv_both", 10);

        // Populate both layers
        sync(FullCacheTestItemRepo::find(id));

        // Modify DB
        updateTestItem(id, "inv_updated", 20);

        // Invalidate both layers
        sync(FullCacheTestItemRepo::invalidate(id));

        // Next read should get fresh value from DB
        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item->name == "inv_updated");
        REQUIRE(item->value == 20);
    }

    SECTION("[invalidate] evict clears L1 but preserves L2") {
        auto id = insertTestItem("inv_l1_only", 10);

        // Populate both layers
        sync(FullCacheTestItemRepo::find(id));

        // Modify DB
        updateTestItem(id, "inv_l1_updated", 20);

        // Clear L1 only
        FullCacheTestItemRepo::evict(id);

        // Read should come from L2 (stale)
        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item->name == "inv_l1_only");
        REQUIRE(item->value == 10);
    }

    SECTION("[insert] populates both L1 and L2") {
        auto entity = makeTestItem("created_both", 77);
        auto created = sync(FullCacheTestItemRepo::insert(entity));
        REQUIRE(created != nullptr);

        // Modify DB directly
        updateTestItem(created->id, "sneaky_update", 0);

        // L1 cache should serve the original
        auto cached = sync(FullCacheTestItemRepo::find(created->id));
        REQUIRE(cached->name == "created_both");
        REQUIRE(cached->value == 77);
    }

    SECTION("[update] invalidates L1 and L2 (lazy reload)") {
        auto id = insertTestItem("before_update", 10);

        // Populate caches
        sync(FullCacheTestItemRepo::find(id));

        auto updated = makeTestItem("after_update", 20, "", true, id);
        sync(FullCacheTestItemRepo::update(id, updated));

        // Next read should get updated value
        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item->name == "after_update");
        REQUIRE(item->value == 20);
    }

    SECTION("[erase] invalidates both L1 and L2") {
        auto id = insertTestItem("to_erase", 10);

        // Populate caches
        sync(FullCacheTestItemRepo::find(id));

        sync(FullCacheTestItemRepo::erase(id));

        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item == nullptr);
    }
}


// #############################################################################
//
//  3. L1 expiration with L2 fallback
//
// #############################################################################

TEST_CASE("FullCache<TestItem> - L1 expiration with L2 fallback",
          "[integration][db][full-cache][ttl]")
{
    TransactionGuard tx;

    SECTION("[ttl] L1 expires but L2 still serves") {
        auto id = insertTestItem("ttl_item", 42);

        // Populate both caches via short-L1 repo
        sync(ShortL1BothItemRepo::find(id));

        // Wait for L1 to expire (150ms TTL)
        waitForExpiration(std::chrono::milliseconds{200});

        // Ensure L1 cleanup has run
        forcePurge<ShortL1BothItemRepo>();

        // Modify DB directly
        updateTestItem(id, "db_modified", 999);

        // L2 should still serve the old value (L2 has much longer TTL)
        auto item = sync(ShortL1BothItemRepo::find(id));
        REQUIRE(item->name == "ttl_item");
        REQUIRE(item->value == 42);
    }

    SECTION("[ttl] L2 repopulates L1 after L1 expiration") {
        auto id = insertTestItem("repopulate_item", 55);

        // Populate both layers
        sync(ShortL1BothItemRepo::find(id));

        // Wait for L1 expiration
        waitForExpiration(std::chrono::milliseconds{200});
        forcePurge<ShortL1BothItemRepo>();

        // Read again — should come from L2 and repopulate L1
        auto item = sync(ShortL1BothItemRepo::find(id));
        REQUIRE(item != nullptr);
        REQUIRE(item->name == "repopulate_item");

        // Verify L1 is now populated again (DB modification wouldn't affect cached value)
        updateTestItem(id, "sneaky", 0);
        auto cached = sync(ShortL1BothItemRepo::find(id));
        REQUIRE(cached->name == "repopulate_item");
    }
}


// #############################################################################
//
//  4. Write-through at L1+L2
//
// #############################################################################

TEST_CASE("FullCache<TestItem> - write-through at L1+L2",
          "[integration][db][full-cache][write-through]")
{
    TransactionGuard tx;

    SECTION("[write-through] update populates L1 immediately") {
        auto id = insertTestItem("wt_before", 10);

        // Populate cache
        sync(WriteThroughBothItemRepo::find(id));

        // Update via write-through
        auto updated = makeTestItem("wt_after", 20, "", true, id);
        sync(WriteThroughBothItemRepo::update(id, updated));

        // L1 should immediately have the new value
        auto item = sync(WriteThroughBothItemRepo::find(id));
        REQUIRE(item->name == "wt_after");
        REQUIRE(item->value == 20);
    }
}


// #############################################################################
//
//  5. Binary entity at L1+L2
//
// #############################################################################

TEST_CASE("FullCache<TestUser> - binary entity at L1+L2",
          "[integration][db][full-cache][binary]")
{
    TransactionGuard tx;

    SECTION("[binary] BEVE entity cached in both L1 and L2") {
        auto id = insertTestUser("beve_user", "beve@test.com", 100);

        auto user = sync(FullCacheTestUserRepo::find(id));
        REQUIRE(user != nullptr);
        REQUIRE(user->username == "beve_user");
        REQUIRE(user->balance == 100);

        // Staleness confirms L1 caching
        updateTestUserBalance(id, 999);
        auto cached = sync(FullCacheTestUserRepo::find(id));
        REQUIRE(cached->balance == 100);
    }

    SECTION("[binary] L1 miss reads from L2 binary") {
        auto id = insertTestUser("l2_binary", "l2@test.com", 200);

        // Populate both
        sync(FullCacheTestUserRepo::find(id));

        // Clear L1
        FullCacheTestUserRepo::evict(id);

        // Modify DB
        updateTestUserBalance(id, 999);

        // Should read from L2 (stale binary value)
        auto user = sync(FullCacheTestUserRepo::find(id));
        REQUIRE(user->username == "l2_binary");
        REQUIRE(user->balance == 200);
    }

    SECTION("[binary] patch invalidates both layers") {
        auto id = insertTestUser("patch_user", "patch@test.com", 50);

        // Populate cache
        sync(FullCacheTestUserRepo::find(id));

        // Partial update
        auto updated = sync(FullCacheTestUserRepo::patch(id,
            set<F::balance>(300)));
        REQUIRE(updated != nullptr);
        REQUIRE(updated->balance == 300);

        // Fresh read should reflect the update
        auto fresh = sync(FullCacheTestUserRepo::find(id));
        REQUIRE(fresh->balance == 300);
    }
}


// #############################################################################
//
//  6. Cross-invalidation at L1+L2
//
// #############################################################################

TEST_CASE("FullCache - cross-invalidation at L1+L2",
          "[integration][db][full-cache][cross-inv]")
{
    TransactionGuard tx;

    SECTION("[cross-inv] purchase insert invalidates user in both L1 and L2") {
        auto userId = insertTestUser("cross_user", "cross@test.com", 100);

        // Populate user cache in both layers
        sync(FullCacheInvUserRepo::find(userId));

        // Modify user balance directly in DB
        updateTestUserBalance(userId, 200);

        // insert purchase — should invalidate user in both L1 and L2
        auto purchase = makeTestPurchase(userId, "Widget", 50);
        sync(FullCachePurchaseRepo::insert(purchase));

        // User should now be re-fetched from DB (both layers invalidated)
        auto user = sync(FullCacheInvUserRepo::find(userId));
        REQUIRE(user->balance == 200);
    }

    SECTION("[cross-inv] FK change invalidates old and new user in both layers") {
        auto user1Id = insertTestUser("old_user", "old@test.com", 100);
        auto user2Id = insertTestUser("new_user", "new@test.com", 200);

        // Populate both user caches
        sync(FullCacheInvUserRepo::find(user1Id));
        sync(FullCacheInvUserRepo::find(user2Id));

        // insert purchase for user1
        auto purchase = makeTestPurchase(user1Id, "Gadget", 30);
        auto created = sync(FullCachePurchaseRepo::insert(purchase));

        // Modify both users directly in DB
        updateTestUserBalance(user1Id, 111);
        updateTestUserBalance(user2Id, 222);

        // Re-populate caches
        sync(FullCacheInvUserRepo::find(user1Id));
        sync(FullCacheInvUserRepo::find(user2Id));

        // Update purchase to point to user2 (FK change)
        updateTestPurchaseUserId(created->id, user2Id);
        auto updatedPurchase = makeTestPurchase(user2Id, "Gadget", 30, "pending", created->id);
        sync(FullCachePurchaseRepo::update(created->id, updatedPurchase));

        // Modify both users again
        updateTestUserBalance(user1Id, 333);
        updateTestUserBalance(user2Id, 444);

        // Both users should be invalidated
        auto u1 = sync(FullCacheInvUserRepo::find(user1Id));
        auto u2 = sync(FullCacheInvUserRepo::find(user2Id));
        REQUIRE(u1->balance == 333);
        REQUIRE(u2->balance == 444);
    }
}


// #############################################################################
//
//  7. Hierarchy verification
//
// #############################################################################

TEST_CASE("FullCache - hierarchy verification",
          "[integration][db][full-cache][hierarchy]")
{
    TransactionGuard tx;

    SECTION("[hierarchy] L1 hit prevents L2/DB query (short-circuit)") {
        auto id = insertTestItem("hierarchy_item", 10);

        // Populate all layers
        sync(FullCacheTestItemRepo::find(id));

        // Modify DB — L1 and L2 are stale
        updateTestItem(id, "hierarchy_modified", 99);

        // L1 serves stale value (short-circuits)
        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item->name == "hierarchy_item");
    }

    SECTION("[hierarchy] L2 hit prevents DB query after L1 miss") {
        auto id = insertTestItem("l2_hit_item", 20);

        // Populate all layers
        sync(FullCacheTestItemRepo::find(id));

        // Clear L1 only
        FullCacheTestItemRepo::evict(id);

        // Modify DB — L2 still has old value
        updateTestItem(id, "l2_hit_modified", 99);

        // L2 serves old value (promotes to L1)
        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item->name == "l2_hit_item");
        REQUIRE(item->value == 20);
    }

    SECTION("[hierarchy] full miss queries DB and populates L2 then L1") {
        auto id = insertTestItem("full_miss_item", 30);

        // Populate and then invalidate both
        sync(FullCacheTestItemRepo::find(id));
        sync(FullCacheTestItemRepo::invalidate(id));

        // Update DB
        updateTestItem(id, "full_miss_updated", 60);

        // Full miss → DB fetch → repopulate both
        auto item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(item->name == "full_miss_updated");
        REQUIRE(item->value == 60);

        // Verify it's cached: DB modification won't be visible
        updateTestItem(id, "sneaky", 0);
        auto cached = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(cached->name == "full_miss_updated");
    }
}


// #############################################################################
//
//  8. findJson at L1+L2
//
// #############################################################################

TEST_CASE("FullCache - findJson at L1+L2",
          "[integration][db][full-cache][json]")
{
    TransactionGuard tx;

    SECTION("[json] returns cached JSON from L1") {
        auto id = insertTestItem("json_item", 42);

        auto json1 = sync(FullCacheTestItemRepo::findJson(id));
        REQUIRE(json1 != nullptr);
        REQUIRE(json1->find("\"json_item\"") != std::string::npos);

        // Second call returns same cached pointer
        auto json2 = sync(FullCacheTestItemRepo::findJson(id));
        REQUIRE(json2 != nullptr);
        REQUIRE(*json1 == *json2);
    }

    SECTION("[json] L1 miss falls back to L2 JSON") {
        auto id = insertTestItem("json_l2_item", 99);

        // Populate L1+L2
        sync(FullCacheTestItemRepo::findJson(id));

        // Clear L1
        FullCacheTestItemRepo::evict(id);

        // Should fall back to L2
        auto json = sync(FullCacheTestItemRepo::findJson(id));
        REQUIRE(json != nullptr);
        REQUIRE(json->find("\"json_l2_item\"") != std::string::npos);
    }
}


// =============================================================================
// Helper: build a purchase list query for the L1+L2 purchase list repo
// =============================================================================

static BothPurchaseListQuery makeBothPurchaseQuery(
    std::optional<int64_t> user_id = std::nullopt,
    std::optional<std::string> status = std::nullopt,
    uint16_t limit = 10
) {
    BothPurchaseListQuery q;
    q.limit = limit;
    if (status)  q.filters.template get<0>() = std::move(*status);
    if (user_id) q.filters.template get<1>() = *user_id;

    using Desc = BothPurchaseListRepo::ListDescriptorType;
    q.group_key = decl::groupCacheKey<Desc>(q);
    q.cache_key = decl::cacheKey<Desc>(q);
    return q;
}


// #############################################################################
//
//  9. Patch at L1+L2 (verifies both layers invalidated)
//
// #############################################################################

TEST_CASE("FullCache<TestUser> - patch at L1+L2",
          "[integration][db][full-cache][patch]")
{
    TransactionGuard tx;

    SECTION("[patch] single field invalidates both L1 and L2") {
        auto id = insertTestUser("patch_both", "patch@both.com", 100);

        // Populate both layers
        sync(FullCacheTestUserRepo::find(id));

        // Patch single field
        auto result = sync(FullCacheTestUserRepo::patch(id, set<F::balance>(500)));
        REQUIRE(result != nullptr);
        REQUIRE(result->balance == 500);

        // Evict L1 to force L2 read — verifies L2 was also invalidated
        FullCacheTestUserRepo::evict(id);

        auto fresh = sync(FullCacheTestUserRepo::find(id));
        REQUIRE(fresh->balance == 500);
    }

    SECTION("[patch] multiple fields invalidates both L1 and L2") {
        auto id = insertTestUser("carol", "carol@both.com", 200);

        // Populate both layers
        sync(FullCacheTestUserRepo::find(id));

        // Patch multiple fields
        auto result = sync(FullCacheTestUserRepo::patch(id,
            set<F::balance>(0),
            set<F::username>(std::string("caroline"))));
        REQUIRE(result->balance == 0);
        REQUIRE(result->username == "caroline");
        REQUIRE(result->email == "carol@both.com");

        // Evict L1 to verify L2 invalidation
        FullCacheTestUserRepo::evict(id);

        auto fresh = sync(FullCacheTestUserRepo::find(id));
        REQUIRE(fresh->balance == 0);
        REQUIRE(fresh->username == "caroline");
    }
}


// #############################################################################
//
//  10. InvalidateVia custom cross-invalidation at L1+L2
//
// #############################################################################

TEST_CASE("FullCache - custom cross-invalidation via resolver at L1+L2",
          "[integration][db][full-cache][custom-inv]")
{
    TransactionGuard tx;

    SECTION("[custom-inv] purchase creation invalidates user AND related articles") {
        auto userId    = insertTestUser("author", "author@both.com", 1000);
        auto articleId = insertTestArticle("tech", userId, "My Article", 42, true);

        // Populate user and article caches in both L1+L2
        auto user1    = sync(FullCacheInvUserRepo::find(userId));
        auto article1 = sync(FullCacheInvArticleRepo::find(articleId));
        REQUIRE(user1 != nullptr);
        REQUIRE(article1 != nullptr);

        // Modify DB directly (bypasses cache)
        updateTestUserBalance(userId, 500);
        updateTestArticle(articleId, "Updated Title", 999);

        // Caches should return stale values
        CHECK(sync(FullCacheInvUserRepo::find(userId))->balance == 1000);
        CHECK(sync(FullCacheInvArticleRepo::find(articleId))->title == "My Article");

        // Insert purchase → triggers Invalidate<User> + InvalidateVia<Article>
        sync(FullCacheCustomPurchaseRepo::insert(
            makeTestPurchase(userId, "Trigger", 50, "pending")));

        // Both user and article should now return fresh values
        CHECK(sync(FullCacheInvUserRepo::find(userId))->balance == 500);
        auto article2 = sync(FullCacheInvArticleRepo::find(articleId));
        CHECK(article2->title == "Updated Title");
        CHECK(article2->view_count == 999);
    }

    SECTION("[custom-inv] L2 is also invalidated (not just L1)") {
        auto userId    = insertTestUser("author", "author@both.com", 1000);
        auto articleId = insertTestArticle("tech", userId, "My Article", 42, true);

        // Populate both layers
        sync(FullCacheInvUserRepo::find(userId));
        sync(FullCacheInvArticleRepo::find(articleId));

        // Modify DB directly
        updateTestUserBalance(userId, 500);
        updateTestArticle(articleId, "Updated Title", 999);

        // Insert purchase → triggers cross-invalidation
        sync(FullCacheCustomPurchaseRepo::insert(
            makeTestPurchase(userId, "Trigger", 50, "pending")));

        // Evict L1 to force L2 read — verifies L2 was also invalidated
        FullCacheInvUserRepo::evict(userId);
        FullCacheInvArticleRepo::evict(articleId);

        CHECK(sync(FullCacheInvUserRepo::find(userId))->balance == 500);
        CHECK(sync(FullCacheInvArticleRepo::find(articleId))->title == "Updated Title");
    }

    SECTION("[custom-inv] resolver with no related articles does not crash") {
        auto userId = insertTestUser("orphan", "orphan@both.com", 100);

        // No articles for this user
        sync(FullCacheInvUserRepo::find(userId));

        // Should not crash even though resolver returns empty
        sync(FullCacheCustomPurchaseRepo::insert(
            makeTestPurchase(userId, "Safe", 50, "pending")));

        auto user = sync(FullCacheInvUserRepo::find(userId));
        CHECK(user != nullptr);
    }

    SECTION("[custom-inv] resolver invalidates multiple articles") {
        auto userId = insertTestUser("author", "author@both.com", 1000);
        auto a1 = insertTestArticle("tech", userId, "Article 1", 10, true);
        auto a2 = insertTestArticle("news", userId, "Article 2", 20, true);
        auto a3 = insertTestArticle("tech", userId, "Article 3", 30, true);

        // Populate all caches
        sync(FullCacheInvArticleRepo::find(a1));
        sync(FullCacheInvArticleRepo::find(a2));
        sync(FullCacheInvArticleRepo::find(a3));

        // Modify all articles in DB
        updateTestArticle(a1, "New 1", 100);
        updateTestArticle(a2, "New 2", 200);
        updateTestArticle(a3, "New 3", 300);

        // Stale check
        CHECK(sync(FullCacheInvArticleRepo::find(a1))->title == "Article 1");

        // Insert purchase → resolver returns 3 article IDs → all invalidated
        sync(FullCacheCustomPurchaseRepo::insert(
            makeTestPurchase(userId, "Trigger", 50, "pending")));

        CHECK(sync(FullCacheInvArticleRepo::find(a1))->title == "New 1");
        CHECK(sync(FullCacheInvArticleRepo::find(a2))->title == "New 2");
        CHECK(sync(FullCacheInvArticleRepo::find(a3))->title == "New 3");
    }
}


// #############################################################################
//
//  11. InvalidateList cross-invalidation at L1+L2
//
// #############################################################################

TEST_CASE("FullCache - list cross-invalidation at L1+L2",
          "[integration][db][full-cache][list-inv]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<BothPurchaseListRepo>();

    SECTION("[list-inv] purchase creation invalidates purchase list in both L1 and L2") {
        auto userId = insertTestUser("list_user", "list@both.com", 1000);
        insertTestPurchase(userId, "Existing Product", 50);

        // Populate L1+L2 list cache
        auto query   = makeBothPurchaseQuery(userId);
        auto result1 = sync(BothPurchaseListRepo::query(query));
        REQUIRE(result1->size() == 1);

        // Insert directly in DB (bypasses cache)
        insertTestPurchase(userId, "Direct Insert", 75);

        // Should be stale from L1 cache
        CHECK(sync(BothPurchaseListRepo::query(query))->size() == 1);

        // Insert via repo → triggers InvalidateList<BothPurchaseListInvalidator>
        sync(FullCacheListInvPurchaseRepo::insert(
            makeTestPurchase(userId, "Via Repo", 100, "pending")));

        // Both L1 and L2 invalidated → should see all 3
        auto result3 = sync(BothPurchaseListRepo::query(query));
        CHECK(result3->size() == 3);
    }

    SECTION("[list-inv] purchase deletion invalidates purchase list in both layers") {
        auto userId = insertTestUser("list_user", "list@both.com", 1000);
        auto p1 = insertTestPurchase(userId, "Keep", 50);
        auto p2 = insertTestPurchase(userId, "Delete", 100);

        // Populate L1+L2 list cache
        auto query   = makeBothPurchaseQuery(userId);
        auto result1 = sync(BothPurchaseListRepo::query(query));
        REQUIRE(result1->size() == 2);

        // Delete via repo → triggers cross-invalidation
        sync(FullCacheListInvPurchaseRepo::erase(p2));

        // Should see only 1 purchase
        auto result2 = sync(BothPurchaseListRepo::query(query));
        CHECK(result2->size() == 1);
    }
}
