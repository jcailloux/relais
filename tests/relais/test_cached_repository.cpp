/**
 * test_cached_repository.cpp
 *
 * Tests for CachedRepo (L1 - RAM caching on top of database).
 * Uses L1 configurations that resolve to CachedRepo via Repo<>.
 *
 * Progressive complexity:
 *   1. TestItem    — basic CRUD with L1 cache (staleness, populate, invalidate)
 *   2. Config      — TTL, refresh, accept-expired, write-through, cleanup
 *   3. Cross-inv   — Invalidate<> Purchase → User at L1
 *   4. Custom-inv  — InvalidateVia<> with async resolver at L1
 *   5. List-inv    — InvalidateList<> entity → ListDescriptor bridge at L1
 *   6. ListVia     — InvalidateListVia<> with GroupKey (3 granularities)
 *   7. Binary      — binary entity CRUD with L1 caching
 *   8. patch    — partial field updates with L1 invalidation
 *   9. JSON        — findJson with L1 caching
 *  10. ReadOnly    — read-only repository at L1
 *  11. RO+Inv      — read-only as cross-invalidation target at L1
 *
 * SECTION naming convention:
 *   [find]      — read by primary key with caching
 *   [insert]        — insert with L1 cache population
 *   [update]        — modify with L1 invalidation/population
 *   [erase]        — delete with L1 invalidation
 *   [invalidate]    — explicit cache invalidation
 *   [ttl]           — TTL expiration behavior
 *   [refresh]       — TTL refresh on get
 *   [expired]       — accept-expired behavior
 *   [write-through] — PopulateImmediately strategy
 *   [cleanup]       — segment-based cleanup
 *   [cross-inv]     — cross-cache invalidation (standard)
 *   [custom-inv]    — custom invalidation (InvalidateVia)
 *   [list-inv]      — entity → ListDescriptor cross-invalidation
 *   [list-resolver] — InvalidateListVia with typed GroupKey
 *   [list-granularity] — per-page, per-group, full pattern dispatch
 *   [binary]    — binary entity caching
 *   [patch]      — partial field updates
 *   [json]          — findJson raw JSON retrieval
 *   [readonly]      — read-only repository
 *   [readonly-inv]  — read-only as cross-invalidation target
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;

// #############################################################################
//
//  Local L1 configs, repos, resolvers, and helpers
//
// #############################################################################

namespace relais_test {

// =============================================================================
// L1 repos for cross-invalidation testing
// =============================================================================

/// L1 user repo as cross-invalidation target.
using L1InvTestUserRepo = Repo<TestUserWrapper, "test:user:l1:inv">;

/// L1 article repo as cross-invalidation target (for InvalidateVia).
using L1InvTestArticleRepo = Repo<TestArticleWrapper, "test:article:l1:inv">;

// =============================================================================
// Standard cross-invalidation: Purchase → User (L1)
// =============================================================================

using L1InvTestPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l1:inv",
    cfg::Local,
    cache::Invalidate<L1InvTestUserRepo, purchaseUserId>>;

// =============================================================================
// Custom cross-invalidation: Purchase → User + Articles (via resolver, L1)
// =============================================================================

/**
 * Async resolver: given a user_id, finds all article IDs by that author.
 */
struct L1UserArticleResolver {
    static io::Task<std::vector<int64_t>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT id FROM relais_test_articles WHERE author_id = $1", user_id);
        std::vector<int64_t> ids;
        for (size_t i = 0; i < result.rows(); ++i) {
            ids.push_back(result[i].get<int64_t>(0));
        }
        co_return ids;
    }
};

using L1CustomTestPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l1:custom",
    cfg::Local,
    cache::Invalidate<L1InvTestUserRepo, purchaseUserId>,
    cache::InvalidateVia<L1InvTestArticleRepo, purchaseUserId, &L1UserArticleResolver::resolve>>;

// =============================================================================
// Entity → ListDescriptor cross-invalidation bridge
// =============================================================================

/**
 * Invalidator bridge: when a purchase entity changes, reset the purchase ListDescriptor.
 * In production, one would use notifyCreated/Updated/Deleted for precision;
 * here we use resetListCacheState() to demonstrate the bridge mechanism.
 */
class L1PurchaseListInvalidator {
public:
    static io::Task<void> onEntityModified(
        std::shared_ptr<const TestPurchaseWrapper>)
    {
        TestInternals::resetListCacheState<TestPurchaseListRepo>();
        co_return;
    }
};

using L1ListInvPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l1:listinv",
    cfg::Local,
    cache::Invalidate<L1InvTestUserRepo, purchaseUserId>,
    cache::InvalidateList<L1PurchaseListInvalidator>>;

// =============================================================================
// Mock list repo for InvalidateListVia testing at L1
// =============================================================================

/**
 * Mock list repo that records invalidation calls for verification.
 * Not backed by real cache — used to test the InvalidateListVia dispatch logic.
 */
class L1MockArticleListRepo {
public:
    struct GroupKey {
        std::string category;
    };

    struct InvocationRecord {
        std::string category;
        std::optional<int64_t> sort_value;
    };

    static inline std::vector<InvocationRecord> invocations;
    static inline bool all_groups_invalidated = false;

    static void reset() {
        invocations.clear();
        all_groups_invalidated = false;
    }

    static io::Task<size_t> invalidateByTarget(
        const GroupKey& gk, std::optional<int64_t> sort_value)
    {
        invocations.push_back({gk.category, sort_value});
        co_return 1;
    }

    static io::Task<size_t> invalidateAllListGroups() {
        all_groups_invalidated = true;
        co_return 1;
    }
};

// =============================================================================
// Resolvers for InvalidateListVia granularity tests
// =============================================================================

using L1MockGroupKey = L1MockArticleListRepo::GroupKey;
using L1MockTarget = jcailloux::relais::cache::ListInvalidationTarget<L1MockGroupKey>;

/**
 * Per-page resolver: returns targets WITH sort_value → per-page invalidation.
 */
struct L1PerPageResolver {
    static io::Task<std::vector<L1MockTarget>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT category, view_count FROM relais_test_articles WHERE author_id = $1",
            user_id);
        std::vector<L1MockTarget> targets;
        for (size_t i = 0; i < result.rows(); ++i) {
            L1MockTarget t;
            t.filters.category = result[i].get<std::string>(0);
            t.sort_value = result[i].get<int64_t>(1);
            targets.push_back(std::move(t));
        }
        co_return targets;
    }
};

/**
 * Per-group resolver: returns targets WITHOUT sort_value → per-group invalidation.
 */
struct L1PerGroupResolver {
    static io::Task<std::vector<L1MockTarget>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT DISTINCT category FROM relais_test_articles WHERE author_id = $1",
            user_id);
        std::vector<L1MockTarget> targets;
        for (size_t i = 0; i < result.rows(); ++i) {
            L1MockTarget t;
            t.filters.category = result[i].get<std::string>(0);
            // No sort_value → per-group invalidation
            targets.push_back(std::move(t));
        }
        co_return targets;
    }
};

/**
 * Full pattern resolver: returns nullopt → all groups invalidated.
 */
struct L1FullPatternResolver {
    static io::Task<std::optional<std::vector<L1MockTarget>>> resolve(
        [[maybe_unused]] int64_t user_id)
    {
        co_return std::nullopt;
    }
};

/**
 * Mixed resolver: per-page for "tech", per-group for other categories.
 */
struct L1MixedResolver {
    static io::Task<std::vector<L1MockTarget>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT category, view_count FROM relais_test_articles WHERE author_id = $1",
            user_id);
        std::vector<L1MockTarget> targets;
        std::set<std::string> seen;

        for (size_t i = 0; i < result.rows(); ++i) {
            auto category = result[i].get<std::string>(0);

            if (category == "tech") {
                // Per-page: include sort_value
                L1MockTarget t;
                t.filters.category = category;
                t.sort_value = result[i].get<int64_t>(1);
                targets.push_back(std::move(t));
            } else if (seen.insert(category).second) {
                // Per-group: no sort_value, deduplicated
                L1MockTarget t;
                t.filters.category = category;
                targets.push_back(std::move(t));
            }
        }
        co_return targets;
    }
};

// =============================================================================
// Purchase repos for InvalidateListVia granularity tests
// =============================================================================

using L1PerPagePurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l1:perpage",
    cfg::Local,
    cache::InvalidateListVia<L1MockArticleListRepo, purchaseUserId, &L1PerPageResolver::resolve>>;

using L1PerGroupPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l1:pergroup",
    cfg::Local,
    cache::InvalidateListVia<L1MockArticleListRepo, purchaseUserId, &L1PerGroupResolver::resolve>>;

using L1FullPatternPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l1:fullpattern",
    cfg::Local,
    cache::InvalidateListVia<L1MockArticleListRepo, purchaseUserId, &L1FullPatternResolver::resolve>>;

using L1MixedPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l1:mixed",
    cfg::Local,
    cache::InvalidateListVia<L1MockArticleListRepo, purchaseUserId, &L1MixedResolver::resolve>>;

// =============================================================================
// Read-only L1 configs and repos
// =============================================================================

// CacheConfig presets for read-only tests
namespace test_local {
using namespace jcailloux::relais::config;
inline constexpr auto ReadOnlyL1 = Local.with_read_only();
inline constexpr auto ReadOnlyUserL1 = Local.with_read_only();
} // namespace test_local

/// L1 read-only item repository — no writes allowed.
using ReadOnlyL1TestItemRepo = Repo<TestItemWrapper, "test:readonly:l1",
    test_local::ReadOnlyL1>;

/// L1 read-only user repository — CachedRepo provides invalidate().
using ReadOnlyL1TestUserRepo = Repo<TestUserWrapper, "test:readonly:user:l1",
    test_local::ReadOnlyUserL1>;

/// L1 purchase repo whose writes invalidate a read-only user repo.
using L1ReadOnlyInvPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l1:readonly:inv",
    cfg::Local,
    cache::Invalidate<ReadOnlyL1TestUserRepo, purchaseUserId>>;

} // namespace relais_test

using jcailloux::relais::wrapper::set;
using F = TestUserWrapper::Field;


// #############################################################################
//
//  1. TestItem — basic CRUD with L1 cache
//
// #############################################################################

TEST_CASE("CachedRepo<TestItem> - find",
          "[integration][db][cached][item]")
{
    TransactionGuard tx;

    SECTION("[find] caches result in L1 and returns stale data") {
        auto id = insertTestItem("Cached", 42);

        // First call: cache miss → fetches from DB → populates L1
        auto result1 = sync(L1TestItemRepo::find(id));
        REQUIRE(result1 != nullptr);
        REQUIRE(result1->name == "Cached");
        REQUIRE(result1->value == 42);

        // Modify directly in DB (bypass cache)
        updateTestItem(id, "Modified", 99);

        // Second call: cache hit → returns stale data
        auto result2 = sync(L1TestItemRepo::find(id));
        REQUIRE(result2 != nullptr);
        CHECK(result2->name == "Cached");   // Still old value
        CHECK(result2->value == 42);
    }

    SECTION("[find] returns nullptr for non-existent id") {
        auto result = sync(L1TestItemRepo::find(999999));
        CHECK(result == nullptr);
    }

    SECTION("[find] returns correct entity among multiple") {
        auto id1 = insertTestItem("First", 1);
        auto id2 = insertTestItem("Second", 2);

        auto r1 = sync(L1TestItemRepo::find(id1));
        auto r2 = sync(L1TestItemRepo::find(id2));

        REQUIRE(r1 != nullptr);
        REQUIRE(r2 != nullptr);
        CHECK(r1->name == "First");
        CHECK(r2->name == "Second");
    }
}

TEST_CASE("CachedRepo<TestItem> - insert",
          "[integration][db][cached][item]")
{
    TransactionGuard tx;

    SECTION("[insert] inserts entity and populates L1 cache") {
        auto created = sync(L1TestItemRepo::insert(makeTestItem("New Item", 100, "Created via repo")));
        REQUIRE(created != nullptr);
        REQUIRE(created->id > 0);
        CHECK(created->name == "New Item");
        CHECK(created->value == 100);

        // Modify in DB directly
        updateTestItem(created->id, "DB Modified", 999);

        // L1 cache populated by insert → returns stale value
        auto cached = sync(L1TestItemRepo::find(created->id));
        REQUIRE(cached != nullptr);
        CHECK(cached->name == "New Item");  // From L1 cache
    }
}

TEST_CASE("CachedRepo<TestItem> - update",
          "[integration][db][cached][item]")
{
    TransactionGuard tx;

    SECTION("[update] invalidates L1 cache (default lazy reload strategy)") {
        auto id = insertTestItem("Original", 1);

        // Populate L1 cache
        sync(L1TestItemRepo::find(id));

        // Update through repo (invalidates L1, writes to DB)
        auto success = sync(L1TestItemRepo::update(id, makeTestItem("Updated", 2, "", true, id)));
        REQUIRE(success);

        // Modify again directly in DB
        updateTestItem(id, "DB Override", 99);

        // InvalidateAndLazyReload: L1 was invalidated, next read fetches from DB
        auto result = sync(L1TestItemRepo::find(id));
        REQUIRE(result != nullptr);
        CHECK(result->name == "DB Override");
        CHECK(result->value == 99);
    }
}

TEST_CASE("CachedRepo<TestItem> - erase",
          "[integration][db][cached][item]")
{
    TransactionGuard tx;

    SECTION("[erase] invalidates L1 cache") {
        auto id = insertTestItem("ToDelete", 1);

        // Populate L1 cache
        sync(L1TestItemRepo::find(id));

        // erase through repo
        auto result = sync(L1TestItemRepo::erase(id));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        // Entity gone from DB and cache
        auto gone = sync(L1TestItemRepo::find(id));
        CHECK(gone == nullptr);
    }

    SECTION("[erase] returns 0 for non-existent id") {
        auto result = sync(L1TestItemRepo::erase(999999));
        REQUIRE(result.has_value());
        CHECK(*result == 0);
    }
}

TEST_CASE("CachedRepo<TestItem> - explicit invalidation",
          "[integration][db][cached][invalidate]")
{
    TransactionGuard tx;

    SECTION("[invalidate] clears L1 cache entry") {
        auto id = insertTestItem("Invalidate Me", 42);

        // Populate L1
        sync(L1TestItemRepo::find(id));
        updateTestItem(id, "Fresh Value", 99);

        // Still cached
        CHECK(sync(L1TestItemRepo::find(id))->name == "Invalidate Me");

        // Invalidate
        sync(L1TestItemRepo::invalidate(id));

        // Next read gets fresh data
        auto fresh = sync(L1TestItemRepo::find(id));
        REQUIRE(fresh != nullptr);
        CHECK(fresh->name == "Fresh Value");
        CHECK(fresh->value == 99);
    }

    SECTION("[invalidate] does not affect other entries") {
        auto id1 = insertTestItem("Keep", 1);
        auto id2 = insertTestItem("Invalidate", 2);

        // Populate both
        sync(L1TestItemRepo::find(id1));
        sync(L1TestItemRepo::find(id2));

        updateTestItem(id1, "Keep Modified", 10);
        updateTestItem(id2, "Inv Modified", 20);

        // Invalidate only id2
        sync(L1TestItemRepo::invalidate(id2));

        // id1 still cached (stale)
        CHECK(sync(L1TestItemRepo::find(id1))->name == "Keep");
        // id2 refreshed from DB
        CHECK(sync(L1TestItemRepo::find(id2))->name == "Inv Modified");
    }
}


// #############################################################################
//
//  2. Config behaviors — TTL, refresh, strategies, cleanup
//
// #############################################################################

TEST_CASE("CachedRepo - ShortTTL config",
          "[integration][db][cached][config][ttl]")
{
    TransactionGuard tx;

    SECTION("[ttl] expired entry not returned (l1_accept_expired_on_get = false)") {
        auto id = insertTestItem("Short TTL", 42);

        // Populate cache (TTL = 100ms)
        auto r1 = sync(ShortTTLTestItemRepo::find(id));
        REQUIRE(r1 != nullptr);

        updateTestItem(id, "After Expiry", 99);

        // Wait for TTL expiration
        waitForExpiration(std::chrono::milliseconds{150});

        // Expired entry rejected → fetches fresh from DB
        auto r2 = sync(ShortTTLTestItemRepo::find(id));
        REQUIRE(r2 != nullptr);
        CHECK(r2->name == "After Expiry");
    }
}

TEST_CASE("CachedRepo - AcceptExpired config",
          "[integration][db][cached][config][expired]")
{
    TransactionGuard tx;

    SECTION("[expired] expired entry returned until cleanup") {
        auto id = insertTestItem("Accept Expired", 42);

        // Populate cache (TTL = 100ms)
        sync(AcceptExpiredTestItemRepo::find(id));
        updateTestItem(id, "Fresh", 99);

        waitForExpiration(std::chrono::milliseconds{150});

        // Expired but accepted (l1_accept_expired_on_get = true)
        auto stale = sync(AcceptExpiredTestItemRepo::find(id));
        REQUIRE(stale != nullptr);
        CHECK(stale->name == "Accept Expired");  // Stale, but accepted

        // Cleanup evicts expired entries
        forcePurge<AcceptExpiredTestItemRepo>();

        auto fresh = sync(AcceptExpiredTestItemRepo::find(id));
        REQUIRE(fresh != nullptr);
        CHECK(fresh->name == "Fresh");
    }
}

TEST_CASE("CachedRepo - NoRefresh config",
          "[integration][db][cached][config][refresh]")
{
    TransactionGuard tx;

    SECTION("[refresh] TTL not extended on get (l1_refresh_on_get = false)") {
        auto id = insertTestItem("No Refresh", 42);

        // Populate cache (TTL = 200ms, no refresh, accept expired)
        sync(NoRefreshTestItemRepo::find(id));

        // Read at 120ms (within TTL)
        waitForExpiration(std::chrono::milliseconds{120});
        sync(NoRefreshTestItemRepo::find(id));

        // Wait until past original 200ms TTL (total ~220ms)
        waitForExpiration(std::chrono::milliseconds{100});

        updateTestItem(id, "Refreshed", 99);

        // Entry expired; cleanup evicts it
        forcePurge<NoRefreshTestItemRepo>();

        auto fresh = sync(NoRefreshTestItemRepo::find(id));
        REQUIRE(fresh != nullptr);
        CHECK(fresh->name == "Refreshed");
    }
}

TEST_CASE("CachedRepo - WriteThrough config",
          "[integration][db][cached][config][write-through]")
{
    TransactionGuard tx;

    SECTION("[write-through] update populates cache immediately") {
        auto id = insertTestItem("Original", 1);

        // Populate cache
        sync(WriteThroughTestItemRepo::find(id));

        // Update through repo (PopulateImmediately strategy)
        sync(WriteThroughTestItemRepo::update(id, makeTestItem("Updated WT", 2, "", true, id)));

        // Modify in DB directly (bypass cache)
        updateTestItem(id, "DB Direct", 99);

        // Cache still has the write-through value
        auto cached = sync(WriteThroughTestItemRepo::find(id));
        REQUIRE(cached != nullptr);
        CHECK(cached->name == "Updated WT");
        CHECK(cached->value == 2);
    }
}

TEST_CASE("CachedRepo - FewShards config",
          "[integration][db][cached][config][cleanup]")
{
    TransactionGuard tx;

    SECTION("[cleanup] full cleanup only erases expired entries") {
        auto id1 = insertTestItem("Seg1", 1);
        auto id2 = insertTestItem("Seg2", 2);
        auto id3 = insertTestItem("Seg3", 3);

        sync(FewShardsTestItemRepo::find(id1));
        sync(FewShardsTestItemRepo::find(id2));
        sync(FewShardsTestItemRepo::find(id3));

        auto sizeBefore = getCacheSize<FewShardsTestItemRepo>();
        CHECK(sizeBefore >= 3);

        // Full cleanup: non-expired entries are NOT erased
        auto erased = FewShardsTestItemRepo::purge();
        CHECK(erased == 0);
        CHECK(getCacheSize<FewShardsTestItemRepo>() == sizeBefore);
    }

    SECTION("[cleanup] trySweep processes one shard at a time") {
        auto id = insertTestItem("Trigger", 1);
        sync(FewShardsTestItemRepo::find(id));

        // trySweep should return true (cleanup performed)
        auto cleaned = FewShardsTestItemRepo::trySweep();
        CHECK(cleaned);

        // Non-expired entry survives
        auto result = sync(FewShardsTestItemRepo::find(id));
        REQUIRE(result != nullptr);
        CHECK(result->name == "Trigger");
    }
}


// #############################################################################
//
//  3. Cross-invalidation: Purchase → User (Invalidate<>)
//
// #############################################################################

TEST_CASE("CachedRepo - cross-invalidation Purchase → User",
          "[integration][db][cached][cross-inv]")
{
    TransactionGuard tx;

    SECTION("[cross-inv] insert purchase invalidates user L1 cache") {
        auto userId = insertTestUser("inv_user", "inv@test.com", 1000);

        // Cache user in L1
        auto user1 = sync(L1InvTestUserRepo::find(userId));
        REQUIRE(user1 != nullptr);
        REQUIRE(user1->balance == 1000);

        // Modify user balance directly in DB
        updateTestUserBalance(userId, 500);

        // User still cached (stale)
        CHECK(sync(L1InvTestUserRepo::find(userId))->balance == 1000);

        // insert purchase → triggers Invalidate<User, &Purchase::user_id>
        auto created = sync(L1InvTestPurchaseRepo::insert(makeTestPurchase(userId, "Widget", 100, "pending")));
        REQUIRE(created != nullptr);

        // User L1 cache invalidated → next read gets fresh data from DB
        auto user2 = sync(L1InvTestUserRepo::find(userId));
        REQUIRE(user2 != nullptr);
        CHECK(user2->balance == 500);
    }

    SECTION("[cross-inv] update purchase invalidates user L1 cache") {
        auto userId = insertTestUser("update_user", "update@test.com", 1000);
        auto purchaseId = insertTestPurchase(userId, "Product", 50);

        // Cache user
        sync(L1InvTestUserRepo::find(userId));
        updateTestUserBalance(userId, 750);

        // Update purchase through repo
        sync(L1InvTestPurchaseRepo::update(purchaseId, makeTestPurchase(userId, "Updated Product", 100, "completed", purchaseId)));

        // User cache invalidated
        auto user = sync(L1InvTestUserRepo::find(userId));
        REQUIRE(user != nullptr);
        CHECK(user->balance == 750);
    }

    SECTION("[cross-inv] delete purchase invalidates user L1 cache") {
        auto userId = insertTestUser("del_user", "del@test.com", 1000);
        auto purchaseId = insertTestPurchase(userId, "To Delete", 50);

        sync(L1InvTestUserRepo::find(userId));
        updateTestUserBalance(userId, 200);

        sync(L1InvTestPurchaseRepo::erase(purchaseId));

        auto user = sync(L1InvTestUserRepo::find(userId));
        REQUIRE(user != nullptr);
        CHECK(user->balance == 200);
    }

    SECTION("[cross-inv] FK change invalidates both old and new user") {
        auto user1Id = insertTestUser("user_one", "one@test.com", 1000);
        auto user2Id = insertTestUser("user_two", "two@test.com", 2000);
        auto purchaseId = insertTestPurchase(user1Id, "Product", 100);

        // Cache both users
        sync(L1InvTestUserRepo::find(user1Id));
        sync(L1InvTestUserRepo::find(user2Id));

        // Modify both in DB
        updateTestUserBalance(user1Id, 111);
        updateTestUserBalance(user2Id, 222);

        // Both still cached
        CHECK(sync(L1InvTestUserRepo::find(user1Id))->balance == 1000);
        CHECK(sync(L1InvTestUserRepo::find(user2Id))->balance == 2000);

        // Update purchase: change user_id from user1 to user2
        sync(L1InvTestPurchaseRepo::update(purchaseId, makeTestPurchase(user2Id, "Product", 100, "pending", purchaseId)));

        // Both users invalidated (old FK + new FK)
        CHECK(sync(L1InvTestUserRepo::find(user1Id))->balance == 111);
        CHECK(sync(L1InvTestUserRepo::find(user2Id))->balance == 222);
    }
}


// #############################################################################
//
//  4. Custom cross-invalidation — InvalidateVia with resolver
//
// #############################################################################

TEST_CASE("CachedRepo - custom cross-invalidation via resolver",
          "[integration][db][cached][custom-inv]")
{
    TransactionGuard tx;

    SECTION("[custom-inv] purchase creation invalidates user AND related articles") {
        auto userId = insertTestUser("author", "author@test.com", 1000);
        auto articleId = insertTestArticle("tech", userId, "My Article", 42, true);

        // Cache user and article in L1
        auto user1 = sync(L1InvTestUserRepo::find(userId));
        auto article1 = sync(L1InvTestArticleRepo::find(articleId));
        REQUIRE(user1 != nullptr);
        REQUIRE(article1 != nullptr);

        // Modify both in DB
        updateTestUserBalance(userId, 500);
        updateTestArticle(articleId, "Updated Title", 999);

        // Both still cached
        CHECK(sync(L1InvTestUserRepo::find(userId))->balance == 1000);
        CHECK(sync(L1InvTestArticleRepo::find(articleId))->title == "My Article");

        // insert purchase → triggers Invalidate<User> + InvalidateVia<Article>
        sync(L1CustomTestPurchaseRepo::insert(makeTestPurchase(userId, "Trigger", 50, "pending")));

        // User invalidated (standard Invalidate<>)
        CHECK(sync(L1InvTestUserRepo::find(userId))->balance == 500);

        // Article invalidated (InvalidateVia resolver)
        auto article2 = sync(L1InvTestArticleRepo::find(articleId));
        CHECK(article2->title == "Updated Title");
        CHECK(article2->view_count == 999);
    }

    SECTION("[custom-inv] resolver with no related articles does not crash") {
        auto userId = insertTestUser("no_articles", "noart@test.com", 100);

        sync(L1InvTestUserRepo::find(userId));

        // Resolver returns empty vector — no crash
        auto created = sync(L1CustomTestPurchaseRepo::insert(makeTestPurchase(userId, "Safe Trigger", 10, "pending")));
        REQUIRE(created != nullptr);
    }

    SECTION("[custom-inv] resolver invalidates multiple articles") {
        auto userId = insertTestUser("prolific", "prolific@test.com", 1000);
        auto a1 = insertTestArticle("tech", userId, "Tech 1", 10, true);
        auto a2 = insertTestArticle("news", userId, "News 1", 20, true);
        auto a3 = insertTestArticle("tech", userId, "Tech 2", 30, true);

        // Cache all articles
        sync(L1InvTestArticleRepo::find(a1));
        sync(L1InvTestArticleRepo::find(a2));
        sync(L1InvTestArticleRepo::find(a3));

        // Modify all in DB
        updateTestArticle(a1, "New Tech 1", 100);
        updateTestArticle(a2, "New News 1", 200);
        updateTestArticle(a3, "New Tech 2", 300);

        // insert purchase → resolver finds all 3 articles
        sync(L1CustomTestPurchaseRepo::insert(makeTestPurchase(userId, "Big Trigger", 999, "pending")));

        // All articles refreshed from DB
        CHECK(sync(L1InvTestArticleRepo::find(a1))->title == "New Tech 1");
        CHECK(sync(L1InvTestArticleRepo::find(a2))->title == "New News 1");
        CHECK(sync(L1InvTestArticleRepo::find(a3))->title == "New Tech 2");
    }
}


// #############################################################################
//
//  5. Entity → ListDescriptor cross-invalidation via InvalidateList<>
//
// #############################################################################

TEST_CASE("CachedRepo - entity to ListDescriptor cross-invalidation",
          "[integration][db][cached][list-inv]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestPurchaseListRepo>();

    SECTION("[list-inv] purchase creation invalidates purchase ListDescriptor cache") {
        auto userId = insertTestUser("list_user", "list@test.com", 1000);
        insertTestPurchase(userId, "Existing Product", 50);

        // Query ListDescriptor to populate cache
        auto query = makePurchaseQuery(userId);
        auto result1 = sync(TestPurchaseListRepo::query(query));
        auto count1 = result1->size();
        CHECK(count1 == 1);

        // Insert purchase directly in DB (bypasses cache)
        insertTestPurchase(userId, "Direct Insert", 75);

        // ListDescriptor still cached → same count
        auto result2 = sync(TestPurchaseListRepo::query(query));
        CHECK(result2->size() == count1);  // Stale

        // insert purchase through cross-invalidating repo
        // → triggers InvalidateList<L1PurchaseListInvalidator>
        // → resets ListDescriptor cache
        sync(L1ListInvPurchaseRepo::insert(makeTestPurchase(userId, "Via Repo", 100, "pending")));

        // ListDescriptor cache invalidated → fresh from DB
        auto result3 = sync(TestPurchaseListRepo::query(query));
        // Includes: "Existing Product" + "Direct Insert" + "Via Repo" = 3
        CHECK(result3->size() == count1 + 2);
    }

    SECTION("[list-inv] purchase deletion invalidates purchase ListDescriptor cache") {
        auto userId = insertTestUser("list_del_user", "listdel@test.com", 1000);
        insertTestPurchase(userId, "Product A", 50);
        insertTestPurchase(userId, "Product B", 75);

        TestInternals::resetListCacheState<TestPurchaseListRepo>();

        // Query ListDescriptor
        auto query = makePurchaseQuery(userId);
        auto result1 = sync(TestPurchaseListRepo::query(query));
        auto count1 = result1->size();
        CHECK(count1 == 2);

        // insert one through repo first (so we have an ID to delete)
        auto created = sync(L1ListInvPurchaseRepo::insert(makeTestPurchase(userId, "To Delete", 25, "pending")));
        REQUIRE(created != nullptr);

        // Cache was reset by insert; re-populate
        TestInternals::resetListCacheState<TestPurchaseListRepo>();
        auto result2 = sync(TestPurchaseListRepo::query(query));
        auto count2 = result2->size();
        CHECK(count2 == 3);  // A + B + "To Delete"

        // Delete through repo → triggers ListDescriptor invalidation
        sync(L1ListInvPurchaseRepo::erase(created->id));

        auto result3 = sync(TestPurchaseListRepo::query(query));
        CHECK(result3->size() == 2);  // A + B
    }
}


// #############################################################################
//
//  6. InvalidateListVia — GroupKey dispatch at L1 (3 granularities)
//
// #############################################################################

TEST_CASE("CachedRepo - InvalidateListVia per-page resolver",
          "[integration][db][cached][list-resolver]")
{
    TransactionGuard tx;
    L1MockArticleListRepo::reset();

    auto aliceId = insertTestUser("alice_perpage", "alice_pp@test.com", 1000);
    insertTestArticle("tech", aliceId, "alice_tech_10", 10, true);
    insertTestArticle("tech", aliceId, "alice_tech_20", 20, true);
    insertTestArticle("news", aliceId, "alice_news_100", 100, true);

    SECTION("[list-resolver] per-page resolver sends sort_value for each article") {
        sync(L1PerPagePurchaseRepo::insert(makeTestPurchase(aliceId, "PerPageTest", 100, "completed")));

        // Resolver found 3 articles → 3 invalidateByTarget calls
        REQUIRE(L1MockArticleListRepo::invocations.size() == 3);
        CHECK_FALSE(L1MockArticleListRepo::all_groups_invalidated);

        // Each invocation has a sort_value (per-page granularity)
        for (const auto& inv : L1MockArticleListRepo::invocations) {
            CHECK(inv.sort_value.has_value());
        }

        // Verify categories and sort values
        bool found_tech_10 = false, found_tech_20 = false, found_news_100 = false;
        for (const auto& inv : L1MockArticleListRepo::invocations) {
            if (inv.category == "tech" && inv.sort_value == 10) found_tech_10 = true;
            if (inv.category == "tech" && inv.sort_value == 20) found_tech_20 = true;
            if (inv.category == "news" && inv.sort_value == 100) found_news_100 = true;
        }
        CHECK(found_tech_10);
        CHECK(found_tech_20);
        CHECK(found_news_100);
    }
}

TEST_CASE("CachedRepo - InvalidateListVia per-group resolver",
          "[integration][db][cached][list-resolver][list-granularity]")
{
    TransactionGuard tx;
    L1MockArticleListRepo::reset();

    auto aliceId = insertTestUser("alice_pergroup", "alice_pg@test.com", 1000);
    insertTestArticle("tech", aliceId, "alice_tech_a", 10, true);
    insertTestArticle("tech", aliceId, "alice_tech_b", 20, true);
    insertTestArticle("news", aliceId, "alice_news_a", 100, true);

    SECTION("[list-granularity] per-group resolver sends nullopt sort_value") {
        sync(L1PerGroupPurchaseRepo::insert(makeTestPurchase(aliceId, "PerGroupTest", 100, "completed")));

        // DISTINCT categories: "tech" and "news" → 2 invalidateByTarget calls
        REQUIRE(L1MockArticleListRepo::invocations.size() == 2);
        CHECK_FALSE(L1MockArticleListRepo::all_groups_invalidated);

        // No sort_value on any invocation (per-group)
        for (const auto& inv : L1MockArticleListRepo::invocations) {
            CHECK_FALSE(inv.sort_value.has_value());
        }

        // Verify both categories present
        std::set<std::string> categories;
        for (const auto& inv : L1MockArticleListRepo::invocations) {
            categories.insert(inv.category);
        }
        CHECK(categories.count("tech") == 1);
        CHECK(categories.count("news") == 1);
    }
}

TEST_CASE("CachedRepo - InvalidateListVia full pattern resolver",
          "[integration][db][cached][list-resolver][list-granularity]")
{
    TransactionGuard tx;
    L1MockArticleListRepo::reset();

    auto aliceId = insertTestUser("alice_full", "alice_fp@test.com", 1000);
    insertTestArticle("tech", aliceId, "alice_tech", 10, true);

    SECTION("[list-granularity] full pattern resolver calls invalidateAllListGroups") {
        sync(L1FullPatternPurchaseRepo::insert(makeTestPurchase(aliceId, "FullPatternTest", 100, "completed")));

        // Resolver returned nullopt → invalidateAllListGroups called
        CHECK(L1MockArticleListRepo::all_groups_invalidated);
        // invalidateByTarget NOT called
        CHECK(L1MockArticleListRepo::invocations.empty());
    }
}

TEST_CASE("CachedRepo - InvalidateListVia mixed granularity",
          "[integration][db][cached][list-resolver][list-granularity]")
{
    TransactionGuard tx;
    L1MockArticleListRepo::reset();

    auto aliceId = insertTestUser("alice_mixed", "alice_mx@test.com", 1000);
    insertTestArticle("tech", aliceId, "alice_tech_10", 10, true);
    insertTestArticle("tech", aliceId, "alice_tech_20", 20, true);
    insertTestArticle("news", aliceId, "alice_news_100", 100, true);
    insertTestArticle("news", aliceId, "alice_news_200", 200, true);

    SECTION("[list-granularity] mixed: per-page tech + per-group news") {
        sync(L1MixedPurchaseRepo::insert(makeTestPurchase(aliceId, "MixedTest", 100, "completed")));

        CHECK_FALSE(L1MockArticleListRepo::all_groups_invalidated);

        // Expected invocations:
        //   tech (per-page): sort_value=10, sort_value=20  → 2 calls
        //   news (per-group): no sort_value, deduplicated  → 1 call
        REQUIRE(L1MockArticleListRepo::invocations.size() == 3);

        int tech_perpage = 0, news_pergroup = 0;
        for (const auto& inv : L1MockArticleListRepo::invocations) {
            if (inv.category == "tech" && inv.sort_value.has_value()) {
                tech_perpage++;
            } else if (inv.category == "news" && !inv.sort_value.has_value()) {
                news_pergroup++;
            }
        }

        CHECK(tech_perpage == 2);   // Per-page: 2 tech articles with sort_value
        CHECK(news_pergroup == 1);  // Per-group: 1 deduplicated news without sort_value
    }
}


// #############################################################################
//
//  7. Binary entity CRUD with L1 caching
//
// #############################################################################

TEST_CASE("CachedRepo<TestUser> - binary caching",
          "[integration][db][cached][binary]")
{
    TransactionGuard tx;

    SECTION("[binary] caches binary entity in L1") {
        auto id = insertTestUser("alice", "alice@example.com", 1000);

        // First fetch — DB, cached in L1
        auto result1 = sync(L1TestUserRepo::find(id));
        REQUIRE(result1 != nullptr);
        REQUIRE(result1->username == "alice");
        REQUIRE(result1->balance == 1000);

        // Modify DB directly (bypass cache)
        updateTestUserBalance(id, 999);

        // Second fetch — L1 cached (stale)
        auto result2 = sync(L1TestUserRepo::find(id));
        REQUIRE(result2 != nullptr);
        REQUIRE(result2->username == "alice");
        REQUIRE(result2->balance == 1000);  // Still cached
    }

    SECTION("[binary] patch invalidates L1 binary cache") {
        auto id = insertTestUser("fb_update", "fb_up@example.com", 100);

        // Populate cache
        sync(L1TestUserRepo::find(id));

        // Partial update through repo → invalidates L1
        auto result = sync(L1TestUserRepo::patch(id, set<F::balance>(200)));
        REQUIRE(result != nullptr);
        REQUIRE(result->balance == 200);

        // Fetch again — should reflect update (re-fetched from DB)
        auto fetched = sync(L1TestUserRepo::find(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->balance == 200);
    }
}


// #############################################################################
//
//  8. patch — partial field updates with L1 invalidation
//
// #############################################################################

TEST_CASE("CachedRepo<TestUser> - patch",
          "[integration][db][cached][patch]")
{
    TransactionGuard tx;

    SECTION("[patch] invalidates L1 then re-fetches") {
        auto id = insertTestUser("bob", "bob@example.com", 500);

        // Populate cache
        sync(L1TestUserRepo::find(id));

        // Partial update: only change balance
        auto result = sync(L1TestUserRepo::patch(id, set<F::balance>(777)));

        REQUIRE(result != nullptr);
        REQUIRE(result->balance == 777);
        REQUIRE(result->username == "bob");       // Unchanged
        REQUIRE(result->email == "bob@example.com");

        // Independent fetch confirms DB state
        auto fetched = sync(L1TestUserRepo::find(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->balance == 777);
    }

    SECTION("[patch] updates multiple fields") {
        auto id = insertTestUser("carol", "carol@example.com", 200);

        auto result = sync(L1TestUserRepo::patch(id,
            set<F::balance>(0),
            set<F::username>(std::string("caroline"))));

        REQUIRE(result != nullptr);
        REQUIRE(result->balance == 0);
        REQUIRE(result->username == "caroline");
        REQUIRE(result->email == "carol@example.com");  // Unchanged
    }
}


// #############################################################################
//
//  9. findJson — raw JSON retrieval with L1 caching
//
// #############################################################################

TEST_CASE("CachedRepo - findJson",
          "[integration][db][cached][json]")
{
    TransactionGuard tx;

    // Uses L1TestUserRepo (generated entity with shared_ptr<const string> json)

    SECTION("[json] returns JSON string from L1 cache") {
        auto id = insertTestUser("json_user", "json@example.com", 42);

        auto result = sync(L1TestUserRepo::findJson(id));

        REQUIRE(result != nullptr);
        REQUIRE(result->find("json_user") != std::string::npos);
    }

    SECTION("[json] returns nullptr for non-existent id") {
        auto result = sync(L1TestUserRepo::findJson(999999999));

        REQUIRE(result == nullptr);
    }

    SECTION("[json] second call returns cached JSON") {
        auto id = insertTestUser("cache_json", "cj@example.com", 10);

        // First call — DB fetch, cache entity in L1
        auto result1 = sync(L1TestUserRepo::findJson(id));
        REQUIRE(result1 != nullptr);

        // Modify DB directly
        updateTestUserBalance(id, 999);

        // Second call — L1 cached entity converted to JSON
        auto result2 = sync(L1TestUserRepo::findJson(id));
        REQUIRE(result2 != nullptr);
        REQUIRE(result2->find("cache_json") != std::string::npos);
        // Balance should still be 10 (stale from L1 cache)
        REQUIRE(result2->find("999") == std::string::npos);
    }
}


// #############################################################################
//
//  10. Read-only repository at L1
//
// #############################################################################

TEST_CASE("CachedRepo - read-only",
          "[integration][db][cached][readonly]")
{
    TransactionGuard tx;

    // Compile-time checks
    static_assert(test_local::ReadOnlyL1.read_only == true);
    static_assert(test_local::ReadOnlyL1.cache_level
                  == jcailloux::relais::config::CacheLevel::L1);

    SECTION("[readonly] find works and caches in L1") {
        auto id = insertTestItem("ReadOnly L1", 42);

        auto result1 = sync(ReadOnlyL1TestItemRepo::find(id));
        REQUIRE(result1 != nullptr);
        REQUIRE(result1->name == "ReadOnly L1");

        // Modify DB directly
        updateTestItem(id, "Modified", 999);

        // Should return cached value (stale)
        auto result2 = sync(ReadOnlyL1TestItemRepo::find(id));
        REQUIRE(result2 != nullptr);
        REQUIRE(result2->name == "ReadOnly L1");  // Still cached
    }

    SECTION("[readonly] returns nullptr for non-existent id") {
        auto result = sync(ReadOnlyL1TestItemRepo::find(999999999));
        REQUIRE(result == nullptr);
    }

    // Note: insert(), update(), erase() are compile-time errors on read-only repos.
    // They use `requires (!Cfg.read_only)` and will not compile if called.
}


// #############################################################################
//
//  11. Read-only as cross-invalidation target at L1
//
// #############################################################################

TEST_CASE("CachedRepo - read-only as cross-invalidation target",
          "[integration][db][cached][readonly-inv]")
{
    TransactionGuard tx;

    SECTION("[readonly-inv] purchase creation invalidates read-only user L1 cache") {
        auto userId = insertTestUser("ro_user", "ro@test.com", 1000);

        // Cache user via read-only repo
        auto user1 = sync(ReadOnlyL1TestUserRepo::find(userId));
        REQUIRE(user1 != nullptr);
        REQUIRE(user1->balance == 1000);

        // Modify user in DB directly
        updateTestUserBalance(userId, 500);

        // Still cached (read-only, no writes to trigger invalidation)
        REQUIRE(sync(ReadOnlyL1TestUserRepo::find(userId))->balance == 1000);

        // insert purchase via repo that targets the read-only user cache
        sync(L1ReadOnlyInvPurchaseRepo::insert(makeTestPurchase(userId, "RO Trigger", 50, "pending")));

        // Read-only user cache should be invalidated — fresh data from DB
        auto user2 = sync(ReadOnlyL1TestUserRepo::find(userId));
        REQUIRE(user2 != nullptr);
        REQUIRE(user2->balance == 500);
    }

    SECTION("[readonly-inv] purchase deletion invalidates read-only user L1 cache") {
        auto userId = insertTestUser("ro_del", "rodel@test.com", 2000);

        // insert purchase through repo (need an ID to delete later)
        auto created = sync(L1ReadOnlyInvPurchaseRepo::insert(makeTestPurchase(userId, "To Delete", 100, "pending")));
        REQUIRE(created != nullptr);

        // Cache user
        sync(ReadOnlyL1TestUserRepo::find(userId));

        // Modify user in DB directly
        updateTestUserBalance(userId, 1);

        // Still cached
        REQUIRE(sync(ReadOnlyL1TestUserRepo::find(userId))->balance == 2000);

        // Delete purchase → triggers read-only user invalidation
        sync(L1ReadOnlyInvPurchaseRepo::erase(created->id));

        auto user = sync(ReadOnlyL1TestUserRepo::find(userId));
        REQUIRE(user->balance == 1);
    }
}