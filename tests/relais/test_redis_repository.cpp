/**
 * test_redis_repository.cpp
 *
 * Tests for RedisRepo (L2 - Redis caching on top of database).
 * Uses L2 configurations that resolve to RedisRepo via Repo<>.
 *
 * Progressive complexity:
 *   1. TestItem    — basic CRUD with L2 JSON caching
 *   2. TestUser  — BEVE binary caching, patch
 *   3. JSON access — findAsJson raw string path
 *   4. Invalidation— explicit invalidateRedis control
 *   5. Read-only   — compile-time write enforcement at L2
 *   6. Cross-inv   — Purchase → User (lazy, standard Invalidate<>)
 *   7. Custom inv   — InvalidateVia with async resolver
 *   8. RO target   — read-only repo as cross-invalidation target
 *   9. Lists       — cachedList in Redis (JSON entities)
 *  10. FB Lists    — cachedListAs with binary list entity
 *  11. List inv    — entity writes invalidate cached lists
 *  12. List custom — resolver-based list invalidation
 *  15. Selective   — Lua-based fine-grained list invalidation with SortBounds
 *  16. ListVia     — InvalidateListVia with enriched resolver
 *
 * SECTION naming convention:
 *   [find]      — read by primary key with caching
 *   [insert]        — insert with L2 cache population
 *   [update]        — modify with L2 invalidation/population
 *   [erase]        — delete with L2 invalidation
 *   [patch]      — partial field update
 *   [json]          — JSON string access path
 *   [invalidate]    — explicit cache invalidation
 *   [readonly]      — read-only enforcement
 *   [cross-inv]     — cross-cache invalidation (standard)
 *   [custom-inv]    — custom invalidation (InvalidateVia)
 *   [readonly-inv]  — read-only as invalidation target
 *   [list]          — list caching
 *   [fb-list]       — binary list caching
 *   [list-inv]      — list + cross-invalidation
 *   [list-custom]   — list + custom cross-invalidation
 *   [list-selective] — selective list invalidation with SortBounds headers
 *   [list-resolver]  — InvalidateListVia with enriched resolver
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"

using namespace relais_test;

// #############################################################################
//
//  Local L2 configs, repos, and helpers for Redis-specific tests
//
// #############################################################################

namespace relais_test {

// CacheConfig presets for L2 tests with custom TTL
namespace test_l2 {
using namespace jcailloux::relais::config;
inline constexpr auto RedisShortTTL = Redis.with_l2_ttl(std::chrono::seconds{2});
} // namespace test_l2

// =============================================================================
// L2 repos — RedisRepo provides invalidate() natively
// =============================================================================

/// L2 user repo as cross-invalidation target.
using L2InvTestUserRepo = Repo<TestUserWrapper, "test:user:l2:inv", cfg::Redis>;

/// L2 article repo as cross-invalidation target.
using L2InvTestArticleRepo = Repo<TestArticleWrapper, "test:article:l2:inv", cfg::Redis>;

// =============================================================================
// Standard cross-invalidation: Purchase → User
// =============================================================================

using L2TestPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2",
    cfg::Redis,
    cache::Invalidate<L2InvTestUserRepo, purchaseUserId>>;

// =============================================================================
// Custom cross-invalidation: Purchase → User + Purchase → Articles (via resolver)
// =============================================================================

/**
 * Async resolver: given a user_id, finds all article IDs by that author.
 */
struct UserArticleResolver {
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

/**
 * L2 purchase repo with custom cross-invalidation.
 * When a purchase is created/updated/deleted:
 * - Standard: invalidate the user's Redis cache (direct FK)
 * - Custom:   resolve user_id → article IDs, invalidate each article's Redis cache
 */
using L2CustomTestPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2:custom",
    cfg::Redis,
    cache::Invalidate<L2InvTestUserRepo, purchaseUserId>,
    cache::InvalidateVia<L2InvTestArticleRepo, purchaseUserId, &UserArticleResolver::resolve>>;

// =============================================================================
// Cross-invalidation targeting a read-only repo
// =============================================================================

/**
 * L2 purchase repo whose writes invalidate a read-only user repo.
 */
using L2ReadOnlyInvPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2:readonly:inv",
    cfg::Redis,
    cache::Invalidate<ReadOnlyL2TestUserRepo, purchaseUserId>>;

// =============================================================================
// L2 list repos with custom query methods
// =============================================================================

/**
 * L2 article repo with cached list queries (JSON serialization).
 */
class L2TestArticleListRepo : public Repo<TestArticleWrapper, "test:article:list:l2", cfg::Redis> {
public:
    static io::Task<std::vector<TestArticleWrapper>> getByCategory(
        const std::string& category, int limit = 10)
    {
        co_return co_await cachedList(
            [category, limit]() -> io::Task<std::vector<TestArticleWrapper>> {
                auto result = co_await jcailloux::relais::DbProvider::queryArgs(
                    "SELECT id, category, author_id, title, view_count, is_published, published_at, created_at "
                    "FROM relais_test_articles WHERE category = $1 ORDER BY created_at DESC LIMIT $2",
                    category, limit);
                std::vector<TestArticleWrapper> entities;
                for (size_t i = 0; i < result.rows(); ++i) {
                    if (auto e = entity::generated::TestArticleMapping::fromRow<TestArticleWrapper>(result[i]))
                        entities.push_back(std::move(*e));
                }
                co_return entities;
            },
            "category", category
        );
    }

    static io::Task<bool> invalidateCategoryList(const std::string& category) {
        auto key = makeListCacheKey("category", category);
        co_return co_await jcailloux::relais::cache::RedisCache::invalidate(key);
    }
};

/**
 * L2 article repo with binary list caching (BEVE serialization).
 */
class L2TestArticleListAsRepo : public Repo<TestArticleWrapper, "test:article:listas:l2", cfg::Redis> {
public:
    static io::Task<TestArticleList> getByCategory(
        const std::string& category, int limit = 10)
    {
        co_return co_await cachedListAs<TestArticleList>(
            [category, limit]() -> io::Task<TestArticleList> {
                auto result = co_await jcailloux::relais::DbProvider::queryArgs(
                    "SELECT id, category, author_id, title, view_count, is_published, published_at, created_at "
                    "FROM relais_test_articles WHERE category = $1 ORDER BY created_at DESC LIMIT $2",
                    category, limit);
                co_return TestArticleList::fromRows(result);
            },
            "category", category
        );
    }

    static io::Task<bool> invalidateCategoryList(const std::string& category) {
        auto key = makeListCacheKey("category", category);
        co_return co_await jcailloux::relais::cache::RedisCache::invalidate(key);
    }
};

/**
 * L2 purchase list repo: caches purchase lists by user_id.
 */
class L2TestPurchaseListRepo : public Repo<TestPurchaseWrapper, "test:purchase:list:l2", cfg::Redis> {
public:
    static io::Task<std::vector<TestPurchaseWrapper>> getByUserId(
        int64_t user_id, int limit = 10)
    {
        co_return co_await cachedList(
            [user_id, limit]() -> io::Task<std::vector<TestPurchaseWrapper>> {
                auto result = co_await jcailloux::relais::DbProvider::queryArgs(
                    "SELECT id, user_id, product_name, amount, status, created_at "
                    "FROM relais_test_purchases WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2",
                    user_id, limit);
                std::vector<TestPurchaseWrapper> entities;
                for (size_t i = 0; i < result.rows(); ++i) {
                    if (auto e = entity::generated::TestPurchaseMapping::fromRow<TestPurchaseWrapper>(result[i]))
                        entities.push_back(std::move(*e));
                }
                co_return entities;
            },
            "user", user_id
        );
    }

    static io::Task<bool> invalidateUserList(int64_t user_id) {
        auto key = makeListCacheKey("user", user_id);
        co_return co_await jcailloux::relais::cache::RedisCache::invalidate(key);
    }
};

// =============================================================================
// Virtual invalidator: receives purchase entity notifications, invalidates list
// =============================================================================

/**
 * Virtual cache used as an InvalidateList target.
 * When a purchase entity changes, invalidates the purchase list for that user.
 */
class L2PurchaseListInvalidator {
public:
    static io::Task<void> onEntityModified(
        std::shared_ptr<const TestPurchaseWrapper> entity)
    {
        if (entity) {
            co_await L2TestPurchaseListRepo::invalidateUserList(entity->user_id);
        }
    }
};

/**
 * L2 purchase repo with list cross-invalidation.
 * When a purchase is created/updated/deleted:
 * - Invalidates the user's entity cache
 * - Invalidates the user's purchase list cache
 */
using L2ListInvPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2:listinv",
    cfg::Redis,
    cache::Invalidate<L2InvTestUserRepo, purchaseUserId>,
    cache::InvalidateList<L2PurchaseListInvalidator>>;

// =============================================================================
// Custom list cross-invalidation via resolver
// =============================================================================

/**
 * Resolver: given a user_id, returns the distinct categories of articles
 * authored by that user. Used for indirect list invalidation.
 */
struct PurchaseToArticleCategoryResolver {
    static io::Task<std::vector<std::string>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT DISTINCT category FROM relais_test_articles WHERE author_id = $1",
            user_id);
        std::vector<std::string> categories;
        for (size_t i = 0; i < result.rows(); ++i) {
            categories.push_back(result[i].get<std::string>(0));
        }
        co_return categories;
    }
};

/**
 * Virtual cache: invalidates article list cache for a given category.
 */
class L2ArticleCategoryListInvalidator {
public:
    static io::Task<void> invalidate(const std::string& category) {
        co_await L2TestArticleListRepo::invalidateCategoryList(category);
    }
};

/**
 * L2 purchase repo with custom list cross-invalidation.
 * When a purchase changes:
 * - Standard: invalidate user entity cache
 * - Custom:   resolve user_id → article categories → invalidate article list caches
 */
using L2CustomListPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2:listcustom",
    cfg::Redis,
    cache::Invalidate<L2InvTestUserRepo, purchaseUserId>,
    cache::InvalidateVia<L2ArticleCategoryListInvalidator, purchaseUserId, &PurchaseToArticleCategoryResolver::resolve>>;

// =============================================================================
// L2 tracked list repos with group tracking + pagination
// =============================================================================

/**
 * L2 article repo with tracked list caching (group tracking for O(M) invalidation).
 * Tracks page keys in a Redis SET for efficient group invalidation.
 */
class L2TrackedArticleListRepo : public Repo<TestArticleWrapper, "test:article:tracked:list:l2", cfg::Redis> {
public:
    static io::Task<std::vector<TestArticleWrapper>> getByCategory(
        const std::string& category, int limit = 10, int offset = 0)
    {
        co_return co_await cachedListTracked(
            [category, limit, offset]() -> io::Task<std::vector<TestArticleWrapper>> {
                auto result = co_await jcailloux::relais::DbProvider::queryArgs(
                    "SELECT id, category, author_id, title, view_count, is_published, published_at, created_at "
                    "FROM relais_test_articles WHERE category = $1 ORDER BY view_count DESC LIMIT $2 OFFSET $3",
                    category, limit, offset);
                std::vector<TestArticleWrapper> entities;
                for (size_t i = 0; i < result.rows(); ++i) {
                    if (auto e = entity::generated::TestArticleMapping::fromRow<TestArticleWrapper>(result[i]))
                        entities.push_back(std::move(*e));
                }
                co_return entities;
            },
            limit, offset,
            "category", category
        );
    }

    static io::Task<size_t> invalidateCategoryList(const std::string& category) {
        co_return co_await invalidateListGroup("category", category);
    }
};

/**
 * Same as L2TrackedArticleListRepo but with a short TTL (6s) for timing tests.
 */
class L2TrackedArticleShortTTLRepo : public Repo<TestArticleWrapper, "test:article:tracked:list:l2:short", test_l2::RedisShortTTL> {
public:
    static io::Task<std::vector<TestArticleWrapper>> getByCategory(
        const std::string& category, int limit = 10, int offset = 0)
    {
        co_return co_await cachedListTracked(
            [category, limit, offset]() -> io::Task<std::vector<TestArticleWrapper>> {
                auto result = co_await jcailloux::relais::DbProvider::queryArgs(
                    "SELECT id, category, author_id, title, view_count, is_published, published_at, created_at "
                    "FROM relais_test_articles WHERE category = $1 ORDER BY view_count DESC LIMIT $2 OFFSET $3",
                    category, limit, offset);
                std::vector<TestArticleWrapper> entities;
                for (size_t i = 0; i < result.rows(); ++i) {
                    if (auto e = entity::generated::TestArticleMapping::fromRow<TestArticleWrapper>(result[i]))
                        entities.push_back(std::move(*e));
                }
                co_return entities;
            },
            limit, offset,
            "category", category
        );
    }

    static io::Task<size_t> invalidateCategoryList(const std::string& category) {
        co_return co_await invalidateListGroup("category", category);
    }
};

/**
 * Virtual cache: invalidates tracked article list cache for a given category.
 */
class L2TrackedArticleCategoryInvalidator {
public:
    static io::Task<void> invalidate(const std::string& category) {
        co_await L2TrackedArticleListRepo::invalidateCategoryList(category);
    }
};

/**
 * L2 purchase repo with tracked list cross-invalidation.
 * When a purchase changes:
 * - Standard: invalidate user entity cache
 * - Custom:   resolve user_id → article categories → invalidate tracked article list groups
 */
using L2TrackedListPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2:trackedlist",
    cfg::Redis,
    cache::Invalidate<L2InvTestUserRepo, purchaseUserId>,
    cache::InvalidateVia<L2TrackedArticleCategoryInvalidator, purchaseUserId, &PurchaseToArticleCategoryResolver::resolve>>;

} // namespace relais_test


// #############################################################################
//
//  1. TestItem — basic CRUD with L2 JSON caching
//
// #############################################################################

TEST_CASE("RedisRepo<TestItem> - find", "[integration][db][redis][item]") {
    TransactionGuard tx;

    SECTION("[find] caches result in Redis") {
        auto id = insertTestItem("Redis Cached", 100);

        // First fetch — from database, populated into Redis
        auto result1 = sync(L2TestItemRepo::find(id));
        REQUIRE(result1 != nullptr);
        REQUIRE(result1->name == "Redis Cached");
        REQUIRE(result1->value == 100);

        // Modify directly in DB (bypass repository)
        updateTestItem(id, "Modified In DB", 999);

        // Second fetch — should return cached value from Redis
        auto result2 = sync(L2TestItemRepo::find(id));
        REQUIRE(result2 != nullptr);
        REQUIRE(result2->name == "Redis Cached");
        REQUIRE(result2->value == 100);
    }

    SECTION("[find] returns nullptr for non-existent id") {
        auto result = sync(L2TestItemRepo::find(999999999));
        REQUIRE(result == nullptr);
    }

    SECTION("[find] returns correct entity among multiple") {
        auto id1 = insertTestItem("First", 1);
        auto id2 = insertTestItem("Second", 2);
        auto id3 = insertTestItem("Third", 3);

        auto result = sync(L2TestItemRepo::find(id2));
        REQUIRE(result != nullptr);
        REQUIRE(result->name == "Second");
        REQUIRE(result->value == 2);
    }
}

TEST_CASE("RedisRepo<TestItem> - insert", "[integration][db][redis][item]") {
    TransactionGuard tx;

    SECTION("[insert] inserts entity and populates Redis cache") {
        auto created = sync(L2TestItemRepo::insert(makeTestItem("Created L2", 200)));
        REQUIRE(created != nullptr);
        REQUIRE(created->id > 0);

        // Modify directly in DB
        updateTestItem(created->id, "Modified", 0);

        // Should still get cached value from Redis
        auto fetched = sync(L2TestItemRepo::find(created->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->name == "Created L2");
        REQUIRE(fetched->value == 200);
    }
}

TEST_CASE("RedisRepo<TestItem> - update", "[integration][db][redis][item]") {
    TransactionGuard tx;

    SECTION("[update] invalidates Redis cache (lazy reload)") {
        auto id = insertTestItem("Original", 10);

        // Populate cache
        sync(L2TestItemRepo::find(id));

        // Update through repository
        auto success = sync(L2TestItemRepo::update(id, makeTestItem("Updated", 20, "", true, id)));
        REQUIRE(success == true);

        // Next read should fetch fresh data (cache was invalidated)
        auto fetched = sync(L2TestItemRepo::find(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->name == "Updated");
        REQUIRE(fetched->value == 20);
    }
}

TEST_CASE("RedisRepo<TestItem> - erase", "[integration][db][redis][item]") {
    TransactionGuard tx;

    SECTION("[erase] invalidates Redis cache") {
        auto id = insertTestItem("To erase", 0);

        // Populate cache
        sync(L2TestItemRepo::find(id));

        // Erase through repository
        auto erased = sync(L2TestItemRepo::erase(id));
        REQUIRE(erased.has_value());
        REQUIRE(*erased == 1);

        // Should return nullptr (not from cache)
        auto result = sync(L2TestItemRepo::find(id));
        REQUIRE(result == nullptr);
    }

    SECTION("[erase] returns 0 for non-existent id") {
        auto erased = sync(L2TestItemRepo::erase(999999999));
        REQUIRE(erased.has_value());
        REQUIRE(*erased == 0);
    }
}

// #############################################################################
//
//  2. TestUser — BEVE binary caching, patch
//
// #############################################################################

using jcailloux::relais::wrapper::set;
using F = TestUserWrapper::Field;

TEST_CASE("RedisRepo<TestUser> - binary caching", "[integration][db][redis][binary]") {
    TransactionGuard tx;

    SECTION("[find] caches BEVE entity as binary in Redis") {
        auto id = insertTestUser("alice", "alice@example.com", 1000);

        // First fetch — DB, cached as binary in Redis
        auto result1 = sync(L2TestUserRepo::find(id));
        REQUIRE(result1 != nullptr);
        REQUIRE(result1->username == "alice");
        REQUIRE(result1->balance == 1000);

        // Modify DB directly
        updateTestUserBalance(id, 999);

        // Second fetch — cached binary from Redis
        auto result2 = sync(L2TestUserRepo::find(id));
        REQUIRE(result2 != nullptr);
        REQUIRE(result2->username == "alice");
        REQUIRE(result2->balance == 1000);  // Still cached
    }
}

TEST_CASE("RedisRepo<TestUser> - patch", "[integration][db][redis][patch]") {
    TransactionGuard tx;

    SECTION("[patch] invalidates Redis then re-fetches") {
        auto id = insertTestUser("bob", "bob@example.com", 500);

        // Populate cache
        sync(L2TestUserRepo::find(id));

        // Partial update: only change balance
        auto result = sync(L2TestUserRepo::patch(id, set<F::balance>(777)));

        REQUIRE(result != nullptr);
        REQUIRE(result->balance == 777);
        REQUIRE(result->username == "bob");  // Unchanged
        REQUIRE(result->email == "bob@example.com");

        // Independent fetch confirms DB state
        auto fetched = sync(L2TestUserRepo::find(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->balance == 777);
    }

    SECTION("[patch] updates multiple fields") {
        auto id = insertTestUser("carol", "carol@example.com", 200);

        auto result = sync(L2TestUserRepo::patch(id,
            set<F::balance>(0),
            set<F::username>(std::string("caroline"))));

        REQUIRE(result != nullptr);
        REQUIRE(result->balance == 0);
        REQUIRE(result->username == "caroline");
        REQUIRE(result->email == "carol@example.com");
    }
}

// #############################################################################
//
//  3. findAsJson — raw JSON string path
//
// #############################################################################

TEST_CASE("RedisRepo - findAsJson", "[integration][db][redis][json]") {
    TransactionGuard tx;

    SECTION("[json] returns raw JSON string from Redis") {
        auto id = insertTestItem("JSON Item", 42, std::optional<std::string>{"desc"}, true);

        auto result = sync(L2TestItemRepo::findAsJson(id));

        REQUIRE(result != nullptr);
        REQUIRE(result->find("\"JSON Item\"") != std::string::npos);
    }

    SECTION("[json] returns nullptr for non-existent id") {
        auto result = sync(L2TestItemRepo::findAsJson(999999999));

        REQUIRE(result == nullptr);
    }

    SECTION("[json] second call returns cached JSON") {
        auto id = insertTestItem("Cache JSON", 10);

        // First call — DB fetch, cache as JSON
        auto result1 = sync(L2TestItemRepo::findAsJson(id));
        REQUIRE(result1 != nullptr);

        // Modify DB directly
        updateTestItem(id, "Modified", 999);

        // Second call — cached JSON
        auto result2 = sync(L2TestItemRepo::findAsJson(id));
        REQUIRE(result2 != nullptr);
        REQUIRE(result2->find("\"Cache JSON\"") != std::string::npos);
    }
}

// #############################################################################
//
//  4. Explicit invalidation — invalidateRedis
//
// #############################################################################

TEST_CASE("RedisRepo - explicit invalidation", "[integration][db][redis][invalidate]") {
    TransactionGuard tx;

    SECTION("[invalidate] invalidateRedis clears cached entry") {
        auto id = insertTestItem("To Invalidate L2", 50);

        // Populate cache
        sync(L2TestItemRepo::find(id));

        // Modify in DB
        updateTestItem(id, "Updated After Invalidate", 999);

        // Still cached
        auto cached = sync(L2TestItemRepo::find(id));
        REQUIRE(cached != nullptr);
        REQUIRE(cached->name == "To Invalidate L2");

        // Invalidate
        sync(L2TestItemRepo::invalidateRedis(id));

        // Now should fetch from DB
        auto fresh = sync(L2TestItemRepo::find(id));
        REQUIRE(fresh != nullptr);
        REQUIRE(fresh->name == "Updated After Invalidate");
        REQUIRE(fresh->value == 999);
    }

    SECTION("[invalidate] does not affect other entries") {
        auto id1 = insertTestItem("Keep", 1);
        auto id2 = insertTestItem("Invalidate", 2);

        // Populate both
        sync(L2TestItemRepo::find(id1));
        sync(L2TestItemRepo::find(id2));

        // Modify both in DB
        updateTestItem(id1, "DB Keep", 11);
        updateTestItem(id2, "DB Invalidate", 22);

        // Invalidate only id2
        sync(L2TestItemRepo::invalidateRedis(id2));

        // id1 still cached, id2 fresh
        auto r1 = sync(L2TestItemRepo::find(id1));
        auto r2 = sync(L2TestItemRepo::find(id2));

        REQUIRE(r1->name == "Keep");         // Still cached
        REQUIRE(r2->name == "DB Invalidate"); // Fresh from DB
    }
}

// #############################################################################
//
//  5. Read-only L2 repository
//
// #############################################################################

TEST_CASE("RedisRepo - read-only", "[integration][db][redis][readonly]") {
    TransactionGuard tx;

    // Compile-time checks
    static_assert(test_config::ReadOnlyL2.read_only == true);
    static_assert(test_config::ReadOnlyL2.cache_level == jcailloux::relais::config::CacheLevel::L2);

    SECTION("[readonly] find works and caches in Redis") {
        auto id = insertTestItem("ReadOnly L2", 42);

        auto result1 = sync(ReadOnlyL2TestItemRepo::find(id));
        REQUIRE(result1 != nullptr);
        REQUIRE(result1->name == "ReadOnly L2");

        // Modify DB directly
        updateTestItem(id, "Modified", 999);

        // Should return cached value
        auto result2 = sync(ReadOnlyL2TestItemRepo::find(id));
        REQUIRE(result2 != nullptr);
        REQUIRE(result2->name == "ReadOnly L2");  // Still cached
    }

    SECTION("[readonly] returns nullptr for non-existent id") {
        auto result = sync(ReadOnlyL2TestItemRepo::find(999999999));
        REQUIRE(result == nullptr);
    }

    // Note: insert(), update(), erase() are compile-time errors on read-only repos.
    // They use `requires (!Cfg.read_only)` and will not compile if called.
}

// #############################################################################
//
//  6. Cross-invalidation — Purchase → User (lazy, standard)
//
// #############################################################################

TEST_CASE("RedisRepo - cross-invalidation Purchase → User", "[integration][db][redis][cross-inv]") {
    TransactionGuard tx;

    SECTION("[cross-inv] insert purchase invalidates user Redis cache") {
        auto userId = insertTestUser("inv_user", "inv@test.com", 1000);

        // Cache user in Redis
        auto user1 = sync(L2InvTestUserRepo::find(userId));
        REQUIRE(user1 != nullptr);
        REQUIRE(user1->balance == 1000);

        // Modify user balance directly in DB
        updateTestUserBalance(userId, 500);

        // User still cached
        auto user2 = sync(L2InvTestUserRepo::find(userId));
        REQUIRE(user2->balance == 1000);

        // insert purchase through invalidating repo
        auto created = sync(L2TestPurchaseRepo::insert(makeTestPurchase(userId, "Widget", 100, "pending")));
        REQUIRE(created != nullptr);

        // User cache should be invalidated — next read gets fresh data
        auto user3 = sync(L2InvTestUserRepo::find(userId));
        REQUIRE(user3 != nullptr);
        REQUIRE(user3->balance == 500);  // Fresh from DB
    }

    SECTION("[cross-inv] update purchase invalidates user Redis cache") {
        auto userId = insertTestUser("update_user", "update@test.com", 1000);
        auto purchaseId = insertTestPurchase(userId, "Product", 50);

        // Cache user
        sync(L2InvTestUserRepo::find(userId));

        // Modify user in DB
        updateTestUserBalance(userId, 750);

        // Update purchase through repo
        sync(L2TestPurchaseRepo::update(purchaseId, makeTestPurchase(userId, "Updated Product", 100, "completed", purchaseId)));

        // User cache invalidated
        auto user = sync(L2InvTestUserRepo::find(userId));
        REQUIRE(user != nullptr);
        REQUIRE(user->balance == 750);
    }

    SECTION("[cross-inv] delete purchase invalidates user Redis cache") {
        auto userId = insertTestUser("del_user", "del@test.com", 1000);
        auto purchaseId = insertTestPurchase(userId, "To Delete", 50);

        // Cache user
        sync(L2InvTestUserRepo::find(userId));
        updateTestUserBalance(userId, 200);

        // Delete purchase
        sync(L2TestPurchaseRepo::erase(purchaseId));

        // User cache invalidated
        auto user = sync(L2InvTestUserRepo::find(userId));
        REQUIRE(user != nullptr);
        REQUIRE(user->balance == 200);
    }

    SECTION("[cross-inv] FK change invalidates both old and new user") {
        auto user1Id = insertTestUser("user_one", "one@test.com", 1000);
        auto user2Id = insertTestUser("user_two", "two@test.com", 2000);
        auto purchaseId = insertTestPurchase(user1Id, "Product", 100);

        // Cache both users
        sync(L2InvTestUserRepo::find(user1Id));
        sync(L2InvTestUserRepo::find(user2Id));

        // Modify both in DB
        updateTestUserBalance(user1Id, 111);
        updateTestUserBalance(user2Id, 222);

        // Both still cached
        REQUIRE(sync(L2InvTestUserRepo::find(user1Id))->balance == 1000);
        REQUIRE(sync(L2InvTestUserRepo::find(user2Id))->balance == 2000);

        // Update purchase to change user_id from user1 to user2
        sync(L2TestPurchaseRepo::update(purchaseId, makeTestPurchase(user2Id, "Product", 100, "pending", purchaseId)));

        // Both users should be invalidated
        auto u1 = sync(L2InvTestUserRepo::find(user1Id));
        auto u2 = sync(L2InvTestUserRepo::find(user2Id));
        REQUIRE(u1->balance == 111);
        REQUIRE(u2->balance == 222);
    }
}

// #############################################################################
//
//  7. Custom cross-invalidation — InvalidateVia with resolver
//
// #############################################################################

TEST_CASE("RedisRepo - custom cross-invalidation via resolver", "[integration][db][redis][custom-inv]") {
    TransactionGuard tx;

    SECTION("[custom-inv] purchase creation invalidates user AND related articles") {
        auto userId = insertTestUser("author", "author@test.com", 1000);
        auto articleId = insertTestArticle("tech", userId, "My Article", 42, true);

        // Cache user and article in Redis
        auto user1 = sync(L2InvTestUserRepo::find(userId));
        auto article1 = sync(L2InvTestArticleRepo::find(articleId));
        REQUIRE(user1 != nullptr);
        REQUIRE(article1 != nullptr);

        // Modify both in DB
        updateTestUserBalance(userId, 500);
        updateTestArticle(articleId, "Updated Title", 999);

        // Both still cached
        REQUIRE(sync(L2InvTestUserRepo::find(userId))->balance == 1000);
        REQUIRE(sync(L2InvTestArticleRepo::find(articleId))->title == "My Article");

        // insert purchase — triggers standard + custom invalidation
        sync(L2CustomTestPurchaseRepo::insert(makeTestPurchase(userId, "Trigger", 50, "pending")));

        // User cache invalidated (standard Invalidate<>)
        auto user2 = sync(L2InvTestUserRepo::find(userId));
        REQUIRE(user2->balance == 500);

        // Article cache invalidated (InvalidateVia resolver found this article)
        auto article2 = sync(L2InvTestArticleRepo::find(articleId));
        REQUIRE(article2->title == "Updated Title");
        REQUIRE(article2->view_count == 999);
    }

    SECTION("[custom-inv] resolver with no related articles does not crash") {
        auto userId = insertTestUser("no_articles", "noart@test.com", 100);
        // No articles for this user

        // Cache user
        sync(L2InvTestUserRepo::find(userId));

        // Should not throw — resolver returns empty vector
        auto created = sync(L2CustomTestPurchaseRepo::insert(makeTestPurchase(userId, "Safe Trigger", 10, "pending")));
        REQUIRE(created != nullptr);
    }

    SECTION("[custom-inv] resolver invalidates multiple articles") {
        auto userId = insertTestUser("prolific", "prolific@test.com", 1000);
        auto a1 = insertTestArticle("tech", userId, "Tech 1", 10, true);
        auto a2 = insertTestArticle("news", userId, "News 1", 20, true);
        auto a3 = insertTestArticle("tech", userId, "Tech 2", 30, true);

        // Cache all articles
        sync(L2InvTestArticleRepo::find(a1));
        sync(L2InvTestArticleRepo::find(a2));
        sync(L2InvTestArticleRepo::find(a3));

        // Modify all in DB
        updateTestArticle(a1, "New Tech 1", 100);
        updateTestArticle(a2, "New News 1", 200);
        updateTestArticle(a3, "New Tech 2", 300);

        // insert purchase — invalidates all 3 articles via resolver
        sync(L2CustomTestPurchaseRepo::insert(makeTestPurchase(userId, "Big Trigger", 999, "completed")));

        // All articles should now return fresh data
        REQUIRE(sync(L2InvTestArticleRepo::find(a1))->title == "New Tech 1");
        REQUIRE(sync(L2InvTestArticleRepo::find(a2))->title == "New News 1");
        REQUIRE(sync(L2InvTestArticleRepo::find(a3))->title == "New Tech 2");
    }
}

// #############################################################################
//
//  8. Read-only as cross-invalidation target
//
// #############################################################################

TEST_CASE("RedisRepo - read-only as cross-invalidation target", "[integration][db][redis][readonly-inv]") {
    TransactionGuard tx;

    SECTION("[readonly-inv] purchase creation invalidates read-only user cache") {
        auto userId = insertTestUser("ro_user", "ro@test.com", 1000);

        // Cache user via read-only repo
        auto user1 = sync(ReadOnlyL2TestUserRepo::find(userId));
        REQUIRE(user1 != nullptr);
        REQUIRE(user1->balance == 1000);

        // Modify user in DB
        updateTestUserBalance(userId, 500);

        // Still cached (read-only, no writes to trigger invalidation)
        REQUIRE(sync(ReadOnlyL2TestUserRepo::find(userId))->balance == 1000);

        // insert purchase via repo that targets the read-only user cache
        sync(L2ReadOnlyInvPurchaseRepo::insert(makeTestPurchase(userId, "RO Trigger", 50, "pending")));

        // Read-only user cache should be invalidated — fresh data
        auto user2 = sync(ReadOnlyL2TestUserRepo::find(userId));
        REQUIRE(user2 != nullptr);
        REQUIRE(user2->balance == 500);
    }

    SECTION("[readonly-inv] delete purchase invalidates read-only user cache") {
        auto userId = insertTestUser("ro_del", "rodel@test.com", 2000);
        auto purchaseId = insertTestPurchase(userId, "To Delete", 100);

        // Cache user
        sync(ReadOnlyL2TestUserRepo::find(userId));
        updateTestUserBalance(userId, 1);

        // Delete purchase
        sync(L2ReadOnlyInvPurchaseRepo::erase(purchaseId));

        auto user = sync(ReadOnlyL2TestUserRepo::find(userId));
        REQUIRE(user->balance == 1);
    }
}

// #############################################################################
//
//  9. List caching — cachedList in Redis (JSON entities)
//
// #############################################################################

TEST_CASE("RedisRepo - list caching", "[integration][db][redis][list]") {
    TransactionGuard tx;

    SECTION("[list] query returns articles from database") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10, true);
        insertTestArticle("tech", userId, "Tech 2", 20, true);
        insertTestArticle("news", userId, "News 1", 30, true);

        auto result = sync(L2TestArticleListRepo::getByCategory("tech"));

        REQUIRE(result.size() == 2);
        // Ordered by created_at DESC — last inserted first
        REQUIRE(result[0].title == "Tech 2");
        REQUIRE(result[1].title == "Tech 1");
    }

    SECTION("[list] second query returns cached result") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("cache_cat", userId, "Article 1", 10, true);

        // First query — cache miss, fetches from DB
        auto result1 = sync(L2TestArticleListRepo::getByCategory("cache_cat"));
        REQUIRE(result1.size() == 1);

        // Insert another article directly in DB (bypass repo)
        insertTestArticle("cache_cat", userId, "Article 2", 20, true);

        // Second query — cache hit, should still return 1 article
        auto result2 = sync(L2TestArticleListRepo::getByCategory("cache_cat"));
        REQUIRE(result2.size() == 1);
    }

    SECTION("[list] manual invalidation clears list cache") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("inv_cat", userId, "Article 1", 10, true);

        // Populate cache
        sync(L2TestArticleListRepo::getByCategory("inv_cat"));

        // Insert another article
        insertTestArticle("inv_cat", userId, "Article 2", 20, true);

        // Invalidate list cache
        sync(L2TestArticleListRepo::invalidateCategoryList("inv_cat"));

        // Should now return 2 articles (fresh from DB)
        auto result = sync(L2TestArticleListRepo::getByCategory("inv_cat"));
        REQUIRE(result.size() == 2);
    }

    SECTION("[list] different categories have independent caches") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10, true);
        insertTestArticle("news", userId, "News 1", 20, true);

        auto tech = sync(L2TestArticleListRepo::getByCategory("tech"));
        auto news = sync(L2TestArticleListRepo::getByCategory("news"));

        REQUIRE(tech.size() == 1);
        REQUIRE(news.size() == 1);
        REQUIRE(tech[0].category == "tech");
        REQUIRE(news[0].category == "news");
    }

    SECTION("[list] empty category returns empty list") {
        auto result = sync(L2TestArticleListRepo::getByCategory("nonexistent"));
        REQUIRE(result.empty());
    }
}

// #############################################################################
//
//  10. Binary list caching — cachedListAs
//
// #############################################################################

TEST_CASE("RedisRepo - binary list caching", "[integration][db][redis][fb-list]") {
    TransactionGuard tx;

    SECTION("[fb-list] query returns binary list entity") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("fb_cat", userId, "FB Article 1", 10, true);
        insertTestArticle("fb_cat", userId, "FB Article 2", 20, true);

        auto result = sync(L2TestArticleListAsRepo::getByCategory("fb_cat"));

        REQUIRE(result.size() == 2);
        REQUIRE_FALSE(result.empty());
    }

    SECTION("[fb-list] second query returns cached binary") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("fb_cache", userId, "FB Cached", 10, true);

        // First query
        auto result1 = sync(L2TestArticleListAsRepo::getByCategory("fb_cache"));
        REQUIRE(result1.size() == 1);

        // Insert directly in DB
        insertTestArticle("fb_cache", userId, "FB Not Cached", 20, true);

        // Second query — cached
        auto result2 = sync(L2TestArticleListAsRepo::getByCategory("fb_cache"));
        REQUIRE(result2.size() == 1);
    }

    SECTION("[fb-list] invalidation clears binary list cache") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("fb_inv", userId, "FB Inv 1", 10, true);

        sync(L2TestArticleListAsRepo::getByCategory("fb_inv"));
        insertTestArticle("fb_inv", userId, "FB Inv 2", 20, true);

        sync(L2TestArticleListAsRepo::invalidateCategoryList("fb_inv"));

        auto result = sync(L2TestArticleListAsRepo::getByCategory("fb_inv"));
        REQUIRE(result.size() == 2);
    }

    SECTION("[fb-list] ItemView accessors work on cached list") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("fb_view", userId, "View Test", 42, true);

        auto result = sync(L2TestArticleListAsRepo::getByCategory("fb_view"));
        REQUIRE(result.size() >= 1);

        auto* first = result.firstItem();
        REQUIRE(first != nullptr);
        REQUIRE(first->category == "fb_view");
        REQUIRE(first->author_id == userId);
    }
}

// #############################################################################
//
//  11. List cross-invalidation — entity writes invalidate cached lists
//
// #############################################################################

TEST_CASE("RedisRepo - list cross-invalidation", "[integration][db][redis][list-inv]") {
    TransactionGuard tx;

    SECTION("[list-inv] purchase creation invalidates user's purchase list") {
        auto userId = insertTestUser("buyer", "buyer@test.com", 1000);
        insertTestPurchase(userId, "Existing", 50, "completed");

        // Cache the purchase list for this user
        auto list1 = sync(L2TestPurchaseListRepo::getByUserId(userId));
        REQUIRE(list1.size() == 1);

        // Insert another purchase directly in DB
        insertTestPurchase(userId, "Direct Insert", 100, "pending");

        // Still cached — 1 result
        auto list2 = sync(L2TestPurchaseListRepo::getByUserId(userId));
        REQUIRE(list2.size() == 1);

        // insert purchase through the list-invalidating repo
        sync(L2ListInvPurchaseRepo::insert(makeTestPurchase(userId, "Via Repo", 200, "pending")));

        // List cache invalidated — fresh fetch returns all 3 purchases
        auto list3 = sync(L2TestPurchaseListRepo::getByUserId(userId));
        REQUIRE(list3.size() == 3);
    }

    SECTION("[list-inv] purchase deletion invalidates user's purchase list") {
        auto userId = insertTestUser("buyer", "buyer@test.com", 1000);
        auto p1 = insertTestPurchase(userId, "Keep", 50);
        auto p2 = insertTestPurchase(userId, "Delete", 100);

        // Cache list
        auto list1 = sync(L2TestPurchaseListRepo::getByUserId(userId));
        REQUIRE(list1.size() == 2);

        // Delete through invalidating repo
        sync(L2ListInvPurchaseRepo::erase(p2));

        // List cache invalidated — only 1 purchase left
        auto list2 = sync(L2TestPurchaseListRepo::getByUserId(userId));
        REQUIRE(list2.size() == 1);
        REQUIRE(list2[0].product_name == "Keep");
    }
}

// #############################################################################
//
//  12. List custom cross-invalidation — resolver-based
//
// #############################################################################

TEST_CASE("RedisRepo - list custom cross-invalidation", "[integration][db][redis][list-custom]") {
    TransactionGuard tx;

    SECTION("[list-custom] purchase creation invalidates article list for author's categories") {
        auto userId = insertTestUser("author", "author@test.com", 1000);
        insertTestArticle("tech", userId, "Tech Article", 10, true);
        insertTestArticle("tech", userId, "Tech Article 2", 20, true);

        // Cache article list for "tech" category
        auto list1 = sync(L2TestArticleListRepo::getByCategory("tech"));
        REQUIRE(list1.size() == 2);

        // Insert another tech article directly in DB
        insertTestArticle("tech", userId, "Tech Article 3", 30, true);

        // Still cached — 2 results
        auto list2 = sync(L2TestArticleListRepo::getByCategory("tech"));
        REQUIRE(list2.size() == 2);

        // insert purchase — triggers custom resolver:
        //   user_id → distinct categories ("tech") → invalidate article list
        sync(L2CustomListPurchaseRepo::insert(makeTestPurchase(userId, "List Custom Trigger", 50, "pending")));

        // Article list cache for "tech" should be invalidated
        auto list3 = sync(L2TestArticleListRepo::getByCategory("tech"));
        REQUIRE(list3.size() == 3);
    }

    SECTION("[list-custom] resolver does not affect unrelated categories") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("tech", userId, "Tech", 10, true);

        auto otherUserId = insertTestUser("other", "other@test.com", 0);
        insertTestArticle("news", otherUserId, "News", 20, true);

        // Cache both category lists
        sync(L2TestArticleListRepo::getByCategory("tech"));
        sync(L2TestArticleListRepo::getByCategory("news"));

        // Insert directly in DB
        insertTestArticle("news", otherUserId, "News 2", 30, true);

        // insert purchase for userId (author of "tech", not "news")
        sync(L2CustomListPurchaseRepo::insert(makeTestPurchase(userId, "Selective Trigger", 10, "pending")));

        // "tech" list invalidated (userId is author of tech articles)
        // "news" list NOT invalidated (userId has no news articles)
        auto news = sync(L2TestArticleListRepo::getByCategory("news"));
        REQUIRE(news.size() == 1);  // Still cached (1, not 2)
    }
}

// #############################################################################
//
//  13. Tracked list caching — cachedListTracked with group tracking
//
// #############################################################################

namespace {

// Redis inspection coroutines for tracking data verification

io::Task<int64_t> redisTTL(const std::string& key) {
    auto result = co_await jcailloux::relais::DbProvider::redis("TTL", key);
    co_return result.asInteger();
}

io::Task<int64_t> redisExists(const std::string& key) {
    auto result = co_await jcailloux::relais::DbProvider::redis("EXISTS", key);
    co_return result.asInteger();
}

io::Task<int64_t> redisSCard(const std::string& key) {
    auto result = co_await jcailloux::relais::DbProvider::redis("SCARD", key);
    co_return result.asInteger();
}

io::Task<bool> redisSetContains(const std::string& setKey, const std::string& member) {
    auto result = co_await jcailloux::relais::DbProvider::redis("SISMEMBER", setKey, member);
    co_return result.asInteger() == 1;
}

// Key construction helpers matching makeListGroupKey / cachedListTracked internals

std::string trackedGroupKey(const std::string& repoName, const std::string& category) {
    return repoName + ":list:category:" + category;
}

std::string trackedTrackingKey(const std::string& repoName, const std::string& category) {
    return trackedGroupKey(repoName, category) + ":_keys";
}

std::string trackedPageKey(const std::string& repoName, const std::string& category,
                           int limit, int offset) {
    return trackedGroupKey(repoName, category) + ":limit:" + std::to_string(limit)
                                               + ":offset:" + std::to_string(offset);
}

constexpr auto kTrackedRepoName = "test:article:tracked:list:l2";
constexpr auto kTrackedShortTTLRepoName = "test:article:tracked:list:l2:short";

}  // namespace

TEST_CASE("RedisRepo - tracked list pagination + group invalidation",
          "[integration][db][redis][list-tracked]") {
    TransactionGuard tx;

    SECTION("[list-tracked] all pages of invalidated group refreshed, other groups preserved") {
        auto aliceId = insertTestUser("alice", "alice@test.com", 0);
        auto bobId = insertTestUser("bob", "bob@test.com", 0);

        // Alice writes 7 tech articles (view_count 10-70, sorted DESC: 70,60,...,10)
        for (int i = 1; i <= 7; ++i) {
            insertTestArticle("tech", aliceId, "Tech " + std::to_string(i), i * 10, true);
        }

        // Alice writes 3 science articles
        for (int i = 1; i <= 3; ++i) {
            insertTestArticle("science", aliceId, "Science " + std::to_string(i), i * 100, true);
        }

        // Bob writes 5 news articles (view_count 50-90)
        for (int i = 0; i < 5; ++i) {
            insertTestArticle("news", bobId, "News " + std::to_string(i), 50 + i * 10, true);
        }

        // Cache tech page 1 (limit=5, offset=0) → [70,60,50,40,30]
        auto techP1 = sync(L2TrackedArticleListRepo::getByCategory("tech", 5, 0));
        REQUIRE(techP1.size() == 5);

        // Cache tech page 2 (limit=5, offset=5) → [20,10]
        auto techP2 = sync(L2TrackedArticleListRepo::getByCategory("tech", 5, 5));
        REQUIRE(techP2.size() == 2);

        // Cache news page 1 (limit=5, offset=0) → 5 articles
        auto newsP1 = sync(L2TrackedArticleListRepo::getByCategory("news", 5, 0));
        REQUIRE(newsP1.size() == 5);

        // Insert directly in DB (bypass repo)
        insertTestArticle("tech", aliceId, "Tech New", 45, true);
        insertTestArticle("news", bobId, "News New", 100, true);

        // insert purchase for Alice → resolver → ["tech", "science"]
        // → invalidates "tech" and "science" tracked groups
        sync(L2TrackedListPurchaseRepo::insert(makeTestPurchase(aliceId, "Tracked Trigger", 100, "pending")));

        // tech page 1: invalidated → re-fetch → 5 articles (fresh data)
        auto techP1Fresh = sync(L2TrackedArticleListRepo::getByCategory("tech", 5, 0));
        REQUIRE(techP1Fresh.size() == 5);

        // tech page 2: invalidated → re-fetch → 3 articles (was 2, proves invalidation)
        auto techP2Fresh = sync(L2TrackedArticleListRepo::getByCategory("tech", 5, 5));
        REQUIRE(techP2Fresh.size() == 3);

        // news page 1: preserved (Bob ≠ Alice, resolver doesn't touch "news")
        auto newsP1Cached = sync(L2TrackedArticleListRepo::getByCategory("news", 5, 0));
        REQUIRE(newsP1Cached.size() == 5);  // Still 5, not 6
    }

    SECTION("[list-tracked] resolver invalidates all resolved categories") {
        auto aliceId = insertTestUser("alice", "alice@test.com", 0);
        auto bobId = insertTestUser("bob", "bob@test.com", 0);

        // Alice: 3 tech, 2 science
        for (int i = 1; i <= 3; ++i)
            insertTestArticle("tech", aliceId, "Tech " + std::to_string(i), i * 10, true);
        for (int i = 1; i <= 2; ++i)
            insertTestArticle("science", aliceId, "Science " + std::to_string(i), i * 100, true);

        // Bob: 2 news
        for (int i = 1; i <= 2; ++i)
            insertTestArticle("news", bobId, "News " + std::to_string(i), i * 50, true);

        // Cache all three categories
        auto tech = sync(L2TrackedArticleListRepo::getByCategory("tech"));
        auto science = sync(L2TrackedArticleListRepo::getByCategory("science"));
        auto news = sync(L2TrackedArticleListRepo::getByCategory("news"));
        REQUIRE(tech.size() == 3);
        REQUIRE(science.size() == 2);
        REQUIRE(news.size() == 2);

        // Insert 1 article in each category directly in DB
        insertTestArticle("tech", aliceId, "Tech Extra", 99, true);
        insertTestArticle("science", aliceId, "Science Extra", 999, true);
        insertTestArticle("news", bobId, "News Extra", 999, true);

        // insert purchase for Alice → resolver → ["tech", "science"]
        sync(L2TrackedListPurchaseRepo::insert(makeTestPurchase(aliceId, "Multi Trigger", 50, "pending")));

        // tech: invalidated → 4 (was 3)
        REQUIRE(sync(L2TrackedArticleListRepo::getByCategory("tech")).size() == 4);

        // science: invalidated → 3 (was 2)
        REQUIRE(sync(L2TrackedArticleListRepo::getByCategory("science")).size() == 3);

        // news: preserved → still 2 (not 3)
        REQUIRE(sync(L2TrackedArticleListRepo::getByCategory("news")).size() == 2);
    }

    SECTION("[list-tracked] empty resolver = no invalidation") {
        auto aliceId = insertTestUser("alice", "alice@test.com", 0);
        auto noArticlesId = insertTestUser("nemo", "nemo@test.com", 0);

        // Alice writes 3 tech articles
        for (int i = 1; i <= 3; ++i)
            insertTestArticle("tech", aliceId, "Tech " + std::to_string(i), i * 10, true);

        // Cache tech list
        auto tech = sync(L2TrackedArticleListRepo::getByCategory("tech"));
        REQUIRE(tech.size() == 3);

        // Insert tech article directly in DB
        insertTestArticle("tech", aliceId, "Tech Extra", 99, true);

        // insert purchase for nemo (no articles) → resolver → []
        sync(L2TrackedListPurchaseRepo::insert(makeTestPurchase(noArticlesId, "Empty Resolver Trigger", 10, "pending")));

        // tech: preserved → still 3 (resolver returned nothing)
        REQUIRE(sync(L2TrackedArticleListRepo::getByCategory("tech")).size() == 3);
    }
}

// #############################################################################
//
//  14. Tracked list Redis tracking data inspection
//
// #############################################################################

TEST_CASE("RedisRepo - tracked list Redis tracking data",
          "[integration][db][redis][tracked-data]") {
    TransactionGuard tx;

    SECTION("[tracked-data] tracking set has fixed TTL (not renewed on page addition)") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        for (int i = 0; i < 7; ++i)
            insertTestArticle("ttl_test", userId, "TTL " + std::to_string(i), (i + 1) * 10, true);

        // Cache page 1 → creates tracking set with TTL
        sync(L2TrackedArticleListRepo::getByCategory("ttl_test", 5, 0));

        auto trackKey = trackedTrackingKey(kTrackedRepoName, "ttl_test");
        auto ttl1 = sync(redisTTL(trackKey));
        REQUIRE(ttl1 > 0);

        // Wait 1 second
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Cache page 2 → EXPIRE NX should NOT renew TTL
        sync(L2TrackedArticleListRepo::getByCategory("ttl_test", 5, 5));

        auto ttl2 = sync(redisTTL(trackKey));
        REQUIRE(ttl2 > 0);
        REQUIRE(ttl2 < ttl1);  // TTL decreased, proving it was NOT renewed
    }

    SECTION("[tracked-data] tracking set contains all tracked page keys") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        for (int i = 0; i < 15; ++i)
            insertTestArticle("scard_test", userId, "SC " + std::to_string(i), (i + 1) * 10, true);

        // Cache 3 pages
        sync(L2TrackedArticleListRepo::getByCategory("scard_test", 5, 0));
        sync(L2TrackedArticleListRepo::getByCategory("scard_test", 5, 5));
        sync(L2TrackedArticleListRepo::getByCategory("scard_test", 5, 10));

        auto trackKey = trackedTrackingKey(kTrackedRepoName, "scard_test");

        // Tracking set should have exactly 3 members
        REQUIRE(sync(redisSCard(trackKey)) == 3);

        // Verify each expected page key is in the set
        auto page0 = trackedPageKey(kTrackedRepoName, "scard_test", 5, 0);
        auto page5 = trackedPageKey(kTrackedRepoName, "scard_test", 5, 5);
        auto page10 = trackedPageKey(kTrackedRepoName, "scard_test", 5, 10);

        REQUIRE(sync(redisSetContains(trackKey, page0)));
        REQUIRE(sync(redisSetContains(trackKey, page5)));
        REQUIRE(sync(redisSetContains(trackKey, page10)));
    }

    SECTION("[tracked-data] group invalidation cleans tracking set and all pages") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        for (int i = 0; i < 10; ++i)
            insertTestArticle("clean_test", userId, "Clean " + std::to_string(i), (i + 1) * 10, true);

        // Cache 2 pages
        sync(L2TrackedArticleListRepo::getByCategory("clean_test", 5, 0));
        sync(L2TrackedArticleListRepo::getByCategory("clean_test", 5, 5));

        auto trackKey = trackedTrackingKey(kTrackedRepoName, "clean_test");
        auto page0 = trackedPageKey(kTrackedRepoName, "clean_test", 5, 0);
        auto page5 = trackedPageKey(kTrackedRepoName, "clean_test", 5, 5);

        // Verify all keys exist before invalidation
        REQUIRE(sync(redisExists(trackKey)) == 1);
        REQUIRE(sync(redisExists(page0)) == 1);
        REQUIRE(sync(redisExists(page5)) == 1);

        // Invalidate the group
        auto deleted = sync(L2TrackedArticleListRepo::invalidateCategoryList("clean_test"));
        REQUIRE(deleted == 2);  // 2 page keys deleted

        // All keys should be gone
        REQUIRE(sync(redisExists(trackKey)) == 0);
        REQUIRE(sync(redisExists(page0)) == 0);
        REQUIRE(sync(redisExists(page5)) == 0);

        // Re-query returns fresh data from DB
        auto fresh = sync(L2TrackedArticleListRepo::getByCategory("clean_test", 5, 0));
        REQUIRE(fresh.size() == 5);
    }

    SECTION("[tracked-data] expired tracking set leaves orphaned pages") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        for (int i = 0; i < 10; ++i)
            insertTestArticle("orphan_test", userId, "Orphan " + std::to_string(i), (i + 1) * 10, true);

        // Cache page 1 at t=0 → tracking set TTL = 2s
        sync(L2TrackedArticleShortTTLRepo::getByCategory("orphan_test", 5, 0));

        // Wait 1 second, then cache page 2
        // Page 2 TTL = 2s (expires at t≈3), tracking set EXPIRE NX unchanged (expires at t≈2)
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sync(L2TrackedArticleShortTTLRepo::getByCategory("orphan_test", 5, 5));

        auto trackKey = trackedTrackingKey(kTrackedShortTTLRepoName, "orphan_test");
        auto page5Key = trackedPageKey(kTrackedShortTTLRepoName, "orphan_test", 5, 5);

        // Wait until tracking set expires but page 2 is still alive
        // At t≈2.5, tracking set (TTL 2s from t=0) is expired,
        // page 2 (TTL 2s from t=1) still alive until t≈3
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));

        // Tracking set should be expired
        REQUIRE(sync(redisExists(trackKey)) == 0);

        // Page 2 should still exist (orphaned)
        REQUIRE(sync(redisExists(page5Key)) == 1);

        // invalidateListGroup can't find the pages anymore
        auto deleted = sync(L2TrackedArticleShortTTLRepo::invalidateCategoryList("orphan_test"));
        REQUIRE(deleted == 0);
    }
}

// #############################################################################
//
//  15. Selective list invalidation — Lua-based fine-grained invalidation
//
// #############################################################################

namespace relais_test {

namespace list = jcailloux::relais::cache::list;

/**
 * L2 article repo with tracked list caching + sort bounds header.
 * Uses cachedListTrackedWithHeader to prepend a ListBoundsHeader to each page,
 * enabling fine-grained Lua-based invalidation via invalidateListGroupSelective.
 *
 * Sort: view_count DESC, Pagination: Offset
 */
class L2SelectiveArticleListRepo : public Repo<TestArticleWrapper, "test:article:selective:list:l2", cfg::Redis> {
public:
    static io::Task<std::vector<TestArticleWrapper>> getByCategory(
        const std::string& category, int limit = 5, int offset = 0)
    {
        co_return co_await cachedListTrackedWithHeader(
            [category, limit, offset]() -> io::Task<std::vector<TestArticleWrapper>> {
                auto result = co_await jcailloux::relais::DbProvider::queryArgs(
                    "SELECT id, category, author_id, title, view_count, is_published, published_at, created_at "
                    "FROM relais_test_articles WHERE category = $1 ORDER BY view_count DESC LIMIT $2 OFFSET $3",
                    category, limit, offset);
                std::vector<TestArticleWrapper> entities;
                for (size_t i = 0; i < result.rows(); ++i) {
                    if (auto e = entity::generated::TestArticleMapping::fromRow<TestArticleWrapper>(result[i]))
                        entities.push_back(std::move(*e));
                }
                co_return entities;
            },
            limit, offset,
            // headerBuilder: extract view_count bounds from results
            [](const std::vector<TestArticleWrapper>& results, int lim, int off)
                -> std::optional<list::ListBoundsHeader>
            {
                if (results.empty()) return std::nullopt;

                list::ListBoundsHeader h;
                h.bounds.first_value = results.front().view_count.value_or(0);
                h.bounds.last_value = results.back().view_count.value_or(0);
                h.bounds.is_valid = true;
                h.sort_direction = list::SortDirection::Desc;
                h.pagination_mode = list::PaginationMode::Offset;
                h.is_first_page = (off == 0);
                h.is_incomplete = (static_cast<int>(results.size()) < lim);
                return h;
            },
            "category", category
        );
    }

    // Typed filter key for cross-invalidation via InvalidateListVia
    struct GroupKey {
        std::string category;
    };

    // Translate typed filters → cache invalidation operations
    static io::Task<size_t> invalidateByTarget(
        const GroupKey& gk,
        std::optional<int64_t> sort_value)
    {
        if (sort_value) {
            co_return co_await invalidateListGroupSelective(*sort_value, "category", gk.category);
        } else {
            co_return co_await invalidateListGroup("category", gk.category);
        }
    }

    // Full group invalidation (fallback)
    static io::Task<size_t> invalidateCategoryList(const std::string& category) {
        co_return co_await invalidateListGroup("category", category);
    }

    // Selective invalidation for insert/delete
    static io::Task<size_t> invalidateCategoryListSelective(
        const std::string& category, int64_t entity_sort_val)
    {
        co_return co_await invalidateListGroupSelective(entity_sort_val, "category", category);
    }

    // Selective invalidation for update
    static io::Task<size_t> invalidateCategoryListSelectiveUpdate(
        const std::string& category, int64_t old_sort_val, int64_t new_sort_val)
    {
        co_return co_await invalidateListGroupSelectiveUpdate(
            old_sort_val, new_sort_val, "category", category);
    }
};

} // namespace relais_test

namespace {

constexpr auto kSelectiveRepoName = "test:article:selective:list:l2";

std::string selectiveGroupKey(const std::string& category) {
    return std::string(kSelectiveRepoName) + ":list:category:" + category;
}

std::string selectivePageKey(const std::string& category, int limit, int offset) {
    return selectiveGroupKey(category) + ":limit:" + std::to_string(limit)
                                       + ":offset:" + std::to_string(offset);
}

std::string selectiveTrackingKey(const std::string& category) {
    return selectiveGroupKey(category) + ":_keys";
}

} // namespace

TEST_CASE("RedisRepo - selective list invalidation with SortBounds",
          "[integration][db][redis][list-selective]")
{
    TransactionGuard tx;

    auto aliceId = insertTestUser("alice_sel", "alice_sel@test.com", 0);

    // insert 15 "tech" articles with view_count 10, 20, ..., 150
    for (int vc = 10; vc <= 150; vc += 10) {
        insertTestArticle("tech", aliceId, "tech_" + std::to_string(vc), vc, true);
    }

    SECTION("[selective] insert cascade only from affected segment") {
        // Cache 3 pages (limit=5, offset 0/5/10):
        //   Page 0: [150, 140, 130, 120, 110] → bounds(150, 110) complete
        //   Page 1: [100, 90, 80, 70, 60]     → bounds(100, 60)  complete
        //   Page 2: [50, 40, 30, 20, 10]       → bounds(50, 10)   complete
        auto p0 = sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        auto p1 = sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        auto p2 = sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));
        REQUIRE(p0.size() == 5);
        REQUIRE(p1.size() == 5);
        REQUIRE(p2.size() == 5);

        // Verify tracking set has 3 members
        REQUIRE(sync(redisSCard(selectiveTrackingKey("tech"))) == 3);

        // Selective invalidation: insert entity with view_count=85
        // Offset mode, DESC: cascade = entity_val >= last_value
        //   Page 0: 85 >= 110? NO  → PRESERVED
        //   Page 1: 85 >= 60?  YES → DELETED
        //   Page 2: 85 >= 10?  YES → DELETED
        auto deleted = sync(L2SelectiveArticleListRepo::invalidateCategoryListSelective("tech", 85));
        CHECK(deleted == 2);

        // Page 0 preserved
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        // Pages 1 and 2 deleted
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 0);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 0);

        // Tracking set updated (only page 0 remains)
        CHECK(sync(redisSCard(selectiveTrackingKey("tech"))) == 1);
    }

    SECTION("[selective] delete cascade") {
        // Same setup as above
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));

        // Delete entity with view_count=90 (would be in page 1)
        // Cascade DESC: entity_val >= last_value
        //   Page 0: 90 >= 110? NO  → PRESERVED
        //   Page 1: 90 >= 60?  YES → DELETED
        //   Page 2: 90 >= 10?  YES → DELETED
        auto deleted = sync(L2SelectiveArticleListRepo::invalidateCategoryListSelective("tech", 90));
        CHECK(deleted == 2);

        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 0);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 0);
    }

    SECTION("[selective] update with interval overlap") {
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));

        // Update: view_count 130 → 55
        // Interval: [min(130,55), max(130,55)] = [55, 130]
        // Offset DESC overlap: [page_min, page_max] ∩ [55, 130]
        //   Page 0 [110, 150]: 110 <= 130 AND 55 <= 150 → YES → DELETED
        //   Page 1 [60, 100]:  60 <= 130 AND 55 <= 100  → YES → DELETED
        //   Page 2 [10, 50]:   10 <= 130 AND 55 <= 50   → NO  → PRESERVED
        auto deleted = sync(L2SelectiveArticleListRepo::invalidateCategoryListSelectiveUpdate(
            "tech", 130, 55));
        CHECK(deleted == 2);

        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 0);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 0);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 1);
    }

    SECTION("[selective] update within same segment") {
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));

        // Update: view_count 130 → 120 (both in page 0)
        // Interval: [120, 130]
        //   Page 0 [110, 150]: 110 <= 130 AND 120 <= 150 → YES → DELETED
        //   Page 1 [60, 100]:  60 <= 130 AND 120 <= 100  → NO  → PRESERVED
        //   Page 2 [10, 50]:   10 <= 130 AND 120 <= 50   → NO  → PRESERVED
        auto deleted = sync(L2SelectiveArticleListRepo::invalidateCategoryListSelectiveUpdate(
            "tech", 130, 120));
        CHECK(deleted == 1);

        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 0);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 1);
    }

    SECTION("[selective] unrelated pages preserved") {
        // Cache only pages 0 and 1
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));

        // Selective invalidation with value=105 (in the gap between pages 0 and 1)
        // Cascade DESC: entity_val >= last_value
        //   Page 0: 105 >= 110? NO  → PRESERVED
        //   Page 1: 105 >= 60?  YES → DELETED
        auto deleted = sync(L2SelectiveArticleListRepo::invalidateCategoryListSelective("tech", 105));
        CHECK(deleted == 1);

        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 0);
    }

    SECTION("[selective] backward compat: no header → always invalidated") {
        // Use the old repo (no header) to cache a page
        sync(L2TrackedArticleListRepo::getByCategory("tech", 5, 0));

        auto trackKey = trackedTrackingKey(kTrackedRepoName, "tech");
        REQUIRE(sync(redisSCard(trackKey)) == 1);

        // Selective invalidation on the old repo's group (pages have no header)
        // No magic bytes → conservative → always deleted
        auto deleted = sync(jcailloux::relais::cache::RedisCache::invalidateListGroupSelective(
            trackedGroupKey(kTrackedRepoName, "tech"), 999));
        CHECK(deleted == 1);
    }

    SECTION("[selective] fallback full invalidation") {
        // Cache pages with headers
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));

        // Full invalidation (no sort value) — deletes everything
        auto deleted = sync(L2SelectiveArticleListRepo::invalidateCategoryList("tech"));
        CHECK(deleted == 3);

        // All gone
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 0);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 0);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 0);
        CHECK(sync(redisExists(selectiveTrackingKey("tech"))) == 0);
    }

    SECTION("[selective] different groups independent") {
        // insert 3 "news" articles
        for (int vc = 100; vc <= 300; vc += 100) {
            insertTestArticle("news", aliceId, "news_" + std::to_string(vc), vc, true);
        }

        // Cache tech and news pages
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("news", 5, 0));

        // Selective invalidation on tech only
        auto deleted = sync(L2SelectiveArticleListRepo::invalidateCategoryListSelective("tech", 130));
        CHECK(deleted == 1);

        // Tech page deleted
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 0);

        // News page intact
        CHECK(sync(redisExists(selectivePageKey("news", 5, 0))) == 1);
    }
}

// #############################################################################
//
//  16. InvalidateListVia — enriched resolver for selective list cross-invalidation
//
// #############################################################################

namespace relais_test {

using ArticleGroupKey = L2SelectiveArticleListRepo::GroupKey;
using ArticleListTarget = jcailloux::relais::cache::ListInvalidationTarget<ArticleGroupKey>;

/**
 * Enriched resolver: given a user_id, finds all articles by that author
 * and returns typed ListInvalidationTarget with filter values + sort value (view_count).
 *
 * This enables InvalidateListVia to selectively invalidate only the list pages
 * whose sort range contains the affected article's view_count.
 */
struct PurchaseToArticleSelectiveResolver {
    static io::Task<std::vector<ArticleListTarget>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT category, view_count FROM relais_test_articles WHERE author_id = $1",
            user_id);

        std::vector<ArticleListTarget> targets;
        for (size_t i = 0; i < result.rows(); ++i) {
            ArticleListTarget t;
            t.filters.category = result[i].get<std::string>(0);
            t.sort_value = result[i].get<int64_t>(1);
            targets.push_back(std::move(t));
        }
        co_return targets;
    }
};

/**
 * L2 purchase repo with InvalidateListVia cross-invalidation.
 * When a purchase is created/updated/deleted:
 * - Enriched resolver finds the user's articles with their sort values
 * - Selective invalidation targets only the affected list pages
 */
using L2SelectiveListPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2:selectivelist",
    cfg::Redis,
    cache::InvalidateListVia<
        L2SelectiveArticleListRepo,
        purchaseUserId,
        &PurchaseToArticleSelectiveResolver::resolve
    >>;

} // namespace relais_test


TEST_CASE("RedisRepo - InvalidateListVia enriched resolver",
          "[integration][db][redis][list-resolver]")
{
    TransactionGuard tx;

    // =========================================================================
    // Setup: 15 tech articles (view_count 10..150) by different authors.
    // Alice authors only view_count 10 and 20 (in page 2 range [50, 10]).
    // Bob authors the rest (view_count 30..150, in pages 0 and 1).
    // =========================================================================
    auto aliceId = insertTestUser("alice_resolver", "alice_resolver@test.com", 1000);
    auto bobId = insertTestUser("bob_resolver", "bob_resolver@test.com", 1000);

    // Alice: tech articles at view_count 10, 20
    insertTestArticle("tech", aliceId, "alice_tech_10", 10, true);
    insertTestArticle("tech", aliceId, "alice_tech_20", 20, true);

    // Bob: tech articles at view_count 30, 40, ..., 150 (13 articles)
    for (int vc = 30; vc <= 150; vc += 10) {
        insertTestArticle("tech", bobId, "bob_tech_" + std::to_string(vc), vc, true);
    }

    SECTION("[resolver] enriched resolver triggers selective invalidation") {
        // Cache 3 pages of tech articles (limit=5, DESC by view_count)
        // Page 0: [150, 140, 130, 120, 110] → bounds(150, 110)
        // Page 1: [100, 90, 80, 70, 60]     → bounds(100, 60)
        // Page 2: [50, 40, 30, 20, 10]      → bounds(50, 10)
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));

        // Verify all 3 pages are cached
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 1);

        // insert a purchase for Alice — triggers InvalidateListVia
        // Resolver finds Alice's articles: view_count 10 and 20
        // Cascade check (DESC):
        //   10 >= 110? NO  → page 0 preserved
        //   10 >= 60?  NO  → page 1 preserved
        //   10 >= 10?  YES → page 2 invalidated
        //   20 >= 110? NO  → page 0 still preserved
        //   20 >= 60?  NO  → page 1 still preserved
        //   20 >= 10?  YES → page 2 already invalidated
        auto result = sync(L2SelectiveListPurchaseRepo::insert(makeTestPurchase(aliceId, "Widget", 100, "completed")));
        REQUIRE(result != nullptr);

        // Page 0 (bounds 150, 110): PRESERVED (10, 20 < 110)
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        // Page 1 (bounds 100, 60): PRESERVED (10, 20 < 60)
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 1);
        // Page 2 (bounds 50, 10): INVALIDATED (10 >= 10, 20 >= 10)
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 0);
    }

    SECTION("[resolver] resolver for different categories invalidates independently") {
        // Add 3 news articles by Alice (view_count 100, 200, 300)
        insertTestArticle("news", aliceId, "alice_news_100", 100, true);
        insertTestArticle("news", aliceId, "alice_news_200", 200, true);
        insertTestArticle("news", aliceId, "alice_news_300", 300, true);

        // Cache tech pages 0, 1, 2 and news page 0
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));
        sync(L2SelectiveArticleListRepo::getByCategory("news", 5, 0));

        // insert a purchase for Alice
        // Resolver returns targets for BOTH tech and news:
        //   tech targets: sort_value 10, 20 → cascade hits only page 2
        //   news targets: sort_value 100, 200, 300 → cascade hits page 0
        auto result = sync(L2SelectiveListPurchaseRepo::insert(makeTestPurchase(aliceId, "Gadget", 200, "completed")));
        REQUIRE(result != nullptr);

        // Tech page 0 (bounds 150, 110): PRESERVED
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        // Tech page 1 (bounds 100, 60): PRESERVED
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 1);
        // Tech page 2 (bounds 50, 10): INVALIDATED
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 0);

        // News page 0 (bounds 300, 100): INVALIDATED (100 >= 100)
        CHECK(sync(redisExists(selectivePageKey("news", 5, 0))) == 0);
    }

    SECTION("[resolver] articles outside cached range preserve all pages") {
        // Add 5 extra "science" articles by Charlie (view_count 500..900)
        auto charlieId = insertTestUser("charlie_resolver", "charlie_resolver@test.com", 0);
        for (int vc = 500; vc <= 900; vc += 100) {
            insertTestArticle("science", charlieId, "science_" + std::to_string(vc), vc, true);
        }

        // Cache tech pages
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));

        // insert a purchase for Charlie — resolver finds science articles only
        // Science group key is different from tech → tech pages untouched
        auto result = sync(L2SelectiveListPurchaseRepo::insert(makeTestPurchase(charlieId, "Book", 50, "completed")));
        REQUIRE(result != nullptr);

        // All tech pages preserved (Charlie has no tech articles)
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 1);
    }

    SECTION("[resolver] delete triggers resolver for old entity") {
        // Cache tech pages
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));

        // insert a purchase for Alice, then delete it
        auto created = sync(L2SelectiveListPurchaseRepo::insert(makeTestPurchase(aliceId, "Temp", 50, "pending")));
        REQUIRE(created != nullptr);
        auto purchaseId = created->key();

        // Pages were partially invalidated by insert — re-cache
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));

        // Delete the purchase — triggers resolver with old entity's user_id
        auto deleted = sync(L2SelectiveListPurchaseRepo::erase(purchaseId));
        REQUIRE(deleted.has_value());
        REQUIRE(*deleted == 1);

        // Same pattern: Alice's articles at view_count 10, 20
        // Only page 2 should be invalidated
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 0);
    }
}

// #############################################################################
//
//  17. Three granularities — per-page, per-group, full pattern
//
// #############################################################################

namespace relais_test {

/**
 * Per-group resolver: returns targets WITHOUT sort_value (per-group invalidation).
 * All pages in the targeted group are invalidated.
 */
struct PerGroupResolver {
    static io::Task<std::vector<ArticleListTarget>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT DISTINCT category FROM relais_test_articles WHERE author_id = $1",
            user_id);

        std::vector<ArticleListTarget> targets;
        for (size_t i = 0; i < result.rows(); ++i) {
            ArticleListTarget t;
            t.filters.category = result[i].get<std::string>(0);
            // No sort_value → per-group invalidation
            targets.push_back(std::move(t));
        }
        co_return targets;
    }
};

/**
 * Full pattern resolver: returns nullopt (all list groups invalidated).
 */
struct FullPatternResolver {
    static io::Task<std::optional<std::vector<ArticleListTarget>>> resolve(
        [[maybe_unused]] int64_t user_id)
    {
        co_return std::nullopt;
    }
};

/**
 * Mixed resolver: returns a mix of per-page and per-group targets.
 * - "tech" articles: per-page (with sort_value)
 * - Other categories: per-group (without sort_value)
 */
struct MixedResolver {
    static io::Task<std::vector<ArticleListTarget>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT category, view_count FROM relais_test_articles WHERE author_id = $1",
            user_id);

        std::vector<ArticleListTarget> targets;
        // Track which non-tech categories we've already seen (for dedup)
        std::set<std::string> seen_categories;

        for (size_t i = 0; i < result.rows(); ++i) {
            auto category = result[i].get<std::string>(0);

            if (category == "tech") {
                // Per-page: include sort_value
                ArticleListTarget t;
                t.filters.category = category;
                t.sort_value = result[i].get<int64_t>(1);
                targets.push_back(std::move(t));
            } else if (seen_categories.insert(category).second) {
                // Per-group: no sort_value, one target per category
                ArticleListTarget t;
                t.filters.category = category;
                targets.push_back(std::move(t));
            }
        }
        co_return targets;
    }
};

/**
 * Purchase repos for each granularity test.
 */
using L2PerGroupPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2:pergroup",
    cfg::Redis,
    cache::InvalidateListVia<L2SelectiveArticleListRepo, purchaseUserId, &PerGroupResolver::resolve>>;

using L2FullPatternPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2:fullpattern",
    cfg::Redis,
    cache::InvalidateListVia<L2SelectiveArticleListRepo, purchaseUserId, &FullPatternResolver::resolve>>;

using L2MixedPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l2:mixed",
    cfg::Redis,
    cache::InvalidateListVia<L2SelectiveArticleListRepo, purchaseUserId, &MixedResolver::resolve>>;

} // namespace relais_test


TEST_CASE("RedisRepo - InvalidateListVia per-group invalidation",
          "[integration][db][redis][list-granularity]")
{
    TransactionGuard tx;

    auto aliceId = insertTestUser("alice_pergroup", "alice_pergroup@test.com", 1000);

    // insert 15 "tech" articles (view_count 10..150)
    for (int vc = 10; vc <= 150; vc += 10) {
        insertTestArticle("tech", aliceId, "tech_pg_" + std::to_string(vc), vc, true);
    }

    // insert 3 "news" articles
    for (int vc = 100; vc <= 300; vc += 100) {
        insertTestArticle("news", aliceId, "news_pg_" + std::to_string(vc), vc, true);
    }

    SECTION("[granularity] per-group deletes all pages in targeted groups") {
        // Cache 3 tech pages + 1 news page
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));
        sync(L2SelectiveArticleListRepo::getByCategory("news", 5, 0));

        // Verify all cached
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 1);
        CHECK(sync(redisExists(selectivePageKey("news", 5, 0))) == 1);

        // insert purchase → PerGroupResolver returns targets for "tech" and "news"
        // without sort_value → all pages in those groups are invalidated
        auto result = sync(L2PerGroupPurchaseRepo::insert(makeTestPurchase(aliceId, "PerGroupTest", 100, "completed")));
        REQUIRE(result != nullptr);

        // All tech pages deleted (per-group)
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 0);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 0);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 0);

        // News page also deleted (Alice has news articles too)
        CHECK(sync(redisExists(selectivePageKey("news", 5, 0))) == 0);
    }

    SECTION("[granularity] per-group preserves unrelated groups") {
        auto bobId = insertTestUser("bob_pergroup", "bob_pergroup@test.com", 0);
        // Bob has only "science" articles
        insertTestArticle("science", bobId, "sci_1", 100, true);

        // Cache tech and science pages
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("science", 5, 0));

        // insert purchase for Bob → PerGroupResolver returns only "science"
        auto result = sync(L2PerGroupPurchaseRepo::insert(makeTestPurchase(bobId, "SciTest", 50, "completed")));
        REQUIRE(result != nullptr);

        // Tech preserved (Bob has no tech articles)
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        // Science deleted
        CHECK(sync(redisExists(selectivePageKey("science", 5, 0))) == 0);
    }
}

TEST_CASE("RedisRepo - InvalidateListVia full pattern invalidation",
          "[integration][db][redis][list-granularity]")
{
    TransactionGuard tx;

    auto aliceId = insertTestUser("alice_fullpat", "alice_fullpat@test.com", 0);

    // insert articles in two categories
    for (int vc = 10; vc <= 50; vc += 10) {
        insertTestArticle("tech", aliceId, "tech_fp_" + std::to_string(vc), vc, true);
    }
    for (int vc = 100; vc <= 300; vc += 100) {
        insertTestArticle("news", aliceId, "news_fp_" + std::to_string(vc), vc, true);
    }

    SECTION("[granularity] full pattern deletes all list groups") {
        // Cache tech and news pages
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("news", 5, 0));

        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        CHECK(sync(redisExists(selectivePageKey("news", 5, 0))) == 1);
        CHECK(sync(redisExists(selectiveTrackingKey("tech"))) == 1);
        CHECK(sync(redisExists(selectiveTrackingKey("news"))) == 1);

        // insert purchase → FullPatternResolver returns nullopt
        // → invalidateAllListGroups() → SCAN "test:article:selective:list:l2:list:*"
        auto result = sync(L2FullPatternPurchaseRepo::insert(makeTestPurchase(aliceId, "FullPatternTest", 100, "completed")));
        REQUIRE(result != nullptr);

        // All pages AND tracking sets deleted
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 0);
        CHECK(sync(redisExists(selectivePageKey("news", 5, 0))) == 0);
        CHECK(sync(redisExists(selectiveTrackingKey("tech"))) == 0);
        CHECK(sync(redisExists(selectiveTrackingKey("news"))) == 0);
    }
}

TEST_CASE("RedisRepo - InvalidateListVia mixed granularity",
          "[integration][db][redis][list-granularity]")
{
    TransactionGuard tx;

    auto aliceId = insertTestUser("alice_mixed", "alice_mixed@test.com", 1000);

    // Alice has tech articles at view_count 10, 20 (in page 2 range [50, 10])
    insertTestArticle("tech", aliceId, "alice_mixed_10", 10, true);
    insertTestArticle("tech", aliceId, "alice_mixed_20", 20, true);

    // Bob has remaining tech articles
    auto bobId = insertTestUser("bob_mixed", "bob_mixed@test.com", 0);
    for (int vc = 30; vc <= 150; vc += 10) {
        insertTestArticle("tech", bobId, "bob_mixed_" + std::to_string(vc), vc, true);
    }

    // Alice also has news articles (for per-group invalidation)
    insertTestArticle("news", aliceId, "alice_mixed_news_100", 100, true);
    insertTestArticle("news", aliceId, "alice_mixed_news_200", 200, true);

    SECTION("[granularity] mixed: per-page tech + per-group news") {
        // Cache 3 tech pages + 1 news page
        // Page 0: [150..110] → bounds(150, 110)
        // Page 1: [100..60]  → bounds(100, 60)
        // Page 2: [50..10]   → bounds(50, 10)
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 0));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 5));
        sync(L2SelectiveArticleListRepo::getByCategory("tech", 5, 10));
        sync(L2SelectiveArticleListRepo::getByCategory("news", 5, 0));

        // Verify all cached
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 1);
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 1);
        CHECK(sync(redisExists(selectivePageKey("news", 5, 0))) == 1);

        // insert purchase → MixedResolver returns:
        //   tech targets (per-page): sort_value=10, sort_value=20
        //     → 10 >= 110? NO  → page 0 preserved
        //     → 10 >= 60?  NO  → page 1 preserved
        //     → 10 >= 10?  YES → page 2 invalidated
        //     → 20 same cascade pattern
        //   news target (per-group): no sort_value
        //     → all news pages deleted
        auto result = sync(L2MixedPurchaseRepo::insert(makeTestPurchase(aliceId, "MixedTest", 100, "completed")));
        REQUIRE(result != nullptr);

        // Tech page 0 (bounds 150, 110): PRESERVED (per-page, 10,20 < 110)
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 0))) == 1);
        // Tech page 1 (bounds 100, 60): PRESERVED (per-page, 10,20 < 60)
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 5))) == 1);
        // Tech page 2 (bounds 50, 10): INVALIDATED (per-page, 10 >= 10)
        CHECK(sync(redisExists(selectivePageKey("tech", 5, 10))) == 0);

        // News page 0: INVALIDATED (per-group, all pages deleted)
        CHECK(sync(redisExists(selectivePageKey("news", 5, 0))) == 0);
    }
}
