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


// #############################################################################
//
//  4. L1+L2 notify* path (synchronous invalidation)
//
// #############################################################################

TEST_CASE("[DeclList L1+L2] notify* path (L1 sync + L2 invalidation)",
          "[integration][db][list][full-cache][notify]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<FullCacheArticleListRepo>();

    SECTION("[notify] notifyCreated invalidates both L1 and L2") {
        auto userId = insertTestUser("author", "author@notify.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate L1+L2
        auto r1 = sync(FullCacheArticleListRepo::query(makeFullArticleQuery("tech")));
        REQUIRE(r1->size() == 1);

        // Insert sentinel in DB
        insertTestArticle("tech", userId, "Sentinel", 20);

        // Verify cache returns stale
        REQUIRE(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")))->size() == 1);

        // notifyCreatedSync → invalidates both L1 and L2
        auto entity = makeTestArticle("tech", userId, "Notified", 30);
        TestInternals::notifyCreatedSync<FullCacheArticleListRepo>(entity);

        // Both layers invalidated → DB hit → sentinel visible
        auto r2 = sync(FullCacheArticleListRepo::query(makeFullArticleQuery("tech")));
        CHECK(r2->size() == 2);
    }

    SECTION("[notify] notifyUpdated invalidates both layers") {
        auto userId = insertTestUser("author", "author@notify.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Before", 10);

        // Populate L1+L2
        sync(FullCacheArticleListRepo::query(makeFullArticleQuery("tech")));

        // Update DB directly
        updateTestArticle(articleId, "After", 20);

        // notifyUpdatedSync
        auto oldEntity = makeTestArticle("tech", userId, "Before", 10, false, articleId);
        auto newEntity = makeTestArticle("tech", userId, "After", 20, false, articleId);
        TestInternals::notifyUpdatedSync<FullCacheArticleListRepo>(oldEntity, newEntity);

        // Both layers invalidated → DB hit
        auto result = sync(FullCacheArticleListRepo::query(makeFullArticleQuery("tech")));
        CHECK(result->size() == 1);
        CHECK(result->items.front().title == "After");
    }

    SECTION("[notify] notifyDeleted invalidates both layers") {
        auto userId = insertTestUser("author", "author@notify.com", 0);
        auto articleId = insertTestArticle("tech", userId, "To Delete", 10);
        insertTestArticle("tech", userId, "To Keep", 20);

        // Populate L1+L2
        REQUIRE(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")))->size() == 2);

        // Delete from DB
        deleteTestArticle(articleId);

        // notifyDeletedSync
        auto entity = makeTestArticle("tech", userId, "To Delete", 10, false, articleId);
        TestInternals::notifyDeletedSync<FullCacheArticleListRepo>(entity);

        // Both layers invalidated
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")))->size() == 1);
    }
}


// #############################################################################
//
//  5. L1+L2 Filter-based selective invalidation
//
// #############################################################################

TEST_CASE("[DeclList L1+L2] Filter-based selective invalidation",
          "[integration][db][list][full-cache][filter-match]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<FullCacheArticleListRepo>();

    SECTION("[filter-match] insert tech: tech supprimé, news conservé") {
        auto userId = insertTestUser("author", "author@fm.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);

        // Populate both groups in L1+L2
        sync(FullCacheArticleListRepo::query(makeFullArticleQuery("tech")));
        sync(FullCacheArticleListRepo::query(makeFullArticleQuery("news")));

        // Insert sentinels
        insertTestArticle("tech", userId, "Tech Sentinel", 30);
        insertTestArticle("news", userId, "News Sentinel", 40);

        // notifyCreatedSync with tech entity → selective invalidation
        auto entity = makeTestArticle("tech", userId, "Tech Notify", 50);
        TestInternals::notifyCreatedSync<FullCacheArticleListRepo>(entity);

        // tech SUPPRIMÉ: DB hit → original + sentinel
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")))->size() == 2);
        // news CONSERVÉ: L1 cache hit → sentinel invisible
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("news")))->size() == 1);
    }

    SECTION("[filter-match] update tech→news: tech+news supprimés, sports conservé") {
        auto userId = insertTestUser("author", "author@fm.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Migrating", 10);
        insertTestArticle("news", userId, "News 1", 20);
        insertTestArticle("sports", userId, "Sports 1", 30);

        // Populate 3 groups
        sync(FullCacheArticleListRepo::query(makeFullArticleQuery("tech")));
        sync(FullCacheArticleListRepo::query(makeFullArticleQuery("news")));
        sync(FullCacheArticleListRepo::query(makeFullArticleQuery("sports")));

        // Insert sentinels
        insertTestArticle("tech", userId, "Tech Sentinel", 40);
        insertTestArticle("news", userId, "News Sentinel", 50);
        insertTestArticle("sports", userId, "Sports Sentinel", 60);

        // Update DB
        updateTestArticleCategory(articleId, "news");

        // notifyUpdatedSync: old=tech, new=news
        auto oldEntity = makeTestArticle("tech", userId, "Migrating", 10, false, articleId);
        auto newEntity = makeTestArticle("news", userId, "Migrating", 10, false, articleId);
        TestInternals::notifyUpdatedSync<FullCacheArticleListRepo>(oldEntity, newEntity);

        // tech SUPPRIMÉ (old group): sentinel only
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")))->size() == 1);
        // news SUPPRIMÉ (new group): existing + migrated + sentinel
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("news")))->size() == 3);
        // sports CONSERVÉ
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("sports")))->size() == 1);
    }
}


// #############################################################################
//
//  6. L2 hit repopulates L1
//
// #############################################################################

TEST_CASE("[DeclList L1+L2] L2 hit repopulates L1",
          "[integration][db][list][full-cache][l2-to-l1]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<FullCacheArticleListRepo>();

    SECTION("[l2-to-l1] L1 miss falls through to L2 hit and repopulates L1") {
        auto userId = insertTestUser("author", "author@l2tol1.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("tech", userId, "Tech 2", 20);

        // 1st query → populates both L1 and L2
        auto r1 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r1->size() == 2);

        // Clear L1 only (shardmap + ModificationTracker + get_counter). L2 intact.
        TestInternals::resetListCacheState<FullCacheArticleListRepo>();

        // Insert sentinel directly in DB (bypasses repo, no invalidation)
        insertTestArticle("tech", userId, "Sentinel", 30);

        // 2nd query → L1 miss → L2 hit → returns 2 (sentinel invisible, stale from L2)
        auto r2 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        CHECK(r2->size() == 2);

        // 3rd query → L1 hit → still returns 2 (proves L1 was repopulated from L2)
        auto r3 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        CHECK(r3->size() == 2);
    }
}


// #############################################################################
//
//  7. Insertion invalidation edge cases (L1+L2)
//
// #############################################################################

namespace {
namespace list_ns = jcailloux::relais::cache::list;

using FullArticleDecl = FullCacheArticleListRepo::ListDescriptorType;
using FullArticleDescQuery = decl::ListDescriptorQuery<FullArticleDecl>;

/// Build a sorted query for L1+L2 articles (view_count DESC).
static FullArticleDescQuery makeFullViewCountQuery(
    std::string_view category, uint16_t limit)
{
    FullArticleDescQuery q;
    q.limit = limit;
    q.filters.get<0>() = category;
    q.sort = list_ns::SortSpec<size_t>{1, list_ns::SortDirection::Desc};
    q.group_key = decl::groupCacheKey<FullArticleDecl>(q);
    q.cache_key = decl::cacheKey<FullArticleDecl>(q);
    return q;
}
} // anonymous namespace

TEST_CASE("[DeclList L1+L2] Insertion invalidation edge cases",
          "[integration][db][list][full-cache][edge-invalidation]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<FullCacheArticleListRepo>();

    SECTION("[edge] insert when no list queries are cached (L1+L2)") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // No query executed — no cache populated
        auto newArticle = makeTestArticle("tech", userId, "Tech 2", 20);
        auto created = sync(FullCacheArticleListRepo::insert(newArticle));
        CHECK(created != nullptr);

        auto result = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        CHECK(result->size() == 2);
    }

    SECTION("[edge] rapid sequential inserts invalidate both L1 and L2") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate L1+L2
        auto r1 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r1->size() == 1);

        // 3 rapid sequential inserts via repo
        sync(FullCacheArticleListRepo::insert(
            makeTestArticle("tech", userId, "Tech 2", 20)));
        sync(FullCacheArticleListRepo::insert(
            makeTestArticle("tech", userId, "Tech 3", 30)));
        sync(FullCacheArticleListRepo::insert(
            makeTestArticle("tech", userId, "Tech 4", 40)));

        // Each insert invalidated both L1 and L2 → query hits DB
        auto result = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        CHECK(result->size() == 4);
    }

    SECTION("[edge] insert into empty cached list (L1+L2)") {
        auto userId = insertTestUser("author", "author@edge.com", 0);

        // Query empty category
        auto r1 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("empty_cat")));
        REQUIRE(r1->size() == 0);

        // Insert sentinel directly
        insertTestArticle("empty_cat", userId, "First", 10);

        // Cache returns stale empty result
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("empty_cat")))->size() == 0);

        // notifyCreated → empty page (is_valid=false) → always invalidated
        auto entity = makeTestArticle("empty_cat", userId, "Notify", 20);
        TestInternals::notifyCreatedSync<FullCacheArticleListRepo>(entity);

        // Both L1 and L2 invalidated → DB hit → sentinel visible
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("empty_cat")))->size() == 1);
    }

    SECTION("[edge] L1 and L2 independence verifiable via insert") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        // Populate L1+L2
        auto r1 = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        REQUIRE(r1->size() == 1);

        // Insert sentinel directly in DB (no repo, no invalidation)
        insertTestArticle("tech", userId, "Sentinel", 20);

        // L1 hit → stale
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")))->size() == 1);

        // Clear L1 only, L2 intact
        TestInternals::resetListCacheState<FullCacheArticleListRepo>();

        // L1 miss → L2 hit → still stale (proves L2 was NOT invalidated)
        CHECK(sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")))->size() == 1);

        // Insert via repo → triggers invalidation of both L1 and L2
        sync(FullCacheArticleListRepo::insert(
            makeTestArticle("tech", userId, "Tech 3", 30)));

        // DB hit → sees all 3 (original + sentinel + repo-inserted)
        auto result = sync(FullCacheArticleListRepo::query(
            makeFullArticleQuery("tech")));
        CHECK(result->size() == 3);
    }

    SECTION("[edge] L1 incomplete page preserved when sort value out of range") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A80", 80);
        insertTestArticle("tech", userId, "A60", 60);

        // Page 1 [100, 80]: first, complete (limit=2)
        auto q1 = makeFullViewCountQuery("tech", 2);
        auto p1 = sync(FullCacheArticleListRepo::query(q1));
        REQUIRE(p1->size() == 2);

        // Page 2 [60] via cursor: NOT first, incomplete (1 < limit 2)
        auto q2 = makeFullViewCountQuery("tech", 2);
        q2.cursor = list_ns::Cursor::decode(std::string(p1->cursor())).value();
        q2.cache_key = decl::cacheKey<FullArticleDecl>(q2);
        auto p2 = sync(FullCacheArticleListRepo::query(q2));
        REQUIRE(p2->size() == 1);
        REQUIRE(p2->items[0].view_count.value() == 60);

        // Insert sentinel in DB (not through repo)
        insertTestArticle("tech", userId, "Sentinel999", 999);

        // notifyCreated with sort value 999 (far above all ranges, DESC)
        // L1 page 2 (cursor, incomplete, [60]): isValueInRange(999, false, true, true)
        //   → 999 <= 60? NO → L1 page 2 PRESERVED (lazy invalidation does range check)
        auto entity = makeTestArticle("tech", userId, "E999", 999);
        TestInternals::notifyCreatedSync<FullCacheArticleListRepo>(entity);

        // Page 2 PRESERVED at L1: cache hit → stale (sentinel not visible)
        auto p2_cached = sync(FullCacheArticleListRepo::query(q2));
        CHECK(p2_cached->size() == 1);
        CHECK(p2_cached->items[0].view_count.value() == 60);
    }
}
