/**
 * test_decl_list_full.cpp
 *
 * Tests for declarative list invalidation (L1): how writes reach cached pages.
 * Complements test_decl_list_cache.cpp, which covers queries and the
 * ModificationTracker lifecycle.
 *
 * Covers:
 *   1. Write paths (insert / update / erase) invalidate the list
 *   2. Entity and list on the same repo (cross-interaction)
 *   3. Filter-based selective invalidation (EQ on string and int64, combined,
 *      partial and absent filters, update moving an entity between groups)
 *   4. Sort-range invalidation on cursor pages
 *   5. Insertion invalidation edge cases (no cached page, empty page, exact
 *      boundary, offset vs cursor incomplete page, null sort value)
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"
using namespace relais_test;

namespace {

namespace decl = jcailloux::relais::list::spec;
namespace list_ns = jcailloux::relais::list;

using ListRepo = TestArticleListRepo;
using ArticleDecl = ListRepo::ListDescriptorType;

/// Articles of a category sorted by view_count DESC, offset pagination.
ListRepo::ListQuery viewCountQuery(std::string_view category, uint16_t limit,
                                   uint32_t offset = 0) {
    decl::ListQueryParams<ArticleDecl> q;
    q.limit = limit;
    q.offset = offset;
    q.filters.get<"category">() = std::string(category);
    q.sort = list_ns::SortSpec<size_t>{1, list_ns::SortDirection::Desc};
    return decl::seal<ArticleDecl>(std::move(q));
}

/// The page following `page` of `category` (same sort and limit), by cursor.
template<typename Page>
ListRepo::ListQuery viewCountQueryAfter(std::string_view category, uint16_t limit,
                                        const Page& page) {
    auto params = viewCountQuery(category, limit).params();
    params.cursor = ListRepo::Cursor::decode(std::string(page->cursor())).value();
    return decl::seal<ArticleDecl>(std::move(params));
}

size_t countOf(std::optional<std::string> category = std::nullopt,
               std::optional<int64_t> author_id = std::nullopt) {
    return sync(ListRepo::query(makeArticleQuery(std::move(category), author_id)))->size();
}

} // anonymous namespace


// #############################################################################
//
//  1. Write paths invalidate the list
//
// #############################################################################

TEST_CASE("[DeclList] Write paths invalidate the list",
          "[integration][db][list][invalidation]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<ListRepo>();

    SECTION("[invalidation] insert invalidates the list") {
        auto userId = insertTestUser("author", "author@write.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        REQUIRE(countOf("tech") == 1);

        sync(ListRepo::insert(makeTestArticle("tech", userId, "Tech 2", 20)));

        CHECK(countOf("tech") == 2);
    }

    SECTION("[invalidation] update invalidates the list") {
        auto userId = insertTestUser("author", "author@write.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Before", 10);

        sync(ListRepo::query(makeArticleQuery("tech")));

        auto updated = makeTestArticle("tech", userId, "After", 20, false, articleId);
        sync(ListRepo::update(articleId, updated));

        auto result = sync(ListRepo::query(makeArticleQuery("tech")));
        REQUIRE(result->size() == 1);
        CHECK(result->items.front().title == "After");
    }

    SECTION("[invalidation] erase invalidates the list") {
        auto userId = insertTestUser("author", "author@write.com", 0);
        auto articleId = insertTestArticle("tech", userId, "To Delete", 10);
        insertTestArticle("tech", userId, "To Keep", 20);

        REQUIRE(countOf("tech") == 2);

        sync(ListRepo::erase(articleId));

        CHECK(countOf("tech") == 1);
    }
}


// #############################################################################
//
//  2. Entity and list on the same repo
//
// #############################################################################

TEST_CASE("[DeclList] Entity and list on the same repo",
          "[integration][db][list][cross-inv]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<ListRepo>();

    SECTION("[cross-inv] entity update invalidates the list and refreshes the entity") {
        auto userId = insertTestUser("author", "author@same.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Original", 10);

        // Cache both entity and list
        sync(ListRepo::find(articleId));
        sync(ListRepo::query(makeArticleQuery("tech")));

        auto updated = makeTestArticle("tech", userId, "Updated", 20, false, articleId);
        sync(ListRepo::update(articleId, updated));

        auto entity = sync(ListRepo::find(articleId));
        REQUIRE(entity != nullptr);
        CHECK(entity->title == "Updated");

        auto list = sync(ListRepo::query(makeArticleQuery("tech")));
        REQUIRE(list->size() == 1);
        CHECK(list->items.front().title == "Updated");
    }

    SECTION("[cross-inv] the list is cached again after a write invalidated it") {
        auto userId = insertTestUser("author", "author@same.com", 0);
        insertTestArticle("tech", userId, "Article 1", 10);

        REQUIRE(countOf("tech") == 1);

        auto created = sync(ListRepo::insert(makeTestArticle("tech", userId, "Article 2", 20)));
        REQUIRE(created != nullptr);

        // Re-fetched from the database, then cached
        REQUIRE(countOf("tech") == 2);

        // A direct insert is not seen: the page is cached again
        insertTestArticle("tech", userId, "Article 3", 30);
        CHECK(countOf("tech") == 2);
    }
}


// #############################################################################
//
//  3. Filter-based selective invalidation
//
// #############################################################################
//
// Each section caches pages, inserts sentinel rows directly in the database
// (invisible to a cached page), then notifies a modification: an invalidated
// page is re-fetched and shows its sentinel, a preserved page does not.

TEST_CASE("[DeclList] Filter-based selective invalidation",
          "[integration][db][list][filter-match]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<ListRepo>();

    SECTION("[filter-match] string EQ: insert tech invalidates tech, keeps news and sports") {
        auto userId = insertTestUser("author", "author@fm.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);
        insertTestArticle("news", userId, "News 1", 20);
        insertTestArticle("sports", userId, "Sports 1", 30);

        countOf("tech");
        countOf("news");
        countOf("sports");

        insertTestArticle("tech", userId, "Tech Sentinel", 40);
        insertTestArticle("news", userId, "News Sentinel", 50);
        insertTestArticle("sports", userId, "Sports Sentinel", 60);

        ListRepo::notifyCreated(makeTestArticle("tech", userId, "Tech Notify", 70));

        CHECK(countOf("tech") == 2);    // invalidated
        CHECK(countOf("news") == 1);    // preserved
        CHECK(countOf("sports") == 1);  // preserved
    }

    SECTION("[filter-match] int64 EQ: only the matching author's page is invalidated") {
        auto user42 = insertTestUser("user42", "user42@fm.com", 0);
        auto user99 = insertTestUser("user99", "user99@fm.com", 0);
        insertTestArticle("tech", user42, "By 42", 10);
        insertTestArticle("tech", user99, "By 99", 20);

        countOf(std::nullopt, user42);
        countOf(std::nullopt, user99);

        insertTestArticle("tech", user42, "Sentinel 42", 30);
        insertTestArticle("tech", user99, "Sentinel 99", 40);

        ListRepo::notifyCreated(makeTestArticle("tech", user42, "Notify", 50));

        CHECK(countOf(std::nullopt, user42) == 2);  // invalidated
        CHECK(countOf(std::nullopt, user99) == 1);  // preserved
    }

    SECTION("[filter-match] combined filters: a mismatch on one of them preserves the page") {
        auto user42 = insertTestUser("user42", "user42@fm.com", 0);
        auto user99 = insertTestUser("user99", "user99@fm.com", 0);
        insertTestArticle("tech", user42, "Tech by 42", 10);

        countOf("tech", user42);
        insertTestArticle("tech", user42, "Sentinel", 20);

        // Category matches, author does not
        ListRepo::notifyCreated(makeTestArticle("tech", user99, "Tech by 99", 30));

        CHECK(countOf("tech", user42) == 1);  // preserved
    }

    SECTION("[filter-match] an inactive filter does not constrain the match") {
        auto user42 = insertTestUser("user42", "user42@fm.com", 0);
        auto user99 = insertTestUser("user99", "user99@fm.com", 0);
        insertTestArticle("tech", user42, "Tech 1", 10);

        // Group (category=tech, author=any)
        countOf("tech");
        insertTestArticle("tech", user99, "Sentinel", 20);

        ListRepo::notifyCreated(makeTestArticle("tech", user99, "Tech by 99", 30));

        CHECK(countOf("tech") == 2);  // invalidated
    }

    SECTION("[filter-match] the unfiltered page matches any entity") {
        auto userId = insertTestUser("author", "author@fm.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        countOf();
        countOf("tech");
        insertTestArticle("tech", userId, "Sentinel", 20);

        ListRepo::notifyCreated(makeTestArticle("tech", userId, "Tech Notify", 30));

        CHECK(countOf() == 2);        // invalidated
        CHECK(countOf("tech") == 2);  // invalidated
    }

    SECTION("[filter-match] update tech→news invalidates both groups, keeps sports") {
        auto userId = insertTestUser("author", "author@fm.com", 0);
        auto articleId = insertTestArticle("tech", userId, "Migrating", 10);
        insertTestArticle("news", userId, "News 1", 20);
        insertTestArticle("sports", userId, "Sports 1", 30);

        countOf("tech");
        countOf("news");
        countOf("sports");

        insertTestArticle("tech", userId, "Tech Sentinel", 40);
        insertTestArticle("news", userId, "News Sentinel", 50);
        insertTestArticle("sports", userId, "Sports Sentinel", 60);

        updateTestArticleCategory(articleId, "news");

        auto oldEntity = makeTestArticle("tech", userId, "Migrating", 10, false, articleId);
        auto newEntity = makeTestArticle("news", userId, "Migrating", 10, false, articleId);
        ListRepo::notifyUpdated(oldEntity, newEntity);

        CHECK(countOf("tech") == 1);    // old group: sentinel only
        CHECK(countOf("news") == 3);    // new group: existing + migrated + sentinel
        CHECK(countOf("sports") == 1);  // preserved
    }

    SECTION("[filter-match] delete invalidates only the matching group") {
        auto userId = insertTestUser("author", "author@fm.com", 0);
        auto articleId = insertTestArticle("tech", userId, "To Delete", 10);
        insertTestArticle("news", userId, "News 1", 20);

        countOf("tech");
        countOf("news");

        insertTestArticle("tech", userId, "Tech Sentinel", 30);
        insertTestArticle("news", userId, "News Sentinel", 40);
        deleteTestArticle(articleId);

        ListRepo::notifyDeleted(makeTestArticle("tech", userId, "To Delete", 10, false, articleId));

        CHECK(countOf("tech") == 1);  // invalidated: sentinel only
        CHECK(countOf("news") == 1);  // preserved
    }
}


// #############################################################################
//
//  4. Sort-range invalidation on cursor pages
//
// #############################################################################

TEST_CASE("[DeclList] Sort-range invalidation on cursor pages",
          "[integration][db][list][sortbounds]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<ListRepo>();

    auto userId = insertTestUser("author", "author@sort.com", 0);
    // [100, 80, 60, 40] sorted by view_count DESC, limit=2
    insertTestArticle("tech", userId, "A100", 100);
    auto a80 = insertTestArticle("tech", userId, "A80", 80);
    insertTestArticle("tech", userId, "A60", 60);
    insertTestArticle("tech", userId, "A40", 40);

    // Page 1 [100, 80]: first, complete
    auto p1 = sync(ListRepo::query(viewCountQuery("tech", 2)));
    REQUIRE(p1->size() == 2);
    REQUIRE(p1->items[0].view_count.value() == 100);
    REQUIRE(p1->items[1].view_count.value() == 80);

    // Page 2 [60, 40] via cursor: not first, complete
    auto q2 = viewCountQueryAfter("tech", 2, p1);
    auto p2 = sync(ListRepo::query(q2));
    REQUIRE(p2->size() == 2);
    REQUIRE(p2->items[0].view_count.value() == 60);
    REQUIRE(p2->items[1].view_count.value() == 40);

    SECTION("[sortbounds] a value inside a middle page's range invalidates it") {
        insertTestArticle("tech", userId, "Sentinel55", 55);

        // 55 <= 60 and 55 >= 40 → page 2 invalidated
        ListRepo::notifyCreated(makeTestArticle("tech", userId, "Notify55", 55));

        auto fresh = sync(ListRepo::query(q2));
        bool sentinel_visible = false;
        for (const auto& item : fresh->items) {
            if (item.view_count.value_or(0) == 55) { sentinel_visible = true; break; }
        }
        CHECK(sentinel_visible);
    }

    SECTION("[sortbounds] an update whose old and new values are both outside preserves it") {
        insertTestArticle("tech", userId, "Sentinel105", 105);
        updateTestArticle(a80, "A80-updated", 110);

        // old=80, new=110: neither is within [60, 40] → page 2 preserved
        auto oldEntity = makeTestArticle("tech", userId, "A80", 80, false, a80);
        auto newEntity = makeTestArticle("tech", userId, "A80-updated", 110, false, a80);
        ListRepo::notifyUpdated(oldEntity, newEntity);

        auto cached = sync(ListRepo::query(q2));
        REQUIRE(cached->size() == 2);
        CHECK(cached->items[0].view_count.value() == 60);
        CHECK(cached->items[1].view_count.value() == 40);
    }

    SECTION("[sortbounds] a value at a first page's last bound invalidates it, not the next page") {
        insertTestArticle("tech", userId, "Boundary80", 80);

        // Page 1 (first, DESC): 80 >= 80 → invalidated
        // Page 2 ([60, 40]): 80 <= 60? no → preserved
        ListRepo::notifyCreated(makeTestArticle("tech", userId, "Boundary80", 80));

        auto cached = sync(ListRepo::query(q2));
        REQUIRE(cached->size() == 2);
        CHECK(cached->items[0].view_count.value() == 60);
        CHECK(cached->items[1].view_count.value() == 40);
    }
}


// #############################################################################
//
//  5. Insertion invalidation edge cases
//
// #############################################################################

TEST_CASE("[DeclList] Insertion invalidation edge cases",
          "[integration][db][list][edge-invalidation]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<ListRepo>();

    SECTION("[edge] insert when no list query is cached") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        auto created = sync(ListRepo::insert(makeTestArticle("tech", userId, "Tech 2", 20)));
        CHECK(created != nullptr);

        CHECK(countOf("tech") == 2);
    }

    SECTION("[edge] rapid sequential inserts each invalidate the list") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10);

        REQUIRE(countOf("tech") == 1);

        sync(ListRepo::insert(makeTestArticle("tech", userId, "Tech 2", 20)));
        sync(ListRepo::insert(makeTestArticle("tech", userId, "Tech 3", 30)));
        sync(ListRepo::insert(makeTestArticle("tech", userId, "Tech 4", 40)));

        CHECK(countOf("tech") == 4);
    }

    SECTION("[edge] insert into an empty cached list") {
        auto userId = insertTestUser("author", "author@edge.com", 0);

        REQUIRE(countOf("empty_cat") == 0);

        insertTestArticle("empty_cat", userId, "First", 10);
        CHECK(countOf("empty_cat") == 0);  // stale

        // An empty page has no sort bounds: any matching entity affects it
        ListRepo::notifyCreated(makeTestArticle("empty_cat", userId, "Notify", 20));

        CHECK(countOf("empty_cat") == 1);
    }

    SECTION("[edge] cursor incomplete page preserved when the sort value is out of range") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A80", 80);
        insertTestArticle("tech", userId, "A60", 60);

        // Page 1 [100, 80]: first, complete (limit=2)
        auto p1 = sync(ListRepo::query(viewCountQuery("tech", 2)));
        REQUIRE(p1->size() == 2);

        // Page 2 [60] via cursor: not first, incomplete (1 < limit 2)
        auto q2 = viewCountQueryAfter("tech", 2, p1);
        auto p2 = sync(ListRepo::query(q2));
        REQUIRE(p2->size() == 1);
        REQUIRE(p2->items[0].view_count.value() == 60);

        insertTestArticle("tech", userId, "Sentinel999", 999);

        // Incomplete cursor page (DESC) covers (-inf, 60]: 999 is out of range
        ListRepo::notifyCreated(makeTestArticle("tech", userId, "E999", 999));

        auto p2_cached = sync(ListRepo::query(q2));
        CHECK(p2_cached->size() == 1);
        CHECK(p2_cached->items[0].view_count.value() == 60);
    }

    SECTION("[edge] offset incomplete page invalidated by a value below every bound") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A80", 80);
        insertTestArticle("tech", userId, "A60", 60);

        // Page 1 (offset=0, complete): [100, 80]
        auto q_off1 = viewCountQuery("tech", 2, 0);
        REQUIRE(sync(ListRepo::query(q_off1))->size() == 2);

        // Page 2 (offset=2, incomplete): [60]
        auto q_off2 = viewCountQuery("tech", 2, 2);
        REQUIRE(sync(ListRepo::query(q_off2))->size() == 1);

        insertTestArticle("tech", userId, "Sentinel1", 1);

        // Page 1 (DESC, complete) covers [80, +inf): 1 is out of range → preserved
        // Page 2 is incomplete: it holds the tail of the result → invalidated
        ListRepo::notifyCreated(makeTestArticle("tech", userId, "E1", 1));

        CHECK(sync(ListRepo::query(q_off1))->size() == 2);
        CHECK(sync(ListRepo::query(q_off2))->size() == 2);  // [60, 1]
    }

    SECTION("[edge] insert with a null sort value") {
        auto userId = insertTestUser("author", "author@edge.com", 0);
        insertTestArticle("tech", userId, "A100", 100);
        insertTestArticle("tech", userId, "A50", 50);

        // First and incomplete page (2 items, limit=10): affected by any match
        auto q = viewCountQuery("tech", 10);
        REQUIRE(sync(ListRepo::query(q))->size() == 2);

        insertTestArticle("tech", userId, "Sentinel", 30);

        ListRepo::notifyCreated(makeTestArticle("tech", userId, "NullSort", std::nullopt));

        CHECK(sync(ListRepo::query(q))->size() == 3);
    }
}
