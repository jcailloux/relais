/**
 * test_warmup.cpp
 *
 * Tests for warmup() — priming cache infrastructure at startup.
 *
 * Covers:
 *   1. CachedRepository::warmup() — L1 entity cache priming
 *   2. ListMixin::warmup() — entity + list cache priming
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;


// #############################################################################
//
//  1. CachedRepository::warmup — L1 entity cache
//
// #############################################################################

TEST_CASE("CachedRepository::warmup - L1 entity cache",
          "[integration][db][warmup][l1]")
{
    TransactionGuard tx;

    SECTION("[warmup] primes L1 cache infrastructure without error") {
        L1TestItemRepository::warmup();
        // No crash, no exception — success
    }

    SECTION("[warmup] is idempotent (can be called twice)") {
        L1TestItemRepository::warmup();
        L1TestItemRepository::warmup();
        // Second call should be a no-op
    }

    SECTION("[warmup] findById works after warmup") {
        L1TestItemRepository::warmup();

        auto id = insertTestItem("warmup_item", 42);
        auto item = sync(L1TestItemRepository::findById(id));
        REQUIRE(item != nullptr);
        REQUIRE(item->name == "warmup_item");
    }

    SECTION("[warmup] cacheSize is 0 after warmup (probe cleaned up)") {
        TestInternals::resetEntityCacheState<L1TestItemRepository>();
        L1TestItemRepository::warmup();
        // warmup() inserts a probe then invalidates it — cache should be empty
        REQUIRE(getCacheSize<L1TestItemRepository>() == 0);
    }
}


// #############################################################################
//
//  2. ListMixin::warmup — entity + list cache
//
// #############################################################################

TEST_CASE("ListMixin::warmup - entity + list cache",
          "[integration][db][warmup][list]")
{
    TransactionGuard tx;

    SECTION("[warmup] primes both entity and list cache") {
        TestArticleListRepository::warmup();
        // No crash, no exception — success
    }

    SECTION("[warmup] list query works after warmup") {
        TestArticleListRepository::warmup();

        auto userId = insertTestUser("warmup_author", "warmup@test.com", 0);
        insertTestArticle("tech", userId, "Warmup Article", 10, true);

        auto result = sync(TestArticleListRepository::query(
            makeArticleQuery("tech")));
        REQUIRE(result->size() == 1);
    }

    SECTION("[warmup] listCacheSize is 0 after warmup (probe cleaned up)") {
        TestInternals::resetEntityCacheState<TestArticleListRepository>();
        TestInternals::resetListCacheState<TestArticleListRepository>();
        TestArticleListRepository::warmup();
        // warmup() inserts probes then invalidates them — caches should be empty
        REQUIRE(getCacheSize<TestArticleListRepository>() == 0);
        REQUIRE(TestArticleListRepository::listCacheSize() == 0);
    }
}
