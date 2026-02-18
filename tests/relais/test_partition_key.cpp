/**
 * test_partition_key.cpp
 * Integration tests for partition key repositories.
 *
 * Tests CRUD, L1/L2 caching, and cross-invalidation
 * with a partitioned table where Key=int64_t and region is a partition key hint.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_section_info.hpp>
#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;

// =============================================================================
// Local configs and repos for cross-invalidation tests
// =============================================================================

namespace {

// L1 user repo as cross-invalidation target for event tests
using L1EventTargetUserRepo = Repo<TestUserWrapper, "test:user:l1:event">;

// L1 event repo as cross-invalidation SOURCE (Event → User)
using L1EventSourceRepo = Repo<TestEventWrapper, "test:event:l1:crossinv",
    cfg::Local,
    cache::Invalidate<L1EventTargetUserRepo, eventUserId>>;

// L1 event repo as cross-invalidation TARGET
using L1EventAsTargetRepo = Repo<TestEventWrapper, "test:event:l1:target">;

// Async resolver: given a user_id, find event IDs for that user
struct PurchaseToEventResolver {
    static io::Task<std::vector<int64_t>> resolve(int64_t user_id) {
        auto result = co_await jcailloux::relais::DbProvider::queryArgs(
            "SELECT id FROM relais_test_events WHERE user_id = $1", user_id);
        std::vector<int64_t> ids;
        for (size_t i = 0; i < result.rows(); ++i) {
            ids.push_back(result[i].get<int64_t>(0));
        }
        co_return ids;
    }
};

// L1 purchase repo that invalidates event cache via resolver
using L1PurchaseInvEventRepo = Repo<TestPurchaseWrapper, "test:purchase:l1:event:target",
    cfg::Local,
    cache::InvalidateVia<L1EventAsTargetRepo, purchaseUserId, &PurchaseToEventResolver::resolve>>;

} // anonymous namespace

// #############################################################################
//
//  1. PartitionKey CRUD (Uncached / BaseRepo)
//
// #############################################################################

TEST_CASE("PartitionKey<TestEvent> - find",
          "[integration][db][partition-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("event_user", "event@test.com", 100);

    SECTION("[find] finds event in 'eu' partition") {
        auto eventId = insertTestEvent("eu", userId, "EU Conference", 5);

        auto result = sync(UncachedTestEventRepo::find(eventId));
        REQUIRE(result != nullptr);
        CHECK(result->id == eventId);
        CHECK(result->region == "eu");
        CHECK(result->title == "EU Conference");
        CHECK(result->priority == 5);
        CHECK(result->user_id == userId);
    }

    SECTION("[find] finds event in 'us' partition") {
        auto eventId = insertTestEvent("us", userId, "US Launch", 3);

        auto result = sync(UncachedTestEventRepo::find(eventId));
        REQUIRE(result != nullptr);
        CHECK(result->id == eventId);
        CHECK(result->region == "us");
        CHECK(result->title == "US Launch");
    }

    SECTION("[find] returns nullptr for non-existent id") {
        auto result = sync(UncachedTestEventRepo::find(999999));
        CHECK(result == nullptr);
    }

    SECTION("[find] correct event among multiple across partitions") {
        auto id1 = insertTestEvent("eu", userId, "Event A", 1);
        auto id2 = insertTestEvent("us", userId, "Event B", 2);
        auto id3 = insertTestEvent("eu", userId, "Event C", 3);

        auto r1 = sync(UncachedTestEventRepo::find(id1));
        auto r2 = sync(UncachedTestEventRepo::find(id2));
        auto r3 = sync(UncachedTestEventRepo::find(id3));

        REQUIRE(r1 != nullptr);
        REQUIRE(r2 != nullptr);
        REQUIRE(r3 != nullptr);
        CHECK(r1->title == "Event A");
        CHECK(r2->title == "Event B");
        CHECK(r3->title == "Event C");
        CHECK(r1->region == "eu");
        CHECK(r2->region == "us");
        CHECK(r3->region == "eu");
    }
}

TEST_CASE("PartitionKey<TestEvent> - insert",
          "[integration][db][partition-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("create_user", "insert@test.com", 100);

    SECTION("[insert] inserts into 'eu' partition with generated id") {
        auto created = sync(UncachedTestEventRepo::insert(
            makeTestEvent("eu", userId, "New EU Event", 5)));
        REQUIRE(created != nullptr);
        CHECK(created->id > 0);
        CHECK(created->region == "eu");
        CHECK(created->title == "New EU Event");
        CHECK(created->priority == 5);
    }

    SECTION("[insert] inserts into 'us' partition with generated id") {
        auto created = sync(UncachedTestEventRepo::insert(
            makeTestEvent("us", userId, "New US Event", 3)));
        REQUIRE(created != nullptr);
        CHECK(created->id > 0);
        CHECK(created->region == "us");
    }

    SECTION("[insert] event retrievable after insert") {
        auto created = sync(UncachedTestEventRepo::insert(
            makeTestEvent("eu", userId, "Findable Event")));
        REQUIRE(created != nullptr);

        auto found = sync(UncachedTestEventRepo::find(created->id));
        REQUIRE(found != nullptr);
        CHECK(found->title == "Findable Event");
        CHECK(found->region == "eu");
    }

    SECTION("[insert] ids are unique across partitions (shared sequence)") {
        auto eu = sync(UncachedTestEventRepo::insert(
            makeTestEvent("eu", userId, "EU")));
        auto us = sync(UncachedTestEventRepo::insert(
            makeTestEvent("us", userId, "US")));

        REQUIRE(eu != nullptr);
        REQUIRE(us != nullptr);
        CHECK(eu->id != us->id);
    }
}

TEST_CASE("PartitionKey<TestEvent> - update",
          "[integration][db][partition-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("update_user", "update@test.com", 100);

    SECTION("[update] modifies event in partitioned table") {
        auto eventId = insertTestEvent("eu", userId, "Original", 1);

        auto updated = makeTestEvent("eu", userId, "Updated", 9, eventId);
        auto success = sync(UncachedTestEventRepo::update(eventId, updated));
        REQUIRE(success);

        auto found = sync(UncachedTestEventRepo::find(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->title == "Updated");
        CHECK(found->priority == 9);
    }

    SECTION("[update] preserves region after update") {
        auto eventId = insertTestEvent("us", userId, "US Event", 2);

        auto updated = makeTestEvent("us", userId, "US Updated", 7, eventId);
        sync(UncachedTestEventRepo::update(eventId, updated));

        auto found = sync(UncachedTestEventRepo::find(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->region == "us");
        CHECK(found->title == "US Updated");
    }
}

TEST_CASE("PartitionKey<TestEvent> - erase",
          "[integration][db][partition-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("erase_user", "erase@test.com", 100);

    SECTION("[erase] deletes via partial key criteria") {
        auto eventId = insertTestEvent("eu", userId, "To Delete", 1);

        auto result = sync(UncachedTestEventRepo::erase(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        auto found = sync(UncachedTestEventRepo::find(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[erase] returns 0 for non-existent id") {
        auto result = sync(UncachedTestEventRepo::erase(999999));
        REQUIRE(result.has_value());
        CHECK(*result == 0);
    }
}

// #############################################################################
//
//  2. PartitionKey with L1 caching
//
// #############################################################################

TEST_CASE("PartitionKey<TestEvent> - L1 caching",
          "[integration][db][partition-key][cached]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("cache_user", "cache@test.com", 100);

    SECTION("[find] caches in L1, returns stale after direct DB change") {
        auto eventId = insertTestEvent("eu", userId, "Cacheable", 5);

        // Cache in L1
        auto result1 = sync(L1TestEventRepo::find(eventId));
        REQUIRE(result1 != nullptr);
        CHECK(result1->title == "Cacheable");

        // Modify directly in DB (bypass cache)
        updateTestEvent(eventId, "Modified", 9);

        // L1 still returns stale
        auto result2 = sync(L1TestEventRepo::find(eventId));
        REQUIRE(result2 != nullptr);
        CHECK(result2->title == "Cacheable");
    }

    SECTION("[insert] populates L1 cache") {
        auto created = sync(L1TestEventRepo::insert(
            makeTestEvent("eu", userId, "Created via L1")));
        REQUIRE(created != nullptr);

        // Modify in DB
        updateTestEvent(created->id, "DB Modified", 99);

        // L1 returns cached (pre-modification) value
        auto cached = sync(L1TestEventRepo::find(created->id));
        REQUIRE(cached != nullptr);
        CHECK(cached->title == "Created via L1");
    }

    SECTION("[update] invalidates L1 cache") {
        auto eventId = insertTestEvent("eu", userId, "Before Update", 1);

        // Cache in L1
        sync(L1TestEventRepo::find(eventId));

        // Modify in DB directly
        updateTestEvent(eventId, "DB Changed", 7);

        // Update via repo (invalidates L1)
        auto wrapper = makeTestEvent("eu", userId, "Repo Updated", 5, eventId);
        sync(L1TestEventRepo::update(eventId, wrapper));

        // Next read gets fresh data from DB
        auto found = sync(L1TestEventRepo::find(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->title == "Repo Updated");
    }

    SECTION("[erase] invalidates L1 cache") {
        auto eventId = insertTestEvent("eu", userId, "To erase", 1);

        // Cache in L1
        sync(L1TestEventRepo::find(eventId));

        // erase via repo
        sync(L1TestEventRepo::erase(eventId));

        // Not found
        auto found = sync(L1TestEventRepo::find(eventId));
        CHECK(found == nullptr);
    }
}

// #############################################################################
//
//  3. PartitionKey with L2 caching (Redis)
//
// #############################################################################

TEST_CASE("PartitionKey<TestEvent> - L2 caching",
          "[integration][db][partition-key][redis]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("redis_user", "redis@test.com", 100);

    SECTION("[find] caches in Redis, returns on second read") {
        auto eventId = insertTestEvent("us", userId, "Redis Event", 3);

        // First read: DB → Redis
        auto result1 = sync(L2TestEventRepo::find(eventId));
        REQUIRE(result1 != nullptr);
        CHECK(result1->title == "Redis Event");
        CHECK(result1->region == "us");

        // Modify in DB directly
        updateTestEvent(eventId, "DB Modified", 99);

        // Second read: Redis (stale)
        auto result2 = sync(L2TestEventRepo::find(eventId));
        REQUIRE(result2 != nullptr);
        CHECK(result2->title == "Redis Event");
    }

    SECTION("[update] invalidates Redis cache") {
        auto eventId = insertTestEvent("eu", userId, "Redis Before", 1);

        // Cache in Redis
        sync(L2TestEventRepo::find(eventId));

        // Modify in DB directly
        updateTestEvent(eventId, "DB Changed", 7);

        // Update via repo (invalidates Redis)
        auto wrapper = makeTestEvent("eu", userId, "Redis After", 5, eventId);
        sync(L2TestEventRepo::update(eventId, wrapper));

        // Next read gets fresh data
        auto found = sync(L2TestEventRepo::find(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->title == "Redis After");
    }
}

// #############################################################################
//
//  4. Cross-invalidation: Event (PartitionKey) as SOURCE
//
// #############################################################################

TEST_CASE("PartitionKey cross-invalidation - Event as source",
          "[integration][db][partition-key][cross-inv]")
{
    TransactionGuard tx;

    SECTION("[cross-inv] insert event invalidates user L1 cache") {
        auto userId = insertTestUser("inv_user", "inv@test.com", 1000);

        // Cache user in L1
        auto user1 = sync(L1EventTargetUserRepo::find(userId));
        REQUIRE(user1 != nullptr);
        REQUIRE(user1->balance == 1000);

        // Modify user balance directly in DB
        updateTestUserBalance(userId, 500);

        // User still cached (stale)
        CHECK(sync(L1EventTargetUserRepo::find(userId))->balance == 1000);

        // insert event → triggers Invalidate<User, &Event::user_id>
        auto created = sync(L1EventSourceRepo::insert(
            makeTestEvent("eu", userId, "New Event")));
        REQUIRE(created != nullptr);

        // User L1 cache invalidated → fresh data
        auto user2 = sync(L1EventTargetUserRepo::find(userId));
        REQUIRE(user2 != nullptr);
        CHECK(user2->balance == 500);
    }

    SECTION("[cross-inv] update event invalidates user L1 cache") {
        auto userId = insertTestUser("upd_user", "upd@test.com", 1000);
        auto eventId = insertTestEvent("eu", userId, "Event", 1);

        // Cache user
        sync(L1EventTargetUserRepo::find(userId));
        updateTestUserBalance(userId, 750);

        // Update event through repo
        sync(L1EventSourceRepo::update(eventId,
            makeTestEvent("eu", userId, "Updated Event", 5, eventId)));

        // User cache invalidated
        auto user = sync(L1EventTargetUserRepo::find(userId));
        REQUIRE(user != nullptr);
        CHECK(user->balance == 750);
    }

    SECTION("[cross-inv] delete event invalidates user L1 cache") {
        auto userId = insertTestUser("del_user", "del@test.com", 1000);
        auto eventId = insertTestEvent("eu", userId, "To Delete", 1);

        sync(L1EventTargetUserRepo::find(userId));
        updateTestUserBalance(userId, 200);

        sync(L1EventSourceRepo::erase(eventId));

        auto user = sync(L1EventTargetUserRepo::find(userId));
        REQUIRE(user != nullptr);
        CHECK(user->balance == 200);
    }
}

// #############################################################################
//
//  5. Cross-invalidation: Event (PartitionKey) as TARGET
//
// #############################################################################

TEST_CASE("PartitionKey cross-invalidation - Event as target",
          "[integration][db][partition-key][cross-inv]")
{
    TransactionGuard tx;

    SECTION("[cross-inv] purchase creation invalidates event L1 cache (via InvalidateVia resolver)") {
        auto userId = insertTestUser("target_user", "target@test.com", 100);
        auto eventId = insertTestEvent("eu", userId, "Cached Event", 5);

        // Cache event in L1
        auto event1 = sync(L1EventAsTargetRepo::find(eventId));
        REQUIRE(event1 != nullptr);
        CHECK(event1->title == "Cached Event");

        // Modify event in DB directly
        updateTestEvent(eventId, "DB Modified", 99);

        // Event still cached (stale)
        CHECK(sync(L1EventAsTargetRepo::find(eventId))->title == "Cached Event");

        // insert purchase for same user → resolver finds event IDs → invalidates event cache
        auto created = sync(L1PurchaseInvEventRepo::insert(
            makeTestPurchase(userId, "Widget", 50)));
        REQUIRE(created != nullptr);

        // Event cache invalidated → fresh data
        auto event2 = sync(L1EventAsTargetRepo::find(eventId));
        REQUIRE(event2 != nullptr);
        CHECK(event2->title == "DB Modified");
        CHECK(event2->priority == 99);
    }
}

// #############################################################################
//
//  6. Serialization
//
// #############################################################################

TEST_CASE("PartitionKey - serialization",
          "[integration][db][partition-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("serial_user", "serial@test.com", 100);

    SECTION("[json] round-trip preserves region field") {
        auto eventId = insertTestEvent("eu", userId, "JSON Test", 7);

        auto original = sync(UncachedTestEventRepo::find(eventId));
        REQUIRE(original != nullptr);

        auto json = original->json();
        REQUIRE(json != nullptr);

        // Verify region is in the JSON
        auto jsonStr = *json;
        CHECK(jsonStr.find("\"region\"") != std::string::npos);
        CHECK(jsonStr.find("\"eu\"") != std::string::npos);

        // Round-trip
        auto restored = TestEventWrapper::fromJson(jsonStr);
        REQUIRE(restored.has_value());
        CHECK(restored->region == "eu");
        CHECK(restored->title == "JSON Test");
        CHECK(restored->priority == 7);
    }

    SECTION("[beve] round-trip preserves region field") {
        auto eventId = insertTestEvent("us", userId, "BEVE Test", 3);

        auto original = sync(UncachedTestEventRepo::find(eventId));
        REQUIRE(original != nullptr);

        auto binary = original->binary();
        REQUIRE(binary != nullptr);
        REQUIRE(!binary->empty());

        auto restored = TestEventWrapper::fromBinary(*binary);
        REQUIRE(restored.has_value());
        CHECK(restored->region == "us");
        CHECK(restored->title == "BEVE Test");
        CHECK(restored->priority == 3);
    }
}

// #############################################################################
//
//  7. patch — criteria-based partial update for PartitionKey
//
// #############################################################################

using jcailloux::relais::wrapper::set;
using EF = TestEventWrapper::Field;

TEST_CASE("PartitionKey<TestEvent> - patch (Uncached)",
          "[integration][db][partition-key][patch]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("patch_user", "patch@test.com", 100);

    SECTION("[patch] updates single field via criteria-based partial update") {
        auto eventId = insertTestEvent("eu", userId, "Original", 1);

        auto result = sync(UncachedTestEventRepo::patch(
            eventId, set<EF::title>(std::string("Updated"))));

        REQUIRE(result != nullptr);
        CHECK(result->title == "Updated");
        CHECK(result->priority == 1);       // Unchanged
        CHECK(result->region == "eu");       // Partition preserved
        CHECK(result->user_id == userId);    // Unchanged
    }

    SECTION("[patch] updates multiple fields") {
        auto eventId = insertTestEvent("us", userId, "Multi", 3);

        auto result = sync(UncachedTestEventRepo::patch(
            eventId,
            set<EF::title>(std::string("Changed")),
            set<EF::priority>(9)));

        REQUIRE(result != nullptr);
        CHECK(result->title == "Changed");
        CHECK(result->priority == 9);
        CHECK(result->region == "us");       // Partition preserved
    }

    SECTION("[patch] preserves partition (region) after update") {
        auto eventId = insertTestEvent("eu", userId, "EU Event", 5);

        auto result = sync(UncachedTestEventRepo::patch(
            eventId, set<EF::priority>(99)));

        REQUIRE(result != nullptr);
        CHECK(result->region == "eu");

        // Independent verification via raw SQL
        auto dbResult = execQueryArgs(
            "SELECT region FROM relais_test_events WHERE id = $1", eventId);
        REQUIRE(dbResult.rows() == 1);
        CHECK(dbResult[0].get<std::string>(0) == "eu");
    }

    SECTION("[patch] returns re-fetched entity with all fields") {
        auto eventId = insertTestEvent("us", userId, "Before", 2);

        auto result = sync(UncachedTestEventRepo::patch(
            eventId, set<EF::title>(std::string("After"))));

        REQUIRE(result != nullptr);
        CHECK(result->id == eventId);
        CHECK(result->region == "us");
        CHECK(result->user_id == userId);
        CHECK(result->title == "After");
        CHECK(result->priority == 2);
        CHECK(!result->created_at.empty());
    }

    SECTION("[patch] returns nullptr for non-existent id") {
        auto result = sync(UncachedTestEventRepo::patch(
            999999, set<EF::title>(std::string("Ghost"))));

        CHECK(result == nullptr);
    }
}

TEST_CASE("PartitionKey<TestEvent> - patch (L1)",
          "[integration][db][partition-key][cached][patch]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("l1patch_user", "l1patch@test.com", 100);

    SECTION("[patch] invalidates L1 and returns fresh entity") {
        auto eventId = insertTestEvent("eu", userId, "Cached", 5);

        // Populate L1 cache
        auto cached = sync(L1TestEventRepo::find(eventId));
        REQUIRE(cached != nullptr);
        CHECK(cached->title == "Cached");

        // Modify directly in DB (bypass cache)
        updateTestEvent(eventId, "DB Changed", 99);

        // L1 still returns stale
        CHECK(sync(L1TestEventRepo::find(eventId))->title == "Cached");

        // patch invalidates L1 and re-fetches
        auto result = sync(L1TestEventRepo::patch(
            eventId, set<EF::priority>(7)));

        REQUIRE(result != nullptr);
        CHECK(result->priority == 7);
        CHECK(result->title == "DB Changed");  // Re-fetched from DB, not stale L1
    }

    SECTION("[patch] updates multiple fields with L1 invalidation") {
        auto eventId = insertTestEvent("us", userId, "Multi", 1);

        // Populate L1
        sync(L1TestEventRepo::find(eventId));

        auto result = sync(L1TestEventRepo::patch(
            eventId,
            set<EF::title>(std::string("New")),
            set<EF::priority>(8)));

        REQUIRE(result != nullptr);
        CHECK(result->title == "New");
        CHECK(result->priority == 8);
        CHECK(result->region == "us");
    }
}

TEST_CASE("PartitionKey<TestEvent> - patch (L2)",
          "[integration][db][partition-key][redis][patch]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("l2patch_user", "l2patch@test.com", 100);

    SECTION("[patch] invalidates Redis then re-fetches") {
        auto eventId = insertTestEvent("eu", userId, "Redis Cached", 5);

        // Populate Redis
        sync(L2TestEventRepo::find(eventId));

        // Modify in DB directly (bypass cache)
        updateTestEvent(eventId, "DB Changed", 99);

        // Redis still returns stale data
        auto stale = sync(L2TestEventRepo::find(eventId));
        REQUIRE(stale != nullptr);
        CHECK(stale->title == "Redis Cached");

        // patch invalidates Redis, updates priority, then re-fetches from DB
        auto result = sync(L2TestEventRepo::patch(
            eventId, set<EF::priority>(42)));

        REQUIRE(result != nullptr);
        CHECK(result->priority == 42);
        CHECK(result->title == "DB Changed");  // Re-fetched from DB, not stale Redis

        // Independent fetch confirms correct state
        auto found = sync(L2TestEventRepo::find(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->priority == 42);
        CHECK(found->title == "DB Changed");
    }
}

TEST_CASE("PartitionKey<TestEvent> - patch cross-invalidation",
          "[integration][db][partition-key][cross-inv][patch]")
{
    TransactionGuard tx;

    SECTION("[patch] on event invalidates user L1 cache") {
        auto userId = insertTestUser("crossinv_user", "crossinv@test.com", 1000);
        auto eventId = insertTestEvent("eu", userId, "Event", 1);

        // Cache user in L1
        auto user1 = sync(L1EventTargetUserRepo::find(userId));
        REQUIRE(user1 != nullptr);
        CHECK(user1->balance == 1000);

        // Modify user balance directly in DB
        updateTestUserBalance(userId, 500);

        // User still cached (stale)
        CHECK(sync(L1EventTargetUserRepo::find(userId))->balance == 1000);

        // patch on event → triggers cross-invalidation → invalidates user cache
        auto result = sync(L1EventSourceRepo::patch(
            eventId, set<EF::priority>(99)));
        REQUIRE(result != nullptr);

        // User L1 cache invalidated → fresh data
        auto user2 = sync(L1EventTargetUserRepo::find(userId));
        REQUIRE(user2 != nullptr);
        CHECK(user2->balance == 500);
    }
}

// #############################################################################
//
//  8. erase — Opportunistic full PK via L1/L2 hint
//
// #############################################################################

TEST_CASE("PartitionKey<TestEvent> - erase with L1 hint",
          "[integration][db][partition-key][cached]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("l1erase_user", "l1erase@test.com", 100);

    SECTION("[erase] succeeds when entity is in L1 cache (full PK path)") {
        auto eventId = insertTestEvent("eu", userId, "L1 Cached", 5);

        // Populate L1 cache
        sync(L1TestEventRepo::find(eventId));

        // Verify precondition: L1 cache has the entity (hint will be provided)
        auto cached = TestInternals::getFromCache<L1TestEventRepo>(eventId);
        REQUIRE(cached != nullptr);
        CHECK(cached->region == "eu");

        // erase (L1 hit → provides hint → delete_with_partition)
        // If hint had wrong region, DELETE ... WHERE id=$1 AND region=$2 would return 0
        auto result = sync(L1TestEventRepo::erase(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        // Verify deletion
        auto found = sync(L1TestEventRepo::find(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[erase] succeeds when entity is NOT in L1 cache (criteria path)") {
        auto eventId = insertTestEvent("us", userId, "Not Cached", 3);

        // Verify precondition: L1 cache does NOT have the entity (no hint)
        auto cached = TestInternals::getFromCache<L1TestEventRepo>(eventId);
        REQUIRE(cached == nullptr);

        // erase without hint → delete_by_pk (criteria-based, scans all partitions)
        auto result = sync(L1TestEventRepo::erase(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        // Verify deletion
        auto found = sync(L1TestEventRepo::find(eventId));
        CHECK(found == nullptr);
    }
}

TEST_CASE("PartitionKey<TestEvent> - erase with L2 hint",
          "[integration][db][partition-key][redis]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("l2erase_user", "l2erase@test.com", 100);

    SECTION("[erase] succeeds when entity is in Redis (L2 hint path)") {
        auto eventId = insertTestEvent("eu", userId, "Redis Cached", 5);

        // Populate Redis cache
        sync(L2TestEventRepo::find(eventId));

        // erase (L2 hit → provides hint → full PK delete)
        auto result = sync(L2TestEventRepo::erase(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        // Verify deletion
        auto found = sync(L2TestEventRepo::find(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[erase] succeeds when entity is NOT in Redis (criteria fallback)") {
        auto eventId = insertTestEvent("us", userId, "Not Cached", 3);

        // Ensure no Redis data
        flushRedis();

        // erase (no L2 hint → criteria-based)
        auto result = sync(L2TestEventRepo::erase(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);
    }
}

TEST_CASE("PartitionKey<TestEvent> - erase with L1+L2 hint chain",
          "[integration][db][partition-key][cached][redis]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("botherase_user", "botherase@test.com", 100);

    SECTION("[erase] L1 hit provides hint (skips L2 check)") {
        auto eventId = insertTestEvent("eu", userId, "Both Cached", 5);

        // Populate L1 + L2
        sync(L1L2TestEventRepo::find(eventId));

        // Verify precondition: L1 has entity with correct partition key
        auto cached = TestInternals::getFromCache<L1L2TestEventRepo>(eventId);
        REQUIRE(cached != nullptr);
        CHECK(cached->region == "eu");

        // erase (L1 hit → hint with region="eu" → delete_with_partition)
        auto result = sync(L1L2TestEventRepo::erase(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        auto found = sync(L1L2TestEventRepo::find(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[erase] L1 miss, L2 hit provides hint") {
        auto eventId = insertTestEvent("us", userId, "L2 Only", 3);

        // Populate L1 + L2
        sync(L1L2TestEventRepo::find(eventId));

        // Invalidate L1 only (L2 still has the entity)
        L1L2TestEventRepo::evict(eventId);

        // Verify precondition: L1 is empty (hint must come from L2)
        auto cachedL1 = TestInternals::getFromCache<L1L2TestEventRepo>(eventId);
        REQUIRE(cachedL1 == nullptr);

        // erase (L1 miss → L2 hit → hint with region="us" → delete_with_partition)
        auto result = sync(L1L2TestEventRepo::erase(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        auto found = sync(L1L2TestEventRepo::find(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[erase] both L1 and L2 miss - criteria fallback") {
        auto eventId = insertTestEvent("eu", userId, "No Cache", 1);

        // Ensure no L1 and no L2
        flushRedis();

        // Verify precondition: L1 is empty
        auto cachedL1 = TestInternals::getFromCache<L1L2TestEventRepo>(eventId);
        REQUIRE(cachedL1 == nullptr);

        // erase (no L1, no L2 → no hint → delete_by_pk, scans all partitions)
        auto result = sync(L1L2TestEventRepo::erase(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        auto found = sync(L1L2TestEventRepo::find(eventId));
        CHECK(found == nullptr);
    }
}
