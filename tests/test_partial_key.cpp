/**
 * test_partial_key.cpp
 * Integration tests for PartialKey repositories.
 *
 * Tests CRUD, L1/L2 caching, cross-invalidation, and PartialKeyValidator
 * with a partitioned table where Key=int64_t but Model::PrimaryKeyType=tuple<int64_t,string>.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_section_info.hpp>
#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include <jcailloux/relais/repository/PartialKeyValidator.h>

using namespace relais_test;

// =============================================================================
// Local configs and repos for cross-invalidation tests
// =============================================================================

namespace {

// L1 user repo as cross-invalidation target for event tests
using L1EventTargetUserRepository = Repository<TestUserWrapper, "test:user:l1:event">;

// L1 event repo as cross-invalidation SOURCE (Event → User)
using L1EventSourceRepository = Repository<TestEventWrapper, "test:event:l1:crossinv",
    cfg::Local,
    cache::Invalidate<L1EventTargetUserRepository, eventUserId>>;

// L1 event repo as cross-invalidation TARGET
using L1EventAsTargetRepository = Repository<TestEventWrapper, "test:event:l1:target">;

// Async resolver: given a user_id, find event IDs for that user
struct PurchaseToEventResolver {
    static pqcoro::Task<std::vector<int64_t>> resolve(int64_t user_id) {
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
using L1PurchaseInvEventRepository = Repository<TestPurchaseWrapper, "test:purchase:l1:event:target",
    cfg::Local,
    cache::InvalidateVia<L1EventAsTargetRepository, purchaseUserId, &PurchaseToEventResolver::resolve>>;

} // anonymous namespace

// #############################################################################
//
//  1. PartialKey CRUD (Uncached / BaseRepository)
//
// #############################################################################

TEST_CASE("PartialKey<TestEvent> - findById",
          "[integration][db][partial-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("event_user", "event@test.com", 100);

    SECTION("[findById] finds event in 'eu' partition") {
        auto eventId = insertTestEvent("eu", userId, "EU Conference", 5);

        auto result = sync(UncachedTestEventRepository::findById(eventId));
        REQUIRE(result != nullptr);
        CHECK(result->id == eventId);
        CHECK(result->region == "eu");
        CHECK(result->title == "EU Conference");
        CHECK(result->priority == 5);
        CHECK(result->user_id == userId);
    }

    SECTION("[findById] finds event in 'us' partition") {
        auto eventId = insertTestEvent("us", userId, "US Launch", 3);

        auto result = sync(UncachedTestEventRepository::findById(eventId));
        REQUIRE(result != nullptr);
        CHECK(result->id == eventId);
        CHECK(result->region == "us");
        CHECK(result->title == "US Launch");
    }

    SECTION("[findById] returns nullptr for non-existent id") {
        auto result = sync(UncachedTestEventRepository::findById(999999));
        CHECK(result == nullptr);
    }

    SECTION("[findById] correct event among multiple across partitions") {
        auto id1 = insertTestEvent("eu", userId, "Event A", 1);
        auto id2 = insertTestEvent("us", userId, "Event B", 2);
        auto id3 = insertTestEvent("eu", userId, "Event C", 3);

        auto r1 = sync(UncachedTestEventRepository::findById(id1));
        auto r2 = sync(UncachedTestEventRepository::findById(id2));
        auto r3 = sync(UncachedTestEventRepository::findById(id3));

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

TEST_CASE("PartialKey<TestEvent> - create",
          "[integration][db][partial-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("create_user", "create@test.com", 100);

    SECTION("[create] inserts into 'eu' partition with generated id") {
        auto created = sync(UncachedTestEventRepository::create(
            makeTestEvent("eu", userId, "New EU Event", 5)));
        REQUIRE(created != nullptr);
        CHECK(created->id > 0);
        CHECK(created->region == "eu");
        CHECK(created->title == "New EU Event");
        CHECK(created->priority == 5);
    }

    SECTION("[create] inserts into 'us' partition with generated id") {
        auto created = sync(UncachedTestEventRepository::create(
            makeTestEvent("us", userId, "New US Event", 3)));
        REQUIRE(created != nullptr);
        CHECK(created->id > 0);
        CHECK(created->region == "us");
    }

    SECTION("[create] event retrievable after insert") {
        auto created = sync(UncachedTestEventRepository::create(
            makeTestEvent("eu", userId, "Findable Event")));
        REQUIRE(created != nullptr);

        auto found = sync(UncachedTestEventRepository::findById(created->id));
        REQUIRE(found != nullptr);
        CHECK(found->title == "Findable Event");
        CHECK(found->region == "eu");
    }

    SECTION("[create] ids are unique across partitions (shared sequence)") {
        auto eu = sync(UncachedTestEventRepository::create(
            makeTestEvent("eu", userId, "EU")));
        auto us = sync(UncachedTestEventRepository::create(
            makeTestEvent("us", userId, "US")));

        REQUIRE(eu != nullptr);
        REQUIRE(us != nullptr);
        CHECK(eu->id != us->id);
    }
}

TEST_CASE("PartialKey<TestEvent> - update",
          "[integration][db][partial-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("update_user", "update@test.com", 100);

    SECTION("[update] modifies event in partitioned table") {
        auto eventId = insertTestEvent("eu", userId, "Original", 1);

        auto updated = makeTestEvent("eu", userId, "Updated", 9, eventId);
        auto success = sync(UncachedTestEventRepository::update(eventId, updated));
        REQUIRE(success);

        auto found = sync(UncachedTestEventRepository::findById(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->title == "Updated");
        CHECK(found->priority == 9);
    }

    SECTION("[update] preserves region after update") {
        auto eventId = insertTestEvent("us", userId, "US Event", 2);

        auto updated = makeTestEvent("us", userId, "US Updated", 7, eventId);
        sync(UncachedTestEventRepository::update(eventId, updated));

        auto found = sync(UncachedTestEventRepository::findById(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->region == "us");
        CHECK(found->title == "US Updated");
    }
}

TEST_CASE("PartialKey<TestEvent> - remove",
          "[integration][db][partial-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("remove_user", "remove@test.com", 100);

    SECTION("[remove] deletes via partial key criteria") {
        auto eventId = insertTestEvent("eu", userId, "To Delete", 1);

        auto result = sync(UncachedTestEventRepository::remove(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        auto found = sync(UncachedTestEventRepository::findById(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[remove] returns 0 for non-existent id") {
        auto result = sync(UncachedTestEventRepository::remove(999999));
        REQUIRE(result.has_value());
        CHECK(*result == 0);
    }
}

// #############################################################################
//
//  2. PartialKey with L1 caching
//
// #############################################################################

TEST_CASE("PartialKey<TestEvent> - L1 caching",
          "[integration][db][partial-key][cached]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("cache_user", "cache@test.com", 100);

    SECTION("[findById] caches in L1, returns stale after direct DB change") {
        auto eventId = insertTestEvent("eu", userId, "Cacheable", 5);

        // Cache in L1
        auto result1 = sync(L1TestEventRepository::findById(eventId));
        REQUIRE(result1 != nullptr);
        CHECK(result1->title == "Cacheable");

        // Modify directly in DB (bypass cache)
        updateTestEvent(eventId, "Modified", 9);

        // L1 still returns stale
        auto result2 = sync(L1TestEventRepository::findById(eventId));
        REQUIRE(result2 != nullptr);
        CHECK(result2->title == "Cacheable");
    }

    SECTION("[create] populates L1 cache") {
        auto created = sync(L1TestEventRepository::create(
            makeTestEvent("eu", userId, "Created via L1")));
        REQUIRE(created != nullptr);

        // Modify in DB
        updateTestEvent(created->id, "DB Modified", 99);

        // L1 returns cached (pre-modification) value
        auto cached = sync(L1TestEventRepository::findById(created->id));
        REQUIRE(cached != nullptr);
        CHECK(cached->title == "Created via L1");
    }

    SECTION("[update] invalidates L1 cache") {
        auto eventId = insertTestEvent("eu", userId, "Before Update", 1);

        // Cache in L1
        sync(L1TestEventRepository::findById(eventId));

        // Modify in DB directly
        updateTestEvent(eventId, "DB Changed", 7);

        // Update via repo (invalidates L1)
        auto wrapper = makeTestEvent("eu", userId, "Repo Updated", 5, eventId);
        sync(L1TestEventRepository::update(eventId, wrapper));

        // Next read gets fresh data from DB
        auto found = sync(L1TestEventRepository::findById(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->title == "Repo Updated");
    }

    SECTION("[remove] invalidates L1 cache") {
        auto eventId = insertTestEvent("eu", userId, "To Remove", 1);

        // Cache in L1
        sync(L1TestEventRepository::findById(eventId));

        // Remove via repo
        sync(L1TestEventRepository::remove(eventId));

        // Not found
        auto found = sync(L1TestEventRepository::findById(eventId));
        CHECK(found == nullptr);
    }
}

// #############################################################################
//
//  3. PartialKey with L2 caching (Redis)
//
// #############################################################################

TEST_CASE("PartialKey<TestEvent> - L2 caching",
          "[integration][db][partial-key][redis]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("redis_user", "redis@test.com", 100);

    SECTION("[findById] caches in Redis, returns on second read") {
        auto eventId = insertTestEvent("us", userId, "Redis Event", 3);

        // First read: DB → Redis
        auto result1 = sync(L2TestEventRepository::findById(eventId));
        REQUIRE(result1 != nullptr);
        CHECK(result1->title == "Redis Event");
        CHECK(result1->region == "us");

        // Modify in DB directly
        updateTestEvent(eventId, "DB Modified", 99);

        // Second read: Redis (stale)
        auto result2 = sync(L2TestEventRepository::findById(eventId));
        REQUIRE(result2 != nullptr);
        CHECK(result2->title == "Redis Event");
    }

    SECTION("[update] invalidates Redis cache") {
        auto eventId = insertTestEvent("eu", userId, "Redis Before", 1);

        // Cache in Redis
        sync(L2TestEventRepository::findById(eventId));

        // Modify in DB directly
        updateTestEvent(eventId, "DB Changed", 7);

        // Update via repo (invalidates Redis)
        auto wrapper = makeTestEvent("eu", userId, "Redis After", 5, eventId);
        sync(L2TestEventRepository::update(eventId, wrapper));

        // Next read gets fresh data
        auto found = sync(L2TestEventRepository::findById(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->title == "Redis After");
    }
}

// #############################################################################
//
//  4. Cross-invalidation: Event (PartialKey) as SOURCE
//
// #############################################################################

TEST_CASE("PartialKey cross-invalidation - Event as source",
          "[integration][db][partial-key][cross-inv]")
{
    TransactionGuard tx;

    SECTION("[cross-inv] create event invalidates user L1 cache") {
        auto userId = insertTestUser("inv_user", "inv@test.com", 1000);

        // Cache user in L1
        auto user1 = sync(L1EventTargetUserRepository::findById(userId));
        REQUIRE(user1 != nullptr);
        REQUIRE(user1->balance == 1000);

        // Modify user balance directly in DB
        updateTestUserBalance(userId, 500);

        // User still cached (stale)
        CHECK(sync(L1EventTargetUserRepository::findById(userId))->balance == 1000);

        // Create event → triggers Invalidate<User, &Event::user_id>
        auto created = sync(L1EventSourceRepository::create(
            makeTestEvent("eu", userId, "New Event")));
        REQUIRE(created != nullptr);

        // User L1 cache invalidated → fresh data
        auto user2 = sync(L1EventTargetUserRepository::findById(userId));
        REQUIRE(user2 != nullptr);
        CHECK(user2->balance == 500);
    }

    SECTION("[cross-inv] update event invalidates user L1 cache") {
        auto userId = insertTestUser("upd_user", "upd@test.com", 1000);
        auto eventId = insertTestEvent("eu", userId, "Event", 1);

        // Cache user
        sync(L1EventTargetUserRepository::findById(userId));
        updateTestUserBalance(userId, 750);

        // Update event through repo
        sync(L1EventSourceRepository::update(eventId,
            makeTestEvent("eu", userId, "Updated Event", 5, eventId)));

        // User cache invalidated
        auto user = sync(L1EventTargetUserRepository::findById(userId));
        REQUIRE(user != nullptr);
        CHECK(user->balance == 750);
    }

    SECTION("[cross-inv] delete event invalidates user L1 cache") {
        auto userId = insertTestUser("del_user", "del@test.com", 1000);
        auto eventId = insertTestEvent("eu", userId, "To Delete", 1);

        sync(L1EventTargetUserRepository::findById(userId));
        updateTestUserBalance(userId, 200);

        sync(L1EventSourceRepository::remove(eventId));

        auto user = sync(L1EventTargetUserRepository::findById(userId));
        REQUIRE(user != nullptr);
        CHECK(user->balance == 200);
    }
}

// #############################################################################
//
//  5. Cross-invalidation: Event (PartialKey) as TARGET
//
// #############################################################################

TEST_CASE("PartialKey cross-invalidation - Event as target",
          "[integration][db][partial-key][cross-inv]")
{
    TransactionGuard tx;

    SECTION("[cross-inv] purchase creation invalidates event L1 cache (via InvalidateVia resolver)") {
        auto userId = insertTestUser("target_user", "target@test.com", 100);
        auto eventId = insertTestEvent("eu", userId, "Cached Event", 5);

        // Cache event in L1
        auto event1 = sync(L1EventAsTargetRepository::findById(eventId));
        REQUIRE(event1 != nullptr);
        CHECK(event1->title == "Cached Event");

        // Modify event in DB directly
        updateTestEvent(eventId, "DB Modified", 99);

        // Event still cached (stale)
        CHECK(sync(L1EventAsTargetRepository::findById(eventId))->title == "Cached Event");

        // Create purchase for same user → resolver finds event IDs → invalidates event cache
        auto created = sync(L1PurchaseInvEventRepository::create(
            makeTestPurchase(userId, "Widget", 50)));
        REQUIRE(created != nullptr);

        // Event cache invalidated → fresh data
        auto event2 = sync(L1EventAsTargetRepository::findById(eventId));
        REQUIRE(event2 != nullptr);
        CHECK(event2->title == "DB Modified");
        CHECK(event2->priority == 99);
    }
}

// #############################################################################
//
//  6. PartialKeyValidator
//
// #############################################################################

TEST_CASE("PartialKeyValidator",
          "[integration][db][partial-key][validator]")
{
    TransactionGuard tx;

    using Validator = jcailloux::relais::PartialKeyValidator;

    SECTION("[validator] validateKeyUsesSequenceOrUuid passes for events.id") {
        auto result = sync(Validator::validateKeyUsesSequenceOrUuid(
            "relais_test_events", "id"));
        CHECK(result.valid);
        CHECK(result.reason.find("SEQUENCE") != std::string::npos);
    }

    SECTION("[validator] validatePartitionColumns passes for events table") {
        auto result = sync(Validator::validatePartitionColumns(
            "relais_test_events", {"id"}));
        CHECK(result.valid);
        CHECK(result.reason.find("partition") != std::string::npos);
    }

    SECTION("[validator] validateAll passes") {
        auto result = sync(Validator::validateAll(
            "relais_test_events", "id"));
        CHECK(result == true);
    }

    SECTION("[validator] rejects non-sequence column") {
        auto result = sync(Validator::validateKeyUsesSequenceOrUuid(
            "relais_test_events", "region"));
        CHECK_FALSE(result.valid);
    }
}

// #############################################################################
//
//  7. Serialization
//
// #############################################################################

TEST_CASE("PartialKey - serialization",
          "[integration][db][partial-key]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("serial_user", "serial@test.com", 100);

    SECTION("[json] round-trip preserves region field") {
        auto eventId = insertTestEvent("eu", userId, "JSON Test", 7);

        auto original = sync(UncachedTestEventRepository::findById(eventId));
        REQUIRE(original != nullptr);

        auto json = original->toJson();
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

        auto original = sync(UncachedTestEventRepository::findById(eventId));
        REQUIRE(original != nullptr);

        auto binary = original->toBinary();
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
//  8. updateBy — criteria-based partial update for PartialKey
//
// #############################################################################

using jcailloux::relais::wrapper::set;
using EF = TestEventWrapper::Field;

TEST_CASE("PartialKey<TestEvent> - updateBy (Uncached)",
          "[integration][db][partial-key][updateBy]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("updateby_user", "updateby@test.com", 100);

    SECTION("[updateBy] updates single field via criteria-based partial update") {
        auto eventId = insertTestEvent("eu", userId, "Original", 1);

        auto result = sync(UncachedTestEventRepository::updateBy(
            eventId, set<EF::title>(std::string("Updated"))));

        REQUIRE(result != nullptr);
        CHECK(result->title == "Updated");
        CHECK(result->priority == 1);       // Unchanged
        CHECK(result->region == "eu");       // Partition preserved
        CHECK(result->user_id == userId);    // Unchanged
    }

    SECTION("[updateBy] updates multiple fields") {
        auto eventId = insertTestEvent("us", userId, "Multi", 3);

        auto result = sync(UncachedTestEventRepository::updateBy(
            eventId,
            set<EF::title>(std::string("Changed")),
            set<EF::priority>(9)));

        REQUIRE(result != nullptr);
        CHECK(result->title == "Changed");
        CHECK(result->priority == 9);
        CHECK(result->region == "us");       // Partition preserved
    }

    SECTION("[updateBy] preserves partition (region) after update") {
        auto eventId = insertTestEvent("eu", userId, "EU Event", 5);

        auto result = sync(UncachedTestEventRepository::updateBy(
            eventId, set<EF::priority>(99)));

        REQUIRE(result != nullptr);
        CHECK(result->region == "eu");

        // Independent verification via raw SQL
        auto dbResult = execQueryArgs(
            "SELECT region FROM relais_test_events WHERE id = $1", eventId);
        REQUIRE(dbResult.rows() == 1);
        CHECK(dbResult[0].get<std::string>(0) == "eu");
    }

    SECTION("[updateBy] returns re-fetched entity with all fields") {
        auto eventId = insertTestEvent("us", userId, "Before", 2);

        auto result = sync(UncachedTestEventRepository::updateBy(
            eventId, set<EF::title>(std::string("After"))));

        REQUIRE(result != nullptr);
        CHECK(result->id == eventId);
        CHECK(result->region == "us");
        CHECK(result->user_id == userId);
        CHECK(result->title == "After");
        CHECK(result->priority == 2);
        CHECK(!result->created_at.empty());
    }

    SECTION("[updateBy] returns nullptr for non-existent id") {
        auto result = sync(UncachedTestEventRepository::updateBy(
            999999, set<EF::title>(std::string("Ghost"))));

        CHECK(result == nullptr);
    }
}

TEST_CASE("PartialKey<TestEvent> - updateBy (L1)",
          "[integration][db][partial-key][cached][updateBy]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("l1updateby_user", "l1updateby@test.com", 100);

    SECTION("[updateBy] invalidates L1 and returns fresh entity") {
        auto eventId = insertTestEvent("eu", userId, "Cached", 5);

        // Populate L1 cache
        auto cached = sync(L1TestEventRepository::findById(eventId));
        REQUIRE(cached != nullptr);
        CHECK(cached->title == "Cached");

        // Modify directly in DB (bypass cache)
        updateTestEvent(eventId, "DB Changed", 99);

        // L1 still returns stale
        CHECK(sync(L1TestEventRepository::findById(eventId))->title == "Cached");

        // updateBy invalidates L1 and re-fetches
        auto result = sync(L1TestEventRepository::updateBy(
            eventId, set<EF::priority>(7)));

        REQUIRE(result != nullptr);
        CHECK(result->priority == 7);
        CHECK(result->title == "DB Changed");  // Re-fetched from DB, not stale L1
    }

    SECTION("[updateBy] updates multiple fields with L1 invalidation") {
        auto eventId = insertTestEvent("us", userId, "Multi", 1);

        // Populate L1
        sync(L1TestEventRepository::findById(eventId));

        auto result = sync(L1TestEventRepository::updateBy(
            eventId,
            set<EF::title>(std::string("New")),
            set<EF::priority>(8)));

        REQUIRE(result != nullptr);
        CHECK(result->title == "New");
        CHECK(result->priority == 8);
        CHECK(result->region == "us");
    }
}

TEST_CASE("PartialKey<TestEvent> - updateBy (L2)",
          "[integration][db][partial-key][redis][updateBy]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("l2updateby_user", "l2updateby@test.com", 100);

    SECTION("[updateBy] invalidates Redis then re-fetches") {
        auto eventId = insertTestEvent("eu", userId, "Redis Cached", 5);

        // Populate Redis
        sync(L2TestEventRepository::findById(eventId));

        // Modify in DB directly (bypass cache)
        updateTestEvent(eventId, "DB Changed", 99);

        // Redis still returns stale data
        auto stale = sync(L2TestEventRepository::findById(eventId));
        REQUIRE(stale != nullptr);
        CHECK(stale->title == "Redis Cached");

        // updateBy invalidates Redis, updates priority, then re-fetches from DB
        auto result = sync(L2TestEventRepository::updateBy(
            eventId, set<EF::priority>(42)));

        REQUIRE(result != nullptr);
        CHECK(result->priority == 42);
        CHECK(result->title == "DB Changed");  // Re-fetched from DB, not stale Redis

        // Independent fetch confirms correct state
        auto found = sync(L2TestEventRepository::findById(eventId));
        REQUIRE(found != nullptr);
        CHECK(found->priority == 42);
        CHECK(found->title == "DB Changed");
    }
}

TEST_CASE("PartialKey<TestEvent> - updateBy cross-invalidation",
          "[integration][db][partial-key][cross-inv][updateBy]")
{
    TransactionGuard tx;

    SECTION("[updateBy] on event invalidates user L1 cache") {
        auto userId = insertTestUser("crossinv_user", "crossinv@test.com", 1000);
        auto eventId = insertTestEvent("eu", userId, "Event", 1);

        // Cache user in L1
        auto user1 = sync(L1EventTargetUserRepository::findById(userId));
        REQUIRE(user1 != nullptr);
        CHECK(user1->balance == 1000);

        // Modify user balance directly in DB
        updateTestUserBalance(userId, 500);

        // User still cached (stale)
        CHECK(sync(L1EventTargetUserRepository::findById(userId))->balance == 1000);

        // updateBy on event → triggers cross-invalidation → invalidates user cache
        auto result = sync(L1EventSourceRepository::updateBy(
            eventId, set<EF::priority>(99)));
        REQUIRE(result != nullptr);

        // User L1 cache invalidated → fresh data
        auto user2 = sync(L1EventTargetUserRepository::findById(userId));
        REQUIRE(user2 != nullptr);
        CHECK(user2->balance == 500);
    }
}

// #############################################################################
//
//  9. remove — Opportunistic full PK via L1/L2 hint
//
// #############################################################################

TEST_CASE("PartialKey<TestEvent> - remove with L1 hint",
          "[integration][db][partial-key][cached]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("l1remove_user", "l1remove@test.com", 100);

    SECTION("[remove] succeeds when entity is in L1 cache (full PK path)") {
        auto eventId = insertTestEvent("eu", userId, "L1 Cached", 5);

        // Populate L1 cache
        sync(L1TestEventRepository::findById(eventId));

        // Remove (L1 hit → provides hint → full PK delete)
        auto result = sync(L1TestEventRepository::remove(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        // Verify deletion
        auto found = sync(L1TestEventRepository::findById(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[remove] succeeds when entity is NOT in L1 cache (criteria path)") {
        auto eventId = insertTestEvent("us", userId, "Not Cached", 3);

        // Remove without prior findById (no L1 hint → criteria-based)
        auto result = sync(L1TestEventRepository::remove(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        // Verify deletion
        auto found = sync(L1TestEventRepository::findById(eventId));
        CHECK(found == nullptr);
    }
}

TEST_CASE("PartialKey<TestEvent> - remove with L2 hint",
          "[integration][db][partial-key][redis]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("l2remove_user", "l2remove@test.com", 100);

    SECTION("[remove] succeeds when entity is in Redis (L2 hint path)") {
        auto eventId = insertTestEvent("eu", userId, "Redis Cached", 5);

        // Populate Redis cache
        sync(L2TestEventRepository::findById(eventId));

        // Remove (L2 hit → provides hint → full PK delete)
        auto result = sync(L2TestEventRepository::remove(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        // Verify deletion
        auto found = sync(L2TestEventRepository::findById(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[remove] succeeds when entity is NOT in Redis (criteria fallback)") {
        auto eventId = insertTestEvent("us", userId, "Not Cached", 3);

        // Ensure no Redis data
        flushRedis();

        // Remove (no L2 hint → criteria-based)
        auto result = sync(L2TestEventRepository::remove(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);
    }
}

TEST_CASE("PartialKey<TestEvent> - remove with L1+L2 hint chain",
          "[integration][db][partial-key][cached][redis]")
{
    TransactionGuard tx;
    auto userId = insertTestUser("bothremove_user", "bothremove@test.com", 100);

    SECTION("[remove] L1 hit provides hint (skips L2 check)") {
        auto eventId = insertTestEvent("eu", userId, "Both Cached", 5);

        // Populate L1 + L2
        sync(L1L2TestEventRepository::findById(eventId));

        // Remove (L1 hit → hint → full PK)
        auto result = sync(L1L2TestEventRepository::remove(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        auto found = sync(L1L2TestEventRepository::findById(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[remove] L1 miss, L2 hit provides hint") {
        auto eventId = insertTestEvent("us", userId, "L2 Only", 3);

        // Populate L1 + L2
        sync(L1L2TestEventRepository::findById(eventId));

        // Invalidate L1 only (L2 still has the entity)
        L1L2TestEventRepository::invalidateL1(eventId);

        // Remove (L1 miss → L2 hit → hint → full PK)
        auto result = sync(L1L2TestEventRepository::remove(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        auto found = sync(L1L2TestEventRepository::findById(eventId));
        CHECK(found == nullptr);
    }

    SECTION("[remove] both L1 and L2 miss - criteria fallback") {
        auto eventId = insertTestEvent("eu", userId, "No Cache", 1);

        // Ensure no L2
        flushRedis();

        // Remove (no L1, no L2 → criteria-based)
        auto result = sync(L1L2TestEventRepository::remove(eventId));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        auto found = sync(L1L2TestEventRepository::findById(eventId));
        CHECK(found == nullptr);
    }
}
