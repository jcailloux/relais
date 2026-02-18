/**
 * test_composite_key.cpp
 * Integration tests for composite key repositories.
 *
 * Tests CRUD, L1/L2 caching with std::tuple<int64_t, int64_t> as Key.
 */

#include <catch2/catch_test_macros.hpp>
#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;
using jcailloux::relais::wrapper::set;

// #############################################################################
//
//  1. Composite Key type verification
//
// #############################################################################

TEST_CASE("CompositeKey<TestMembership> - key() returns tuple",
          "[composite-key][compile]")
{
    TestMembershipWrapper m;
    m.user_id = 1;
    m.group_id = 2;
    auto k = m.key();

    STATIC_REQUIRE(std::is_same_v<decltype(k), std::tuple<int64_t, int64_t>>);
    CHECK(std::get<0>(k) == 1);
    CHECK(std::get<1>(k) == 2);
}

TEST_CASE("CompositeKey<TestMembership> - SQL strings",
          "[composite-key][compile][sql]")
{
    using Mapping = entity::generated::TestMembershipMapping;

    SECTION("select_by_pk uses both PK columns") {
        std::string sql = Mapping::SQL::select_by_pk;
        REQUIRE(sql.find("WHERE user_id = $1 AND group_id = $2") != std::string::npos);
    }

    SECTION("delete_by_pk uses both PK columns") {
        std::string sql = Mapping::SQL::delete_by_pk;
        REQUIRE(sql.find("WHERE user_id = $1 AND group_id = $2") != std::string::npos);
    }

    SECTION("update uses PK in WHERE and field in SET") {
        std::string sql = Mapping::SQL::update;
        REQUIRE(sql.find("SET role=$3") != std::string::npos);
        REQUIRE(sql.find("WHERE user_id = $1 AND group_id = $2") != std::string::npos);
    }

    SECTION("insert includes non-db_managed PK fields") {
        std::string sql = Mapping::SQL::insert;
        REQUIRE(sql.find("user_id, group_id, role") != std::string::npos);
        REQUIRE(sql.find("$1, $2, $3") != std::string::npos);
    }

    SECTION("primary_key_columns array") {
        STATIC_REQUIRE(Mapping::primary_key_columns.size() == 2);
        CHECK(std::string(Mapping::primary_key_columns[0]) == "user_id");
        CHECK(std::string(Mapping::primary_key_columns[1]) == "group_id");
    }
}

// #############################################################################
//
//  2. Composite Key CRUD (Uncached / BaseRepo)
//
// #############################################################################

TEST_CASE("CompositeKey<TestMembership> - CRUD (Uncached)",
          "[integration][db][composite-key]")
{
    TransactionGuard tx;

    using Key = std::tuple<int64_t, int64_t>;

    SECTION("[insert + find] basic round-trip") {
        auto wrapper = makeTestMembership(100, 200, "admin");
        auto inserted = sync(UncachedTestMembershipRepo::insert(wrapper));
        REQUIRE(inserted != nullptr);
        CHECK(inserted->user_id == 100);
        CHECK(inserted->group_id == 200);
        CHECK(inserted->role == "admin");
        CHECK(inserted->joined_at > 0);  // db_managed

        Key key{100, 200};
        auto found = sync(UncachedTestMembershipRepo::find(key));
        REQUIRE(found != nullptr);
        CHECK(found->user_id == 100);
        CHECK(found->group_id == 200);
        CHECK(found->role == "admin");
    }

    SECTION("[find] returns nullptr for non-existent composite key") {
        Key key{999, 888};
        auto found = sync(UncachedTestMembershipRepo::find(key));
        CHECK(found == nullptr);
    }

    SECTION("[update] updates entity by composite key") {
        auto wrapper = makeTestMembership(101, 201, "member");
        sync(UncachedTestMembershipRepo::insert(wrapper));

        auto updated = makeTestMembership(101, 201, "owner");
        Key key{101, 201};
        bool ok = sync(UncachedTestMembershipRepo::update(key, updated));
        REQUIRE(ok);

        auto found = sync(UncachedTestMembershipRepo::find(key));
        REQUIRE(found != nullptr);
        CHECK(found->role == "owner");
    }

    SECTION("[erase] deletes entity by composite key") {
        auto wrapper = makeTestMembership(102, 202, "viewer");
        sync(UncachedTestMembershipRepo::insert(wrapper));

        Key key{102, 202};
        auto result = sync(UncachedTestMembershipRepo::erase(key));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        auto found = sync(UncachedTestMembershipRepo::find(key));
        CHECK(found == nullptr);
    }

    SECTION("[erase] returns 0 for non-existent key") {
        Key key{999, 888};
        auto result = sync(UncachedTestMembershipRepo::erase(key));
        REQUIRE(result.has_value());
        CHECK(*result == 0);
    }

    SECTION("[patch] partial update by composite key") {
        auto wrapper = makeTestMembership(103, 203, "guest");
        sync(UncachedTestMembershipRepo::insert(wrapper));

        using F = TestMembershipWrapper::Field;
        Key key{103, 203};
        auto patched = sync(UncachedTestMembershipRepo::patch(key,
            set<F::role>(std::string("moderator"))));
        REQUIRE(patched != nullptr);
        CHECK(patched->role == "moderator");
        CHECK(patched->user_id == 103);
        CHECK(patched->group_id == 203);
    }

    SECTION("[insert] multiple memberships for same user") {
        sync(UncachedTestMembershipRepo::insert(makeTestMembership(104, 301, "admin")));
        sync(UncachedTestMembershipRepo::insert(makeTestMembership(104, 302, "member")));
        sync(UncachedTestMembershipRepo::insert(makeTestMembership(104, 303, "viewer")));

        auto m1 = sync(UncachedTestMembershipRepo::find(Key{104, 301}));
        auto m2 = sync(UncachedTestMembershipRepo::find(Key{104, 302}));
        auto m3 = sync(UncachedTestMembershipRepo::find(Key{104, 303}));

        REQUIRE(m1 != nullptr);
        REQUIRE(m2 != nullptr);
        REQUIRE(m3 != nullptr);
        CHECK(m1->role == "admin");
        CHECK(m2->role == "member");
        CHECK(m3->role == "viewer");
    }
}

// #############################################################################
//
//  3. Composite Key + L1 Cache
//
// #############################################################################

TEST_CASE("CompositeKey<TestMembership> - L1 Cache",
          "[integration][db][composite-key][cache]")
{
    TransactionGuard tx;

    using Key = std::tuple<int64_t, int64_t>;

    SECTION("[find] caches in L1 on first access") {
        insertTestMembership(110, 210, "admin");
        Key key{110, 210};

        // First find populates L1
        auto found = sync(L1TestMembershipRepo::find(key));
        REQUIRE(found != nullptr);
        CHECK(found->role == "admin");

        // Second find should come from L1
        auto cached = sync(L1TestMembershipRepo::find(key));
        REQUIRE(cached != nullptr);
        CHECK(cached->role == "admin");
    }

    SECTION("[insert] populates L1 cache") {
        auto wrapper = makeTestMembership(111, 211, "member");
        auto inserted = sync(L1TestMembershipRepo::insert(wrapper));
        REQUIRE(inserted != nullptr);

        Key key{111, 211};
        auto cached = TestInternals::getFromCache<L1TestMembershipRepo>(key);
        REQUIRE(cached != nullptr);
        CHECK(cached->role == "member");
    }

    SECTION("[erase] removes from L1 cache") {
        insertTestMembership(112, 212, "viewer");
        Key key{112, 212};

        // Populate L1
        sync(L1TestMembershipRepo::find(key));

        // Erase
        auto result = sync(L1TestMembershipRepo::erase(key));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        // L1 should be empty
        auto cached = TestInternals::getFromCache<L1TestMembershipRepo>(key);
        CHECK(cached == nullptr);
    }
}

// #############################################################################
//
//  4. Composite Key + L2 (Redis) Cache
//
// #############################################################################

TEST_CASE("CompositeKey<TestMembership> - L2 Cache",
          "[integration][db][composite-key][redis]")
{
    TransactionGuard tx;

    using Key = std::tuple<int64_t, int64_t>;

    SECTION("[find] caches in Redis") {
        insertTestMembership(120, 220, "admin");
        Key key{120, 220};

        auto found = sync(L2TestMembershipRepo::find(key));
        REQUIRE(found != nullptr);
        CHECK(found->role == "admin");

        // Verify Redis key format
        auto redisKey = L2TestMembershipRepo::makeRedisKey(key);
        CHECK(redisKey == "test:member:l2:120:220");
    }

    SECTION("[insert] populates Redis cache") {
        auto wrapper = makeTestMembership(121, 221, "member");
        auto inserted = sync(L2TestMembershipRepo::insert(wrapper));
        REQUIRE(inserted != nullptr);

        // Should be findable from Redis
        auto found = sync(L2TestMembershipRepo::find(Key{121, 221}));
        REQUIRE(found != nullptr);
        CHECK(found->role == "member");
    }

    SECTION("[erase] invalidates Redis cache") {
        insertTestMembership(122, 222, "viewer");
        Key key{122, 222};

        // Populate Redis
        sync(L2TestMembershipRepo::find(key));

        // Erase
        auto result = sync(L2TestMembershipRepo::erase(key));
        REQUIRE(result.has_value());
        CHECK(*result == 1);

        // Not found after erase
        auto found = sync(L2TestMembershipRepo::find(key));
        CHECK(found == nullptr);
    }
}

// #############################################################################
//
//  5. Composite Key + L1+L2 Cache
//
// #############################################################################

TEST_CASE("CompositeKey<TestMembership> - L1+L2 Cache",
          "[integration][db][composite-key][cached][redis]")
{
    TransactionGuard tx;

    using Key = std::tuple<int64_t, int64_t>;

    SECTION("[find] populates both L1 and L2") {
        insertTestMembership(130, 230, "admin");
        Key key{130, 230};

        auto found = sync(FullCacheTestMembershipRepo::find(key));
        REQUIRE(found != nullptr);
        CHECK(found->role == "admin");

        // Check L1
        auto cachedL1 = TestInternals::getFromCache<FullCacheTestMembershipRepo>(key);
        REQUIRE(cachedL1 != nullptr);
        CHECK(cachedL1->role == "admin");
    }

    SECTION("[update] invalidates caches") {
        insertTestMembership(131, 231, "member");
        Key key{131, 231};

        // Populate caches
        sync(FullCacheTestMembershipRepo::find(key));

        // Update
        auto updated = makeTestMembership(131, 231, "admin");
        bool ok = sync(FullCacheTestMembershipRepo::update(key, updated));
        REQUIRE(ok);

        // Find updated entity
        auto found = sync(FullCacheTestMembershipRepo::find(key));
        REQUIRE(found != nullptr);
        CHECK(found->role == "admin");
    }
}
