/**
 * test_simple_assigned_key.cpp
 * Regression tests for UPDATE on a simple, caller-assigned (non-db_managed) PK.
 *
 * The single-key fixtures (TestItem, TestUser, ...) all use a db_managed PK,
 * so toInsertParams already excludes the PK and coincides with toUpdateParams.
 * That coincidence hid a param off-by-one: updateOutcome used to fall back to
 * toInsertParams for simple keys, which begins with the PK column, shifting
 * every SET value by one slot. It only surfaced on an *assigned* simple PK.
 *
 * TestAssignedKey reproduces that layout: key_id (PK, not db_managed) followed
 * by payload — under the bug, payload received key_id's value.
 */

#include <catch2/catch_test_macros.hpp>
#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;
using jcailloux::relais::entity::set;

// #############################################################################
//
//  1. Generated SQL / params layout (no DB)
//
// #############################################################################

TEST_CASE("AssignedKey<TestAssignedKey> - UPDATE SQL/params layout",
          "[assigned-key][compile][sql]")
{
    using Mapping = entity::generated::TestAssignedKeyMapping;

    SECTION("update sets non-PK columns and keys on the PK") {
        std::string sql = Mapping::SQL::update;
        REQUIRE(sql.find("SET payload=$2, note=$3") != std::string::npos);
        REQUIRE(sql.find("WHERE key_id = $1") != std::string::npos);
    }

    SECTION("toUpdateParams excludes the PK (only SET fields, in order)") {
        // The exact regression: the PK must NOT lead the field params, or the
        // values shift against the "SET <non-pk>=$2.. WHERE <pk>=$1" layout.
        auto e = makeTestAssignedKey(7, 99, "hello");
        auto fieldParams = TestAssignedKeyEntity::toUpdateParams(e);
        CHECK(fieldParams.params.size() == 2);  // payload, note — NOT key_id

        // toInsertParams, by contrast, leads with the assigned PK.
        auto insertParams = TestAssignedKeyEntity::toInsertParams(e);
        CHECK(insertParams.params.size() == 3);  // key_id, payload, note
    }
}

// #############################################################################
//
//  2. Round-trip UPDATE (Uncached / PgRepo)
//
// #############################################################################

TEST_CASE("AssignedKey<TestAssignedKey> - update round-trip (Uncached)",
          "[integration][db][assigned-key]")
{
    TransactionGuard tx;

    SECTION("[update] writes the new value, leaves the PK intact") {
        sync(UncachedTestAssignedKeyRepo::insert(makeTestAssignedKey(5, 100, "a")));

        bool ok = sync(UncachedTestAssignedKeyRepo::update(
            5, makeTestAssignedKey(5, 200, "b")));
        REQUIRE(ok);

        auto found = sync(UncachedTestAssignedKeyRepo::find(5));
        REQUIRE(found != nullptr);
        CHECK(found->key_id == 5);     // PK unchanged
        CHECK(found->payload == 200);  // under the old bug this held key_id (5)
        CHECK(found->note == "b");
    }

    SECTION("[update] second update persists (the original symptom)") {
        // The reported symptom: first write OK, subsequent writes silently lost.
        sync(UncachedTestAssignedKeyRepo::insert(makeTestAssignedKey(6, 1, "x")));
        REQUIRE(sync(UncachedTestAssignedKeyRepo::update(6, makeTestAssignedKey(6, 2, "y"))));
        REQUIRE(sync(UncachedTestAssignedKeyRepo::update(6, makeTestAssignedKey(6, 3, "z"))));

        auto found = sync(UncachedTestAssignedKeyRepo::find(6));
        REQUIRE(found != nullptr);
        CHECK(found->payload == 3);
        CHECK(found->note == "z");
    }
}

// #############################################################################
//
//  3. Round-trip UPDATE through the full cache (L1+L2)
//
// #############################################################################

TEST_CASE("AssignedKey<TestAssignedKey> - update round-trip (L1+L2)",
          "[integration][db][assigned-key][cached][redis]")
{
    TransactionGuard tx;

    SECTION("[update] invalidates caches and reloads the new value") {
        sync(FullCacheTestAssignedKeyRepo::insert(makeTestAssignedKey(8, 10, "a")));
        sync(FullCacheTestAssignedKeyRepo::find(8));  // populate caches

        REQUIRE(sync(FullCacheTestAssignedKeyRepo::update(8, makeTestAssignedKey(8, 20, "b"))));

        auto found = sync(FullCacheTestAssignedKeyRepo::find(8));
        REQUIRE(found != nullptr);
        CHECK(found->key_id == 8);
        CHECK(found->payload == 20);
        CHECK(found->note == "b");
    }
}