/**
 * test_l2_config.cpp
 *
 * Exhaustive tests for L2 (Redis cache) configuration parameters.
 * Each CacheConfig field gets systematic coverage with dedicated repos.
 *
 * Covers:
 *   1. l2_ttl            — Redis entry lifetime
 *   2. l2_refresh_on_get — GETEX TTL extension on read
 *   3. update_strategy   — InvalidateAndLazyReload vs PopulateImmediately at L2
 *   4. read_only         — write restriction at L2
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;

// #############################################################################
//
//  Local repos for L2 config parameter testing
//
// #############################################################################

namespace relais_test::l2_config_test {

using namespace jcailloux::relais::config;

// --- l2_ttl ---
inline constexpr auto ShortTTL = Redis
    .with_l2_ttl(std::chrono::seconds{1});

inline constexpr auto LongTTL = Redis
    .with_l2_ttl(std::chrono::seconds{30});

// --- l2_refresh_on_get ---
inline constexpr auto RefreshTrue = Redis
    .with_l2_ttl(std::chrono::seconds{1})
    .with_l2_refresh_on_get(true);

inline constexpr auto RefreshFalse = Redis
    .with_l2_ttl(std::chrono::seconds{1})
    .with_l2_refresh_on_get(false);

// --- update_strategy ---
inline constexpr auto LazyReload = Redis
    .with_update_strategy(UpdateStrategy::InvalidateAndLazyReload);

inline constexpr auto PopImmediate = Redis
    .with_update_strategy(UpdateStrategy::PopulateImmediately);

// --- read_only ---
inline constexpr auto ReadOnlyL2 = Redis.with_read_only();

} // namespace relais_test::l2_config_test

namespace relais_test {

namespace l2ct = l2_config_test;

// TTL repos
using L2ShortTTLRepo = Repo<TestItemEntity, "cfg:l2:ttl3s",  l2ct::ShortTTL>;
using L2LongTTLRepo  = Repo<TestItemEntity, "cfg:l2:ttl30s", l2ct::LongTTL>;

// Refresh repos
using L2RefreshTrueRepo  = Repo<TestItemEntity, "cfg:l2:refresh:t",  l2ct::RefreshTrue>;
using L2RefreshFalseRepo = Repo<TestItemEntity, "cfg:l2:refresh:f",  l2ct::RefreshFalse>;

// Strategy repos
using L2LazyReloadRepo  = Repo<TestItemEntity, "cfg:l2:lazy",  l2ct::LazyReload>;
using L2PopImmediateRepo = Repo<TestItemEntity, "cfg:l2:pop",   l2ct::PopImmediate>;

// Read-only repo
using L2ReadOnlyCfgRepo = Repo<TestItemEntity, "cfg:l2:ro", l2ct::ReadOnlyL2>;

} // namespace relais_test


// #############################################################################
//
//  1. l2_ttl
//
// #############################################################################

TEST_CASE("L2 Config - l2_ttl",
          "[integration][db][config][l2][ttl]")
{
    TransactionGuard tx;
    SECTION("[ttl] short TTL (1s): entry expires and re-fetched from DB") {
        auto id = insertTestItem("l2_ttl_short", 10);

        sync(L2ShortTTLRepo::find(id));

        // Update DB
        updateTestItem(id, "l2_ttl_updated", 99);

        // Wait for Redis TTL to expire (1s + margin)
        waitForExpiration(std::chrono::milliseconds{1500});

        // Should fetch from DB now
        auto item = sync(L2ShortTTLRepo::find(id));
        REQUIRE(item->name == "l2_ttl_updated");
        REQUIRE(item->value == 99);
    }

    SECTION("[ttl] long TTL: entry survives moderate wait") {
        auto id = insertTestItem("l2_ttl_long", 20);

        sync(L2LongTTLRepo::find(id));

        // Modify DB
        updateTestItem(id, "invisible", 99);

        // Wait a bit — should still be cached (30s TTL)
        waitForExpiration(std::chrono::milliseconds{500});

        auto item = sync(L2LongTTLRepo::find(id));
        REQUIRE(item->name == "l2_ttl_long");
        REQUIRE(item->value == 20);
    }
}


// #############################################################################
//
//  2. l2_refresh_on_get
//
// #############################################################################

TEST_CASE("L2 Config - l2_refresh_on_get",
          "[integration][db][config][l2][refresh]")
{
    TransactionGuard tx;

    SECTION("[refresh] true: GETEX extends TTL, entry survives past original expiry") {
        auto id = insertTestItem("l2_refresh_item", 10);

        // Populate cache (TTL = 1s)
        sync(L2RefreshTrueRepo::find(id));

        // Wait 700ms, then read (GETEX extends TTL by 1s from now)
        waitForExpiration(std::chrono::milliseconds{700});
        sync(L2RefreshTrueRepo::find(id));

        // Wait 700ms more (1.4s total > 1s original TTL)
        waitForExpiration(std::chrono::milliseconds{700});

        // Update DB
        updateTestItem(id, "l2_ref_modified", 99);

        // TTL was extended — should still serve old value
        auto item = sync(L2RefreshTrueRepo::find(id));
        REQUIRE(item->name == "l2_refresh_item");
        REQUIRE(item->value == 10);
    }

    SECTION("[refresh] false: GET does not extend TTL, entry expires on schedule") {
        auto id = insertTestItem("l2_noref_item", 10);

        // Populate cache (TTL = 1s)
        sync(L2RefreshFalseRepo::find(id));

        // Wait 700ms, read (plain GET, no TTL extension)
        waitForExpiration(std::chrono::milliseconds{700});
        sync(L2RefreshFalseRepo::find(id));

        // Wait 500ms more (1.2s total > 1s original TTL)
        waitForExpiration(std::chrono::milliseconds{500});

        // Update DB
        updateTestItem(id, "l2_noref_updated", 99);

        // TTL expired — should fetch from DB
        auto item = sync(L2RefreshFalseRepo::find(id));
        REQUIRE(item->name == "l2_noref_updated");
        REQUIRE(item->value == 99);
    }
}


// Multi-key counterpart: the batched L2 read (findManyRaw) must refresh every
// hit's TTL in one MGET round-trip (mgetRawEx fans out N GETEX via whenAll).
// Driven at the L2 tier (findManyRaw, via TestInternals) — a public findMany
// with L1 present would short-circuit on L1 and never re-touch Redis, so it
// could not exercise the GETEX path at all.
TEST_CASE("L2 Config - l2_refresh_on_get (multi-key MGET)",
          "[integration][db][config][l2][refresh][findmany]")
{
    TransactionGuard tx;

    SECTION("[refresh] true: gathered GETEX extends TTL for every key") {
        std::vector<int64_t> ids;
        for (int i = 0; i < 4; ++i)
            ids.push_back(insertTestItem("l2_mget_ref_" + std::to_string(i), i));

        // Populate L2 for all keys (TTL = 1s).
        for (auto id : ids) sync(L2RefreshTrueRepo::find(id));

        // Wait 700ms, then a multi-key read: mgetRawEx bumps every key's TTL by
        // 1s from now, in a single round-trip.
        waitForExpiration(std::chrono::milliseconds{700});
        {
            auto v = sync(TestInternals::findManyRaw<L2RefreshTrueRepo, int64_t>(ids));
            REQUIRE(v.size() == ids.size());
        }

        // 700ms more (1.4s total > 1s original TTL), then mutate the DB rows.
        waitForExpiration(std::chrono::milliseconds{700});
        for (auto id : ids)
            updateTestItem(id, "l2_mget_modified", 99);

        // Every key's TTL was extended → all still serve the cached value.
        auto v = sync(TestInternals::findManyRaw<L2RefreshTrueRepo, int64_t>(ids));
        REQUIRE(v.size() == ids.size());
        for (size_t i = 0; i < ids.size(); ++i) {
            REQUIRE(v[i].has_value());
            REQUIRE(v[i]->name == "l2_mget_ref_" + std::to_string(i));
            REQUIRE(v[i]->value == static_cast<int32_t>(i));
        }
    }

    SECTION("[refresh] false: plain MGET does not extend TTL, keys expire") {
        std::vector<int64_t> ids;
        for (int i = 0; i < 4; ++i)
            ids.push_back(insertTestItem("l2_mget_noref_" + std::to_string(i), i));

        for (auto id : ids) sync(L2RefreshFalseRepo::find(id));

        waitForExpiration(std::chrono::milliseconds{700});
        {
            auto v = sync(TestInternals::findManyRaw<L2RefreshFalseRepo, int64_t>(ids));
            REQUIRE(v.size() == ids.size());
        }

        // 500ms more (1.2s total > 1s TTL) — no refresh, keys have expired.
        waitForExpiration(std::chrono::milliseconds{500});
        for (auto id : ids)
            updateTestItem(id, "l2_mget_updated", 99);

        auto v = sync(TestInternals::findManyRaw<L2RefreshFalseRepo, int64_t>(ids));
        REQUIRE(v.size() == ids.size());
        for (size_t i = 0; i < ids.size(); ++i) {
            REQUIRE(v[i].has_value());
            REQUIRE(v[i]->name == "l2_mget_updated");
            REQUIRE(v[i]->value == 99);
        }
    }
}


// #############################################################################
//
//  3. update_strategy at L2
//
// #############################################################################

TEST_CASE("L2 Config - update_strategy at L2",
          "[integration][db][config][l2][strategy]")
{
    TransactionGuard tx;

    SECTION("[strategy] InvalidateAndLazyReload: update invalidates L2, next read re-fetches") {
        auto id = insertTestItem("l2_lazy_item", 10);

        // Populate cache
        sync(L2LazyReloadRepo::find(id));

        // Update via repo
        auto updated = makeTestItem("l2_lazy_updated", 20, "", true, id);
        sync(L2LazyReloadRepo::update(id, updated));

        // Next read should get updated value from DB
        auto item = sync(L2LazyReloadRepo::find(id));
        REQUIRE(item->name == "l2_lazy_updated");
        REQUIRE(item->value == 20);
    }

    SECTION("[strategy] PopulateImmediately: update writes through to L2") {
        auto id = insertTestItem("l2_pop_item", 10);

        // Populate cache
        sync(L2PopImmediateRepo::find(id));

        // Update via repo (write-through)
        auto updated = makeTestItem("l2_pop_updated", 20, "", true, id);
        sync(L2PopImmediateRepo::update(id, updated));

        // Modify DB directly
        updateTestItem(id, "sneaky", 99);

        // L2 should serve the write-through value
        auto item = sync(L2PopImmediateRepo::find(id));
        REQUIRE(item->name == "l2_pop_updated");
        REQUIRE(item->value == 20);
    }
}


// #############################################################################
//
//  4. read_only
//
// #############################################################################

TEST_CASE("L2 Config - read_only",
          "[integration][db][config][l2][readonly]")
{
    TransactionGuard tx;

    SECTION("[readonly] find works and caches in Redis") {
        auto id = insertTestItem("l2_ro_item", 42);

        auto item = sync(L2ReadOnlyCfgRepo::find(id));
        REQUIRE(item != nullptr);
        REQUIRE(item->name == "l2_ro_item");

        // Verify caching: DB change not visible
        updateTestItem(id, "modified", 99);
        auto cached = sync(L2ReadOnlyCfgRepo::find(id));
        REQUIRE(cached->name == "l2_ro_item");
    }

    SECTION("[readonly] findJson works") {
        auto id = insertTestItem("l2_ro_json", 10);

        auto json = sync(L2ReadOnlyCfgRepo::findJson(id));
        REQUIRE(!json.empty());
        REQUIRE(json.find("\"l2_ro_json\"") != std::string::npos);
    }
}
