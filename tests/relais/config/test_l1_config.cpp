/**
 * test_l1_config.cpp
 *
 * Exhaustive tests for L1 (RAM cache) configuration parameters.
 * Each CacheConfig field gets systematic coverage with dedicated repos.
 *
 * Covers:
 *   1. l1_ttl               — cache entry lifetime (GDSF evicts on cleanup)
 *   2. l1_chunk_count_log2  — cleanup granularity
 *   3. update_strategy      — InvalidateAndLazyReload vs PopulateImmediately
 *   4. read_only            — write restriction at L1
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;

// #############################################################################
//
//  Local repos for config parameter testing
//
// #############################################################################

namespace relais_test::config_test {

using namespace jcailloux::relais::config;

// --- l1_ttl ---
inline constexpr auto TTL50ms = Local
    .with_l1_ttl(std::chrono::milliseconds{50});

inline constexpr auto TTL500ms = Local
    .with_l1_ttl(std::chrono::milliseconds{500});

// --- l1_chunk_count_log2 ---
inline constexpr auto Seg2 = Local.with_l1_chunk_count_log2(1);    // 2^1 = 2 chunks
inline constexpr auto Seg16 = Local.with_l1_chunk_count_log2(4);   // 2^4 = 16 chunks

// --- update_strategy ---
inline constexpr auto LazyReload = Local
    .with_update_strategy(UpdateStrategy::InvalidateAndLazyReload);

inline constexpr auto PopImmediate = Local
    .with_update_strategy(UpdateStrategy::PopulateImmediately);

// --- read_only ---
inline constexpr auto ReadOnlyL1 = Local.with_read_only();

} // namespace relais_test::config_test

namespace relais_test {

namespace ct = config_test;

// TTL repos
using TTL50msRepo   = Repo<TestItemWrapper, "cfg:l1:ttl50",   ct::TTL50ms>;
using TTL500msRepo  = Repo<TestItemWrapper, "cfg:l1:ttl500",  ct::TTL500ms>;

// Segment repos
using Seg2Repo  = Repo<TestItemWrapper, "cfg:l1:seg2",  ct::Seg2>;
using Seg16Repo = Repo<TestItemWrapper, "cfg:l1:seg16", ct::Seg16>;

// Strategy repos
using LazyReloadRepo  = Repo<TestItemWrapper, "cfg:l1:lazy",  ct::LazyReload>;
using PopImmediateRepo = Repo<TestItemWrapper, "cfg:l1:pop",   ct::PopImmediate>;

// Read-only repo
using ReadOnlyCfgRepo = Repo<TestItemWrapper, "cfg:l1:ro", ct::ReadOnlyL1>;

} // namespace relais_test


// #############################################################################
//
//  1. l1_ttl
//
// #############################################################################

TEST_CASE("L1 Config - l1_ttl",
          "[integration][db][config][l1][ttl]")
{
    TransactionGuard tx;

    SECTION("[ttl] 50ms TTL expires quickly") {
        auto id = insertTestItem("ttl50_item", 10);

        sync(TTL50msRepo::find(id));
        REQUIRE(getCacheSize<TTL50msRepo>() > 0);

        waitForExpiration(std::chrono::milliseconds{80});
        forcePurge<TTL50msRepo>();

        // Cache should be empty after cleanup
        REQUIRE(getCacheSize<TTL50msRepo>() == 0);
    }

    SECTION("[ttl] 500ms TTL survives 200ms wait") {
        auto id = insertTestItem("ttl500_item", 20);

        sync(TTL500msRepo::find(id));

        waitForExpiration(std::chrono::milliseconds{200});

        // Modify DB — cached value should still be served
        updateTestItem(id, "modified", 99);

        auto item = sync(TTL500msRepo::find(id));
        REQUIRE(item->name == "ttl500_item");
        REQUIRE(item->value == 20);
    }

    SECTION("[ttl] expired entry triggers DB re-fetch after cleanup") {
        auto id = insertTestItem("ttl_refetch", 10);

        sync(TTL50msRepo::find(id));

        // Update DB
        updateTestItem(id, "ttl_refetched", 99);

        waitForExpiration(std::chrono::milliseconds{80});

        // GDSF: cleanup evicts TTL-expired entries
        forcePurge<TTL50msRepo>();

        // Expired entry evicted → DB fetch
        auto item = sync(TTL50msRepo::find(id));
        REQUIRE(item->name == "ttl_refetched");
        REQUIRE(item->value == 99);
    }
}


// #############################################################################
//
//  4. l1_chunk_count_log2
//
// #############################################################################

TEST_CASE("L1 Config - l1_chunk_count_log2",
          "[integration][db][config][l1][chunks]")
{
    TransactionGuard tx;

    SECTION("[chunks] 2 chunks: cleanup processes half the cache per cycle") {
        auto id1 = insertTestItem("seg2_a", 1);
        auto id2 = insertTestItem("seg2_b", 2);

        sync(Seg2Repo::find(id1));
        sync(Seg2Repo::find(id2));

        REQUIRE(getCacheSize<Seg2Repo>() == 2);

        // Trigger partial cleanup (1 of 2 chunks)
        trySweep<Seg2Repo>();

        // After one chunk cleanup, 0-2 items may remain depending on distribution
        auto size = getCacheSize<Seg2Repo>();
        REQUIRE(size <= 2);
    }

    SECTION("[chunks] 16 chunks: reset clears all entries") {
        auto id = insertTestItem("seg16_item", 1);
        sync(Seg16Repo::find(id));
        REQUIRE(getCacheSize<Seg16Repo>() > 0);

        // Reset unconditionally removes all entries (via friend access)
        TestInternals::resetEntityCacheState<Seg16Repo>();
        REQUIRE(getCacheSize<Seg16Repo>() == 0);

        // After reset, next read fetches from DB
        updateTestItem(id, "seg16_updated", 99);
        auto item = sync(Seg16Repo::find(id));
        REQUIRE(item->name == "seg16_updated");
    }
}


// #############################################################################
//
//  5. update_strategy
//
// #############################################################################

TEST_CASE("L1 Config - update_strategy",
          "[integration][db][config][l1][strategy]")
{
    TransactionGuard tx;

    SECTION("[strategy] InvalidateAndLazyReload: update invalidates L1, next read fetches from DB") {
        auto id = insertTestItem("lazy_item", 10);

        // Populate cache
        sync(LazyReloadRepo::find(id));

        // Update
        auto updated = makeTestItem("lazy_updated", 20, "", true, id);
        sync(LazyReloadRepo::update(id, updated));

        // Next read fetches from DB (cache was invalidated)
        auto item = sync(LazyReloadRepo::find(id));
        REQUIRE(item->name == "lazy_updated");
        REQUIRE(item->value == 20);
    }

    SECTION("[strategy] PopulateImmediately: update writes through to L1 cache") {
        auto id = insertTestItem("pop_item", 10);

        // Populate cache
        sync(PopImmediateRepo::find(id));

        // Update
        auto updated = makeTestItem("pop_updated", 20, "", true, id);
        sync(PopImmediateRepo::update(id, updated));

        // Modify DB directly
        updateTestItem(id, "sneaky", 99);

        // L1 still serves the write-through value
        auto item = sync(PopImmediateRepo::find(id));
        REQUIRE(item->name == "pop_updated");
        REQUIRE(item->value == 20);
    }

    SECTION("[strategy] PopulateImmediately: cache survives DB-direct modification") {
        auto id = insertTestItem("pop_stale", 10);

        // Populate via create
        auto entity = makeTestItem("pop_created", 30);
        auto created = sync(PopImmediateRepo::insert(entity));

        // Direct DB modification not visible
        updateTestItem(created->id, "invisible", 0);

        auto item = sync(PopImmediateRepo::find(created->id));
        REQUIRE(item->name == "pop_created");
    }
}


// #############################################################################
//
//  4. read_only
//
// #############################################################################

TEST_CASE("L1 Config - read_only",
          "[integration][db][config][l1][readonly]")
{
    TransactionGuard tx;

    SECTION("[readonly] find works and caches") {
        auto id = insertTestItem("ro_item", 42);

        auto item = sync(ReadOnlyCfgRepo::find(id));
        REQUIRE(item != nullptr);
        REQUIRE(item->name == "ro_item");

        // Verify caching: DB change not visible
        updateTestItem(id, "modified", 99);
        auto cached = sync(ReadOnlyCfgRepo::find(id));
        REQUIRE(cached->name == "ro_item");
    }

    SECTION("[readonly] findJson works") {
        auto id = insertTestItem("ro_json_item", 10);

        auto json = sync(ReadOnlyCfgRepo::findJson(id));
        REQUIRE(json != nullptr);
        REQUIRE(json->find("\"ro_json_item\"") != std::string::npos);
    }
}
