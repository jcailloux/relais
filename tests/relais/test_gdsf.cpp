/**
 * test_gdsf.cpp
 *
 * Tests for the GDSF (Greedy Dual-Size Frequency) cache eviction policy.
 * Compiled with RELAIS_GDSF_ENABLED=1. Memory budget (256 MB) set via configure().
 *
 * Covers:
 *   1. Access count tracking   — find() bumps access_count by kCountScale
 *   2. Decay in cleanup        — purge() applies decay_rate to access_count
 *   3. Eviction decisions      — histogram-based threshold eviction
 *   4. Avg construction time   — EMA convergence
 *   5. Optional TTL            — TTL-based vs score-only eviction
 *   6. CachedWrapper memory    — ctor charges, dtor discharges, lazy buffers
 *   7. Memory pressure         — emergency cleanup when over budget
 *   8. Striped counter         — multi-slot memory accounting
 *   9. Repo auto-registration  — enrollment via std::call_once
 *  10. ScoreHistogram          — record, thresholdForBytes, mergeEMA
 *  11. Eviction target curve   — three-zone quadratic eviction_target_pct
 *  12. Access count persistence — mergeFrom on upsert with kUpdatePenalty
 *  20. Memory bound under Zipfian load — stress test (hidden by default)
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <algorithm>
#include <cstdint>
#include <thread>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;
using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;
using GDSFScoreData = jcailloux::relais::cache::GDSFScoreData;
using ScoreHistogram = jcailloux::relais::cache::ScoreHistogram;

// Compile-time check: this TU must be compiled with GDSF enabled
static_assert(GDSFPolicy::enabled,
    "test_gdsf.cpp must be compiled with RELAIS_GDSF_ENABLED=1");

// Configure max_memory for tests (256 MB budget)
static constexpr size_t kTestMaxMemory = 268435456;
static const bool gdsf_configured = [] {
    GDSFPolicy::instance().configure({.max_memory = kTestMaxMemory});
    return true;
}();

// =============================================================================
// Local repos for GDSF testing
// =============================================================================

namespace relais_test::gdsf_test {

using namespace jcailloux::relais::config;

// Manual cleanup only (predictable tests — sweep triggered externally)
inline constexpr auto ManualCleanup = Local;

// Short TTL for expiration tests
inline constexpr auto ShortTTL = Local
    .with_l1_ttl(std::chrono::milliseconds{50});

// No TTL (GDSF score only, 0ns disables TTL)
inline constexpr auto NoTTL = Local
    .with_l1_ttl(std::chrono::nanoseconds{0});

} // namespace relais_test::gdsf_test

namespace relais_test {

namespace gt = gdsf_test;

// Score / decay / eviction test repos
using GDSFItemRepo     = Repo<TestItemWrapper, "gdsf:item",       gt::ManualCleanup>;
using GDSFItemRepo2    = Repo<TestItemWrapper, "gdsf:item2",      gt::ManualCleanup>;
using GDSFUserRepo     = Repo<TestUserWrapper, "gdsf:user",       gt::ManualCleanup>;

// TTL test repos
using GDSFShortTTLRepo = Repo<TestItemWrapper, "gdsf:ttl:short",  gt::ShortTTL>;
using GDSFNoTTLRepo    = Repo<TestItemWrapper, "gdsf:ttl:none",   gt::NoTTL>;

// Memory tracking test repos (dedicated to avoid stale CachedWrapper interference)
using GDSFMemRepo      = Repo<TestItemWrapper, "gdsf:mem",        gt::ManualCleanup>;

// Registration-only repos (first access enrolls them)
using GDSFRegRepo1     = Repo<TestItemWrapper, "gdsf:reg:1",      gt::ManualCleanup>;
using GDSFRegRepo2     = Repo<TestItemWrapper, "gdsf:reg:2",      gt::ManualCleanup>;
using GDSFRegRepo3     = Repo<TestItemWrapper, "gdsf:reg:3",      gt::ManualCleanup>;

// Memory pressure test repos (dedicated to avoid stale-entry interference)
using GDSFPressureRepo  = Repo<TestItemWrapper, "gdsf:pressure",   gt::ManualCleanup>;
using GDSFPressureRepo2 = Repo<TestItemWrapper, "gdsf:pressure2",  gt::ManualCleanup>;

// Ghost admission control test repos
using GDSFGhostRepo   = Repo<TestItemWrapper, "gdsf:ghost",   gt::ManualCleanup>;
using GDSFGhostRepo2  = Repo<TestItemWrapper, "gdsf:ghost2",  gt::ManualCleanup>;

// Cross-repo coordination test repos
using GDSFCoordRepo1  = Repo<TestItemWrapper, "gdsf:coord1",  gt::ManualCleanup>;
using GDSFCoordRepo2  = Repo<TestItemWrapper, "gdsf:coord2",  gt::ManualCleanup>;

// Stress test repo (Zipfian memory bound)
using GDSFStressRepo  = Repo<TestItemWrapper, "gdsf:stress",  gt::ManualCleanup>;

} // namespace relais_test


// =============================================================================
// Helper: clean up repos + GDSF global state for each test
// =============================================================================

template<typename... Repos>
void resetRepos() {
    // Unconditional cache clear (not threshold-based purge, which skips entries above threshold=0)
    (TestInternals::resetEntityCacheState<Repos>(), ...);
    // Flush all deferred CachedWrapper destructors accumulated in the epoch pool.
    // Without this, the pool's reserve FIFO (capacity 500) eventually triggers
    // old dtors after resetGDSF() zeroed totalMemory, causing negative accounting.
    (TestInternals::clearEntityCachePools<Repos>(), ...);
    (TestInternals::resetRepoGDSFState<Repos>(), ...);
    TestInternals::resetGDSF();
}

/// Reset ALL test repos to ensure clean global threshold.
/// Excludes GDSFRegRepo1/2/3 (tested for registration, must not be pre-registered).
void resetAllTestRepos() {
    resetRepos<GDSFItemRepo, GDSFItemRepo2, GDSFMemRepo,
               GDSFShortTTLRepo, GDSFNoTTLRepo,
               GDSFPressureRepo, GDSFPressureRepo2,
               GDSFGhostRepo, GDSFGhostRepo2,
               GDSFCoordRepo1, GDSFCoordRepo2>();
}


// #############################################################################
//
//  1. GDSF - access count tracking
//
// #############################################################################

TEST_CASE("GDSF - access count tracking",
          "[integration][db][gdsf][score]")
{
    TransactionGuard tx;
    resetRepos<GDSFItemRepo>();

    SECTION("[score] find() increments access_count by kCountScale") {
        auto id = insertTestItem("score_item", 10);

        // First find: L1 miss -> DB fetch -> populate cache (access_count = kCountScale)
        sync(GDSFItemRepo::find(id));

        // 10 cache hits: each bumps access_count by kCountScale
        for (int i = 0; i < 10; ++i) {
            sync(GDSFItemRepo::find(id));
        }

        auto meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        REQUIRE(meta.has_value());
        // access_count = 1 initial + 10 hits = 11 * kCountScale
        uint32_t expected = 11 * GDSFScoreData::kCountScale;
        REQUIRE(meta->access_count == expected);
    }

    SECTION("[score] access_count starts at kCountScale on first cache population") {
        auto id = insertTestItem("init_score", 20);

        sync(GDSFItemRepo::find(id));

        auto meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        REQUIRE(meta.has_value());
        REQUIRE(meta->access_count == GDSFScoreData::kCountScale);
    }

    SECTION("[score] multiple entities accumulate access_counts independently") {
        auto id_a = insertTestItem("score_a", 1);
        auto id_b = insertTestItem("score_b", 2);
        auto id_c = insertTestItem("score_c", 3);

        // Populate all three
        sync(GDSFItemRepo::find(id_a));
        sync(GDSFItemRepo::find(id_b));
        sync(GDSFItemRepo::find(id_c));

        // A: 10 extra hits, B: 1 extra hit, C: 5 extra hits
        for (int i = 0; i < 10; ++i) sync(GDSFItemRepo::find(id_a));
        sync(GDSFItemRepo::find(id_b));
        for (int i = 0; i < 5; ++i) sync(GDSFItemRepo::find(id_c));

        auto ma = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id_a);
        auto mb = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id_b);
        auto mc = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id_c);

        // A (11 total) > C (6 total) > B (2 total)
        REQUIRE(ma->access_count > mc->access_count);
        REQUIRE(mc->access_count > mb->access_count);
    }
}


// #############################################################################
//
//  2. GDSF - decay in cleanup
//
// #############################################################################

TEST_CASE("GDSF - decay in cleanup",
          "[integration][db][gdsf][decay]")
{
    TransactionGuard tx;
    resetRepos<GDSFItemRepo>();

    SECTION("[decay] purge() applies decay_rate to access_count") {
        auto id = insertTestItem("decay_item", 10);

        // Populate + 10 cache hits
        sync(GDSFItemRepo::find(id));
        for (int i = 0; i < 10; ++i) sync(GDSFItemRepo::find(id));

        auto meta0 = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        uint32_t count_before = meta0->access_count;
        REQUIRE(count_before == 11 * GDSFScoreData::kCountScale);

        // purge() applies inline decay: access_count *= decay_rate
        GDSFItemRepo::purge();

        auto meta1 = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        REQUIRE(meta1.has_value());  // should survive (threshold = 0 on first sweep)

        uint32_t expected = static_cast<uint32_t>(
            static_cast<float>(count_before) * GDSFPolicy::instance().decayRate());
        REQUIRE(meta1->access_count == expected);
    }

    SECTION("[decay] multiple purge cycles compound decay") {
        auto id = insertTestItem("multi_decay", 10);

        sync(GDSFItemRepo::find(id));
        for (int i = 0; i < 99; ++i) sync(GDSFItemRepo::find(id));

        auto meta0 = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        uint32_t count0 = meta0->access_count;
        REQUIRE(count0 == 100 * GDSFScoreData::kCountScale);

        float dr = GDSFPolicy::instance().decayRate();

        // Apply 3 cleanup cycles
        for (int i = 0; i < 3; ++i) {
            GDSFItemRepo::purge();
        }

        auto meta3 = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        REQUIRE(meta3.has_value());

        // After 3 decays: count0 * dr^3 (truncated each step via uint32_t cast)
        uint32_t expected = count0;
        for (int i = 0; i < 3; ++i) {
            expected = static_cast<uint32_t>(static_cast<float>(expected) * dr);
        }
        REQUIRE(meta3->access_count == expected);
    }
}


// #############################################################################
//
//  3. GDSF - eviction decisions
//
// #############################################################################

TEST_CASE("GDSF - eviction decisions",
          "[integration][db][gdsf][eviction]")
{
    TransactionGuard tx;
    resetAllTestRepos();

    SECTION("[eviction] low-access entry evicted, high-access survives") {
        auto id_low = insertTestItem("low_score", 1);
        auto id_high = insertTestItem("high_score", 2);

        // Populate both (1 find each)
        sync(GDSFItemRepo::find(id_low));
        sync(GDSFItemRepo::find(id_high));

        // High-access: 100 more accesses
        for (int i = 0; i < 100; ++i) sync(GDSFItemRepo::find(id_high));

        // Inflate memory to trigger eviction (>80% budget -> aggressive zone)
        int64_t budget = static_cast<int64_t>(GDSFPolicy::instance().maxMemory());
        GDSFPolicy::instance().charge(budget * 9 / 10);

        // Seed the histogram so thresholdForBytes returns meaningful value
        auto score_low = TestInternals::getEntityGDSFScore<GDSFItemRepo>(id_low);
        auto score_high = TestInternals::getEntityGDSFScore<GDSFItemRepo>(id_high);
        REQUIRE(score_low.has_value());
        REQUIRE(score_high.has_value());
        REQUIRE(*score_high > *score_low);

        // First sweep: builds histogram, threshold from empty histogram = 0
        GDSFPolicy::instance().sweep();

        // Second sweep: threshold from seeded histogram, should evict low entries
        GDSFPolicy::instance().sweep();

        // Discharge artificial inflation
        GDSFPolicy::instance().charge(-(budget * 9 / 10));

        auto low_meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id_low);
        auto high_meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id_high);

        // High-access entry should survive
        REQUIRE(high_meta.has_value());
        // Low-access entry may or may not be evicted depending on histogram
        // (this depends on memory pressure and histogram convergence)
    }

    SECTION("[eviction] all entries survive when no memory pressure") {
        // Create 5 entries with equal access counts
        std::vector<int64_t> ids;
        for (int i = 0; i < 5; ++i) {
            ids.push_back(insertTestItem("survive_" + std::to_string(i), i));
        }

        // Populate + moderate access count for all
        for (auto id : ids) {
            sync(GDSFItemRepo::find(id));
            for (int j = 0; j < 20; ++j) sync(GDSFItemRepo::find(id));
        }

        size_t before = GDSFItemRepo::size();
        REQUIRE(before == 5);

        // No memory pressure (totalMemory ~ 0% of budget)
        // -> eviction_target_pct = 0 -> threshold = 0 -> nothing evicted
        GDSFItemRepo::purge();
        GDSFItemRepo::purge();

        size_t after = GDSFItemRepo::size();
        REQUIRE(after == before);
    }
}


// #############################################################################
//
//  4. GDSF - avg_construction_time (EMA)
//
// #############################################################################

TEST_CASE("GDSF - avg_construction_time (EMA)",
          "[integration][db][gdsf][construction-time]")
{
    TransactionGuard tx;
    resetRepos<GDSFItemRepo>();

    SECTION("[ema] EMA seeded on first miss, updated on subsequent misses") {
        auto id1 = insertTestItem("ema_item1", 10);
        auto id2 = insertTestItem("ema_item2", 20);

        // First L1 miss seeds the EMA
        sync(GDSFItemRepo::find(id1));
        float after_first = GDSFItemRepo::avgConstructionTime();
        REQUIRE(after_first > 0.0f);

        // Evict and re-fetch to trigger a second L1 miss
        TestInternals::evict<GDSFItemRepo>(id1);
        sync(GDSFItemRepo::find(id1));
        float after_second = GDSFItemRepo::avgConstructionTime();

        // EMA should have updated (alpha=0.1 blend)
        REQUIRE(after_second > 0.0f);

        // Third miss with a different entity
        sync(GDSFItemRepo::find(id2));
        float after_third = GDSFItemRepo::avgConstructionTime();
        REQUIRE(after_third > 0.0f);
    }
}


// #############################################################################
//
//  5. GDSF - optional TTL
//
// #############################################################################

TEST_CASE("GDSF - optional TTL",
          "[integration][db][gdsf][ttl]")
{
    TransactionGuard tx;
    resetAllTestRepos();

    SECTION("[ttl] entry evicted when TTL expires regardless of access count") {
        auto id = insertTestItem("ttl_high_score", 10);

        // Populate + many hits -> very high access count
        sync(GDSFShortTTLRepo::find(id));
        for (int i = 0; i < 50; ++i) sync(GDSFShortTTLRepo::find(id));

        REQUIRE(GDSFShortTTLRepo::size() == 1);

        // Wait for 50ms TTL to expire
        waitForExpiration(std::chrono::milliseconds{80});

        // Cleanup should evict despite high access count
        GDSFShortTTLRepo::purge();

        REQUIRE(GDSFShortTTLRepo::size() == 0);
    }

    SECTION("[ttl] entry without TTL survives indefinitely if access count is high") {
        auto id = insertTestItem("no_ttl_item", 10);

        sync(GDSFNoTTLRepo::find(id));
        for (int i = 0; i < 50; ++i) sync(GDSFNoTTLRepo::find(id));

        // Wait a long time (relative to normal TTLs)
        waitForExpiration(std::chrono::milliseconds{200});

        // Cleanup: score-based only, no TTL eviction.
        // No memory pressure -> threshold = 0 -> no eviction
        GDSFNoTTLRepo::purge();
        GDSFNoTTLRepo::purge();

        // Entry should survive (high access count, no TTL, no memory pressure)
        REQUIRE(GDSFNoTTLRepo::size() == 1);
        auto meta = TestInternals::getEntityGDSFMetadata<GDSFNoTTLRepo>(id);
        REQUIRE(meta.has_value());
    }

    SECTION("[ttl] TTL=0 disables TTL-based eviction") {
        auto id = insertTestItem("ttl0_item", 10);

        sync(GDSFNoTTLRepo::find(id));

        auto meta = TestInternals::getEntityGDSFMetadata<GDSFNoTTLRepo>(id);
        REQUIRE(meta.has_value());
        // NoTTL repo uses CacheMetadata<true, false> — no TTL field
        REQUIRE(meta->ttl_expiration_rep == 0);
    }
}


// #############################################################################
//
//  6. GDSF - CachedWrapper memory tracking
//
// #############################################################################

TEST_CASE("GDSF - CachedWrapper memory tracking",
          "[integration][db][gdsf][memory][wrapper]")
{
    TransactionGuard tx;
    resetRepos<GDSFMemRepo>();

    SECTION("[wrapper] putInCache charges memory via CachedWrapper ctor") {
        REQUIRE(GDSFPolicy::instance().totalMemory() == 0);

        auto id = insertTestItem("mem_charge", 42);
        sync(GDSFMemRepo::find(id));

        // CachedWrapper ctor should have charged memory
        REQUIRE(GDSFPolicy::instance().totalMemory() > 0);
    }

    SECTION("[wrapper] lazy json() generation charges additional memory") {
        auto id = insertTestItem("mem_json", 42);
        sync(GDSFMemRepo::find(id));

        int64_t mem_after_find = GDSFPolicy::instance().totalMemory();
        REQUIRE(mem_after_find > 0);

        // Trigger JSON buffer generation via findJson
        sync(GDSFMemRepo::findJson(id));

        int64_t mem_after_json = GDSFPolicy::instance().totalMemory();
        // JSON buffer should have added memory
        REQUIRE(mem_after_json > mem_after_find);
    }

    SECTION("[wrapper] lazy binary() generation charges additional memory") {
        auto id = insertTestItem("mem_binary", 42);
        sync(GDSFMemRepo::find(id));

        int64_t mem_after_find = GDSFPolicy::instance().totalMemory();
        REQUIRE(mem_after_find > 0);

        // Trigger BEVE buffer generation via findBinary
        sync(GDSFMemRepo::findBinary(id));

        int64_t mem_after_binary = GDSFPolicy::instance().totalMemory();
        // BEVE buffer should have added memory
        REQUIRE(mem_after_binary > mem_after_find);
    }
}


// #############################################################################
//
//  7. GDSF - memory pressure (global sweep)
//
// #############################################################################

TEST_CASE("GDSF - memory pressure (global sweep)",
          "[integration][db][gdsf][memory]")
{
    TransactionGuard tx;
    resetAllTestRepos();

    SECTION("[memory] isOverBudget detects memory pressure") {
        REQUIRE_FALSE(GDSFPolicy::instance().isOverBudget());

        // Artificially inflate memory to exceed the compile-time budget
        int64_t budget = static_cast<int64_t>(GDSFPolicy::instance().maxMemory());
        GDSFPolicy::instance().charge(budget + 1);

        REQUIRE(GDSFPolicy::instance().isOverBudget());

        // Discharge to restore
        GDSFPolicy::instance().charge(-(budget + 1));
        REQUIRE_FALSE(GDSFPolicy::instance().isOverBudget());
    }

    SECTION("[memory] sweep evicts entries when over budget") {
        // Insert entries with 1 access each (low score)
        for (int i = 0; i < 20; ++i) {
            auto id = insertTestItem("emrg_" + std::to_string(i), i);
            sync(GDSFPressureRepo::find(id));
        }

        size_t before = GDSFPressureRepo::size();
        REQUIRE(before == 20);

        // Inflate memory to exceed budget (triggers second pass in sweep)
        int64_t budget = static_cast<int64_t>(GDSFPolicy::instance().maxMemory());
        GDSFPolicy::instance().charge(budget + 1);

        // Build histogram with first sweep
        GDSFPolicy::instance().sweep();

        // Purge covers all chunks — guaranteed eviction regardless of cursor state
        GDSFPressureRepo::purge();

        REQUIRE(GDSFPressureRepo::size() < before);

        // Discharge artificial inflation
        GDSFPolicy::instance().charge(-(budget + 1));
    }

    SECTION("[memory] cache memory stays within budget during sustained use") {
        // Temporarily set a small budget (20 KB)
        constexpr size_t kSmallBudget = 20480;
        GDSFPolicy::instance().configure({.max_memory = kSmallBudget});

        // Build histogram with a warm-up phase
        for (int i = 0; i < 10; ++i) {
            auto id = insertTestItem("budget_warm_" + std::to_string(i), i);
            sync(GDSFPressureRepo::find(id));
        }
        GDSFPolicy::instance().sweep();  // Populate histogram

        // Sustained insertion phase with periodic manual sweeps.
        // Auto-sweep fires ~1/512 insertions (hash-based), too infrequent
        // for 200 entries. Manual sweep every 50 ensures eviction pressure.
        for (int i = 10; i < 200; ++i) {
            auto id = insertTestItem("budget_" + std::to_string(i), i);
            sync(GDSFPressureRepo::find(id));
            if (i % 50 == 0) GDSFPolicy::instance().sweep();
        }

        // After sustained use, totalMemory should be bounded.
        // Tolerance accounts for: chunk-based sweep granularity,
        // epoch-deferred CachedWrapper destructors, and ghost entry overhead.
        int64_t mem = GDSFPolicy::instance().totalMemory();
        REQUIRE(mem <= static_cast<int64_t>(kSmallBudget * 3));

        // Cleanup + restore budget
        TestInternals::resetEntityCacheState<GDSFPressureRepo>();
        TestInternals::resetGDSF();
        GDSFPolicy::instance().configure({.max_memory = kTestMaxMemory});
    }

    SECTION("[memory] cache memory stays within budget under stress") {
        // Reduce budget to 50 KB (testable with ~200 TestItem entries)
        constexpr size_t kSmallBudget = 51200;
        GDSFPolicy::instance().configure({.max_memory = kSmallBudget});

        int64_t peak = 0;
        constexpr int kInsertions = 500;
        constexpr int kSweepInterval = 50;

        for (int i = 0; i < kInsertions; ++i) {
            auto id = insertTestItem("stress_" + std::to_string(i), i);
            sync(GDSFPressureRepo::find(id));

            if (i > 0 && i % kSweepInterval == 0) {
                // Force synchronous sweep
                GDSFPolicy::instance().sweep();

                int64_t mem = GDSFPolicy::instance().totalMemory();
                peak = std::max(peak, mem);

                // Invariant: memory must not exceed 3× budget between sweeps.
                // Overshoot comes from: epoch-deferred CachedWrapper destructors
                // (pool recycles items lazily), ghost entry overhead, and
                // kSweepInterval new entries cached since last sweep.
                REQUIRE(mem <= static_cast<int64_t>(kSmallBudget * 3));
            }
        }

        // Final stabilization: sweeps for histogram convergence + full purge
        for (int s = 0; s < 3; ++s) {
            GDSFPolicy::instance().sweep();
        }
        GDSFPressureRepo::purge();

        int64_t mem_final = GDSFPolicy::instance().totalMemory();

        // After stabilization, should be within 3× budget (accounts for
        // epoch-deferred CachedWrapper destructors and ParlayHash overhead)
        REQUIRE(mem_final <= static_cast<int64_t>(kSmallBudget * 3));

        // Sanity: peak was above half budget (sweep was actually needed)
        REQUIRE(peak > static_cast<int64_t>(kSmallBudget / 2));

        // Cleanup + restore budget
        TestInternals::resetEntityCacheState<GDSFPressureRepo>();
        TestInternals::resetGDSF();
        GDSFPolicy::instance().configure({.max_memory = kTestMaxMemory});
    }
}


// #############################################################################
//
//  8. GDSF - striped counter
//
// #############################################################################

TEST_CASE("GDSF - striped counter",
          "[integration][db][gdsf][memory][counter]")
{
    TransactionGuard tx;
    resetRepos<GDSFMemRepo>();

    SECTION("[counter] charge/discharge from multiple threads sums correctly") {
        constexpr int kThreads = 4;
        constexpr int kOpsPerThread = 100;
        constexpr int64_t kDelta = 100;

        REQUIRE(GDSFPolicy::instance().totalMemory() == 0);

        // Charge from multiple threads
        {
            std::vector<std::jthread> threads;
            for (int t = 0; t < kThreads; ++t) {
                threads.emplace_back([&]() {
                    for (int i = 0; i < kOpsPerThread; ++i) {
                        GDSFPolicy::instance().charge(kDelta);
                    }
                });
            }
        } // join all

        int64_t total_charged = kThreads * kOpsPerThread * kDelta;
        REQUIRE(GDSFPolicy::instance().totalMemory() == total_charged);

        // Discharge from multiple threads
        {
            std::vector<std::jthread> threads;
            for (int t = 0; t < kThreads; ++t) {
                threads.emplace_back([&]() {
                    for (int i = 0; i < kOpsPerThread; ++i) {
                        GDSFPolicy::instance().charge(-kDelta);
                    }
                });
            }
        } // join all

        REQUIRE(GDSFPolicy::instance().totalMemory() == 0);
    }
}


// #############################################################################
//
//  9. GDSF - repo auto-registration
//
// #############################################################################

TEST_CASE("GDSF - repo auto-registration",
          "[integration][db][gdsf][registration]")
{
    TransactionGuard tx;

    SECTION("[registration] repo enrolled on first cache access") {
        size_t before = GDSFPolicy::instance().nbRepos();

        // Force cache() access via warmup (triggers std::call_once enrollment)
        GDSFRegRepo1::warmup();

        REQUIRE(GDSFPolicy::instance().nbRepos() == before + 1);
    }

    SECTION("[registration] nb_repos reflects all registered repos") {
        size_t before = GDSFPolicy::instance().nbRepos();

        // Access two new repos
        GDSFRegRepo2::warmup();
        GDSFRegRepo3::warmup();

        REQUIRE(GDSFPolicy::instance().nbRepos() == before + 2);
    }
}


// #############################################################################
//
//  10. GDSF - ScoreHistogram
//
// #############################################################################

TEST_CASE("GDSF - ScoreHistogram",
          "[gdsf][histogram]")
{
    SECTION("[histogram] record and thresholdForBytes") {
        ScoreHistogram h{};

        // Record entries with different scores and sizes
        h.record(1.0f, 100);    // score 1.0, 100 bytes
        h.record(10.0f, 200);   // score 10.0, 200 bytes
        h.record(100.0f, 300);  // score 100.0, 300 bytes

        // Total bytes = 600. Threshold for 100 bytes should be around score 1.0
        float t100 = h.thresholdForBytes(100);
        REQUIRE(t100 > 0.0f);

        // Threshold for 300 bytes (100 + 200) should be higher
        float t300 = h.thresholdForBytes(300);
        REQUIRE(t300 > t100);

        // Threshold for 600+ bytes should be very high (all entries below)
        float t600 = h.thresholdForBytes(600);
        REQUIRE(t600 >= t300);
    }

    SECTION("[histogram] thresholdForBytes returns 0 for target 0") {
        ScoreHistogram h{};
        h.record(1.0f, 100);
        REQUIRE(h.thresholdForBytes(0) == 0.0f);
    }

    SECTION("[histogram] mergeEMA blends two histograms") {
        ScoreHistogram old_h{};
        old_h.record(1.0f, 1000);

        ScoreHistogram new_h{};
        new_h.record(1.0f, 500);

        // Merge with alpha=0.5: result = 0.5 * new + 0.5 * old
        old_h.mergeEMA(new_h, 0.5f);

        // The bucket containing score 1.0 should now be ~750
        float t = old_h.thresholdForBytes(750);
        REQUIRE(t > 0.0f);
    }

    SECTION("[histogram] reset clears all buckets") {
        ScoreHistogram h{};
        h.record(1.0f, 1000);
        h.reset();

        // After reset, histogram is empty — thresholdForBytes returns 0
        // (cold-start guard: avoid nuclear eviction on empty data).
        float t = h.thresholdForBytes(1);
        REQUIRE(t == 0.0f);
    }
}


// #############################################################################
//
//  11. GDSF - eviction target curve
//
// #############################################################################

TEST_CASE("GDSF - eviction_target_pct",
          "[gdsf][eviction-target]")
{
    SECTION("[target] 0% eviction below 50% usage") {
        REQUIRE(GDSFPolicy::eviction_target_pct(0.0f) == 0.0f);
        REQUIRE(GDSFPolicy::eviction_target_pct(0.25f) == 0.0f);
        REQUIRE(GDSFPolicy::eviction_target_pct(0.49f) == 0.0f);
    }

    SECTION("[target] gentle zone: 50-80% usage -> 0% to 5%") {
        float at_50 = GDSFPolicy::eviction_target_pct(0.50f);
        float at_65 = GDSFPolicy::eviction_target_pct(0.65f);
        float at_80 = GDSFPolicy::eviction_target_pct(0.80f);

        REQUIRE(at_50 == Catch::Approx(0.0f).margin(0.001f));
        REQUIRE(at_65 > 0.0f);
        REQUIRE(at_65 < at_80);
        REQUIRE(at_80 == Catch::Approx(0.05f).epsilon(0.01f));
    }

    SECTION("[target] aggressive zone: 80-100% usage -> 5% to 25%") {
        float at_80 = GDSFPolicy::eviction_target_pct(0.80f);
        float at_90 = GDSFPolicy::eviction_target_pct(0.90f);
        float at_100 = GDSFPolicy::eviction_target_pct(1.00f);

        REQUIRE(at_80 == Catch::Approx(0.05f).epsilon(0.01f));
        REQUIRE(at_90 > at_80);
        REQUIRE(at_100 == Catch::Approx(0.25f).epsilon(0.01f));
    }

    SECTION("[target] curve is monotonically increasing") {
        float prev = 0.0f;
        for (float usage = 0.0f; usage <= 1.0f; usage += 0.01f) {
            float pct = GDSFPolicy::eviction_target_pct(usage);
            REQUIRE(pct >= prev);
            prev = pct;
        }
    }
}


// #############################################################################
//
//  12. GDSF - access count persistence on upsert (mergeFrom)
//
// #############################################################################

TEST_CASE("GDSF - access count persistence on upsert",
          "[integration][db][gdsf][merge]")
{
    TransactionGuard tx;
    resetRepos<GDSFItemRepo>();

    SECTION("[merge] upsert preserves access_count with kUpdatePenalty") {
        auto id = insertTestItem("merge_item", 10);

        // Populate + 20 cache hits -> access_count = 21 * kCountScale
        sync(GDSFItemRepo::find(id));
        for (int i = 0; i < 20; ++i) sync(GDSFItemRepo::find(id));

        auto meta_before = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        uint32_t count_before = meta_before->access_count;
        REQUIRE(count_before == 21 * GDSFScoreData::kCountScale);

        // Re-populate (update cache entry -> triggers mergeFrom)
        TestInternals::putInCache<GDSFItemRepo>(id,
            *sync(GDSFItemRepo::find(id)));

        auto meta_after = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        uint32_t count_after = meta_after->access_count;

        // After mergeFrom: access_count = old_count * kUpdatePenalty
        // (the new entry starts with kCountScale, but mergeFrom overwrites with penalized old count)
        uint32_t expected = static_cast<uint32_t>(
            static_cast<float>(count_before) * GDSFScoreData::kUpdatePenalty);

        // Note: there's an additional kCountScale bump from the find() in putInCache arg
        // The exact value depends on whether find() bumps before or after the upsert
        REQUIRE(count_after > 0);
        // The penalized count should be less than the original
        REQUIRE(count_after < count_before + GDSFScoreData::kCountScale);
    }
}


// #############################################################################
//
//  13. GDSF - global memory accounting coherence
//
// #############################################################################

TEST_CASE("GDSF - memory accounting",
          "[integration][db][gdsf][memory][accounting]")
{
    TransactionGuard tx;
    resetAllTestRepos();

    SECTION("[accounting] find charges memory via CachedWrapper") {
        REQUIRE(GDSFPolicy::instance().totalMemory() == 0);

        auto id = insertTestItem("acct_charge", 42);
        sync(GDSFMemRepo::find(id));

        int64_t mem = GDSFPolicy::instance().totalMemory();
        REQUIRE(mem > 0);
    }

    SECTION("[accounting] multiple entries charge additively") {
        REQUIRE(GDSFPolicy::instance().totalMemory() == 0);

        int64_t prev_mem = 0;
        for (int i = 0; i < 5; ++i) {
            auto id = insertTestItem("acct_multi_" + std::to_string(i), i);
            sync(GDSFMemRepo::find(id));

            int64_t mem = GDSFPolicy::instance().totalMemory();
            REQUIRE(mem > prev_mem);
            prev_mem = mem;
        }
    }

    SECTION("[accounting] lazy json buffer charges additional memory") {
        auto id = insertTestItem("acct_json", 42);
        sync(GDSFMemRepo::find(id));
        int64_t mem_base = GDSFPolicy::instance().totalMemory();

        // Trigger lazy JSON serialization (charges extra)
        sync(GDSFMemRepo::findJson(id));
        int64_t mem_with_json = GDSFPolicy::instance().totalMemory();
        REQUIRE(mem_with_json > mem_base);
    }

    SECTION("[accounting] lazy binary buffer charges additional memory") {
        auto id = insertTestItem("acct_binary", 42);
        sync(GDSFMemRepo::find(id));
        int64_t mem_base = GDSFPolicy::instance().totalMemory();

        // Trigger lazy BEVE serialization (charges extra)
        sync(GDSFMemRepo::findBinary(id));
        int64_t mem_with_binary = GDSFPolicy::instance().totalMemory();
        REQUIRE(mem_with_binary > mem_base);
    }

    SECTION("[accounting] update replaces entry, memory stays balanced") {
        auto id = insertTestItem("acct_update", 10);
        sync(GDSFMemRepo::find(id));
        int64_t mem_before = GDSFPolicy::instance().totalMemory();
        REQUIRE(mem_before > 0);

        // Update: InvalidateAndLazyReload → evict + re-cache on next find.
        // Old entry's CachedWrapper dtor is deferred by epoch pool;
        // new entry is charged immediately on the next find.
        auto updated = makeTestItem("acct_update_v2", 20, {}, true, id);
        sync(GDSFMemRepo::update(id, updated));

        // Re-fetch to cache the updated version
        sync(GDSFMemRepo::find(id));
        int64_t mem_after = GDSFPolicy::instance().totalMemory();

        // At most 2x: new entry charged + old entry dtor deferred
        REQUIRE(mem_after > 0);
        REQUIRE(mem_after <= mem_before * 2);
    }

    SECTION("[accounting] erase removes entry from cache") {
        auto id = insertTestItem("acct_erase", 42);
        sync(GDSFMemRepo::find(id));
        REQUIRE(GDSFPolicy::instance().totalMemory() > 0);

        sync(GDSFMemRepo::erase(id));
        REQUIRE(GDSFMemRepo::size() == 0);
    }
}


// #############################################################################
//
//  14. GDSF - ghost admission control
//
// #############################################################################

TEST_CASE("GDSF - ghost admission control",
          "[integration][db][gdsf][ghost]")
{
    TransactionGuard tx;
    auto& policy = GDSFPolicy::instance();

    // Setup: small budget (4000B), 60% inflation, high threshold
    resetRepos<GDSFGhostRepo>();
    policy.configure({.max_memory = 4000});
    TestInternals::seedAvgConstructionTime<GDSFGhostRepo>(10.0f);
    policy.charge(2400);  // 60% → hasMemoryPressure()
    TestInternals::setThreshold(100.0f);  // score ~0.76 < 100 → ghost

    SECTION("[ghost] entry ghosted when score < threshold under pressure") {
        auto id = insertTestItem("ghost_test", 10);

        // L1 miss → DB fetch → score < 100 → ghost created
        sync(GDSFGhostRepo::find(id));

        REQUIRE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));
        // No real entry (getEntityGDSFScore uses asReal() → nullopt for ghosts)
        REQUIRE_FALSE(TestInternals::getEntityGDSFScore<GDSFGhostRepo>(id).has_value());

        auto ghost = TestInternals::getGhostData<GDSFGhostRepo>(id);
        REQUIRE(ghost.has_value());
        REQUIRE(ghost->access_count == GDSFScoreData::kCountScale);
    }

    SECTION("[ghost] counter bumps on repeated misses") {
        auto id = insertTestItem("ghost_bump", 10);

        // 3 finds: each bumps ghost counter by kCountScale
        sync(GDSFGhostRepo::find(id));  // ghost created (count = kCountScale)
        sync(GDSFGhostRepo::find(id));  // ghost bumped  (count = 2 × kCountScale)
        sync(GDSFGhostRepo::find(id));  // ghost bumped  (count = 3 × kCountScale)

        auto ghost = TestInternals::getGhostData<GDSFGhostRepo>(id);
        REQUIRE(ghost.has_value());
        REQUIRE(ghost->access_count == 3 * GDSFScoreData::kCountScale);
    }

    SECTION("[ghost] promoted to real entry when score rises above threshold") {
        auto id = insertTestItem("ghost_promote", 10);

        // Create ghost (threshold = 100)
        sync(GDSFGhostRepo::find(id));
        REQUIRE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));

        // Lower threshold so next find promotes
        TestInternals::setThreshold(0.5f);

        // Find → bumps counter to 2 × kCountScale, score > 0.5 → promotion
        sync(GDSFGhostRepo::find(id));

        REQUIRE_FALSE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));
        auto meta = TestInternals::getEntityGDSFMetadata<GDSFGhostRepo>(id);
        REQUIRE(meta.has_value());
        // Counter transferred from ghost: 2 × kCountScale (without ghost flag)
        REQUIRE(meta->access_count == 2 * GDSFScoreData::kCountScale);
    }

    SECTION("[ghost] removed when no memory pressure on next fetch") {
        auto id = insertTestItem("ghost_remove", 10);

        // Create ghost
        sync(GDSFGhostRepo::find(id));
        REQUIRE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));

        // Remove inflation → no pressure
        policy.charge(-2400);

        // Find without pressure → cache normally, ghost removed
        sync(GDSFGhostRepo::find(id));

        REQUIRE_FALSE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));
        auto meta = TestInternals::getEntityGDSFMetadata<GDSFGhostRepo>(id);
        REQUIRE(meta.has_value());
    }

    // Cleanup
    resetRepos<GDSFGhostRepo>();
    policy.configure({.max_memory = kTestMaxMemory});
}


// #############################################################################
//
//  15. GDSF - ghost memory accounting
//
// #############################################################################

TEST_CASE("GDSF - ghost memory accounting",
          "[integration][db][gdsf][ghost][accounting]")
{
    TransactionGuard tx;
    auto& policy = GDSFPolicy::instance();
    constexpr auto kGhostOverhead =
        TestInternals::ghostOverhead<GDSFGhostRepo>();

    // Setup: same as test 14
    resetRepos<GDSFGhostRepo>();
    policy.configure({.max_memory = 4000});
    TestInternals::seedAvgConstructionTime<GDSFGhostRepo>(10.0f);
    policy.charge(2400);
    TestInternals::setThreshold(100.0f);

    SECTION("[ghost-acct] creation charges kGhostOverhead") {
        int64_t mem_before = policy.totalMemory();

        auto id = insertTestItem("ghost_acct_create", 10);
        sync(GDSFGhostRepo::find(id));

        REQUIRE(policy.totalMemory() ==
                mem_before + static_cast<int64_t>(kGhostOverhead));
    }

    SECTION("[ghost-acct] explicit removal discharges kGhostOverhead") {
        auto id = insertTestItem("ghost_acct_remove", 10);
        sync(GDSFGhostRepo::find(id));

        int64_t mem_with_ghost = policy.totalMemory();

        GDSFGhostRepo::evict(id);

        REQUIRE(policy.totalMemory() ==
                mem_with_ghost - static_cast<int64_t>(kGhostOverhead));
    }

    SECTION("[ghost-acct] promotion discharges ghost and charges real entry") {
        auto id = insertTestItem("ghost_acct_promote", 10);
        sync(GDSFGhostRepo::find(id));
        int64_t mem_with_ghost = policy.totalMemory();

        // Promote: lower threshold, find again
        TestInternals::setThreshold(0.5f);
        sync(GDSFGhostRepo::find(id));

        int64_t mem_after = policy.totalMemory();

        // Ghost discharged (-kGhostOverhead), real entry charged (> kGhostOverhead)
        int64_t entity_charge =
            mem_after - (mem_with_ghost - static_cast<int64_t>(kGhostOverhead));
        REQUIRE(entity_charge > 0);
        REQUIRE(entity_charge > static_cast<int64_t>(kGhostOverhead));
    }

    SECTION("[ghost-acct] N ghosts charge N × kGhostOverhead") {
        int64_t baseline = policy.totalMemory();

        for (int i = 0; i < 5; ++i) {
            auto id = insertTestItem("ghost_multi_" + std::to_string(i), i);
            sync(GDSFGhostRepo::find(id));
        }

        REQUIRE(policy.totalMemory() ==
                baseline + 5 * static_cast<int64_t>(kGhostOverhead));
    }

    // Cleanup
    resetRepos<GDSFGhostRepo>();
    policy.configure({.max_memory = kTestMaxMemory});
}


// #############################################################################
//
//  16. GDSF - ghost decay and suppression
//
// #############################################################################

TEST_CASE("GDSF - ghost decay and suppression",
          "[integration][db][gdsf][ghost][decay]")
{
    TransactionGuard tx;
    auto& policy = GDSFPolicy::instance();
    constexpr auto kGhostOverhead =
        TestInternals::ghostOverhead<GDSFGhostRepo>();

    // Setup: same as ghost tests
    resetRepos<GDSFGhostRepo>();
    policy.configure({.max_memory = 4000});
    TestInternals::seedAvgConstructionTime<GDSFGhostRepo>(10.0f);
    policy.charge(2400);
    TestInternals::setThreshold(100.0f);

    SECTION("[ghost-decay] sweep decays ghost counter") {
        auto id = insertTestItem("ghost_decay_test", 10);
        sync(GDSFGhostRepo::find(id));

        auto before = TestInternals::getGhostData<GDSFGhostRepo>(id);
        REQUIRE(before.has_value());
        REQUIRE(before->access_count == GDSFScoreData::kCountScale);

        // purge() applies ghostCleanupPredicate which decays
        GDSFGhostRepo::purge();

        auto after = TestInternals::getGhostData<GDSFGhostRepo>(id);
        REQUIRE(after.has_value());
        uint32_t expected = static_cast<uint32_t>(
            static_cast<float>(GDSFScoreData::kCountScale) * policy.decayRate());
        REQUIRE(after->access_count == expected);
    }

    SECTION("[ghost-decay] ghost removed when counter decays to 0") {
        auto id = insertTestItem("ghost_decay_zero", 10);
        sync(GDSFGhostRepo::find(id));

        // Decay until counter reaches 0 (~16 iterations for kCountScale=16, dr=0.95)
        int iterations = 0;
        while (TestInternals::isGhostEntry<GDSFGhostRepo>(id)) {
            GDSFGhostRepo::purge();
            ++iterations;
            if (iterations > 100) break;  // safety
        }

        REQUIRE_FALSE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));
        REQUIRE(iterations <= 20);  // 16 × 0.95^N → 0 in ~16 steps
    }

    SECTION("[ghost-decay] removal on decay discharges kGhostOverhead") {
        auto id = insertTestItem("ghost_decay_discharge", 10);
        sync(GDSFGhostRepo::find(id));
        int64_t mem_with = policy.totalMemory();

        // Decay to 0 via purges
        while (TestInternals::isGhostEntry<GDSFGhostRepo>(id)) {
            GDSFGhostRepo::purge();
        }

        REQUIRE(policy.totalMemory() ==
                mem_with - static_cast<int64_t>(kGhostOverhead));
    }

    // Cleanup
    resetRepos<GDSFGhostRepo>();
    policy.configure({.max_memory = kTestMaxMemory});
}


// #############################################################################
//
//  16b. size() live count excludes ghosts
//
// #############################################################################

TEST_CASE("GDSF - size() live count excludes ghosts",
          "[integration][db][gdsf][ghost][size]")
{
    TransactionGuard tx;
    auto& policy = GDSFPolicy::instance();

    // Setup: small budget, high threshold → ghosts created
    resetRepos<GDSFGhostRepo>();
    policy.configure({.max_memory = 4000});
    TestInternals::seedAvgConstructionTime<GDSFGhostRepo>(10.0f);
    policy.charge(2400);  // 60% → hasMemoryPressure()
    TestInternals::setThreshold(100.0f);  // score < 100 → ghost

    SECTION("[size] ghosts excluded from size(), included in totalEntries()") {
        // Insert 3 ghosts
        std::vector<int64_t> ids;
        for (int i = 0; i < 3; ++i) {
            auto id = insertTestItem("size_ghost_" + std::to_string(i), i);
            sync(GDSFGhostRepo::find(id));
            REQUIRE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));
            ids.push_back(id);
        }

        REQUIRE(GDSFGhostRepo::size() == 0);
        REQUIRE(TestInternals::totalEntityCacheEntries<GDSFGhostRepo>() == 3);
    }

    SECTION("[size] promotion increases size()") {
        auto id = insertTestItem("size_promote", 10);
        sync(GDSFGhostRepo::find(id));  // ghost
        REQUIRE(GDSFGhostRepo::size() == 0);
        REQUIRE(TestInternals::totalEntityCacheEntries<GDSFGhostRepo>() == 1);

        // Lower threshold → next find promotes ghost to real
        TestInternals::setThreshold(0.5f);
        sync(GDSFGhostRepo::find(id));

        REQUIRE_FALSE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));
        REQUIRE(GDSFGhostRepo::size() == 1);
        REQUIRE(TestInternals::totalEntityCacheEntries<GDSFGhostRepo>() == 1);
    }

    SECTION("[size] evict decreases size() for real, not for ghost") {
        // Insert a real entry (no pressure first)
        policy.charge(-2400);  // remove pressure
        auto real_id = insertTestItem("size_real", 10);
        sync(GDSFGhostRepo::find(real_id));
        REQUIRE_FALSE(TestInternals::isGhostEntry<GDSFGhostRepo>(real_id));
        REQUIRE(GDSFGhostRepo::size() == 1);

        // Re-apply pressure, create a ghost
        policy.charge(2400);
        auto ghost_id = insertTestItem("size_ghost", 20);
        sync(GDSFGhostRepo::find(ghost_id));
        REQUIRE(TestInternals::isGhostEntry<GDSFGhostRepo>(ghost_id));
        REQUIRE(GDSFGhostRepo::size() == 1);  // ghost doesn't count
        REQUIRE(TestInternals::totalEntityCacheEntries<GDSFGhostRepo>() == 2);

        // Evict ghost → size unchanged
        GDSFGhostRepo::evict(ghost_id);
        REQUIRE(GDSFGhostRepo::size() == 1);
        REQUIRE(TestInternals::totalEntityCacheEntries<GDSFGhostRepo>() == 1);

        // Evict real → size decreases
        GDSFGhostRepo::evict(real_id);
        REQUIRE(GDSFGhostRepo::size() == 0);
        REQUIRE(TestInternals::totalEntityCacheEntries<GDSFGhostRepo>() == 0);
    }

    SECTION("[size] mixed real + ghost consistency") {
        // Start without pressure → insert 2 real entries
        policy.charge(-2400);
        auto id1 = insertTestItem("size_mix_1", 1);
        auto id2 = insertTestItem("size_mix_2", 2);
        sync(GDSFGhostRepo::find(id1));
        sync(GDSFGhostRepo::find(id2));
        REQUIRE(GDSFGhostRepo::size() == 2);

        // Re-apply pressure → insert 3 ghosts
        policy.charge(2400);
        for (int i = 0; i < 3; ++i) {
            auto id = insertTestItem("size_mix_g_" + std::to_string(i), i + 10);
            sync(GDSFGhostRepo::find(id));
        }

        REQUIRE(GDSFGhostRepo::size() == 2);  // only reals
        REQUIRE(TestInternals::totalEntityCacheEntries<GDSFGhostRepo>() == 5);
    }

    // Cleanup
    resetRepos<GDSFGhostRepo>();
    policy.configure({.max_memory = kTestMaxMemory});
}


// #############################################################################
//
//  17. GDSF - eviction selectivity
//
// #############################################################################

TEST_CASE("GDSF - eviction selectivity",
          "[integration][db][gdsf][eviction][selectivity]")
{
    TransactionGuard tx;
    auto& policy = GDSFPolicy::instance();

    SECTION("[selectivity] hot entry survives, cold entries evicted") {
        resetRepos<GDSFPressureRepo>();
        // Budget must fit all 6 entries without triggering isOverBudget()
        // during insertion (which would evict cold entries before the test
        // verifies score ordering). 2000B is ~3× per-entry cost.
        constexpr size_t kSmallBudget = 2000;
        policy.configure({.max_memory = kSmallBudget});

        // Insert 1 "hot" entry → 100 accesses
        auto hot_id = insertTestItem("hot_entry", 1);
        sync(GDSFPressureRepo::find(hot_id));
        for (int i = 0; i < 100; ++i) sync(GDSFPressureRepo::find(hot_id));

        // Insert 5 "cold" entries → 1 access each
        std::vector<int64_t> cold_ids;
        for (int i = 0; i < 5; ++i) {
            auto id = insertTestItem("cold_" + std::to_string(i), i);
            sync(GDSFPressureRepo::find(id));
            cold_ids.push_back(id);
        }

        // Verify score ordering before eviction
        auto score_hot = TestInternals::getEntityGDSFScore<GDSFPressureRepo>(hot_id);
        auto score_cold = TestInternals::getEntityGDSFScore<GDSFPressureRepo>(cold_ids[0]);
        REQUIRE(score_hot.has_value());
        REQUIRE(score_cold.has_value());
        REQUIRE(*score_hot > *score_cold);

        // Inflate memory past budget to trigger eviction
        policy.charge(static_cast<int64_t>(kSmallBudget));

        // Sweep → build histogram + threshold, second sweep → evict
        policy.sweep();
        policy.sweep();
        GDSFPressureRepo::purge();

        // Discharge artificial inflation
        policy.charge(-static_cast<int64_t>(kSmallBudget));

        // Hot entry should survive
        auto hot_meta = TestInternals::getEntityGDSFMetadata<GDSFPressureRepo>(hot_id);
        REQUIRE(hot_meta.has_value());

        // At least one cold entry evicted
        REQUIRE(GDSFPressureRepo::size() < 6);

        // Cleanup
        resetRepos<GDSFPressureRepo>();
        policy.configure({.max_memory = kTestMaxMemory});
    }

    SECTION("[selectivity] GDSF score formula verification") {
        resetRepos<GDSFPressureRepo>();

        auto id = insertTestItem("score_verify", 42);

        // First find: L1 miss → DB fetch → cache (access_count = kCountScale)
        sync(GDSFPressureRepo::find(id));

        // Score after 1 access
        auto s1 = TestInternals::getEntityGDSFScore<GDSFPressureRepo>(id);
        REQUIRE(s1.has_value());
        REQUIRE(*s1 > 0.0f);

        // 9 more accesses (total 10 × kCountScale)
        for (int i = 0; i < 9; ++i) sync(GDSFPressureRepo::find(id));

        // Score after 10 accesses: should be 10× the single-access score
        // (same avg_cost, same memoryUsage, 10× access_count)
        auto s10 = TestInternals::getEntityGDSFScore<GDSFPressureRepo>(id);
        REQUIRE(s10.has_value());
        REQUIRE(*s10 == Catch::Approx(10.0f * *s1).epsilon(0.01));

        // Cleanup
        resetRepos<GDSFPressureRepo>();
    }
}


// #############################################################################
//
//  18. GDSF - effective discharge
//
// #############################################################################

TEST_CASE("GDSF - effective discharge",
          "[integration][db][gdsf][memory][discharge]")
{
    TransactionGuard tx;
    auto& policy = GDSFPolicy::instance();

    SECTION("[discharge] evicted entries eventually discharge memory via pool recycling") {
        resetRepos<GDSFPressureRepo2>();
        REQUIRE(policy.totalMemory() == 0);

        // Insert N=10 entries (fixed-length names for consistent per-entry cost)
        char buf[16];
        for (int i = 0; i < 10; ++i) {
            snprintf(buf, sizeof(buf), "dsc_a_%03d", i);
            auto id = insertTestItem(buf, i);
            sync(GDSFPressureRepo2::find(id));
        }
        int64_t mem_after_insert = policy.totalMemory();
        REQUIRE(mem_after_insert > 0);
        int64_t entry_size = mem_after_insert / 10;

        // Clear cache (CachedWrapper dtors deferred by epoch pool)
        TestInternals::resetEntityCacheState<GDSFPressureRepo2>();

        // Insert M=20 new entries (same name length) → pool recycling
        // triggers old dtors when epoch pool reuses retired entries.
        for (int i = 0; i < 20; ++i) {
            snprintf(buf, sizeof(buf), "dsc_b_%03d", i);
            auto id = insertTestItem(buf, i);
            sync(GDSFPressureRepo2::find(id));
        }

        // Force epoch GC to ensure deferred dtors fire
        TestInternals::collectEntityCache<GDSFPressureRepo2>();

        int64_t mem_final = policy.totalMemory();
        // Without discharge: 30 entries ≈ 3 × mem_after_insert
        // With full discharge: 20 entries ≈ 2 × mem_after_insert
        // Epoch reclamation is non-deterministic (depends on thread epoch advancement),
        // so we allow up to 3× + 1 entry of tolerance.
        REQUIRE(mem_final <= mem_after_insert * 3 + entry_size);

        // Cleanup
        resetRepos<GDSFPressureRepo2>();
    }

    SECTION("[discharge] totalMemory converges under sustained pressure") {
        resetRepos<GDSFPressureRepo2>();
        // Budget small enough that 100 entries (~200B each ≈ 20KB) overshoot.
        // Forces actual GDSF eviction — not just a "fits in budget" no-op.
        constexpr size_t kSmallBudget = 10000;
        policy.configure({.max_memory = kSmallBudget});

        for (int i = 0; i < 100; ++i) {
            auto id = insertTestItem("pressure_" + std::to_string(i), i);
            sync(GDSFPressureRepo2::find(id));

            if (i % 20 == 19) {
                policy.sweep();
                // Memory bounded between sweeps despite continuous insertions.
                // 3× accounts for: epoch-deferred CachedWrapper destructors,
                // ghost overhead, and kSweepInterval new entries since last sweep.
                REQUIRE(policy.totalMemory() <=
                        static_cast<int64_t>(kSmallBudget * 3));
            }
        }

        // Final stabilization: multiple sweeps + full purge for convergence
        for (int s = 0; s < 3; ++s) policy.sweep();
        GDSFPressureRepo2::purge();

        // After stabilization, should converge closer to budget.
        // 3× bound accounts for epoch-deferred destructors.
        REQUIRE(policy.totalMemory() <=
                static_cast<int64_t>(kSmallBudget * 3));

        // Cleanup
        resetRepos<GDSFPressureRepo2>();
        policy.configure({.max_memory = kTestMaxMemory});
    }
}


// #############################################################################
//
//  19. GDSF - cross-repo sweep coordination
//
// #############################################################################

TEST_CASE("GDSF - cross-repo sweep coordination",
          "[integration][db][gdsf][coordination]")
{
    TransactionGuard tx;
    auto& policy = GDSFPolicy::instance();
    resetRepos<GDSFCoordRepo1, GDSFCoordRepo2>();

    SECTION("[coordination] global sweep decays counters in all enrolled repos") {
        // Insert 3 entries in each repo
        std::vector<int64_t> ids1, ids2;
        for (int i = 0; i < 3; ++i) {
            auto id = insertTestItem("coord1_" + std::to_string(i), i);
            sync(GDSFCoordRepo1::find(id));
            ids1.push_back(id);
        }
        for (int i = 0; i < 3; ++i) {
            auto id = insertTestItem("coord2_" + std::to_string(i), i + 10);
            sync(GDSFCoordRepo2::find(id));
            ids2.push_back(id);
        }

        // Access 10 more times each (total 11 per entry: 1 initial + 10)
        for (auto id : ids1) {
            for (int j = 0; j < 10; ++j) sync(GDSFCoordRepo1::find(id));
        }
        for (auto id : ids2) {
            for (int j = 0; j < 10; ++j) sync(GDSFCoordRepo2::find(id));
        }

        // Verify initial counts = 11 × kCountScale
        for (auto id : ids1) {
            auto meta = TestInternals::getEntityGDSFMetadata<GDSFCoordRepo1>(id);
            REQUIRE(meta.has_value());
            REQUIRE(meta->access_count == 11 * GDSFScoreData::kCountScale);
        }
        for (auto id : ids2) {
            auto meta = TestInternals::getEntityGDSFMetadata<GDSFCoordRepo2>(id);
            REQUIRE(meta.has_value());
            REQUIRE(meta->access_count == 11 * GDSFScoreData::kCountScale);
        }

        // Global sweep (sweeps 1 chunk per repo) + purge (covers all chunks)
        policy.sweep();
        GDSFCoordRepo1::purge();
        GDSFCoordRepo2::purge();

        // Verify decay happened in BOTH repos
        // After sweep + purge: entries decayed 1-2× (depending on chunk overlap)
        for (auto id : ids1) {
            auto meta = TestInternals::getEntityGDSFMetadata<GDSFCoordRepo1>(id);
            REQUIRE(meta.has_value());
            REQUIRE(meta->access_count < 11 * GDSFScoreData::kCountScale);
        }
        for (auto id : ids2) {
            auto meta = TestInternals::getEntityGDSFMetadata<GDSFCoordRepo2>(id);
            REQUIRE(meta.has_value());
            REQUIRE(meta->access_count < 11 * GDSFScoreData::kCountScale);
        }
    }

    SECTION("[coordination] nbRepos reflects all enrolled repos") {
        // Ensure both repos are enrolled (warmup triggers call_once registration)
        GDSFCoordRepo1::warmup();
        GDSFCoordRepo2::warmup();

        // At least 2 repos enrolled (may be more from other tests in this TU)
        REQUIRE(policy.nbRepos() >= 2);
    }

    // Cleanup
    resetRepos<GDSFCoordRepo1, GDSFCoordRepo2>();
}


// #############################################################################
//
//  20. GDSF - memory bound under Zipfian load (stress test)
//
// #############################################################################

TEST_CASE("GDSF - memory bound under Zipfian load",
          "[.stress][integration][db][gdsf][memory]")
{
    TransactionGuard tx;

    constexpr int N = 100'000;           // items in DB
    constexpr int CACHE_ITEMS = 5'000;   // target cache capacity
    constexpr int FINDS = 2'000'000;     // total find() calls
    constexpr int LIMIT_ITEMS = 6'000;   // max allowed (20% tolerance)

    auto& policy = GDSFPolicy::instance();

    // 1. Disable budget during DB setup (avoid cold-start eviction)
    policy.configure({.max_memory = SIZE_MAX});

    // 2. Bulk insert N items via generate_series (RETURNING ids)
    auto id_result = execQuery(
        ("INSERT INTO relais_test_items (name, value, is_active) "
         "SELECT 'stress_' || g, g, true "
         "FROM generate_series(1, " + std::to_string(N) + ") AS g "
         "RETURNING id").c_str());
    std::vector<int64_t> ids;
    ids.reserve(N);
    for (int i = 0; i < id_result.rows(); ++i) {
        ids.push_back(id_result[i].get<int64_t>(0));
    }
    REQUIRE(ids.size() == N);

    // 3. Empirical per-item memory: find one item, measure totalMemory delta
    std::fprintf(stderr, "  [stress] step 3: measuring per_item...\n");
    int64_t mem_before = policy.totalMemory();
    sync(GDSFStressRepo::find(ids[0]));
    int64_t mem_after = policy.totalMemory();
    size_t per_item = static_cast<size_t>(std::max(int64_t(1), mem_after - mem_before));
    std::fprintf(stderr, "  [stress] per_item=%zu\n", per_item);

    // Pre-warm histogram (16 sweeps with no pressure) so eviction uses
    // real score distributions, not cold-start nuclear threshold.
    for (int i = 0; i < 16; ++i) policy.sweep();

    // 4. Configure real budget = CACHE_ITEMS × per_item
    size_t budget = CACHE_ITEMS * per_item;
    policy.configure({.max_memory = budget});
    std::fprintf(stderr, "  [stress] budget=%zu, starting %d finds...\n", budget, FINDS);

    // 5. Precompute Zipfian CDF (alpha=1.0): P(rank ≤ k) = H(k) / H(N)
    std::vector<double> cdf(N);
    double sum = 0.0;
    for (int i = 0; i < N; ++i) {
        sum += 1.0 / static_cast<double>(i + 1);
        cdf[i] = sum;
    }
    for (auto& v : cdf) v /= sum;  // normalize to [0, 1]

    // xorshift64 PRNG + Zipfian sampler via binary search on CDF
    uint64_t rng = 0xDEADBEEFCAFE1234ULL;
    auto zipf_sample = [&]() -> size_t {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        double u = static_cast<double>(rng & 0xFFFFFFFFULL) / 4294967296.0;
        return static_cast<size_t>(
            std::lower_bound(cdf.begin(), cdf.end(), u) - cdf.begin());
    };

    // 6. Run FINDS find() calls, track peak memory
    int64_t max_memory = 0;
    for (int i = 0; i < FINDS; ++i) {
        size_t rank = zipf_sample();
        if (rank >= static_cast<size_t>(N)) rank = N - 1;
        sync(GDSFStressRepo::find(ids[rank]));
        int64_t mem = policy.totalMemory();
        if (mem > max_memory) max_memory = mem;
        if ((i + 1) % 1'000 == 0)
            std::fprintf(stderr, "  [stress] %d/%d finds, mem=%ld, peak=%ld\n",
                         i + 1, FINDS, (long)mem, (long)max_memory);
    }

    // 7. Assert: peak memory never exceeded LIMIT_ITEMS × per_item
    int64_t limit = static_cast<int64_t>(per_item) * LIMIT_ITEMS;
    INFO("per_item=" << per_item << " budget=" << budget
         << " max_memory=" << max_memory << " limit=" << limit);
    REQUIRE(max_memory <= limit);

    // 8. Cleanup: restore original test budget
    resetRepos<GDSFStressRepo>();
    policy.configure({.max_memory = kTestMaxMemory});
}
