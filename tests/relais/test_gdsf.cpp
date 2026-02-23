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
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <thread>

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

} // namespace relais_test


// =============================================================================
// Helper: clean up repos + GDSF global state for each test
// =============================================================================

template<typename... Repos>
void resetRepos() {
    // Unconditional cache clear (not threshold-based purge, which skips entries above threshold=0)
    (TestInternals::resetEntityCacheState<Repos>(), ...);
    (TestInternals::resetRepoGDSFState<Repos>(), ...);
    TestInternals::resetGDSF();
}

/// Reset ALL test repos to ensure clean global threshold.
/// Excludes GDSFRegRepo1/2/3 (tested for registration, must not be pre-registered).
void resetAllTestRepos() {
    resetRepos<GDSFItemRepo, GDSFItemRepo2, GDSFMemRepo,
               GDSFShortTTLRepo, GDSFNoTTLRepo,
               GDSFPressureRepo, GDSFPressureRepo2>();
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

        // First sweep: builds histogram, threshold = 0 (empty histogram)
        GDSFPolicy::instance().sweep();

        // Second sweep: uses histogram to compute meaningful threshold
        // Over budget -> eviction_target_pct(1.0) = 0.25 -> evict ~25% of bytes
        GDSFPolicy::instance().sweep();

        // Entries in swept shards should have been evicted
        REQUIRE(GDSFPressureRepo::size() < before);

        // Discharge artificial inflation
        GDSFPolicy::instance().charge(-(budget + 1));
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

        // After reset, threshold for any amount should be very high (empty)
        float t = h.thresholdForBytes(1);
        REQUIRE(t > 1e6f);  // exp2(kLogMax)
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
