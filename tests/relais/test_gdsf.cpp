/**
 * test_gdsf.cpp
 *
 * Tests for the GDSF (Greedy Dual-Size Frequency) cache eviction policy.
 * Compiled with RELAIS_L1_MAX_MEMORY=268435456 (256 MB) to enable GDSF.
 *
 * Verifies score tracking, lazy decay, eviction decisions, memory tracking,
 * global sweep, pressure factor, and repo auto-registration.
 *
 * Covers:
 *   1. Score tracking          — find() bumps score by avg_construction_time
 *   2. Lazy decay              — generation-based score decay via CAS
 *   3. Eviction decisions      — threshold-based eviction
 *   4. Avg construction time   — EMA convergence
 *   5. Optional TTL            — TTL-based vs score-only eviction
 *   6. CachedWrapper memory    — ctor charges, dtor discharges, lazy buffers
 *   7. Memory pressure         — emergency cleanup when over budget
 *   8. Striped counter         — multi-slot memory accounting
 *   9. Repo auto-registration  — enrollment via std::call_once
 *  10. Repo score update       — formula verification after cleanup
 *  11. Correction coefficient  — EMA convergence
 *  12. Pressure factor         — quadratic scaling of threshold
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <thread>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;
using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;

// Compile-time check: this TU must be compiled with GDSF enabled
static_assert(GDSFPolicy::kMaxMemory > 0,
    "test_gdsf.cpp must be compiled with RELAIS_L1_MAX_MEMORY > 0");

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

// Repo score / correction test repos
using GDSFScoreRepo    = Repo<TestItemWrapper, "gdsf:score",      gt::ManualCleanup>;

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
               GDSFPressureRepo, GDSFPressureRepo2, GDSFScoreRepo>();
}


// #############################################################################
//
//  1. GDSF - score tracking
//
// #############################################################################

TEST_CASE("GDSF - score tracking",
          "[integration][db][gdsf][score]")
{
    TransactionGuard tx;
    resetRepos<GDSFItemRepo>();

    SECTION("[score] find() increments score by avg_construction_time") {
        auto id = insertTestItem("score_item", 10);

        // First find: L1 miss -> DB fetch -> populate cache (score = cost)
        sync(GDSFItemRepo::find(id));
        float cost = GDSFItemRepo::avgConstructionTime();
        REQUIRE(cost > 0.0f);

        // 10 cache hits: each bumps score by cost
        for (int i = 0; i < 10; ++i) {
            sync(GDSFItemRepo::find(id));
        }

        auto meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        REQUIRE(meta.has_value());
        // score = initial_cost + 10 * cost = 11 * cost
        float expected = 11.0f * cost;
        REQUIRE(meta->score == Catch::Approx(expected).epsilon(0.01));
    }

    SECTION("[score] score starts at cost on first cache population") {
        auto id = insertTestItem("init_score", 20);

        sync(GDSFItemRepo::find(id));
        float cost = GDSFItemRepo::avgConstructionTime();

        auto meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        REQUIRE(meta.has_value());
        REQUIRE(meta->score == Catch::Approx(cost).epsilon(0.01));
    }

    SECTION("[score] multiple entities accumulate scores independently") {
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
        REQUIRE(ma->score > mc->score);
        REQUIRE(mc->score > mb->score);
    }
}


// #############################################################################
//
//  2. GDSF - lazy decay
//
// #############################################################################

TEST_CASE("GDSF - lazy decay",
          "[integration][db][gdsf][decay]")
{
    TransactionGuard tx;
    resetRepos<GDSFItemRepo>();

    SECTION("[decay] score decays after generation increment") {
        auto id = insertTestItem("decay_item", 10);

        // Populate + 10 cache hits -> score = 11 * cost
        sync(GDSFItemRepo::find(id));
        for (int i = 0; i < 10; ++i) sync(GDSFItemRepo::find(id));

        float cost = GDSFItemRepo::avgConstructionTime();
        auto meta0 = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        float score_before = meta0->score;

        // Without decay, one more find would yield: score_before + cost
        float expected_no_decay = score_before + cost;

        // Force generation increment (tick nb_repos times)
        size_t nb = GDSFPolicy::instance().nbRepos();
        for (size_t i = 0; i < nb; ++i) {
            GDSFPolicy::instance().tick();
        }

        // One more find() -> triggers lazy decay then score bump
        sync(GDSFItemRepo::find(id));

        auto meta1 = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id);
        float score_after = meta1->score;

        // Score should be LESS than expected_no_decay due to decay
        REQUIRE(score_after < expected_no_decay);

        // Expected: score_before * 0.95 + cost
        float expected = score_before * GDSFPolicy::instance().decayFactor(1) + cost;
        REQUIRE(score_after == Catch::Approx(expected).epsilon(0.01));
    }

    SECTION("[decay] score unaffected when generation unchanged") {
        auto id = insertTestItem("no_decay_item", 10);

        sync(GDSFItemRepo::find(id));
        for (int i = 0; i < 10; ++i) sync(GDSFItemRepo::find(id));

        float cost = GDSFItemRepo::avgConstructionTime();
        float score_before = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id)->score;

        // No generation increment -> no decay
        sync(GDSFItemRepo::find(id));

        float score_after = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id)->score;

        // Exact: score_before + cost (no decay applied)
        REQUIRE(score_after == Catch::Approx(score_before + cost).epsilon(0.01));
    }

    SECTION("[decay] decay clamps at decay_table[64] for large gaps") {
        auto id = insertTestItem("big_gap_item", 10);

        sync(GDSFItemRepo::find(id));
        for (int i = 0; i < 10; ++i) sync(GDSFItemRepo::find(id));

        float cost = GDSFItemRepo::avgConstructionTime();
        float score_before = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id)->score;

        // Force 100 generation increments (decay table clamps at index 64)
        size_t nb = GDSFPolicy::instance().nbRepos();
        for (int g = 0; g < 100; ++g) {
            for (size_t i = 0; i < nb; ++i) {
                GDSFPolicy::instance().tick();
            }
        }

        // One find -> decay with clamped gap then bump
        sync(GDSFItemRepo::find(id));

        float score_after = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id)->score;

        // Expected: score_before * decay_table[64] + cost
        float factor_64 = GDSFPolicy::instance().decayFactor(64);
        float expected = score_before * factor_64 + cost;
        REQUIRE(score_after == Catch::Approx(expected).epsilon(0.01));
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
    // Reset ALL test repos for clean global threshold (random TEST_CASE order)
    resetAllTestRepos();

    SECTION("[eviction] low-score entry evicted, high-score survives") {
        auto id_low = insertTestItem("low_score", 1);
        auto id_high = insertTestItem("high_score", 2);

        // Populate both (1 find each)
        sync(GDSFItemRepo::find(id_low));
        sync(GDSFItemRepo::find(id_high));

        // High-score: 100 more accesses
        for (int i = 0; i < 100; ++i) sync(GDSFItemRepo::find(id_high));

        // Inflate memory to make pressureFactor() ~ 1.0 (needed for meaningful threshold)
        int64_t budget = static_cast<int64_t>(GDSFPolicy::kMaxMemory);
        GDSFPolicy::instance().charge(budget);

        // First purge: threshold = 0, nothing evicted, but repo_score established
        // purge() visits ALL shards (unlike sweep() which hits one, possibly empty)
        GDSFItemRepo::purge();

        // Second purge: threshold > 0, low-score entry should be evicted
        GDSFItemRepo::purge();

        // Discharge artificial inflation
        GDSFPolicy::instance().charge(-budget);

        auto low_meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id_low);
        auto high_meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id_high);

        // Low-score entry should be evicted (score ~ cost < threshold)
        REQUIRE_FALSE(low_meta.has_value());
        // High-score entry should survive (score ~ 96*cost > threshold)
        REQUIRE(high_meta.has_value());
    }

    SECTION("[eviction] all entries survive when scores above threshold") {
        // Create 5 entries with equal, high scores
        std::vector<int64_t> ids;
        for (int i = 0; i < 5; ++i) {
            ids.push_back(insertTestItem("survive_" + std::to_string(i), i));
        }

        // Populate + high access count for all
        for (auto id : ids) {
            sync(GDSFItemRepo::find(id));
            for (int j = 0; j < 20; ++j) sync(GDSFItemRepo::find(id));
        }

        size_t before = GDSFItemRepo::size();
        REQUIRE(before == 5);

        // Multiple cleanup cycles — all similar scores, threshold well below
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
        // The exact value depends on timings, but it should still be positive
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
    // Reset ALL test repos for clean global threshold
    resetAllTestRepos();

    SECTION("[ttl] entry evicted when TTL expires regardless of score") {
        auto id = insertTestItem("ttl_high_score", 10);

        // Populate + many hits -> very high score
        sync(GDSFShortTTLRepo::find(id));
        for (int i = 0; i < 50; ++i) sync(GDSFShortTTLRepo::find(id));

        REQUIRE(GDSFShortTTLRepo::size() == 1);

        // Wait for 50ms TTL to expire
        waitForExpiration(std::chrono::milliseconds{80});

        // Cleanup should evict despite high score
        GDSFShortTTLRepo::purge();

        REQUIRE(GDSFShortTTLRepo::size() == 0);
    }

    SECTION("[ttl] entry without TTL survives indefinitely if score is high") {
        auto id = insertTestItem("no_ttl_item", 10);

        sync(GDSFNoTTLRepo::find(id));
        for (int i = 0; i < 50; ++i) sync(GDSFNoTTLRepo::find(id));

        // Wait a long time (relative to normal TTLs)
        waitForExpiration(std::chrono::milliseconds{200});

        // Cleanup: score-based only, no TTL eviction.
        // With clean global state, threshold << entry score (51*cost >> cost/8).
        GDSFNoTTLRepo::purge();
        GDSFNoTTLRepo::purge();

        // Entry should survive (high score, no TTL)
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
        // The type-erased accessor leaves ttl_expiration_rep at 0
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

    // TODO: epoch-based reclamation defers destruction — memory is not discharged
    // synchronously after evict(). This test needs rethinking for the epoch model.
    // SECTION("[wrapper] evict discharges memory via CachedWrapper dtor") { ... }

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
        int64_t budget = static_cast<int64_t>(GDSFPolicy::kMaxMemory);
        GDSFPolicy::instance().charge(budget + 1);

        REQUIRE(GDSFPolicy::instance().isOverBudget());

        // Discharge to restore
        GDSFPolicy::instance().charge(-(budget + 1));
        REQUIRE_FALSE(GDSFPolicy::instance().isOverBudget());
    }

    SECTION("[memory] sweep evicts entries below threshold") {
        // Insert entries with 1 access each (low score ~ cost)
        for (int i = 0; i < 20; ++i) {
            auto id = insertTestItem("emrg_" + std::to_string(i), i);
            sync(GDSFPressureRepo::find(id));
        }

        size_t before = GDSFPressureRepo::size();
        REQUIRE(before == 20);

        // Pre-seed repo_score very high so threshold >> entry scores.
        // Entry scores ~ cost (1 access). Setting repo_score = 10000 * cost
        // makes threshold ~ 10000 * cost, so all entries are below threshold.
        float cost = GDSFPressureRepo::avgConstructionTime();
        TestInternals::setRepoScore<GDSFPressureRepo>(10000.0f * cost);

        // Artificially inflate memory to exceed budget (triggers second pass in sweep)
        int64_t budget = static_cast<int64_t>(GDSFPolicy::kMaxMemory);
        GDSFPolicy::instance().charge(budget + 1);

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

    SECTION("[registration] tick increments generation after nb_repos cleanups") {
        // Ensure at least one repo is registered
        (void)GDSFPolicy::instance().nbRepos();

        uint32_t gen_before = GDSFPolicy::instance().generation();
        size_t nb = GDSFPolicy::instance().nbRepos();
        REQUIRE(nb > 0);

        // tick() nb times -> should trigger exactly 1 generation increment
        for (size_t i = 0; i < nb; ++i) {
            GDSFPolicy::instance().tick();
        }

        REQUIRE(GDSFPolicy::instance().generation() == gen_before + 1);
    }
}


// #############################################################################
//
//  10. GDSF - repo_score update
//
// #############################################################################

TEST_CASE("GDSF - repo_score update",
          "[integration][db][gdsf][repo-score]")
{
    TransactionGuard tx;
    resetAllTestRepos();

    SECTION("[repo-score] repo_score updated after full cleanup") {
        // repo_score starts at 0 after reset
        REQUIRE(GDSFScoreRepo::repoScore() == 0.0f);

        // Insert entities with varying access counts
        for (int i = 0; i < 5; ++i) {
            auto id = insertTestItem("score_" + std::to_string(i), i);
            sync(GDSFScoreRepo::find(id));
            for (int j = 0; j < (i + 1) * 5; ++j) {
                sync(GDSFScoreRepo::find(id));
            }
        }

        // purge() visits ALL shards, guaranteeing entries are encountered
        // (sweep() only visits one shard which may be empty)
        GDSFScoreRepo::purge();

        // repo_score should now be non-zero (reflects kept entries' average)
        float rs = GDSFScoreRepo::repoScore();
        REQUIRE(rs > 0.0f);

        // Second purge should further adjust repo_score
        GDSFScoreRepo::purge();
        float rs_after_second = GDSFScoreRepo::repoScore();

        // repo_score should converge toward the kept entries' average
        // (formula: (old * (shards-1) + avg_kept) / shards)
        REQUIRE(rs_after_second > 0.0f);
    }
}


// #############################################################################
//
//  11. GDSF - correction coefficient
//
// #############################################################################

TEST_CASE("GDSF - correction coefficient",
          "[integration][db][gdsf][correction]")
{
    TransactionGuard tx;
    resetAllTestRepos();

    SECTION("[correction] correction starts at 1.0 and adjusts with cleanup cycles") {
        // After reset, correction is 1.0
        REQUIRE(GDSFPolicy::instance().correction() == Catch::Approx(1.0f));

        // Insert entries and do some finds
        for (int i = 0; i < 5; ++i) {
            auto id = insertTestItem("corr_" + std::to_string(i), i);
            sync(GDSFItemRepo::find(id));
            for (int j = 0; j < 10; ++j) sync(GDSFItemRepo::find(id));
        }

        // Multiple cleanup cycles should adjust correction via EMA
        for (int cycle = 0; cycle < 5; ++cycle) {
            GDSFItemRepo::sweep();
        }

        float corr = GDSFPolicy::instance().correction();
        // Correction should remain reasonable (not diverge wildly)
        REQUIRE(corr > 0.0f);
        REQUIRE(corr < 10.0f);
    }
}


// #############################################################################
//
//  12. GDSF - pressure factor
//
// #############################################################################

TEST_CASE("GDSF - pressure factor",
          "[integration][db][gdsf][pressure]")
{
    TransactionGuard tx;
    resetAllTestRepos();

    auto& policy = GDSFPolicy::instance();

    SECTION("[pressure] pressure_factor is 0 when memory empty") {
        REQUIRE(policy.totalMemory() == 0);
        REQUIRE(policy.pressureFactor() == Catch::Approx(0.0f));
    }

    SECTION("[pressure] pressure_factor scales quadratically") {
        int64_t budget = static_cast<int64_t>(GDSFPolicy::kMaxMemory);

        // 50% usage -> factor = 0.5^2 = 0.25
        policy.charge(budget / 2);
        REQUIRE(policy.pressureFactor() == Catch::Approx(0.25f).epsilon(0.01));
        policy.charge(-(budget / 2));

        // 75% usage -> factor = 0.75^2 = 0.5625
        policy.charge(budget * 3 / 4);
        REQUIRE(policy.pressureFactor() == Catch::Approx(0.5625f).epsilon(0.01));
        policy.charge(-(budget * 3 / 4));
    }

    SECTION("[pressure] pressure_factor clamps at 1.0") {
        int64_t budget = static_cast<int64_t>(GDSFPolicy::kMaxMemory);

        // Exactly at budget
        policy.charge(budget);
        REQUIRE(policy.pressureFactor() == Catch::Approx(1.0f));
        policy.charge(-budget);

        // Over budget
        policy.charge(budget * 2);
        REQUIRE(policy.pressureFactor() == 1.0f);
        policy.charge(-(budget * 2));
    }

    SECTION("[pressure] low memory usage prevents score-based eviction") {
        // With nearly empty memory, pressure_factor ~ 0
        // -> threshold ~ 0 -> no score-based eviction
        auto id = insertTestItem("pressure_item", 42);
        sync(GDSFPressureRepo2::find(id));

        // Pre-seed repo_score to a high value
        float cost = GDSFPressureRepo2::avgConstructionTime();
        TestInternals::setRepoScore<GDSFPressureRepo2>(10000.0f * cost);

        // threshold = repo_score_avg * correction * pressure_factor
        // pressure_factor ~ 0 (memory near empty) -> threshold ~ 0
        float threshold = policy.threshold();

        // Entry score = cost > 0 > threshold ~ 0
        // So entry survives despite high repo_score
        GDSFPressureRepo2::purge();
        REQUIRE(GDSFPressureRepo2::size() == 1);
    }
}
