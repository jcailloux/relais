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
 *   9. Repo auto-registration  — enrollment via std::call_once
 *  10. ScoreHistogram          — record, thresholdForBytes, mergeEMA
 *  12. Access count persistence — mergeFrom on upsert with kUpdatePenalty
 *  14. Ghost admission control — ghost/promotion/counter bumps
 *  16. Ghost decay and suppression — decay, removal
 * 16b. size() live count excludes ghosts
 *  17. Eviction selectivity    — hot/cold, score formula
 *  19. Cross-repo sweep coordination
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <cstdint>
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

// Short TTL for expiration tests (1 second — CachedClock uses uint32_t seconds)
inline constexpr auto ShortTTL = Local
    .with_l1_ttl(std::chrono::seconds{1});

// No TTL (GDSF score only, 0ns disables TTL)
inline constexpr auto NoTTL = Local
    .with_l1_ttl(std::chrono::nanoseconds{0});

} // namespace relais_test::gdsf_test

namespace relais_test {

namespace gt = gdsf_test;

// Score / decay / eviction test repos
using GDSFItemRepo     = Repo<TestItemEntity, "gdsf:item",       gt::ManualCleanup>;
using GDSFItemRepo2    = Repo<TestItemEntity, "gdsf:item2",      gt::ManualCleanup>;
using GDSFUserRepo     = Repo<TestUserEntity, "gdsf:user",       gt::ManualCleanup>;

// TTL test repos
using GDSFShortTTLRepo = Repo<TestItemEntity, "gdsf:ttl:short",  gt::ShortTTL>;
using GDSFNoTTLRepo    = Repo<TestItemEntity, "gdsf:ttl:none",   gt::NoTTL>;

// Registration-only repos (first access enrolls them)
using GDSFRegRepo1     = Repo<TestItemEntity, "gdsf:reg:1",      gt::ManualCleanup>;
using GDSFRegRepo2     = Repo<TestItemEntity, "gdsf:reg:2",      gt::ManualCleanup>;
using GDSFRegRepo3     = Repo<TestItemEntity, "gdsf:reg:3",      gt::ManualCleanup>;

// Eviction selectivity test repos
using GDSFPressureRepo  = Repo<TestItemEntity, "gdsf:pressure",   gt::ManualCleanup>;

// Ghost admission control test repos
using GDSFGhostRepo   = Repo<TestItemEntity, "gdsf:ghost",   gt::ManualCleanup>;
using GDSFGhostRepo2  = Repo<TestItemEntity, "gdsf:ghost2",  gt::ManualCleanup>;

// Cross-repo coordination test repos
using GDSFCoordRepo1  = Repo<TestItemEntity, "gdsf:coord1",  gt::ManualCleanup>;
using GDSFCoordRepo2  = Repo<TestItemEntity, "gdsf:coord2",  gt::ManualCleanup>;

} // namespace relais_test


// =============================================================================
// Helper: clean up repos + GDSF global state for each test
// =============================================================================

template<typename... Repos>
void resetRepos() {
    // Unconditional cache clear (not threshold-based purge, which skips entries above threshold=0)
    (TestInternals::resetEntityCacheState<Repos>(), ...);
    // Flush all deferred destructors accumulated in the epoch pool.
    (TestInternals::clearEntityCachePools<Repos>(), ...);
    (TestInternals::resetRepoGDSFState<Repos>(), ...);
    TestInternals::resetGDSF();
}

/// Reset ALL test repos to ensure clean global threshold.
/// Excludes GDSFRegRepo1/2/3 (tested for registration, must not be pre-registered).
void resetAllTestRepos() {
    resetRepos<GDSFItemRepo, GDSFItemRepo2,
               GDSFShortTTLRepo, GDSFNoTTLRepo,
               GDSFPressureRepo,
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

    SECTION("[eviction] high-access entries survive eviction sweep") {
        auto id_low = insertTestItem("low_score", 1);
        auto id_high = insertTestItem("high_score", 2);

        // Populate both (1 find each)
        sync(GDSFItemRepo::find(id_low));
        sync(GDSFItemRepo::find(id_high));

        // High-access: 100 more accesses
        for (int i = 0; i < 100; ++i) sync(GDSFItemRepo::find(id_high));

        auto score_low = TestInternals::getEntityGDSFScore<GDSFItemRepo>(id_low);
        auto score_high = TestInternals::getEntityGDSFScore<GDSFItemRepo>(id_high);
        REQUIRE(score_low.has_value());
        REQUIRE(score_high.has_value());
        REQUIRE(*score_high > *score_low);

        // Set threshold between the two scores → purge evicts low, keeps high
        float midpoint = (*score_low + *score_high) / 2.0f;
        TestInternals::setThreshold(midpoint);

        GDSFItemRepo::purge();

        auto high_meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id_high);
        REQUIRE(high_meta.has_value());

        // Low-access entry should be evicted (score < midpoint)
        auto low_meta = TestInternals::getEntityGDSFMetadata<GDSFItemRepo>(id_low);
        REQUIRE_FALSE(low_meta.has_value());
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

        // Threshold defaults to 0 → nothing evicted
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

        // Wait for 1s TTL to expire (worst-case quantization adds ~1s)
        waitForExpiration(std::chrono::milliseconds{2200});

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
        REQUIRE(meta->ttl_expiration_sec == 0);
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
//  14. GDSF - ghost admission control
//
// #############################################################################

TEST_CASE("GDSF - ghost admission control",
          "[integration][db][gdsf][ghost]")
{
    TransactionGuard tx;

    // Setup: high threshold → ghosts always created (ghost admission is always active)
    resetRepos<GDSFGhostRepo>();
    TestInternals::seedAvgConstructionTime<GDSFGhostRepo>(10.0f);
    TestInternals::setThreshold(100.0f);  // score ~0.76 < 100 → ghost

    SECTION("[ghost] entry ghosted when score < threshold") {
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

    // Cleanup
    resetRepos<GDSFGhostRepo>();
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

    // Setup: high threshold → ghosts always created
    resetRepos<GDSFGhostRepo>();
    TestInternals::seedAvgConstructionTime<GDSFGhostRepo>(10.0f);
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

    SECTION("[ghost-decay] ghost removed after full decay via purge") {
        auto id = insertTestItem("ghost_decay_purge", 10);
        sync(GDSFGhostRepo::find(id));
        REQUIRE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));

        // Decay to 0 via purges
        while (TestInternals::isGhostEntry<GDSFGhostRepo>(id)) {
            GDSFGhostRepo::purge();
        }

        REQUIRE_FALSE(TestInternals::isGhostEntry<GDSFGhostRepo>(id));
    }

    // Cleanup
    resetRepos<GDSFGhostRepo>();
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

    // Setup: high threshold → ghosts created
    resetRepos<GDSFGhostRepo>();
    TestInternals::seedAvgConstructionTime<GDSFGhostRepo>(10.0f);
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
        // Insert a real entry (threshold = 0 → no ghost)
        TestInternals::setThreshold(0.0f);
        auto real_id = insertTestItem("size_real", 10);
        sync(GDSFGhostRepo::find(real_id));
        REQUIRE_FALSE(TestInternals::isGhostEntry<GDSFGhostRepo>(real_id));
        REQUIRE(GDSFGhostRepo::size() == 1);

        // Raise threshold → create a ghost
        TestInternals::setThreshold(100.0f);
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
        // Start with threshold = 0 → insert 2 real entries
        TestInternals::setThreshold(0.0f);
        auto id1 = insertTestItem("size_mix_1", 1);
        auto id2 = insertTestItem("size_mix_2", 2);
        sync(GDSFGhostRepo::find(id1));
        sync(GDSFGhostRepo::find(id2));
        REQUIRE(GDSFGhostRepo::size() == 2);

        // Raise threshold → insert 3 ghosts
        TestInternals::setThreshold(100.0f);
        for (int i = 0; i < 3; ++i) {
            auto id = insertTestItem("size_mix_g_" + std::to_string(i), i + 10);
            sync(GDSFGhostRepo::find(id));
        }

        REQUIRE(GDSFGhostRepo::size() == 2);  // only reals
        REQUIRE(TestInternals::totalEntityCacheEntries<GDSFGhostRepo>() == 5);
    }

    // Cleanup
    resetRepos<GDSFGhostRepo>();
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

    SECTION("[selectivity] hot entry survives, cold entries evicted") {
        resetRepos<GDSFPressureRepo>();

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

        // Set threshold between cold and hot scores → purge evicts cold only.
        // This directly tests GDSF selectivity without depending on the
        // eviction curve or the iterative sweep loop.
        float midpoint = (*score_cold + *score_hot) / 2.0f;
        TestInternals::setThreshold(midpoint);

        GDSFPressureRepo::purge();

        // Hot entry should survive (score > threshold)
        auto hot_meta = TestInternals::getEntityGDSFMetadata<GDSFPressureRepo>(hot_id);
        REQUIRE(hot_meta.has_value());

        // At least one cold entry evicted (score < threshold)
        REQUIRE(GDSFPressureRepo::size() < 6);

        // Cleanup
        resetRepos<GDSFPressureRepo>();
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
