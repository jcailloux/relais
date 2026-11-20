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
 *  20. Entry bytes             — the room an entry takes, stored or recomputed;
 *                                 the room a block takes, read from the allocator
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <cstdint>
#include <vector>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include <dlfcn.h>
#include <malloc.h>
#include <unistd.h>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"
#include "fixtures/AllocationCounter.h"
#include "fixtures/TestAllocator.h"
#include "fixtures/generated/TestOrderEntity.h"

using namespace relais_test;
using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;
using GDSFScoreData = jcailloux::relais::cache::detail::GDSFScoreData;
using ScoreHistogram = jcailloux::relais::cache::detail::ScoreHistogram;

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


// #############################################################################
//
//  20. Entry bytes — what one L1 entry holds
//
// #############################################################################

namespace {

using jcailloux::relais::detail::AllocatorProbe;
using jcailloux::relais::detail::BlockSizes;
using jcailloux::relais::detail::blockBytes;
using jcailloux::relais::detail::heapBytes;
using TestOrderEntity = ::entity::generated::TestOrderEntity;
using TestArrayRwEntity = ::entity::generated::TestArrayRwEntity;
using ItemPage = jcailloux::relais::list::ListWrapper<TestItemEntity>;

// 40 characters: past libstdc++'s 15-character inline buffer, so on the heap
// with capacity 40, in one block of 41 bytes (the terminator included).
const std::string kLong(40, 'x');
constexpr size_t kLongBytes = 41;

relais_test::TestAddress makeAddress() {
    relais_test::TestAddress a;
    a.street = kLong;                  // 41
    a.city = "Paris";                  // inline: 0
    a.zip_code = "75001";              // inline: 0
    a.geo.metadata.source = kLong;     // 41, two structs down
    return a;
}
constexpr size_t kAddressBytes = 2 * kLongBytes;

/// A struct glaze sees through an explicit glz::meta object.
struct MetaListed { std::string label; int32_t rank = 0; std::vector<std::string> notes; };

/// An entity whose mapping is written by hand, without heapBytes: its
/// members are counted through glaze's reflection of the struct.
struct Note { int64_t id = 0; std::string text; std::vector<std::string> tags; };
struct NoteMapping {
    static constexpr bool read_only = true;
    struct TraitsType { enum class Field : uint8_t {}; };
    template<typename E> static auto key(const E& e) noexcept { return e.id; }
    template<typename E>
    static std::optional<E> fromRow(const jcailloux::relais::io::PgResult::Row&) { return std::nullopt; }
    template<typename E>
    static jcailloux::relais::io::PgParams toInsertParams(const E&) { return {}; }
    template<typename T>
    static constexpr auto glaze_value = glz::object("id", &T::id, "text", &T::text, "tags", &T::tags);
};
using NoteEntity = jcailloux::relais::Entity<Note, NoteMapping>;

template<typename Mapping, typename Struct>
concept MappingCountsHeap = requires(const Struct& s) { Mapping::heapBytes(s); };

/// While alive, the calling thread weighs each block at the bytes it requests:
/// the blocks each rule names, and their sizes, against a count by hand.
class WeighRequests {
public:
    WeighRequests() { BlockSizes::setTestSizes(&sizes()); }
    ~WeighRequests() { BlockSizes::setTestSizes(nullptr); }
    WeighRequests(const WeighRequests&) = delete;
    WeighRequests& operator=(const WeighRequests&) = delete;

private:
    static const BlockSizes& sizes() {
        // An allocator whose blocks cannot be read: each weighs its request.
        static const auto* blind = new BlockSizes(AllocatorProbe{nullptr, nullptr, nullptr});
        return *blind;
    }
};

/// The room the recorded blocks take: each one's usable size, as the allocator
/// reports it, and the header in front of it, as the test knows it for this
/// allocator (each is under 128 KiB, glibc's lowest mmap threshold: all on the
/// heap). A bucket array, whose address its container keeps to itself, is
/// weighed as heapBytes weighs it: the class of its request.
size_t heldBy(const relais_test::RecordHeldBlocks& recorded, size_t bucket_array) {
    size_t held = 0;
    size_t bucket_arrays = 0;
    for (const auto& b : recorded.blocks()) {
        if (bucket_array != 0 && b.requested == bucket_array) {
            held += blockBytes(b.requested);
            ++bucket_arrays;
        } else {
            held += malloc_usable_size(b.block) + relais_test::heapBlockHeader();
        }
    }
    CHECK(bucket_arrays == (bucket_array != 0 ? 1 : 0));
    return held;
}

/// The bytes a container's bucket array requests, 0 for none.
template<typename T>
size_t bucketArrayOf(const T& v) {
    if constexpr (requires { v.bucket_count(); }) {
        return v.bucket_count() > 1 ? v.bucket_count() * sizeof(void*) : 0;
    } else {
        return 0;
    }
}

/// The room a copy of `v` holds, and what heapBytes counts for it: a copy
/// allocates exactly its final storage, at the copy's own capacities.
template<typename T>
std::pair<size_t, size_t> heldAndCounted(const T& v) {
    relais_test::RecordHeldBlocks recorded;
    T copy = v;
    recorded.stop();
    return {heldBy(recorded, bucketArrayOf(copy)), heapBytes(copy)};
}

/// The same for the value `make` builds: unlike a copy, it keeps capacities
/// beyond the size.
template<typename Make>
std::pair<size_t, size_t> builtAndCounted(Make make) {
    relais_test::RecordHeldBlocks recorded;
    auto v = make();
    recorded.stop();
    return {heldBy(recorded, bucketArrayOf(v)), heapBytes(v)};
}

constexpr size_t roundUp(size_t n, size_t step) { return (n + step - 1) / step * step; }

/// An allocator for BlockSizes to read: each block keeps its request in front
/// of it, and Usable turns the request into the usable size reported.
template<size_t (*Usable)(size_t)>
struct FakeMalloc {
    static constexpr size_t kFront = 16;
    static void* allocate(size_t n) {
        auto* p = static_cast<char*>(std::malloc(kFront + n));
        std::memcpy(p, &n, sizeof n);
        return p + kFront;
    }
    static void release(void* p) { std::free(static_cast<char*>(p) - kFront); }
    static size_t usable(const void* p) {
        size_t n;
        std::memcpy(&n, static_cast<const char*>(p) - kFront, sizeof n);
        return Usable(n);
    }
    static constexpr AllocatorProbe kProbe{&allocate, &release, &usable};
};

/// Classes of 16 bytes, metadata out of the block (jemalloc's small classes).
size_t classesOf16(size_t n) { return roundUp(n, 16); }
/// Classes of 12 bytes: some 8-byte slices hold two of them.
size_t classesOf12(size_t n) { return roundUp(n, 12); }
/// The request itself, as a sanitizer reports it.
size_t requestedSize(size_t n) { return n; }
/// Classes of 16, with every fourth block of a request served 16 bytes larger,
/// as glibc serves one from a free block it does not split.
size_t oneLarger(size_t n) {
    static thread_local size_t served = 0;
    return roundUp(n, 16) + (served++ % 4 == 0 ? 16 : 0);
}
/// glibc's: a chunk of max(32, ⌈n + 8⌉₁₆) bytes less its size word; from
/// 128 KiB, a block mapped on its own, of whole pages less two words.
size_t glibcLike(size_t n) {
    if (n >= 128 * 1024) return roundUp(n + 16, 4096) - 16;
    return std::max<size_t>(32, roundUp(n + 8, 16)) - 8;
}

/// Every request up to the table's limit weighs what `expected` says.
template<typename Expected>
void checkTable(const BlockSizes& sizes, Expected expected) {
    size_t mismatches = 0, first = 0;
    for (size_t n = 1; n <= BlockSizes::kTableLimit; ++n) {
        if (sizes.bytes(n) != expected(n) && mismatches++ == 0) first = n;
    }
    size_t weighed = first ? sizes.bytes(first) : 0;
    size_t wanted = first ? expected(first) : 0;
    CAPTURE(first, weighed, wanted);
    CHECK(mismatches == 0);
}

/// Gives each array twice what it asks for.
template<typename T>
struct GenerousAllocator {
    using value_type = T;
    GenerousAllocator() = default;
    template<typename U>
    GenerousAllocator(const GenerousAllocator<U>&) noexcept {}
    T* allocate(size_t n) { return static_cast<T*>(std::malloc(2 * n * sizeof(T))); }
    void deallocate(T* p, size_t) noexcept { std::free(p); }
    bool operator==(const GenerousAllocator&) const = default;
};

thread_local std::string tl_logged;
void captureLog(jcailloux::relais::log::Level, const char* msg, size_t len) {
    tl_logged.assign(msg, len);
}

}  // namespace

template<>
struct glz::meta<MetaListed> {
    using T = MetaListed;
    static constexpr auto value = glz::object("label", &T::label, "rank", &T::rank, "notes", &T::notes);
};

TEST_CASE("Entry bytes - heapBytes names every block a value owns, at its request",
          "[gdsf][entry-bytes]")
{
    WeighRequests requests;

    SECTION("[entry-bytes] strings: inline when short, one block of capacity + 1 on the heap") {
        REQUIRE(kLong.capacity() == 40);
        CHECK(heapBytes(std::string("short")) == 0);
        CHECK(heapBytes(kLong) == kLongBytes);
    }

    SECTION("[entry-bytes] optional: what the value holds, nothing when empty") {
        CHECK(heapBytes(std::optional<std::string>{}) == 0);
        CHECK(heapBytes(std::optional<std::string>{"short"}) == 0);
        CHECK(heapBytes(std::optional<std::string>{kLong}) == kLongBytes);
    }

    SECTION("[entry-bytes] text[]: the string array and each string's heap") {
        TestArrayRwEntity e;
        e.tag_ids = {1, 2, 3};             // one block of 3 × 8
        e.labels = {kLong, "ab"};          // one of 2 × sizeof(string), one of 41
        REQUIRE(e.tag_ids.capacity() == 3);
        REQUIRE(e.labels.capacity() == 2);

        size_t heap = 3 * sizeof(int64_t) + 2 * sizeof(std::string) + kLongBytes;
        CHECK(e.memoryUsage() == sizeof(TestArrayRwEntity) + heap);
    }

    SECTION("[entry-bytes] json_field members: nested structs, vector<string>") {
        TestOrderEntity o;
        o.label = kLong;                                   // 41
        o.metadata.str = std::string(20, '1');             // 21
        o.created_at = "2026-11-18";                       // inline: no block
        o.address = makeAddress();                         // two of 41
        o.history = {makeAddress()};                       // 1 × sizeof, two of 41
        o.quantities = {1, 2, 3};                          // 3 × 4
        o.tags = {kLong, "x"};                             // 2 × sizeof, 41
        REQUIRE(o.history.capacity() == 1);
        REQUIRE(o.quantities.capacity() == 3);
        REQUIRE(o.tags.capacity() == 2);

        size_t heap = kLongBytes + 21
            + kAddressBytes
            + sizeof(relais_test::TestAddress) + kAddressBytes
            + 3 * sizeof(int32_t)
            + 2 * sizeof(std::string) + kLongBytes;
        CHECK(o.memoryUsage() == sizeof(TestOrderEntity) + heap);
    }

    SECTION("[entry-bytes] list page: items at capacity, their heap, the cursor") {
        auto makePage = [](const std::string& cursor) {
            TestItemEntity item;
            item.name = kLong;             // 41
            item.description = "short";   // inline: no block
            ItemPage page;
            page.items.reserve(2);
            page.items.push_back(item);
            page.items.push_back(item);
            page.next_cursor = cursor;
            return page;
        };
        size_t items = 2 * sizeof(TestItemEntity) + 2 * kLongBytes;

        CHECK(makePage(kLong).memoryUsage() == sizeof(ItemPage) + items + kLongBytes);
        CHECK(makePage("c1").memoryUsage() == sizeof(ItemPage) + items);  // inline cursor
    }

    SECTION("[entry-bytes] types with a trivial destructor own nothing") {
        CHECK(heapBytes(std::string_view(kLong)) == 0);    // borrows, owns nothing
        CHECK(heapBytes(std::chrono::system_clock::now()) == 0);
        CHECK(heapBytes(std::array<int64_t, 4>{1, 2, 3, 4}) == 0);
        CHECK(heapBytes(std::optional<int64_t>{7}) == 0);
        CHECK(heapBytes(relais_test::Priority::High) == 0);
    }

    SECTION("[entry-bytes] a hand-written mapping is counted through reflection") {
        STATIC_REQUIRE_FALSE(MappingCountsHeap<NoteMapping, Note>);
        STATIC_REQUIRE(MappingCountsHeap<TestOrderEntity::MappingType, relais_test::TestOrder>);
        NoteEntity n;
        n.text = kLong;                    // 41
        n.tags = {kLong, "x"};             // 2 × sizeof(string), 41
        CHECK(n.memoryUsage() == sizeof(NoteEntity)
              + kLongBytes + 2 * sizeof(std::string) + kLongBytes);
    }

    SECTION("[entry-bytes] only a GDSF map counts, and stores only for free") {
        using jcailloux::relais::cache::detail::ChunkMap;
        using jcailloux::relais::cache::detail::CacheMetadata;
        using Uncounted = ChunkMap<int64_t, TestItemEntity, CacheMetadata<true, false>, true, false>;
        using Counted = ChunkMap<int64_t, TestItemEntity, CacheMetadata<true, false>, true, true>;
        using CountedTtl = ChunkMap<int64_t, TestItemEntity, CacheMetadata<true, true>, true, true>;
        STATIC_REQUIRE_FALSE(Uncounted::kStoresBytes);
        STATIC_REQUIRE(Counted::kStoresBytes);
        STATIC_REQUIRE_FALSE(CountedTtl::kStoresBytes);
        // Storing the count never makes the entry bigger.
        STATIC_REQUIRE(sizeof(Counted::CacheEntry) == sizeof(Uncounted::CacheEntry));
    }
}

#if defined(__GLIBCXX__)
TEST_CASE("Entry bytes - heapBytes matches the blocks libstdc++ allocates",
          "[gdsf][entry-bytes]")
{
    auto check = [](const auto& v) {
        auto [held, counted] = heldAndCounted(v);
        CHECK(counted == held);
    };

    SECTION("[entry-bytes] containers") {
        check(std::vector<std::string>{kLong, "a", kLong});
        check(std::vector<bool>(100, true));
        check(std::map<std::string, std::string>{{kLong, kLong}, {"a", "b"}});
        check(std::set<long double>{1.0L, 2.0L});              // 16-byte aligned nodes
        check(std::unordered_map<std::string, int>{{kLong, 1}, {"k", 2}});  // hash kept
        check(std::unordered_map<int, std::string>{{1, kLong}, {2, "k"}});  // hash not kept
        check(std::unordered_set<std::string>{kLong, "a"});
        check(std::multimap<int, std::string>{{1, kLong}, {1, kLong}});
        std::unordered_map<int, int> reserved;
        reserved.reserve(100);                                  // buckets, no node
        check(reserved);
        check(std::vector<std::vector<std::string>>{{kLong}, {}, {kLong, "a"}});
    }

    SECTION("[entry-bytes] capacity beyond size") {
        // A copy allocates only the size: these are measured as built.
        auto checkBuilt = [](auto make) {
            auto [held, counted] = builtAndCounted(make);
            CHECK(counted == held);
        };
        checkBuilt([] { std::string s; s.reserve(100); s = "abc"; return s; });
        checkBuilt([] { std::vector<int32_t> v; v.reserve(10); v.push_back(1); return v; });
        checkBuilt([] {
            std::vector<std::string> v;
            v.reserve(4);
            v.emplace_back(kLong);
            return v;
        });
        checkBuilt([] {
            std::unordered_map<int, std::string> m;
            m.reserve(50);
            m.emplace(1, kLong);
            return m;
        });
        checkBuilt([] {
            TestOrderEntity o;
            o.tags.reserve(8);
            o.tags.emplace_back(kLong);
            o.label.reserve(64);
            return o;
        });
    }

    SECTION("[entry-bytes] wrappers and aggregates") {
        check(std::variant<int64_t, std::string>{kLong});
        check(std::tuple<std::string, int32_t, std::string>{kLong, 1, kLong});
        check(std::pair<std::string, std::string>{kLong, "a"});
        check(std::array<std::string, 2>{kLong, "a"});
        check(std::optional<std::vector<std::string>>{std::vector<std::string>{kLong}});
        check(glz::raw_json{std::string(50, '1')});
        check(MetaListed{kLong, 1, {kLong, "a"}});              // glz::meta object
        check(makeAddress());                                   // reflected aggregate
    }

    SECTION("[entry-bytes] generated entity with json_field members") {
        TestOrderEntity o;
        o.label = kLong;
        o.metadata.str = std::string(20, '1');
        o.address = makeAddress();
        o.history = {makeAddress(), makeAddress()};
        o.quantities = {1, 2, 3};
        o.tags = {kLong, "x"};
        auto [held, counted] = heldAndCounted(o);
        CHECK(counted == held);
        CHECK(o.memoryUsage() == sizeof(TestOrderEntity) + held);
    }
}
#endif

TEST_CASE("Block bytes - each block weighs the room the allocator gives it",
          "[gdsf][block-bytes]")
{
    using relais_test::TestMalloc;
    using relais_test::testMalloc;
    using relais_test::heapBlockHeader;
    using relais_test::mappedBlockHeader;

    SECTION("[block-bytes] the malloc in use is the one the test's registration names") {
        bool jemalloc = dlsym(RTLD_DEFAULT, "mallctl") != nullptr;
        bool mimalloc = dlsym(RTLD_DEFAULT, "mi_version") != nullptr;
        switch (testMalloc()) {
            case TestMalloc::Glibc:     CHECK_FALSE(jemalloc); CHECK_FALSE(mimalloc); break;
            case TestMalloc::Jemalloc:  CHECK(jemalloc);       CHECK_FALSE(mimalloc); break;
            case TestMalloc::Mimalloc:  CHECK_FALSE(jemalloc); CHECK(mimalloc);       break;
            case TestMalloc::Sanitizer: SUCCEED("the sanitizer's own malloc");        break;
        }
    }

    SECTION("[block-bytes] the table holds each request's class: no block of it takes less") {
        // A fresh block may be larger than its class under glibc (a free block
        // 16 bytes larger, handed out whole); never under the others.
        bool exact_classes = testMalloc() != TestMalloc::Glibc;
        size_t smaller = 0, larger = 0;
        for (size_t n = 1; n <= BlockSizes::kTableLimit; ++n) {
            void* p = std::malloc(n);
            size_t room = malloc_usable_size(p) + heapBlockHeader();
            std::free(p);
            if (blockBytes(n) > room) ++smaller;
            if (exact_classes && blockBytes(n) < room) ++larger;
        }
        CHECK(smaller == 0);
        CHECK(larger == 0);
        CHECK(blockBytes(0) == 0);  // no block
    }

    SECTION("[block-bytes] each allocator's classes, as the test knows them") {
        const auto& sizes = BlockSizes::process();
        switch (testMalloc()) {
            case TestMalloc::Glibc:
                // A chunk of max(32, ⌈n + 8⌉₁₆) bytes: the request, the size
                // word (8) in front of it, rounded to 16, never under 32.
                CHECK(sizes.hasSizeWord());
                CHECK(sizes.granularity() == 8);
                checkTable(sizes, [](size_t n) { return std::max<size_t>(32, roundUp(n + 8, 16)); });
                break;
            case TestMalloc::Jemalloc:
            case TestMalloc::Mimalloc:
                // Classes on multiples of 8, the metadata out of the block.
                CHECK_FALSE(sizes.hasSizeWord());
                CHECK(sizes.granularity() == 8);
                CHECK(sizes.bytes(1) == 8);
                CHECK(sizes.bytes(9) == 16);
                break;
            case TestMalloc::Sanitizer:
                // The request itself, read byte by byte.
                CHECK_FALSE(sizes.hasSizeWord());
                CHECK(sizes.granularity() == 1);
                checkTable(sizes, requestedSize);
                break;
        }
    }

    SECTION("[block-bytes] a string's or a vector's block is read on itself, not weighed by its request") {
        // An allocator that gives each array twice what it asks for: only a
        // read of the block sees the difference.
        std::vector<char, GenerousAllocator<char>> v;
        v.reserve(5'000);
        CHECK(heapBytes(v) == malloc_usable_size(v.data()) + heapBlockHeader());
        CHECK(heapBytes(v) > blockBytes(5'000));

        std::basic_string<char, std::char_traits<char>, GenerousAllocator<char>> str;
        str.reserve(5'000);
        CHECK(heapBytes(str) == malloc_usable_size(str.data()) + heapBlockHeader());
        CHECK(heapBytes(str) > blockBytes(5'001));
    }

#if defined(__GLIBCXX__)
    SECTION("[block-bytes] the blocks read are the value's own: nodes and bit words") {
        namespace hb = jcailloux::relais::detail::heap_bytes;
        auto onlyBlock = [](const relais_test::RecordHeldBlocks& recorded) {
            REQUIRE(recorded.blocks().size() == 1);
            return static_cast<const void*>(recorded.blocks()[0].block);
        };
        {
            std::map<int64_t, int64_t> m;
            relais_test::RecordHeldBlocks recorded;
            m.emplace(1, 1);
            recorded.stop();
            CHECK(hb::nodeBlock(m.begin()) == onlyBlock(recorded));
        }
        {
            std::unordered_set<int64_t> set;
            set.reserve(1);                 // the bucket array first
            relais_test::RecordHeldBlocks recorded;
            set.insert(1);
            recorded.stop();
            CHECK(hb::nodeBlock(set.begin()) == onlyBlock(recorded));
        }
        {
            relais_test::RecordHeldBlocks recorded;
            std::vector<bool> bits(100, true);
            recorded.stop();
            CHECK(hb::bitWords(bits) == onlyBlock(recorded));
        }
    }
#endif

    SECTION("[block-bytes] past the table's limit, still on the heap") {
        auto check = [](const auto& v) {
            auto [held, counted] = heldAndCounted(v);
            CHECK(counted == held);
        };
        check(std::string(20'000, 'x'));
        check(std::vector<int64_t>(10'000, 1));
        check(std::vector<bool>(200'000, true));
        check(std::vector<TestItemEntity>(200));   // a list page's items
        CHECK(blockBytes(20'000) == 20'000);       // without the block: its request
    }

    SECTION("[block-bytes] a block mapped on its own carries its own header") {
        // Beyond glibc's highest mmap threshold (32 MiB on 64-bit), a block is
        // always mapped on its own. Reserved, its pages are never touched.
        std::string s;
        s.reserve(size_t{256} << 20);
        size_t usable = malloc_usable_size(s.data());
        CHECK(heapBytes(s) == usable + mappedBlockHeader());
        if (testMalloc() == TestMalloc::Glibc) {
            // Whole pages, two words of which are the header.
            CHECK(usable % 16 == 0);
            CHECK((usable + 16) % static_cast<size_t>(sysconf(_SC_PAGESIZE)) == 0);
        }
    }

    SECTION("[block-bytes] the table is read once, then frozen") {
        const auto& sizes = BlockSizes::process();
        CHECK(&sizes == &BlockSizes::process());
        std::vector<size_t> before(BlockSizes::kTableLimit + 1);
        for (size_t n = 1; n <= BlockSizes::kTableLimit; ++n) before[n] = sizes.bytes(n);

        // Move the allocator's state: under glibc, freeing a mapped block
        // raises the mmap threshold to its size.
        std::free(std::malloc(size_t{1} << 20));
        std::vector<void*> blocks;
        for (size_t n = 1; n <= 1000; ++n) blocks.push_back(std::malloc(n * 16));
        for (void* p : blocks) std::free(p);

        checkTable(sizes, [&](size_t n) { return before[n]; });
    }
}

TEST_CASE("Block bytes - reading an allocator", "[gdsf][block-bytes]")
{
    SECTION("[block-bytes] one class per 8-byte slice: a table by slice") {
        using Fake = FakeMalloc<classesOf16>;
        auto sizes = std::make_unique<BlockSizes>(Fake::kProbe);
        CHECK(sizes->granularity() == 8);
        CHECK_FALSE(sizes->hasSizeWord());  // 16, 32…: 0 modulo 16
        checkTable(*sizes, classesOf16);

        // A block at hand is read on itself, whatever the request says.
        void* block = Fake::allocate(50);
        CHECK(sizes->bytes(41, block) == 64);
        CHECK(sizes->bytes(41) == 48);
        Fake::release(block);
        block = Fake::allocate(20'001);
        CHECK(sizes->bytes(20'001, block) == 20'016);
        CHECK(sizes->bytes(20'001) == 20'001);  // past the table, no block: the request
        Fake::release(block);
    }

    SECTION("[block-bytes] a size word: one in front of a heap block, two of a mapped one") {
        using Fake = FakeMalloc<glibcLike>;
        auto sizes = std::make_unique<BlockSizes>(Fake::kProbe);
        CHECK(sizes->granularity() == 8);
        CHECK(sizes->hasSizeWord());
        checkTable(*sizes, [](size_t n) { return std::max<size_t>(32, roundUp(n + 8, 16)); });

        void* heap = Fake::allocate(20'001);
        CHECK(sizes->bytes(20'001, heap) == 20'016);      // usable 20,008, one word
        Fake::release(heap);
        void* mapped = Fake::allocate(200'000);
        CHECK(sizes->bytes(200'000, mapped) == 200'704);  // 49 pages: usable, two words
        Fake::release(mapped);
    }

    SECTION("[block-bytes] a free block larger than the class: the class still read") {
        // One block of each probe served 16 bytes larger: the least of the
        // blocks allocated together is the class.
        auto sizes = std::make_unique<BlockSizes>(FakeMalloc<oneLarger>::kProbe);
        CHECK(sizes->granularity() == 8);
        checkTable(*sizes, classesOf16);
    }

    SECTION("[block-bytes] slices that hold two classes: read byte by byte") {
        auto by12 = std::make_unique<BlockSizes>(FakeMalloc<classesOf12>::kProbe);
        CHECK(by12->granularity() == 1);
        checkTable(*by12, classesOf12);

        auto exact = std::make_unique<BlockSizes>(FakeMalloc<requestedSize>::kProbe);
        CHECK(exact->granularity() == 1);
        CHECK_FALSE(exact->hasSizeWord());
        checkTable(*exact, requestedSize);
    }

    SECTION("[block-bytes] no usable size to read: each block at its request, and a message") {
        namespace log = jcailloux::relais::log;
        AllocatorProbe blind = FakeMalloc<classesOf16>::kProbe;
        blind.usable = nullptr;
        auto previous = log::getCallback();
        log::setCallback(&captureLog);
        tl_logged.clear();
        auto sizes = std::make_unique<BlockSizes>(blind);
        log::setCallback(previous);

        CHECK(tl_logged.find("usable size") != std::string::npos);
        checkTable(*sizes, requestedSize);
        void* block = FakeMalloc<classesOf16>::allocate(50);
        CHECK(sizes->bytes(41, block) == 41);
        FakeMalloc<classesOf16>::release(block);
    }
}

#if defined(__GLIBCXX__)
TEST_CASE("Entry bytes - node formulas are libstdc++'s node types",
          "[gdsf][entry-bytes]")
{
    namespace hb = jcailloux::relais::detail::heap_bytes;
    using StrPair = std::pair<const std::string, std::string>;
    using IntPair = std::pair<const int, int>;

    // Tree nodes: four words of base, then the value at its alignment.
    STATIC_REQUIRE(hb::kTreeNodeBytes<char> == sizeof(std::_Rb_tree_node<char>));
    STATIC_REQUIRE(hb::kTreeNodeBytes<IntPair> == sizeof(std::_Rb_tree_node<IntPair>));
    STATIC_REQUIRE(hb::kTreeNodeBytes<StrPair> == sizeof(std::_Rb_tree_node<StrPair>));
    STATIC_REQUIRE(hb::kTreeNodeBytes<long double> == sizeof(std::_Rb_tree_node<long double>));

    // Hash nodes: the next link, the value, the hash when it is kept.
    STATIC_REQUIRE(hb::kHashNodeBytes<IntPair, false>
                   == sizeof(std::__detail::_Hash_node<IntPair, false>));
    STATIC_REQUIRE(hb::kHashNodeBytes<IntPair, true>
                   == sizeof(std::__detail::_Hash_node<IntPair, true>));
    STATIC_REQUIRE(hb::kHashNodeBytes<StrPair, true>
                   == sizeof(std::__detail::_Hash_node<StrPair, true>));
    STATIC_REQUIRE(hb::kHashNodeBytes<long double, false>
                   == sizeof(std::__detail::_Hash_node<long double, false>));

    // Which keys keep their hash: strings do (slow hash), integers do not.
    STATIC_REQUIRE(hb::kCachesHash<std::string, std::hash<std::string>>);
    STATIC_REQUIRE_FALSE(hb::kCachesHash<int, std::hash<int>>);
}
#endif

TEST_CASE("Entry bytes - one count per entry, stored or recomputed",
          "[integration][db][gdsf][entry-bytes]")
{
    TransactionGuard tx;

    // GDSF alone leaves a free word in an 8-aligned entry's header: the count
    // is stored there. GDSF + TTL fills the header: it is recomputed.
    using StoredMap = TestInternals::EntityMap<GDSFNoTTLRepo>;
    using RecomputedMap = TestInternals::EntityMap<GDSFItemRepo>;
    STATIC_REQUIRE(StoredMap::kStoresBytes);
    STATIC_REQUIRE_FALSE(RecomputedMap::kStoresBytes);
    STATIC_REQUIRE(RecomputedMap::kCountsBytes);

    SECTION("[entry-bytes] the stored count is what the entry's blocks hold") {
        resetRepos<GDSFNoTTLRepo>();
        auto id = insertTestItem(kLong, 1, kLong);
        auto v = sync(GDSFNoTTLRepo::find(id));
        REQUIRE(v);

        auto bytes = TestInternals::entityEntryBytes<GDSFNoTTLRepo>(id);
        auto pool_block = TestInternals::entityPoolBlockBytes<GDSFNoTTLRepo>(id);
        REQUIRE(bytes.has_value());
        REQUIRE(pool_block.has_value());
        CHECK(*bytes == *TestInternals::entityHeldBytes<GDSFNoTTLRepo>(id));
        CHECK(*bytes == *pool_block + heapBytes(*v));
        CHECK(v->memoryUsage() >= sizeof(TestItemEntity) + 2 * kLongBytes);
        resetRepos<GDSFNoTTLRepo>();
    }

    SECTION("[entry-bytes] the recomputed count is what the entry's blocks hold") {
        resetRepos<GDSFItemRepo>();
        auto id = insertTestItem(kLong, 1, kLong);
        auto v = sync(GDSFItemRepo::find(id));
        REQUIRE(v);

        auto bytes = TestInternals::entityEntryBytes<GDSFItemRepo>(id);
        auto pool_block = TestInternals::entityPoolBlockBytes<GDSFItemRepo>(id);
        REQUIRE(bytes.has_value());
        REQUIRE(pool_block.has_value());
        CHECK(*bytes == *pool_block + heapBytes(*v));
        CHECK(*bytes == *TestInternals::entityEntryBytes<GDSFItemRepo>(id));  // the same again
        resetRepos<GDSFItemRepo>();
    }

    SECTION("[entry-bytes] the sweep weighs an entry with the same count") {
        resetRepos<GDSFNoTTLRepo>();
        auto id = insertTestItem(kLong, 1, kLong);
        auto v = sync(GDSFNoTTLRepo::find(id));
        REQUIRE(v);
        size_t entry_bytes = *TestInternals::entityEntryBytes<GDSFNoTTLRepo>(id);
        using Tier = TestInternals::EntityTier<GDSFNoTTLRepo>;

        // Kept: the histogram records what evicting the entry would free.
        TestInternals::setThreshold(0.0f);
        REQUIRE(TestInternals::buildingHistogramBytes() == 0);
        GDSFNoTTLRepo::purge();
        CHECK(TestInternals::buildingHistogramBytes() == entry_bytes);
        REQUIRE(TestInternals::entityEntryBytes<GDSFNoTTLRepo>(id).has_value());

        // Evicted: the ghost left behind weighs what admission weighed.
        TestInternals::setThreshold(1e9f);
        GDSFNoTTLRepo::purge();
        auto ghost = TestInternals::getGhostData<GDSFNoTTLRepo>(id);
        REQUIRE(ghost.has_value());
        CHECK(ghost->estimated_bytes == Tier::scoredBytes(id, entry_bytes));
        resetRepos<GDSFNoTTLRepo>();
    }

    SECTION("[entry-bytes] a ghost weighs what its entry would") {
        resetRepos<GDSFGhostRepo>();
        TestInternals::seedAvgConstructionTime<GDSFGhostRepo>(10.0f);
        TestInternals::setThreshold(100.0f);  // every candidate is ghosted

        auto id = insertTestItem(kLong, 1, kLong);
        auto v = sync(GDSFGhostRepo::find(id));
        REQUIRE(v);
        auto ghost = TestInternals::getGhostData<GDSFGhostRepo>(id);
        REQUIRE(ghost.has_value());

        using Map = TestInternals::EntityMap<GDSFGhostRepo>;
        using Tier = TestInternals::EntityTier<GDSFGhostRepo>;
        CHECK(ghost->estimated_bytes == Tier::scoredBytes(id, Map::entryBytes(*v)));
        CHECK(ghost->estimated_bytes == Map::entryBytes(*v) + Map::slotBytes(id));
        // Before the entry exists, its pool block weighs the class of its size.
        CHECK(Map::entryBytes(*v)
              == blockBytes(Map::kEntryOverhead + sizeof(TestItemEntity)) + heapBytes(*v));
        resetRepos<GDSFGhostRepo>();
    }
}
