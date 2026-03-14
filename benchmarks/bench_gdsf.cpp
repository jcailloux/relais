/**
 * bench_gdsf.cpp
 *
 * Parametric matrix benchmark for GDSF eviction policy.
 * Compiled with RELAIS_GDSF_ENABLED=1 to enable size-aware eviction.
 *
 * Matrix: 3 skews × 3 pressures × 2 size profiles = 18 combinations
 *   - Skew:     s=0.8 (mild), s=1.0 (standard Zipf), s=1.2 (heavy)
 *   - Pressure: 90% (low eviction), 50% (medium), 20% (high eviction)
 *   - Sizes:    uniform (~1KB each) or varied (alternating ~1KB / ~2KB)
 *
 * Working set: 10K items × ~1KB = ~10MB cache footprint (significant vs ~15-25MB RSS).
 *
 * Budget is self-calibrating: RSS is measured before and after cache insertion.
 * max_memory = RSS_baseline + actual_cache_footprint × pressure.
 * Each entry carries a ~1KB description so the cache footprint (~10MB for 10K items)
 * is significant relative to process RSS.
 *
 * Design:
 *   1. Snapshot RSS baseline, insert N items into DB + L1 (all cached, access_count=1)
 *   2. Set budget = RSS_baseline + cache_headroom (pressure × estimated cache size)
 *   3. Warm up: run target distribution to build access counts (all L1 hits)
 *   4. Sweep rounds: threshold computed from RSS vs budget via histogram
 *   5. Measure: 100K fixed-ops L1-only lookups, count hits vs misses
 *
 * Run with:
 *   ./bench_gdsf                      # all 18 combinations
 *   ./bench_gdsf "[gdsf]"            # same (single tag)
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include "BenchEngine.h"

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>

#include <jcailloux/relais/runtime/CachedHeap.h>
#include <jcailloux/relais/runtime/RuntimeThread.h>

using namespace relais_test;
using namespace relais_bench;
using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;
using CachedHeap = jcailloux::relais::runtime::CachedHeap;
using RuntimeThread = jcailloux::relais::runtime::RuntimeThread;

static_assert(GDSFPolicy::enabled,
    "bench_gdsf.cpp must be compiled with RELAIS_GDSF_ENABLED=1");

// =============================================================================
// GDSF Benchmark Repos (dedicated names to avoid interference)
// =============================================================================

namespace relais_test::gdsf_bench {

using namespace jcailloux::relais::config;

// No TTL — pure GDSF eviction
inline constexpr auto NoTTL = Local
    .with_l1_ttl(std::chrono::nanoseconds{0});

} // namespace relais_test::gdsf_bench

namespace relais_test {

using GDSFBenchRepo = Repo<TestItemEntity, "bench:gdsf:zipf", gdsf_bench::NoTTL>;

} // namespace relais_test

// =============================================================================
// Zipf Generator — inverse CDF sampling, O(log N) per draw
// =============================================================================

class ZipfGenerator {
    std::vector<double> cdf_;
    std::mt19937_64 rng_;

public:
    ZipfGenerator(size_t n, double s, uint64_t seed)
        : rng_(seed)
    {
        cdf_.resize(n);
        double sum = 0.0;
        for (size_t i = 0; i < n; ++i)
            sum += 1.0 / std::pow(static_cast<double>(i + 1), s);
        double cumul = 0.0;
        for (size_t i = 0; i < n; ++i) {
            cumul += (1.0 / std::pow(static_cast<double>(i + 1), s)) / sum;
            cdf_[i] = cumul;
        }
    }

    size_t next() {
        double u = std::uniform_real_distribution<double>(0.0, 1.0)(rng_);
        auto it = std::lower_bound(cdf_.begin(), cdf_.end(), u);
        if (it == cdf_.end()) return cdf_.size() - 1;
        return static_cast<size_t>(std::distance(cdf_.begin(), it));
    }
};

// =============================================================================
// Helpers
// =============================================================================

namespace {

/// Insert N items into DB and populate L1 via sync(find(id)).
/// All items start with access_count=1. sync() is called from the main thread.
/// The histogram is pre-warmed (16 non-evicting sweeps) so that subsequent
/// eviction is guided by GDSF scores, not a cold-start nuclear threshold.
struct SetupResult {
    std::vector<int64_t> ids;
};

SetupResult setupGDSFBench(size_t n_items, double pressure,
                            bool varied_sizes = false) {
    // Ensure RuntimeThread is running so CachedHeap is populated.
    RuntimeThread::ensureStarted();

    // Reset everything (drain epoch pool before zeroing memory counters)
    TestInternals::resetCacheForGDSF<GDSFBenchRepo>();

    // Disable budget during insertion to prevent periodic sweeps from
    // nuking chunks with an empty histogram.
    GDSFPolicy::instance().configure({.max_memory = SIZE_MAX});

    // Snapshot RSS BEFORE inserting cache entries.
    CachedHeap::tick();
    auto rss_baseline = CachedHeap::bytes();

    // Each entry gets a ~1KB description so cache footprint is significant.
    // With 10K items × ~1KB = ~10MB of cache, visible in RSS.
    std::string base_desc(1024, 'x');

    std::vector<int64_t> ids;
    ids.reserve(n_items);
    for (size_t i = 0; i < n_items; ++i) {
        // Varied: alternate ~1KB / ~2KB descriptions.
        auto desc = varied_sizes && (i % 2 == 1)
            ? base_desc + base_desc + "_" + std::to_string(i)
            : base_desc + "_" + std::to_string(i);
        auto kid = insertTestItem("gdsf_bench_" + std::to_string(i),
                                   static_cast<int32_t>(i), desc);
        sync(GDSFBenchRepo::find(kid));
        ids.push_back(kid);
    }

    // Pre-warm histogram: 16 sweeps (2 full rounds of 8 chunks).
    // Populates the persistent EMA histogram so that eviction uses
    // real score distributions, not exp2(23.25).
    for (int i = 0; i < 16; ++i) GDSFPolicy::instance().sweep();

    // Measure actual cache footprint in RSS, then apply pressure.
    // pressure=0.9 → allow 90% of cache to stay, evict 10%.
    // pressure=0.2 → allow 20% of cache to stay, evict 80%.
    CachedHeap::tick();
    auto rss_after = CachedHeap::bytes();
    auto cache_footprint = (rss_after > rss_baseline)
        ? rss_after - rss_baseline : 0ULL;
    auto budget = rss_baseline
        + static_cast<uint64_t>(static_cast<double>(cache_footprint) * pressure);
    GDSFPolicy::instance().configure({.max_memory = static_cast<size_t>(budget)});

    WARN("  [setup] heap_baseline=" << (rss_baseline / 1024) << " KB"
         << "  heap_after=" << (rss_after / 1024) << " KB"
         << "  footprint=" << (cache_footprint / 1024) << " KB"
         << "  budget=" << (budget / 1024) << " KB");

    return {std::move(ids)};
}

/// Build access counts with the given distribution (L1 hits only, no sync).
/// Biases GDSF scores so hot items are retained during eviction.
/// Then rebuilds the histogram (16 non-evicting sweeps) so that eviction
/// uses the post-warmup score distribution, not the stale setup-time one.
template<typename KeyGen>
void warmupAccess(const std::vector<int64_t>& ids, KeyGen&& gen, size_t ops = 10'000) {
    for (size_t i = 0; i < ops; ++i) {
        size_t idx = gen();
        auto task = GDSFBenchRepo::find(ids[idx]);
        if (task.await_ready()) {
            doNotOptimize(task.await_resume());
        }
    }

    // Rebuild histogram to reflect post-warmup scores.
    // Temporarily disable budget so sweeps only record, not evict.
    // Then re-anchor budget to current RSS + original cache headroom.
    auto saved_budget = GDSFPolicy::instance().maxMemory();
    GDSFPolicy::instance().configure({.max_memory = SIZE_MAX});
    for (int i = 0; i < 16; ++i) GDSFPolicy::instance().sweep();
    GDSFPolicy::instance().configure({.max_memory = saved_budget});
}

/// Run multiple sweep rounds to converge on eviction threshold.
void sweepRounds(int rounds = 8) {
    for (int i = 0; i < rounds; ++i)
        GDSFPolicy::instance().sweep();
}

/// Running statistics for heap sampling during workload.
struct HeapStats {
    uint64_t min_kb = UINT64_MAX;
    uint64_t max_kb = 0;
    double sum_kb = 0.0;
    double sum_sq_kb = 0.0;
    size_t count = 0;

    void sample() {
        auto kb = CachedHeap::bytes() / 1024;
        if (kb < min_kb) min_kb = kb;
        if (kb > max_kb) max_kb = kb;
        sum_kb += static_cast<double>(kb);
        sum_sq_kb += static_cast<double>(kb) * static_cast<double>(kb);
        ++count;
    }

    double avg() const { return count > 0 ? sum_kb / static_cast<double>(count) : 0.0; }
    double stddev() const {
        if (count < 2) return 0.0;
        double n = static_cast<double>(count);
        double mean = sum_kb / n;
        return std::sqrt((sum_sq_kb / n) - mean * mean);
    }
};

struct AccessStats {
    int64_t hits = 0;
    int64_t misses = 0;
    int64_t cache_size = 0;
    Clock::duration elapsed{};
    HeapStats heap{};

    double hitRate() const {
        auto total = hits + misses;
        return total > 0 ? 100.0 * hits / total : 0.0;
    }

    double opsPerSec() const {
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        auto total = hits + misses;
        return us > 0 ? total * 1'000'000.0 / us : 0.0;
    }
};

/// Fixed-ops steady-state workload: misses fetch from DB and re-admit into L1,
/// triggering GDSF sweeps. Measures the dynamic equilibrium hit rate.
/// Samples heap every 1024 ops for min/max/avg/stddev statistics.
template<typename KeyGen>
AccessStats runWorkloadFixed(const std::vector<int64_t>& ids, KeyGen&& gen,
                             size_t num_ops) {
    AccessStats stats;

    GDSFBenchRepo::resetMetrics();
    auto start = Clock::now();

    for (size_t i = 0; i < num_ops; ++i) {
        size_t idx = gen();
        doNotOptimize(sync(GDSFBenchRepo::find(ids[idx])));
        if ((i & 1023) == 0) stats.heap.sample();
    }
    stats.heap.sample();  // final sample

    auto m = GDSFBenchRepo::metrics();
    stats.hits = static_cast<int64_t>(m.l1_hits);
    stats.misses = static_cast<int64_t>(m.l1_misses);
    stats.cache_size = static_cast<int64_t>(GDSFBenchRepo::size());
    stats.elapsed = Clock::now() - start;
    return stats;
}

/// Run N trials of a hit-rate scenario, report mean ± stddev.
/// TrialSetup: void(uint64_t seed) — called before each trial to reset state.
/// TrialRun: AccessStats(uint64_t seed) — runs the workload with a given seed.
template<typename TrialSetup, typename TrialRun>
std::string runTrials(const std::string& label, int n_trials,
                      TrialSetup&& setup, TrialRun&& run) {
    std::vector<double> hit_rates;
    hit_rates.reserve(n_trials);

    for (int t = 0; t < n_trials; ++t) {
        uint64_t seed = static_cast<uint64_t>(t * 997 + 31);
        setup(seed);
        auto stats = run(seed);
        hit_rates.push_back(stats.hitRate());
    }

    double mean = std::accumulate(hit_rates.begin(), hit_rates.end(), 0.0)
                / static_cast<double>(n_trials);
    double sq_sum = 0.0;
    for (double r : hit_rates) sq_sum += (r - mean) * (r - mean);
    double stddev = n_trials > 1
        ? std::sqrt(sq_sum / static_cast<double>(n_trials - 1)) : 0.0;

    auto bar = std::string(55, '-');
    std::ostringstream out;
    out << "\n  " << bar
        << "\n  " << label
        << "\n  " << bar
        << "\n  trials:       " << n_trials
        << "\n  hit rate:     " << std::fixed << std::setprecision(1)
        << mean << "% ± " << std::setprecision(2) << stddev << "%"
        << "\n  per trial:    ";
    for (size_t i = 0; i < hit_rates.size(); ++i) {
        if (i > 0) out << ", ";
        out << std::fixed << std::setprecision(1) << hit_rates[i] << "%";
    }
    out << "\n  " << bar;
    return out.str();
}

std::string formatAccessStats(const std::string& label, const AccessStats& s) {
    auto bar = std::string(55, '-');
    std::ostringstream out;
    out << "\n  " << bar
        << "\n  " << label
        << "\n  " << bar
        << "\n  total ops:    " << (s.hits + s.misses)
        << "\n  L1 hits:      " << s.hits
        << "\n  L1 misses:    " << s.misses
        << "\n  hit rate:     " << std::fixed << std::setprecision(1) << s.hitRate() << "%"
        << "\n  cache size:   " << s.cache_size << " entries"
        << "\n  heap:         " << "min=" << s.heap.min_kb
            << " avg=" << std::fixed << std::setprecision(0) << s.heap.avg()
            << " max=" << s.heap.max_kb
            << " sd=" << std::setprecision(0) << s.heap.stddev() << " KB"
        << "\n  max_memory:   " << (GDSFPolicy::instance().maxMemory() / 1024) << " KB"
        << "\n  throughput:   " << fmtOps(s.opsPerSec())
#if RELAIS_ENABLE_METRICS
        << formatSweepMetrics()
#endif
        << "\n  " << bar;
    return out.str();
}

} // anonymous namespace


// #############################################################################
//
//  GDSF matrix benchmark: 3 skews × 3 pressures × 2 size profiles = 18 combos
//
// #############################################################################

TEST_CASE("Benchmark - GDSF matrix", "[benchmark][gdsf]")
{
    TransactionGuard tx;

    static constexpr size_t NUM_KEYS = 10'000;
    static constexpr size_t NUM_OPS = 1'00'000;

    auto skew = GENERATE(0.8, 1.0, 1.2);
    auto pressure = GENERATE(0.90, 0.50, 0.20);
    auto varied = GENERATE(false, true);

    // 1. Insert all items, measure actual RSS footprint, apply pressure.
    //    pressure=0.9 → allow 90% of cache, pressure=0.2 → allow 20%.
    auto [ids] = setupGDSFBench(NUM_KEYS, pressure, varied);

    // 2. Warmup access counts with target distribution
    ZipfGenerator warmup_zipf(NUM_KEYS, skew, 42);
    warmupAccess(ids, [&]() { return warmup_zipf.next(); });

    // 3. Sweep to converge on eviction threshold
    sweepRounds();

    // 4. Measure hit rate (100K fixed ops)
    ZipfGenerator zipf(NUM_KEYS, skew, 123);
    auto stats = runWorkloadFixed(ids, [&]() { return zipf.next(); }, NUM_OPS);

    // 6. Report
    std::ostringstream lbl;
    lbl << "s=" << std::fixed << std::setprecision(1) << skew
        << " p=" << std::setprecision(0) << (pressure * 100) << "%"
        << (varied ? " varied" : " uniform");
    WARN(formatAccessStats(lbl.str(), stats));
}


