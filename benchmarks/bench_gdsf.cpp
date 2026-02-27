/**
 * bench_gdsf.cpp
 *
 * Parametric matrix benchmark for GDSF eviction policy.
 * Compiled with RELAIS_GDSF_ENABLED=1 to enable size-aware eviction.
 *
 * Matrix: 3 skews × 3 pressures × 2 size profiles = 18 combinations
 *   - Skew:     s=0.8 (mild), s=1.0 (standard Zipf), s=1.2 (heavy)
 *   - Pressure: 90% (low eviction), 50% (medium), 20% (high eviction)
 *   - Sizes:    uniform (~200B each) or varied (alternating ~200B / ~450B)
 *
 * Budget is computed dynamically from totalMemory() AFTER insertion,
 * so ParlayHash bucket array overhead is automatically included.
 *
 * Design:
 *   1. Insert N items into DB + L1 (all cached, access_count=1)
 *   2. Compute budget = totalMemory() × pressure_ratio
 *   3. Warm up: run target distribution to build access counts (all L1 hits)
 *   4. Evict: sweep until memory <= budget (GDSF retains hot items)
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

using namespace relais_test;
using namespace relais_bench;
using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;

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

using GDSFBenchRepo = Repo<TestItemWrapper, "bench:gdsf:zipf", gdsf_bench::NoTTL>;

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
    size_t entry_memory;   // delta: memory from entries only (no structural overhead)
};

SetupResult setupGDSFBench(size_t n_items, size_t max_memory_bytes,
                            bool varied_sizes = false) {
    // Reset everything (drain epoch pool before zeroing memory counters)
    TestInternals::resetCacheForGDSF<GDSFBenchRepo>();

    // Disable budget during insertion to prevent periodic sweeps from
    // nuking chunks with an empty histogram.
    GDSFPolicy::instance().configure({.max_memory = SIZE_MAX});

    // Snapshot memory BEFORE insertions (captures structural overhead baseline)
    auto mem_before = GDSFPolicy::instance().totalMemory();

    std::string long_desc(200, 'x');

    std::vector<int64_t> ids;
    ids.reserve(n_items);
    for (size_t i = 0; i < n_items; ++i) {
        std::optional<std::string> desc;
        if (varied_sizes && (i % 2 == 1))
            desc = long_desc + "_" + std::to_string(i);
        auto kid = insertTestItem("gdsf_bench_" + std::to_string(i),
                                   static_cast<int32_t>(i), desc);
        sync(GDSFBenchRepo::find(kid));
        ids.push_back(kid);
    }

    // Entry-only memory (excludes bucket array / structural overhead)
    auto entry_memory = GDSFPolicy::instance().totalMemory() - mem_before;

    // Pre-warm histogram: 16 sweeps (2 full rounds of 8 chunks) with no
    // budget pressure. This populates the persistent EMA histogram so that
    // eviction uses real score distributions, not exp2(23.25).
    for (int i = 0; i < 16; ++i) GDSFPolicy::instance().sweep();

    // Now set the real budget for eviction.
    GDSFPolicy::instance().configure({.max_memory = max_memory_bytes});

    return {std::move(ids), entry_memory};
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
    auto saved = GDSFPolicy::instance().maxMemory();
    GDSFPolicy::instance().configure({.max_memory = SIZE_MAX});
    for (int i = 0; i < 16; ++i) GDSFPolicy::instance().sweep();
    GDSFPolicy::instance().configure({.max_memory = saved});
}

/// Sweep until memory is within budget (or no progress / max iterations).
/// Stops early when sweep didn't evict anything (epoch-deferred memory lag).
int evictToBudget(int max_rounds = 200) {
    int rounds = 0;
    int stalls = 0;
    while (rounds < max_rounds && GDSFPolicy::instance().isOverBudget()) {
        auto before = GDSFBenchRepo::size();
        GDSFPolicy::instance().sweep();
        auto after = GDSFBenchRepo::size();
        if (before == after) {
            if (++stalls >= 3) break;  // 3 stalls → epoch lag, give up
        } else {
            stalls = 0;
        }
        ++rounds;
    }
    return rounds;
}

struct AccessStats {
    int64_t hits = 0;
    int64_t misses = 0;
    int64_t cache_size = 0;
    Clock::duration elapsed{};

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
template<typename KeyGen>
AccessStats runWorkloadFixed(const std::vector<int64_t>& ids, KeyGen&& gen,
                             size_t num_ops) {
    AccessStats stats;

    GDSFBenchRepo::resetMetrics();
    auto start = Clock::now();

    for (size_t i = 0; i < num_ops; ++i) {
        size_t idx = gen();
        doNotOptimize(sync(GDSFBenchRepo::find(ids[idx])));
    }

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
        << "\n  L1 memory:    " << GDSFPolicy::instance().totalMemory() << " B"
        << "\n  throughput:   " << fmtOps(s.opsPerSec())
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

    static constexpr size_t NUM_KEYS = 1000;
    static constexpr size_t NUM_OPS = 100'000;

    auto skew = GENERATE(0.8, 1.0, 1.2);
    auto pressure = GENERATE(0.90, 0.50, 0.20);
    auto varied = GENERATE(false, true);

    // 1. Insert all items (no budget limit)
    auto [ids, entry_memory] = setupGDSFBench(NUM_KEYS, SIZE_MAX, varied);

    // 2. Compute budget: evict (1 - pressure) fraction of entry memory.
    //    Formulated as total − bytes_to_evict (safe unsigned arithmetic)
    //    rather than structural + entries×pressure (vulnerable to epoch drift).
    auto total = GDSFPolicy::instance().totalMemory();
    auto bytes_to_evict = static_cast<size_t>(
        static_cast<double>(entry_memory) * (1.0 - pressure));
    auto budget = (total > bytes_to_evict) ? (total - bytes_to_evict) : size_t{0};
    GDSFPolicy::instance().configure({.max_memory = budget});

    // 3. Warmup access counts with target distribution
    ZipfGenerator warmup_zipf(NUM_KEYS, skew, 42);
    warmupAccess(ids, [&]() { return warmup_zipf.next(); });

    // 4. Evict to budget
    evictToBudget();

    // 5. Measure hit rate (100K fixed ops)
    ZipfGenerator zipf(NUM_KEYS, skew, 123);
    auto stats = runWorkloadFixed(ids, [&]() { return zipf.next(); }, NUM_OPS);

    // 6. Report
    std::ostringstream lbl;
    lbl << "s=" << std::fixed << std::setprecision(1) << skew
        << " p=" << std::setprecision(0) << (pressure * 100) << "%"
        << (varied ? " varied" : " uniform");
    WARN(formatAccessStats(lbl.str(), stats));
}


