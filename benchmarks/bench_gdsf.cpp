/**
 * bench_gdsf.cpp
 *
 * Zipfian access pattern benchmark for GDSF eviction policy.
 * Compiled with RELAIS_GDSF_ENABLED=1 to enable size-aware eviction.
 *
 * Measures L1 cache hit rate and throughput under:
 *   - Zipfian distribution (skew s=0.8, 1.0, 1.2)
 *   - Uniform distribution (baseline)
 *   - Different working set sizes vs memory budget ratios
 *
 * Design:
 *   1. Insert N items into DB + L1 (all cached, access_count=1)
 *   2. Warm up: run target distribution to build access counts (all L1 hits)
 *   3. Evict: sweep until memory <= budget (GDSF retains hot items)
 *   4. Measure: L1-only lookups, count hits vs misses (no sync from workers)
 *
 * Run with:
 *   ./bench_gdsf                      # all benchmarks
 *   ./bench_gdsf "[zipfian]"          # zipfian only
 *   BENCH_DURATION_S=10 ./bench_gdsf  # longer runs
 */

#include <catch2/catch_test_macros.hpp>

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
std::vector<int64_t> setupGDSFBench(size_t n_items, size_t max_memory_bytes) {
    // Reset everything
    TestInternals::resetEntityCacheState<GDSFBenchRepo>();
    TestInternals::resetRepoGDSFState<GDSFBenchRepo>();
    TestInternals::resetGDSF();

    // Disable budget during insertion to prevent periodic sweeps from
    // nuking chunks with an empty histogram.
    GDSFPolicy::instance().configure({.max_memory = SIZE_MAX});

    std::vector<int64_t> ids;
    ids.reserve(n_items);
    for (size_t i = 0; i < n_items; ++i) {
        auto kid = insertTestItem("gdsf_bench_" + std::to_string(i),
                                   static_cast<int32_t>(i));
        sync(GDSFBenchRepo::find(kid));
        ids.push_back(kid);
    }

    // Pre-warm histogram: 16 sweeps (2 full rounds of 8 chunks) with no
    // budget pressure. This populates the persistent EMA histogram so that
    // eviction uses real score distributions, not exp2(23.25).
    for (int i = 0; i < 16; ++i) GDSFPolicy::instance().sweep();

    // Now set the real budget for eviction.
    GDSFPolicy::instance().configure({.max_memory = max_memory_bytes});

    return ids;
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

/// L1-only workload: count hits and misses without fetching misses from DB.
/// No sync() calls — safe from worker threads.
template<typename KeyGen>
AccessStats runWorkload(const std::vector<int64_t>& ids, KeyGen&& gen) {
    AccessStats stats;

    auto result = measureDuration(1, [&](int, std::atomic<bool>& r) -> int64_t {
        int64_t local_hits = 0, local_misses = 0;
        while (r.load(std::memory_order_relaxed)) {
            size_t idx = gen();
            auto task = GDSFBenchRepo::find(ids[idx]);
            if (task.await_ready()) {
                doNotOptimize(task.await_resume());
                ++local_hits;
            } else {
                ++local_misses;
            }
        }
        stats.hits = local_hits;
        stats.misses = local_misses;
        return local_hits + local_misses;
    });

    stats.cache_size = static_cast<int64_t>(GDSFBenchRepo::size());
    stats.elapsed = result.elapsed;
    return stats;
}

/// Fixed-ops workload: deterministic hit rate measurement.
/// Runs exactly num_ops operations, eliminating timing noise.
template<typename KeyGen>
AccessStats runWorkloadFixed(const std::vector<int64_t>& ids, KeyGen&& gen,
                             size_t num_ops) {
    AccessStats stats;
    auto start = Clock::now();

    for (size_t i = 0; i < num_ops; ++i) {
        size_t idx = gen();
        auto task = GDSFBenchRepo::find(ids[idx]);
        if (task.await_ready()) {
            doNotOptimize(task.await_resume());
            ++stats.hits;
        } else {
            ++stats.misses;
        }
    }

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
//  Zipfian GDSF benchmark
//
// #############################################################################

TEST_CASE("Benchmark - GDSF zipfian hit rate", "[benchmark][gdsf][zipfian]")
{
    TransactionGuard tx;

    // Working set: 1000 items, memory budget holds ~500 (50% pressure)
    // Each TestItem is ~200-300 bytes in CachedWrapper. Budget = 150 KB.
    static constexpr size_t NUM_KEYS = 1000;
    static constexpr size_t BUDGET = 150'000;

    auto ids = setupGDSFBench(NUM_KEYS, BUDGET);

    SECTION("Uniform baseline (no skew)") {
        std::mt19937_64 rng(42);
        std::uniform_int_distribution<size_t> dist(0, NUM_KEYS - 1);
        warmupAccess(ids, [&]() { return dist(rng); });
        evictToBudget();

        std::mt19937_64 rng2(123);
        std::uniform_int_distribution<size_t> dist2(0, NUM_KEYS - 1);
        auto stats = runWorkload(ids, [&]() { return dist2(rng2); });
        WARN(formatAccessStats(
            "Uniform (1000 keys, 150KB budget)", stats));
    }

    SECTION("Zipfian s=0.8 (mild skew)") {
        ZipfGenerator warmup_zipf(NUM_KEYS, 0.8, 42);
        warmupAccess(ids, [&]() { return warmup_zipf.next(); });
        evictToBudget();

        ZipfGenerator zipf(NUM_KEYS, 0.8, 123);
        auto stats = runWorkload(ids, [&]() { return zipf.next(); });
        WARN(formatAccessStats(
            "Zipfian s=0.8 (1000 keys, 150KB budget)", stats));
    }

    SECTION("Zipfian s=1.0 (standard Zipf)") {
        ZipfGenerator warmup_zipf(NUM_KEYS, 1.0, 42);
        warmupAccess(ids, [&]() { return warmup_zipf.next(); });
        evictToBudget();

        ZipfGenerator zipf(NUM_KEYS, 1.0, 123);
        auto stats = runWorkload(ids, [&]() { return zipf.next(); });
        WARN(formatAccessStats(
            "Zipfian s=1.0 (1000 keys, 150KB budget)", stats));
    }

    SECTION("Zipfian s=1.2 (heavy skew)") {
        ZipfGenerator warmup_zipf(NUM_KEYS, 1.2, 42);
        warmupAccess(ids, [&]() { return warmup_zipf.next(); });
        evictToBudget();

        ZipfGenerator zipf(NUM_KEYS, 1.2, 123);
        auto stats = runWorkload(ids, [&]() { return zipf.next(); });
        WARN(formatAccessStats(
            "Zipfian s=1.2 (1000 keys, 150KB budget)", stats));
    }
}


TEST_CASE("Benchmark - GDSF memory pressure levels", "[benchmark][gdsf][pressure]")
{
    TransactionGuard tx;

    static constexpr size_t NUM_KEYS = 500;

    SECTION("Low pressure (budget >> working set)") {
        auto ids = setupGDSFBench(NUM_KEYS, 10'000'000); // 10 MB, way more than needed
        ZipfGenerator warmup_zipf(NUM_KEYS, 1.0, 42);
        warmupAccess(ids, [&]() { return warmup_zipf.next(); });
        evictToBudget();

        ZipfGenerator zipf(NUM_KEYS, 1.0, 123);
        auto stats = runWorkload(ids, [&]() { return zipf.next(); });
        WARN(formatAccessStats(
            "Zipfian s=1.0, low pressure (10MB budget)", stats));
    }

    SECTION("Medium pressure (budget ~ working set)") {
        auto ids = setupGDSFBench(NUM_KEYS, 100'000);
        ZipfGenerator warmup_zipf(NUM_KEYS, 1.0, 42);
        warmupAccess(ids, [&]() { return warmup_zipf.next(); });
        evictToBudget();

        ZipfGenerator zipf(NUM_KEYS, 1.0, 123);
        auto stats = runWorkload(ids, [&]() { return zipf.next(); });
        WARN(formatAccessStats(
            "Zipfian s=1.0, medium pressure (100KB budget)", stats));
    }

    SECTION("High pressure (budget << working set)") {
        auto ids = setupGDSFBench(NUM_KEYS, 30'000);
        ZipfGenerator warmup_zipf(NUM_KEYS, 1.0, 42);
        warmupAccess(ids, [&]() { return warmup_zipf.next(); });
        evictToBudget();

        ZipfGenerator zipf(NUM_KEYS, 1.0, 123);
        auto stats = runWorkload(ids, [&]() { return zipf.next(); });
        WARN(formatAccessStats(
            "Zipfian s=1.0, high pressure (30KB budget)", stats));
    }
}


TEST_CASE("Benchmark - GDSF multi-thread zipfian", "[benchmark][gdsf][zipfian][throughput]")
{
    TransactionGuard tx;

    static constexpr size_t NUM_KEYS = 2000;
    static constexpr size_t BUDGET = 200'000;
    static constexpr int THREADS = 4;

    auto ids = setupGDSFBench(NUM_KEYS, BUDGET);

    // Warm up with zipfian from main thread before eviction
    ZipfGenerator warmup_zipf(NUM_KEYS, 1.0, 42);
    warmupAccess(ids, [&]() { return warmup_zipf.next(); }, 20'000);
    evictToBudget();

    SECTION("Multi-thread zipfian s=1.0") {
        std::vector<int64_t> thread_hits(THREADS, 0);
        std::vector<int64_t> thread_misses(THREADS, 0);

        auto result = measureDuration(THREADS,
            [&](int tid, std::atomic<bool>& running) -> int64_t {
                ZipfGenerator zipf(NUM_KEYS, 1.0, tid * 42 + 7);
                int64_t hits = 0, misses = 0;
                while (running.load(std::memory_order_relaxed)) {
                    size_t idx = zipf.next();
                    auto task = GDSFBenchRepo::find(ids[idx]);
                    if (task.await_ready()) {
                        doNotOptimize(task.await_resume());
                        ++hits;
                    } else {
                        ++misses;
                    }
                }
                thread_hits[tid] = hits;
                thread_misses[tid] = misses;
                return hits + misses;
            });

        int64_t total_hits = 0, total_misses = 0;
        for (int i = 0; i < THREADS; ++i) {
            total_hits += thread_hits[i];
            total_misses += thread_misses[i];
        }

        double hit_rate = (total_hits + total_misses > 0)
            ? 100.0 * total_hits / (total_hits + total_misses) : 0.0;

        auto msg = formatDurationThroughput(
            "GDSF zipfian s=1.0 (4 threads, 2000 keys, 200KB budget)",
            THREADS, result);
        std::ostringstream extra;
        extra << "\n  L1 hit rate:  " << std::fixed << std::setprecision(1)
              << hit_rate << "% (" << total_hits << " hits, "
              << total_misses << " misses)"
              << "\n  cache size:   " << GDSFBenchRepo::size() << " entries"
              << "\n  L1 memory:    " << GDSFPolicy::instance().totalMemory() << " B";
        WARN(msg + extra.str());
    }
}


