/**
 * test_benchmark.cpp
 *
 * Performance benchmarks for the relais cache hierarchy.
 * Uses a custom micro-benchmarker for consistent, human-readable output
 * regardless of Catch2 reporter (console or XML).
 *
 * Run with:
 *   ./test_relais_benchmark                    # all benchmarks
 *   ./test_relais_benchmark "[l1]"             # L1 only
 *   ./test_relais_benchmark "[throughput]"      # multi-threaded only
 *   BENCH_SAMPLES=500 ./test_relais_benchmark  # 500 samples per benchmark
 *
 * Covers:
 *   1. L1 cache hit latency (findById, findByIdAsJson)
 *   2. L2 cache hit latency
 *   3. L1+L2 cache hit latency (L1 serves, L2 fallback)
 *   4. Cache miss latency (DB fetch)
 *   5. Write operations (create+remove, update)
 *   6. List query latency (cached)
 *   7. Multi-threaded throughput (L1, L1+L2, mixed)
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"

#include <thread>
#include <vector>
#include <latch>
#include <random>
#include <sstream>
#include <iomanip>
#include <numeric>
#include <algorithm>
#include <cmath>
#include <fstream>
#include <sched.h>

using namespace relais_test;

// =============================================================================
// Benchmark environment setup (runs before main via static init)
// =============================================================================
//
// BENCH_PIN_CPU=N  — pin main thread to core N (default: no pinning)
//                    Use for single-thread latency tests: BENCH_PIN_CPU=2 ./bench "[l1]"
//                    Omit for multi-threaded throughput tests.
//
// Automatically checks CPU governor and warns if not "performance".
//

static const bool bench_env_ready = [] {
    // 1. Optional CPU pinning
    if (auto* env = std::getenv("BENCH_PIN_CPU")) {
        int core = std::atoi(env);
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(core, &mask);
        if (sched_setaffinity(0, sizeof(mask), &mask) == 0) {
            std::fprintf(stderr, "  [bench] pinned to CPU %d\n", core);
        } else {
            std::fprintf(stderr, "  [bench] WARNING: failed to pin to CPU %d\n", core);
        }
    }

    // 2. Check CPU governor
    int cpu = 0;
    if (auto* env = std::getenv("BENCH_PIN_CPU")) cpu = std::atoi(env);
    std::string path = "/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/cpufreq/scaling_governor";
    if (std::ifstream gov(path); gov.is_open()) {
        std::string g;
        std::getline(gov, g);
        if (g == "performance") {
            std::fprintf(stderr, "  [bench] CPU governor: performance\n");
        } else {
            std::fprintf(stderr,
                "  [bench] WARNING: CPU governor is '%s', not 'performance'\n"
                "          Run: sudo cpupower frequency-set -g performance\n", g.c_str());
        }
    }

    // 3. Check turbo boost (Intel + AMD)
    for (auto* turbo_path : {
        "/sys/devices/system/cpu/intel_pstate/no_turbo",
        "/sys/devices/system/cpu/cpufreq/boost"
    }) {
        if (std::ifstream f(turbo_path); f.is_open()) {
            int val = 0;
            f >> val;
            // Intel: no_turbo=0 means turbo ON. AMD: boost=1 means turbo ON.
            bool turbo_on = (std::string(turbo_path).find("no_turbo") != std::string::npos)
                            ? (val == 0) : (val == 1);
            if (turbo_on) {
                std::fprintf(stderr,
                    "  [bench] WARNING: turbo boost is ON (frequency varies with temperature)\n"
                    "          Disable: echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo\n"
                    "              or: echo 0 | sudo tee /sys/devices/system/cpu/cpufreq/boost\n");
            } else {
                std::fprintf(stderr, "  [bench] turbo boost: disabled\n");
            }
            break;
        }
    }

    return true;
}();

template<typename T>
static void doNotOptimize(const T& val) {
    asm volatile("" : : "r,m"(val) : "memory");
}

// =============================================================================
// Micro-benchmark engine
// =============================================================================

static constexpr int WARMUP = 50;

/// Number of samples per benchmark. Configurable via BENCH_SAMPLES env var.
static int benchSamples() {
    static int n = [] {
        if (auto* env = std::getenv("BENCH_SAMPLES"))
            if (int v = std::atoi(env); v > 0) return v;
        return 500;
    }();
    return n;
}

using Clock = std::chrono::steady_clock;

struct BenchResult {
    std::string name;
    double median_us;
    double p99_us;
    double mean_us;
    double min_us;
    double max_us;
};

/// Compute stats from a vector of timings (in microseconds).
static BenchResult computeStats(const std::string& name, std::vector<double>& times) {
    std::sort(times.begin(), times.end());
    auto n = static_cast<int>(times.size());
    double median = times[n / 2];
    double p99 = times[static_cast<int>(n * 0.99)];
    double mean = std::accumulate(times.begin(), times.end(), 0.0) / n;
    return {name, median, p99, mean, times.front(), times.back()};
}

/// Async benchmark: runs fn() inside a single coroutine using co_await.
/// This measures real production performance — no sync_wait() overhead per iteration.
template<typename Fn>
static drogon::Task<BenchResult> benchAsync(const std::string& name, Fn&& fn) {
    for (int i = 0; i < WARMUP; ++i) co_await fn();

    const int samples = benchSamples();
    std::vector<double> times(samples);
    for (int i = 0; i < samples; ++i) {
        auto t0 = Clock::now();
        co_await fn();
        times[i] = std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
    }

    co_return computeStats(name, times);
}

/// Async benchmark with per-iteration setup. setup() is NOT measured.
template<typename SetupFn, typename Fn>
static drogon::Task<BenchResult> benchWithSetupAsync(
        const std::string& name, SetupFn&& setup, Fn&& fn) {
    for (int i = 0; i < WARMUP; ++i) { co_await setup(); co_await fn(); }

    const int samples = benchSamples();
    std::vector<double> times(samples);
    for (int i = 0; i < samples; ++i) {
        co_await setup();
        auto t0 = Clock::now();
        co_await fn();
        times[i] = std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
    }

    co_return computeStats(name, times);
}


// =============================================================================
// Formatting utilities
// =============================================================================

static std::string fmtDuration(double us) {
    std::ostringstream out;
    out << std::fixed;
    if (us < 1.0)            out << std::setprecision(0) << us * 1000 << " ns";
    else if (us < 1'000)     out << std::setprecision(1) << us << " us";
    else if (us < 1'000'000) out << std::setprecision(2) << us / 1'000 << " ms";
    else                     out << std::setprecision(2) << us / 1'000'000 << " s";
    return out.str();
}

static std::string fmtOps(double ops) {
    std::ostringstream out;
    out << std::fixed;
    if (ops >= 1'000'000)  out << std::setprecision(1) << ops / 1'000'000 << "M ops/s";
    else if (ops >= 1'000) out << std::setprecision(1) << ops / 1'000 << "K ops/s";
    else                   out << std::setprecision(0) << ops << " ops/s";
    return out.str();
}

/// Format a group of benchmark results as an aligned table.
static std::string formatTable(const std::string& title,
                               const std::vector<BenchResult>& results) {
    size_t max_name = 0;
    for (const auto& r : results)
        max_name = std::max(max_name, r.name.size());
    max_name += 2;

    std::ostringstream out;
    auto w = static_cast<int>(max_name + 55);
    auto bar = std::string(w, '-');

    auto samples = benchSamples();
    out << "\n  " << bar << "\n"
        << "  " << title;
    auto pad = w - static_cast<int>(title.size())
                 - static_cast<int>(std::to_string(samples).size()) - 11;
    if (pad > 0) out << std::string(pad, ' ');
    out << "(" << samples << " samples)\n"
        << "  " << bar << "\n"
        << "  " << std::left << std::setw(static_cast<int>(max_name + 1)) << ""
        << std::right
        << std::setw(10) << "median"
        << std::setw(10) << "min"
        << std::setw(12) << "p99"
        << std::setw(10) << "max" << "\n"
        << "  " << bar << "\n";

    for (const auto& r : results) {
        out << "   " << std::left << std::setw(static_cast<int>(max_name)) << r.name
            << std::right
            << std::setw(10) << fmtDuration(r.median_us)
            << std::setw(10) << fmtDuration(r.min_us)
            << std::setw(12) << fmtDuration(r.p99_us)
            << std::setw(10) << fmtDuration(r.max_us)
            << "\n";
    }

    out << "  " << bar;
    return out.str();
}

/// Run N threads x ops, synchronized with latches. Returns wall time (work only).
/// Each thread is pinned to a separate CPU core for true parallelism.
template<typename Fn>
static auto measureParallel(int num_threads, int ops_per_thread, Fn&& fn) {
    std::latch ready{num_threads};  // threads signal readiness
    std::latch go{1};               // main releases all threads
    std::vector<std::jthread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            // Pin each thread to its own core
            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(i % std::thread::hardware_concurrency(), &mask);
            sched_setaffinity(0, sizeof(mask), &mask);

            ready.count_down();  // signal ready
            go.wait();           // wait for go
            fn(i, ops_per_thread);
        });
    }

    ready.wait();               // wait for all threads pinned and ready
    auto t0 = Clock::now();
    go.count_down();            // release all threads simultaneously
    for (auto& t : threads) t.join();
    return Clock::now() - t0;
}

/// Format a multi-threaded throughput measurement.
static std::string formatThroughput(
        const std::string& label, int threads, int ops_per_thread,
        std::chrono::steady_clock::duration elapsed) {
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    auto total_ops = threads * ops_per_thread;
    auto ops_per_sec = (us > 0) ? total_ops * 1'000'000.0 / us : 0.0;
    auto avg_us = (total_ops > 0) ? static_cast<double>(us) / total_ops : 0.0;

    auto bar = std::string(50, '-');
    std::ostringstream out;
    out << "\n"
        << "  " << bar << "\n"
        << "  " << label << "\n"
        << "  " << bar << "\n"
        << "  threads:      " << threads << "\n"
        << "  ops/thread:   " << ops_per_thread << "\n"
        << "  total ops:    " << total_ops << "\n"
        << "  wall time:    " << fmtDuration(static_cast<double>(us)) << "\n"
        << "  throughput:   " << fmtOps(ops_per_sec) << "\n"
        << "  avg latency:  " << fmtDuration(avg_us) << "\n"
        << "  " << bar;
    return out.str();
}


// #############################################################################
//
//  1. L1 cache hit latency
//
// #############################################################################

TEST_CASE("Benchmark - L1 cache hit", "[benchmark][l1]")
{
    TransactionGuard tx;
    auto id = insertTestItem("bench_l1", 42);
    sync(L1TestItemRepository::findById(id));

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("findById", [&]() -> drogon::Task<void> {
        co_await L1TestItemRepository::findById(id);
    })));

    results.push_back(sync(benchAsync("findByIdAsJson", [&]() -> drogon::Task<void> {
        co_await L1TestItemRepository::findByIdAsJson(id);
    })));

    WARN(formatTable("L1 cache hit", results));
}


// #############################################################################
//
//  2. L2 cache hit latency
//
// #############################################################################

TEST_CASE("Benchmark - L2 cache hit", "[benchmark][l2]")
{
    TransactionGuard tx;
    auto id = insertTestItem("bench_l2", 42);
    sync(L2TestItemRepository::findById(id));

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("findById", [&]() -> drogon::Task<void> {
        co_await L2TestItemRepository::findById(id);
    })));

    results.push_back(sync(benchAsync("findByIdAsJson", [&]() -> drogon::Task<void> {
        co_await L2TestItemRepository::findByIdAsJson(id);
    })));

    WARN(formatTable("L2 cache hit (Redis)", results));
}


// #############################################################################
//
//  3. L1+L2 cache hit latency
//
// #############################################################################

TEST_CASE("Benchmark - L1+L2 cache hit", "[benchmark][full-cache]")
{
    TransactionGuard tx;
    auto id = insertTestItem("bench_both", 42);
    sync(FullCacheTestItemRepository::findById(id));

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("findById (L1 serves)", [&]() -> drogon::Task<void> {
        co_await FullCacheTestItemRepository::findById(id);
    })));

    results.push_back(sync(benchWithSetupAsync("findById (L2 fallback)",
        [&]() -> drogon::Task<void> { FullCacheTestItemRepository::invalidateL1(id); co_return; },
        [&]() -> drogon::Task<void> { co_await FullCacheTestItemRepository::findById(id); }
    )));

    WARN(formatTable("L1+L2 cache hit", results));
}


// #############################################################################
//
//  4. Cache miss latency (DB fetch)
//
// #############################################################################

TEST_CASE("Benchmark - cache miss (DB fetch)", "[benchmark][db]")
{
    TransactionGuard tx;
    auto id = insertTestItem("bench_miss", 42);

    std::vector<BenchResult> results;

    results.push_back(sync(benchWithSetupAsync("findById (L1 miss -> DB)",
        [&]() -> drogon::Task<void> { L1TestItemRepository::invalidateL1(id); co_return; },
        [&]() -> drogon::Task<void> { co_await L1TestItemRepository::findById(id); }
    )));

    results.push_back(sync(benchWithSetupAsync("findById (L1+L2 miss -> DB)",
        [&]() -> drogon::Task<void> { co_await FullCacheTestItemRepository::invalidate(id); },
        [&]() -> drogon::Task<void> { co_await FullCacheTestItemRepository::findById(id); }
    )));

    WARN(formatTable("Cache miss (DB fetch)", results));
}


// #############################################################################
//
//  5. Write operations
//
// #############################################################################

TEST_CASE("Benchmark - write operations", "[benchmark][write]")
{
    TransactionGuard tx;

    auto upd_id = insertTestItem("bench_upd", 42);
    sync(L1TestItemRepository::findById(upd_id));
    int c1 = 0;

    auto upd_both_id = insertTestItem("bench_upd_both", 42);
    sync(FullCacheTestItemRepository::findById(upd_both_id));
    int c2 = 0;

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("create + remove (L1)", [&]() -> drogon::Task<void> {
        auto entity = makeTestItem("bench_cr", 42);
        auto created = co_await L1TestItemRepository::create(entity);
        if (created) co_await L1TestItemRepository::remove(created->id);
    })));

    results.push_back(sync(benchAsync("update (L1)", [&]() -> drogon::Task<void> {
        ++c1;
        auto entity = makeTestItem(
            "bench_u_" + std::to_string(c1), c1,
            std::nullopt, true, upd_id);
        co_await L1TestItemRepository::update(upd_id, entity);
    })));

    results.push_back(sync(benchAsync("update (L1+L2)", [&]() -> drogon::Task<void> {
        ++c2;
        auto entity = makeTestItem(
            "bench_ub_" + std::to_string(c2), c2,
            std::nullopt, true, upd_both_id);
        co_await FullCacheTestItemRepository::update(upd_both_id, entity);
    })));

    WARN(formatTable("Write operations", results));
}


// #############################################################################
//
//  6. List query latency
//
// #############################################################################

TEST_CASE("Benchmark - list query", "[benchmark][list]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepository>();

    auto userId = insertTestUser("bench_author", "bench@test.com", 0);
    for (int i = 0; i < 10; ++i) {
        insertTestArticle("bench_cat", userId, "Article_" + std::to_string(i), i * 10);
    }

    auto query = makeArticleQuery("bench_cat");
    sync(TestArticleListRepository::query(query));

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("query (10 articles, L1 hit)", [&]() -> drogon::Task<void> {
        co_await TestArticleListRepository::query(query);
    })));

    WARN(formatTable("List query", results));
}


// #############################################################################
//
//  7. Multi-threaded throughput
//
// #############################################################################

// #############################################################################
//
//  7a. Raw L1 cache throughput (no coroutine, no sync_wait)
//      Pure ShardMap performance — 8 threads truly parallel on 8 cores.
//
// #############################################################################

TEST_CASE("Benchmark - L1 raw throughput", "[benchmark][throughput][raw]")
{
    TransactionGuard tx;

    static constexpr int THREADS = 8;
    static constexpr int OPS = 2'000'000;
    static constexpr int RUNS = 3;
    static constexpr int NUM_KEYS = 64;

    // Insert and warm cache
    std::vector<int64_t> ids;
    ids.reserve(NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; ++i) {
        auto kid = insertTestItem("bench_raw_" + std::to_string(i), i);
        sync(L1TestItemRepository::findById(kid));
        ids.push_back(kid);
    }

    SECTION("L1 raw — single key (contention)") {
        auto id = ids[0];
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int, int n) {
                for (int j = 0; j < n; ++j) {
                    auto ptr = TestInternals::getFromCache<L1TestItemRepository>(id);
                    doNotOptimize(ptr);
                }
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("L1 raw (single key)", THREADS, OPS, best));
    }

    SECTION("L1 raw — distributed keys (parallel)") {
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int tid, int n) {
                for (int j = 0; j < n; ++j) {
                    auto ptr = TestInternals::getFromCache<L1TestItemRepository>(
                        ids[(tid * n + j) % NUM_KEYS]);
                    doNotOptimize(ptr);
                }
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("L1 raw (distributed)", THREADS, OPS, best));
    }

    SECTION("L1 raw — findByIdAsJson distributed") {
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int tid, int n) {
                for (int j = 0; j < n; ++j) {
                    auto ptr = TestInternals::getFromCache<L1TestItemRepository>(
                        ids[(tid * n + j) % NUM_KEYS]);
                    if (ptr) doNotOptimize(ptr->toJson());
                }
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("L1 raw findByIdAsJson (distributed)", THREADS, OPS, best));
    }

    SECTION("L1 raw — mixed read/write distributed (75R/25W)") {
        // Grab a cached entity to use for put operations
        auto template_ptr = TestInternals::getFromCache<L1TestItemRepository>(ids[0]);
        REQUIRE(template_ptr != nullptr);

        static constexpr int MIXED_OPS = 2'000'000;
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, MIXED_OPS, [&](int tid, int n) {
                std::mt19937 rng(tid * 42 + 7);
                for (int j = 0; j < n; ++j) {
                    auto kid = ids[(tid * n + j) % NUM_KEYS];
                    if (rng() % 4 != 0) {
                        // 75% read
                        auto ptr = TestInternals::getFromCache<L1TestItemRepository>(kid);
                        doNotOptimize(ptr);
                    } else {
                        // 25% write: invalidate + put (simulates write-through)
                        TestInternals::invalidateL1<L1TestItemRepository>(kid);
                        TestInternals::putInCache<L1TestItemRepository>(kid, template_ptr);
                    }
                }
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("L1 raw mixed (distributed, 75R/25W)", THREADS, MIXED_OPS, best));
    }
}


// #############################################################################
//
//  7b. Full-path throughput (coroutine + sync_wait overhead)
//
// #############################################################################

TEST_CASE("Benchmark - multi-threaded throughput", "[benchmark][throughput]")
{
    TransactionGuard tx;

    static constexpr int THREADS = 8;
    static constexpr int OPS = 500'000;
    static constexpr int RUNS = 3;  // best-of-N for stability

    // --- Single key (contention test: all threads hammer the same shard) ---
    auto id = insertTestItem("bench_mt", 42);
    sync(L1TestItemRepository::findById(id));
    sync(FullCacheTestItemRepository::findById(id));

    SECTION("L1 findById — single key (contention)") {
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int, int n) {
                sync([&, n]() -> drogon::Task<void> {
                    for (int j = 0; j < n; ++j) {
                        co_await L1TestItemRepository::findById(id);
                    }
                }());
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("L1 findById (single key)", THREADS, OPS, best));
    }

    // --- Distributed keys (realistic: each thread hits different shards) ---
    static constexpr int NUM_KEYS = 64;
    std::vector<int64_t> ids;
    ids.reserve(NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; ++i) {
        auto kid = insertTestItem("bench_dist_" + std::to_string(i), i);
        sync(L1TestItemRepository::findById(kid));  // warm L1
        ids.push_back(kid);
    }

    SECTION("L1 findById — distributed keys (parallel)") {
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int tid, int n) {
                sync([&, tid, n]() -> drogon::Task<void> {
                    for (int j = 0; j < n; ++j) {
                        co_await L1TestItemRepository::findById(
                            ids[(tid * n + j) % NUM_KEYS]);
                    }
                }());
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("L1 findById (distributed)", THREADS, OPS, best));
    }

    SECTION("L1 findByIdAsJson — distributed keys (parallel)") {
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int tid, int n) {
                sync([&, tid, n]() -> drogon::Task<void> {
                    for (int j = 0; j < n; ++j) {
                        co_await L1TestItemRepository::findByIdAsJson(
                            ids[(tid * n + j) % NUM_KEYS]);
                    }
                }());
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("L1 findByIdAsJson (distributed)", THREADS, OPS, best));
    }

}
