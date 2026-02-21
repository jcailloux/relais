#pragma once

/**
 * BenchEngine.h
 *
 * Reusable micro-benchmark engine for relais performance tests.
 * Provides sample-based and duration-based benchmarking with formatted output.
 *
 * Environment variables:
 *   BENCH_SAMPLES=N     — samples per latency benchmark (default: 500)
 *   BENCH_DURATION_S=N  — seconds per duration benchmark (default: 10)
 *   BENCH_PIN_CPU=N     — pin main thread to core N (default: no pinning)
 */

#include "jcailloux/relais/io/Task.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <latch>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <sched.h>

namespace relais_bench {

namespace io = jcailloux::relais::io;

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
inline void doNotOptimize(const T& val) {
    asm volatile("" : : "r,m"(val) : "memory");
}

// =============================================================================
// Micro-benchmark engine
// =============================================================================

inline constexpr int WARMUP = 50;

inline int benchSamples() {
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

inline BenchResult computeStats(const std::string& name, std::vector<double>& times) {
    std::sort(times.begin(), times.end());
    auto n = static_cast<int>(times.size());
    double median = times[n / 2];
    double p99 = times[static_cast<int>(n * 0.99)];
    double mean = std::accumulate(times.begin(), times.end(), 0.0) / n;
    return {name, median, p99, mean, times.front(), times.back()};
}

template<typename Fn>
inline BenchResult bench(const std::string& name, Fn&& fn) {
    for (int i = 0; i < WARMUP; ++i) fn();

    const int samples = benchSamples();
    std::vector<double> times(samples);
    for (int i = 0; i < samples; ++i) {
        auto t0 = Clock::now();
        fn();
        times[i] = std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
    }

    return computeStats(name, times);
}

template<typename SetupFn, typename Fn>
inline BenchResult benchWithSetup(
        const std::string& name, SetupFn&& setup, Fn&& fn) {
    for (int i = 0; i < WARMUP; ++i) { setup(); fn(); }

    const int samples = benchSamples();
    std::vector<double> times(samples);
    for (int i = 0; i < samples; ++i) {
        setup();
        auto t0 = Clock::now();
        fn();
        times[i] = std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
    }

    return computeStats(name, times);
}

// Coroutine variants — used by I/O layer benchmarks (bench_io_redis, bench_io_pg)
// where the overhead (~40ns) is negligible vs μs-scale I/O round-trips.

template<typename Fn>
inline io::Task<BenchResult> benchAsync(const std::string& name, Fn&& fn) {
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


// =============================================================================
// Formatting utilities
// =============================================================================

inline std::string fmtDuration(double us) {
    std::ostringstream out;
    out << std::fixed;
    if (us < 1.0)            out << std::setprecision(0) << us * 1000 << " ns";
    else if (us < 1'000)     out << std::setprecision(1) << us << " us";
    else if (us < 1'000'000) out << std::setprecision(2) << us / 1'000 << " ms";
    else                     out << std::setprecision(2) << us / 1'000'000 << " s";
    return out.str();
}

inline std::string fmtOps(double ops) {
    std::ostringstream out;
    out << std::fixed;
    if (ops >= 1'000'000)  out << std::setprecision(1) << ops / 1'000'000 << "M ops/s";
    else if (ops >= 1'000) out << std::setprecision(1) << ops / 1'000 << "K ops/s";
    else                   out << std::setprecision(0) << ops << " ops/s";
    return out.str();
}

inline std::string formatTable(const std::string& title,
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

template<typename Fn>
inline auto measureParallel(int num_threads, int ops_per_thread, Fn&& fn) {
    std::latch ready{num_threads};
    std::latch go{1};
    std::vector<std::jthread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(i % std::thread::hardware_concurrency(), &mask);
            sched_setaffinity(0, sizeof(mask), &mask);

            ready.count_down();
            go.wait();
            fn(i, ops_per_thread);
        });
    }

    ready.wait();
    auto t0 = Clock::now();
    go.count_down();
    for (auto& t : threads) t.join();
    return Clock::now() - t0;
}

inline std::string formatThroughput(
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

struct DurationResult {
    Clock::duration elapsed;
    int64_t total_ops;
};

inline int benchDurationSeconds() {
    static int n = [] {
        if (auto* env = std::getenv("BENCH_DURATION_S"))
            if (int v = std::atoi(env); v > 0) return v;
        return 5;
    }();
    return n;
}

template<typename Fn>
inline DurationResult measureDuration(int num_threads, Fn&& fn) {
    std::latch ready{num_threads};
    std::latch go{1};
    std::atomic<bool> running{true};
    std::vector<std::atomic<int64_t>> ops_counts(num_threads);
    std::vector<std::jthread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        ops_counts[i].store(0);
        threads.emplace_back([&, i]() {
            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(i % std::thread::hardware_concurrency(), &mask);
            sched_setaffinity(0, sizeof(mask), &mask);

            ready.count_down();
            go.wait();
            ops_counts[i].store(fn(i, running), std::memory_order_relaxed);
        });
    }

    ready.wait();
    auto t0 = Clock::now();
    go.count_down();
    std::this_thread::sleep_for(std::chrono::seconds(benchDurationSeconds()));
    running.store(false, std::memory_order_relaxed);
    for (auto& t : threads) t.join();
    auto elapsed = Clock::now() - t0;

    int64_t total = 0;
    for (auto& c : ops_counts) total += c.load(std::memory_order_relaxed);
    return {elapsed, total};
}

inline std::string formatDurationThroughput(
        const std::string& label, int threads,
        const DurationResult& result) {
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(result.elapsed).count();
    auto ops_per_sec = (us > 0) ? result.total_ops * 1'000'000.0 / us : 0.0;
    // Per-thread avg latency: wall_time / ops_per_thread
    auto ops_per_thread = result.total_ops / std::max(threads, 1);
    auto avg_us = (ops_per_thread > 0) ? static_cast<double>(us) / ops_per_thread : 0.0;

    auto bar = std::string(50, '-');
    std::ostringstream out;
    out << "\n"
        << "  " << bar << "\n"
        << "  " << label << "\n"
        << "  " << bar << "\n"
        << "  threads:      " << threads << "\n"
        << "  duration:     " << std::fixed << std::setprecision(2)
                              << static_cast<double>(us) / 1'000'000 << " s\n"
        << "  total ops:    " << result.total_ops << "\n"
        << "  throughput:   " << fmtOps(ops_per_sec) << "\n"
        << "  avg latency:  " << fmtDuration(avg_us) << "\n"
        << "  " << bar;
    return out.str();
}

inline std::string formatMixedThroughput(
        const std::string& label, int threads,
        const DurationResult& result,
        int64_t total_reads, int64_t total_writes,
        double read_only_ops_per_sec = 0.0) {
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(result.elapsed).count();
    auto read_ops_s = (us > 0) ? total_reads * 1'000'000.0 / us : 0.0;
    auto write_ops_s = (us > 0) ? total_writes * 1'000'000.0 / us : 0.0;
    auto total_ops_s = (us > 0) ? result.total_ops * 1'000'000.0 / us : 0.0;

    // Per-thread avg operation time (blended reads + writes)
    auto ops_per_thread = result.total_ops / std::max(threads, 1);
    auto avg_op_us = (ops_per_thread > 0) ? static_cast<double>(us) / ops_per_thread : 0.0;

    // Estimate per-write cost: if read cost = avg_op from read-only benchmark,
    // then 0.75 * read_cost + 0.25 * write_cost = avg_op
    // => write_cost = (avg_op - 0.75 * read_cost) / 0.25
    double read_ratio = (result.total_ops > 0)
        ? static_cast<double>(total_reads) / result.total_ops : 0.75;
    double write_ratio = 1.0 - read_ratio;

    auto bar = std::string(50, '-');
    std::ostringstream out;
    out << "\n"
        << "  " << bar << "\n"
        << "  " << label << "\n"
        << "  " << bar << "\n"
        << "  threads:         " << threads << "\n"
        << "  duration:        " << std::fixed << std::setprecision(2)
                                 << static_cast<double>(us) / 1'000'000 << " s\n"
        << "  read  throughput:" << std::setw(15) << fmtOps(read_ops_s) << "\n"
        << "  write throughput:" << std::setw(15) << fmtOps(write_ops_s) << "\n"
        << "  total throughput:" << std::setw(15) << fmtOps(total_ops_s) << "\n"
        << "  avg op latency:  " << fmtDuration(avg_op_us) << " /thread\n";

    if (read_only_ops_per_sec > 0.0) {
        double read_only_lat = 1'000'000.0 / (read_only_ops_per_sec / threads);
        double est_write_lat = (avg_op_us - read_ratio * read_only_lat)
                             / std::max(write_ratio, 0.01);
        out << "  est. read cost:  " << fmtDuration(read_only_lat) << "\n"
            << "  est. write cost: " << fmtDuration(est_write_lat) << "\n";
    }

    out << "  " << bar;
    return out.str();
}

} // namespace relais_bench
