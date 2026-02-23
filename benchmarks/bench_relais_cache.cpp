/**
 * bench_relais_cache.cpp
 *
 * Performance benchmarks for the relais cache hierarchy.
 * All benchmarks use the real Relais public API (Repo::find, etc.).
 *
 * Measurement strategy:
 * - L1 operations (ns-scale): duration-based (tight loop, 1 or N threads).
 *   Per-sample timing would add ~30ns of Clock::now() overhead, dwarfing the
 *   5-10ns actual cost. Duration-based amortizes one timer over millions of ops.
 * - L2/DB/write operations (μs/ms-scale): sample-based (per-op timing).
 *   Clock overhead is negligible vs the measured I/O latency.
 *
 * Run with:
 *   ./bench_relais_cache                    # all benchmarks
 *   ./bench_relais_cache "[l1]"             # L1 only
 *   ./bench_relais_cache "[throughput]"      # multi-threaded only
 *   BENCH_SAMPLES=500 ./bench_relais_cache  # 500 samples per latency benchmark
 *   BENCH_DURATION_S=5 ./bench_relais_cache # custom duration for throughput
 */

#include <catch2/catch_test_macros.hpp>

#include "BenchEngine.h"

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"

#include <random>

using namespace relais_test;
using namespace relais_bench;


// #############################################################################
//
//  1. L1 cache latency (single-thread, duration-based)
//
// #############################################################################

/// Bare L1 — no TTL, no GDSF, zero metadata per entry
using BareL1TestItemRepo = Repo<TestItemWrapper, "bench:bare_l1", test_config::BareL1>;

TEST_CASE("Benchmark - L1 cache hit", "[benchmark][l1]")
{
    TransactionGuard tx;

    static constexpr int NUM_KEYS = 10000;

    std::vector<int64_t> ids;
    ids.reserve(NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; ++i) {
        auto kid = insertTestItem("bench_l1_" + std::to_string(i), i);
        sync(BareL1TestItemRepo::find(kid));
        sync(L1TestItemRepo::find(kid));
        ids.push_back(kid);
    }

    // Pure L1 lookup via getFromCache — no Immediate, no coroutine, no sync().
    // Measures: ParlayHash find + epoch guard + TTL check (if enabled).
    using TI = TestInternals;

    auto bare = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            doNotOptimize(TI::getFromCache<BareL1TestItemRepo>(ids[ops % NUM_KEYS]));
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 getFromCache bare (1 thread)", 1, bare));

    auto withTtl = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            doNotOptimize(TI::getFromCache<L1TestItemRepo>(ids[ops % NUM_KEYS]));
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 getFromCache +TTL (1 thread)", 1, withTtl));

    auto viaFind = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            auto imm = BareL1TestItemRepo::find(ids[ops % NUM_KEYS]);
            doNotOptimize(imm.await_resume());
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 find() bare (1 thread)", 1, viaFind));
}


// #############################################################################
//
//  2. L2 cache hit latency (sample-based — clock overhead negligible vs μs I/O)
//
// #############################################################################

TEST_CASE("Benchmark - L2 cache hit", "[benchmark][l2]")
{
    TransactionGuard tx;
    auto id = insertTestItem("bench_l2", 42);
    sync(L2TestItemRepo::find(id));

    std::vector<BenchResult> results;

    results.push_back(bench("find", [&]() {
        doNotOptimize(sync(L2TestItemRepo::find(id)));
    }));

    results.push_back(bench("findJson", [&]() {
        doNotOptimize(sync(L2TestItemRepo::findJson(id)));
    }));

    WARN(formatTable("L2 cache hit (Redis)", results));
}


// #############################################################################
//
//  3. L1+L2 cache hit latency (L2 fallback)
//
// #############################################################################

TEST_CASE("Benchmark - L1+L2 cache hit", "[benchmark][full-cache]")
{
    TransactionGuard tx;
    auto id = insertTestItem("bench_both", 42);
    sync(FullCacheTestItemRepo::find(id));

    std::vector<BenchResult> results;

    results.push_back(benchWithSetup("find (L2 fallback)",
        [&]() { FullCacheTestItemRepo::evict(id); },
        [&]() { doNotOptimize(sync(FullCacheTestItemRepo::find(id))); }
    ));

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

    results.push_back(benchWithSetup("find (L1 miss -> DB)",
        [&]() { L1TestItemRepo::evict(id); },
        [&]() { doNotOptimize(sync(L1TestItemRepo::find(id))); }
    ));

    results.push_back(benchWithSetup("find (L1+L2 miss -> DB)",
        [&]() { sync(FullCacheTestItemRepo::invalidate(id)); },
        [&]() { doNotOptimize(sync(FullCacheTestItemRepo::find(id))); }
    ));

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
    sync(L1TestItemRepo::find(upd_id));
    int c1 = 0;

    auto upd_both_id = insertTestItem("bench_upd_both", 42);
    sync(FullCacheTestItemRepo::find(upd_both_id));
    int c2 = 0;

    std::vector<BenchResult> results;

    results.push_back(bench("insert + erase (L1)", [&]() {
        auto entity = makeTestItem("bench_cr", 42);
        auto created = sync(L1TestItemRepo::insert(entity));
        if (created) sync(L1TestItemRepo::erase(created->id));
    }));

    results.push_back(bench("update (L1)", [&]() {
        ++c1;
        auto entity = makeTestItem(
            "bench_u_" + std::to_string(c1), c1,
            "bench_u_description", true, upd_id);
        sync(L1TestItemRepo::update(upd_id, entity));
    }));

    results.push_back(bench("update (L1+L2)", [&]() {
        ++c2;
        auto entity = makeTestItem(
            "bench_ub_" + std::to_string(c2), c2,
            "bench_ub_description", true, upd_both_id);
        sync(FullCacheTestItemRepo::update(upd_both_id, entity));
    }));

    WARN(formatTable("Write operations", results));
}


// #############################################################################
//
//  6. List query latency (L1 hit — duration-based)
//
// #############################################################################

TEST_CASE("Benchmark - list query", "[benchmark][list]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    auto userId = insertTestUser("bench_author", "bench@test.com", 0);
    for (int i = 0; i < 10; ++i) {
        insertTestArticle("bench_cat", userId, "Article_" + std::to_string(i), i * 10);
    }

    auto query = makeArticleQuery("bench_cat");
    sync(TestArticleListRepo::query(query));

    auto result = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            auto task = TestArticleListRepo::query(query);
            doNotOptimize(task.await_resume());
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("list query (10 articles, L1 hit)", 1, result));
}


// #############################################################################
//
//  7. Multi-threaded throughput (duration-based, default 5s)
//
// #############################################################################

TEST_CASE("Benchmark - L1 throughput", "[benchmark][throughput]")
{
    TransactionGuard tx;

    static constexpr int THREADS = 6;
    static constexpr int NUM_KEYS = 10000;

    std::vector<int64_t> ids;
    ids.reserve(NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; ++i) {
        auto kid = insertTestItem("bench_tp_" + std::to_string(i), i);
        sync(L1TestItemRepo::find(kid));
        ids.push_back(kid);
    }

    SECTION("L1 find — single key (contention)") {
        auto id = ids[0];
        auto result = measureDuration(THREADS, [&](int, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto task = L1TestItemRepo::find(id);
                doNotOptimize(task.await_resume());
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("L1 find (single key)", THREADS, result));
    }

    SECTION("L1 find — distributed keys (parallel)") {
        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto task = L1TestItemRepo::find(
                    ids[(tid * 11 + ops) % NUM_KEYS]);
                doNotOptimize(task.await_resume());
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("L1 find (distributed)", THREADS, result));
    }

    SECTION("L1 findJson — distributed") {
        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto task = L1TestItemRepo::findJson(
                    ids[(tid * 11 + ops) % NUM_KEYS]);
                doNotOptimize(task.await_resume());
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("L1 findJson (distributed)", THREADS, result));
    }
}

TEST_CASE("Benchmark - L1 throughput mixed", "[benchmark][throughput][mixed]") {
    TransactionGuard tx;

    static constexpr int THREADS = 6;
    static constexpr int NUM_KEYS = 10000;

    std::vector<int64_t> ids;
    ids.reserve(NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; ++i) {
        auto kid = insertTestItem("bench_tp_" + std::to_string(i), i);
        sync(L1TestItemRepo::find(kid));
        ids.push_back(kid);
    }

    SECTION("L1 mixed read/write — distributed (75R/25W)") {
        // Copy entity value from cache, then release the EpochGuard immediately.
        // A long-lived EpochGuard pins the epoch, preventing memory_pool rotation
        // (current → old → reserve). Benchmark threads start with empty pools,
        // so every pool_.New() would fall through to malloc() — causing massive
        // allocator contention that slows all threads including pure readers.
        auto template_entity = [&] {
            auto v = TestInternals::getFromCache<L1TestItemRepo>(ids[0]);
            REQUIRE(v != nullptr);
            return *v;
        }();

        // Track read/write ops separately to measure write impact on read throughput.
        // Write = putInCache only (UpdateInPlace strategy). No evict → no L1 miss
        // window → find() always hits fromValue() fast path.
        struct MixedOps { int64_t reads = 0; int64_t writes = 0; };
        std::vector<MixedOps> thread_ops(THREADS);

        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            std::mt19937 rng(tid * 42 + 7);
            MixedOps local;
            while (running.load(std::memory_order_relaxed)) {
                auto kid = ids[(tid * 11 + local.reads + local.writes) % NUM_KEYS];
                if (rng() % 4 != 0) {
                    auto task = L1TestItemRepo::find(kid);
                    doNotOptimize(task.await_resume());
                    ++local.reads;
                } else {
                    TestInternals::putInCache<L1TestItemRepo>(kid, template_entity);
                    ++local.writes;
                }
            }
            thread_ops[tid] = local;
            return local.reads + local.writes;
        });

        int64_t total_reads = 0, total_writes = 0;
        for (auto& t : thread_ops) {
            total_reads += t.reads;
            total_writes += t.writes;
        }
        WARN(formatMixedThroughput("L1 mixed (distributed, 75R/25W)",
                                   THREADS, result, total_reads, total_writes));
    }

    SECTION("L1 mixed read/evict \xe2\x80\x94 distributed (95R/5W)") {
        // Production model: reads hit L1, writes are non-blocking evict() only.
        // In production, eviction is instant (~20ns) and re-population happens
        // lazily via coroutine suspension (non-blocking). Reads that miss go to
        // DB but in production this suspends the coroutine, not the thread.
        // Here we skip DB misses to measure pure L1 read + evict throughput.
        struct MixedOps { int64_t reads = 0; int64_t evicts = 0; int64_t misses = 0; };
        std::vector<MixedOps> thread_ops(THREADS);

        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            std::mt19937 rng(tid * 42 + 7);
            MixedOps local;
            while (running.load(std::memory_order_relaxed)) {
                auto kid = ids[(tid * 11 + local.reads + local.evicts) % NUM_KEYS];
                if (rng() % 20 != 0) {  // 95% reads
                    auto task = L1TestItemRepo::find(kid);
                    if (task.await_ready()) {
                        doNotOptimize(task.await_resume());
                    } else {
                        // Key evicted by another thread — in production this
                        // suspends the coroutine (non-blocking). Skip here.
                        ++local.misses;
                    }
                    ++local.reads;
                } else {  // 5% evictions (non-blocking, ~20ns)
                    L1TestItemRepo::evict(kid);
                    ++local.evicts;
                }
            }
            thread_ops[tid] = local;
            return local.reads + local.evicts;
        });

        int64_t total_reads = 0, total_evicts = 0, total_misses = 0;
        for (auto& t : thread_ops) {
            total_reads += t.reads;
            total_evicts += t.evicts;
            total_misses += t.misses;
        }
        double miss_rate = (total_reads > 0) ? 100.0 * total_misses / total_reads : 0.0;
        auto msg = formatMixedThroughput("L1 mixed read/evict (95R/5W)",
                                         THREADS, result, total_reads, total_evicts);
        std::ostringstream extra;
        extra << "\n  L1 miss rate:    " << std::fixed << std::setprecision(1)
              << miss_rate << "% (" << total_misses << " misses)";
        WARN(msg + extra.str());
    }

    SECTION("L1 mixed read/evict \xe2\x80\x94 coroutine (95R/5W)") {
        // Production model: concurrent coroutines on a single event loop.
        // L1 hits complete synchronously (fromValue → no suspension).
        // L1 misses (key evicted) suspend the coroutine → async DB fetch →
        // event loop serves other coroutines during the ~60μs round-trip.
        // This is how a real server handles mixed read/write: no thread blocks.

        static constexpr int CORO_COUNT = 64;

        struct CoroStats { int64_t reads = 0; int64_t evicts = 0; int64_t db_fetches = 0; };
        std::vector<CoroStats> coro_stats(CORO_COUNT);
        std::atomic<bool> coro_running{true};
        std::latch done{CORO_COUNT};

        auto t0 = Clock::now();

        for (int cid = 0; cid < CORO_COUNT; ++cid) {
            detail::testLoop().dispatch([&ids, cid, &coro_running, &coro_stats, &done]() {
                [](std::vector<int64_t>& ids, int cid, int num_keys,
                   std::atomic<bool>& running, CoroStats& stats,
                   std::latch& done) -> DetachedHandle {
                    std::mt19937 rng(cid * 42 + 7);
                    while (running.load(std::memory_order_relaxed)) {
                        auto kid = ids[(cid * 11 + stats.reads + stats.evicts) % num_keys];
                        if (rng() % 20 != 0) {  // 95% reads
                            auto task = L1TestItemRepo::find(kid);
                            bool was_l1 = task.await_ready();
                            auto result = co_await std::move(task);
                            doNotOptimize(result);
                            ++stats.reads;
                            if (!was_l1) ++stats.db_fetches;
                        } else {  // 5% evictions
                            L1TestItemRepo::evict(kid);
                            ++stats.evicts;
                        }
                    }
                    done.count_down();
                }(ids, cid, NUM_KEYS, coro_running, coro_stats[cid], done);
            });
        }

        std::this_thread::sleep_for(std::chrono::seconds(benchDurationSeconds()));
        coro_running.store(false, std::memory_order_relaxed);
        done.wait();
        auto elapsed = Clock::now() - t0;

        int64_t total_reads = 0, total_evicts = 0, total_db = 0;
        for (auto& s : coro_stats) {
            total_reads += s.reads;
            total_evicts += s.evicts;
            total_db += s.db_fetches;
        }

        DurationResult result{elapsed, total_reads + total_evicts};
        auto msg = formatMixedThroughput("L1 mixed read/evict coroutine (95R/5W)",
                                         1, result, total_reads, total_evicts);
        double db_pct = (total_reads > 0) ? 100.0 * total_db / total_reads : 0.0;
        std::ostringstream extra;
        extra << "\n  DB fetches:      " << total_db
              << " (" << std::fixed << std::setprecision(1) << db_pct << "% of reads)"
              << "\n  coroutines:      " << CORO_COUNT;
        WARN(msg + extra.str());
    }
}


// #############################################################################
//
//  8. Production simulation (coroutine, L1+L2+DB, pinned event loop)
//
//  Realistic model: concurrent coroutines on a single event loop.
//  Two variants compared side-by-side:
//    - L1+DB (no Redis): misses go directly to PostgreSQL
//    - L1+L2+DB (with Redis): misses try Redis first, then PostgreSQL
//
//  Run with:
//    BENCH_PG_POOL_MAX=16 ./bench_relais_cache "[production]"
//    BENCH_PIN_IO=2 BENCH_PG_POOL_MAX=16 ./bench_relais_cache "[production]"
//
// #############################################################################

namespace {

// Coroutine worker for production benchmark.
// Templated on Repo to compare L1-only vs L1+L2 with identical logic.
template<typename Repo>
struct ProdStats {
    int64_t reads = 0;
    int64_t l1_evicts = 0;
    int64_t invalidates = 0;
    int64_t l1_hits = 0;
};

template<typename Repo>
DetachedHandle prodWorker(
        const std::vector<int64_t>& ids, int cid, int num_keys,
        std::atomic<bool>& running, ProdStats<Repo>& stats,
        std::latch& done) {
    std::mt19937 rng(cid * 42 + 7);
    while (running.load(std::memory_order_relaxed)) {
        auto kid = ids[(cid * 11 + stats.reads + stats.l1_evicts
                        + stats.invalidates) % num_keys];
        auto roll = rng() % 100;
        if (roll >= 2) {  // 98% reads
            auto task = Repo::find(kid);
            bool was_l1 = task.await_ready();
            auto result = co_await std::move(task);
            doNotOptimize(result);
            ++stats.reads;
            if (was_l1) ++stats.l1_hits;
        } else if (roll == 1) {  // 1% L1 evictions (next read → L2 or DB)
            Repo::evict(kid);
            ++stats.l1_evicts;
        } else {  // 1% full invalidations (next read → DB)
            co_await Repo::invalidate(kid);
            ++stats.invalidates;
        }
    }
    done.count_down();
}

template<typename Repo>
std::string runProductionBench(
        const std::string& label,
        const std::vector<int64_t>& ids, int num_keys,
        int coro_count, int io_core) {
    std::vector<ProdStats<Repo>> coro_stats(coro_count);
    std::atomic<bool> running{true};
    std::latch done{coro_count};

    auto t0 = Clock::now();

    for (int cid = 0; cid < coro_count; ++cid) {
        detail::testLoop().dispatch(
            [&ids, cid, num_keys, &running, &coro_stats, &done]() {
                prodWorker<Repo>(ids, cid, num_keys,
                                 running, coro_stats[cid], done);
            });
    }

    std::this_thread::sleep_for(std::chrono::seconds(benchDurationSeconds()));
    running.store(false, std::memory_order_relaxed);
    done.wait();
    auto elapsed = Clock::now() - t0;

    // Aggregate
    ProdStats<Repo> total{};
    for (auto& s : coro_stats) {
        total.reads += s.reads;
        total.l1_evicts += s.l1_evicts;
        total.invalidates += s.invalidates;
        total.l1_hits += s.l1_hits;
    }

    int64_t total_ops = total.reads + total.l1_evicts + total.invalidates;
    int64_t total_writes = total.l1_evicts + total.invalidates;
    DurationResult result{elapsed, total_ops};

    auto msg = formatMixedThroughput(label, 1, result, total.reads, total_writes);

    double l1_pct = (total.reads > 0) ? 100.0 * total.l1_hits / total.reads : 0.0;
    int64_t l1_misses = total.reads - total.l1_hits;

    std::ostringstream extra;
    extra << "\n  L1 hit rate:     " << std::fixed << std::setprecision(1) << l1_pct << "%"
          << "\n  L1 misses:       " << l1_misses << " (\u2192 L2 or DB)"
          << "\n  L1 evictions:    " << total.l1_evicts << " (next read \u2192 L2 or DB)"
          << "\n  invalidations:   " << total.invalidates << " (next read \u2192 DB)"
          << "\n  coroutines:      " << coro_count
          << "\n  IO pinned:       core " << io_core;
    return msg + extra.str();
}

} // anonymous namespace


TEST_CASE("Benchmark - production simulation", "[benchmark][production]") {
    TransactionGuard tx;

    static constexpr int NUM_KEYS = 10000;
    static constexpr int CORO_COUNT = 128;

    // Pin event loop thread to a dedicated core.
    // Default: core 1 (avoids core 0 often used by OS/interrupts).
    // Override: BENCH_PIN_IO=N
    int io_core = 1;
    if (auto* env = std::getenv("BENCH_PIN_IO"))
        io_core = std::atoi(env);

    {
        std::promise<void> p;
        auto f = p.get_future();
        detail::testLoop().dispatch([&p, io_core]() {
            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(io_core, &mask);
            sched_setaffinity(0, sizeof(mask), &mask);
            p.set_value();
        });
        f.wait();
    }

    // ---------- L1+DB variant (no Redis) ----------

    SECTION("L1+DB baseline — warm reads (single thread)") {
        std::vector<int64_t> ids;
        ids.reserve(NUM_KEYS);
        for (int i = 0; i < NUM_KEYS; ++i) {
            auto kid = insertTestItem("bench_prod_" + std::to_string(i), i);
            sync(L1TestItemRepo::find(kid));
            ids.push_back(kid);
        }

        auto result = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto task = L1TestItemRepo::find(ids[ops % NUM_KEYS]);
                doNotOptimize(task.await_resume());
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("L1+DB find, warm L1 (1 thread)", 1, result));
    }

    SECTION("L1+DB production mix (98R/1E/1I)") {
        std::vector<int64_t> ids;
        ids.reserve(NUM_KEYS);
        for (int i = 0; i < NUM_KEYS; ++i) {
            auto kid = insertTestItem("bench_prod_" + std::to_string(i), i);
            sync(L1TestItemRepo::find(kid));
            ids.push_back(kid);
        }

        auto msg = runProductionBench<L1TestItemRepo>(
            "L1+DB (98R/1E/1I, no Redis)",
            ids, NUM_KEYS, CORO_COUNT, io_core);
        WARN(msg);
    }

    // ---------- L1+DB high-miss variant (50R/25E/25I) ----------

    SECTION("L1+DB high-miss workload (50R/25E/25I)") {
        std::vector<int64_t> ids;
        ids.reserve(NUM_KEYS);
        for (int i = 0; i < NUM_KEYS; ++i) {
            auto kid = insertTestItem("bench_hmiss_" + std::to_string(i), i);
            sync(L1TestItemRepo::find(kid));
            ids.push_back(kid);
        }

        // High-miss variant: 50% reads, 25% evictions, 25% invalidations
        // ~50% DB miss rate → batch sizes of 10-50 → demonstrates pipelining gains
        std::vector<ProdStats<L1TestItemRepo>> coro_stats(CORO_COUNT);
        std::atomic<bool> running{true};
        std::latch done{CORO_COUNT};

        auto t0 = Clock::now();

        for (int cid = 0; cid < CORO_COUNT; ++cid) {
            detail::testLoop().dispatch(
                [&ids, cid, &running, &coro_stats, &done]() {
                    [](const std::vector<int64_t>& ids, int cid, int num_keys,
                       std::atomic<bool>& running, ProdStats<L1TestItemRepo>& stats,
                       std::latch& done) -> DetachedHandle {
                        std::mt19937 rng(cid * 42 + 7);
                        while (running.load(std::memory_order_relaxed)) {
                            auto kid = ids[(cid * 11 + stats.reads + stats.l1_evicts
                                            + stats.invalidates) % num_keys];
                            auto roll = rng() % 4;
                            if (roll < 2) {  // 50% reads
                                auto task = L1TestItemRepo::find(kid);
                                bool was_l1 = task.await_ready();
                                auto result = co_await std::move(task);
                                doNotOptimize(result);
                                ++stats.reads;
                                if (was_l1) ++stats.l1_hits;
                            } else if (roll == 2) {  // 25% L1 evictions
                                L1TestItemRepo::evict(kid);
                                ++stats.l1_evicts;
                            } else {  // 25% full invalidations
                                co_await L1TestItemRepo::invalidate(kid);
                                ++stats.invalidates;
                            }
                        }
                        done.count_down();
                    }(ids, cid, NUM_KEYS, running, coro_stats[cid], done);
                });
        }

        std::this_thread::sleep_for(std::chrono::seconds(benchDurationSeconds()));
        running.store(false, std::memory_order_relaxed);
        done.wait();
        auto elapsed = Clock::now() - t0;

        ProdStats<L1TestItemRepo> total{};
        for (auto& s : coro_stats) {
            total.reads += s.reads;
            total.l1_evicts += s.l1_evicts;
            total.invalidates += s.invalidates;
            total.l1_hits += s.l1_hits;
        }

        int64_t total_ops = total.reads + total.l1_evicts + total.invalidates;
        int64_t total_writes = total.l1_evicts + total.invalidates;
        DurationResult result{elapsed, total_ops};

        auto msg = formatMixedThroughput("L1+DB high-miss (50R/25E/25I)",
                                         1, result, total.reads, total_writes);

        double l1_pct = (total.reads > 0) ? 100.0 * total.l1_hits / total.reads : 0.0;
        int64_t l1_misses = total.reads - total.l1_hits;

        std::ostringstream extra;
        extra << "\n  L1 hit rate:     " << std::fixed << std::setprecision(1) << l1_pct << "%"
              << "\n  L1 misses:       " << l1_misses << " (\xe2\x86\x92 DB)"
              << "\n  L1 evictions:    " << total.l1_evicts
              << "\n  invalidations:   " << total.invalidates
              << "\n  coroutines:      " << CORO_COUNT
              << "\n  IO pinned:       core " << io_core;
        WARN(msg + extra.str());
    }

    // ---------- L1+L2+DB variant (with Redis) ----------

    SECTION("L1+L2+DB baseline — warm reads (single thread)") {
        std::vector<int64_t> ids;
        ids.reserve(NUM_KEYS);
        for (int i = 0; i < NUM_KEYS; ++i) {
            auto kid = insertTestItem("bench_prod_" + std::to_string(i), i);
            sync(FullCacheTestItemRepo::find(kid));
            ids.push_back(kid);
        }

        auto result = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto task = FullCacheTestItemRepo::find(ids[ops % NUM_KEYS]);
                doNotOptimize(task.await_resume());
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("L1+L2+DB find, warm L1 (1 thread)", 1, result));
    }

    SECTION("L1+L2+DB production mix (98R/1E/1I)") {
        std::vector<int64_t> ids;
        ids.reserve(NUM_KEYS);
        for (int i = 0; i < NUM_KEYS; ++i) {
            auto kid = insertTestItem("bench_prod_" + std::to_string(i), i);
            sync(FullCacheTestItemRepo::find(kid));
            ids.push_back(kid);
        }

        auto msg = runProductionBench<FullCacheTestItemRepo>(
            "L1+L2+DB (98R/1E/1I, with Redis)",
            ids, NUM_KEYS, CORO_COUNT, io_core);
        WARN(msg);
    }
}