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
#include <span>

using namespace relais_test;
using namespace relais_bench;


// #############################################################################
//
//  1. L1 cache latency (single-thread, duration-based)
//
// #############################################################################

/// Bare L1 — no TTL, no GDSF, zero metadata per entry
using BareL1TestItemRepo = Repo<TestItemEntity, "bench:bare_l1", test_config::BareL1>;

namespace {
inline std::string gdsf_banner() {
    using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;
    std::ostringstream out;
    out << "\n  [config] GDSF: " << (GDSFPolicy::enabled ? "ON" : "OFF")
        << "  |  access: direct fetch_add"
        << "  |  cleanup every " << (GDSFPolicy::kCleanupMask + 1) << " insertions";
    if (GDSFPolicy::enabled && GDSFPolicy::instance().maxMemory() > 0)
        out << "  |  budget " << GDSFPolicy::instance().maxMemory() << " B";
    return out.str();
}
} // anonymous namespace

TEST_CASE("Benchmark - L1 cache hit", "[benchmark][l1]")
{
    TransactionGuard tx;

    static constexpr int NUM_KEYS = 1000000;
    static constexpr int THREADS = 6;

    WARN(gdsf_banner());

    // Bulk insert (single SQL round-trip)
    auto t_insert = Clock::now();
    auto bulk = execQueryArgs(
        "INSERT INTO relais_test_items (name, value, description, is_active) "
        "SELECT 'bench_l1_' || i, i::int4, 'desc_' || i, true "
        "FROM generate_series(0, $1::int) AS i "
        "RETURNING id",
        NUM_KEYS - 1);
    auto insert_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - t_insert).count();

    std::vector<int64_t> ids;
    ids.reserve(NUM_KEYS);
    for (int r = 0; r < bulk.rows(); ++r)
        ids.push_back(bulk[r].get<int64_t>(0));

    // Warm both L1 caches (sequential find, each triggers DB fetch → L1 store)
    auto t_warm = Clock::now();
    for (auto id : ids) {
        sync(BareL1TestItemRepo::find(id));
        sync(L1TestItemRepo::find(id));
    }
    auto warm_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - t_warm).count();
    WARN("\n  Insert: " << insert_ms << " ms  |  Warmup: " << warm_ms << " ms  |  "
         << ids.size() << " keys");

    // Pure L1 lookup via getFromCache — no Immediate, no coroutine, no sync().
    // Measures: ParlayHash find + epoch guard + TTL check (if enabled)
    // + GDSF ghost check + bumpScore (if GDSF enabled).
    //
    // Each variant runs single-thread then multi-thread (6T, distributed keys)
    // to isolate contention cost of bumpScore's fetch_add on metadata cache lines.
    using TI = TestInternals;

    L1TestItemRepo::resetMetrics();

    // --- getFromCache bare ---

    auto bare_1t = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            doNotOptimize(TI::getFromCache<BareL1TestItemRepo>(ids[ops % NUM_KEYS]));
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 getFromCache bare (1T)", 1, bare_1t));

    auto bare_mt = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            doNotOptimize(TI::getFromCache<BareL1TestItemRepo>(
                ids[(tid * 11 + ops) % NUM_KEYS]));
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 getFromCache bare (6T)", THREADS, bare_mt));

    // --- getFromCache +TTL ---

    auto ttl_1t = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            doNotOptimize(TI::getFromCache<L1TestItemRepo>(ids[ops % NUM_KEYS]));
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 getFromCache +TTL (1T)", 1, ttl_1t));

    auto ttl_mt = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            doNotOptimize(TI::getFromCache<L1TestItemRepo>(
                ids[(tid * 11 + ops) % NUM_KEYS]));
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 getFromCache +TTL (6T)", THREADS, ttl_mt));

    // --- find() bare (includes Immediate wrapper overhead) ---

    auto find_1t = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            auto imm = BareL1TestItemRepo::find(ids[ops % NUM_KEYS]);
            doNotOptimize(imm.await_resume());
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 find() bare (1T)", 1, find_1t));

    auto find_mt = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            auto imm = BareL1TestItemRepo::find(
                ids[(tid * 11 + ops) % NUM_KEYS]);
            doNotOptimize(imm.await_resume());
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 find() bare (6T)", THREADS, find_mt));

    // --- Single-key contention: all threads hit the same cache line ---
    // Isolates the cost of bumpScore's fetch_add under maximum contention.
    // Compare with distributed variants above to measure cache-line bouncing.

    auto id0 = ids[0];

    auto contention_bare = measureDuration(THREADS, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            doNotOptimize(TI::getFromCache<BareL1TestItemRepo>(id0));
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 getFromCache bare (6T, 1 key)", THREADS, contention_bare));

    auto contention_ttl = measureDuration(THREADS, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            doNotOptimize(TI::getFromCache<L1TestItemRepo>(id0));
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 getFromCache +TTL (6T, 1 key)", THREADS, contention_ttl));

    auto contention_find = measureDuration(THREADS, [&](int, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            auto imm = BareL1TestItemRepo::find(id0);
            doNotOptimize(imm.await_resume());
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("L1 find() bare (6T, 1 key)", THREADS, contention_find));

    WARN(formatSweepMetrics());
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
    WARN(gdsf_banner());

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
    WARN(gdsf_banner());

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
        struct MixedOps { int64_t reads = 0; int64_t evicts = 0; };
        std::vector<MixedOps> thread_ops(THREADS);

        L1TestItemRepo::resetMetrics();
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

        int64_t total_reads = 0, total_evicts = 0;
        for (auto& t : thread_ops) {
            total_reads += t.reads;
            total_evicts += t.evicts;
        }
        auto m = L1TestItemRepo::metrics();
        double miss_rate = (1.0 - m.l1HitRatio()) * 100.0;
        auto msg = formatMixedThroughput("L1 mixed read/evict (95R/5W)",
                                         THREADS, result, total_reads, total_evicts);
        std::ostringstream extra;
        extra << "\n  L1 miss rate:    " << std::fixed << std::setprecision(1)
              << miss_rate << "% (" << m.l1_misses << " misses)"
              << formatSweepMetrics();
        WARN(msg + extra.str());
    }

    SECTION("L1 mixed read/evict \xe2\x80\x94 coroutine (95R/5W)") {
        // Production model: concurrent coroutines on a single event loop.
        // L1 hits complete synchronously (fromValue → no suspension).
        // L1 misses (key evicted) suspend the coroutine → async DB fetch →
        // event loop serves other coroutines during the ~60μs round-trip.
        // This is how a real server handles mixed read/write: no thread blocks.

        static constexpr int CORO_COUNT = 64;

        struct CoroStats { int64_t reads = 0; int64_t evicts = 0; };
        std::vector<CoroStats> coro_stats(CORO_COUNT);
        std::atomic<bool> coro_running{true};
        std::latch done{CORO_COUNT};

        L1TestItemRepo::resetMetrics();
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
                            auto result = co_await L1TestItemRepo::find(kid);
                            doNotOptimize(result);
                            ++stats.reads;
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

        int64_t total_reads = 0, total_evicts = 0;
        for (auto& s : coro_stats) {
            total_reads += s.reads;
            total_evicts += s.evicts;
        }

        auto m = L1TestItemRepo::metrics();
        DurationResult result{elapsed, total_reads + total_evicts};
        auto msg = formatMixedThroughput("L1 mixed read/evict coroutine (95R/5W)",
                                         1, result, total_reads, total_evicts);
        double db_pct = (1.0 - m.l1HitRatio()) * 100.0;
        std::ostringstream extra;
        extra << "\n  DB fetches:      " << m.l1_misses
              << " (" << std::fixed << std::setprecision(1) << db_pct << "% of reads)"
              << "\n  coroutines:      " << CORO_COUNT
              << formatSweepMetrics();
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

/// Zipf CDF table — shared, immutable after construction.
/// Separate from RNG so multiple coroutines can sample without copying the CDF.
class ZipfCDF {
    std::vector<double> cdf_;
public:
    explicit ZipfCDF(size_t n, double s) {
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
    [[nodiscard]] size_t sample(std::mt19937_64& rng) const {
        double u = std::uniform_real_distribution<double>(0.0, 1.0)(rng);
        auto it = std::lower_bound(cdf_.begin(), cdf_.end(), u);
        return (it == cdf_.end()) ? cdf_.size() - 1
             : static_cast<size_t>(std::distance(cdf_.begin(), it));
    }
    [[nodiscard]] size_t size() const noexcept { return cdf_.size(); }
};

// Coroutine worker for production benchmark.
// Templated on Repo to compare L1-only vs L1+L2 with identical logic.
template<typename Repo>
struct ProdStats {
    int64_t reads = 0;
    int64_t l1_evicts = 0;
    int64_t invalidates = 0;
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
            auto result = co_await Repo::find(kid);
            doNotOptimize(result);
            ++stats.reads;
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

/// Zipf-driven coroutine worker for realism benchmarks.
/// Each coroutine samples on-the-fly from the shared CDF with its own RNG.
template<typename Repo>
DetachedHandle zipfWorker(
        const std::vector<int64_t>& ids,
        const ZipfCDF& cdf,
        int cid,
        std::atomic<bool>& running, ProdStats<Repo>& stats,
        std::latch& done) {
    std::mt19937_64 rng(static_cast<uint64_t>(cid) * 2654435761ULL + 42);
    std::mt19937 op_rng(cid * 42 + 7);
    while (running.load(std::memory_order_relaxed)) {
        auto idx = cdf.sample(rng);
        auto kid = ids[idx];
        auto roll = op_rng() % 100;
        if (roll >= 2) {  // 98% reads
            auto result = co_await Repo::find(kid);
            doNotOptimize(result);
            ++stats.reads;
        } else if (roll == 1) {  // 1% L1 evictions
            Repo::evict(kid);
            ++stats.l1_evicts;
        } else {  // 1% full invalidations
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

    Repo::resetMetrics();
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
    }

    int64_t total_ops = total.reads + total.l1_evicts + total.invalidates;
    int64_t total_writes = total.l1_evicts + total.invalidates;
    DurationResult result{elapsed, total_ops};

    auto msg = formatMixedThroughput(label, 1, result, total.reads, total_writes);

    auto m = Repo::metrics();
    double l1_pct = m.l1HitRatio() * 100.0;
    int64_t l1_misses = static_cast<int64_t>(m.l1_misses);

    std::ostringstream extra;
    extra << "\n  L1 hit rate:     " << std::fixed << std::setprecision(1) << l1_pct << "%"
          << "\n  L1 misses:       " << l1_misses << " (\u2192 L2 or DB)"
          << "\n  L1 evictions:    " << total.l1_evicts << " (next read \u2192 L2 or DB)"
          << "\n  invalidations:   " << total.invalidates << " (next read \u2192 DB)"
          << "\n  coroutines:      " << coro_count
          << "\n  IO pinned:       core " << io_core
          << formatSweepMetrics();
    return msg + extra.str();
}

} // anonymous namespace


TEST_CASE("Benchmark - production simulation", "[benchmark][production]") {
    TransactionGuard tx;
    WARN(gdsf_banner());

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

        L1TestItemRepo::resetMetrics();
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
                                auto result = co_await L1TestItemRepo::find(kid);
                                doNotOptimize(result);
                                ++stats.reads;
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
        }

        int64_t total_ops = total.reads + total.l1_evicts + total.invalidates;
        int64_t total_writes = total.l1_evicts + total.invalidates;
        DurationResult result{elapsed, total_ops};

        auto msg = formatMixedThroughput("L1+DB high-miss (50R/25E/25I)",
                                         1, result, total.reads, total_writes);

        auto m = L1TestItemRepo::metrics();
        double l1_pct = m.l1HitRatio() * 100.0;
        int64_t l1_misses = static_cast<int64_t>(m.l1_misses);

        std::ostringstream extra;
        extra << "\n  L1 hit rate:     " << std::fixed << std::setprecision(1) << l1_pct << "%"
              << "\n  L1 misses:       " << l1_misses << " (\xe2\x86\x92 DB)"
              << "\n  L1 evictions:    " << total.l1_evicts
              << "\n  invalidations:   " << total.invalidates
              << "\n  coroutines:      " << CORO_COUNT
              << "\n  IO pinned:       core " << io_core
              << formatSweepMetrics();
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


// #############################################################################
//
//  9. Pure L1 throughput at scale — TLS vs direct fetch_add A/B
//
//  Fills the cache to capacity with variable-size entities, then measures
//  pure L1 read throughput (100% hits, no DB) across N threads.
//  No sync() overhead — uses task.await_resume() directly.
//
// #############################################################################

TEST_CASE("Benchmark - L1 throughput at scale", "[benchmark][throughput-scale]") {
    using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;
    using TI = relais_test::TestInternals;

    auto& policy = GDSFPolicy::instance();
    size_t max_mem = policy.maxMemory();
    if (max_mem == 0) {
        WARN("SKIP: Set RELAIS_L1_MAX_MEMORY (e.g. 1073741824 for 1GB)");
        return;
    }

    TransactionGuard tx;
    WARN(gdsf_banner());

    static constexpr size_t DESC_MIN = 64;
    static constexpr size_t DESC_MAX = 8192;
    static constexpr size_t EST_CACHED_BYTES = 512 + 256;

    int threads = 6;
    if (auto* env = std::getenv("BENCH_THREADS"))
        if (int v = std::atoi(env); v > 0) threads = v;

    // Fill cache to ~80% capacity (all hits, no eviction pressure)
    size_t est_capacity = max_mem / EST_CACHED_BYTES;
    size_t num_keys = static_cast<size_t>(est_capacity * 0.8);
    if (num_keys < 100) num_keys = 100;

    WARN("\n  Cache budget: " << max_mem / 1024 / 1024 << " MB  |  "
         << num_keys << " entities  |  " << threads << " threads");

    // Bulk insert
    auto t_insert = Clock::now();
    auto bulk = execQueryArgs(
        "INSERT INTO relais_test_items (name, value, description, is_active) "
        "SELECT 'scale_' || i, i::int4, "
        "  repeat('x', $1::int + abs(hashint4(i::int4)) % ($2::int - $1::int + 1)), "
        "  true "
        "FROM generate_series(0, $3::int) AS i "
        "RETURNING id",
        static_cast<int>(DESC_MIN), static_cast<int>(DESC_MAX),
        static_cast<int>(num_keys - 1));
    auto insert_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - t_insert).count();

    std::vector<int64_t> ids;
    ids.reserve(num_keys);
    for (int r = 0; r < bulk.rows(); ++r)
        ids.push_back(bulk[r].get<int64_t>(0));

    WARN("  Inserted " << ids.size() << " entities in " << insert_ms << " ms");

    // Warm both caches: fetch every entity once (sync, sequential)
    auto t_warm = Clock::now();
    for (auto id : ids) {
        sync(L1TestItemRepo::find(id));
        sync(BareL1TestItemRepo::find(id));
    }
    auto warm_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - t_warm).count();
    WARN("  Warmup in " << warm_ms << " ms  (100% L1 hit expected)\n");

    auto m0 = L1TestItemRepo::metrics();
    WARN("  L1 hits: " << m0.l1_hits << "  misses: " << m0.l1_misses);

    // Reset metrics for the actual benchmark
    L1TestItemRepo::resetMetrics();
#if RELAIS_ENABLE_METRICS
    policy.sweepCounters().reset();
#endif

    int nkeys = static_cast<int>(ids.size());

    ZipfCDF cdf(nkeys, 0.99);

    // Bare (no GDSF metadata) vs GDSF (direct fetch_add on access_count)
    {
        auto bare = measureDuration(threads, [&](int tid, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                doNotOptimize(TI::getFromCache<BareL1TestItemRepo>(
                    ids[(tid * 11 + ops) % nkeys]));
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("Bare   distrib", threads, bare));

        auto gdsf = measureDuration(threads, [&](int tid, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                doNotOptimize(TI::getFromCache<L1TestItemRepo>(
                    ids[(tid * 11 + ops) % nkeys]));
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("GDSF   distrib", threads, gdsf));
    }
}


// #############################################################################
//
//  10. Pure L1 find() throughput — 100% hit ratio at scale
//
//  Pre-warms the full cache, then measures pure find() (coroutine path)
//  throughput on 6 threads with distributed keys. No DB access, no eviction.
//
//  Run with:
//    RELAIS_L1_MAX_MEMORY=$((2*1024*1024*1024)) ./bench_relais_cache "[find-scale]"
//
// #############################################################################

TEST_CASE("Benchmark - L1 find at scale", "[benchmark][find-scale]") {
    using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;

    auto& policy = GDSFPolicy::instance();
    size_t max_mem = policy.maxMemory();
    if (max_mem == 0) {
        WARN("SKIP: Set RELAIS_L1_MAX_MEMORY (e.g. 2147483648 for 2GB)");
        return;
    }

    TransactionGuard tx;
    WARN(gdsf_banner());

    static constexpr size_t DESC_MIN = 64;
    static constexpr size_t DESC_MAX = 8192;
    static constexpr size_t EST_CACHED_BYTES = 512 + 256;

    int threads = 6;
    if (auto* env = std::getenv("BENCH_THREADS"))
        if (int v = std::atoi(env); v > 0) threads = v;

    // Fill cache to ~80% capacity
    size_t est_capacity = max_mem / EST_CACHED_BYTES;
    size_t num_keys = static_cast<size_t>(est_capacity * 0.8);
    if (num_keys < 100) num_keys = 100;

    WARN("\n  Cache budget: " << max_mem / 1024 / 1024 << " MB  |  "
         << num_keys << " entities  |  " << threads << " threads");

    // Bulk insert
    auto t_insert = Clock::now();
    auto bulk = execQueryArgs(
        "INSERT INTO relais_test_items (name, value, description, is_active) "
        "SELECT 'findscale_' || i, i::int4, "
        "  repeat('x', $1::int + abs(hashint4(i::int4)) % ($2::int - $1::int + 1)), "
        "  true "
        "FROM generate_series(0, $3::int) AS i "
        "RETURNING id",
        static_cast<int>(DESC_MIN), static_cast<int>(DESC_MAX),
        static_cast<int>(num_keys - 1));
    auto insert_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - t_insert).count();

    std::vector<int64_t> ids;
    ids.reserve(num_keys);
    for (int r = 0; r < bulk.rows(); ++r)
        ids.push_back(bulk[r].get<int64_t>(0));

    WARN("  Inserted " << ids.size() << " entities in " << insert_ms << " ms");

    // Pre-warm: fetch every entity once → 100% L1 hit after this
    auto t_warm = Clock::now();
    for (auto id : ids)
        sync(L1TestItemRepo::find(id));
    auto warm_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - t_warm).count();
    WARN("  Warmup in " << warm_ms << " ms\n");

    L1TestItemRepo::resetMetrics();

    int nkeys = static_cast<int>(ids.size());

    // Pure find() — coroutine path, 100% L1 hit, distributed keys
    auto result = measureDuration(threads, [&](int tid, std::atomic<bool>& running) -> int64_t {
        int64_t ops = 0;
        while (running.load(std::memory_order_relaxed)) {
            auto task = L1TestItemRepo::find(ids[(tid * 11 + ops) % nkeys]);
            doNotOptimize(task.await_resume());
            ++ops;
        }
        return ops;
    });
    WARN(formatDurationThroughput("find() 100% L1 hit", threads, result));

    auto m = L1TestItemRepo::metrics();
    WARN("  L1 hits: " << m.l1_hits << "  misses: " << m.l1_misses
         << "  hit ratio: " << (m.l1_hits * 100.0 / (m.l1_hits + m.l1_misses + 1)) << "%");
}


// #############################################################################
//
//  11. Production Realism — scaling test
//
//  Progressive keyspace growth to identify throughput cliffs.
//  Entities have variable-size descriptions (64-8KB).
//  Each L1 miss hits the real PostgreSQL backend.
//
//  Run with:
//    RELAIS_L1_MAX_MEMORY=$((64*1024*1024)) ./bench_relais_cache "[realism]"
//    RELAIS_L1_MAX_MEMORY=$((1024*1024*1024)) BENCH_DURATION_S=10 ./bench_relais_cache "[realism]"
//
// #############################################################################

TEST_CASE("Benchmark - production realism scaling", "[benchmark][realism]") {
    using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;
    using TI = relais_test::TestInternals;

    auto& policy = GDSFPolicy::instance();
    size_t max_mem = policy.maxMemory();
    if (max_mem == 0) {
        WARN("SKIP: Set RELAIS_L1_MAX_MEMORY (e.g. 67108864 for 64MB)");
        return;
    }

    TransactionGuard tx;
    WARN(gdsf_banner());

    // --- Config ---
    // Variable-size descriptions: log-normal distribution centered around 512 bytes.
    // min ~64B, median ~512B, max ~8KB.  Exercises GDSF size-aware scoring.
    static constexpr size_t DESC_MEDIAN = 512;
    static constexpr size_t DESC_MIN = 64;
    static constexpr size_t DESC_MAX = 8192;
    static constexpr size_t EST_CACHED_BYTES = DESC_MEDIAN + 256;  // median entity + wrapper overhead

    int threads = 6;
    if (auto* env = std::getenv("BENCH_THREADS"))
        if (int v = std::atoi(env); v > 0) threads = v;

    size_t est_capacity = max_mem / EST_CACHED_BYTES;

    // Ratios: keyspace as fraction of estimated cache capacity.
    // ratio < 1.0 → everything fits (100% hit). ratio > 1.0 → eviction pressure.
    // Fine-grained sweep: few MB in cache → cache full → beyond (miss pressure).
    std::vector<double> ratios = {
        0.001, 0.005, 0.01, 0.05, 0.10, 0.25, 0.50, 0.75,
        1.0, 1.25, 1.5, 2.0
    };
    size_t max_keys = static_cast<size_t>(est_capacity * ratios.back());
    if (max_keys < 100) max_keys = 100;

    WARN("\n  Cache budget: " << max_mem / 1024 / 1024 << " MB  |  "
         << "est. capacity: " << est_capacity << " entities  |  "
         << "max keyspace: " << max_keys << "  |  "
         << threads << " threads");

    // --- Bulk insert with variable-size descriptions ---
    // Size per row: 64 + (hash(i) % (8192-64)).  Pseudo-random, deterministic.
    auto t_insert = Clock::now();
    auto bulk = execQueryArgs(
        "INSERT INTO relais_test_items (name, value, description, is_active) "
        "SELECT 'realism_' || i, i::int4, "
        "  repeat('x', $1::int + abs(hashint4(i::int4)) % ($2::int - $1::int + 1)), "
        "  true "
        "FROM generate_series(0, $3::int) AS i "
        "RETURNING id",
        static_cast<int>(DESC_MIN), static_cast<int>(DESC_MAX),
        static_cast<int>(max_keys - 1));
    auto insert_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - t_insert).count();

    std::vector<int64_t> ids;
    ids.reserve(max_keys);
    for (int r = 0; r < bulk.rows(); ++r)
        ids.push_back(bulk[r].get<int64_t>(0));

    WARN("  Inserted " << ids.size() << " entities in " << insert_ms << " ms\n");

    // --- Header ---
    WARN(std::string(74, '='));
    WARN("  ratio   | keys      | hit ratio | throughput     | sweeps");
    WARN(std::string(74, '-'));

    static constexpr double ZIPF_ALPHA = 0.80;

    for (double ratio : ratios) {
        int num_keys = static_cast<int>(est_capacity * ratio);
        if (num_keys < 10) continue;
        if (num_keys > static_cast<int>(ids.size())) num_keys = static_cast<int>(ids.size());

        // Build shared Zipf CDF (read-only, ~num_keys * 8 bytes)
        ZipfCDF cdf(num_keys, ZIPF_ALPHA);

        // Reset cache + GDSF state
        TI::resetCacheForGDSF<L1TestItemRepo>();
        L1TestItemRepo::resetMetrics();
#if RELAIS_ENABLE_METRICS
        policy.sweepCounters().reset();
#endif

        // Warm up: Zipf-weighted, enough to fill cache with realistic working set.
        // num_keys * 3 samples exercises the full Zipf tail.
        {
            std::mt19937_64 warmup_rng(42);
            size_t warmup_count = std::min(static_cast<size_t>(num_keys) * 3,
                                           static_cast<size_t>(2'000'000));
            for (size_t i = 0; i < warmup_count; ++i)
                sync(L1TestItemRepo::find(ids[cdf.sample(warmup_rng)]));
        }

        // Production workload — N real threads, each with Zipf sampling.
        // sync(Repo::find()) dispatches to the IO event loop; L1 hits resolve
        // immediately (no suspension), misses go to DB asynchronously.
        L1TestItemRepo::resetMetrics();
#if RELAIS_ENABLE_METRICS
        policy.sweepCounters().reset();
#endif

        auto result = measureDuration(threads, [&](int tid, std::atomic<bool>& running) -> int64_t {
            std::mt19937_64 rng(static_cast<uint64_t>(tid) * 2654435761ULL + 42);
            std::mt19937 op_rng(tid * 42 + 7);
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto idx = cdf.sample(rng);
                auto kid = ids[idx];
                auto roll = op_rng() % 100;
                if (roll >= 2) {  // 98% reads
                    doNotOptimize(sync(L1TestItemRepo::find(kid)));
                } else if (roll == 1) {  // 1% L1 evictions
                    L1TestItemRepo::evict(kid);
                } else {  // 1% full invalidations
                    sync(L1TestItemRepo::invalidate(kid));
                }
                ++ops;
            }
            return ops;
        });

        auto m = L1TestItemRepo::metrics();
        double hr = m.l1HitRatio() * 100.0;
        double ops_s = (result.total_ops > 0)
            ? result.total_ops * 1'000'000.0
              / std::chrono::duration_cast<std::chrono::microseconds>(result.elapsed).count()
            : 0.0;

        // Sweep info
        std::string sweep_info = "-";
#if RELAIS_ENABLE_METRICS
        auto& sc = policy.sweepCounters();
        auto sweep_count = sc.count.load(std::memory_order_relaxed);
        if (sweep_count > 0) {
            auto total_ns = sc.total_ns.load(std::memory_order_relaxed);
            double avg_us = static_cast<double>(total_ns) / sweep_count / 1000.0;
            std::ostringstream ss;
            ss << sweep_count << " (avg " << std::fixed << std::setprecision(0) << avg_us << "us)";
            sweep_info = ss.str();
        }
#endif

        // Format ratio
        std::ostringstream line;
        line << "  " << std::fixed << std::setprecision(2) << ratio
             << "    | " << std::setw(9) << num_keys
             << " | " << std::setw(7) << std::setprecision(1) << hr << "%"
             << "  | " << std::setw(14) << fmtOps(ops_s)
             << " | " << sweep_info;
        WARN(line.str());
    }

    WARN(std::string(74, '='));
}


// #############################################################################
//
//  12. findMany — batched multi-id read vs N × find
//
//  findMany(N) collapses N point lookups into one guarded MultiView:
//    - all-L1-hit  → zero-copy, synchronous (await_ready), no coroutine frame
//    - all-miss    → ONE batched round-trip per tier (L2 MGET, L3 ANY)
//                    vs N sequential round-trips for N × find
//    - mixed       → only the misses pay I/O, batched
//
//  The round-trip win is read directly off latency: N × find scales linearly
//  with N (sequential RTTs), findMany stays ~flat (one batch). FullCache repo
//  (L1+L2) exercises the multi-tier batch; L1-only isolates the hot path.
//
// #############################################################################

namespace {

// Drain detached L2 warm-fills so the next sample's setup starts from a known
// state. invalidate() clears L1+L2 synchronously for the requested ids.
template<typename Repo>
void invalidateAll(const std::vector<int64_t>& ids) {
    for (auto id : ids) sync(Repo::invalidate(id));
}

template<typename Repo>
void primeAll(const std::vector<int64_t>& ids) {
    for (auto id : ids) sync(Repo::find(id));
}

// findMany over the first N ids; returns the view so the caller can sink it.
template<typename Repo>
auto findManyN(const std::vector<int64_t>& ids, int n) {
    return sync(Repo::findMany(std::span<const int64_t>(ids.data(), n)));
}

// N sequential point lookups — the baseline findMany replaces.
template<typename Repo>
void nFinds(const std::vector<int64_t>& ids, int n) {
    for (int k = 0; k < n; ++k)
        doNotOptimize(sync(Repo::find(ids[k])));
}

} // anonymous namespace

TEST_CASE("Benchmark - findMany batched read", "[benchmark][findmany]") {
    TransactionGuard tx;
    WARN(gdsf_banner());

    static constexpr int POOL = 128;
    const std::vector<int> Ns = {1, 8, 32, 128};

    std::vector<int64_t> ids;
    ids.reserve(POOL);
    for (int i = 0; i < POOL; ++i)
        ids.push_back(insertTestUser(
            "fm_bench_" + std::to_string(i),
            "fmb" + std::to_string(i) + "@test.com", i));

    // --- A. all-L1-hit fast path (ns-scale → duration-based) -----------------
    // Prime L1, then confirm findMany(N) resolves synchronously (no frame pool
    // churn) before measuring. Compares zero-copy batch vs N × find.
    primeAll<L1TestUserRepo>(ids);
    {
        auto imm = L1TestUserRepo::findMany(std::span<const int64_t>(ids.data(), POOL));
        REQUIRE(imm.await_ready());   // all-hit → synchronous, no coroutine frame
        doNotOptimize(sync(std::move(imm)));
    }

    for (int n : Ns) {
        auto fm = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                doNotOptimize(findManyN<L1TestUserRepo>(ids, n));
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput(
            "L1 hit  findMany(" + std::to_string(n) + ")", 1, fm));

        auto nf = measureDuration(1, [&](int, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                nFinds<L1TestUserRepo>(ids, n);
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput(
            "L1 hit  " + std::to_string(n) + " x find", 1, nf));
    }

    // --- B. all-miss cold path (μs/ms-scale → sample-based latency) ----------
    // FullCache (L1+L2): findMany issues one MGET + one ANY; N × find issues N
    // sequential round-trips. invalidate() between samples re-arms the miss.
    {
        std::vector<BenchResult> miss;
        for (int n : Ns) {
            miss.push_back(benchWithSetup(
                "findMany(" + std::to_string(n) + ")",
                [&]() { invalidateAll<FullCacheTestUserRepo>(ids); },
                [&]() { doNotOptimize(findManyN<FullCacheTestUserRepo>(ids, n)); }));
            miss.push_back(benchWithSetup(
                std::to_string(n) + " x find",
                [&]() { invalidateAll<FullCacheTestUserRepo>(ids); },
                [&]() { nFinds<FullCacheTestUserRepo>(ids, n); }));
        }
        WARN(formatTable("findMany all-miss (L1+L2 cold, 1 batch vs N RTT)", miss));
    }

    // --- C. mixed path (half in L1, half cold) -------------------------------
    // Only the misses pay I/O — batched into a single round-trip per tier.
    {
        std::vector<BenchResult> mixed;
        for (int n : Ns) {
            int half = n / 2;
            mixed.push_back(benchWithSetup(
                "findMany(" + std::to_string(n) + ")",
                [&]() {
                    invalidateAll<FullCacheTestUserRepo>(ids);
                    for (int k = 0; k < half; ++k)
                        sync(FullCacheTestUserRepo::find(ids[k]));   // warm half
                },
                [&]() { doNotOptimize(findManyN<FullCacheTestUserRepo>(ids, n)); }));
            mixed.push_back(benchWithSetup(
                std::to_string(n) + " x find",
                [&]() {
                    invalidateAll<FullCacheTestUserRepo>(ids);
                    for (int k = 0; k < half; ++k)
                        sync(FullCacheTestUserRepo::find(ids[k]));
                },
                [&]() { nFinds<FullCacheTestUserRepo>(ids, n); }));
        }
        WARN(formatTable("findMany mixed (50% L1 hit, misses batched)", mixed));
    }
}


// #############################################################################
//
//  13. eraseMany / invalidateMany — batched write vs N × mono
//
//  Both collapse N enumerated keys into one cascade:
//    - eraseMany(N)      → ONE  DELETE WHERE pk = ANY($1) RETURNING  (L3, one
//                          round-trip) + ONE batched UNLINK (L2), vs N sequential
//                          DELETE + N UNLINK for N × erase.
//    - invalidateMany(N) → ONE  variadic UNLINK sub-chunked at K_redis (L2), vs N
//                          UNLINK for N × invalidate. Rows persist (evict only).
//
//  FullCache (L1+L2) exercises the multi-tier batch; the win is read directly off
//  latency — N × mono scales linearly with N (sequential RTTs), batch stays flat.
//
// #############################################################################

TEST_CASE("Benchmark - batch erase / invalidate", "[benchmark][batch]") {
    TransactionGuard tx;
    WARN(gdsf_banner());

    static constexpr int POOL = 128;
    const std::vector<int> Ns = {8, 32, 128};

    // --- A. eraseMany(N) vs N × erase — FullCache cold (L1+L2 primed, deleted) --
    // Rows are consumed each sample → re-insert + re-prime in setup (unmeasured).
    {
        std::vector<int64_t> live;
        live.reserve(POOL);

        auto reinsert = [&](int n) {
            live.clear();
            for (int i = 0; i < n; ++i) {
                auto id = insertTestUser(
                    "em_bench_" + std::to_string(i),
                    "emb" + std::to_string(i) + "@test.com", i);
                sync(FullCacheTestUserRepo::find(id));   // prime L1+L2
                live.push_back(id);
            }
        };

        std::vector<BenchResult> erase;
        for (int n : Ns) {
            erase.push_back(benchWithSetup(
                "eraseMany(" + std::to_string(n) + ")",
                [&, n]() { reinsert(n); },
                [&]() { doNotOptimize(sync(FullCacheTestUserRepo::eraseMany(
                            std::span<const int64_t>(live)))); }));
            erase.push_back(benchWithSetup(
                std::to_string(n) + " x erase",
                [&, n]() { reinsert(n); },
                [&]() {
                    for (auto id : live)
                        doNotOptimize(sync(FullCacheTestUserRepo::erase(id)));
                }));
        }
        WARN(formatTable("eraseMany vs N x erase (L1+L2, 1 DELETE ANY vs N RTT)", erase));
    }

    // --- B. invalidateMany(N) vs N × invalidate — FullCache warm (rows persist) -
    // invalidateMany batches the L2 UNLINK; rows are NOT deleted → fixed pool,
    // re-prime L1+L2 in setup.
    {
        std::vector<int64_t> ids;
        ids.reserve(POOL);
        for (int i = 0; i < POOL; ++i)
            ids.push_back(insertTestUser(
                "im_bench_" + std::to_string(i),
                "imb" + std::to_string(i) + "@test.com", i));

        auto prime = [&](int n) {
            for (int k = 0; k < n; ++k) sync(FullCacheTestUserRepo::find(ids[k]));
        };

        std::vector<BenchResult> inval;
        for (int n : Ns) {
            inval.push_back(benchWithSetup(
                "invalidateMany(" + std::to_string(n) + ")",
                [&, n]() { prime(n); },
                [&, n]() { sync(FullCacheTestUserRepo::invalidateMany(
                            std::span<const int64_t>(ids.data(), n))); }));
            inval.push_back(benchWithSetup(
                std::to_string(n) + " x invalidate",
                [&, n]() { prime(n); },
                [&, n]() {
                    for (int k = 0; k < n; ++k)
                        sync(FullCacheTestUserRepo::invalidate(ids[k]));
                }));
        }
        WARN(formatTable("invalidateMany vs N x invalidate (L1+L2, batched UNLINK)", inval));
    }
}


// #############################################################################
//
//  14. eraseWhere / invalidateWhere — predicate-driven batch
//
//  The predicate resolves its own affected set server-side, so the caller never
//  enumerates ids:
//    - invalidateWhere(P) → ONE SELECT WHERE P (resolve) + the same cascade
//      invalidateMany runs over explicit ids. The delta vs invalidateMany(ids)
//      is exactly the resolve round-trip — priced against not knowing the ids.
//    - eraseWhere(P)      → ONE DELETE WHERE P RETURNING (resolve + delete fused)
//      + ONE RangeModification (L1 own-list) + ONE predicate EVAL (L2 own-list),
//      O(1)/O(groups), regardless of matched-set size.
//
// #############################################################################

TEST_CASE("Benchmark - predicate erase / invalidate (where)", "[benchmark][where]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    static constexpr int M = 64;

    // Author A — persistent set for invalidateWhere (no delete).
    auto A = insertTestUser("where_bench_a", "where_bench_a@test.com", 0);
    std::vector<int64_t> articleIds;
    articleIds.reserve(M);
    for (int i = 0; i < M; ++i)
        articleIds.push_back(insertTestArticle(
            "tech", A, "WB_" + std::to_string(i), i * 10));

    // --- invalidateWhere vs invalidateMany(ids) — isolate the resolve cost ------
    {
        auto primeGroup = [&]() {
            sync(TestArticleListRepo::query(makeArticleQuery(std::nullopt, A, 100)));
            for (auto id : articleIds) sync(TestArticleListRepo::find(id));
        };
        std::vector<BenchResult> inval;
        inval.push_back(benchWithSetup("invalidateWhere({author})",
            primeGroup,
            [&]() { sync(TestArticleListRepo::invalidateWhere({.author_id = A})); }));
        inval.push_back(benchWithSetup("invalidateMany(ids)",
            primeGroup,
            [&]() { sync(TestArticleListRepo::invalidateMany(
                        std::span<const int64_t>(articleIds))); }));
        WARN(formatTable("invalidateWhere vs invalidateMany (resolve round-trip cost)", inval));
    }

    // --- eraseWhere absolute latency — author B, re-inserted each sample --------
    {
        auto B = insertTestUser("where_bench_b", "where_bench_b@test.com", 0);
        std::vector<int64_t> live;
        live.reserve(M);
        auto reinsert = [&]() {
            live.clear();
            for (int i = 0; i < M; ++i)
                live.push_back(insertTestArticle("tech", B, "EW_" + std::to_string(i), i * 10));
            sync(TestArticleListRepo::query(makeArticleQuery(std::nullopt, B, 100)));
            for (auto id : live) sync(TestArticleListRepo::find(id));
        };
        std::vector<BenchResult> erase;
        erase.push_back(benchWithSetup(
            "eraseWhere({author}) [" + std::to_string(M) + " rows]",
            reinsert,
            [&]() { doNotOptimize(sync(TestArticleListRepo::eraseWhere({.author_id = B}))); }));
        WARN(formatTable("eraseWhere predicate delete (1 DELETE RETURNING + 1 RangeMod + 1 EVAL)", erase));
    }
}


// #############################################################################
//
//  15. Detached cleanup (commit 13) — caller throughput / time-to-return
//
//  eraseMany/eraseWhere split the batch cleanup into a CRITICAL pass (awaited:
//  L3 DELETE + L1 evict + L2 entity UNLINK + gen bump + L1 list bump) and a
//  DEFERRED pass (fired fire-and-forget: cross-target + the L2 list EVALs). The
//  caller returns after the critical pass; the deferred Redis work drains in the
//  background, l2_ttl-bounded (cache-staleness tolerance).
//
//  This isolates what detachment buys, on the WithLists=true cascade — the only
//  path with a non-trivial deferred half (invalidateMany uses WithLists=false,
//  so its deferred pass is empty). Rows persist (invalidate semantics), so the
//  affected set is reusable across samples without re-insert:
//    A. latency    — drain (critical+deferred awaited) vs detached (critical
//                    only) → the per-call latency the L2 list EVAL no longer
//                    adds to the caller's path.
//    B. throughput — K concurrent coroutines issuing the cascade in a loop on
//                    one event loop. Detached lets call N+1 start while N's EVALs
//                    are in flight → concurrent EVALs coalesce into fewer Redis
//                    round-trips. Reported as caller-observed op rate (critical
//                    completions); deferred EVALs trail in the background.
//
//  Local L1+L2+lists repo so the deferred pass hits Redis (the detachable work).
//
//  Run with:
//    ./bench_relais_cache "[batch-detach]"
//    BENCH_DURATION_S=10 ./bench_relais_cache "[batch-detach]"
//
// #############################################################################

/// L1+L2 + auto-detected ListMixin (TestArticleEntity carries a ListDescriptor).
using FullCacheArticleListRepo =
    Repo<TestArticleEntity, "bench:article:both", cfg::Both>;

TEST_CASE("Benchmark - detached batch cleanup", "[benchmark][batch-detach]") {
    using Repo = FullCacheArticleListRepo;

    TransactionGuard tx;
    TestInternals::resetListCacheState<Repo>();
    WARN(gdsf_banner());

    static constexpr int POOL = 128;
    const std::vector<int> Ns = {8, 32, 128};

    // Shared author + article pool. Rows persist (invalidate, no delete), so the
    // same set is re-primed and re-evicted every sample.
    auto author = insertTestUser("detach_bench", "detach@test.com", 0);
    std::vector<int64_t> ids;
    std::vector<TestArticleEntity> entities;
    ids.reserve(POOL);
    entities.reserve(POOL);
    for (int i = 0; i < POOL; ++i) {
        auto id = insertTestArticle("tech", author, "DT_" + std::to_string(i), i * 10);
        ids.push_back(id);
        auto v = sync(Repo::find(id));            // materialize entity + warm L1+L2
        REQUIRE(v != nullptr);
        entities.push_back(*v);
    }

    // Local list query for THIS repo's Descriptor — makeArticleQuery builds for
    // TestArticleListRepo, whose Descriptor (parameterized by repo Name) differs.
    auto makeQuery = [&](uint16_t limit) {
        namespace ld = jcailloux::relais::list::spec;
        using Desc = Repo::ListDescriptorType;
        ld::ListQueryParams<Desc> p;
        p.limit = limit;
        p.filters.template get<"author_id">() = author;
        p.filters.template get<"category">() = std::string("tech");
        return ld::seal<Desc>(std::move(p));
    };

    // Re-prime the list page + the first n entity entries (unmeasured setup).
    auto reprime = [&](int n) {
        sync(Repo::query(makeQuery(POOL)));
        for (int k = 0; k < n; ++k) sync(Repo::find(ids[k]));
    };

    // --- A. latency: drain vs detached -------------------------------------
    // Both arms borrow the same span over entities[0..n) (symmetric caller cost).
    // drain awaits the L2 list EVAL; detached fires it and returns. Delta ≈ the
    // EVAL round-trip removed from the caller path.
    {
        std::vector<BenchResult> lat;
        for (int n : Ns) {
            std::span<const TestArticleEntity> setN(entities.data(), n);
            lat.push_back(benchWithSetup(
                "drain(" + std::to_string(n) + ")",
                [&, n]() { reprime(n); },
                [&, setN]() { sync(TestInternals::invalidateManyImpl<Repo>(setN)); }));
            lat.push_back(benchWithSetup(
                "detached(" + std::to_string(n) + ")",
                [&, n]() { reprime(n); },
                [&, setN]() { sync(TestInternals::invalidateManyDetached<Repo>(setN)); }));
        }
        WARN(formatTable("detached cleanup — time-to-return vs full drain", lat));
    }

    // --- B. cascade throughput sentinel ------------------------------------
    // A single regression number: caller op rate (critical completion) of the
    // detached cascade under saturated concurrency on one event loop. At this
    // batch size the critical pass is CPU-bound on the loop thread (N L1 evicts +
    // N list-tracker bumps + the deferred copy per op), so concurrency does NOT
    // scale it — that is expected. The I/O-bound pipelining curve (distinct keys,
    // super-linear Redis batching) is bench_io_batch's "[batch][scaling]"; this
    // tracks the FULL facade-cascade cost (L1 + list bookkeeping + fire) that
    // bench_io_batch's raw-Redis path does not see.
    {
        static constexpr int CORO = 32;
        static constexpr int N = 32;
        std::span<const TestArticleEntity> setN(entities.data(), N);

        std::atomic<bool> running{true};
        std::atomic<int64_t> ops{0};
        std::latch done{CORO};
        auto t0 = Clock::now();
        for (int c = 0; c < CORO; ++c) {
            detail::testLoop().dispatch([&, setN]() {
                [](std::span<const TestArticleEntity> set,
                   std::atomic<bool>& running, std::atomic<int64_t>& ops,
                   std::latch& done) -> DetachedHandle {
                    while (running.load(std::memory_order_relaxed)) {
                        co_await TestInternals::invalidateManyDetached<Repo>(set);
                        ops.fetch_add(1, std::memory_order_relaxed);
                    }
                    done.count_down();
                }(setN, running, ops, done);
            });
        }
        std::this_thread::sleep_for(std::chrono::seconds(benchDurationSeconds()));
        running.store(false, std::memory_order_relaxed);
        done.wait();
        auto el = Clock::now() - t0;
        // Settle fired deferreds before TransactionGuard flush.
        for (int s = 0; s < 4; ++s) sync(Repo::query(makeQuery(1)));

        int64_t cascade_ops = ops.load(std::memory_order_relaxed);
        DurationResult result{el, cascade_ops};
        auto base = formatDurationThroughput(
            "detached cascade throughput (" + std::to_string(CORO)
                + " coros, " + std::to_string(N) + " entities/op, single loop)",
            1, result);
        // The op unit is a 32-entity batch; report the per-entity rate too, and
        // flag the single-loop ceiling — the shared-nothing model scales by adding
        // loops (one per core), not threads per loop.
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(el).count();
        double entity_s = (us > 0) ? cascade_ops * N * 1'000'000.0 / us : 0.0;
        std::ostringstream extra;
        extra << "\n  per-entity:      " << fmtOps(entity_s)
              << " (cascade x " << N << " entities)"
              << "\n  note:            single event loop; ~Nx with N per-core loops"
              << " (cf. bench_io_batch [batch][scaling] for I/O pipelining)";
        WARN(base + extra.str());
    }
}


// #############################################################################
//
//  16. Tier matrix — read latency / throughput per cache tier, isolated
//
//  One repo per tier, each measured on its own (no cross-pollution: every Repo
//  type owns a distinct L1 cache + Redis prefix, so warming one never warms
//  another). Reads target DISTINCT keys (stride) so L2/DB reads actually pipeline
//  through the BatchScheduler instead of coalescing onto one key.
//
//    DB only (Uncached) — every find hits PostgreSQL.
//    L2 only (Redis)    — cfg::Redis has no L1, so every find is an L2 hit.
//    L1 only (Local)    — warm, every find is a synchronous RAM hit.
//
//  Per tier, a concurrency sweep on ONE event loop. The "1 coro" row is the
//  sequential rate (latency = 1/throughput); higher rows expose the pipelining
//  win for the I/O tiers. L1 hits resolve synchronously (no suspend), so its
//  curve is flat — concurrency does not apply to a RAM read.
//
//  Run with:
//    ./bench_relais_cache "[tier-matrix]"
//    BENCH_DURATION_S=5 ./bench_relais_cache "[tier-matrix]"
//
// #############################################################################

namespace {

inline double percentile(const std::vector<double>& sorted, double q) {
    if (sorted.empty()) return 0.0;
    auto idx = static_cast<size_t>(q * (sorted.size() - 1));
    return sorted[idx];
}

struct TierStat {
    int concurrency;
    int64_t ops;          // operations completed in the window
    double tput;          // ops/s
    double avg_us;        // wall / ops
    double p50, p90, p99; // µs (0 when not sampled)
    bool sampled;
};

// Concurrent reads on the shared test loop: `coro` DetachedHandle workers, each
// reading DISTINCT keys (stride) so the I/O tiers pipeline through the batcher
// instead of coalescing onto one key. Sample=true times every op (negligible
// Clock overhead vs µs I/O); Sample=false keeps the ns-scale L1 path timer-free
// so its throughput stays real (two Clock::now() would dwarf a ~30ns RAM hit).
template<typename Repo, bool Sample>
TierStat tierReadStat(const std::vector<int64_t>& ids, int coro) {
    static constexpr size_t CAP = 20000;   // max latency samples per coro
    int nkeys = static_cast<int>(ids.size());
    std::atomic<bool> running{true};
    std::atomic<int64_t> ops{0};
    std::latch done{coro};
    std::vector<std::vector<double>> lat(coro);
    if constexpr (Sample) for (auto& v : lat) v.reserve(CAP);

    auto t0 = Clock::now();
    for (int c = 0; c < coro; ++c) {
        detail::testLoop().dispatch([&ids, nkeys, c, &running, &ops, &done, &lat]() {
            [](const std::vector<int64_t>& ids, int nkeys, int c,
               std::atomic<bool>& running, std::atomic<int64_t>& ops,
               std::latch& done, std::vector<double>& mine) -> DetachedHandle {
                int64_t i = c;
                while (running.load(std::memory_order_relaxed)) {
                    if constexpr (Sample) {
                        auto s = Clock::now();
                        auto v = co_await Repo::find(ids[(c * 131 + i++) % nkeys]);
                        auto e = Clock::now();
                        doNotOptimize(v);
                        if (mine.size() < CAP)
                            mine.push_back(
                                std::chrono::duration<double, std::micro>(e - s).count());
                    } else {
                        auto v = co_await Repo::find(ids[(c * 131 + i++) % nkeys]);
                        doNotOptimize(v);
                    }
                    ops.fetch_add(1, std::memory_order_relaxed);
                }
                done.count_down();
            }(ids, nkeys, c, running, ops, done, lat[c]);
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(benchDurationSeconds()));
    running.store(false, std::memory_order_relaxed);
    done.wait();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
        Clock::now() - t0).count();

    int64_t total = ops.load(std::memory_order_relaxed);
    TierStat st{coro, total,
                (us > 0) ? total * 1'000'000.0 / us : 0.0,
                (total > 0) ? static_cast<double>(us) / total : 0.0,
                0.0, 0.0, 0.0, Sample};
    if constexpr (Sample) {
        std::vector<double> all;
        size_t n = 0; for (auto& v : lat) n += v.size();
        all.reserve(n);
        for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
        std::sort(all.begin(), all.end());
        st.p50 = percentile(all, 0.50);
        st.p90 = percentile(all, 0.90);
        st.p99 = percentile(all, 0.99);
    }
    return st;
}

inline void tierRow(std::ostringstream& o, const std::string& head, const TierStat& r) {
    o << "  " << std::left << std::setw(15) << head
      << std::right << std::setw(10) << r.ops
      << std::setw(13) << fmtOps(r.tput)
      << std::setw(9) << fmtDuration(r.avg_us);
    if (r.sampled)
        o << std::setw(9) << fmtDuration(r.p50)
          << std::setw(9) << fmtDuration(r.p90)
          << std::setw(9) << fmtDuration(r.p99);
    else
        o << std::setw(9) << "-" << std::setw(9) << "-" << std::setw(9) << "-";
    o << "\n";
}

inline void tierHeader(std::ostringstream& o, const std::string& col1) {
    o << "  " << std::left << std::setw(15) << col1
      << std::right << std::setw(10) << "ops"
      << std::setw(13) << "throughput"
      << std::setw(9) << "avg"
      << std::setw(9) << "p50"
      << std::setw(9) << "p90"
      << std::setw(9) << "p99" << "\n  " << std::string(74, '-') << "\n";
}

// Per-tier sweep: rows = concurrency levels.
inline std::string tierSweepTable(const std::string& title, int nkeys,
                                  const std::vector<TierStat>& rows) {
    auto bar = std::string(74, '=');
    std::ostringstream o;
    o << "\n  " << bar << "\n  " << title
      << "   (keys=" << nkeys << ", " << benchDurationSeconds() << "s/level)\n  "
      << std::string(74, '-') << "\n";
    tierHeader(o, "concurrency");
    for (const auto& r : rows)
        tierRow(o, std::to_string(r.concurrency) + " coros", r);
    o << "  " << bar;
    return o.str();
}

// Cross-tier comparison at one concurrency level: rows = tiers, side by side.
inline std::string tierCompareTable(int nkeys, int conc,
        const std::vector<std::pair<std::string, TierStat>>& rows) {
    auto bar = std::string(74, '=');
    std::ostringstream o;
    o << "\n  " << bar << "\n  TIER COMPARISON @ concurrency=" << conc
      << "   (keys=" << nkeys << ", " << benchDurationSeconds() << "s)\n  "
      << std::string(74, '-') << "\n";
    tierHeader(o, "tier");
    for (const auto& [label, r] : rows)
        tierRow(o, label, r);
    o << "  " << bar
      << "\n  (L1 latency not sampled: ns-scale, see avg; a per-op timer would distort it)";
    return o.str();
}

} // anonymous namespace

TEST_CASE("Benchmark - tier matrix", "[benchmark][tier-matrix]") {
    TransactionGuard tx;
    WARN(gdsf_banner());

    static constexpr int N = 1000;
    const std::vector<int> levels = {1, 16, 64, 128};

    std::vector<int64_t> ids;
    ids.reserve(N);
    for (int i = 0; i < N; ++i)
        ids.push_back(insertTestItem("tier_" + std::to_string(i), i));

    std::vector<TierStat> db, l2, l1;

    // DB only — no cache to warm; every find is a PostgreSQL round-trip.
    for (int k : levels) db.push_back(tierReadStat<UncachedTestItemRepo, true>(ids, k));
    WARN(tierSweepTable("DB only (Uncached) — find -> PostgreSQL", N, db));

    // L2 only — cfg::Redis, no L1; warm L2 so every find is a Redis hit.
    for (auto id : ids) sync(L2TestItemRepo::find(id));
    for (int k : levels) l2.push_back(tierReadStat<L2TestItemRepo, true>(ids, k));
    WARN(tierSweepTable("L2 only (Redis hit) — find -> Redis GET", N, l2));

    // L1 only — warm RAM; finds resolve synchronously (latency not sampled).
    for (auto id : ids) sync(L1TestItemRepo::find(id));
    for (int k : levels) l1.push_back(tierReadStat<L1TestItemRepo, false>(ids, k));
    WARN(tierSweepTable("L1 only (RAM hit) — synchronous (concurrency N/A)", N, l1));

    // Side-by-side at the top concurrency level — the easy-compare view.
    WARN(tierCompareTable(N, levels.back(), {
        {"L1 (RAM)",    l1.back()},
        {"L2 (Redis)",  l2.back()},
        {"DB (Pg)",     db.back()},
    }));
}