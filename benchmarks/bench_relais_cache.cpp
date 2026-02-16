/**
 * bench_relais_cache.cpp
 *
 * Performance benchmarks for the relais cache hierarchy.
 * Uses a custom micro-benchmarker for consistent, human-readable output
 * regardless of Catch2 reporter (console or XML).
 *
 * Run with:
 *   ./bench_relais_cache                    # all benchmarks
 *   ./bench_relais_cache "[l1]"             # L1 only
 *   ./bench_relais_cache "[throughput]"      # multi-threaded only
 *   BENCH_SAMPLES=500 ./bench_relais_cache  # 500 samples per benchmark
 *   BENCH_DURATION_S=5 ./bench_relais_cache "[throughput]"  # custom duration
 *
 * Covers:
 *   1. L1 cache hit latency (findById, findByIdAsJson)
 *   2. L2 cache hit latency
 *   3. L1+L2 cache hit latency (L1 serves, L2 fallback)
 *   4. Cache miss latency (DB fetch)
 *   5. Write operations (create+remove, update)
 *   6. List query latency (cached)
 *   7. Raw L1 multi-threaded throughput (duration-based, no coroutine overhead)
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
//  1. L1 cache hit latency
//
// #############################################################################

TEST_CASE("Benchmark - L1 cache hit", "[benchmark][l1]")
{
    TransactionGuard tx;
    auto id = insertTestItem("bench_l1", 42);
    sync(L1TestItemRepo::findById(id));

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("findById", [&]() -> io::Task<void> {
        co_await L1TestItemRepo::findById(id);
    })));

    results.push_back(sync(benchAsync("findByIdAsJson", [&]() -> io::Task<void> {
        co_await L1TestItemRepo::findByIdAsJson(id);
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
    sync(L2TestItemRepo::findById(id));

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("findById", [&]() -> io::Task<void> {
        co_await L2TestItemRepo::findById(id);
    })));

    results.push_back(sync(benchAsync("findByIdAsJson", [&]() -> io::Task<void> {
        co_await L2TestItemRepo::findByIdAsJson(id);
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
    sync(FullCacheTestItemRepo::findById(id));

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("findById (L1 serves)", [&]() -> io::Task<void> {
        co_await FullCacheTestItemRepo::findById(id);
    })));

    results.push_back(sync(benchWithSetupAsync("findById (L2 fallback)",
        [&]() -> io::Task<void> { FullCacheTestItemRepo::invalidateL1(id); co_return; },
        [&]() -> io::Task<void> { co_await FullCacheTestItemRepo::findById(id); }
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
        [&]() -> io::Task<void> { L1TestItemRepo::invalidateL1(id); co_return; },
        [&]() -> io::Task<void> { co_await L1TestItemRepo::findById(id); }
    )));

    results.push_back(sync(benchWithSetupAsync("findById (L1+L2 miss -> DB)",
        [&]() -> io::Task<void> { co_await FullCacheTestItemRepo::invalidate(id); },
        [&]() -> io::Task<void> { co_await FullCacheTestItemRepo::findById(id); }
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
    sync(L1TestItemRepo::findById(upd_id));
    int c1 = 0;

    auto upd_both_id = insertTestItem("bench_upd_both", 42);
    sync(FullCacheTestItemRepo::findById(upd_both_id));
    int c2 = 0;

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("create + remove (L1)", [&]() -> io::Task<void> {
        auto entity = makeTestItem("bench_cr", 42);
        auto created = co_await L1TestItemRepo::create(entity);
        if (created) co_await L1TestItemRepo::remove(created->id);
    })));

    results.push_back(sync(benchAsync("update (L1)", [&]() -> io::Task<void> {
        ++c1;
        auto entity = makeTestItem(
            "bench_u_" + std::to_string(c1), c1,
            "bench_u_description", true, upd_id);
        co_await L1TestItemRepo::update(upd_id, entity);
    })));

    results.push_back(sync(benchAsync("update (L1+L2)", [&]() -> io::Task<void> {
        ++c2;
        auto entity = makeTestItem(
            "bench_ub_" + std::to_string(c2), c2,
            "bench_ub_description", true, upd_both_id);
        co_await FullCacheTestItemRepo::update(upd_both_id, entity);
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
    TestInternals::resetListCacheState<TestArticleListRepo>();

    auto userId = insertTestUser("bench_author", "bench@test.com", 0);
    for (int i = 0; i < 10; ++i) {
        insertTestArticle("bench_cat", userId, "Article_" + std::to_string(i), i * 10);
    }

    auto query = makeArticleQuery("bench_cat");
    sync(TestArticleListRepo::query(query));

    std::vector<BenchResult> results;

    results.push_back(sync(benchAsync("query (10 articles, L1 hit)", [&]() -> io::Task<void> {
        co_await TestArticleListRepo::query(query);
    })));

    WARN(formatTable("List query", results));
}


// #############################################################################
//
//  7. Multi-threaded throughput (duration-based, default 5s)
//
// #############################################################################

// #############################################################################
//
//  7a. Raw L1 cache throughput — duration-based (sustained measurement)
//
// #############################################################################

TEST_CASE("Benchmark - L1 raw throughput", "[benchmark][throughput][raw]")
{
    TransactionGuard tx;

    static constexpr int THREADS = 6;
    static constexpr int NUM_KEYS = 64;

    // Insert and warm cache
    std::vector<int64_t> ids;
    ids.reserve(NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; ++i) {
        auto kid = insertTestItem("bench_raw_dur_" + std::to_string(i), i);
        sync(L1TestItemRepo::findById(kid));
        ids.push_back(kid);
    }

    SECTION("L1 raw — single key (contention)") {
        auto id = ids[0];
        auto result = measureDuration(THREADS, [&](int, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto ptr = TestInternals::getFromCache<L1TestItemRepo>(id);
                doNotOptimize(ptr);
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("L1 raw (single key)", THREADS, result));
    }

    SECTION("L1 raw — distributed keys (parallel)") {
        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto ptr = TestInternals::getFromCache<L1TestItemRepo>(
                    ids[(tid * 1000000 + ops) % NUM_KEYS]);
                doNotOptimize(ptr);
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("L1 raw (distributed)", THREADS, result));
    }

    SECTION("L1 raw — findByIdAsJson distributed") {
        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto ptr = TestInternals::getFromCache<L1TestItemRepo>(
                    ids[(tid * 1000000 + ops) % NUM_KEYS]);
                if (ptr) doNotOptimize(ptr->toJson());
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("L1 raw findByIdAsJson (distributed)", THREADS, result));
    }

    SECTION("L1 raw — mixed read/write distributed (75R/25W)") {
        auto template_ptr = TestInternals::getFromCache<L1TestItemRepo>(ids[0]);
        REQUIRE(template_ptr != nullptr);

        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            std::mt19937 rng(tid * 42 + 7);
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto kid = ids[(tid * 1000000 + ops) % NUM_KEYS];
                if (rng() % 4 != 0) {
                    auto ptr = TestInternals::getFromCache<L1TestItemRepo>(kid);
                    doNotOptimize(ptr);
                } else {
                    TestInternals::invalidateL1<L1TestItemRepo>(kid);
                    TestInternals::putInCache<L1TestItemRepo>(kid, template_ptr);
                }
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("L1 raw mixed (distributed, 75R/25W)", THREADS, result));
    }
}


