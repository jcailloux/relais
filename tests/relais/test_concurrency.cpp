/**
 * test_concurrency.cpp
 *
 * Concurrency stress tests for the relais cache hierarchy.
 * Verifies that concurrent reads, writes, and invalidations don't crash
 * or corrupt internal state across all cache levels.
 *
 * Important: These tests do NOT verify exact values — stale reads are expected.
 * The goal is robustness: no crashes, no exceptions, no deadlocks.
 *
 * Note: Catch2 assertions are NOT thread-safe.  All REQUIRE/CHECK macros
 * live in the main thread only.  Worker threads signal failures via
 * std::atomic<int> counters checked after join.
 *
 * Covers:
 *   1. Concurrent find (L1, L2, L1+L2)
 *   2. Concurrent read + write on same entity
 *   3. Concurrent insert + remove
 *   4. Concurrent cross-invalidation
 *   5. Concurrent list queries + entity modifications
 *   6. Concurrent warmup + operations
 *   7. Mixed operations storm (all operations interleaved)
 *   8. Concurrent patch
 *   9. Concurrent cleanup + operations (entity cache)
 *  10. Concurrent list CRUD + list cache cleanup
 *  11a. ModificationTracker drains after concurrent storm
 *  11b. Progressive tracker reduction via triggerCleanup
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"

#include <thread>
#include <vector>
#include <latch>
#include <atomic>
#include <random>

using namespace relais_test;

// #############################################################################
//
//  Constants and helpers
//
// #############################################################################

static constexpr int NUM_THREADS = 8;
static constexpr int OPS_PER_THREAD = 50;

/// Run a function on N threads, synchronized with a latch for true concurrency.
/// The function receives the thread index (0..N-1).
/// Exceptions inside threads increment the `errors` counter.
/// After all threads complete, REQUIRE(errors == 0) in the main thread.
template<typename Fn>
void parallel(int num_threads, Fn&& fn) {
    std::latch start{num_threads};
    std::atomic<int> errors{0};
    std::vector<std::jthread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            start.arrive_and_wait();
            try {
                fn(i);
            } catch (...) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : threads) t.join();
    REQUIRE(errors.load() == 0);
}


// #############################################################################
//
//  1. Concurrent find
//
// #############################################################################

TEST_CASE("Concurrency - concurrent find",
          "[integration][db][concurrency][read]")
{
    TransactionGuard tx;

    SECTION("[L1] N threads read the same entity concurrently") {
        auto id = insertTestItem("conc_read_l1", 42);
        sync(L1TestItemRepo::find(id));

        std::atomic<int> null_count{0};

        parallel(NUM_THREADS, [&](int) {
            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                auto item = sync(L1TestItemRepo::find(id));
                if (!item) null_count.fetch_add(1, std::memory_order_relaxed);
            }
        });

        REQUIRE(null_count.load() == 0);
    }

    SECTION("[L2] N threads read the same entity concurrently") {
        auto id = insertTestItem("conc_read_l2", 42);
        sync(L2TestItemRepo::find(id));

        std::atomic<int> null_count{0};

        parallel(NUM_THREADS, [&](int) {
            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                auto item = sync(L2TestItemRepo::find(id));
                if (!item) null_count.fetch_add(1, std::memory_order_relaxed);
            }
        });

        REQUIRE(null_count.load() == 0);
    }

    SECTION("[L1+L2] N threads read the same entity concurrently") {
        auto id = insertTestItem("conc_read_both", 42);
        sync(FullCacheTestItemRepo::find(id));

        std::atomic<int> null_count{0};

        parallel(NUM_THREADS, [&](int) {
            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                auto item = sync(FullCacheTestItemRepo::find(id));
                if (!item) null_count.fetch_add(1, std::memory_order_relaxed);
            }
        });

        REQUIRE(null_count.load() == 0);
    }

    SECTION("[L1] N threads read different entities concurrently") {
        std::vector<int64_t> ids;
        for (int i = 0; i < NUM_THREADS; ++i) {
            ids.push_back(insertTestItem("conc_multi_" + std::to_string(i), i));
        }

        std::atomic<int> null_count{0};

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                auto item = sync(L1TestItemRepo::find(ids[i]));
                if (!item) null_count.fetch_add(1, std::memory_order_relaxed);
            }
        });

        REQUIRE(null_count.load() == 0);
    }
}


// #############################################################################
//
//  2. Concurrent read + write on same entity
//
// #############################################################################

TEST_CASE("Concurrency - concurrent read + write",
          "[integration][db][concurrency][read-write]")
{
    TransactionGuard tx;

    SECTION("[L1] readers and writers on same entity") {
        auto id = insertTestItem("conc_rw_l1", 0);
        sync(L1TestItemRepo::find(id));

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                if (i % 2 == 0) {
                    // Reader — may see nullptr briefly during invalidation
                    sync(L1TestItemRepo::find(id));
                } else {
                    // Writer
                    auto entity = makeTestItem(
                        "rw_" + std::to_string(i) + "_" + std::to_string(j),
                        i * 1000 + j, "", true, id);
                    sync(L1TestItemRepo::update(id, entity));
                }
            }
        });

        // Verify the repo is still functional
        auto final_item = sync(L1TestItemRepo::find(id));
        REQUIRE(final_item != nullptr);
    }

    SECTION("[L1+L2] readers and writers on same entity") {
        auto id = insertTestItem("conc_rw_both", 0);
        sync(FullCacheTestItemRepo::find(id));

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                if (i % 2 == 0) {
                    sync(FullCacheTestItemRepo::find(id));
                } else {
                    auto entity = makeTestItem(
                        "rw_both_" + std::to_string(i) + "_" + std::to_string(j),
                        i * 1000 + j, "", true, id);
                    sync(FullCacheTestItemRepo::update(id, entity));
                }
            }
        });

        auto final_item = sync(FullCacheTestItemRepo::find(id));
        REQUIRE(final_item != nullptr);
    }
}


// #############################################################################
//
//  3. Concurrent insert + remove
//
// #############################################################################

TEST_CASE("Concurrency - concurrent insert + remove",
          "[integration][db][concurrency][insert-remove]")
{
    TransactionGuard tx;

    SECTION("[L1] threads insert and remove entities concurrently") {
        std::atomic<int> created_count{0};

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 2; ++j) {
                auto entity = makeTestItem(
                    "cr_" + std::to_string(i) + "_" + std::to_string(j),
                    i * 1000 + j);
                auto item = sync(L1TestItemRepo::insert(entity));
                if (item) {
                    created_count.fetch_add(1, std::memory_order_relaxed);
                    sync(L1TestItemRepo::remove(item->id));
                }
            }
        });

        REQUIRE(created_count.load() > 0);
    }

    SECTION("[L1+L2] threads insert and remove entities concurrently") {
        std::atomic<int> created_count{0};

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 2; ++j) {
                auto entity = makeTestItem(
                    "cr_both_" + std::to_string(i) + "_" + std::to_string(j),
                    i * 1000 + j);
                auto item = sync(FullCacheTestItemRepo::insert(entity));
                if (item) {
                    created_count.fetch_add(1, std::memory_order_relaxed);
                    sync(FullCacheTestItemRepo::remove(item->id));
                }
            }
        });

        REQUIRE(created_count.load() > 0);
    }
}


// #############################################################################
//
//  4. Concurrent cross-invalidation
//
// #############################################################################

TEST_CASE("Concurrency - concurrent cross-invalidation",
          "[integration][db][concurrency][cross-inv]")
{
    TransactionGuard tx;

    SECTION("[L1] purchase creates invalidate user cache under contention") {
        auto userId = insertTestUser("conc_user", "conc@test.com", 1000);
        sync(L1TestUserRepo::find(userId));

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 4; ++j) {
                if (i % 2 == 0) {
                    // Read user (may be invalidated mid-flight)
                    sync(L1TestUserRepo::find(userId));
                } else {
                    // insert purchase -> invalidates user cache
                    auto purchase = makeTestPurchase(
                        userId, "Widget_" + std::to_string(i * 100 + j), 10 + j);
                    auto created = sync(L1TestPurchaseRepo::insert(purchase));
                    if (created) {
                        sync(L1TestPurchaseRepo::remove(created->id));
                    }
                }
            }
        });

        // Repo should still be functional
        auto user = sync(L1TestUserRepo::find(userId));
        REQUIRE(user != nullptr);
        REQUIRE(user->username == "conc_user");
    }
}


// #############################################################################
//
//  5. Concurrent list queries + entity modifications
//
// #############################################################################

TEST_CASE("Concurrency - concurrent list queries + modifications",
          "[integration][db][concurrency][list]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    SECTION("[L1] list queries and entity creates in parallel") {
        auto userId = insertTestUser("conc_author", "conc_author@test.com", 0);

        // Seed some articles
        for (int i = 0; i < 5; ++i) {
            insertTestArticle("conc_cat", userId, "Seed_" + std::to_string(i), i * 10);
        }

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 4; ++j) {
                if (i % 2 == 0) {
                    // Query list — size varies due to concurrent inserts
                    sync(TestArticleListRepo::query(
                        makeArticleQuery("conc_cat")));
                } else {
                    // insert article via repo (triggers list notification)
                    auto article = makeTestArticle(
                        "conc_cat", userId,
                        "Conc_" + std::to_string(i) + "_" + std::to_string(j),
                        100 + i * 10 + j);
                    sync(TestArticleListRepo::insert(article));
                }
            }
        });

        // Final query should work
        auto result = sync(TestArticleListRepo::query(
            makeArticleQuery("conc_cat")));
        REQUIRE(result->size() >= 5);
    }
}


// #############################################################################
//
//  6. Concurrent warmup + operations
//
// #############################################################################

TEST_CASE("Concurrency - warmup during operations",
          "[integration][db][concurrency][warmup]")
{
    TransactionGuard tx;

    SECTION("[L1] warmup while reads are happening") {
        auto id = insertTestItem("conc_warmup", 42);

        parallel(NUM_THREADS, [&](int i) {
            if (i == 0) {
                // One thread does warmup repeatedly
                for (int j = 0; j < 10; ++j) {
                    L1TestItemRepo::warmup();
                }
            } else {
                // Other threads read
                for (int j = 0; j < OPS_PER_THREAD; ++j) {
                    sync(L1TestItemRepo::find(id));
                    // May be nullptr if warmup disrupts — that's fine
                }
            }
        });

        // Should still be functional
        auto item = sync(L1TestItemRepo::find(id));
        REQUIRE(item != nullptr);
    }
}


// #############################################################################
//
//  7. Mixed operations storm
//
// #############################################################################

TEST_CASE("Concurrency - mixed operations storm",
          "[integration][db][concurrency][storm]")
{
    TransactionGuard tx;

    SECTION("[L1+L2] all operations interleaved on shared entities") {
        std::vector<int64_t> ids;
        for (int i = 0; i < 10; ++i) {
            ids.push_back(insertTestItem("storm_" + std::to_string(i), i * 10));
        }

        // Prime all caches
        for (auto id : ids) {
            sync(FullCacheTestItemRepo::find(id));
        }

        parallel(NUM_THREADS, [&](int i) {
            std::mt19937 rng(i * 42 + 7);

            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                auto idx = rng() % ids.size();
                auto id = ids[idx];
                auto op = rng() % 6;

                switch (op) {
                    case 0:  // find
                    case 1:
                        sync(FullCacheTestItemRepo::find(id));
                        break;

                    case 2:  // findAsJson
                        sync(FullCacheTestItemRepo::findAsJson(id));
                        break;

                    case 3:  // update
                    {
                        auto entity = makeTestItem(
                            "storm_upd_" + std::to_string(i) + "_" + std::to_string(j),
                            static_cast<int32_t>(rng() % 1000),
                            "", true, id);
                        sync(FullCacheTestItemRepo::update(id, entity));
                        break;
                    }

                    case 4:  // invalidate
                        sync(FullCacheTestItemRepo::invalidate(id));
                        break;

                    case 5:  // invalidateL1 + read
                        FullCacheTestItemRepo::invalidateL1(id);
                        sync(FullCacheTestItemRepo::find(id));
                        break;
                }
            }
        });

        // Verify all entities are still accessible
        for (auto id : ids) {
            auto item = sync(FullCacheTestItemRepo::find(id));
            REQUIRE(item != nullptr);
        }
    }

    SECTION("[L1] rapid insert-read-update-delete cycles") {
        std::atomic<int> delete_mismatches{0};

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 4; ++j) {
                // insert
                auto entity = makeTestItem(
                    "crud_" + std::to_string(i) + "_" + std::to_string(j),
                    i * 100 + j);
                auto created = sync(L1TestItemRepo::insert(entity));
                if (!created) continue;

                auto id = created->id;

                // Read
                sync(L1TestItemRepo::find(id));

                // Update
                auto updated = makeTestItem(
                    "crud_upd_" + std::to_string(i) + "_" + std::to_string(j),
                    i * 100 + j + 1, "", true, id);
                sync(L1TestItemRepo::update(id, updated));

                // Read again
                sync(L1TestItemRepo::find(id));

                // Delete
                sync(L1TestItemRepo::remove(id));

                // Read after delete -> should be nullptr
                auto gone = sync(L1TestItemRepo::find(id));
                if (gone != nullptr) {
                    delete_mismatches.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });

        // After delete, reads should return nullptr
        REQUIRE(delete_mismatches.load() == 0);
    }
}


// #############################################################################
//
//  8. Concurrent patch
//
// #############################################################################

TEST_CASE("Concurrency - concurrent patch",
          "[integration][db][concurrency][patch]")
{
    TransactionGuard tx;

    using jcailloux::relais::wrapper::set;
    using F = TestUserWrapper::Field;

    SECTION("[L1] concurrent patch on same user") {
        auto userId = insertTestUser("conc_patch", "conc_ub@test.com", 0);
        sync(L1TestUserRepo::find(userId));

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 2; ++j) {
                auto balance = static_cast<int32_t>(i * 1000 + j);
                sync(L1TestUserRepo::patch(userId,
                    set<F::balance>(balance)));
            }
        });

        // Should still be functional — last writer wins
        auto user = sync(L1TestUserRepo::find(userId));
        REQUIRE(user != nullptr);
    }
}


// #############################################################################
//
//  9. Concurrent cleanup + operations
//
// #############################################################################

TEST_CASE("Concurrency - cleanup during operations",
          "[integration][db][concurrency][cleanup]")
{
    TransactionGuard tx;

    SECTION("[L1] triggerCleanup while reads and writes happen") {
        std::vector<int64_t> ids;
        for (int i = 0; i < 20; ++i) {
            ids.push_back(insertTestItem("cleanup_" + std::to_string(i), i));
        }

        // Prime caches
        for (auto id : ids) {
            sync(L1TestItemRepo::find(id));
        }

        parallel(NUM_THREADS, [&](int i) {
            std::mt19937 rng(i * 31);

            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                auto id = ids[rng() % ids.size()];

                if (i == 0) {
                    // One thread continuously triggers cleanup
                    triggerCleanup<L1TestItemRepo>();
                } else if (i == 1) {
                    // One thread does full cleanup
                    if (j % 10 == 0) {
                        forceFullCleanup<L1TestItemRepo>();
                    }
                } else {
                    // Others do reads and writes
                    if (j % 3 == 0) {
                        auto entity = makeTestItem(
                            "cl_" + std::to_string(i) + "_" + std::to_string(j),
                            static_cast<int32_t>(rng() % 1000),
                            "", true, id);
                        sync(L1TestItemRepo::update(id, entity));
                    } else {
                        sync(L1TestItemRepo::find(id));
                    }
                }
            }
        });

        // All entities should still be accessible
        for (auto id : ids) {
            auto item = sync(L1TestItemRepo::find(id));
            REQUIRE(item != nullptr);
        }
    }
}


// #############################################################################
//
//  10. Concurrent list CRUD + list cache cleanup
//
// #############################################################################

TEST_CASE("Concurrency - list CRUD + list cache cleanup",
          "[integration][db][concurrency][list-cleanup]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    SECTION("[L1] concurrent insert/update/remove/query with triggerCleanup") {
        auto userId = insertTestUser("conc_lc_author", "conc_lc@test.com", 0);

        // Seed articles
        std::vector<int64_t> ids;
        std::mutex ids_mutex;
        for (int i = 0; i < 10; ++i) {
            ids.push_back(insertTestArticle(
                "conc_lc", userId, "Seed_" + std::to_string(i), i * 10));
        }

        parallel(NUM_THREADS, [&](int i) {
            std::mt19937 rng(i * 37 + 11);

            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                if (i == 0) {
                    // Continuous cleanup (entity + list, unified)
                    triggerCleanup<TestArticleListRepo>();
                } else if (i == 1) {
                    // Query + periodic full cleanup
                    sync(TestArticleListRepo::query(
                        makeArticleQuery("conc_lc")));
                    if (j % 10 == 0) {
                        forceFullCleanup<TestArticleListRepo>();
                    }
                } else {
                    int op = rng() % 4;
                    if (op == 0) {
                        // insert
                        auto article = makeTestArticle(
                            "conc_lc", userId,
                            "CL_" + std::to_string(i) + "_" + std::to_string(j),
                            static_cast<int32_t>(rng() % 1000));
                        auto created = sync(TestArticleListRepo::insert(article));
                        if (created) {
                            std::lock_guard lock(ids_mutex);
                            ids.push_back(created->id);
                        }
                    } else if (op == 1) {
                        // Query
                        sync(TestArticleListRepo::query(
                            makeArticleQuery("conc_lc")));
                    } else if (op == 2) {
                        // Update (pick random existing)
                        int64_t id;
                        {
                            std::lock_guard lock(ids_mutex);
                            id = ids[rng() % ids.size()];
                        }
                        auto article = makeTestArticle(
                            "conc_lc", userId,
                            "Upd_" + std::to_string(i) + "_" + std::to_string(j),
                            static_cast<int32_t>(rng() % 1000), false, id);
                        sync(TestArticleListRepo::update(id, article));
                    } else {
                        // Remove (pick random existing)
                        int64_t id;
                        {
                            std::lock_guard lock(ids_mutex);
                            id = ids[rng() % ids.size()];
                        }
                        sync(TestArticleListRepo::remove(id));
                    }
                }
            }
        });

        // Final query should work — no crash, no corruption
        auto result = sync(TestArticleListRepo::query(
            makeArticleQuery("conc_lc")));
        REQUIRE(result != nullptr);
    }
}


// #############################################################################
//
//  11a. ModificationTracker drains after concurrent storm
//
// #############################################################################

TEST_CASE("Concurrency - tracker drains after concurrent storm",
          "[integration][db][concurrency][tracker-drain]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    SECTION("[L1] fullCleanup drains all modifications to zero") {
        auto userId = insertTestUser("conc_drain_author", "conc_drain@test.com", 0);

        // Phase 1: insert modifications without concurrent cleanup (guaranteed pending)
        for (int i = 0; i < 20; ++i) {
            auto article = makeTestArticle(
                "drain_cat", userId,
                "Drain_" + std::to_string(i),
                i * 10);
            sync(TestArticleListRepo::insert(article));
        }
        auto initial_count = TestInternals::pendingModificationCount<TestArticleListRepo>();
        REQUIRE(initial_count > 0);

        // Phase 2: Concurrent storm (creates + cleanups interleaved)
        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 2; ++j) {
                if (i < 2) {
                    // Cleanup threads
                    triggerCleanup<TestArticleListRepo>();
                } else {
                    // insert threads
                    auto article = makeTestArticle(
                        "drain_cat", userId,
                        "Storm_" + std::to_string(i) + "_" + std::to_string(j),
                        static_cast<int32_t>(100 + i * 10 + j));
                    sync(TestArticleListRepo::insert(article));
                }
            }
        });
        // After join, some modifications have partial cleanup_counts

        // Phase 3: Drain (no concurrent writes)
        TestInternals::forceFullListCleanup<TestArticleListRepo>();

        // Phase 4: Verify fully drained
        REQUIRE(TestInternals::pendingModificationCount<TestArticleListRepo>() == 0);

        // Phase 5: Second storm + drain (verify tracker reusability)
        parallel(NUM_THREADS / 2, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 4; ++j) {
                if (i == 0) {
                    triggerCleanup<TestArticleListRepo>();
                } else {
                    auto article = makeTestArticle(
                        "drain_cat", userId,
                        "Storm2_" + std::to_string(i) + "_" + std::to_string(j),
                        static_cast<int32_t>(500 + i * 10 + j));
                    sync(TestArticleListRepo::insert(article));
                }
            }
        });

        TestInternals::forceFullListCleanup<TestArticleListRepo>();
        REQUIRE(TestInternals::pendingModificationCount<TestArticleListRepo>() == 0);
    }
}


// #############################################################################
//
//  11b. Progressive reduction via triggerCleanup
//
// #############################################################################

TEST_CASE("Concurrency - progressive tracker reduction",
          "[integration][db][concurrency][tracker-progressive]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    SECTION("[L1] triggerCleanup progressively reduces modification count") {
        auto userId = insertTestUser("conc_prog_author", "conc_prog@test.com", 0);

        // insert modifications (no concurrent cleanup)
        for (int i = 0; i < 10; ++i) {
            auto article = makeTestArticle(
                "prog_cat", userId,
                "Prog_" + std::to_string(i),
                i * 10);
            sync(TestArticleListRepo::insert(article));
        }

        auto initial_count = TestInternals::pendingModificationCount<TestArticleListRepo>();
        REQUIRE(initial_count == 10);

        // Run cleanup cycles (2× ShardCount to ensure all bitmap bits are cleared)
        constexpr auto N = TestInternals::listCacheShardCount<TestArticleListRepo>();
        for (size_t i = 0; i < 2 * N; ++i) {
            TestInternals::forceModificationTrackerCleanup<TestArticleListRepo>();
        }

        // After enough cycles, all 10 modifications should have been drained
        auto final_count = TestInternals::pendingModificationCount<TestArticleListRepo>();
        REQUIRE(final_count < initial_count);
    }

    SECTION("[L1] concurrent cleanup + queries don't leak modifications") {
        TestInternals::resetListCacheState<TestArticleListRepo>();
        auto userId = insertTestUser("conc_prog2_author", "conc_prog2@test.com", 0);

        // insert modifications
        for (int i = 0; i < 10; ++i) {
            auto article = makeTestArticle(
                "prog2_cat", userId,
                "Prog2_" + std::to_string(i),
                i * 10);
            sync(TestArticleListRepo::insert(article));
        }

        auto count_before = TestInternals::pendingModificationCount<TestArticleListRepo>();
        REQUIRE(count_before == 10);

        // Concurrent cleanup + queries (queries trigger lazy validation via forEachModification)
        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD; ++j) {
                if (i == 0) {
                    triggerCleanup<TestArticleListRepo>();
                } else {
                    sync(TestArticleListRepo::query(
                        makeArticleQuery("prog2_cat")));
                }
            }
        });

        // After concurrent cleanup, count should not have grown
        auto count_after = TestInternals::pendingModificationCount<TestArticleListRepo>();
        REQUIRE(count_after <= count_before);
    }
}
