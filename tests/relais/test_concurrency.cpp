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
 *   3. Concurrent insert + erase
 *   4. Concurrent cross-invalidation
 *   5. Concurrent list queries + entity modifications
 *   6. Concurrent warmup + operations
 *   7. Mixed operations storm (all operations interleaved)
 *   8. Concurrent patch
 *   9. Concurrent cleanup + operations (entity cache)
 *  10. Concurrent list CRUD + list cache cleanup
 *  11a. ModificationTracker drains after concurrent storm
 *  11b. Progressive tracker reduction via trySweep
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"

#include "jcailloux/relais/cache/RedisCache.h"

#include <thread>
#include <vector>
#include <latch>
#include <atomic>
#include <random>
#include <span>
#include <optional>
#include <unordered_map>

using namespace relais_test;

// #############################################################################
//
//  ThreadSanitizer suppressions (compiled into the binary)
//
//  parlayhash (third_party, CMU parlaylib) is a lock-free hash map: a reader
//  snapshots a bucket `state` / `Entry` by value while a writer may be CAS-
//  swapping the bucket head, then validates the snapshot against the live head
//  and retries if it changed. The unsynchronized value copy is correct by
//  construction — epoch reclamation keeps the memory live, a stale snapshot is
//  discarded — but TSan cannot see the validate-and-retry, so it flags the copy
//  (state::operator=, Entry::operator=, and the __tsan_memcpy they lower to).
//  Embedding the suppressions here keeps both ctest and direct-exec runs clean
//  without external TSAN_OPTIONS wiring. Scoped to parlayhash internals only —
//  real races in relais code carry relais frames and are not matched.
//
// #############################################################################

#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#    define RELAIS_TSAN_BUILD 1
#  endif
#endif
#if defined(__SANITIZE_THREAD__)
#  define RELAIS_TSAN_BUILD 1
#endif

#ifdef RELAIS_TSAN_BUILD
extern "C" const char* __tsan_default_suppressions() {
    return
        "race:parlay::parlay_hash\n"
        "race:parlay::DirectEntries\n";
}
#endif

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

/// Public findMany entry over a vector of simple int64 keys, returning the
/// guarded MultiView<E>. The L1 probe + epoch acquire run on the calling
/// (worker) thread; the slow path dispatches to the loop.
template<typename Repo>
auto findManyView(const std::vector<int64_t>& ids) {
    return sync(Repo::findMany(std::span<const int64_t>(ids)));
}

/// Raw MGET against Redis — used to observe detached L2 warm-fills.
inline io::Task<std::vector<std::optional<std::string>>>
mgetRawKeys(std::vector<std::string> keys) {
    co_return co_await cache::RedisCache::mgetRaw(keys);
}

/// Poll until a detached L2 fill becomes visible (bounded). Doubles as a drain:
/// returning means the fire-and-forget SET landed, ordering it before teardown.
inline std::optional<std::string> awaitL2Key(const std::string& key) {
    for (int i = 0; i < 200; ++i) {
        auto r = sync(mgetRawKeys({key}));
        if (r[0]) return r[0];
    }
    return std::nullopt;
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
//  3. Concurrent insert + erase
//
// #############################################################################

TEST_CASE("Concurrency - concurrent insert + erase",
          "[integration][db][concurrency][insert-erase]")
{
    TransactionGuard tx;

    SECTION("[L1] threads insert and erase entities concurrently") {
        std::atomic<int> created_count{0};

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 2; ++j) {
                auto entity = makeTestItem(
                    "cr_" + std::to_string(i) + "_" + std::to_string(j),
                    i * 1000 + j);
                auto item = sync(L1TestItemRepo::insert(entity));
                if (item) {
                    created_count.fetch_add(1, std::memory_order_relaxed);
                    sync(L1TestItemRepo::erase(item->id));
                }
            }
        });

        REQUIRE(created_count.load() > 0);
    }

    SECTION("[L1+L2] threads insert and erase entities concurrently") {
        std::atomic<int> created_count{0};

        parallel(NUM_THREADS, [&](int i) {
            for (int j = 0; j < OPS_PER_THREAD / 2; ++j) {
                auto entity = makeTestItem(
                    "cr_both_" + std::to_string(i) + "_" + std::to_string(j),
                    i * 1000 + j);
                auto item = sync(FullCacheTestItemRepo::insert(entity));
                if (item) {
                    created_count.fetch_add(1, std::memory_order_relaxed);
                    sync(FullCacheTestItemRepo::erase(item->id));
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
                        sync(L1TestPurchaseRepo::erase(created->id));
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

                    case 2:  // findJson
                        sync(FullCacheTestItemRepo::findJson(id));
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

                    case 5:  // evict + read
                        FullCacheTestItemRepo::evict(id);
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
                sync(L1TestItemRepo::erase(id));

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

    using jcailloux::relais::entity::set;
    using F = TestUserEntity::Field;

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

    SECTION("[L1] trySweep while reads and writes happen") {
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
                    trySweep<L1TestItemRepo>();
                } else if (i == 1) {
                    // One thread does full cleanup
                    if (j % 10 == 0) {
                        forcePurge<L1TestItemRepo>();
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

    SECTION("[L1] concurrent insert/update/erase/query with trySweep") {
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
                    trySweep<TestArticleListRepo>();
                } else if (i == 1) {
                    // Query + periodic full cleanup
                    sync(TestArticleListRepo::query(
                        makeArticleQuery("conc_lc")));
                    if (j % 10 == 0) {
                        forcePurge<TestArticleListRepo>();
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
                        // Erase (pick random existing)
                        int64_t id;
                        {
                            std::lock_guard lock(ids_mutex);
                            id = ids[rng() % ids.size()];
                        }
                        sync(TestArticleListRepo::erase(id));
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

    SECTION("[L1] purge drains all modifications to zero") {
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
                    trySweep<TestArticleListRepo>();
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
                    trySweep<TestArticleListRepo>();
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
//  11b. Progressive reduction via trySweep
//
// #############################################################################

TEST_CASE("Concurrency - progressive tracker reduction",
          "[integration][db][concurrency][tracker-progressive]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    SECTION("[L1] full drain removes all modifications") {
        auto userId = insertTestUser("conc_prog_author", "conc_prog@test.com", 0);

        // insert modifications (no concurrent cleanup)
        for (int i = 0; i < 10; ++i) {
            auto article = makeTestArticle(
                "prog_cat", userId,
                "Prog_" + std::to_string(i),
                i * 10);
            sync(TestArticleListRepo::insert(article));
        }

        REQUIRE(TestInternals::pendingModificationCount<TestArticleListRepo>() == 10);

        // Drain every chunk deterministically (no background sweep dependency).
        auto cutoff = TestInternals::listCacheGeneration<TestArticleListRepo>();
        TestInternals::drainAllModificationChunks<TestArticleListRepo>(cutoff);

        REQUIRE(TestInternals::pendingModificationCount<TestArticleListRepo>() == 0);
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
                    trySweep<TestArticleListRepo>();
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


// #############################################################################
//
//  12. Concurrent findMany — overlapping ids (zero-copy hits, race-free)
//
//  M threads issue findMany over overlapping windows into a shared pool of
//  primed entities, plus one absent id per request.  The L1 probe + epoch
//  acquire run concurrently on the worker threads, the store path on the loop.
//  Values are stable (no writers), so the result is checked exactly: present
//  ids → correct balance at the right slot, absent id → nullptr.
//
// #############################################################################

template<typename Repo>
static void runConcurrentOverlap(const std::vector<int64_t>& pool,
                                 const std::unordered_map<int64_t, int32_t>& expected) {
    std::atomic<int> mismatches{0};

    parallel(NUM_THREADS, [&](int t) {
        std::mt19937 rng(t * 101 + 3);
        for (int j = 0; j < OPS_PER_THREAD; ++j) {
            // Overlapping 4-wide window + one absent hole, ordered.
            std::vector<int64_t> ids;
            int base = rng() % (static_cast<int>(pool.size()) - 4);
            for (int k = 0; k < 4; ++k) ids.push_back(pool[base + k]);
            ids.push_back(-1);

            auto view = findManyView<Repo>(ids);
            if (view.size() != ids.size()) {
                mismatches.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            for (size_t i = 0; i < ids.size(); ++i) {
                const auto* u = view[i];
                if (ids[i] == -1) {
                    if (u != nullptr) mismatches.fetch_add(1, std::memory_order_relaxed);
                } else if (!u || u->balance != expected.at(ids[i])) {
                    mismatches.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    });

    REQUIRE(mismatches.load() == 0);
}

TEST_CASE("Concurrency - findMany overlapping reads",
          "[integration][db][concurrency][tsan][findmany]")
{
    TransactionGuard tx;

    constexpr int POOL = 12;
    std::vector<int64_t> pool;
    std::unordered_map<int64_t, int32_t> expected;
    for (int i = 0; i < POOL; ++i) {
        auto id = insertTestUser("conc_fm_" + std::to_string(i),
                                 "conc_fm_" + std::to_string(i) + "@x", i * 7);
        pool.push_back(id);
        expected.emplace(id, i * 7);
    }

    SECTION("[L1] overlapping windows, zero-copy hits") {
        findManyView<L1TestUserRepo>(pool);          // prime L1
        runConcurrentOverlap<L1TestUserRepo>(pool, expected);
    }

    SECTION("[L1+L2] overlapping windows, zero-copy hits") {
        findManyView<FullCacheTestUserRepo>(pool);   // prime L1 (+ detached L2)
        runConcurrentOverlap<FullCacheTestUserRepo>(pool, expected);
    }
}


// #############################################################################
//
//  13. findMany vs concurrent writes
//
//  Readers batch-read a pool while writers update entities in it.  Mid-flight
//  reads may transiently miss (the shared-transaction pg connection serialises
//  interleaved L3 queries — the existing read+write test documents the same
//  transient nullptr), so the invariant is robustness, not per-op values: the
//  pinned pointers stay dereferenceable (UAF bait for TSan/ASan) and a settled
//  batch read after the storm is complete. Covers the read → detached-L2 window.
//
// #############################################################################

template<typename Repo>
static void runFindManyVsWrites(const std::vector<int64_t>& pool) {
    parallel(NUM_THREADS, [&](int t) {
        std::mt19937 rng(t * 71 + 5);
        for (int j = 0; j < OPS_PER_THREAD; ++j) {
            if (t % 2 == 0) {
                // Reader — touch every present slot under the guard.
                auto view = findManyView<Repo>(pool);
                volatile long sink = 0;
                for (size_t i = 0; i < view.size(); ++i)
                    if (view[i]) sink += view[i]->balance;
                (void)sink;
            } else {
                // Writer — update a random pooled entity (invalidates its caches).
                auto id = pool[rng() % pool.size()];
                auto entity = makeTestUser("conc_w_" + std::to_string(t) + "_" + std::to_string(j),
                                           "conc_w@x", static_cast<int32_t>(rng() % 1000), id);
                sync(Repo::update(id, entity));
            }
        }
    });

    // Quiescent: every pooled id exists, so a settled batch read fills all slots.
    auto view = findManyView<Repo>(pool);
    int missing = 0;
    for (size_t i = 0; i < view.size(); ++i) if (!view[i]) ++missing;
    REQUIRE(missing == 0);
}

TEST_CASE("Concurrency - findMany vs concurrent writes",
          "[integration][db][concurrency][tsan][findmany]")
{
    TransactionGuard tx;

    constexpr int POOL = 10;
    std::vector<int64_t> pool;
    for (int i = 0; i < POOL; ++i) {
        pool.push_back(insertTestUser("conc_fmw_" + std::to_string(i),
                                      "conc_fmw_" + std::to_string(i) + "@x", i));
    }

    SECTION("[L1] readers vs writers") {
        findManyView<L1TestUserRepo>(pool);
        runFindManyVsWrites<L1TestUserRepo>(pool);
    }

    SECTION("[L1+L2] readers vs writers") {
        findManyView<FullCacheTestUserRepo>(pool);
        runFindManyVsWrites<FullCacheTestUserRepo>(pool);
    }
}


// #############################################################################
//
//  14. findMany guard survives concurrent invalidation
//
//  A reader holds a MultiView (epoch guard live) and dereferences every slot in
//  a tight loop while other threads invalidate + re-find the same ids, retiring
//  the L1 entries the reader points at.  The batch guard defers the free, so the
//  pointers stay readable — TSan/ASan must see no UAF, no data race.
//
// #############################################################################

template<typename Repo>
static void runGuardVsInvalidation(const std::vector<int64_t>& pool) {
    parallel(NUM_THREADS, [&](int t) {
        std::mt19937 rng(t * 53 + 1);
        for (int j = 0; j < OPS_PER_THREAD; ++j) {
            if (t % 2 == 0) {
                // Reader — pin the view, then read every field repeatedly. The
                // batch guard must keep pointers readable even as a concurrent
                // invalidate retires the entries they point at (UAF bait).
                auto view = findManyView<Repo>(pool);
                volatile long sink = 0;
                for (int r = 0; r < 32; ++r)
                    for (size_t i = 0; i < view.size(); ++i)
                        if (view[i]) sink += view[i]->balance;
                (void)sink;
            } else {
                // Mutator — evict + re-store, retiring the entries under the guard.
                auto id = pool[rng() % pool.size()];
                sync(Repo::invalidate(id));
                sync(Repo::find(id));
            }
        }
    });

    // Quiescent: invalidation only evicts caches, the DB rows remain.
    auto view = findManyView<Repo>(pool);
    int missing = 0;
    for (size_t i = 0; i < view.size(); ++i) if (!view[i]) ++missing;
    REQUIRE(missing == 0);
}

TEST_CASE("Concurrency - findMany guard survives invalidation",
          "[integration][db][concurrency][tsan][findmany]")
{
    TransactionGuard tx;

    constexpr int POOL = 8;
    std::vector<int64_t> pool;
    for (int i = 0; i < POOL; ++i) {
        pool.push_back(insertTestUser("conc_fmi_" + std::to_string(i),
                                      "conc_fmi_" + std::to_string(i) + "@x", i));
    }

    SECTION("[L1] guard pins pointers across invalidation") {
        findManyView<L1TestUserRepo>(pool);
        runGuardVsInvalidation<L1TestUserRepo>(pool);
    }

    SECTION("[L1+L2] guard pins pointers across invalidation") {
        findManyView<FullCacheTestUserRepo>(pool);
        runGuardVsInvalidation<FullCacheTestUserRepo>(pool);
    }
}


// #############################################################################
//
//  15. findMany (Both) — detached L2 fill drains before teardown
//
//  Concurrent findMany over L2-cold ids fans out one detached SET per miss.
//  Each fire-and-forget fill must land before the TransactionGuard flushes
//  Redis — awaitL2Key polls it to completion, ordering the DetachedTask ahead
//  of teardown so it never touches a flushed connection.
//
// #############################################################################

TEST_CASE("Concurrency - findMany detached L2 fill drains before teardown",
          "[integration][db][concurrency][tsan][findmany][l2]")
{
    TransactionGuard tx;

    constexpr int POOL = 8;
    std::vector<int64_t> pool;
    std::vector<std::string> keys;
    for (int i = 0; i < POOL; ++i) {
        auto id = insertTestUser("conc_fml2_" + std::to_string(i),
                                 "conc_fml2_" + std::to_string(i) + "@x", i);  // DB only, L2 cold
        pool.push_back(id);
        keys.push_back(FullCacheTestUserRepo::makeRedisKey(id));
    }

    // Concurrent batches → L1+L2 misses → L3 fetch + one detached L2 SET each.
    parallel(NUM_THREADS, [&](int) {
        for (int j = 0; j < OPS_PER_THREAD / 2; ++j) {
            auto view = findManyView<FullCacheTestUserRepo>(pool);
            for (size_t i = 0; i < view.size(); ++i) (void)view[i];
        }
    });

    // Drain: every detached fill must be visible before TransactionGuard flush.
    for (const auto& k : keys) {
        REQUIRE(awaitL2Key(k).has_value());
    }
}
