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
 *  16. Concurrent upsert on a shared assigned-PK key (+ upsert‖update‖erase)
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


// #############################################################################
//
//  §6 — Batch invalidation concurrency (plan erase-invalidate-batch)
//
//  The batch paths add new shared mutable state the mono paths never touched:
//  the per-key generation bump driven from invalidateMany/eraseMany, and the
//  ModificationTracker's second track (RangeModification) driven by eraseWhere.
//  These cases race those exact surfaces. Worker threads only assert robustness
//  (the `parallel` error counter); the metamorphic correctness lives in the
//  post-quiescence main-thread checks, which are interleaving-independent
//  (delete/evict are idempotent, so the settled state is deterministic).
//
// #############################################################################


// -----------------------------------------------------------------------------
//  §6.1+6.2 — invalidateMany (batch bumpGeneration) vs concurrent L1 fills
//
//  A reader re-fills L1 from L3 for the whole pool while a second thread runs
//  invalidateMany over the same pool: each batched bump races an in-flight fill
//  of the (now stale) entry. The bump must win — a stale fill must never resurrect
//  past the invalidation. invalidate evicts caches only (rows stay in L3), so the
//  settled read fills every slot; a survivor would mean a fill outran its bump.
// -----------------------------------------------------------------------------

template<typename Repo>
static void runInvalidateManyVsFills(const std::vector<int64_t>& pool) {
    parallel(NUM_THREADS, [&](int t) {
        for (int j = 0; j < OPS_PER_THREAD; ++j) {
            if (t % 2 == 0) {
                // Reader — re-fill L1 from L3 for every pooled id, racing the bump.
                for (auto id : pool) sync(Repo::find(id));
            } else {
                // Batch invalidator — one bumpGeneration per key, in one cascade.
                sync(Repo::invalidateMany(std::span<const int64_t>(pool)));
            }
        }
    });

    // Quiescent: invalidate never deletes → every row resolves from L3.
    auto view = findManyView<Repo>(pool);
    int missing = 0;
    for (size_t i = 0; i < view.size(); ++i) if (!view[i]) ++missing;
    REQUIRE(missing == 0);
}

TEST_CASE("Concurrency - invalidateMany vs concurrent fills",
          "[integration][db][concurrency][tsan][batch]")
{
    TransactionGuard tx;

    constexpr int POOL = 10;
    std::vector<int64_t> pool;
    for (int i = 0; i < POOL; ++i) {
        pool.push_back(insertTestUser("conc_im_" + std::to_string(i),
                                      "conc_im_" + std::to_string(i) + "@x", i));
    }

    SECTION("[L1] batch bump vs fills") {
        findManyView<L1TestUserRepo>(pool);
        runInvalidateManyVsFills<L1TestUserRepo>(pool);
    }

    SECTION("[L1+L2] batch bump vs fills") {
        findManyView<FullCacheTestUserRepo>(pool);
        runInvalidateManyVsFills<FullCacheTestUserRepo>(pool);
    }
}


// -----------------------------------------------------------------------------
//  §6.3 — eraseMany vs mono erase on overlapping keys (idempotence)
//
//  Half the threads batch-erase the first 60% of the pool, the other half
//  mono-erase the last 60% — the middle 20% is double-deleted from both paths
//  concurrently. A double DELETE … RETURNING yields zero rows the second time;
//  the eviction must stay idempotent (no crash, no ghost hit). After quiescence
//  every targeted id is gone on all three tiers (no L1 phantom over a deleted row).
// -----------------------------------------------------------------------------

template<typename Repo>
static void runEraseManyVsMono(const std::vector<int64_t>& ids) {
    const size_t lo = ids.size() * 4 / 10;   // mono start (overlap begins)
    const size_t hi = ids.size() * 6 / 10;   // batch end   (overlap ends)
    std::vector<int64_t> batchSub(ids.begin(), ids.begin() + hi);

    parallel(NUM_THREADS, [&](int t) {
        for (int j = 0; j < 4; ++j) {
            if (t % 2 == 0) {
                sync(Repo::eraseMany(std::span<const int64_t>(batchSub)));
            } else {
                for (size_t i = lo; i < ids.size(); ++i) sync(Repo::erase(ids[i]));
            }
        }
    });

    // Idempotent convergence: every id deleted by one path or the other is gone.
    for (auto id : ids) {
        REQUIRE(sync(Repo::find(id)) == nullptr);
    }
}

TEST_CASE("Concurrency - eraseMany vs mono erase overlapping keys",
          "[integration][db][concurrency][tsan][batch][erase]")
{
    TransactionGuard tx;

    constexpr int POOL = 20;

    SECTION("[L1] batch and mono erase overlap") {
        std::vector<int64_t> ids;
        for (int i = 0; i < POOL; ++i)
            ids.push_back(insertTestItem("conc_em_l1_" + std::to_string(i), i));
        runEraseManyVsMono<L1TestItemRepo>(ids);
    }

    SECTION("[L1+L2] batch and mono erase overlap") {
        std::vector<int64_t> ids;
        for (int i = 0; i < POOL; ++i)
            ids.push_back(insertTestItem("conc_em_both_" + std::to_string(i), i));
        runEraseManyVsMono<FullCacheTestItemRepo>(ids);
    }
}


// -----------------------------------------------------------------------------
//  §6.3b — eraseMany vs concurrent find: the deleted-row L2→L1 phantom
//
//  Reader threads hammer find() on the very keys an eraser thread is deleting
//  via eraseMany. If the batch cascade evicts L1 BEFORE clearing L2, a reader
//  hits the window: L1-miss → L2-hit (the still-present deleted row) → re-store
//  into the SHARED L1. Nothing re-evicts L1, so the phantom PERSISTS past
//  quiescence — a resurrected deleted row. With L2 cleared before L1 (matching
//  mono erase), a racing reader can only L1-hit the not-yet-evicted entry
//  (bounded, self-heals at the evict); no L1-miss reaches the L2 phantom.
//  Requires L1+L2 — the bug is an L2 hit repopulating L1 (no L2, no phantom).
// -----------------------------------------------------------------------------
template<typename Repo>
static void runEraseManyVsFinds(const std::vector<int64_t>& ids) {
    parallel(NUM_THREADS, [&](int t) {
        for (int j = 0; j < OPS_PER_THREAD; ++j) {
            if (t % 2 == 0) {
                for (auto id : ids) sync(Repo::find(id));  // probe the L1→L2 path
            } else {
                sync(Repo::eraseMany(std::span<const int64_t>(ids)));
            }
        }
    });

    // No row is re-inserted → every deleted key must be gone from every tier.
    // A surviving L1 phantom would resurrect a deleted row right here.
    for (auto id : ids) {
        REQUIRE(sync(Repo::find(id)) == nullptr);
    }
}

// Closed by the read-fill recheck (commit 12a): a find() whose L3 fetch
// straddles the delete used to write the entity back into L1/L2 AFTER the
// eraser cleared them (the read path cached unconditionally). The reader now
// snapshots the recheck slot at fetch-start and skips the store when a
// mutation bumped it in between — the value still returns to the caller, but
// it never pollutes the cache, so no deleted row is resurrected.
TEST_CASE("Concurrency - eraseMany vs concurrent finds (no L2->L1 phantom)",
          "[integration][db][redis][concurrency][tsan][batch][erase]")
{
    TransactionGuard tx;

    // A wide pool widens the per-window opportunity: the single effective
    // eraseMany evicts every L1 slot then suspends on the multi-key UNLINK
    // (~1 RTT), and readers probing any of these keys during that suspension
    // catch the phantom in the buggy (L1-before-L2) order.
    constexpr int POOL = 48;
    std::vector<int64_t> ids;
    for (int i = 0; i < POOL; ++i)
        ids.push_back(insertTestItem("conc_emf_" + std::to_string(i), i));

    findManyView<FullCacheTestItemRepo>(ids);  // warm L1 + L2
    runEraseManyVsFinds<FullCacheTestItemRepo>(ids);
}

// Mono variant: per-key erase() racing per-key find(). Exercises the mono
// eraseOutcome → evictRedis path and the mono findRaw/findSlow read-fill
// recheck (the batch test above drives eraseMany + findManyRaw instead).
template<typename Repo>
static void runEraseVsFinds(const std::vector<int64_t>& ids) {
    parallel(NUM_THREADS, [&](int t) {
        for (int j = 0; j < OPS_PER_THREAD; ++j) {
            if (t % 2 == 0) {
                for (auto id : ids) sync(Repo::find(id));
            } else {
                for (auto id : ids) sync(Repo::erase(id));
            }
        }
    });
    for (auto id : ids) {
        REQUIRE(sync(Repo::find(id)) == nullptr);
    }
}

TEST_CASE("Concurrency - mono erase vs concurrent finds (no phantom)",
          "[integration][db][redis][concurrency][tsan][erase]")
{
    TransactionGuard tx;
    constexpr int POOL = 48;
    std::vector<int64_t> ids;
    for (int i = 0; i < POOL; ++i)
        ids.push_back(insertTestItem("conc_mef_" + std::to_string(i), i));

    findManyView<FullCacheTestItemRepo>(ids);  // warm L1 + L2
    runEraseVsFinds<FullCacheTestItemRepo>(ids);
}

// Per-tier coverage of the read-fill recheck. L1-only exercises LocalRepo's
// snapshot/gate alone (no Redis). L2-only exercises RedisRepo's own recheck
// AND its bumpGen — there LocalRepo is absent, so RedisRepo is the top layer
// and must bump the shared counter itself.
TEST_CASE("Concurrency - read-fill recheck per tier (L1-only, L2-only)",
          "[integration][db][redis][concurrency][tsan][erase]")
{
    TransactionGuard tx;
    constexpr int POOL = 48;
    std::vector<int64_t> ids;
    for (int i = 0; i < POOL; ++i)
        ids.push_back(insertTestItem("conc_tier_" + std::to_string(i), i));

    SECTION("L1-only") {
        for (auto id : ids) sync(L1TestItemRepo::find(id));  // warm L1
        runEraseManyVsFinds<L1TestItemRepo>(ids);
    }
    SECTION("L2-only") {
        for (auto id : ids) sync(L2TestItemRepo::find(id));  // warm L2
        runEraseManyVsFinds<L2TestItemRepo>(ids);
    }
}

// Multi-instance topology (l2_shared_across_instances): the L2 read-fill recheck
// is the Redis-side generation hash, not the process-local counter. The erase
// HINCRBYs the slot before the entity UNLINK; a straddling fill snapshots the
// gen (HGET) at fetch-start and lands via an atomic conditional SET (setIfGen),
// so a value straddling the delete is rejected by the moved gen — closing the
// same phantom across processes. Single process here exercises the real Redis
// path (find → getGen → setIfGen, erase → bumpGen → UNLINK); the cross-instance
// straddle itself is pinned deterministically in test_l2_gen. L2-only drives the
// Redis-side gen alone; L1+L2 also drives LocalRepo's process-local recheck.
TEST_CASE("Concurrency - read-fill recheck, shared topology (Redis-side gen)",
          "[integration][db][redis][concurrency][tsan][erase]")
{
    TransactionGuard tx;
    constexpr int POOL = 48;
    std::vector<int64_t> ids;
    for (int i = 0; i < POOL; ++i)
        ids.push_back(insertTestItem("conc_shared_" + std::to_string(i), i));

    SECTION("L2-only shared") {
        for (auto id : ids) sync(SharedL2TestItemRepo::find(id));  // warm L2
        runEraseManyVsFinds<SharedL2TestItemRepo>(ids);
    }
    SECTION("L1+L2 shared") {
        findManyView<SharedBothTestItemRepo>(ids);  // warm L1 + L2
        runEraseManyVsFinds<SharedBothTestItemRepo>(ids);
    }
    SECTION("L1+L2 shared, mono erase vs finds") {
        findManyView<SharedBothTestItemRepo>(ids);  // warm L1 + L2
        runEraseVsFinds<SharedBothTestItemRepo>(ids);
    }
}



// -----------------------------------------------------------------------------
//  §6.4 — concurrent eraseWhere with overlapping predicates
//
//  {author = A} and {category = tech} both match A's tech articles. Two thread
//  groups hammer the two predicates concurrently: each call appends a
//  RangeModification (notifyRangeDeleted bumps the shared generation + flips the
//  range bitmap) while the other deletes rows underneath. Convergence is
//  interleaving-independent: everything matching either predicate is gone.
// -----------------------------------------------------------------------------

TEST_CASE("Concurrency - concurrent eraseWhere overlapping predicates",
          "[integration][db][concurrency][tsan][batch][where]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    auto A = insertTestUser("conc_ew_a", "conc_ew_a@x", 0);
    auto B = insertTestUser("conc_ew_b", "conc_ew_b@x", 0);
    for (int i = 0; i < 8; ++i) {
        insertTestArticle("tech", A, "TA_" + std::to_string(i), i);
        insertTestArticle("news", A, "NA_" + std::to_string(i), i);
        insertTestArticle("tech", B, "TB_" + std::to_string(i), i);
    }

    auto qA   = makeArticleQuery(std::nullopt, A, 50);
    auto qTech = makeArticleQuery("tech", std::nullopt, 50);
    REQUIRE(sync(TestArticleListRepo::query(qA))->size() == 16);     // warm: A's tech+news
    REQUIRE(sync(TestArticleListRepo::query(qTech))->size() == 16);  // warm: A+B tech

    parallel(NUM_THREADS, [&](int t) {
        for (int j = 0; j < OPS_PER_THREAD / 8; ++j) {
            if (t % 2 == 0) {
                sync(TestArticleListRepo::eraseWhere({.author_id = A}));
            } else {
                sync(TestArticleListRepo::eraseWhere({.category = std::string("tech")}));
            }
        }
    });

    // Convergence: author A fully purged; every tech article (A's + B's) purged.
    CHECK(sync(TestArticleListRepo::query(qA))->size() == 0);
    CHECK(sync(TestArticleListRepo::query(qTech))->size() == 0);
}


// -----------------------------------------------------------------------------
//  §6.5 — RangeModification production vs concurrent drainChunk / sweep
//
//  eraseWhere appends a RangeModification on every call (even at zero rows, the
//  predicate fast-path still fires). Producers race a sweeper (drainChunk clears
//  range bits under the two-phase lock) and a querier (forEachRangeWithBitmap
//  consumes them lazily) — atomic_ref<BitmapType> on TrackedRange.pending_segments
//  under TSan. A deterministic post-drain proves both tracks empty and reusable.
// -----------------------------------------------------------------------------

TEST_CASE("Concurrency - eraseWhere RangeModification vs concurrent sweep",
          "[integration][db][concurrency][tsan][batch][where][tracker]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    auto A = insertTestUser("conc_rm_a", "conc_rm_a@x", 0);
    for (int i = 0; i < 10; ++i)
        insertTestArticle("tech", A, "RM_" + std::to_string(i), i * 10);

    auto qA = makeArticleQuery(std::nullopt, A);

    parallel(NUM_THREADS, [&](int t) {
        for (int j = 0; j < OPS_PER_THREAD / 2; ++j) {
            if (t == 0) {
                trySweep<TestArticleListRepo>();                  // drainChunk (range track)
            } else if (t == 1) {
                sync(TestArticleListRepo::query(qA));             // lazy range consumption
            } else {
                sync(TestArticleListRepo::eraseWhere({.author_id = A}));  // RangeModification
            }
        }
    });

    // Deterministic drain (no concurrent writers) → both tracks empty, reusable.
    TestInternals::forceFullListCleanup<TestArticleListRepo>();
    REQUIRE(TestInternals::pendingRangeCount<TestArticleListRepo>() == 0);
    REQUIRE(TestInternals::pendingModificationCount<TestArticleListRepo>() == 0);
}


// #############################################################################
//
//  16. Concurrent upsert on a shared assigned-PK key
//
//  Upsert composes the update-model primitives on the shared static caches:
//  a pre-image discriminant, then L1 onMutation+bumpGeneration+storeAndView and
//  (Both) L2 bumpGen+setInCache of the committed RETURNING row. N threads upsert
//  the SAME key — the first resolves to INSERT, the rest to in-place UPDATE, each
//  racing the others' store-through and generation bump on that one slot. Worker
//  threads assert only robustness (the `parallel` error counter); correctness is
//  a post-quiescence deterministic settle: a final single-threaded upsert must
//  supersede whatever the storm left, visible identically in the cached tier and
//  in the DB (probed via the uncached repo — the ON CONFLICT PK guarantees a
//  single row, so cache and DB must agree, no torn value, no stale survivor).
//
//  The second scenario interleaves upsert ‖ update ‖ erase on the same key: any
//  legal serialization is admissible, so the only invariant is no crash / no
//  phantom — the settle proves the tier is still coherent with the DB afterward.
//
// #############################################################################

// Ground-truth read straight from L3 (no cache), same table as the cached repos.
static std::optional<int64_t> dbAkeyPayload(int64_t key) {
    auto v = sync(UncachedTestAssignedKeyRepo::find(key));
    return v ? std::optional<int64_t>(v->payload) : std::nullopt;
}

template<typename Repo>
static void runConcurrentUpsertSameKey(int64_t key) {
    parallel(NUM_THREADS, [&](int t) {
        for (int j = 0; j < OPS_PER_THREAD; ++j) {
            sync(Repo::upsert(makeTestAssignedKey(key, t * 1000 + j)));
        }
    });

    // The row exists (upsert never leaves the slot absent) and the tier is live.
    REQUIRE(sync(Repo::find(key)) != nullptr);

    // Deterministic settle: a single-threaded upsert supersedes the storm's
    // residue in both the cached tier and L3 — they must agree.
    constexpr int64_t SENTINEL = 987654;
    sync(Repo::upsert(makeTestAssignedKey(key, SENTINEL)));

    auto cached = sync(Repo::find(key));
    REQUIRE(cached != nullptr);
    REQUIRE(cached->payload == SENTINEL);
    REQUIRE(dbAkeyPayload(key) == SENTINEL);
}

template<typename Repo>
static void runUpsertUpdateEraseSameKey(int64_t key) {
    // Seed so update/erase have a row to act on from the first iteration.
    sync(Repo::upsert(makeTestAssignedKey(key, 1)));

    parallel(NUM_THREADS, [&](int t) {
        for (int j = 0; j < OPS_PER_THREAD; ++j) {
            switch (t % 3) {
                case 0:
                    sync(Repo::upsert(makeTestAssignedKey(key, t * 1000 + j)));
                    break;
                case 1:
                    // 0 rows affected if a concurrent erase removed it — no throw.
                    sync(Repo::update(key, makeTestAssignedKey(key, t * 1000 + j)));
                    break;
                default:
                    sync(Repo::erase(key));
                    break;
            }
        }
    });

    // Any legal serialization is fine; the settle proves post-storm coherence.
    constexpr int64_t SENTINEL = 987654;
    sync(Repo::upsert(makeTestAssignedKey(key, SENTINEL)));

    auto cached = sync(Repo::find(key));
    REQUIRE(cached != nullptr);
    REQUIRE(cached->payload == SENTINEL);
    REQUIRE(dbAkeyPayload(key) == SENTINEL);
}

TEST_CASE("Concurrency - concurrent upsert on shared key",
          "[integration][db][redis][concurrency][tsan][upsert]")
{
    TransactionGuard tx;

    SECTION("[L1] N threads upsert the same key") {
        runConcurrentUpsertSameKey<L1TestAssignedKeyRepo>(700001);
    }
    SECTION("[L1+L2] N threads upsert the same key") {
        runConcurrentUpsertSameKey<FullCacheTestAssignedKeyRepo>(700002);
    }
    SECTION("[L1] upsert || update || erase on the same key") {
        runUpsertUpdateEraseSameKey<L1TestAssignedKeyRepo>(700003);
    }
    SECTION("[L1+L2] upsert || update || erase on the same key") {
        runUpsertUpdateEraseSameKey<FullCacheTestAssignedKeyRepo>(700004);
    }
}
