/**
 * test_erase_invalidate_batch.cpp
 * Common batch invalidation path (invalidateManyImpl) + its deduplicated
 * cross-invalidation core.
 *
 * Étape 4 du plan erase/invalidate batch :
 *   - detail::dedupSorted(vector<K>) — pure: set(input), never drops a distinct
 *     key, idempotent on duplicates.
 *   - Invalidate<>::targetKeysForDelete(span) — pure: N source entities → M ≤ N
 *     distinct target keys.
 *   - InvalidateVia<>::invalidateManyForDelete(span) — indirect cross-inval,
 *     batched: dedup sources, resolve (per-source loop OR opt-in span overload),
 *     dedup targets, invalidate each once. M distinct targets, M < N.
 *   - invalidateManyImpl(span<const E>) — the shared downstream of every batch
 *     op: L1 evict + L2 UNLINK + list tracker + deduplicated cross-inval.
 *
 * SECTION tags:
 *   [dedup]      — pure dedup (no DB, no Redis) — the §1 units
 *   [indirect]   — InvalidateVia batched cross-inval (mock target + counting resolver)
 *   [many]       — invalidateManyImpl over real repos (L1 evict / list tracker)
 */

#include <atomic>
#include <span>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

#include <jcailloux/relais/repository/InvalidateOn.h>

using namespace relais_test;
namespace rdetail = jcailloux::relais::detail;
using jcailloux::relais::Invalidate;
using jcailloux::relais::InvalidateVia;

// ---------------------------------------------------------------------------
// §1 — pure dedup: the dangerous failure mode is dropping a distinct target,
// not keeping a duplicate. Assert set-equality + idempotence, no I/O.
// ---------------------------------------------------------------------------

TEST_CASE("dedupSorted collapses duplicates, never drops a distinct key",
          "[dedup]") {
    SECTION("duplicates collapse, distinct survive, sorted") {
        std::vector<int64_t> in = {7, 3, 7, 1, 3, 7, 1};
        auto out = rdetail::dedupSorted(in);
        REQUIRE(out == std::vector<int64_t>{1, 3, 7});
    }
    SECTION("already-unique input is preserved (sorted)") {
        std::vector<int64_t> in = {5, 2, 9};
        auto out = rdetail::dedupSorted(in);
        REQUIRE(out == std::vector<int64_t>{2, 5, 9});
    }
    SECTION("idempotent: dedup of a deduped set is itself") {
        std::vector<int64_t> in = {4, 4, 2, 2, 8};
        auto once = rdetail::dedupSorted(in);
        auto twice = rdetail::dedupSorted(once);
        REQUIRE(once == twice);
    }
    SECTION("empty stays empty") {
        REQUIRE(rdetail::dedupSorted(std::vector<int64_t>{}).empty());
    }
    SECTION("all-identical collapses to one") {
        std::vector<int64_t> in(1000, 42);
        REQUIRE(rdetail::dedupSorted(in) == std::vector<int64_t>{42});
    }
}

namespace {

// A cache target that records the keys it was asked to invalidate, so a test
// can assert the exact deduplicated set (and its size) without real I/O.
struct MockTarget {
    static inline std::vector<int64_t> invalidated;
    static void reset() { invalidated.clear(); }
    static io::Task<void> invalidate(int64_t k) {
        invalidated.push_back(k);
        co_return;
    }
};

// Counting resolvers for the indirect path. user_id → user_id/10 collapses
// several distinct sources onto one target (proves target dedup, M < N).
std::atomic<int> g_loopCalls{0};
std::atomic<int> g_batchCalls{0};

inline constexpr auto loopResolver = [](int64_t uid) -> io::Task<std::vector<int64_t>> {
    g_loopCalls.fetch_add(1, std::memory_order_relaxed);
    co_return std::vector<int64_t>{uid / 10};
};

inline constexpr auto batchResolver =
    [](std::span<const int64_t> uids) -> io::Task<std::vector<int64_t>> {
    g_batchCalls.fetch_add(1, std::memory_order_relaxed);
    std::vector<int64_t> out;
    out.reserve(uids.size());
    for (auto u : uids) out.push_back(u / 10);
    co_return out;
};

std::vector<TestPurchaseEntity> purchasesForUsers(std::span<const int64_t> user_ids) {
    std::vector<TestPurchaseEntity> ps;
    ps.reserve(user_ids.size());
    int64_t pid = 1;
    for (auto uid : user_ids) {
        auto p = makeTestPurchase(uid, "p", 1, "pending", pid++);
        ps.push_back(std::move(p));
    }
    return ps;
}

}  // namespace

TEST_CASE("targetKeysForDelete dedups direct cross-inval keys (N → M)",
          "[dedup]") {
    // user_ids [5,5,7,5,7] → distinct targets {5,7}; the 3 duplicate 5s collapse.
    int64_t uids[] = {5, 5, 7, 5, 7};
    auto purchases = purchasesForUsers(uids);

    using Direct = Invalidate<MockTarget, purchaseUserId>;
    auto keys = Direct::targetKeysForDelete<TestPurchaseEntity>(
        std::span<const TestPurchaseEntity>(purchases));

    REQUIRE(keys == std::vector<int64_t>{5, 7});  // M=2 < N=5, no key dropped
}

// ---------------------------------------------------------------------------
// §4 — indirect cross-inval batched. N entities → M distinct targets (M < N).
// Per-source loop (default) and span overload (opt-in) yield the SAME M
// targets; the overload collapses the resolver to a single call.
// ---------------------------------------------------------------------------

TEST_CASE("InvalidateVia batch: per-source resolver — M distinct targets",
          "[indirect][dedup]") {
    MockTarget::reset();
    g_loopCalls.store(0, std::memory_order_relaxed);

    // 5 distinct sources → targets {1,1,2,2,3} → deduped {1,2,3}.
    int64_t uids[] = {10, 11, 20, 21, 30};
    auto purchases = purchasesForUsers(uids);

    using Via = InvalidateVia<MockTarget, purchaseUserId, loopResolver>;
    sync(Via::invalidateManyForDelete<TestPurchaseEntity>(
        std::span<const TestPurchaseEntity>(purchases)));

    REQUIRE(MockTarget::invalidated == std::vector<int64_t>{1, 2, 3});  // M=3
    REQUIRE(g_loopCalls.load() == 5);  // one resolve per *distinct source*
}

TEST_CASE("InvalidateVia batch: span resolver collapses to one call, same targets",
          "[indirect][dedup]") {
    MockTarget::reset();
    g_batchCalls.store(0, std::memory_order_relaxed);

    int64_t uids[] = {10, 11, 20, 21, 30};
    auto purchases = purchasesForUsers(uids);

    using Via = InvalidateVia<MockTarget, purchaseUserId, batchResolver>;
    sync(Via::invalidateManyForDelete<TestPurchaseEntity>(
        std::span<const TestPurchaseEntity>(purchases)));

    REQUIRE(MockTarget::invalidated == std::vector<int64_t>{1, 2, 3});  // same M
    REQUIRE(g_batchCalls.load() == 1);  // one round-trip for the whole set
}

TEST_CASE("InvalidateVia batch: shared source collapses resolver calls too",
          "[indirect][dedup]") {
    MockTarget::reset();
    g_loopCalls.store(0, std::memory_order_relaxed);

    // 5 entities, only 2 distinct sources → 2 resolver calls, 2 targets.
    int64_t uids[] = {10, 10, 10, 20, 20};
    auto purchases = purchasesForUsers(uids);

    using Via = InvalidateVia<MockTarget, purchaseUserId, loopResolver>;
    sync(Via::invalidateManyForDelete<TestPurchaseEntity>(
        std::span<const TestPurchaseEntity>(purchases)));

    REQUIRE(MockTarget::invalidated == std::vector<int64_t>{1, 2});
    REQUIRE(g_loopCalls.load() == 2);  // source-dedup before resolve
}

TEST_CASE("InvalidateVia batch: empty set is a no-op",
          "[indirect]") {
    MockTarget::reset();
    g_loopCalls.store(0, std::memory_order_relaxed);

    std::vector<TestPurchaseEntity> none;
    using Via = InvalidateVia<MockTarget, purchaseUserId, loopResolver>;
    sync(Via::invalidateManyForDelete<TestPurchaseEntity>(
        std::span<const TestPurchaseEntity>(none)));

    REQUIRE(MockTarget::invalidated.empty());
    REQUIRE(g_loopCalls.load() == 0);
}

// ---------------------------------------------------------------------------
// §4 — invalidateManyImpl over real repos: the cascade actually evicts.
// ---------------------------------------------------------------------------

TEST_CASE("invalidateManyImpl evicts the entity set from L1 + deduped cross-inval",
          "[many]") {
    // Pure L1 cascade (no DB/Redis): warm the entity + cross-inval target L1
    // directly, run the common path, assert both tiers are evicted.
    TestInternals::resetEntityCacheState<L1TestPurchaseRepo>();
    TestInternals::resetEntityCacheState<L1TestUserRepo>();

    const int64_t u1 = 1, u2 = 2;
    const int64_t p1 = 11, p2 = 12, p3 = 13;
    // Two purchases on u1, one on u2 → cross-inval target {u1,u2} (u1 deduped).
    std::vector<TestPurchaseEntity> affected = {
        makeTestPurchase(u1, "a", 1, "pending", p1),
        makeTestPurchase(u1, "b", 2, "pending", p2),
        makeTestPurchase(u2, "c", 3, "pending", p3),
    };
    for (const auto& p : affected)
        TestInternals::putInCache<L1TestPurchaseRepo>(p.key(), p);
    TestInternals::putInCache<L1TestUserRepo>(u1, makeTestUser("a", "a@x", 0, u1));
    TestInternals::putInCache<L1TestUserRepo>(u2, makeTestUser("b", "b@x", 0, u2));

    REQUIRE(TestInternals::getFromCache<L1TestPurchaseRepo>(p1));
    REQUIRE(TestInternals::getFromCache<L1TestUserRepo>(u1));

    sync(TestInternals::invalidateManyImpl<L1TestPurchaseRepo>(
        std::span<const TestPurchaseEntity>(affected)));

    // Purchases gone from L1 (point-evicts).
    REQUIRE_FALSE(TestInternals::getFromCache<L1TestPurchaseRepo>(p1));
    REQUIRE_FALSE(TestInternals::getFromCache<L1TestPurchaseRepo>(p2));
    REQUIRE_FALSE(TestInternals::getFromCache<L1TestPurchaseRepo>(p3));
    // Target users invalidated via deduplicated cross-inval.
    REQUIRE_FALSE(TestInternals::getFromCache<L1TestUserRepo>(u1));
    REQUIRE_FALSE(TestInternals::getFromCache<L1TestUserRepo>(u2));
}

TEST_CASE("invalidateManyImpl bumps the list tracker per affected entity",
          "[many]") {
    TestInternals::resetListCacheState<TestArticleListRepo>();
    uint32_t gen0 = TestInternals::listCacheGeneration<TestArticleListRepo>();

    std::vector<TestArticleEntity> articles;
    articles.push_back(makeTestArticle("tech", 1, "t1", 0, true, 101));
    articles.push_back(makeTestArticle("tech", 1, "t2", 0, true, 102));
    articles.push_back(makeTestArticle("news", 2, "t3", 0, true, 103));

    sync(TestInternals::invalidateManyImpl<TestArticleListRepo>(
        std::span<const TestArticleEntity>(articles)));

    uint32_t gen1 = TestInternals::listCacheGeneration<TestArticleListRepo>();
    // L1-only list tier: one onEntityDeleted bump per affected entity (the
    // single-bump collapse is a downstream optimization, not yet wired).
    REQUIRE(gen1 == gen0 + articles.size());
}
