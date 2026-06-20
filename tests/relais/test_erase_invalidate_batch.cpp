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
#include "fixtures/generated/TestFilterOnlyEntity.h"

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

// ===========================================================================
// Étape 5 — public batch API: eraseMany(span<Key>) / invalidateMany(span<Key>)
// over real repos (DB + cache).
//   §3 — erase vs invalidate distinction (the semantic split)
//   §2 — metamorphic oracle: batch ≡ a mono sequence (trusted reference)
//   §5 — edge sets: empty / singleton / duplicates / absent ids
//   §4 — cross-inval propagated by the public eraseMany
// ===========================================================================

namespace {

bool userExists(int64_t id) {
    auto r = execQueryArgs(
        "SELECT COUNT(*) FROM relais_test_users WHERE id = $1", id);
    return r[0].get<int64_t>(0) == 1;
}

// Oracle — eraseMany(setA) must reach the SAME observable state as a mono
// erase loop over a symmetric setB: identical affected count, every id gone
// from find() AND from L3. The mono path is the trusted reference (§2).
template<typename Repo>
void eraseManyMatchesMonoLoop() {
    int64_t a1 = insertTestUser("om_a1", "om_a1@x", 1);
    int64_t a2 = insertTestUser("om_a2", "om_a2@x", 2);
    int64_t a3 = insertTestUser("om_a3", "om_a3@x", 3);
    int64_t b1 = insertTestUser("om_b1", "om_b1@x", 1);
    int64_t b2 = insertTestUser("om_b2", "om_b2@x", 2);
    int64_t b3 = insertTestUser("om_b3", "om_b3@x", 3);
    for (int64_t id : {a1, a2, a3, b1, b2, b3}) sync(Repo::find(id));  // warm

    std::vector<int64_t> A = {a1, a2, a3};
    auto n = sync(Repo::eraseMany(std::span<const int64_t>(A)));

    size_t m = 0;
    for (int64_t id : {b1, b2, b3}) {
        auto r = sync(Repo::erase(id));
        if (r) m += *r;
    }

    REQUIRE(n.has_value());
    REQUIRE(*n == 3);
    REQUIRE(*n == m);  // batch count == mono-loop count
    for (int64_t id : {a1, a2, a3, b1, b2, b3}) {
        REQUIRE_FALSE(sync(Repo::find(id)));  // gone from every tier
        REQUIRE_FALSE(userExists(id));        // gone from L3
    }
}

// Oracle — invalidateMany(setA) ≡ a mono invalidate loop over setB: both evict
// the cached copy (a stale DB mutation becomes visible on the next read) while
// leaving the L3 rows intact. Distinct per-id balances detect a stale survivor.
template<typename Repo>
void invalidateManyMatchesMonoLoop() {
    int64_t a1 = insertTestUser("im_a1", "im_a1@x", 10);
    int64_t a2 = insertTestUser("im_a2", "im_a2@x", 20);
    int64_t b1 = insertTestUser("im_b1", "im_b1@x", 30);
    int64_t b2 = insertTestUser("im_b2", "im_b2@x", 40);
    for (int64_t id : {a1, a2, b1, b2}) sync(Repo::find(id));  // warm cache

    // Mutate the rows under the cache — a cache hit would still see the old value.
    updateTestUserBalance(a1, 111);
    updateTestUserBalance(a2, 222);
    updateTestUserBalance(b1, 333);
    updateTestUserBalance(b2, 444);

    std::vector<int64_t> A = {a1, a2};
    sync(Repo::invalidateMany(std::span<const int64_t>(A)));
    for (int64_t id : {b1, b2}) sync(Repo::invalidate(id));

    // Both paths now refetch fresh from L3, and the rows are still present.
    auto fresh = [](int64_t id) {
        auto v = sync(Repo::find(id));
        REQUIRE(v);
        return v->balance;
    };
    REQUIRE(fresh(a1) == 111);
    REQUIRE(fresh(a2) == 222);
    REQUIRE(fresh(b1) == 333);
    REQUIRE(fresh(b2) == 444);
    for (int64_t id : {a1, a2, b1, b2}) REQUIRE(userExists(id));
}

}  // namespace

// ---------------------------------------------------------------------------
// §3 — the erase/invalidate distinction (the public contract split)
// ---------------------------------------------------------------------------

TEST_CASE("eraseMany removes from L3; invalidateMany evicts cache but keeps L3",
          "[public][many][integration][db][redis]") {
    TransactionGuard tx;
    int64_t u1 = insertTestUser("d_u1", "d_u1@x", 10);
    int64_t u2 = insertTestUser("d_u2", "d_u2@x", 20);
    std::vector<int64_t> ids = {u1, u2};

    SECTION("invalidateMany: evicted then repopulated from L3, rows intact") {
        sync(FullCacheTestUserRepo::find(u1));  // warm L1+L2
        sync(FullCacheTestUserRepo::find(u2));
        updateTestUserBalance(u1, 111);         // mutate under the cache
        updateTestUserBalance(u2, 222);

        sync(FullCacheTestUserRepo::invalidateMany(std::span<const int64_t>(ids)));

        // A surviving cache hit would still read 10/20 — fresh values prove eviction.
        auto a = sync(FullCacheTestUserRepo::find(u1));
        auto b = sync(FullCacheTestUserRepo::find(u2));
        REQUIRE(a);
        REQUIRE(a->balance == 111);
        REQUIRE(b);
        REQUIRE(b->balance == 222);
        REQUIRE(userExists(u1));  // still in L3
        REQUIRE(userExists(u2));
    }

    SECTION("eraseMany: gone from every tier and from L3") {
        sync(FullCacheTestUserRepo::find(u1));
        sync(FullCacheTestUserRepo::find(u2));

        auto n = sync(FullCacheTestUserRepo::eraseMany(std::span<const int64_t>(ids)));
        REQUIRE(n.has_value());
        REQUIRE(*n == 2);

        REQUIRE_FALSE(sync(FullCacheTestUserRepo::find(u1)));  // no L1 ghost
        REQUIRE_FALSE(sync(FullCacheTestUserRepo::find(u2)));
        REQUIRE_FALSE(userExists(u1));
        REQUIRE_FALSE(userExists(u2));
    }
}

// ---------------------------------------------------------------------------
// §2 — metamorphic oracle across every cache config
// ---------------------------------------------------------------------------

TEST_CASE("eraseMany ≡ mono erase loop (Uncached/L1/L2/Both)",
          "[public][many][oracle][integration][db][redis]") {
    TransactionGuard tx;
    SECTION("Uncached") { eraseManyMatchesMonoLoop<UncachedTestUserRepo>(); }
    SECTION("L1")       { eraseManyMatchesMonoLoop<L1TestUserRepo>(); }
    SECTION("L2")       { eraseManyMatchesMonoLoop<L2TestUserRepo>(); }
    SECTION("Both")     { eraseManyMatchesMonoLoop<FullCacheTestUserRepo>(); }
}

TEST_CASE("invalidateMany ≡ mono invalidate loop (Uncached/L1/L2/Both)",
          "[public][many][oracle][integration][db][redis]") {
    TransactionGuard tx;
    SECTION("Uncached") { invalidateManyMatchesMonoLoop<UncachedTestUserRepo>(); }
    SECTION("L1")       { invalidateManyMatchesMonoLoop<L1TestUserRepo>(); }
    SECTION("L2")       { invalidateManyMatchesMonoLoop<L2TestUserRepo>(); }
    SECTION("Both")     { invalidateManyMatchesMonoLoop<FullCacheTestUserRepo>(); }
}

// ---------------------------------------------------------------------------
// §5 — edge sets
// ---------------------------------------------------------------------------

TEST_CASE("eraseMany edge sets: empty / singleton / duplicates / absent ids",
          "[public][many][integration][db][redis]") {
    TransactionGuard tx;
    int64_t u1 = insertTestUser("e_u1", "e_u1@x", 1);
    int64_t u2 = insertTestUser("e_u2", "e_u2@x", 2);

    SECTION("empty set is a no-op: 0 affected, nothing deleted") {
        std::vector<int64_t> none;
        auto n = sync(FullCacheTestUserRepo::eraseMany(std::span<const int64_t>(none)));
        REQUIRE(n.has_value());
        REQUIRE(*n == 0);
        REQUIRE(userExists(u1));
        REQUIRE(userExists(u2));
    }

    SECTION("singleton degenerates to the mono path") {
        std::vector<int64_t> one = {u1};
        auto n = sync(FullCacheTestUserRepo::eraseMany(std::span<const int64_t>(one)));
        REQUIRE(n.has_value());
        REQUIRE(*n == 1);
        REQUIRE_FALSE(userExists(u1));
        REQUIRE(userExists(u2));
    }

    SECTION("duplicates collapse: counted once, no double-decrement") {
        std::vector<int64_t> dups = {u1, u1, u1};
        auto n = sync(FullCacheTestUserRepo::eraseMany(std::span<const int64_t>(dups)));
        REQUIRE(n.has_value());
        REQUIRE(*n == 1);  // one row, not three
        REQUIRE_FALSE(userExists(u1));
    }

    SECTION("absent ids excluded from the affected count") {
        std::vector<int64_t> ids = {u1, 999999, u2};  // 999999 never inserted
        auto n = sync(FullCacheTestUserRepo::eraseMany(std::span<const int64_t>(ids)));
        REQUIRE(n.has_value());
        REQUIRE(*n == 2);  // only u1, u2 existed
        REQUIRE_FALSE(userExists(u1));
        REQUIRE_FALSE(userExists(u2));
    }
}

TEST_CASE("invalidateMany edge sets: empty no-op, duplicates evict once",
          "[public][many][integration][db][redis]") {
    TransactionGuard tx;
    int64_t u1 = insertTestUser("ie_u1", "ie_u1@x", 10);

    SECTION("empty set evicts nothing (cached copy survives stale)") {
        sync(FullCacheTestUserRepo::find(u1));     // warm
        updateTestUserBalance(u1, 999);            // mutate under cache
        std::vector<int64_t> none;
        sync(FullCacheTestUserRepo::invalidateMany(std::span<const int64_t>(none)));
        // No eviction → the next read still hits the stale cached balance.
        auto v = sync(FullCacheTestUserRepo::find(u1));
        REQUIRE(v);
        REQUIRE(v->balance == 10);
    }

    SECTION("duplicate ids evict once, then refetch fresh") {
        sync(FullCacheTestUserRepo::find(u1));
        updateTestUserBalance(u1, 555);
        std::vector<int64_t> dups = {u1, u1, u1};
        sync(FullCacheTestUserRepo::invalidateMany(std::span<const int64_t>(dups)));
        auto v = sync(FullCacheTestUserRepo::find(u1));
        REQUIRE(v);
        REQUIRE(v->balance == 555);
        REQUIRE(userExists(u1));
    }
}

// ---------------------------------------------------------------------------
// §4 — cross-invalidation propagated by the public eraseMany
// ---------------------------------------------------------------------------

TEST_CASE("eraseMany propagates deduplicated cross-inval to the target repo",
          "[public][many][indirect][integration][db]") {
    TransactionGuard tx;
    int64_t u1 = insertTestUser("xi_u1", "xi_u1@x", 1);
    int64_t u2 = insertTestUser("xi_u2", "xi_u2@x", 2);

    // Warm the cross-inval target repo (L1TestUserRepo) so an eviction is observable.
    sync(L1TestUserRepo::find(u1));
    sync(L1TestUserRepo::find(u2));
    REQUIRE(TestInternals::getFromCache<L1TestUserRepo>(u1));
    REQUIRE(TestInternals::getFromCache<L1TestUserRepo>(u2));

    // Two purchases on u1, one on u2 → target set {u1,u2} (u1 deduped).
    int64_t p1 = insertTestPurchase(u1, "a", 1);
    int64_t p2 = insertTestPurchase(u1, "b", 2);
    int64_t p3 = insertTestPurchase(u2, "c", 3);
    for (int64_t pid : {p1, p2, p3}) sync(L1TestPurchaseRepo::find(pid));

    std::vector<int64_t> pids = {p1, p2, p3};
    auto n = sync(L1TestPurchaseRepo::eraseMany(std::span<const int64_t>(pids)));
    REQUIRE(n.has_value());
    REQUIRE(*n == 3);

    // Purchases deleted; both target users invalidated via deduped cross-inval.
    REQUIRE_FALSE(TestInternals::getFromCache<L1TestUserRepo>(u1));
    REQUIRE_FALSE(TestInternals::getFromCache<L1TestUserRepo>(u2));
}

// ===========================================================================
// Étape 6 — public eraseWhere / invalidateWhere (predicate, HasFilterSet)
// ===========================================================================

// §1 — concept availability (compile-time). The decorrelation: a filters-only
// entity exposes a FilterSet (→ where-variants) WITHOUT a ListDescriptor (so
// ListMixin stays out of its chain); a list entity's ListDescriptor inherits
// its FilterSet, so it satisfies both.
static_assert(jcailloux::relais::HasFilterSet<
                  entity::generated::TestFilterOnlyEntity>,
    "filters-only entity must expose a FilterSet");
static_assert(!jcailloux::relais::HasListDescriptor<
                  entity::generated::TestFilterOnlyEntity>,
    "filters-only entity must NOT carry a ListDescriptor");
static_assert(jcailloux::relais::HasFilterSet<TestArticleEntity>,
    "a list entity's ListDescriptor inherits its FilterSet");
static_assert(jcailloux::relais::HasListDescriptor<TestArticleEntity>,
    "TestArticle is a list entity");

namespace {

// Full-cache Article repo (L1+L2+L3 + own lists) for the where oracle.
using WhereArticleRepo = jcailloux::relais::Repo<
    TestArticleEntity, "test:article:where:both", cfg::Both>;

using ArticlePred = jcailloux::relais::FilterSet<TestArticleEntity>;

// The predicate's reference id set, resolved straight from L3.
std::vector<int64_t> articleIdsByAuthor(int64_t author) {
    auto r = execQueryArgs(
        "SELECT id FROM relais_test_articles WHERE author_id = $1 ORDER BY id",
        author);
    std::vector<int64_t> ids;
    for (int i = 0; i < r.rows(); ++i) ids.push_back(r[i].get<int64_t>(0));
    return ids;
}

int64_t articleCount() {
    return execQuery("SELECT COUNT(*) FROM relais_test_articles")[0]
        .get<int64_t>(0);
}

int64_t articleCountByAuthor(int64_t author) {
    return execQueryArgs(
        "SELECT COUNT(*) FROM relais_test_articles WHERE author_id = $1",
        author)[0].get<int64_t>(0);
}

}  // namespace

// §5 — selectivity: the predicate deletes exactly its matched rows from L3,
// returns their count, leaves the rest. Designated-init names the param.
TEST_CASE("eraseWhere deletes exactly the predicate's rows from L3",
          "[public][where][integration][db]") {
    TransactionGuard tx;
    auto author = insertTestUser("ew_a", "ew_a@x", 0);
    auto other = insertTestUser("ew_b", "ew_b@x", 0);
    insertTestArticle("tech", author, "A1", 10);
    insertTestArticle("news", author, "A2", 20);
    insertTestArticle("tech", author, "A3", 30);
    insertTestArticle("tech", other, "B1", 40);
    insertTestArticle("tech", other, "B2", 50);

    auto n = sync(WhereArticleRepo::eraseWhere({.author_id = author}));
    REQUIRE(n.has_value());
    REQUIRE(*n == 3);
    REQUIRE(articleCountByAuthor(author) == 0);
    REQUIRE(articleCountByAuthor(other) == 2);  // untouched
}

// §2 — oracle: eraseWhere(P) ≡ eraseMany(SELECT pk WHERE P). Two symmetric
// author sets; the predicate path and the resolved-id path must agree on count
// and final state.
TEST_CASE("eraseWhere ≡ eraseMany over the resolved id set",
          "[public][where][oracle][integration][db]") {
    TransactionGuard tx;
    auto a = insertTestUser("ewo_a", "ewo_a@x", 0);
    auto b = insertTestUser("ewo_b", "ewo_b@x", 0);
    for (int i = 0; i < 3; ++i) {
        insertTestArticle("tech", a, "A" + std::to_string(i), i);
        insertTestArticle("tech", b, "B" + std::to_string(i), i);
    }

    auto idsB = articleIdsByAuthor(b);
    auto nWhere = sync(WhereArticleRepo::eraseWhere({.author_id = a}));
    auto nMany = sync(WhereArticleRepo::eraseMany(std::span<const int64_t>(idsB)));

    REQUIRE(nWhere.has_value());
    REQUIRE(nMany.has_value());
    REQUIRE(*nWhere == *nMany);
    REQUIRE(*nWhere == 3);
    REQUIRE(articleCount() == 0);  // both authors' rows gone
}

// §3 — invalidateWhere evicts the cache for the matched rows but leaves L3
// intact (the rows still exist → a later find refetches fresh). Proven by the
// staleness trick: warm, mutate L3 directly, invalidate, observe the fresh value.
TEST_CASE("invalidateWhere evicts cache by predicate, keeps L3",
          "[public][where][integration][db][redis]") {
    TransactionGuard tx;
    auto author = insertTestUser("iw_a", "iw_a@x", 0);
    auto id1 = insertTestArticle("tech", author, "T1", 10);
    auto id2 = insertTestArticle("tech", author, "T2", 20);

    sync(WhereArticleRepo::find(id1));  // warm L1+L2
    sync(WhereArticleRepo::find(id2));

    updateTestArticle(id1, "T1", 111);  // mutate L3 behind the cache
    updateTestArticle(id2, "T2", 222);

    sync(WhereArticleRepo::invalidateWhere({.author_id = author}));

    auto v1 = sync(WhereArticleRepo::find(id1));
    auto v2 = sync(WhereArticleRepo::find(id2));
    REQUIRE(v1);
    REQUIRE(v2);
    REQUIRE((*v1).view_count == 111);  // refetched fresh, not the stale 10
    REQUIRE((*v2).view_count == 222);
    REQUIRE(articleCountByAuthor(author) == 2);  // still in L3
}

// §2 — oracle: invalidateWhere(P) ≡ invalidateMany(SELECT pk WHERE P). Both
// leave L3 intact and evict the matched set; symmetric authors must agree.
TEST_CASE("invalidateWhere ≡ invalidateMany over the resolved id set",
          "[public][where][oracle][integration][db][redis]") {
    TransactionGuard tx;
    auto a = insertTestUser("iwo_a", "iwo_a@x", 0);
    auto b = insertTestUser("iwo_b", "iwo_b@x", 0);
    auto a1 = insertTestArticle("tech", a, "A1", 10);
    auto b1 = insertTestArticle("tech", b, "B1", 10);

    for (int64_t id : {a1, b1}) sync(WhereArticleRepo::find(id));
    updateTestArticle(a1, "A1", 777);
    updateTestArticle(b1, "B1", 777);

    auto idsB = articleIdsByAuthor(b);
    sync(WhereArticleRepo::invalidateWhere({.author_id = a}));
    sync(WhereArticleRepo::invalidateMany(std::span<const int64_t>(idsB)));

    auto va = sync(WhereArticleRepo::find(a1));
    auto vb = sync(WhereArticleRepo::find(b1));
    REQUIRE((*va).view_count == 777);  // both evicted → both fresh
    REQUIRE((*vb).view_count == 777);
    REQUIRE(articleCount() == 2);  // neither path deleted
}

// §5 — empty predicate is an unconditional purge of the table.
TEST_CASE("eraseWhere with an empty predicate purges every row",
          "[public][where][integration][db]") {
    TransactionGuard tx;
    auto a = insertTestUser("ewp_a", "ewp_a@x", 0);
    insertTestArticle("tech", a, "P1", 1);
    insertTestArticle("news", a, "P2", 2);
    insertTestArticle("tech", a, "P3", 3);
    REQUIRE(articleCount() == 3);

    auto n = sync(WhereArticleRepo::eraseWhere(ArticlePred{}));
    REQUIRE(n.has_value());
    REQUIRE(*n == 3);
    REQUIRE(articleCount() == 0);
}
