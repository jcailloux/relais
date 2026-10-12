// Cache-coherence suite — the repository contract under I/O timeouts and a down
// Redis. Where test_io_liveness proves the I/O layer stays live, this proves the
// *cache stays correct*: a write that times out after the server committed must
// never leave the old value served (scenario A), a write the server rolled back
// must keep the old value (scenario B), a down L2 must degrade to L3 instead of
// faking a miss or an error, and a read against a dead DB must surface the error
// instead of a false "absent".
//
// It drives the full mixin chain (Repo → … → PgRepo) on a ControllableIoContext,
// binding PgProvider to that loop on the test thread, and injects faults exactly
// where the liveness suite does: holdReads() withholds a connection's result so a
// committed write still trips query_timeout, and PgProbe reads ground truth over
// an independent libpq channel that never touches relais state.
//
// Note on isolation: the repos here use dedicated names (each Repo<…, "coh:…">
// owns its own static L1), and every fixture clears its tables via PgProbe and
// purges L1 — it does NOT rely on TransactionGuard, which despite its name runs
// autocommit DELETEs, not a BEGIN/ROLLBACK (tests/fixtures/test_helper.h).
//
// Fault injection lives entirely in tests/fixtures (ControllableIoContext,
// PgProbe) — nothing in include/ knows it is under test.

#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/repository/Repo.h>
#include <jcailloux/relais/config/CacheConfig.h>
#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/io/redis/RedisPool.h>
#include <jcailloux/relais/io/redis/RedisError.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/batch/TimingEstimator.h>
#include <jcailloux/relais/Log.h>
#include <jcailloux/relais/io/WhenAll.h>

#include <fixtures/ControllableIoContext.h>
#include <fixtures/PgProbe.h>
#include <fixtures/RelaisTestAccessors.h>
#include "fixtures/generated/TestItemEntity.h"
#include "fixtures/generated/TestAssignedKeyEntity.h"
#include "fixtures/generated/TestArticleEntity.h"
#include "fixtures/generated/TestFilterOnlyEntity.h"

#include <algorithm>
#include <chrono>
#include <coroutine>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

using namespace jcailloux::relais;
using namespace std::chrono_literals;

namespace io = jcailloux::relais::io;
namespace cfg = jcailloux::relais::config;
using Io = io::test::ControllableIoContext;

using ::entity::generated::TestItemEntity;
using ::entity::generated::TestAssignedKeyEntity;
using ::entity::generated::TestArticleEntity;

// ThreadSanitizer slows the loop ~5x, so the self-heal retry/backoff cadence
// needs a wider convergence window than the plain build.
#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#    define RELAIS_TSAN_BUILD 1
#  endif
#endif
#if defined(__SANITIZE_THREAD__)
#  define RELAIS_TSAN_BUILD 1
#endif
#ifdef RELAIS_TSAN_BUILD
static constexpr auto kSelfHealBudget = std::chrono::seconds(8);
#else
static constexpr auto kSelfHealBudget = std::chrono::seconds(2);
#endif

// =============================================================================
// Connection strings
// =============================================================================

// The relais pool carries a distinct application_name so PgProbe can terminate
// exactly its backend (the clean DB-down read-path case) without touching the
// probe's own session.
static const char* kAppName = "relais_coh_under_test";

static std::string poolConnInfo() {
    return "host=localhost port=5432 dbname=relais_test user=relais_test "
           "password=relais_test application_name=" + std::string(kAppName);
}
static const char* probeConnInfo() {
    return "host=localhost port=5432 dbname=relais_test "
           "user=relais_test password=relais_test";
}

// =============================================================================
// Test repositories — each owns its own static L1 (dedicated name)
// =============================================================================

using CohItemL1 = Repo<TestItemEntity, "coh:item:l1">;           // cfg::Local (L1 only)
using CohItemBoth = Repo<TestItemEntity, "coh:item:both", cfg::Both>;  // L1 + L2
// l1_ttl=0: the read-fill copy never TTL-heals, so a stale L2 entry would persist —
// this is the config that exposes the staleness bound and proves the self-heal closes it.
using CohItemTtl0 =
    Repo<TestItemEntity, "coh:item:ttl0", cfg::Both.with_l1_ttl(std::chrono::seconds{0})>;
// Assigned-PK, L1 only — the shape upsert applies to. The key is known before the
// call, so the uncertain-timeout handler evicts by precaution on the update model.
using CohAkeyL1 = Repo<TestAssignedKeyEntity, "coh:akey:l1">;
// List repos (TestArticleEntity has a ListDescriptor → ListMixin), with an L2 tier.
using CohArticleRedis = Repo<TestArticleEntity, "coh:article:redis", cfg::Redis>;
using CohArticleBoth = Repo<TestArticleEntity, "coh:article:both", cfg::Both>;
// A list read on no cache tier reaches PostgreSQL every time.
using CohArticlePg = Repo<TestArticleEntity, "coh:article:pg", cfg::Uncached>;
// Filters without a list, for countWhere.
using CohCount = Repo<::entity::generated::TestFilterOnlyEntity, "coh:count">;
using CountPred = ::entity::generated::TestFilterOnlyMapping::FilterSet::Values;

// =============================================================================
// Bounded synchronous driver (mirrors test_io_liveness): a coherence suite must
// never hang the runner, so every Task runs against a wall-clock budget.
// =============================================================================

struct Starter {
    struct promise_type {
        Starter get_return_object() noexcept {
            return Starter{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept {}
    };
    std::coroutine_handle<promise_type> handle;
};

template<typename T>
T driveBounded(Io& io, io::Task<T> task, std::chrono::milliseconds budget = 5s) {
    std::variant<std::monostate, T, std::exception_ptr> result;
    bool done = false;
    auto wrapper = [&](io::Task<T> t) -> io::Task<void> {
        try { result.template emplace<1>(co_await std::move(t)); }
        catch (...) { result.template emplace<2>(std::current_exception()); }
        done = true;
    };
    auto starter = [&](io::Task<void> t) -> Starter { co_await std::move(t); };
    auto s = starter(wrapper(std::move(task)));
    auto deadline = std::chrono::steady_clock::now() + budget;
    while (!done && std::chrono::steady_clock::now() < deadline)
        io.runOnce(20);
    if (s.handle) s.handle.destroy();
    REQUIRE(done);
    if (auto* ex = std::get_if<2>(&result)) std::rethrow_exception(*ex);
    return std::move(std::get<1>(result));
}

inline void driveBounded(Io& io, io::Task<void> task,
                         std::chrono::milliseconds budget = 5s) {
    std::exception_ptr ex;
    bool done = false;
    auto wrapper = [&](io::Task<void> t) -> io::Task<void> {
        try { co_await std::move(t); } catch (...) { ex = std::current_exception(); }
        done = true;
    };
    auto starter = [&](io::Task<void> t) -> Starter { co_await std::move(t); };
    auto s = starter(wrapper(std::move(task)));
    auto deadline = std::chrono::steady_clock::now() + budget;
    while (!done && std::chrono::steady_clock::now() < deadline)
        io.runOnce(20);
    if (s.handle) s.handle.destroy();
    REQUIRE(done);
    if (ex) std::rethrow_exception(ex);
}

// find()/insert()/patch() return Immediate<T> (an L1 hit resolves synchronously);
// resolve it directly when ready, otherwise drive its Task.
template<typename T>
T driveBounded(Io& io, io::Immediate<T> imm, std::chrono::milliseconds budget = 5s) {
    if (imm.await_ready()) return imm.await_resume();
    return driveBounded(io, imm.take_task(), budget);
}

// Spin the loop for a window so any deferred resume / self-heal drain / timer
// surfaces (under ASan/TSan) instead of going unobserved.
static void pump(Io& io, std::chrono::milliseconds window = 100ms) {
    auto deadline = std::chrono::steady_clock::now() + window;
    while (std::chrono::steady_clock::now() < deadline)
        io.runOnce(10);
}

// =============================================================================
// CohEnv — one event loop + pool (+ optional Redis) + bound PgProvider, plus a
// PgProbe for ground truth. The whole repo stack runs on this single loop on the
// test thread; holdPg() withholds the pool connection's result.
// =============================================================================

struct CohEnv {
    Io io;
    std::shared_ptr<io::PgPool<Io>> pool;
    std::shared_ptr<io::RedisClient<Io>> redis;
    std::shared_ptr<io::batch::BatchScheduler<Io>> batcher;
    io::test::PgProbe probe;
    int pg_fd = -1;
    int redis_fd = -1;
    std::chrono::milliseconds q_timeout_;
    std::chrono::milliseconds r_timeout_;

    explicit CohEnv(std::chrono::milliseconds query_timeout,
                    bool with_redis = false,
                    std::chrono::milliseconds redis_query_timeout = 200ms)
        : probe(probeConnInfo()),
          q_timeout_(query_timeout), r_timeout_(redis_query_timeout)
    {
        // min=max=1: a single connection serves every op, so holdReads(pg_fd)
        // deterministically withholds the result of whatever the repo runs.
        pool = driveBounded(io, io::PgPool<Io>::create(io, poolConnInfo(),
            {.min_connections = 1, .max_connections = 1,
             .acquire_timeout = 2s, .query_timeout = query_timeout}));
        pg_fd = io.lastAddedFd();  // the connection min_connections just built

        std::shared_ptr<io::RedisPool<Io>> redis_pool;
        if (with_redis) {
            redis = driveBounded(io, io::RedisClient<Io>::connect(
                io, "127.0.0.1", 6379, redis_query_timeout));
            redis_fd = io.lastAddedFd();
            // A reconnect factory so the self-heal can revive a connection poisoned
            // by a timed-out L2 op — the revived connection gets a FRESH fd (not the
            // held one), which is what lets the self-heal case converge after a down window.
            Io* iop = &io;
            auto qt = redis_query_timeout;
            io::RedisPool<Io>::ReconnectFactory factory =
                [iop, qt](size_t) -> io::Task<std::shared_ptr<io::RedisClient<Io>>> {
                    return io::RedisClient<Io>::connect(*iop, "127.0.0.1", 6379, qt);
                };
            redis_pool = std::make_shared<io::RedisPool<Io>>(
                io::RedisPool<Io>::fromClients({redis}, std::move(factory)));
        }

        // Build the batcher directly (not PgProvider::init) so Redis carries the
        // reconnect factory above. Bound on the test thread, which driveBounded
        // already stamped as the loop thread.
        batcher = std::make_shared<io::batch::BatchScheduler<Io>>(
            io, pool, redis_pool, 8);
        PgProvider::bindBatcher<Io>(batcher, redis_pool != nullptr);
    }

    ~CohEnv() {
        // The manual driver has no IoPool to drain detached coroutines (L2 fills,
        // self-heal retries) still suspended on a held/poisoned connection when a
        // test ends. Mirror IoPool's ordered teardown: release the holds, stop the
        // self-heal re-arming, and pump past the longest in-flight bound so every
        // suspended frame fires its timer and unwinds — otherwise its frame leaks
        // (ASan/LSan). quiescence ends it early when nothing is in flight.
        releasePg();
        releaseRedis();
        if (batcher) batcher->beginShutdown();
        // Pump unconditionally past the longest per-op bound: a detached single-
        // send L2 fill is NOT counted by quiescentForTeardown(), so its watch-
        // bound timer must be given time to fire and unwind the frame.
        auto window = std::max(q_timeout_, r_timeout_) + 400ms;
        auto deadline = std::chrono::steady_clock::now() + window;
        while (std::chrono::steady_clock::now() < deadline)
            io.runOnce(20);
        PgProvider::reset();
    }

    CohEnv(const CohEnv&) = delete;
    CohEnv& operator=(const CohEnv&) = delete;

    void holdPg()    { io.holdReads(pg_fd); }
    void releasePg() { io.releaseReads(pg_fd); }
    void holdRedis()    { if (redis_fd >= 0) io.holdReads(redis_fd); }
    void releaseRedis() { if (redis_fd >= 0) io.releaseReads(redis_fd); }

    // Seed one relais_test_items row, return its id.
    int64_t seedItem(const std::string& name, int32_t value) {
        auto r = probe.query(
            "INSERT INTO relais_test_items (name, value, is_active) VALUES ('"
            + name + "', " + std::to_string(value) + ", true) RETURNING id");
        return std::stoll(r.get(0, 0));
    }
};

// Clear the items table + L1 so each case starts clean (no TransactionGuard).
// purge() only sweeps *expired* entries, so a true reset needs the unconditional
// test-only clear (the static L1 is shared across cases of the same repo).
static void clearL1() {
    relais_test::TestInternals::resetEntityCacheState<CohItemL1>();
}
static void resetItems(CohEnv& env) {
    env.probe.query("DELETE FROM relais_test_items");
    clearL1();
}

static TestItemEntity item(int64_t id, std::string name, int32_t value) {
    TestItemEntity e;
    e.id = id;
    e.name = std::move(name);
    e.value = value;
    e.is_active = true;
    return e;
}

// Ground-truth value of a row, or "<absent>".
static std::string probeValue(CohEnv& env, int64_t id) {
    auto r = env.probe.query(
        "SELECT value FROM relais_test_items WHERE id=" + std::to_string(id));
    return r.empty() ? std::string("<absent>") : r.get(0, 0);
}

// queryParams() prepares its statement (a Parse round-trip) before sending the
// query. Under a held read that Parse would hang first and the target statement
// would never reach the server — so a scenario-A write must run each write shape
// once unheld to cache its prepared statement on the single pooled connection.
// Then a hold exercises only the target op's result wait: the statement is sent,
// the server commits, and only its ACK is withheld.
static void prewarmWriteStmts(CohEnv& env) {
    auto t = env.seedItem("prewarm", 0);
    driveBounded(env.io, CohItemL1::update(t, item(t, "prewarm", 1)));      // UPDATE
    int64_t one[1] = {t};
    driveBounded(env.io, CohItemL1::eraseMany(std::span<const int64_t>(one)));  // ANY-DELETE
    auto c = driveBounded(env.io, CohItemL1::insert(item(0, "prewarm2", 0)));   // INSERT
    driveBounded(env.io, CohItemL1::erase(c->id));                              // DELETE
    env.probe.query("DELETE FROM relais_test_items");
    clearL1();
}

// -- assigned-key helpers (upsert applies to caller-assigned PKs) --------------

static TestAssignedKeyEntity akey(int64_t id, int64_t payload, std::string note = "") {
    TestAssignedKeyEntity e;
    e.key_id = id;
    e.payload = payload;
    e.note = std::move(note);
    return e;
}
static void resetAkey(CohEnv& env) {
    env.probe.query("DELETE FROM relais_test_assigned_keys");
    relais_test::TestInternals::resetEntityCacheState<CohAkeyL1>();
}
static void seedAkey(CohEnv& env, int64_t id, int64_t payload) {
    env.probe.query("INSERT INTO relais_test_assigned_keys (key_id, payload) VALUES ("
                    + std::to_string(id) + ", " + std::to_string(payload) + ")");
}
static std::string probeAkey(CohEnv& env, int64_t id) {
    auto r = env.probe.query(
        "SELECT payload FROM relais_test_assigned_keys WHERE key_id=" + std::to_string(id));
    return r.empty() ? std::string("<absent>") : r.get(0, 0);
}
// The upsert statement is a distinct prepared statement — run it once unheld so a
// later held call exercises only its result wait, not the Parse round-trip.
static void prewarmAkeyUpsert(CohEnv& env) {
    driveBounded(env.io, CohAkeyL1::upsert(akey(999'999, 0, "prewarm")));
    resetAkey(env);
}

// =============================================================================
// Read path — the most visible contract change: no more false 404.
// =============================================================================

TEST_CASE("coherence: read of a truly-absent row returns empty, not an error",
          "[coherence][readpath][integration]") {
    CohEnv env(300ms);
    resetItems(env);

    auto v = driveBounded(env.io, CohItemL1::find(999'999'999));
    REQUIRE_FALSE(static_cast<bool>(v));  // result.empty() precedes the (removed) catch
}

TEST_CASE("coherence: a read whose result is withheld throws PgQueryTimeout",
          "[coherence][readpath][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("read-timeout", 7);

    env.holdPg();  // the SELECT flushes, its rows never arrive → query_timeout
    REQUIRE_THROWS_AS(driveBounded(env.io, CohItemL1::find(id)),
                      io::PgQueryTimeout);
}

TEST_CASE("coherence: a read against a killed backend throws PgError, not empty",
          "[coherence][readpath][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("db-down", 11);

    // Clean server-side disconnect of exactly the relais backend.
    env.probe.terminateBackends(std::string("application_name = '") + kAppName + "'");

    // No false 404: the read surfaces the L3 error rather than reporting absent.
    REQUIRE_THROWS_AS(driveBounded(env.io, CohItemL1::find(id)), io::PgError);
}

// =============================================================================
// countWhere — a read: L3 errors surface, identical counts in flight coalesce.
// =============================================================================

static void seedCounts(CohEnv& env) {
    env.probe.query("DELETE FROM relais_test_filter_only");
    env.probe.query("INSERT INTO relais_test_filter_only (category, owner_id) VALUES "
                    "('coh', 1), ('coh', 1), ('coh', 2)");
}

static io::Task<std::vector<size_t>> countAll(std::vector<CountPred> preds) {
    std::vector<io::Task<size_t>> tasks;
    for (auto& p : preds) tasks.push_back(CohCount::countWhere(p));
    co_return co_await io::whenAll(std::move(tasks));
}

TEST_CASE("coherence: a withheld countWhere throws PgQueryTimeout",
          "[coherence][readpath][count][integration]") {
    CohEnv env(300ms);
    seedCounts(env);

    env.holdPg();
    REQUIRE_THROWS_AS(driveBounded(env.io, CohCount::countWhere({.category = "coh"})),
                      io::PgQueryTimeout);
}

TEST_CASE("coherence: countWhere against a killed backend throws PgError, not 0",
          "[coherence][readpath][count][integration]") {
    CohEnv env(300ms);
    seedCounts(env);

    env.probe.terminateBackends(std::string("application_name = '") + kAppName + "'");
    REQUIRE_THROWS_AS(driveBounded(env.io, CohCount::countWhere({.category = "coh"})),
                      io::PgError);
}

TEST_CASE("coherence: identical countWhere in flight share one statement",
          "[coherence][count][coalescing][integration]") {
    CohEnv env(1s);
    seedCounts(env);
    // Past bootstrap, the first read goes direct and the others join a batch.
    for (int i = 0; i < io::batch::TimingEstimator::kBootstrapThreshold + 2; ++i)
        driveBounded(env.io, CohCount::countWhere({}));

    SECTION("same predicate: the batched pair coalesces") {
        const CountPred p{.category = "coh"};
        auto before = env.batcher->pgReadsSent();
        auto counts = driveBounded(env.io, countAll({p, p, p}));
        CHECK(counts == std::vector<size_t>{3, 3, 3});
        CHECK(env.batcher->pgReadsSent() - before == 2);  // the direct one + the batch
    }
    SECTION("different values: one statement each") {
        auto before = env.batcher->pgReadsSent();
        auto counts = driveBounded(env.io, countAll(
            {{.owner_id = 1}, {.owner_id = 2}, {.owner_id = 3}}));
        CHECK(counts == std::vector<size_t>{2, 1, 0});
        CHECK(env.batcher->pgReadsSent() - before == 3);
    }
}

// =============================================================================
// List reads — one statement per query shape: limit and offset are parameters,
// so pages of a shape share one prepared statement and identical reads in
// flight coalesce.
// =============================================================================

namespace {

using ArticleDesc = CohArticlePg::ListDescriptorType;
namespace ld = jcailloux::relais::list::spec;

auto articlePage(uint16_t limit, uint32_t offset = 0,
                 std::optional<ld::TypedCursor<ArticleDesc>> cursor = std::nullopt) {
    ld::ListQueryParams<ArticleDesc> p;
    p.limit = limit;
    p.offset = offset;
    if (cursor) p.cursor = *cursor;
    p.filters.template get<"category">() = std::string("coh-shape");
    return ld::seal<ArticleDesc>(std::move(p));
}

std::vector<std::string> paramValues(const io::PgParams& params) {
    std::vector<std::string> out;
    for (const auto& p : params.params) out.emplace_back(p.isNull() ? "NULL" : std::string(p.data(), p.length()));
    return out;
}

void seedArticles(CohEnv& env, int n) {
    env.probe.query("DELETE FROM relais_test_articles");
    for (int i = 0; i < n; ++i)
        env.probe.query("INSERT INTO relais_test_articles (category, author_id, title) "
                        "VALUES ('coh-shape', 1, 't" + std::to_string(i) + "')");
}

// Statements prepared on the pool's single connection (the session's own view).
int64_t preparedOnConnection(CohEnv& env) {
    static const char* sql = "SELECT count(*) FROM pg_prepared_statements";
    auto r = driveBounded(env.io, PgProvider::queryParams(sql, io::PgParams{}));
    return r[0].get<int64_t>(0);
}

io::Task<std::vector<size_t>> pageSizes(std::vector<uint32_t> offsets) {
    std::vector<io::Task<size_t>> tasks;
    for (auto o : offsets)
        tasks.push_back([](uint32_t off) -> io::Task<size_t> {
            co_return (co_await CohArticlePg::query(articlePage(5, off)))->size();
        }(o));
    co_return co_await io::whenAll(std::move(tasks));
}

}  // namespace

TEST_CASE("list SQL: limit and offset are parameters of one text per shape",
          "[coherence][list][sql]") {
    using TI = relais_test::TestInternals;
    auto [a, pa] = TI::listQuerySql<CohArticlePg>(articlePage(10));
    auto [b, pb] = TI::listQuerySql<CohArticlePg>(articlePage(25));
    auto [c, pc] = TI::listQuerySql<CohArticlePg>(articlePage(10, 20));
    auto [d, pd] = TI::listQuerySql<CohArticlePg>(articlePage(25, 40));

    CHECK(a == b);  // same text, same pointer
    CHECK(c == d);
    CHECK(a != c);  // no offset is its own shape, not OFFSET $n bound to 0
    CHECK(std::string_view{a}.ends_with(" LIMIT $2"));
    CHECK(std::string_view{c}.ends_with(" LIMIT $2 OFFSET $3"));
    CHECK(paramValues(pa) == std::vector<std::string>{"coh-shape", "10"});
    CHECK(paramValues(pb) == std::vector<std::string>{"coh-shape", "25"});
    CHECK(paramValues(pd) == std::vector<std::string>{"coh-shape", "25", "40"});
}

TEST_CASE("list SQL: pages of one shape share one prepared statement",
          "[coherence][list][sql][integration]") {
    CohEnv env(1s);
    seedArticles(env, 30);

    // Cursor pages: filter $1, cursor ($2, $3), then LIMIT $4.
    auto first = driveBounded(env.io, CohArticlePg::query(articlePage(5)));
    REQUIRE(first->size() == 5);
    auto cursor = ld::TypedCursor<ArticleDesc>::decode(first->cursor());
    REQUIRE(cursor.has_value());
    auto [sql, params] =
        relais_test::TestInternals::listQuerySql<CohArticlePg>(articlePage(5, 0, cursor));
    CHECK(std::string_view{sql}.find("< ($2, $3)") != std::string_view::npos);
    CHECK(std::string_view{sql}.ends_with(" LIMIT $4"));
    CHECK(params.params.size() == 4);
    auto second = driveBounded(env.io, CohArticlePg::query(articlePage(5, 0, cursor)));
    CHECK(second->size() == 5);
    CHECK(second->items.front().id < first->items.back().id);

    driveBounded(env.io, CohArticlePg::query(articlePage(5, 5)));
    const auto prepared = preparedOnConnection(env);
    const auto timings = env.batcher->estimator().sql_timings.size();
    for (uint32_t offset = 10; offset < 30; offset += 5) {
        auto page = driveBounded(env.io, CohArticlePg::query(articlePage(5, offset)));
        CHECK(page->size() == 5);
    }
    driveBounded(env.io, CohArticlePg::query(articlePage(10)));
    CHECK(preparedOnConnection(env) == prepared);
    CHECK(env.batcher->estimator().sql_timings.size() == timings);
}

TEST_CASE("list SQL: identical list reads in flight share one statement",
          "[coherence][list][sql][coalescing][integration]") {
    CohEnv env(1s);
    seedArticles(env, 30);
    // Past bootstrap, the first read goes direct and the others join a batch.
    for (int i = 0; i < io::batch::TimingEstimator::kBootstrapThreshold + 2; ++i)
        driveBounded(env.io, CohArticlePg::query(articlePage(5, 5)));

    SECTION("same page: the batched pair coalesces") {
        auto before = env.batcher->pgReadsSent();
        auto sizes = driveBounded(env.io, pageSizes({5, 5, 5}));
        CHECK(sizes == std::vector<size_t>{5, 5, 5});
        CHECK(env.batcher->pgReadsSent() - before == 2);  // the direct one + the batch
    }
    SECTION("different offsets: one statement each") {
        auto before = env.batcher->pgReadsSent();
        auto sizes = driveBounded(env.io, pageSizes({5, 10, 25}));
        CHECK(sizes == std::vector<size_t>{5, 5, 5});
        CHECK(env.batcher->pgReadsSent() - before == 3);
    }
}

// =============================================================================
// Cache coherence — scenario A (committed, ACK withheld) and B (rolled
// back). The cache must never serve a value the DB no longer holds (A), and must
// keep the value the DB still holds (B).
// =============================================================================

TEST_CASE("coherence: A update — committed but withheld evicts the stale L1 entry",
          "[coherence][scenarioA][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    prewarmWriteStmts(env);
    auto id = env.seedItem("A-update", 1);

    // Warm L1 with the old value, so a missing eviction would surface as stale.
    auto v0 = driveBounded(env.io, CohItemL1::find(id));
    REQUIRE(v0);
    REQUIRE(v0->value == 1);

    // The UPDATE flushes and the server commits the new value; its ACK is withheld
    // → the client gives up with an uncertain timeout.
    env.holdPg();
    REQUIRE_THROWS_AS(
        driveBounded(env.io, CohItemL1::update(id, item(id, "A-update", 2))),
        io::PgQueryTimeout);

    // Ground truth: the DB DID change (autocommit applied before the client gave up).
    REQUIRE(probeValue(env, id) == "2");

    // The contract: the cache must not keep serving the old value.
    env.releasePg();
    auto v1 = driveBounded(env.io, CohItemL1::find(id));
    REQUIRE(v1);
    REQUIRE(v1->value == 2);  // a stale cache would answer 1 here
}

TEST_CASE("coherence: A erase — committed but withheld evicts the deleted entry",
          "[coherence][scenarioA][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    prewarmWriteStmts(env);
    auto id = env.seedItem("A-erase", 5);

    auto v0 = driveBounded(env.io, CohItemL1::find(id));
    REQUIRE(v0);

    env.holdPg();
    REQUIRE_THROWS_AS(driveBounded(env.io, CohItemL1::erase(id)),
                      io::PgQueryTimeout);

    REQUIRE(probeValue(env, id) == "<absent>");  // committed delete

    env.releasePg();
    auto v1 = driveBounded(env.io, CohItemL1::find(id));
    REQUIRE_FALSE(static_cast<bool>(v1));  // not served from a stale L1 entry
}

TEST_CASE("coherence: A insert — committed but withheld leaves no stray cache entry",
          "[coherence][scenarioA][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    prewarmWriteStmts(env);

    env.holdPg();
    // RETURNING id is withheld → uncertain timeout, no id to key a cache entry on.
    REQUIRE_THROWS_AS(
        driveBounded(env.io, CohItemL1::insert(item(0, "A-insert", 9))),
        io::PgQueryTimeout);

    // The row committed server-side …
    auto r = env.probe.query(
        "SELECT count(*) FROM relais_test_items WHERE name='A-insert'");
    REQUIRE(r.get(0, 0) == "1");
    // … but nothing was write-through cached (no stray entry under a bogus key).
    REQUIRE(CohItemL1::size() == 0);
}

TEST_CASE("coherence: A upsert (update branch) — committed but withheld evicts the stale entry",
          "[coherence][scenarioA][upsert][integration]") {
    CohEnv env(300ms);
    resetAkey(env);
    prewarmAkeyUpsert(env);
    seedAkey(env, 1, 1);

    // Warm L1 with the old value, so a missing eviction would surface as stale.
    auto v0 = driveBounded(env.io, CohAkeyL1::find(int64_t{1}));
    REQUIRE(v0);
    REQUIRE(v0->payload == 1);

    // The ON CONFLICT statement flushes and the server commits the DO UPDATE; its
    // ACK is withheld → the client gives up uncertain. Upsert follows the update
    // model: the key may now hold a stale value, so L1 is evicted by precaution.
    env.holdPg();
    REQUIRE_THROWS_AS(
        driveBounded(env.io, CohAkeyL1::upsert(akey(1, 2))),
        io::PgQueryTimeout);

    REQUIRE(probeAkey(env, 1) == "2");   // committed server-side

    env.releasePg();
    auto v1 = driveBounded(env.io, CohAkeyL1::find(int64_t{1}));
    REQUIRE(v1);
    REQUIRE(v1->payload == 2);           // a stale cache would answer 1 here
}

TEST_CASE("coherence: A upsert (insert branch) — committed but withheld leaves no stray entry",
          "[coherence][scenarioA][upsert][integration]") {
    CohEnv env(300ms);
    resetAkey(env);
    prewarmAkeyUpsert(env);

    env.holdPg();
    REQUIRE_THROWS_AS(
        driveBounded(env.io, CohAkeyL1::upsert(akey(9, 99))),
        io::PgQueryTimeout);

    REQUIRE(probeAkey(env, 9) == "99");  // row committed server-side
    // The precautionary evict targets the known key; no stray/partial entry sticks.
    REQUIRE(CohAkeyL1::size() == 0);
}

TEST_CASE("coherence: B update — rolled back keeps the old value, never stale",
          "[coherence][scenarioB][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("B-update", 1);

    auto v0 = driveBounded(env.io, CohItemL1::find(id));
    REQUIRE(v0);
    REQUIRE(v0->value == 1);

    // A concurrent row lock makes the relais UPDATE block server-side until the
    // client times out; it never commits.
    env.probe.query("BEGIN");
    env.probe.query("SELECT * FROM relais_test_items WHERE id=" +
                    std::to_string(id) + " FOR UPDATE");

    REQUIRE_THROWS_AS(
        driveBounded(env.io, CohItemL1::update(id, item(id, "B-update", 2))),
        io::PgQueryTimeout);

    // The relais backend is still parked on the lock; terminate it so its blocked
    // statement aborts before the lock is released (otherwise it would commit on
    // grant). Then drop the lock.
    env.probe.terminateBackends(std::string("application_name = '") + kAppName + "'");
    env.probe.query("ROLLBACK");

    REQUIRE(probeValue(env, id) == "1");  // DB unchanged

    auto v1 = driveBounded(env.io, CohItemL1::find(id));
    REQUIRE(v1);
    REQUIRE(v1->value == 1);  // consistent with the DB
}

// =============================================================================
// eraseMany committed but withheld evicts the input key set,
// independent of any TTL (the keys are known up front, no RETURNING needed).
// =============================================================================

TEST_CASE("coherence: eraseMany — committed but withheld evicts the input keys",
          "[coherence][erasemany][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    prewarmWriteStmts(env);
    auto a = env.seedItem("EM-a", 1);
    auto b = env.seedItem("EM-b", 2);

    // Warm both in L1 so a missing eviction would surface as stale on re-find.
    REQUIRE(driveBounded(env.io, CohItemL1::find(a)));
    REQUIRE(driveBounded(env.io, CohItemL1::find(b)));

    std::vector<int64_t> keys{a, b};
    env.holdPg();
    REQUIRE_THROWS_AS(
        driveBounded(env.io, CohItemL1::eraseMany(std::span<const int64_t>(keys))),
        io::PgQueryTimeout);

    REQUIRE(probeValue(env, a) == "<absent>");
    REQUIRE(probeValue(env, b) == "<absent>");

    env.releasePg();
    REQUIRE_FALSE(static_cast<bool>(driveBounded(env.io, CohItemL1::find(a))));
    REQUIRE_FALSE(static_cast<bool>(driveBounded(env.io, CohItemL1::find(b))));
}

// =============================================================================
// Best-effort eviction on a down L2, and read degradation — a down L2 serves L3
// L3 (a value, not a false miss or an error).
// =============================================================================

static void resetBoth(CohEnv& env) {
    env.probe.query("DELETE FROM relais_test_items");
    relais_test::TestInternals::resetEntityCacheState<CohItemBoth>();
    driveBounded(env.io, env.redis->exec("FLUSHDB"));  // clear L2
}

TEST_CASE("coherence: best-effort — a committed update survives a down L2",
          "[coherence][besteffort][redis][integration]") {
    CohEnv env(300ms, /*with_redis=*/true, 200ms);
    resetBoth(env);
    auto id = env.seedItem("BE-update", 1);

    // Warm L1 + L2 with Redis healthy, then let the detached L2 fill land.
    auto v0 = driveBounded(env.io, CohItemBoth::find(id));
    REQUIRE(v0);
    REQUIRE(v0->value == 1);
    pump(env.io, 100ms);

    // Redis goes dark exactly at the invalidation: the L2 UNLINK/gen-bump times
    // out and is swallowed best-effort — the committed write must NOT fail on it.
    env.holdRedis();
    auto affected =
        driveBounded(env.io, CohItemBoth::update(id, item(id, "BE-update", 2)));
    REQUIRE(affected.has_value());  // success, not an exception
    REQUIRE(*affected == 1);

    // The next read never serves the stale value (write-through L1; L2 still down).
    auto v1 = driveBounded(env.io, CohItemBoth::find(id));
    REQUIRE(v1);
    REQUIRE(v1->value == 2);
}

TEST_CASE("coherence: a down L2 degrades reads to L3, order preserved",
          "[coherence][degrade][redis][integration]") {
    CohEnv env(300ms, /*with_redis=*/true, 200ms);
    resetBoth(env);
    auto a = env.seedItem("deg-a", 10);
    auto b = env.seedItem("deg-b", 20);

    // Real L1 miss, L2 never populated, then L2 goes dark.
    relais_test::TestInternals::resetEntityCacheState<CohItemBoth>();
    env.holdRedis();

    // Single find: L1 miss → L2 timeout (swallowed) → L3 value, not empty/throw.
    auto va = driveBounded(env.io, CohItemBoth::find(a));
    REQUIRE(va);
    REQUIRE(va->value == 10);

    // findMany over a fresh L1: both resolve from L3, in request order.
    relais_test::TestInternals::resetEntityCacheState<CohItemBoth>();
    std::vector<int64_t> ids{b, a};
    auto mv = driveBounded(env.io, CohItemBoth::findMany(std::span<const int64_t>(ids)));
    REQUIRE(mv.size() == 2);
    REQUIRE(mv[0]);
    REQUIRE(mv[0]->value == 20);
    REQUIRE(mv[1]);
    REQUIRE(mv[1]->value == 10);
}

// Captures warnings for the lifetime of the object.
struct WarnCapture {
    static inline std::vector<std::string> lines;
    log::Callback previous;

    WarnCapture() : previous(log::getCallback()) {
        lines.clear();
        log::setCallback([](log::Level level, const char* msg, size_t len) {
            if (level == log::Level::Warn) lines.emplace_back(msg, len);
        });
    }
    ~WarnCapture() { log::setCallback(previous); }

    bool any(std::string_view a, std::string_view b) const {
        return std::ranges::any_of(lines, [&](const std::string& l) {
            return l.find(a) != std::string::npos && l.find(b) != std::string::npos;
        });
    }
};

// Runs the loop until `done` holds, bounded.
template<typename Pred>
static bool pumpUntil(CohEnv& env, Pred done, std::chrono::milliseconds budget = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (!done()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        env.io.runOnce(10);
    }
    return true;
}

// A list miss stores the page back into L2 after PostgreSQL answered, without
// awaiting it. That store-back is best-effort: a Redis that stops answering must
// not turn a read PostgreSQL already served into an error, and its own failure is
// logged, never thrown.
template<typename ListRepo>
static void listReadSurvivesDownL2(CohEnv& env) {
    namespace ld = jcailloux::relais::list::spec;
    using Desc = typename ListRepo::ListDescriptorType;

    // No L2 flush needed: Redis is cut before the read, so the L2 lookup misses.
    env.probe.query("DELETE FROM relais_test_articles");
    if constexpr (requires { ListRepo::listCache(); })
        relais_test::TestInternals::resetListCacheState<ListRepo>();
    env.probe.query(
        "INSERT INTO relais_test_articles (category, author_id, title) VALUES "
        "('coh-l2down', 1, 'a'), ('coh-l2down', 1, 'b'), ('coh-l2down', 1, 'c')");

    ld::ListQueryParams<Desc> p;
    p.limit = 10;
    p.filters.template get<"category">() = std::string("coh-l2down");
    auto query = ld::seal<Desc>(std::move(p));

    WarnCapture warns;
    env.holdRedis();
    auto page = driveBounded(env.io, ListRepo::query(std::move(query)));
    REQUIRE(page->size() == 3);
    CHECK(pumpUntil(env, [&] { return warns.any("storeListPage error", ""); }));
}

TEST_CASE("coherence: a list miss with a down L2 serves the PostgreSQL page",
          "[coherence][degrade][list][redis][integration]") {
    CohEnv env(300ms, /*with_redis=*/true, 200ms);
    SECTION("Redis preset") { listReadSurvivesDownL2<CohArticleRedis>(env); }
    SECTION("Both preset")  { listReadSurvivesDownL2<CohArticleBoth>(env); }
}

// -- Redis error replies under load ---------------------------------------------
//
// Under concurrency a Redis command leaves in a pipelined batch instead of going
// direct. An error reply (here WRONGTYPE: the key holds another type) must fail
// it the same way on both paths, so the cache degrades and logs instead of
// reading the error as a value.

// Successful sends past the batch scheduler's bootstrap threshold, so a command
// sent while another is in flight joins a pipelined batch.
static void bootstrapRedisBatching(CohEnv& env) {
    for (int i = 0; i < io::batch::TimingEstimator::kBootstrapThreshold + 2; ++i)
        driveBounded(env.io, PgProvider::redis("PING"));
}

// Runs `work` while a PING loop keeps a Redis command in flight, so each Redis
// command `work` sends joins a pipelined batch.
template<typename T>
static io::Task<T> underRedisLoad(io::Immediate<T> work) {
    bool stop = false;
    auto noise = [](bool& stop) -> io::Task<void> {
        while (!stop) co_await PgProvider::redis("PING");
    };
    auto run = [](io::Immediate<T> w, bool& stop) -> io::Task<T> {
        try {
            auto r = co_await std::move(w);
            stop = true;
            co_return r;
        } catch (...) {
            stop = true;
            throw;
        }
    };
    // whenAll starts its children in order: the PING loop first, so it is
    // already in flight when `work` sends its first command.
    auto [_, r] = co_await io::whenAll(noise(stop), run(std::move(work), stop));
    co_return std::move(r);
}

template<typename ListRepo>
static void listReadSurvivesL2ErrorReply(CohEnv& env) {
    namespace ld = jcailloux::relais::list::spec;
    using Desc = typename ListRepo::ListDescriptorType;

    env.probe.query("DELETE FROM relais_test_articles");
    if constexpr (requires { ListRepo::listCache(); })
        relais_test::TestInternals::resetListCacheState<ListRepo>();
    env.probe.query(
        "INSERT INTO relais_test_articles (category, author_id, title) VALUES "
        "('coh-l2err', 1, 'a'), ('coh-l2err', 1, 'b'), ('coh-l2err', 1, 'c')");
    driveBounded(env.io, env.redis->exec("FLUSHDB"));
    // The master hash of list groups held as a string: the store-back script's
    // HSET answers WRONGTYPE.
    driveBounded(env.io, env.redis->exec(
        "SET", std::string(ListRepo::name()) + ":dlist_groups", "x"));
    bootstrapRedisBatching(env);

    ld::ListQueryParams<Desc> p;
    p.limit = 10;
    p.filters.template get<"category">() = std::string("coh-l2err");
    auto query = ld::seal<Desc>(std::move(p));

    WarnCapture warns;
    auto before = env.batcher->redisPipelinedEntries();
    auto page = driveBounded(env.io, underRedisLoad(ListRepo::query(std::move(query))));
    REQUIRE(page->size() == 3);
    CHECK(env.batcher->redisPipelinedEntries() > before);
    // The store-back is not awaited: its error arrives after the read returned.
    CHECK(pumpUntil(env, [&] { return warns.any("storeListPage error", "WRONGTYPE"); }));
    // The script writes the page last: the failed HSET left no page behind that
    // no group-wide invalidation could reach.
    auto pages = driveBounded(env.io, env.redis->exec(
        "KEYS", std::string(ListRepo::name()) + ":dlist:p:*"));
    CHECK(pages.arraySize() == 0);
}

TEST_CASE("coherence: a list miss under load survives an L2 error reply",
          "[coherence][degrade][list][redis][integration]") {
    CohEnv env(300ms, /*with_redis=*/true, 200ms);
    SECTION("Redis preset") { listReadSurvivesL2ErrorReply<CohArticleRedis>(env); }
    SECTION("Both preset")  { listReadSurvivesL2ErrorReply<CohArticleBoth>(env); }
}

TEST_CASE("coherence: an entity read under load degrades an L2 error reply to L3",
          "[coherence][degrade][redis][integration]") {
    CohEnv env(300ms, /*with_redis=*/true, 200ms);
    resetBoth(env);
    auto id = env.seedItem("wrongtype", 42);
    // The entity key held as a hash: the L2 GET answers WRONGTYPE.
    driveBounded(env.io, env.redis->exec(
        "HSET", std::string(CohItemBoth::name()) + ":" + std::to_string(id), "f", "v"));
    bootstrapRedisBatching(env);

    WarnCapture warns;
    auto before = env.batcher->redisPipelinedEntries();
    auto e = driveBounded(env.io, underRedisLoad(CohItemBoth::find(id)));
    REQUIRE(e);
    CHECK(e->value == 42);
    CHECK(env.batcher->redisPipelinedEntries() > before);
    CHECK(warns.any("getRawBinary error", "WRONGTYPE"));
}

// =============================================================================
// Redis unconfigured (hasRedis()==false) — the mono-loop mirror of IoPool
// redis=nullopt. A config::Both repo whose batcher was bound WITHOUT Redis
// (bindBatcher(false)) must treat the L2 tier as a clean no-op: reads degrade to
// L3, writes commit, and nothing throws from an unbound Redis. Same
// redis_exec_==nullptr binding IoPool's L1-only path produces — proven here
// end-to-end through the full mixin chain, where the down-L2 tests above only
// cover a bound-but-timing-out Redis.
// =============================================================================

TEST_CASE("coherence: a config::Both repo with Redis unconfigured no-ops L2, serves L3",
          "[coherence][degrade][integration]") {
    CohEnv env(300ms);  // with_redis=false → bindBatcher(false)
    REQUIRE_FALSE(PgProvider::hasRedis());

    env.probe.query("DELETE FROM relais_test_items");
    relais_test::TestInternals::resetEntityCacheState<CohItemBoth>();
    auto id = env.seedItem("noredis", 42);

    // Read: L1 miss → the L2 GET early-returns (hasRedis()==false) → L3 value.
    // Not a false miss, not a throw.
    auto v = driveBounded(env.io, CohItemBoth::find(id));
    REQUIRE(v);
    REQUIRE(v->value == 42);

    // Write: commits through L3; the L2 invalidation is skipped, no throw.
    auto affected =
        driveBounded(env.io, CohItemBoth::update(id, item(id, "noredis", 43)));
    REQUIRE(affected.has_value());
    REQUIRE(*affected == 1);

    // A fresh read (L1 cleared) still degrades to L3 and reflects the new value.
    relais_test::TestInternals::resetEntityCacheState<CohItemBoth>();
    auto v2 = driveBounded(env.io, CohItemBoth::find(id));
    REQUIRE(v2);
    REQUIRE(v2->value == 43);
}

// =============================================================================
// l1_ttl=0 — the executable proof that the self-heal bounds staleness. An erase whose L2 eviction
// fails (Redis down) leaves a stale entry in L2; with l1_ttl=0 a read-fill copy never
// TTL-heals, so without the self-heal the deleted row would be served forever.
// The deferred retry must revive Redis and converge the stale entry away.
// =============================================================================

TEST_CASE("coherence: self-heal converges a stale L2 entry from a down-window erase",
          "[coherence][selfheal][redis][integration]") {
    CohEnv env(300ms, /*with_redis=*/true, 200ms);
    env.probe.query("DELETE FROM relais_test_items");
    relais_test::TestInternals::resetEntityCacheState<CohItemTtl0>();
    driveBounded(env.io, env.redis->exec("FLUSHDB"));
    auto id = env.seedItem("SH-erase", 7);

    // Warm L1 + L2 with the row (the future stale entry), then let the L2 fill land.
    REQUIRE(driveBounded(env.io, CohItemTtl0::find(id)));
    pump(env.io, 100ms);

    // L2 goes dark exactly at the erase: the UNLINK times out and is swallowed
    // best-effort, leaving the row as a stale entry in L2 and enqueueing a self-heal.
    env.holdRedis();
    auto erased = driveBounded(env.io, CohItemTtl0::erase(id));
    REQUIRE(erased.has_value());                  // committed delete
    REQUIRE(probeValue(env, id) == "<absent>");   // DB row is gone

    // Drive the loop: the self-heal drain revives a fresh Redis connection (a new
    // fd, not the held one) and retries the eviction → the stale entry leaves L2.
    pump(env.io, kSelfHealBudget);

    // The read now refetches the DB and finds the row gone — not the stale entry.
    REQUIRE_FALSE(static_cast<bool>(driveBounded(env.io, CohItemTtl0::find(id))));
}

// =============================================================================
// Concurrency (a code-review recommendation). This suite is single-loop, so the
// genuine multi-thread surface — concurrent find on another worker racing an
// uncertain unwind over the shared static L1 — is TSan-validated by
// test_relais_concurrency (multi-worker on the shared L1) and test_io_liveness
// (resume chains), not duplicated here. What this case adds is the cache-
// consistency invariant under concurrent identical writes:
//
// (a) Two identical writes run concurrently behind one frozen connection.
//     Whichever way they resolve — coalesced onto one timed-out outcome,
//     serialized, or one reviving on a fresh connection — the worker must never
//     hang, and the cache must end consistent with the DB (a per-key eviction
//     repeated is idempotent, so concurrent uncertain unwinds cannot diverge it).
//     The per-waiter exception fan-out itself is covered at the I/O layer
//     (test_io_liveness, batched followers behind a frozen acquire).
// =============================================================================

TEST_CASE("coherence: concurrent identical writes stay live and cache-consistent",
          "[coherence][concurrency][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    prewarmWriteStmts(env);
    auto id = env.seedItem("CO", 1);
    REQUIRE(driveBounded(env.io, CohItemL1::find(id)));  // warm L1

    env.holdPg();
    auto upd = [&]() -> io::Task<void> {
        try { co_await CohItemL1::update(id, item(id, "CO", 2)); }
        catch (const io::PgError&) {}  // uncertain/failed — recorded by the DB read
    };
    auto both = [&]() -> io::Task<void> {
        std::vector<io::Task<void>> tasks;
        tasks.push_back(upd());
        tasks.push_back(upd());
        co_await io::whenAll(std::move(tasks));
    };
    driveBounded(env.io, both());  // bounded: a hang here is a test failure

    // The cache must agree with whatever the DB actually holds — never stale.
    auto db = probeValue(env, id);
    env.releasePg();
    auto v = driveBounded(env.io, CohItemL1::find(id));
    if (db == "<absent>") {
        REQUIRE_FALSE(static_cast<bool>(v));
    } else {
        REQUIRE(v);
        REQUIRE(std::to_string(v->value) == db);
    }
}
