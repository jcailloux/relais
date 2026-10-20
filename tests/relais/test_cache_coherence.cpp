// Cache-coherence suite — the repository contract under I/O timeouts. Where
// test_io_liveness proves the I/O layer stays live, this proves the *cache stays
// correct*: a write that times out after the server committed must never leave
// the old value served (scenario A), a write the server rolled back must keep
// the old value (scenario B), and a read against a dead DB must surface the
// error instead of a false "absent".
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
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

#include <jcailloux/relais/repository/Repo.h>
#include <jcailloux/relais/config/CacheConfig.h>
#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/pg/SettleQueue.h>
#include <jcailloux/relais/repository/Precaution.h>
#include <jcailloux/relais/Log.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/batch/TimingEstimator.h>
#include <jcailloux/relais/io/WhenAll.h>

#include <fixtures/ControllableIoContext.h>
#include <fixtures/PgProbe.h>
#include <fixtures/RelaisTestAccessors.h>
#include <fixtures/TestConnInfo.h>
#include "fixtures/generated/TestItemEntity.h"
#include "fixtures/generated/TestAssignedKeyEntity.h"
#include "fixtures/generated/TestArticleEntity.h"
#include "fixtures/generated/TestFilterOnlyEntity.h"
#include "fixtures/generated/TestUserEntity.h"
#include "fixtures/generated/TestPurchaseEntity.h"
#include "fixtures/generated/TestGuardRowEntity.h"

#include <array>
#include <chrono>
#include <coroutine>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <sys/socket.h>

using namespace jcailloux::relais;
using namespace std::chrono_literals;

namespace io = jcailloux::relais::io;
namespace cfg = jcailloux::relais::config;
using Io = io::test::ControllableIoContext;

using ::entity::generated::TestItemEntity;
using ::entity::generated::TestAssignedKeyEntity;
using ::entity::generated::TestArticleEntity;
using ::entity::generated::TestGuardRowEntity;

// =============================================================================
// Connection strings
// =============================================================================

// The relais pool carries a distinct application_name so PgProbe can terminate
// exactly its backend (the clean DB-down read-path case) without touching the
// probe's own session.
static const char* kAppName = "relais_coh_under_test";

// `settings`: more session settings (`-c name=value ...`).
static std::string poolConnInfo(std::string_view settings = {}) {
    return relais_test::connInfoWith(settings) + " application_name=" + kAppName;
}
static const char* probeConnInfo() {
    return relais_test::kConnInfo;
}

// =============================================================================
// Test repositories — each owns its own static L1 (dedicated name)
// =============================================================================

using CohItemL1 = Repo<TestItemEntity, "coh:item:l1">;           // cfg::Local (L1 only)
// Assigned-PK, L1 only — the shape upsert applies to. The key is known before the
// call, so the uncertain-timeout handler evicts by precaution on the update model.
using CohAkeyL1 = Repo<TestAssignedKeyEntity, "coh:akey:l1">;
// List repo (TestArticleEntity has a ListDescriptor → ListMixin) on no cache
// tier: a list read reaches PostgreSQL every time.
using CohArticlePg = Repo<TestArticleEntity, "coh:article:pg", cfg::Uncached>;
// Filters without a list, for countWhere.
using CohCount = Repo<::entity::generated::TestFilterOnlyEntity, "coh:count">;
using CountPred = ::entity::generated::TestFilterOnlyMapping::FilterSet::Values;

// =============================================================================
// Bounded synchronous driver (mirrors test_io_liveness): a coherence suite must
// never hang the runner, so every Task runs against a wall-clock budget. The
// budget catches a hang only: it stays above every bound of the pool.
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
T driveBounded(Io& io, io::Task<T> task, std::chrono::milliseconds budget = 15s) {
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
                         std::chrono::milliseconds budget = 15s) {
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
T driveBounded(Io& io, io::Immediate<T> imm, std::chrono::milliseconds budget = 15s) {
    if (imm.await_ready()) return imm.await_resume();
    return driveBounded(io, imm.take_task(), budget);
}

// =============================================================================
// CohEnv — one event loop + pool + bound PgProvider, plus a
// PgProbe for ground truth. The whole repo stack runs on this single loop on the
// test thread; holdPg() withholds the pool connection's result.
// =============================================================================

struct CohEnv {
    Io io;
    std::shared_ptr<io::PgPool<Io>> pool;
    std::shared_ptr<io::batch::BatchScheduler<Io>> batcher;
    io::test::PgProbe probe;
    int pg_fd = -1;
    std::chrono::milliseconds q_timeout_;

    // `settings` are session settings of the pool's connections (the settle
    // connection uses them too). No client check by default: the server would
    // abort a blocked write once its client left, before the cases settle it.
    explicit CohEnv(std::chrono::milliseconds query_timeout,
                    io::UncertainSettle settle = io::UncertainSettle::Terminate,
                    std::chrono::milliseconds settle_delay = 0ms,
                    std::string_view settings = {},
                    std::chrono::milliseconds client_check_interval = 0ms)
        : probe(probeConnInfo()), q_timeout_(query_timeout)
    {
        // min=max=1: a single connection serves every op, so holdReads(pg_fd)
        // deterministically withholds the result of whatever the repo runs.
        // No case waits on an acquire: its bound only absorbs a late setup
        // (connecting, naming the backend) on a busy machine.
        pool = driveBounded(io, io::PgPool<Io>::create(io, poolConnInfo(settings),
            {.min_connections = 1, .max_connections = 1,
             .acquire_timeout = 10s, .query_timeout = query_timeout,
             .uncertain_settle = settle, .settle_delay = settle_delay,
             .client_check_interval = client_check_interval}));
        pg_fd = io.lastAddedFd();  // the connection min_connections just built

        // Build the batcher directly (not PgProvider::init) so the cases can read
        // its counters and shut it down. Bound on the test thread, which
        // driveBounded already stamped as the loop thread.
        batcher = std::make_shared<io::batch::BatchScheduler<Io>>(io, pool, 8);
        PgProvider::bindBatcher<Io>(batcher);
    }

    ~CohEnv() {
        // The manual driver has no IoPool to drain detached coroutines still
        // suspended on a held connection when a test ends. Mirror IoPool's ordered
        // teardown: release the hold and pump past the per-op bound so every
        // suspended frame fires its timer and unwinds — otherwise its frame leaks
        // (ASan/LSan).
        releasePg();
        auto deadline = std::chrono::steady_clock::now() + q_timeout_ + 400ms;
        while (std::chrono::steady_clock::now() < deadline)
            io.runOnce(20);
        PgProvider::reset();
    }

    CohEnv(const CohEnv&) = delete;
    CohEnv& operator=(const CohEnv&) = delete;

    void holdPg()    { io.holdReads(pg_fd); }
    void releasePg() { io.releaseReads(pg_fd); }

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
    // Past bootstrap, the first read goes direct: identical reads share its
    // result, different ones join a batch.
    for (int i = 0; i < io::batch::TimingEstimator::kBootstrapThreshold + 2; ++i)
        driveBounded(env.io, CohCount::countWhere({}));

    SECTION("same predicate: one statement for all three") {
        const CountPred p{.category = "coh"};
        auto before = env.batcher->pgReadsSent();
        auto counts = driveBounded(env.io, countAll({p, p, p}));
        CHECK(counts == std::vector<size_t>{3, 3, 3});
        CHECK(env.batcher->pgReadsSent() - before == 1);
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
    // Past bootstrap, the first read goes direct: identical reads share its
    // result, different ones join a batch.
    for (int i = 0; i < io::batch::TimingEstimator::kBootstrapThreshold + 2; ++i)
        driveBounded(env.io, CohArticlePg::query(articlePage(5, 5)));

    SECTION("same page: one statement for all three") {
        auto before = env.batcher->pgReadsSent();
        auto sizes = driveBounded(env.io, pageSizes({5, 5, 5}));
        CHECK(sizes == std::vector<size_t>{5, 5, 5});
        CHECK(env.batcher->pgReadsSent() - before == 1);
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

// =============================================================================
// Erase of an entity with lists — the DELETE returns the row it deleted and the
// list pages are keyed on it: no read before the write, and the position is the
// one PostgreSQL removed, not a cached copy. An uncertain erase returns nothing:
// the pages holding the row are invalidated, whatever version they hold.
// =============================================================================

using CohArticleL1 = Repo<TestArticleEntity, "coh:article:l1">;

namespace {

int64_t seedArticle(CohEnv& env, const std::string& category) {
    auto r = env.probe.query(
        "INSERT INTO relais_test_articles (category, author_id, title) VALUES ('"
        + category + "', 1, 'erase') RETURNING id");
    return std::stoll(r.get(0, 0));
}

bool articleExists(CohEnv& env, int64_t id) {
    return !env.probe.query(
        "SELECT 1 FROM relais_test_articles WHERE id=" + std::to_string(id)).empty();
}

template<typename ListRepo>
void resetArticleCaches(CohEnv& env) {
    constexpr auto level = ListRepo::config.cache_level;
    env.probe.query("DELETE FROM relais_test_articles");
    if constexpr (level == cfg::CacheLevel::L1) {
        relais_test::TestInternals::resetEntityCacheState<ListRepo>();
        relais_test::TestInternals::resetListCacheState<ListRepo>();
    }
}

template<typename ListRepo>
bool pageHolds(CohEnv& env, const std::string& category, int64_t id) {
    using Desc = typename ListRepo::ListDescriptorType;
    ld::ListQueryParams<Desc> p;
    p.limit = 10;
    p.filters.template get<"category">() = category;
    auto page = driveBounded(env.io, ListRepo::query(ld::seal<Desc>(std::move(p))));
    for (const auto& a : page->items)
        if (a.id == id) return true;
    return false;
}

template<typename ListRepo>
void eraseReadsNothingFirst(CohEnv& env, bool cached_entity) {
    resetArticleCaches<ListRepo>(env);
    auto id = seedArticle(env, "coh-erase");
    auto other = seedArticle(env, "coh-erase");
    if (cached_entity) REQUIRE(driveBounded(env.io, ListRepo::find(id)));
    REQUIRE(pageHolds<ListRepo>(env, "coh-erase", id));

    auto reads = env.batcher->pgReadsSent();
    CHECK(driveBounded(env.io, ListRepo::erase(id)) == std::optional<size_t>{1});
    CHECK(env.batcher->pgReadsSent() == reads);

    CHECK_FALSE(pageHolds<ListRepo>(env, "coh-erase", id));
    CHECK(pageHolds<ListRepo>(env, "coh-erase", other));
}

template<typename ListRepo>
void eraseKeysOnDeletedRow(CohEnv& env) {
    resetArticleCaches<ListRepo>(env);
    auto id = seedArticle(env, "coh-pre-a");
    REQUIRE(driveBounded(env.io, ListRepo::find(id)));  // cached copy in category a
    // Another writer moves the row to category b; the cached copy stays in a.
    env.probe.query("UPDATE relais_test_articles SET category='coh-pre-b' WHERE id="
                    + std::to_string(id));
    REQUIRE(pageHolds<ListRepo>(env, "coh-pre-b", id));

    REQUIRE(driveBounded(env.io, ListRepo::erase(id)) == std::optional<size_t>{1});
    CHECK_FALSE(pageHolds<ListRepo>(env, "coh-pre-b", id));
}

template<typename ListRepo>
void uncertainEraseLeavesItsPages(CohEnv& env) {
    resetArticleCaches<ListRepo>(env);
    // Prepare the DELETE once, so the held erase waits only on its result.
    driveBounded(env.io, ListRepo::erase(seedArticle(env, "coh-unc")));
    auto id = seedArticle(env, "coh-unc");
    REQUIRE(driveBounded(env.io, ListRepo::find(id)));
    REQUIRE(pageHolds<ListRepo>(env, "coh-unc", id));

    env.holdPg();
    REQUIRE_THROWS_AS(driveBounded(env.io, ListRepo::erase(id)), io::PgQueryTimeout);
    REQUIRE_FALSE(articleExists(env, id));  // the DELETE committed
    env.releasePg();

    CHECK_FALSE(pageHolds<ListRepo>(env, "coh-unc", id));
}

io::Task<std::vector<std::optional<size_t>>> eraseTwice(int64_t id) {
    std::vector<io::Task<std::optional<size_t>>> tasks;
    tasks.push_back(CohArticleL1::erase(id));
    tasks.push_back(CohArticleL1::erase(id));
    co_return co_await io::whenAll(std::move(tasks));
}

}  // namespace

TEST_CASE("erase with lists: no read before the DELETE, the row's pages invalidated",
          "[coherence][list][erase][integration]") {
    CohEnv env(1s);
    SECTION("Uncached")    { eraseReadsNothingFirst<CohArticlePg>(env, false); }
    SECTION("Local, miss") { eraseReadsNothingFirst<CohArticleL1>(env, false); }
    SECTION("Local, hit")  { eraseReadsNothingFirst<CohArticleL1>(env, true); }
}

TEST_CASE("erase with lists: pages keyed on the deleted row, not a stale cached copy",
          "[coherence][list][erase][integration]") {
    CohEnv env(1s);
    eraseKeysOnDeletedRow<CohArticleL1>(env);
}

TEST_CASE("erase with lists: an absent row deletes and invalidates nothing",
          "[coherence][list][erase][integration]") {
    CohEnv env(1s);
    resetArticleCaches<CohArticleL1>(env);
    auto id = seedArticle(env, "coh-absent");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-absent", id));
    auto pending = relais_test::TestInternals::pendingModificationCount<CohArticleL1>();

    CHECK(driveBounded(env.io, CohArticleL1::erase(id + 1'000'000)) == std::optional<size_t>{0});
    CHECK(relais_test::TestInternals::pendingModificationCount<CohArticleL1>() == pending);
    CHECK(pageHolds<CohArticleL1>(env, "coh-absent", id));
}

TEST_CASE("erase with lists: identical erases in flight invalidate the row's pages",
          "[coherence][list][erase][concurrency][integration]") {
    CohEnv env(1s);
    resetArticleCaches<CohArticleL1>(env);
    auto id = seedArticle(env, "coh-twice");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-twice", id));

    auto results = driveBounded(env.io, eraseTwice(id));
    REQUIRE(results.size() == 2);
    REQUIRE(results[0].has_value());
    REQUIRE(results[1].has_value());
    CHECK(*results[0] + *results[1] >= 1);  // coalesced: both see 1; serial: 1 then 0
    CHECK_FALSE(articleExists(env, id));
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-twice", id));
}

TEST_CASE("erase with lists: an uncertain erase invalidates the pages holding the row",
          "[coherence][list][erase][scenarioA][integration]") {
    CohEnv env(300ms);
    uncertainEraseLeavesItsPages<CohArticleL1>(env);
}

TEST_CASE("erase with lists: an uncertain erase needs no cached copy of the row",
          "[coherence][list][erase][scenarioA][integration]") {
    // The row is never read, so L1 holds no copy of it: the pages holding it
    // are found by its key.
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    driveBounded(env.io, CohArticleL1::erase(seedArticle(env, "coh-unc-nocopy")));
    auto id = seedArticle(env, "coh-unc-nocopy");
    auto other = seedArticle(env, "coh-unc-nocopy-other");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-unc-nocopy", id));
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-unc-nocopy-other", other));
    // Written behind relais: a page still cached does not show it.
    auto hidden = seedArticle(env, "coh-unc-nocopy-other");

    env.holdPg();
    CHECK_THROWS_AS(driveBounded(env.io, CohArticleL1::erase(id)), io::PgQueryTimeout);
    env.releasePg();
    CHECK_FALSE(articleExists(env, id));
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-unc-nocopy", id));
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-unc-nocopy-other", hidden));  // left alone
}

// =============================================================================
// Update of an entity with lists — the UPDATE locks the row in a CTE and
// returns it before and after: the pages of both positions are keyed on the
// versions PostgreSQL replaced and committed, with no read before the write. An
// uncertain update returns nothing: the pages holding the row are invalidated,
// and the input stands for its new position until the write is settled. A
// patch already returned both versions; an uncertain one invalidates the pages
// holding the row the same way.
// =============================================================================

namespace {

TestArticleEntity articleIn(int64_t id, const std::string& category) {
    TestArticleEntity e;
    e.id = id;
    e.category = category;
    e.author_id = 1;
    e.title = "update";
    e.view_count = 0;  // NOT NULL in the table
    return e;
}

std::string articleCategory(CohEnv& env, int64_t id) {
    return env.probe.query(
        "SELECT category FROM relais_test_articles WHERE id=" + std::to_string(id)).get(0, 0);
}

auto setCategory(const std::string& category) {
    return jcailloux::relais::entity::set<TestArticleEntity::Field::category>(category);
}

template<typename ListRepo>
void updateReadsNothingFirst(CohEnv& env, bool cached_entity) {
    resetArticleCaches<ListRepo>(env);
    auto id = seedArticle(env, "coh-upd-a");
    auto other = seedArticle(env, "coh-upd-a");
    if (cached_entity) REQUIRE(driveBounded(env.io, ListRepo::find(id)));
    REQUIRE(pageHolds<ListRepo>(env, "coh-upd-a", id));
    REQUIRE_FALSE(pageHolds<ListRepo>(env, "coh-upd-b", id));

    auto reads = env.batcher->pgReadsSent();
    CHECK(driveBounded(env.io, ListRepo::update(id, articleIn(id, "coh-upd-b")))
          == std::optional<size_t>{1});
    CHECK(env.batcher->pgReadsSent() == reads);

    CHECK_FALSE(pageHolds<ListRepo>(env, "coh-upd-a", id));
    CHECK(pageHolds<ListRepo>(env, "coh-upd-a", other));
    CHECK(pageHolds<ListRepo>(env, "coh-upd-b", id));
}

template<typename ListRepo>
void updateKeysOnReplacedRow(CohEnv& env) {
    resetArticleCaches<ListRepo>(env);
    auto id = seedArticle(env, "coh-upd-pre-a");
    REQUIRE(driveBounded(env.io, ListRepo::find(id)));  // cached copy in category a
    // Another writer moves the row to category b; the cached copy stays in a.
    env.probe.query("UPDATE relais_test_articles SET category='coh-upd-pre-b' WHERE id="
                    + std::to_string(id));
    REQUIRE(pageHolds<ListRepo>(env, "coh-upd-pre-b", id));

    REQUIRE(driveBounded(env.io, ListRepo::update(id, articleIn(id, "coh-upd-pre-c")))
            == std::optional<size_t>{1});
    CHECK_FALSE(pageHolds<ListRepo>(env, "coh-upd-pre-b", id));
    CHECK(pageHolds<ListRepo>(env, "coh-upd-pre-c", id));
}

template<typename ListRepo>
void uncertainUpdateMovesBothPositions(CohEnv& env) {
    resetArticleCaches<ListRepo>(env);
    // Prepare the UPDATE once, so the held update waits only on its result.
    auto warm = seedArticle(env, "coh-upd-unc-warm");
    driveBounded(env.io, ListRepo::update(warm, articleIn(warm, "coh-upd-unc-warm")));
    auto id = seedArticle(env, "coh-upd-unc-a");
    REQUIRE(driveBounded(env.io, ListRepo::find(id)));
    REQUIRE(pageHolds<ListRepo>(env, "coh-upd-unc-a", id));
    REQUIRE_FALSE(pageHolds<ListRepo>(env, "coh-upd-unc-b", id));

    env.holdPg();
    REQUIRE_THROWS_AS(driveBounded(env.io, ListRepo::update(id, articleIn(id, "coh-upd-unc-b"))),
                      io::PgQueryTimeout);
    REQUIRE(articleCategory(env, id) == "coh-upd-unc-b");  // the UPDATE committed
    env.releasePg();

    CHECK_FALSE(pageHolds<ListRepo>(env, "coh-upd-unc-a", id));
    CHECK(pageHolds<ListRepo>(env, "coh-upd-unc-b", id));
}

template<typename ListRepo>
void uncertainPatchLeavesItsPages(CohEnv& env) {
    resetArticleCaches<ListRepo>(env);
    // Prepare the patch statement once, so the held one waits only on its result.
    driveBounded(env.io, ListRepo::patch(seedArticle(env, "coh-pat-unc-warm"),
                                         setCategory("coh-pat-unc-warm")));
    auto id = seedArticle(env, "coh-pat-unc-a");
    REQUIRE(driveBounded(env.io, ListRepo::find(id)));
    REQUIRE(pageHolds<ListRepo>(env, "coh-pat-unc-a", id));

    env.holdPg();
    REQUIRE_THROWS_AS(driveBounded(env.io, ListRepo::patch(id, setCategory("coh-pat-unc-b"))),
                      io::PgQueryTimeout);
    REQUIRE(articleCategory(env, id) == "coh-pat-unc-b");  // the UPDATE committed
    env.releasePg();

    CHECK_FALSE(pageHolds<ListRepo>(env, "coh-pat-unc-a", id));
}

io::Task<std::vector<std::optional<size_t>>> updateTwice(int64_t id, TestArticleEntity e) {
    std::vector<io::Task<std::optional<size_t>>> tasks;
    tasks.push_back(CohArticleL1::update(id, e));
    tasks.push_back(CohArticleL1::update(id, e));
    co_return co_await io::whenAll(std::move(tasks));
}

}  // namespace

TEST_CASE("update with lists: no read before the UPDATE, both positions invalidated",
          "[coherence][list][update][integration]") {
    CohEnv env(1s);
    SECTION("Uncached")    { updateReadsNothingFirst<CohArticlePg>(env, false); }
    SECTION("Local, miss") { updateReadsNothingFirst<CohArticleL1>(env, false); }
    SECTION("Local, hit")  { updateReadsNothingFirst<CohArticleL1>(env, true); }
}

TEST_CASE("update with lists: old position keyed on the replaced row, not a stale cached copy",
          "[coherence][list][update][integration]") {
    CohEnv env(1s);
    updateKeysOnReplacedRow<CohArticleL1>(env);
}

TEST_CASE("update with lists: an absent row updates and invalidates nothing",
          "[coherence][list][update][integration]") {
    CohEnv env(1s);
    resetArticleCaches<CohArticleL1>(env);
    auto id = seedArticle(env, "coh-upd-absent");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-upd-absent", id));
    auto pending = relais_test::TestInternals::pendingModificationCount<CohArticleL1>();

    auto absent = id + 1'000'000;
    CHECK(driveBounded(env.io, CohArticleL1::update(absent, articleIn(absent, "coh-upd-absent")))
          == std::optional<size_t>{0});
    CHECK(relais_test::TestInternals::pendingModificationCount<CohArticleL1>() == pending);
    CHECK(pageHolds<CohArticleL1>(env, "coh-upd-absent", id));
}

TEST_CASE("update with lists: identical updates in flight invalidate both positions",
          "[coherence][list][update][concurrency][integration]") {
    CohEnv env(1s);
    resetArticleCaches<CohArticleL1>(env);
    auto id = seedArticle(env, "coh-upd-twice-a");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-upd-twice-a", id));
    REQUIRE_FALSE(pageHolds<CohArticleL1>(env, "coh-upd-twice-b", id));

    auto results = driveBounded(env.io, updateTwice(id, articleIn(id, "coh-upd-twice-b")));
    REQUIRE(results.size() == 2);
    CHECK(results[0] == std::optional<size_t>{1});
    CHECK(results[1] == std::optional<size_t>{1});
    CHECK(articleCategory(env, id) == "coh-upd-twice-b");
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-upd-twice-a", id));
    CHECK(pageHolds<CohArticleL1>(env, "coh-upd-twice-b", id));
}

TEST_CASE("update with lists: an uncertain update moves the row between both positions",
          "[coherence][list][update][scenarioA][integration]") {
    CohEnv env(300ms);
    uncertainUpdateMovesBothPositions<CohArticleL1>(env);
}

TEST_CASE("update with lists: an uncertain update needs no cached copy of the row",
          "[coherence][list][update][scenarioA][integration]") {
    // The row is never read, so L1 holds no copy of it: the pages holding it
    // are found by its key; the new position is the input.
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    auto warm = seedArticle(env, "coh-upd-nocopy-warm");
    driveBounded(env.io, CohArticleL1::update(warm, articleIn(warm, "coh-upd-nocopy-warm")));
    auto id = seedArticle(env, "coh-upd-nocopy-a");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-upd-nocopy-a", id));
    REQUIRE_FALSE(pageHolds<CohArticleL1>(env, "coh-upd-nocopy-b", id));

    env.holdPg();
    CHECK_THROWS_AS(driveBounded(env.io, CohArticleL1::update(
                        id, articleIn(id, "coh-upd-nocopy-b"))),
                    io::PgQueryTimeout);
    env.releasePg();
    CHECK(articleCategory(env, id) == "coh-upd-nocopy-b");
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-upd-nocopy-a", id));
    CHECK(pageHolds<CohArticleL1>(env, "coh-upd-nocopy-b", id));
}

TEST_CASE("patch with lists: an uncertain patch invalidates the pages holding the row",
          "[coherence][list][patch][scenarioA][integration]") {
    CohEnv env(300ms);
    uncertainPatchLeavesItsPages<CohArticleL1>(env);
}

// =============================================================================
// Settle ticket — a write that failed uncertainly names what must end before
// its outcome is final: the backend that ran it (Terminate, Await) or a delay
// (Delay). The backend here is blocked on a row lock held by a second probe, so
// it is still alive, exactly as after a real timeout. These cases pause the
// settle queue: they check what the ticket names, before anything settles it.
// =============================================================================

namespace {

// Holds a row lock in an open transaction until destroyed.
struct RowLock {
    io::test::PgProbe locker{probeConnInfo()};
    RowLock(const std::string& table, const std::string& where) {
        locker.query("BEGIN");
        locker.query("SELECT 1 FROM " + table + " WHERE " + where + " FOR UPDATE");
    }
    ~RowLock() {
        try { locker.query("ROLLBACK"); } catch (...) {}
    }
};

RowLock lockItem(int64_t id) {
    return RowLock("relais_test_items", "id=" + std::to_string(id));
}

// Runs a write expected to fail uncertainly; returns the ticket it carries.
template<typename Op>
std::shared_ptr<io::SettleTicket> uncertainTicket(CohEnv& env, Op op) {
    try {
        (void)driveBounded(env.io, std::move(op));
    } catch (const io::PgUncertainError& e) {
        return e.settleTicket();
    }
    FAIL("expected an uncertain error");
    return nullptr;
}

bool backendAlive(CohEnv& env, const io::BackendId& b) {
    auto r = env.probe.query(
        "SELECT count(*) FROM pg_stat_activity WHERE pid = " + std::to_string(b.pid)
        + " AND backend_start = '" + b.backend_start + "'::timestamptz"
        + " AND application_name = '" + kAppName + "'");
    return r.get(0, 0) == "1";
}

}  // namespace

TEST_CASE("settle ticket: an uncertain write names the backend that ran it",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    env.batcher->settleQueue().pause();
    resetItems(env);
    auto id = env.seedItem("settle-name", 1);

    std::shared_ptr<io::SettleTicket> ticket;
    {
        auto lock = lockItem(id);
        ticket = uncertainTicket(env, CohItemL1::update(id, item(id, "settle-name", 2)));
        REQUIRE(ticket);
        CHECK(ticket->mode == io::UncertainSettle::Terminate);
        CHECK(ticket->state == io::SettleTicket::State::Pending);
        // The backend still runs the write, blocked on the lock: the ticket
        // names it by pid and start time.
        CHECK(backendAlive(env, ticket->backend));
    }
}

TEST_CASE("settle ticket: a connection lost after sending carries a ticket",
          "[coherence][settle][integration]") {
    CohEnv env(1s);
    env.batcher->settleQueue().pause();
    resetItems(env);
    auto id = env.seedItem("settle-lost", 1);
    auto lock = lockItem(id);

    bool done = false;
    bool lost = false;
    std::shared_ptr<io::SettleTicket> ticket;
    auto op = [&]() -> io::Task<void> {
        try {
            (void)co_await CohItemL1::update(id, item(id, "settle-lost", 2));
        } catch (const io::PgConnectionLost& e) {
            lost = true;
            ticket = e.settleTicket();
        } catch (...) {}
        done = true;
    };
    auto starter = [](io::Task<void> t) -> Starter { co_await std::move(t); };
    auto s = starter(op());

    // The UPDATE is sent and waits on the lock; then the read side of the
    // socket reaches end of stream, well before query_timeout.
    auto until = std::chrono::steady_clock::now() + 150ms;
    while (std::chrono::steady_clock::now() < until) env.io.runOnce(10);
    REQUIRE_FALSE(done);
    REQUIRE(::shutdown(env.pg_fd, SHUT_RD) == 0);
    until = std::chrono::steady_clock::now() + 2s;
    while (!done && std::chrono::steady_clock::now() < until) env.io.runOnce(10);
    if (s.handle) s.handle.destroy();

    REQUIRE(done);
    REQUIRE(lost);
    REQUIRE(ticket);
    CHECK(backendAlive(env, ticket->backend));
}

TEST_CASE("settle ticket: an uncertain read carries none",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("settle-read", 1);

    env.holdPg();
    try {
        (void)driveBounded(env.io, CohItemL1::find(id));
        FAIL("expected PgQueryTimeout");
    } catch (const io::PgQueryTimeout& e) {
        CHECK_FALSE(e.settleTicket());
    }
}

TEST_CASE("settle ticket: every caller of a failed batch shares one ticket",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    env.batcher->settleQueue().pause();
    resetItems(env);
    auto first = env.seedItem("settle-batch-first", 0);
    auto locked = env.seedItem("settle-batch-locked", 0);
    auto other = env.seedItem("settle-batch-other", 0);

    static constexpr const char* kSql =
        "UPDATE relais_test_items SET value = $1 WHERE id = $2";
    std::array<std::shared_ptr<io::SettleTicket>, 3> tickets;
    std::array<bool, 3> uncertain{};
    auto write = [&](int slot, int64_t id, int32_t value) -> io::Task<void> {
        try {
            (void)co_await env.batcher->submitPgWrite(kSql, io::PgParams::make(value, id));
        } catch (const io::PgUncertainError& e) {
            if (slot >= 0) {
                uncertain[slot] = true;
                tickets[slot] = e.settleTicket();
            }
        }
    };

    // Past the estimator's bootstrap, writes submitted while one is in flight
    // are batched instead of sent one by one.
    for (int i = 0; i < 6; ++i) driveBounded(env.io, write(-1, first, i));

    auto lock = lockItem(locked);
    auto wave = [&]() -> io::Task<void> {
        std::vector<io::Task<void>> tasks;
        tasks.push_back(write(-1, first, 100));   // sent alone, completes
        tasks.push_back(write(0, locked, 1));     // batched, blocks on the lock
        tasks.push_back(write(1, locked, 1));     // identical: follows slot 0
        tasks.push_back(write(2, other, 1));      // same pipeline, behind slot 0
        co_await io::whenAll(std::move(tasks));
    };
    driveBounded(env.io, wave());

    REQUIRE(uncertain == std::array<bool, 3>{true, true, true});
    REQUIRE(tickets[0]);
    CHECK(tickets[1] == tickets[0]);
    CHECK(tickets[2] == tickets[0]);
    CHECK(backendAlive(env, tickets[0]->backend));
}

namespace {

struct WaveWrite {
    bool uncertain = false;
    bool coalesced = false;
    int64_t affected = -1;
    std::shared_ptr<io::SettleTicket> ticket;
};

// Sends one write alone, then `writes` (row, value) batched behind it into one
// pipeline, in order; the settle queue is paused.
std::vector<WaveWrite> pipelinedWave(CohEnv& env, int64_t alone,
                                     std::vector<std::pair<int64_t, int32_t>> writes) {
    static constexpr const char* kSql =
        "UPDATE relais_test_items SET value = $1 WHERE id = $2";
    env.batcher->settleQueue().pause();
    std::vector<WaveWrite> out(writes.size());
    auto write = [&](WaveWrite* w, int64_t id, int32_t value) -> io::Task<void> {
        try {
            auto [r, coalesced] =
                co_await env.batcher->submitPgWrite(kSql, io::PgParams::make(value, id));
            if (w) { w->affected = r.affectedRows(); w->coalesced = coalesced; }
        } catch (const io::PgUncertainError& e) {
            if (w) { w->uncertain = true; w->ticket = e.settleTicket(); }
        }
    };
    // Past the estimator's bootstrap, writes submitted while one is in flight
    // are batched instead of sent one by one.
    for (int i = 0; i < 6; ++i) driveBounded(env.io, write(nullptr, alone, i));
    auto wave = [&]() -> io::Task<void> {
        std::vector<io::Task<void>> tasks;
        tasks.push_back(write(nullptr, alone, 100));
        for (size_t i = 0; i < writes.size(); ++i)
            tasks.push_back(write(&out[i], writes[i].first, writes[i].second));
        co_await io::whenAll(std::move(tasks));
    };
    driveBounded(env.io, wave());
    return out;
}

}  // namespace

TEST_CASE("settle ticket: a pipelined write whose end was read is not uncertain",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto alone = env.seedItem("resolved-alone", 0);
    auto a = env.seedItem("resolved-a", 0);
    auto b = env.seedItem("resolved-b", 0);
    auto locked = env.seedItem("resolved-locked", 0);

    auto lock = lockItem(locked);
    auto out = pipelinedWave(env, alone, {{a, 1}, {b, 2}, {locked, 3}});

    CHECK_FALSE(out[0].uncertain);
    CHECK(out[0].affected == 1);
    CHECK_FALSE(out[1].uncertain);
    CHECK(out[1].affected == 1);
    REQUIRE(out[2].uncertain);
    REQUIRE(out[2].ticket);
    CHECK(out[2].ticket->unresolved == 1);
    CHECK(probeValue(env, a) == "1");
    CHECK(probeValue(env, b) == "2");
}

TEST_CASE("settle ticket: the writes behind an unresolved one stay uncertain",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto alone = env.seedItem("unresolved-alone", 0);
    auto a = env.seedItem("unresolved-a", 0);
    auto locked = env.seedItem("unresolved-locked", 0);
    auto b = env.seedItem("unresolved-b", 0);

    auto lock = lockItem(locked);
    auto out = pipelinedWave(env, alone, {{a, 1}, {locked, 2}, {b, 3}});

    CHECK_FALSE(out[0].uncertain);
    CHECK(out[0].affected == 1);
    REQUIRE(out[1].uncertain);
    REQUIRE(out[2].uncertain);
    REQUIRE(out[1].ticket);
    CHECK(out[2].ticket == out[1].ticket);
    CHECK(out[1].ticket->unresolved == 2);
}

TEST_CASE("settle ticket: followers of a resolved write get its result",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto alone = env.seedItem("follower-alone", 0);
    auto a = env.seedItem("follower-a", 0);
    auto locked = env.seedItem("follower-locked", 0);

    auto lock = lockItem(locked);
    auto out = pipelinedWave(env, alone, {{a, 1}, {a, 1}, {locked, 2}});

    CHECK_FALSE(out[0].uncertain);
    CHECK(out[0].affected == 1);
    CHECK_FALSE(out[1].uncertain);
    CHECK(out[1].coalesced);
    CHECK(out[1].affected == 1);
    REQUIRE(out[2].uncertain);
    REQUIRE(out[2].ticket);
    CHECK(out[2].ticket->unresolved == 1);
}

TEST_CASE("settle ticket: the mode decides what the ticket names",
          "[coherence][settle][integration]") {
    SECTION("Await names the backend") {
        CohEnv env(300ms, io::UncertainSettle::Await);
        env.batcher->settleQueue().pause();
        resetItems(env);
        auto id = env.seedItem("settle-await", 1);
        auto lock = lockItem(id);
        auto t = uncertainTicket(env, CohItemL1::update(id, item(id, "settle-await", 2)));
        REQUIRE(t);
        CHECK(t->mode == io::UncertainSettle::Await);
        CHECK(backendAlive(env, t->backend));
    }
    SECTION("Delay carries the delay and never reads the backend") {
        CohEnv env(300ms, io::UncertainSettle::Delay, 2s);
        resetItems(env);
        auto id = env.seedItem("settle-delay", 1);
        auto lock = lockItem(id);
        auto t = uncertainTicket(env, CohItemL1::update(id, item(id, "settle-delay", 2)));
        REQUIRE(t);
        CHECK(t->mode == io::UncertainSettle::Delay);
        CHECK(t->delay == 2s);
        CHECK_FALSE(t->backend.known());
    }
    SECTION("Delay with no delay and no statement_timeout has nothing to settle") {
        CohEnv env(300ms, io::UncertainSettle::Delay);
        resetItems(env);
        auto id = env.seedItem("settle-delay0", 1);
        auto lock = lockItem(id);
        CHECK_FALSE(uncertainTicket(env, CohItemL1::update(id, item(id, "settle-delay0", 2))));
    }
    SECTION("Off settles nothing") {
        CohEnv env(300ms, io::UncertainSettle::Off);
        resetItems(env);
        auto id = env.seedItem("settle-off", 1);
        auto lock = lockItem(id);
        CHECK_FALSE(uncertainTicket(env, CohItemL1::update(id, item(id, "settle-off", 2))));
    }
}

// =============================================================================
// Settle queue — works each ticket until the write can no longer commit, then
// marks it settled and runs its replays. Settling uses a connection of its own:
// these cases run with the pool reduced to one connection.
// =============================================================================

namespace {

using Queue = io::SettleQueue<Io>;

template<typename Pred>
bool pumpUntil(CohEnv& env, Pred pred, std::chrono::milliseconds budget) {
    auto deadline = std::chrono::steady_clock::now() + budget;
    while (!pred() && std::chrono::steady_clock::now() < deadline) env.io.runOnce(10);
    return pred();
}

Queue& queueOf(CohEnv& env) { return env.batcher->settleQueue(); }

// Settled, and every replay done (a replay may read the row again).
bool settledAll(CohEnv& env) {
    return queueOf(env).pending() == 0 && queueOf(env).replaying() == 0;
}

// The settle connection carries the pool's application_name: count only the
// backend named by the ticket.
io::BackendId backendOf(io::test::PgProbe& probe) {
    auto r = probe.query(
        "SELECT pid, backend_start::text FROM pg_stat_activity WHERE pid = pg_backend_pid()");
    return {static_cast<int32_t>(std::stol(r.get(0, 0))), r.get(0, 1)};
}

std::shared_ptr<io::SettleTicket> ticketFor(io::UncertainSettle mode, io::BackendId b,
                                            std::chrono::milliseconds delay = 0ms) {
    return std::make_shared<io::SettleTicket>(
        io::SettleTicket{.mode = mode, .backend = std::move(b), .delay = delay});
}

// Captures the log lines of one level while alive.
struct LogCapture {
    static inline std::vector<std::pair<log::Level, std::string>> lines;
    log::Callback previous = log::getCallback();
    LogCapture() {
        lines.clear();
        log::setCallback([](log::Level level, const char* msg, size_t len) {
            lines.emplace_back(level, std::string(msg, len));
        });
    }
    ~LogCapture() { log::setCallback(previous); }
    static size_t count(log::Level level, std::string_view needle) {
        size_t n = 0;
        for (const auto& [l, m] : lines)
            if (l == level && m.find(needle) != std::string::npos) ++n;
        return n;
    }
};

// Resets the injected faults whatever the outcome of the case.
struct FaultReset {
    ~FaultReset() { Queue::fault = {}; }
};

// Holds the row, makes the update of it fail uncertainly, and returns its ticket;
// the queue is paused so that the case decides when settling starts.
std::shared_ptr<io::SettleTicket> blockedUpdate(CohEnv& env, int64_t id, int32_t value) {
    queueOf(env).pause();
    auto t = uncertainTicket(env, CohItemL1::update(id, item(id, "settle", value)));
    REQUIRE(t);
    REQUIRE(queueOf(env).pending() == 1);
    return t;
}

}  // namespace

TEST_CASE("settle queue: Terminate ends the backend, the write never commits",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("settle", 1);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));
    auto t = blockedUpdate(env, id, 2);
    REQUIRE(backendAlive(env, t->backend));

    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    CHECK(t->state == io::SettleTicket::State::Settled);
    CHECK(queueOf(env).settled() == 1);
    CHECK(queueOf(env).statements() == 1);
    CHECK_FALSE(backendAlive(env, t->backend));

    lock.reset();
    CHECK(probeValue(env, id) == "1");
}

TEST_CASE("settle queue: a ticket runs its replays once, in order, past a failing one",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    std::vector<int> ran;
    auto t = ticketFor(io::UncertainSettle::Delay, {}, 50ms);
    t->replays.push_back([&ran](io::WriteOutcome) -> io::Task<void> {
        ran.push_back(1);
        co_return;
    });
    t->replays.push_back([&ran](io::WriteOutcome) -> io::Task<void> {
        ran.push_back(2);
        throw std::runtime_error("replay failure");
        co_return;
    });
    t->replays.push_back([&ran](io::WriteOutcome) -> io::Task<void> {
        ran.push_back(3);
        co_return;
    });

    LogCapture logs;
    queueOf(env).enqueue(t);
    queueOf(env).enqueue(t);  // already queued: ignored
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 2s));
    pumpUntil(env, [] { return false; }, 100ms);

    CHECK(ran == std::vector<int>{1, 2, 3});
    CHECK(t->state == io::SettleTicket::State::Settled);
    CHECK(t->replays.empty());
    CHECK(queueOf(env).settled() == 1);
    CHECK(queueOf(env).statements() == 0);
    CHECK(LogCapture::count(log::Level::Warn, "replay failure") == 1);

    queueOf(env).enqueue(t);  // settled: ignored
    CHECK(queueOf(env).pending() == 0);
}

TEST_CASE("settle queue: tickets of one backend are settled by one statement",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("settle", 1);
    auto lock = lockItem(id);
    auto t = blockedUpdate(env, id, 2);
    auto twin = ticketFor(io::UncertainSettle::Terminate, t->backend);
    queueOf(env).enqueue(twin);
    CHECK(queueOf(env).pending() == 2);

    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    CHECK(t->state == io::SettleTicket::State::Settled);
    CHECK(twin->state == io::SettleTicket::State::Settled);
    CHECK(queueOf(env).settled() == 2);
    CHECK(queueOf(env).statements() == 1);
    // Either ticket may be the transaction seen running.
    CHECK(t->outcome == io::WriteOutcome::Unknown);
    CHECK(twin->outcome == io::WriteOutcome::Unknown);
}

TEST_CASE("settle queue: start time guards against a reused pid",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    io::test::PgProbe other{probeConnInfo()};
    auto b = backendOf(other);
    b.backend_start = "2000-01-01 00:00:00+00";
    auto t = ticketFor(io::UncertainSettle::Terminate, b);

    queueOf(env).enqueue(t);
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 3s));
    CHECK(t->state == io::SettleTicket::State::Settled);
    // The backend with that pid but another start time is left alone.
    CHECK(other.query("SELECT 1").get(0, 0) == "1");
}

TEST_CASE("settle queue: a backend already gone is settled",
          "[coherence][settle][integration]") {
    CohEnv env(300ms);
    std::shared_ptr<io::SettleTicket> t;
    io::BackendId b;
    {
        io::test::PgProbe gone{probeConnInfo()};
        b = backendOf(gone);
    }
    // The probe's backend exits right after the probe disconnects.
    REQUIRE(pumpUntil(env, [&] {
        return env.probe.query("SELECT count(*) FROM pg_stat_activity WHERE pid = "
                               + std::to_string(b.pid)).get(0, 0) == "0";
    }, 2s));
    t = ticketFor(io::UncertainSettle::Terminate, b);
    queueOf(env).enqueue(t);
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 3s));
    CHECK(t->state == io::SettleTicket::State::Settled);
    CHECK(queueOf(env).statements() == 1);
    CHECK(t->outcome == io::WriteOutcome::Unknown);
}

TEST_CASE("settle queue: Await waits for the backend to end on its own",
          "[coherence][settle][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetItems(env);
    auto id = env.seedItem("settle", 1);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));
    auto t = blockedUpdate(env, id, 2);

    queueOf(env).resume();
    pumpUntil(env, [] { return false; }, 400ms);
    CHECK(queueOf(env).pending() == 1);
    CHECK(backendAlive(env, t->backend));

    // Unblocked, the write commits, then its backend finds the client gone.
    lock.reset();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 5s));
    CHECK(t->state == io::SettleTicket::State::Settled);
    CHECK_FALSE(backendAlive(env, t->backend));
    CHECK(probeValue(env, id) == "2");
}

TEST_CASE("settle queue: Delay settles after the delay, without a statement",
          "[coherence][settle][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Delay, 400ms);
    resetItems(env);
    auto id = env.seedItem("settle", 1);
    auto lock = lockItem(id);
    // The delay runs from the failure, which comes query_timeout after the send.
    const auto sent = std::chrono::steady_clock::now();
    auto t = uncertainTicket(env, CohItemL1::update(id, item(id, "settle", 2)));
    REQUIRE(t);
    pumpUntil(env, [] { return false; }, 200ms);
    CHECK(queueOf(env).pending() == 1);

    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 2s));
    CHECK(std::chrono::steady_clock::now() - sent >= 300ms + 400ms);
    CHECK(t->state == io::SettleTicket::State::Settled);
    CHECK(queueOf(env).statements() == 0);
    CHECK(t->outcome == io::WriteOutcome::Unknown);
}

TEST_CASE("settle queue: PostgreSQL unreachable, settling is retried with a growing wait",
          "[coherence][settle][integration]") {
    FaultReset reset;
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("settle", 1);
    auto lock = lockItem(id);
    auto t = blockedUpdate(env, id, 2);

    Queue::fault = {.kind = Queue::Fault::Kind::Unreachable, .count = 3};
    const auto start = std::chrono::steady_clock::now();
    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    // Waits of 50, 100 and 200 ms between the four attempts.
    CHECK(std::chrono::steady_clock::now() - start >= 350ms);
    CHECK(Queue::fault.count == 0);
    CHECK(queueOf(env).statements() == 1);
    CHECK_FALSE(backendAlive(env, t->backend));
}

TEST_CASE("settle queue: a refused termination falls back to statement_timeout",
          "[coherence][settle][integration]") {
    FaultReset reset;
    SECTION("statement_timeout bounds the write: settled after it") {
        LogCapture logs;
        CohEnv env(300ms, io::UncertainSettle::Terminate, 0ms,
                   "-c statement_timeout=700");
        resetItems(env);
        auto id = env.seedItem("settle", 1);
        auto lock = lockItem(id);
        auto t = blockedUpdate(env, id, 2);

        Queue::fault = {.kind = Queue::Fault::Kind::Refused, .count = 1};
        const auto start = std::chrono::steady_clock::now();
        queueOf(env).resume();
        pumpUntil(env, [] { return false; }, 1s);
        CHECK(queueOf(env).pending() == 1);
        REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 3s));
        CHECK(std::chrono::steady_clock::now() - start >= 1700ms);
        CHECK(t->state == io::SettleTicket::State::Settled);
        CHECK(t->outcome == io::WriteOutcome::Unknown);
        CHECK(queueOf(env).abandoned() == 0);
        CHECK(LogCapture::count(log::Level::Error, "statement_timeout instead") == 1);
    }
    SECTION("nothing bounds the write: abandoned") {
        LogCapture logs;
        CohEnv env(300ms);
        resetItems(env);
        auto id = env.seedItem("settle", 1);
        auto lock = lockItem(id);
        auto t = blockedUpdate(env, id, 2);
        t->replays.push_back([](io::WriteOutcome) -> io::Task<void> { co_return; });

        Queue::fault = {.kind = Queue::Fault::Kind::Refused, .count = 1};
        queueOf(env).resume();
        REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 3s));
        CHECK(t->state == io::SettleTicket::State::Pending);
        CHECK(t->replays.empty());
        CHECK(queueOf(env).abandoned() == 1);
        CHECK(queueOf(env).settled() == 0);
        CHECK(LogCapture::count(log::Level::Error, "stays unsettled") == 1);
    }
}

TEST_CASE("settle queue: settles while the pool's only connection is blocked",
          "[coherence][settle][integration]") {
    CohEnv env(3s);
    resetItems(env);
    auto id = env.seedItem("settle", 1);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));
    queueOf(env).pause();

    auto starter = [](io::Task<void> t) -> Starter { co_await std::move(t); };
    auto update = [&](int32_t value, bool& done, std::shared_ptr<io::SettleTicket>* out)
            -> io::Task<void> {
        try {
            (void)co_await CohItemL1::update(id, item(id, "settle", value));
        } catch (const io::PgUncertainError& e) {
            if (out) *out = e.settleTicket();
        } catch (...) {}
        done = true;
    };

    // A: sent, blocked on the lock, then its connection is lost.
    bool a_done = false;
    std::shared_ptr<io::SettleTicket> t;
    auto a = starter(update(2, a_done, &t));
    pumpUntil(env, [] { return false; }, 150ms);
    REQUIRE(::shutdown(env.pg_fd, SHUT_RD) == 0);
    REQUIRE(pumpUntil(env, [&] { return a_done; }, 2s));
    REQUIRE(t);

    // B: takes the pool's only connection and blocks on the same lock.
    bool b_done = false;
    auto b = starter(update(3, b_done, nullptr));
    pumpUntil(env, [] { return false; }, 150ms);
    REQUIRE_FALSE(b_done);
    REQUIRE(env.pool->totalConnections() == 1);
    REQUIRE(env.pool->idleCount() == 0);

    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 2s));
    CHECK_FALSE(b_done);
    CHECK_FALSE(backendAlive(env, t->backend));

    lock.reset();
    REQUIRE(pumpUntil(env, [&] { return b_done; }, 3s));
    CHECK(probeValue(env, id) == "3");
    if (a.handle) a.handle.destroy();
    if (b.handle) b.handle.destroy();
}

TEST_CASE("settle queue: Delay without a delay takes statement_timeout",
          "[coherence][settle][integration]") {
    SECTION("statement_timeout set: the ticket waits it plus a margin") {
        LogCapture logs;
        CohEnv env(300ms, io::UncertainSettle::Delay, 0ms,
                   "-c statement_timeout=700");
        CHECK(LogCapture::count(log::Level::Warn, "uncertain_settle is Delay") == 0);
        resetItems(env);
        auto id = env.seedItem("settle", 1);
        auto lock = lockItem(id);
        auto t = uncertainTicket(env, CohItemL1::update(id, item(id, "settle", 2)));
        REQUIRE(t);
        CHECK(t->delay == 700ms + io::kSettleMargin);
    }
    SECTION("no statement_timeout: warned at startup") {
        LogCapture logs;
        CohEnv env(300ms, io::UncertainSettle::Delay);
        CHECK(LogCapture::count(log::Level::Warn, "uncertain_settle is Delay") == 1);
    }
}

TEST_CASE("settle queue: destroyed with pending tickets, it abandons them",
          "[coherence][settle][integration]") {
    LogCapture logs;
    {
        CohEnv env(300ms);
        resetItems(env);
        auto id = env.seedItem("settle", 1);
        auto lock = lockItem(id);
        auto t = blockedUpdate(env, id, 2);
        CHECK(t->replays.size() == 1);  // dropped with the queue, never run
    }
    CHECK(LogCapture::count(log::Level::Warn, "abandoned unsettled") == 1);
}

// =============================================================================
// Settle outcome — once the backend is gone, the queue reads what became of the
// write: the transaction seen running on its backend, then its status. Only a
// ticket that is its backend's sole unresolved write owns that transaction.
// =============================================================================

namespace {

// Holds a lock on the whole table in an open transaction until destroyed: a
// write waits for it before any change, so before it has a transaction id.
struct TableLock {
    io::test::PgProbe locker{probeConnInfo()};
    explicit TableLock(const std::string& table) {
        locker.query("BEGIN");
        locker.query("LOCK TABLE " + table + " IN SHARE MODE");
    }
    ~TableLock() {
        try { locker.query("ROLLBACK"); } catch (...) {}
    }
};

// Settles `t` once resumed; returns the outcome its replays received.
std::optional<io::WriteOutcome> settleSeen(CohEnv& env,
                                           const std::shared_ptr<io::SettleTicket>& t) {
    auto seen = std::make_shared<std::optional<io::WriteOutcome>>();
    t->replays.push_back([seen](io::WriteOutcome o) -> io::Task<void> {
        *seen = o;
        co_return;
    });
    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    REQUIRE(t->state == io::SettleTicket::State::Settled);
    return *seen;
}

}  // namespace

TEST_CASE("settle outcome: a write terminated while it waits for a row lock was aborted",
          "[coherence][settle][outcome][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("outcome", 1);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));
    auto t = blockedUpdate(env, id, 2);

    // The write took a transaction id before waiting for the row: it is known.
    CHECK(settleSeen(env, t) == io::WriteOutcome::Aborted);
    CHECK(t->outcome == io::WriteOutcome::Aborted);
    CHECK(queueOf(env).settledAs(io::WriteOutcome::Aborted) == 1);
    CHECK_FALSE(backendAlive(env, t->backend));
    lock.reset();
    CHECK(probeValue(env, id) == "1");
}

TEST_CASE("settle outcome: a write that reads the row it replaces has its id while it waits",
          "[coherence][settle][outcome][list][integration]") {
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    auto warm = seedArticle(env, "coh-so-pre-warm");
    driveBounded(env.io, CohArticleL1::update(warm, articleIn(warm, "coh-so-pre-warm")));
    auto id = seedArticle(env, "coh-so-pre-a");
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(id));

    // Its pages need the row it replaces: the UPDATE reads it FOR UPDATE, and
    // waits there for the lock.
    queueOf(env).pause();
    auto t = uncertainTicket(env, CohArticleL1::update(id, articleIn(id, "coh-so-pre-b")));
    REQUIRE(t);
    CHECK(settleSeen(env, t) == io::WriteOutcome::Aborted);
    lock.reset();
    CHECK(articleCategory(env, id) == "coh-so-pre-a");
    resetArticleCaches<CohArticleL1>(env);
}

TEST_CASE("settle outcome: under Await, a write that commits once unblocked was committed",
          "[coherence][settle][outcome][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetItems(env);
    auto id = env.seedItem("outcome", 1);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));
    auto t = blockedUpdate(env, id, 2);

    queueOf(env).resume();
    pumpUntil(env, [] { return false; }, 400ms);
    REQUIRE(queueOf(env).pending() == 1);
    queueOf(env).pause();
    lock.reset();

    CHECK(settleSeen(env, t) == io::WriteOutcome::Committed);
    CHECK(queueOf(env).settledAs(io::WriteOutcome::Committed) == 1);
    CHECK(probeValue(env, id) == "2");
}

TEST_CASE("settle outcome: a write terminated while it waits for a table lock is unknown",
          "[coherence][settle][outcome][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("outcome", 1);
    std::optional<TableLock> lock;
    lock.emplace("relais_test_items");
    auto t = blockedUpdate(env, id, 2);

    // No change made yet, no transaction id: the write may have taken one and
    // committed between the read and the termination.
    CHECK(settleSeen(env, t) == io::WriteOutcome::Unknown);
    CHECK(queueOf(env).settledAs(io::WriteOutcome::Unknown) == 1);
    CHECK_FALSE(backendAlive(env, t->backend));
    lock.reset();
    CHECK(probeValue(env, id) == "1");
}

TEST_CASE("settle outcome: a write behind another unresolved one is unknown",
          "[coherence][settle][outcome][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("outcome", 1);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));
    auto t = blockedUpdate(env, id, 2);
    // As in a pipeline whose last two writes were never read: the transaction
    // seen running may be either one, and the first may have committed.
    t->unresolved = 2;

    CHECK(settleSeen(env, t) == io::WriteOutcome::Unknown);
    CHECK_FALSE(backendAlive(env, t->backend));
    lock.reset();
    CHECK(probeValue(env, id) == "1");
}

// =============================================================================
// Client check — the pool's connections ask the server to abort a statement
// whose client has closed its socket (client_connection_check_interval). A
// write that timed out then ends server-side on its own, settled or not.
// =============================================================================

namespace {

// The pool connection's value of the setting, and where it comes from.
std::pair<std::string, std::string> clientCheckOf(CohEnv& env) {
    auto r = driveBounded(env.io, io::PgClient<Io>(env.pool).query(
        "SELECT setting, source FROM pg_settings "
        "WHERE name = 'client_connection_check_interval'"));
    REQUIRE(r.rows() == 1);
    return {r[0].get<std::string>(0), r[0].get<std::string>(1)};
}

}  // namespace

TEST_CASE("client check: the pool's connections carry the configured interval",
          "[coherence][settle][client_check][integration]") {
    SECTION("set on the session") {
        CohEnv env(300ms, io::UncertainSettle::Terminate, 0ms, {}, 1500ms);
        CHECK(clientCheckOf(env) == std::pair<std::string, std::string>{"1500", "session"});
    }
    SECTION("0 sends nothing") {
        CohEnv env(300ms);
        CHECK(clientCheckOf(env).second == "default");
    }
    SECTION("never sent under Delay") {
        CohEnv env(300ms, io::UncertainSettle::Delay, 1s, {}, 1500ms);
        CHECK(clientCheckOf(env).second == "default");
    }
}

TEST_CASE("client check: a blocked write whose client left ends server-side unsettled",
          "[coherence][settle][client_check][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Terminate, 0ms, {}, 200ms);
    resetItems(env);
    auto id = env.seedItem("client-check", 1);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));
    auto t = blockedUpdate(env, id, 2);

    // The queue is paused: only the server's own check can end the backend.
    CHECK(pumpUntil(env, [&] { return !backendAlive(env, t->backend); }, 3s));
    CHECK(queueOf(env).pending() == 1);
    lock.reset();
    CHECK(probeValue(env, id) == "1");
}

// =============================================================================
// Replayed evictions — each cache tier evicts at once after an uncertain write,
// and registers that eviction on the write's ticket, which runs it again once
// the write can no longer commit. The write here blocks on a lock held by a
// probe; under Await, releasing the lock lets it commit AFTER its first
// eviction, and a read in between refills the cache with the old row: only the
// replay removes it.
// =============================================================================

using CohUserL1 = Repo<::entity::generated::TestUserEntity, "coh:user:l1">;
inline constexpr auto cohPurchaseUser = [](const auto& p) -> int64_t { return p.user_id; };
using CohPurchaseL1 = Repo<::entity::generated::TestPurchaseEntity, "coh:purchase:l1",
                           cfg::Local, Invalidate<CohUserL1, cohPurchaseUser>>;

namespace {

// Fails `op` uncertainly against a held lock, runs `refill` (reads that cache
// the old row once the first eviction is done), releases the lock, then lets
// the queue settle. Under Await the write commits as the lock goes.
template<typename Op, typename Refill>
std::shared_ptr<io::SettleTicket> settleAfterRefill(CohEnv& env, std::optional<RowLock>& lock,
                                                   Op op, Refill refill) {
    queueOf(env).pause();
    auto t = uncertainTicket(env, std::move(op));
    REQUIRE(t);
    REQUIRE_FALSE(t->replays.empty());
    refill();
    lock.reset();
    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    return t;
}

int32_t cachedValue(CohEnv& env, int64_t id) {
    auto v = driveBounded(env.io, CohItemL1::find(id));
    REQUIRE(v);
    return v->value;
}

bool inL1Item(int64_t id) {
    return static_cast<bool>(relais_test::TestInternals::getFromCache<CohItemL1>(id));
}

auto setValue(int32_t v) {
    return jcailloux::relais::entity::set<TestItemEntity::Field::value>(v);
}

int64_t seedUser(CohEnv& env, const std::string& name) {
    return std::stoll(env.probe.query(
        "INSERT INTO relais_test_users (username, email) VALUES ('" + name + "', '"
        + name + "@coh') RETURNING id").get(0, 0));
}

int64_t seedPurchase(CohEnv& env, int64_t user) {
    return std::stoll(env.probe.query(
        "INSERT INTO relais_test_purchases (user_id, product_name, amount, status) VALUES ("
        + std::to_string(user) + ", 'coh', 1, 'pending') RETURNING id").get(0, 0));
}

}  // namespace

TEST_CASE("precaution: an eviction runs at once and again only on a pending ticket",
          "[coherence][settle]") {
    int runs = 0;
    auto evict = [&runs] { ++runs; };

    SECTION("pending: registered") {
        auto t = ticketFor(io::UncertainSettle::Delay, {}, 1s);
        detail::precautionSync(t, evict);
        CHECK(runs == 1);
        REQUIRE(t->replays.size() == 1);
    }
    SECTION("settled: run once, not registered") {
        auto t = ticketFor(io::UncertainSettle::Delay, {}, 1s);
        t->state = io::SettleTicket::State::Settled;
        detail::precautionSync(t, evict);
        CHECK(runs == 1);
        CHECK(t->replays.empty());
    }
    SECTION("no ticket (settling Off): run once") {
        detail::precautionSync(nullptr, evict);
        CHECK(runs == 1);
    }
}

TEST_CASE("replay: a write committed after its eviction is evicted again once settled",
          "[coherence][settle][replay][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetItems(env);
    auto id = env.seedItem("replay", 1);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));

    settleAfterRefill(env, lock, CohItemL1::update(id, item(id, "replay", 2)), [&] {
        CHECK(cachedValue(env, id) == 1);  // the old row, cached after the first eviction
    });
    REQUIRE(probeValue(env, id) == "2");   // committed late
    CHECK(cachedValue(env, id) == 2);
}

TEST_CASE("replay: Terminate settles before the commit, the cache matches PostgreSQL",
          "[coherence][settle][replay][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto id = env.seedItem("replay", 1);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));
    auto t = blockedUpdate(env, id, 2);
    CHECK(cachedValue(env, id) == 1);
    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    REQUIRE(t->outcome == io::WriteOutcome::Aborted);
    CHECK(inL1Item(id));  // aborted: the copy read meanwhile is right, kept
    lock.reset();
    CHECK(probeValue(env, id) == "1");
    CHECK(cachedValue(env, id) == 1);
}

TEST_CASE("replay: patch, erase and upsert are evicted again once settled",
          "[coherence][settle][replay][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);

    SECTION("patch") {
        resetItems(env);
        auto id = env.seedItem("replay", 1);
        std::optional<RowLock> lock;
        lock.emplace("relais_test_items", "id=" + std::to_string(id));
        settleAfterRefill(env, lock, CohItemL1::patch(id, setValue(2)), [&] {
            CHECK(cachedValue(env, id) == 1);
        });
        REQUIRE(probeValue(env, id) == "2");
        CHECK(cachedValue(env, id) == 2);
    }
    SECTION("erase") {
        resetItems(env);
        auto id = env.seedItem("replay", 1);
        std::optional<RowLock> lock;
        lock.emplace("relais_test_items", "id=" + std::to_string(id));
        settleAfterRefill(env, lock, CohItemL1::erase(id), [&] {
            CHECK(cachedValue(env, id) == 1);
        });
        REQUIRE(probeValue(env, id) == "<absent>");
        CHECK_FALSE(driveBounded(env.io, CohItemL1::find(id)));
    }
    SECTION("upsert, update branch") {
        resetAkey(env);
        seedAkey(env, 7, 1);
        std::optional<RowLock> lock;
        lock.emplace("relais_test_assigned_keys", "key_id=7");
        settleAfterRefill(env, lock, CohAkeyL1::upsert(akey(7, 2)), [&] {
            auto v = driveBounded(env.io, CohAkeyL1::find(7));
            REQUIRE(v);
            CHECK(v->payload == 1);
        });
        REQUIRE(probeAkey(env, 7) == "2");
        auto v = driveBounded(env.io, CohAkeyL1::find(7));
        REQUIRE(v);
        CHECK(v->payload == 2);
    }
}

TEST_CASE("replay: an insert committed late enters its list page once settled",
          "[coherence][settle][replay][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetArticleCaches<CohArticleL1>(env);
    // Prepare the INSERT once, so the held one waits only on its result.
    driveBounded(env.io, CohArticleL1::insert(articleIn(0, "coh-rep-warm")));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "false");  // opens the transaction
    lock->locker.query("LOCK TABLE relais_test_articles IN SHARE MODE");

    auto pageSize = [&] {
        using Desc = CohArticleL1::ListDescriptorType;
        ld::ListQueryParams<Desc> p;
        p.limit = 10;
        p.filters.template get<"category">() = std::string("coh-rep-ins");
        return driveBounded(env.io, CohArticleL1::query(ld::seal<Desc>(std::move(p))))
            ->items.size();
    };
    settleAfterRefill(env, lock, CohArticleL1::insert(articleIn(0, "coh-rep-ins")), [&] {
        CHECK(pageSize() == 0);
    });
    REQUIRE(env.probe.query("SELECT count(*) FROM relais_test_articles WHERE category='coh-rep-ins'")
                .get(0, 0) == "1");
    CHECK(pageSize() == 1);
}

TEST_CASE("replay: the pages of both positions follow a move committed late",
          "[coherence][settle][replay][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetArticleCaches<CohArticleL1>(env);
    auto warm = seedArticle(env, "coh-rep-warm");
    driveBounded(env.io, CohArticleL1::update(warm, articleIn(warm, "coh-rep-warm")));
    auto id = seedArticle(env, "coh-rep-a");
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(id));

    settleAfterRefill(env, lock, CohArticleL1::update(id, articleIn(id, "coh-rep-b")), [&] {
        CHECK(pageHolds<CohArticleL1>(env, "coh-rep-a", id));
        CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rep-b", id));
    });
    REQUIRE(articleCategory(env, id) == "coh-rep-b");
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rep-a", id));
    CHECK(pageHolds<CohArticleL1>(env, "coh-rep-b", id));
}

TEST_CASE("replay: both cross-invalidation targets are evicted again once settled",
          "[coherence][settle][replay][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    env.probe.query("DELETE FROM relais_test_purchases");
    relais_test::TestInternals::resetEntityCacheState<CohUserL1>();
    relais_test::TestInternals::resetEntityCacheState<CohPurchaseL1>();
    relais_test::TestInternals::resetListCacheState<CohPurchaseL1>();
    auto u1 = seedUser(env, "coh-rep-u1");
    auto u2 = seedUser(env, "coh-rep-u2");
    auto pid = seedPurchase(env, u1);
    auto before = driveBounded(env.io, CohPurchaseL1::find(pid));  // the L1 copy keys u1
    REQUIRE(before);
    auto moved = ::entity::generated::TestPurchaseEntity(*before);
    moved.user_id = u2;
    driveBounded(env.io, CohPurchaseL1::update(pid, *before));  // prepares the UPDATE
    REQUIRE(driveBounded(env.io, CohPurchaseL1::find(pid)));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_purchases", "id=" + std::to_string(pid));

    auto inL1User = [](int64_t u) {
        return static_cast<bool>(relais_test::TestInternals::getFromCache<CohUserL1>(u));
    };
    settleAfterRefill(env, lock, CohPurchaseL1::update(pid, moved), [&] {
        REQUIRE(driveBounded(env.io, CohUserL1::find(u1)));
        REQUIRE(driveBounded(env.io, CohUserL1::find(u2)));
        REQUIRE(inL1User(u1));
        REQUIRE(inL1User(u2));
    });
    CHECK_FALSE(inL1User(u1));
    CHECK_FALSE(inL1User(u2));
    env.probe.query("DELETE FROM relais_test_users WHERE id IN (" + std::to_string(u1) + ", "
                    + std::to_string(u2) + ")");
}

TEST_CASE("replay: writes of one pipeline share one ticket and replay every eviction",
          "[coherence][settle][replay][integration]") {
    CohEnv env(300ms);
    resetItems(env);
    auto first = env.seedItem("replay-first", 0);
    auto locked = env.seedItem("replay-locked", 0);

    // Past the estimator's bootstrap, writes submitted while one is in flight
    // are batched instead of sent one by one.
    for (int i = 0; i < 6; ++i)
        driveBounded(env.io, CohItemL1::update(first, item(first, "replay-first", i)));

    queueOf(env).pause();
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(locked));
    std::array<std::shared_ptr<io::SettleTicket>, 3> tickets;
    auto write = [&](int slot, int64_t id, int32_t value) -> io::Task<void> {
        try {
            (void)co_await CohItemL1::update(id, item(id, "replay", value));
        } catch (const io::PgUncertainError& e) {
            if (slot >= 0) tickets[slot] = e.settleTicket();
        }
    };
    auto wave = [&]() -> io::Task<void> {
        std::vector<io::Task<void>> tasks;
        tasks.push_back(write(-1, first, 100));  // sent alone, completes
        tasks.push_back(write(0, locked, 1));    // the same row: one sub-batch
        tasks.push_back(write(1, locked, 2));
        tasks.push_back(write(2, locked, 3));
        co_await io::whenAll(std::move(tasks));
    };
    driveBounded(env.io, wave());
    REQUIRE(tickets[0]);
    REQUIRE(tickets[1] == tickets[0]);
    REQUIRE(tickets[2] == tickets[0]);
    CHECK(tickets[0]->replays.size() == 3);

    CHECK(cachedValue(env, locked) == 0);
    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    CHECK(queueOf(env).statements() == 1);
    CHECK_FALSE(inL1Item(locked));
    lock.reset();
    CHECK(probeValue(env, locked) == "0");
}

TEST_CASE("replay: writes of a pipeline whose end was read succeed and replay nothing",
          "[coherence][settle][replay][integration]") {
    // Writes on distinct rows in one pipeline, whatever the rows.
    io::batch::BatchScheduler<Io>::write_fanout_override = 1;
    struct Reset {
        ~Reset() { io::batch::BatchScheduler<Io>::write_fanout_override = 0; }
    } reset;
    CohEnv env(300ms);
    resetItems(env);
    auto first = env.seedItem("through-first", 0);
    auto a = env.seedItem("through-a", 0);
    auto b = env.seedItem("through-b", 0);
    auto locked = env.seedItem("through-locked", 0);

    // Past the estimator's bootstrap, writes submitted while one is in flight
    // are batched instead of sent one by one.
    for (int i = 0; i < 6; ++i)
        driveBounded(env.io, CohItemL1::update(first, item(first, "through-first", i)));

    queueOf(env).pause();
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(locked));
    std::array<bool, 3> failed{};
    std::shared_ptr<io::SettleTicket> ticket;
    auto write = [&](int slot, int64_t id, int32_t value) -> io::Task<void> {
        try {
            (void)co_await CohItemL1::update(id, item(id, "through", value));
        } catch (const io::PgUncertainError& e) {
            if (slot >= 0) { failed[slot] = true; ticket = e.settleTicket(); }
        }
    };
    auto wave = [&]() -> io::Task<void> {
        std::vector<io::Task<void>> tasks;
        tasks.push_back(write(-1, first, 100));  // sent alone, completes
        tasks.push_back(write(0, a, 1));         // one pipeline, in order
        tasks.push_back(write(1, b, 2));
        tasks.push_back(write(2, locked, 3));
        co_await io::whenAll(std::move(tasks));
    };
    driveBounded(env.io, wave());

    CHECK(failed == std::array<bool, 3>{false, false, true});
    REQUIRE(ticket);
    CHECK(ticket->replays.size() == 1);  // the locked row's eviction only
    CHECK(cachedValue(env, a) == 1);
    CHECK(cachedValue(env, b) == 2);

    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    lock.reset();
    CHECK(probeValue(env, locked) == "0");
}

TEST_CASE("replay: the second eviction is as narrow as the first",
          "[coherence][settle][replay][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetItems(env);
    auto id = env.seedItem("replay", 1);
    auto other = env.seedItem("replay-other", 5);
    CHECK(cachedValue(env, other) == 5);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_items", "id=" + std::to_string(id));

    settleAfterRefill(env, lock, CohItemL1::update(id, item(id, "replay", 2)), [&] {
        CHECK(cachedValue(env, id) == 1);
    });
    CHECK_FALSE(inL1Item(id));
    CHECK(inL1Item(other));
}

// =============================================================================
// Realigned list pages — after an uncertain write, the row leaves every page
// holding it (found by its key, whatever version they hold), now and again
// once the write is settled. Once settled, the row read again enters the pages
// of its position as committed, which may not be the caller's version. Each
// case also checks that a page of another group stays cached.
// =============================================================================

namespace {

// A trigger on `table` (relais_test_articles by default) for the length of a case.
struct ArticleTrigger {
    io::test::PgProbe& probe;
    std::string name;
    ArticleTrigger(CohEnv& env, std::string trigger, const std::string& timing,
                   const std::string& body, const std::string& table = "relais_test_articles")
        : probe(env.probe), name(std::move(trigger)) {
        drop();
        probe.query("CREATE FUNCTION " + name + "() RETURNS trigger AS $$ BEGIN " + body
                    + " RETURN NEW; END $$ LANGUAGE plpgsql");
        probe.query("CREATE TRIGGER " + name + " " + timing
                    + " ON " + table + " FOR EACH ROW EXECUTE FUNCTION " + name + "()");
    }
    ~ArticleTrigger() {
        try { drop(); } catch (...) {}
    }
    // Through the function, which the test role owns: dropping the trigger
    // itself needs to own the table.
    void drop() {
        probe.query("SET client_min_messages = warning");
        probe.query("DROP FUNCTION IF EXISTS " + name + "() CASCADE");
    }
};

// Rows of a category's first page, as relais serves it.
template<typename ListRepo>
size_t pageCount(CohEnv& env, const std::string& category) {
    using Desc = typename ListRepo::ListDescriptorType;
    ld::ListQueryParams<Desc> p;
    p.limit = 10;
    p.filters.template get<"category">() = category;
    return driveBounded(env.io, ListRepo::query(ld::seal<Desc>(std::move(p))))->items.size();
}

// Clears the injected reread failures whatever the outcome of the case.
struct RereadFaultReset {
    ~RereadFaultReset() { relais_test::TestInternals::failRereads<CohArticleL1>(0); }
};

}  // namespace

TEST_CASE("realign: an update rewritten by a trigger enters the pages of the committed row",
          "[coherence][settle][realign][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetArticleCaches<CohArticleL1>(env);
    ArticleTrigger trigger(env, "coh_rl_rewrite", "BEFORE UPDATE",
        "IF NEW.category = 'coh-rl-input' THEN NEW.category := 'coh-rl-real'; END IF;");
    auto warm = seedArticle(env, "coh-rl-warm");
    driveBounded(env.io, CohArticleL1::update(warm, articleIn(warm, "coh-rl-warm")));
    auto id = seedArticle(env, "coh-rl-old");
    auto other = seedArticle(env, "coh-rl-other");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-rl-other", other));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(id));

    settleAfterRefill(env, lock, CohArticleL1::update(id, articleIn(id, "coh-rl-input")), [&] {
        CHECK(pageHolds<CohArticleL1>(env, "coh-rl-old", id));
        CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rl-real", id));
    });
    REQUIRE(articleCategory(env, id) == "coh-rl-real");
    auto hidden = seedArticle(env, "coh-rl-other");  // a cached page does not show it
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rl-old", id));
    CHECK(pageHolds<CohArticleL1>(env, "coh-rl-real", id));
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rl-other", hidden));
}

TEST_CASE("realign: a patch committed late moves the row between its pages",
          "[coherence][settle][realign][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetArticleCaches<CohArticleL1>(env);
    driveBounded(env.io, CohArticleL1::patch(seedArticle(env, "coh-rl-pat-warm"),
                                             setCategory("coh-rl-pat-warm")));
    auto id = seedArticle(env, "coh-rl-pat-a");
    auto other = seedArticle(env, "coh-rl-pat-other");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-rl-pat-other", other));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(id));

    settleAfterRefill(env, lock, CohArticleL1::patch(id, setCategory("coh-rl-pat-b")), [&] {
        CHECK(pageHolds<CohArticleL1>(env, "coh-rl-pat-a", id));
        CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rl-pat-b", id));
    });
    REQUIRE(articleCategory(env, id) == "coh-rl-pat-b");
    auto hidden = seedArticle(env, "coh-rl-pat-other");
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rl-pat-a", id));
    CHECK(pageHolds<CohArticleL1>(env, "coh-rl-pat-b", id));
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rl-pat-other", hidden));
}

TEST_CASE("realign: an erase committed late leaves the pages holding the row",
          "[coherence][settle][realign][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetArticleCaches<CohArticleL1>(env);
    driveBounded(env.io, CohArticleL1::erase(seedArticle(env, "coh-rl-era-warm")));
    auto id = seedArticle(env, "coh-rl-era");
    auto other = seedArticle(env, "coh-rl-era-other");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-rl-era-other", other));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(id));

    settleAfterRefill(env, lock, CohArticleL1::erase(id), [&] {
        CHECK(pageHolds<CohArticleL1>(env, "coh-rl-era", id));  // no L1 copy of the row
    });
    REQUIRE_FALSE(articleExists(env, id));
    auto hidden = seedArticle(env, "coh-rl-era-other");
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rl-era", id));
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rl-era-other", hidden));
}

TEST_CASE("realign: Terminate cancels the update, the pages match PostgreSQL",
          "[coherence][settle][realign][list][integration]") {
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    auto warm = seedArticle(env, "coh-rl-ter-warm");
    driveBounded(env.io, CohArticleL1::update(warm, articleIn(warm, "coh-rl-ter-warm")));
    auto id = seedArticle(env, "coh-rl-ter-a");
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(id));

    queueOf(env).pause();
    auto t = uncertainTicket(env, CohArticleL1::update(id, articleIn(id, "coh-rl-ter-b")));
    REQUIRE(t);
    CHECK(pageHolds<CohArticleL1>(env, "coh-rl-ter-a", id));
    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    lock.reset();
    REQUIRE(articleCategory(env, id) == "coh-rl-ter-a");
    CHECK(pageHolds<CohArticleL1>(env, "coh-rl-ter-a", id));
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-rl-ter-b", id));
}

TEST_CASE("realign: a row that cannot be read again invalidates every page",
          "[coherence][settle][realign][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    LogCapture logs;
    RereadFaultReset faults;
    resetArticleCaches<CohArticleL1>(env);
    auto warm = seedArticle(env, "coh-rl-fail-warm");
    driveBounded(env.io, CohArticleL1::update(warm, articleIn(warm, "coh-rl-fail-warm")));
    auto id = seedArticle(env, "coh-rl-fail-a");
    auto other = seedArticle(env, "coh-rl-fail-other");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-rl-fail-other", other));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(id));
    relais_test::TestInternals::failRereads<CohArticleL1>(1);

    settleAfterRefill(env, lock, CohArticleL1::update(id, articleIn(id, "coh-rl-fail-b")), [] {});
    auto hidden = seedArticle(env, "coh-rl-fail-other");
    CHECK(pageHolds<CohArticleL1>(env, "coh-rl-fail-other", hidden));  // dropped as well
    CHECK(pageHolds<CohArticleL1>(env, "coh-rl-fail-b", id));
    CHECK(LogCapture::count(log::Level::Warn, "row unreadable") == 1);
}

TEST_CASE("realign: an insert with a database-assigned key rewritten by a trigger is found",
          "[coherence][settle][realign][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetArticleCaches<CohArticleL1>(env);
    ArticleTrigger trigger(env, "coh_rl_ins_rewrite", "BEFORE INSERT",
        "IF NEW.category = 'coh-rl-ins-input' THEN NEW.category := 'coh-rl-ins-real'; END IF;");
    driveBounded(env.io, CohArticleL1::insert(articleIn(0, "coh-rl-ins-warm")));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "false");  // opens the transaction
    lock->locker.query("LOCK TABLE relais_test_articles IN SHARE MODE");

    settleAfterRefill(env, lock, CohArticleL1::insert(articleIn(0, "coh-rl-ins-input")), [&] {
        CHECK(pageCount<CohArticleL1>(env, "coh-rl-ins-real") == 0);
    });
    REQUIRE(env.probe.query("SELECT count(*) FROM relais_test_articles "
                            "WHERE category='coh-rl-ins-real'").get(0, 0) == "1");
    CHECK(pageCount<CohArticleL1>(env, "coh-rl-ins-real") == 1);
}

TEST_CASE("realign: a page is tested against a row that left by the row's key",
          "[coherence][settle][realign][list]") {
    relais_test::TestInternals::resetListCacheState<CohArticleL1>();
    auto& lc = relais_test::TestInternals::listCacheOf<CohArticleL1>();
    using Cache = std::remove_reference_t<decltype(lc)>;
    auto query = [](std::string key, uint32_t offset = 0) {
        typename Cache::Query q;
        q.limit = 10;
        q.offset = offset;
        q.cache_key = std::move(key);
        return q;
    };
    auto rows = [](std::initializer_list<int64_t> ids) {
        typename Cache::Result r;
        for (auto id : ids) {
            TestArticleEntity a;
            a.id = id;
            r.items.push_back(a);
        }
        return r;
    };
    const auto holds = query("realign:holds");
    const auto other = query("realign:other");

    SECTION("fetched before the row left, stored after: refused only if it holds the row") {
        auto gen = lc.generation();
        lc.onRowLeft(7);
        lc.put(holds, rows({5, 7, 9}), gen);
        lc.put(other, rows({5, 9}), gen);
        CHECK_FALSE(lc.get(holds));
        CHECK(lc.get(other));
    }
    SECTION("stored before: invalidated on its next read only if it holds the row") {
        lc.put(holds, rows({5, 7, 9}), lc.generation());
        lc.put(other, rows({5, 9}), lc.generation());
        lc.onRowLeft(7);
        CHECK_FALSE(lc.get(holds));
        CHECK(lc.get(other));
    }
    SECTION("an offset page past the first counts the row before it: invalidated") {
        const auto past = query("realign:offset", 10);
        lc.put(past, rows({5, 9}), lc.generation());
        lc.onRowLeft(7);
        CHECK_FALSE(lc.get(past));
    }
    SECTION("the sweep drops the page holding the row, then forgets the row") {
        lc.put(holds, rows({5, 7, 9}), lc.generation());
        lc.put(other, rows({5, 9}), lc.generation());
        lc.onRowLeft(7);
        REQUIRE(relais_test::TestInternals::pendingRowLeftCount<CohArticleL1>() == 1);
        for (long c = 0; c < static_cast<long>(Cache::chunkCount()); ++c) lc.sweep(c);
        CHECK(relais_test::TestInternals::pendingRowLeftCount<CohArticleL1>() == 0);
        CHECK_FALSE(lc.get(holds));
        CHECK(lc.get(other));
    }
    relais_test::TestInternals::resetListCacheState<CohArticleL1>();
}

TEST_CASE("realign: writes that succeed leave no row behind and settle nothing",
          "[coherence][settle][realign][list][integration]") {
    CohEnv env(1s);
    resetArticleCaches<CohArticleL1>(env);
    auto created = driveBounded(env.io, CohArticleL1::insert(articleIn(0, "coh-rl-ok")));
    REQUIRE(created);
    auto id = created->id;
    CHECK(pageHolds<CohArticleL1>(env, "coh-rl-ok", id));
    driveBounded(env.io, CohArticleL1::update(id, articleIn(id, "coh-rl-ok-b")));
    driveBounded(env.io, CohArticleL1::patch(id, setCategory("coh-rl-ok-c")));
    CHECK(pageHolds<CohArticleL1>(env, "coh-rl-ok-c", id));
    driveBounded(env.io, CohArticleL1::erase(id));
    CHECK(relais_test::TestInternals::pendingRowLeftCount<CohArticleL1>() == 0);
    CHECK(queueOf(env).settled() == 0);
    CHECK(queueOf(env).pending() == 0);
}

// =============================================================================
// Notes — a write that may have changed rows nobody can name leaves a note on
// the entity tier: the predicate those rows satisfied. Until the write is
// settled, every copy the predicate may hold for is evicted when read; once
// settled, only the copies stored before the seal are, and a pass over the
// cache drains the note. The probe changes rows behind relais: a read that
// returns the probe's value read PostgreSQL, one that returns the old value was
// served from L1.
// =============================================================================

namespace {

using TI = relais_test::TestInternals;
using detail::RowMatch;

auto nameIs(std::string name) {
    return [name = std::move(name)](const TestItemEntity& e) {
        return e.name == name ? RowMatch::Maybe : RowMatch::No;
    };
}

void probeSet(CohEnv& env, int64_t id, int32_t v) {
    env.probe.query("UPDATE relais_test_items SET value=" + std::to_string(v)
                    + " WHERE id=" + std::to_string(id));
}

std::shared_ptr<io::SettleTicket> pendingTicket() {
    return ticketFor(io::UncertainSettle::Delay, {}, 1s);
}

// Settle `t` as the queue does, without a backend: its replays run at once.
void settleByHand(CohEnv& env, const std::shared_ptr<io::SettleTicket>& t) {
    t->state = io::SettleTicket::State::Settled;
    auto replays = std::move(t->replays);
    t->replays.clear();
    for (auto& r : replays) driveBounded(env.io, r(t->outcome));
}

}  // namespace

TEST_CASE("notes: a pending note evicts the copies it may touch, on every read",
          "[coherence][settle][notes][integration]") {
    CohEnv env(1s);
    resetItems(env);
    auto a = env.seedItem("note-a", 1);
    auto b = env.seedItem("note-b", 1);
    REQUIRE(cachedValue(env, a) == 1);
    REQUIRE(cachedValue(env, b) == 1);

    TI::noteRowsChanged<CohItemL1>(pendingTicket(), nameIs("note-a"));
    REQUIRE(TI::rowNoteCount<CohItemL1>() == 1);

    probeSet(env, a, 2);
    probeSet(env, b, 2);
    CHECK(cachedValue(env, a) == 2);  // touched: read again
    CHECK(cachedValue(env, b) == 1);  // not touched: served
    probeSet(env, a, 3);
    CHECK(cachedValue(env, a) == 3);  // the write may still commit: evicted again
    CHECK(TI::rowNoteCount<CohItemL1>() == 1);
    clearL1();
}

TEST_CASE("notes: a read started before the seal is not kept",
          "[coherence][settle][notes][integration]") {
    CohEnv env(2s);
    resetItems(env);
    TI::holdRowNoteDrain<CohItemL1>(true);
    auto a = env.seedItem("note-fill", 1);
    auto t = pendingTicket();
    TI::noteRowsChanged<CohItemL1>(t, nameIs("note-fill"));

    bool done = false;
    std::optional<int32_t> got;
    auto read = [&]() -> io::Task<void> {
        auto v = co_await CohItemL1::find(a);
        if (v) got = v->value;
        done = true;
    };
    env.holdPg();
    auto s = [](io::Task<void> task) -> Starter { co_await std::move(task); }(read());
    pumpUntil(env, [] { return false; }, 50ms);  // the SELECT is sent, its row held
    REQUIRE_FALSE(done);
    settleByHand(env, t);
    env.releasePg();
    REQUIRE(pumpUntil(env, [&] { return done; }, 2s));
    if (s.handle) s.handle.destroy();
    CHECK(got == 1);

    probeSet(env, a, 2);
    CHECK(cachedValue(env, a) == 2);  // the row read before the seal was not cached
    clearL1();
}

TEST_CASE("notes: a copy read after the seal is served, never evicted again",
          "[coherence][settle][notes][integration]") {
    CohEnv env(1s);
    resetItems(env);
    TI::holdRowNoteDrain<CohItemL1>(true);
    auto a = env.seedItem("note-fresh", 1);
    REQUIRE(cachedValue(env, a) == 1);
    auto t = pendingTicket();
    TI::noteRowsChanged<CohItemL1>(t, nameIs("note-fresh"));
    settleByHand(env, t);
    REQUIRE(TI::rowNoteCount<CohItemL1>() == 1);

    probeSet(env, a, 2);
    CHECK(cachedValue(env, a) == 2);  // stored before the seal: read again
    probeSet(env, a, 3);
    CHECK(cachedValue(env, a) == 2);  // read after it: served
    CHECK(cachedValue(env, a) == 2);
    clearL1();
}

TEST_CASE("notes: the pass evicts what a settled note touches, then drops it",
          "[coherence][settle][notes][integration]") {
    CohEnv env(1s);
    resetItems(env);
    std::vector<int64_t> touched, kept;
    for (int i = 0; i < 40; ++i) {
        touched.push_back(env.seedItem("note-drain", i));
        kept.push_back(env.seedItem("note-keep", i));
    }
    for (auto id : touched) cachedValue(env, id);
    for (auto id : kept) cachedValue(env, id);

    SECTION("settled by its ticket") {
        auto t = pendingTicket();
        TI::noteRowsChanged<CohItemL1>(t, nameIs("note-drain"));
        settleByHand(env, t);
    }
    SECTION("without a pending ticket: settled at once") {
        TI::noteRowsChanged<CohItemL1>(nullptr, nameIs("note-drain"));
    }
    REQUIRE(pumpUntil(env, [] { return TI::rowNoteCount<CohItemL1>() == 0; }, 2s));
    CHECK_FALSE(TI::rowNotesActive<CohItemL1>());
    for (auto id : touched) CHECK_FALSE(inL1Item(id));
    for (auto id : kept) CHECK(inL1Item(id));
    clearL1();
}

TEST_CASE("notes: without a note, a hit checks nothing",
          "[coherence][settle][notes][integration]") {
    CohEnv env(1s);
    resetItems(env);
    auto a = env.seedItem("note-none", 1);
    REQUIRE(cachedValue(env, a) == 1);
    REQUIRE(cachedValue(env, a) == 1);
    CHECK_FALSE(TI::rowNotesActive<CohItemL1>());
    CHECK(TI::rowNoteHitChecks<CohItemL1>() == 0);

    TI::holdRowNoteDrain<CohItemL1>(true);
    TI::noteRowsChanged<CohItemL1>(nullptr, nameIs("note-other"));
    REQUIRE(cachedValue(env, a) == 1);
    CHECK(TI::rowNoteHitChecks<CohItemL1>() == 1);
    clearL1();
}

// =============================================================================
// Predicates — a predicate write that fails uncertainly names no row: each tier
// evicts what its predicate may hold for, evaluated on the cached copies by a
// prudent evaluator that says No only where PostgreSQL surely says false. The
// list pages holding such a row are invalidated, and the values the write set
// say which other pages the rows may have entered.
// =============================================================================

namespace {

namespace en = jcailloux::relais::entity;
using CohGuardL1 = Repo<TestGuardRowEntity, "coh:guard:l1">;
using GuardF = TestGuardRowEntity::Field;
using ArticleF = TestArticleEntity::Field;
using ArticleL1Desc = CohArticleL1::ListDescriptorType;

// The column facts of the guard table, read from the catalog.
const detail::ColumnFacts& guardFacts(CohEnv& env) {
    TI::readColumnFacts<CohGuardL1>();
    REQUIRE(pumpUntil(env, [] { return TI::columnFacts<CohGuardL1>().known(); }, 2s));
    return TI::columnFacts<CohGuardL1>();
}

// Rows with bounds, NULL, NaN, accented and case-folded text, padded char(n),
// and the copies relais caches of them.
std::vector<TestGuardRowEntity> seedGuardRows(CohEnv& env) {
    env.probe.query("DELETE FROM relais_test_guard_rows");
    TI::resetEntityCacheState<CohGuardL1>();
    const char* rows[] = {
        "(5, 7, true, 'NaN', 'Élan', 'ab', 'ab', 'Pending', 'high', '2026-01-01 00:00:00+00')",
        "(-3, NULL, false, 1.5, 'élan', 'abc', 'abcd', 'pending', 'low', NULL)",
        "(2147483647, -9223372036854775808, false, 0, '', '', '', '', 'low', NULL)",
        "(0, 0, true, -2.5, 'a', 'ab ', 'ab  ', 'PENDING', 'high', '2030-01-01 00:00:00+00')",
    };
    std::vector<TestGuardRowEntity> out;
    for (const char* r : rows) {
        auto id = std::stoll(env.probe.query(
            std::string("INSERT INTO relais_test_guard_rows "
                        "(num, big, flag, ratio, name, label, code, tag, tone, at) VALUES ")
            + r + " RETURNING id").get(0, 0));
        auto v = driveBounded(env.io, CohGuardL1::find(id));
        REQUIRE(v);
        out.push_back(*v);
    }
    return out;
}

// What PostgreSQL says of `guard` on the row `id`: true, or false / NULL.
template<typename G>
bool pgHolds(CohEnv& env, const G& guard, int64_t id) {
    using Sql = detail::GuardSql<TestGuardRowEntity::TraitsType, G>;
    std::string sql = "SELECT COALESCE((";
    size_t param = 1;
    Sql::emit(sql, param, {});
    sql += "), false) FROM relais_test_guard_rows WHERE id = $" + std::to_string(param);
    io::PgParams params;
    Sql::bind(params, guard);
    params.push(id);
    auto r = driveBounded(env.io, PgProvider::queryParams(sql.c_str(), params));
    REQUIRE(r.rows() == 1);
    return r[0].get<bool>(0);
}

// The evaluator against PostgreSQL on every row: never No where the guard
// holds; when `certain`, No exactly where it does not.
template<typename G>
void differential(CohEnv& env, const std::vector<TestGuardRowEntity>& rows,
                  const detail::ColumnFacts& facts, const char* what, bool certain,
                  const G& guard) {
    for (const auto& row : rows) {
        const bool holds = pgHolds(env, guard, row.id);
        const auto verdict = detail::guardMatch(row, guard, facts);
        INFO(what << " on row " << row.id << ": PostgreSQL says " << holds);
        CHECK_FALSE((holds && verdict == RowMatch::No));
        if (certain) CHECK((verdict == RowMatch::No) == !holds);
    }
}

int64_t seedAuthored(CohEnv& env, const std::string& category, int64_t author,
                     int32_t view_count = 0) {
    return std::stoll(env.probe.query(
        "INSERT INTO relais_test_articles (category, author_id, title, view_count) VALUES ('"
        + category + "', " + std::to_string(author) + ", 'pred', "
        + std::to_string(view_count) + ") RETURNING id").get(0, 0));
}

bool inL1Article(int64_t id) {
    return static_cast<bool>(TI::getFromCache<CohArticleL1>(id));
}

bool authorPageHolds(CohEnv& env, int64_t author, int64_t id) {
    ld::ListQueryParams<ArticleL1Desc> p;
    p.limit = 10;
    p.filters.template get<"author_id">() = author;
    auto page = driveBounded(env.io, CohArticleL1::query(ld::seal<ArticleL1Desc>(std::move(p))));
    for (const auto& a : page->items)
        if (a.id == id) return true;
    return false;
}

// A page of `category` in view_count order.
auto viewPage(CohEnv& env, const std::string& category,
              std::optional<ld::TypedCursor<ArticleL1Desc>> cursor = std::nullopt) {
    ld::ListQueryParams<ArticleL1Desc> p;
    p.limit = 10;
    p.sort = ld::sortAsc<ArticleL1Desc, "view_count">();
    if (cursor) p.cursor = *cursor;
    p.filters.template get<"category">() = category;
    return driveBounded(env.io, CohArticleL1::query(ld::seal<ArticleL1Desc>(std::move(p))));
}

template<typename Page>
bool holds(const Page& page, int64_t id) {
    for (const auto& a : page->items)
        if (a.id == id) return true;
    return false;
}

auto byAuthor(int64_t author) { return en::when(en::eq<ArticleF::author_id>(author)); }

// Runs `op` against a lock on `locked` (it fails uncertainly), runs `check`
// while its ticket is pending, then lets the queue settle it.
template<typename Op, typename Check>
void whilePending(CohEnv& env, int64_t locked, Op op, Check check) {
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(locked));
    queueOf(env).pause();
    REQUIRE(uncertainTicket(env, std::move(op)));
    check();
    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
}

}  // namespace

TEST_CASE("predicate: the catalog tells the columns PostgreSQL compares as C++ does",
          "[coherence][settle][predicate][integration]") {
    CohEnv env(1s);
    auto rows = seedGuardRows(env);

    SECTION("exact: integers, boolean, text and varchar under the default collation, enum text") {
        const auto& facts = guardFacts(env);
        for (const char* column : {"id", "num", "big", "flag", "name", "label", "tone"})
            CHECK(facts.exactEquality(column));
        for (const char* column : {"ratio", "code", "tag", "at", "missing"})
            CHECK_FALSE(facts.exactEquality(column));
    }
    SECTION("a catalog read that fails leaves text Maybe, and is tried again") {
        LogCapture logs;
        TI::resetColumnFacts<CohGuardL1>(1);
        TI::readColumnFacts<CohGuardL1>();
        pumpUntil(env, [] { return false; }, 50ms);
        const auto& facts = TI::columnFacts<CohGuardL1>();
        REQUIRE_FALSE(facts.known());
        CHECK(LogCapture::count(log::Level::Warn, "column facts unreadable") == 1);
        const auto other = en::when(en::eq<GuardF::name>(std::string("nobody")));
        CHECK(detail::guardMatch(rows[0], other, facts) == RowMatch::Maybe);

        // The next uncertain predicate write reads it again.
        TI::holdRowNoteDrain<CohGuardL1>(true);
        TI::rowsMayHaveChanged<CohGuardL1>(nullptr,
            detail::guardPredicate<TestGuardRowEntity>(other, facts));
        REQUIRE(pumpUntil(env, [&] { return facts.known(); }, 2s));
        CHECK(detail::guardMatch(rows[0], other, facts) == RowMatch::No);
        TI::holdRowNoteDrain<CohGuardL1>(false);
        REQUIRE(pumpUntil(env, [] { return TI::rowNoteCount<CohGuardL1>() == 0; }, 2s));
    }
    TI::resetEntityCacheState<CohGuardL1>();
}

TEST_CASE("predicate: the evaluator never says No where PostgreSQL says true",
          "[coherence][settle][predicate][integration]") {
    CohEnv env(1s);
    auto rows = seedGuardRows(env);
    const auto& facts = guardFacts(env);
    const double nan = std::numeric_limits<double>::quiet_NaN();
    using relais_test::GuardTone;

    // Certain: integers, booleans, nullity, equality of exact text and enums.
    differential(env, rows, facts, "num = 5", true, en::when(en::eq<GuardF::num>(5)));
    differential(env, rows, facts, "num != 5", true, en::when(en::ne<GuardF::num>(5)));
    differential(env, rows, facts, "num > 5", true, en::when(en::gt<GuardF::num>(5)));
    differential(env, rows, facts, "num >= max", true,
                 en::when(en::ge<GuardF::num>(std::numeric_limits<int32_t>::max())));
    differential(env, rows, facts, "num <= -3", true, en::when(en::le<GuardF::num>(-3)));
    differential(env, rows, facts, "big = 7", true, en::when(en::eq<GuardF::big>(int64_t{7})));
    differential(env, rows, facts, "big != 7", true, en::when(en::ne<GuardF::big>(int64_t{7})));
    differential(env, rows, facts, "big < 0", true, en::when(en::lt<GuardF::big>(int64_t{0})));
    differential(env, rows, facts, "flag = true", true, en::when(en::eq<GuardF::flag>(true)));
    differential(env, rows, facts, "flag != false", true, en::when(en::ne<GuardF::flag>(false)));
    differential(env, rows, facts, "num in", true, en::when(en::in<GuardF::num>({1, 5, 9})));
    differential(env, rows, facts, "num not in", true, en::when(en::notIn<GuardF::num>({5})));
    differential(env, rows, facts, "big in", true, en::when(en::in<GuardF::big>({int64_t{7}})));
    differential(env, rows, facts, "big not in {}", true,
                 en::when(en::notIn<GuardF::big>(std::vector<int64_t>{})));
    differential(env, rows, facts, "big in {}", true,
                 en::when(en::in<GuardF::big>(std::vector<int64_t>{})));
    differential(env, rows, facts, "name = 'Élan'", true,
                 en::when(en::eq<GuardF::name>(std::string("Élan"))));
    differential(env, rows, facts, "name != 'élan'", true,
                 en::when(en::ne<GuardF::name>(std::string("élan"))));
    differential(env, rows, facts, "name in", true,
                 en::when(en::in<GuardF::name>({std::string("a"), std::string("Élan")})));
    differential(env, rows, facts, "label = 'ab'", true,
                 en::when(en::eq<GuardF::label>(std::string("ab"))));
    differential(env, rows, facts, "tone = high", true,
                 en::when(en::eq<GuardF::tone>(GuardTone::High)));
    differential(env, rows, facts, "tone in", true, en::when(en::in<GuardF::tone>({GuardTone::Low})));
    differential(env, rows, facts, "big is null", true, en::when(en::isNull<GuardF::big>()));
    differential(env, rows, facts, "at is not null", true, en::when(en::isNotNull<GuardF::at>()));
    differential(env, rows, facts, "and", true,
                 en::when(en::eq<GuardF::num>(5), en::ne<GuardF::name>(std::string("élan"))));
    differential(env, rows, facts, "or", true,
                 en::when(en::anyOf(en::eq<GuardF::num>(-3),
                                    en::allOf(en::eq<GuardF::flag>(true),
                                              en::lt<GuardF::big>(int64_t{1})))));

    // Prudent: what C++ cannot decide as PostgreSQL does.
    differential(env, rows, facts, "code = 'ab' (char(n))", false,
                 en::when(en::eq<GuardF::code>(std::string("ab"))));
    differential(env, rows, facts, "tag = 'pending' (case-insensitive)", false,
                 en::when(en::eq<GuardF::tag>(std::string("pending"))));
    differential(env, rows, facts, "tag in (case-insensitive)", false,
                 en::when(en::in<GuardF::tag>({std::string("pending")})));
    differential(env, rows, facts, "ratio = NaN", false, en::when(en::eq<GuardF::ratio>(nan)));
    differential(env, rows, facts, "ratio < 1", false, en::when(en::lt<GuardF::ratio>(1.0)));
    differential(env, rows, facts, "name > 'b' (collation order)", false,
                 en::when(en::gt<GuardF::name>(std::string("b"))));
    differential(env, rows, facts, "tone > low (rank)", false,
                 en::when(en::gt<GuardF::tone>(GuardTone::Low)));
    differential(env, rows, facts, "at <= now()", false, en::when(en::le<GuardF::at>(en::dbNow)));
    differential(env, rows, facts, "at = literal", false,
                 en::when(en::eq<GuardF::at>(std::string("2026-01-01 00:00:00+00"))));

    // Text the catalog does not call exact is never decided.
    CHECK(detail::guardMatch(rows[1], en::when(en::eq<GuardF::code>(std::string("zz"))), facts)
          == RowMatch::Maybe);
    CHECK(detail::guardMatch(rows[1], en::when(en::eq<GuardF::tag>(std::string("zz"))), facts)
          == RowMatch::Maybe);
    CHECK(detail::guardMatch(rows[1], en::when(en::eq<GuardF::name>(std::string("zz"))), facts)
          == RowMatch::No);
    TI::resetEntityCacheState<CohGuardL1>();
}

TEST_CASE("predicate: an uncertain patchWhere evicts the copies and pages its rows may be in",
          "[coherence][settle][predicate][list][integration]") {
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    auto src = seedAuthored(env, "coh-pw-src", 77);
    auto src2 = seedAuthored(env, "coh-pw-src", 77);
    auto other = seedAuthored(env, "coh-pw-oth", 5);
    // The task refers to its predicate and updates: they outlive it.
    const auto pred = byAuthor(77);
    const auto title = en::set<ArticleF::title>(std::string("patched"));
    auto op = [&] { return CohArticleL1::patchWhere(pred, title); };

    SECTION("cached rows: the copies and pages that may hold one") {
        for (auto id : {src, src2, other}) REQUIRE(driveBounded(env.io, CohArticleL1::find(id)));
        REQUIRE(pageHolds<CohArticleL1>(env, "coh-pw-src", src));
        REQUIRE(pageHolds<CohArticleL1>(env, "coh-pw-oth", other));
        REQUIRE(authorPageHolds(env, 5, other));

        whilePending(env, src, op(), [&] {
            CHECK_FALSE(inL1Article(src));
            CHECK_FALSE(inL1Article(src2));
            CHECK(inL1Article(other));
            auto hidden_src = seedAuthored(env, "coh-pw-src", 5);
            auto hidden_oth = seedAuthored(env, "coh-pw-oth", 5);
            CHECK(pageHolds<CohArticleL1>(env, "coh-pw-src", hidden_src));        // read again
            CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-pw-oth", hidden_oth));  // kept
            CHECK_FALSE(authorPageHolds(env, 5, hidden_oth));                     // kept
        });
    }
    SECTION("rows without a copy or a page: nothing else goes") {
        REQUIRE(driveBounded(env.io, CohArticleL1::find(other)));
        REQUIRE(pageHolds<CohArticleL1>(env, "coh-pw-oth", other));
        REQUIRE(authorPageHolds(env, 5, other));

        whilePending(env, src, op(), [&] {
            CHECK(inL1Article(other));
            auto hidden = seedAuthored(env, "coh-pw-oth", 5);
            CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-pw-oth", hidden));
            CHECK_FALSE(authorPageHolds(env, 5, hidden));
        });
    }
    resetArticleCaches<CohArticleL1>(env);
}

TEST_CASE("predicate: rows an uncertain patchWhere may have moved enter only the pages they may reach",
          "[coherence][settle][predicate][list][integration]") {
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    auto src = seedAuthored(env, "coh-pw-src", 77, 100);
    seedAuthored(env, "coh-pw-src", 77, 100);

    SECTION("a sort column set to one value: the pages around it") {
        for (int32_t v = 1; v <= 25; ++v) seedAuthored(env, "coh-pw-sort", 1, v);
        auto first = viewPage(env, "coh-pw-sort");
        REQUIRE(first->items.size() == 10);
        auto cursor = ld::TypedCursor<ArticleL1Desc>::decode(first->cursor());
        REQUIRE(cursor);
        REQUIRE(viewPage(env, "coh-pw-sort", cursor)->items.front().view_count == 11);

        whilePending(env, src,
            CohArticleL1::patchWhere(byAuthor(77), en::set<ArticleF::view_count>(15)), [&] {
                auto below = seedAuthored(env, "coh-pw-sort", 1, 0);
                auto around = seedAuthored(env, "coh-pw-sort", 1, 12);
                CHECK_FALSE(holds(viewPage(env, "coh-pw-sort"), below));      // kept
                CHECK(holds(viewPage(env, "coh-pw-sort", cursor), around));   // read again
            });
    }
    SECTION("a filter column set: the pages of the groups it admits, whole") {
        auto dst = seedAuthored(env, "coh-pw-dst", 1);
        auto oth = seedAuthored(env, "coh-pw-oth", 5);
        REQUIRE(pageHolds<CohArticleL1>(env, "coh-pw-dst", dst));
        REQUIRE(pageHolds<CohArticleL1>(env, "coh-pw-oth", oth));
        REQUIRE(authorPageHolds(env, 5, oth));

        whilePending(env, src,
            CohArticleL1::patchWhere(byAuthor(77),
                                     en::set<ArticleF::category>(std::string("coh-pw-dst"))), [&] {
                auto hidden_dst = seedAuthored(env, "coh-pw-dst", 1);
                auto hidden_oth = seedAuthored(env, "coh-pw-oth", 5);
                CHECK(pageHolds<CohArticleL1>(env, "coh-pw-dst", hidden_dst));        // read again
                CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-pw-oth", hidden_oth));  // incompatible
                CHECK_FALSE(authorPageHolds(env, 5, hidden_oth));  // filters no column it set
            });
    }
    resetArticleCaches<CohArticleL1>(env);
}

TEST_CASE("predicate: an uncertain eraseWhere or claim evicts the copies and pages its rows may be in",
          "[coherence][settle][predicate][list][integration]") {
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    auto src = seedAuthored(env, "coh-pw-src", 77);
    auto other = seedAuthored(env, "coh-pw-oth", 5);
    for (auto id : {src, other}) REQUIRE(driveBounded(env.io, CohArticleL1::find(id)));
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-pw-src", src));
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-pw-oth", other));
    REQUIRE(authorPageHolds(env, 5, other));

    auto check = [&] {
        CHECK_FALSE(inL1Article(src));
        CHECK(inL1Article(other));
        auto hidden_src = seedAuthored(env, "coh-pw-src", 5);
        auto hidden_oth = seedAuthored(env, "coh-pw-oth", 5);
        CHECK(pageHolds<CohArticleL1>(env, "coh-pw-src", hidden_src));
        CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-pw-oth", hidden_oth));
        CHECK_FALSE(authorPageHolds(env, 5, hidden_oth));
    };
    SECTION("eraseWhere") {
        whilePending(env, src, CohArticleL1::eraseWhere({.author_id = int64_t{77}}), check);
    }
    SECTION("claim") {
        whilePending(env, src,
            CohArticleL1::claim<ClaimOptions{.lock = Lock::Wait}>(
                byAuthor(77), 1, en::set<ArticleF::title>(std::string("taken"))),
            check);
    }
    resetArticleCaches<CohArticleL1>(env);
}


TEST_CASE("predicate: an eraseWhere committed after its eviction is evicted again once settled",
          "[coherence][settle][predicate][replay][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    resetArticleCaches<CohArticleL1>(env);
    auto src = seedAuthored(env, "coh-ew-src", 77);
    auto other = seedAuthored(env, "coh-ew-src", 5);
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(src));

    settleAfterRefill(env, lock, CohArticleL1::eraseWhere({.author_id = int64_t{77}}), [&] {
        // The DELETE waits on the lock: the row is read again and cached.
        REQUIRE(driveBounded(env.io, CohArticleL1::find(src)));
        CHECK(pageHolds<CohArticleL1>(env, "coh-ew-src", src));
    });
    REQUIRE_FALSE(articleExists(env, src));  // committed late
    CHECK_FALSE(driveBounded(env.io, CohArticleL1::find(src)));
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-ew-src", src));
    CHECK(pageHolds<CohArticleL1>(env, "coh-ew-src", other));
    resetArticleCaches<CohArticleL1>(env);
}

// =============================================================================
// Cross-invalidation of an uncertain write — the targets of the row before it
// come from its L1 copy, else from a cached page no write has touched since it
// was stored, else every target (logged); those of the row after, from the
// row read again once settled. A list repo over the same rows locates them by
// their key or by what they hold. Each case checks that a target the write
// does not reach stays cached.
// =============================================================================

using PurchaseF = ::entity::generated::TestPurchaseEntity::Field;
using CohPurchaseListL1 = Repo<::entity::generated::TestPurchaseEntity, "coh:purchase:list:l1">;
using CohPurchaseX = Repo<::entity::generated::TestPurchaseEntity, "coh:purchase:x:l1",
                          cfg::Local, Invalidate<CohUserL1, cohPurchaseUser>,
                          InvalidateList<CohPurchaseListL1>>;

namespace {

constexpr std::string_view kEveryTarget = "invalidated every target";

struct CrossRows {
    int64_t u1, u2, u3;  // u1 owns the purchase, u3 is reached by nothing
    int64_t pid;
};

CrossRows resetCross(CohEnv& env, const std::string& tag) {
    env.probe.query("DELETE FROM relais_test_purchases");
    TI::resetEntityCacheState<CohUserL1>();
    TI::resetEntityCacheState<CohPurchaseX>();
    TI::resetListCacheState<CohPurchaseX>();
    TI::resetEntityCacheState<CohPurchaseListL1>();
    TI::resetListCacheState<CohPurchaseListL1>();
    CrossRows r{};
    r.u1 = seedUser(env, tag + "-u1");
    r.u2 = seedUser(env, tag + "-u2");
    r.u3 = seedUser(env, tag + "-u3");
    r.pid = seedPurchase(env, r.u1);
    return r;
}

void dropUsers(CohEnv& env, const CrossRows& r) {
    env.probe.query("DELETE FROM relais_test_purchases");
    env.probe.query("DELETE FROM relais_test_users WHERE id IN (" + std::to_string(r.u1) + ", "
                    + std::to_string(r.u2) + ", " + std::to_string(r.u3) + ")");
}

bool userInL1(int64_t u) {
    return static_cast<bool>(TI::getFromCache<CohUserL1>(u));
}

void cacheUsers(CohEnv& env, std::initializer_list<int64_t> users) {
    for (auto u : users) {
        REQUIRE(driveBounded(env.io, CohUserL1::find(u)));
        REQUIRE(userInL1(u));
    }
}

// Every user is a target until the write is settled: a copy read meanwhile
// is not kept. Once settled, one is.
void everyUserUntilSettled(CohEnv& env, const CrossRows& r) {
    for (auto u : {r.u1, r.u2, r.u3}) {
        REQUIRE(driveBounded(env.io, CohUserL1::find(u)));
        CHECK_FALSE(userInL1(u));
    }
}

void usersKeptOnceSettled(CohEnv& env, const CrossRows& r) {
    cacheUsers(env, {r.u1, r.u2, r.u3});
}

template<typename ListRepo>
bool purchasePageHolds(CohEnv& env, int64_t user, int64_t id) {
    using Desc = typename ListRepo::ListDescriptorType;
    ld::ListQueryParams<Desc> p;
    p.limit = 10;
    p.filters.template get<"user_id">() = user;
    auto page = driveBounded(env.io, ListRepo::query(ld::seal<Desc>(std::move(p))));
    for (const auto& row : page->items)
        if (row.id == id) return true;
    return false;
}

// The purchase moved to `user`, its UPDATE prepared, its L1 copy dropped.
::entity::generated::TestPurchaseEntity movedPurchase(CohEnv& env, int64_t pid, int64_t user) {
    auto before = driveBounded(env.io, CohPurchaseX::find(pid));
    REQUIRE(before);
    ::entity::generated::TestPurchaseEntity moved(*before);
    moved.user_id = user;
    driveBounded(env.io, CohPurchaseX::update(pid, *before));
    TI::evict<CohPurchaseX>(pid);
    REQUIRE_FALSE(TI::getFromCache<CohPurchaseX>(pid));
    return moved;
}

}  // namespace

TEST_CASE("cross: a row only a page holds names its old target",
          "[coherence][settle][cross][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    LogCapture logs;
    auto r = resetCross(env, "coh-x-page");
    auto moved = movedPurchase(env, r.pid, r.u2);
    REQUIRE(purchasePageHolds<CohPurchaseX>(env, r.u1, r.pid));  // the only copy
    std::optional<RowLock> lock;
    lock.emplace("relais_test_purchases", "id=" + std::to_string(r.pid));

    settleAfterRefill(env, lock, CohPurchaseX::update(r.pid, moved),
                      [&] { cacheUsers(env, {r.u1, r.u2, r.u3}); });
    CHECK_FALSE(userInL1(r.u1));  // from the page
    CHECK_FALSE(userInL1(r.u2));  // from the row read again
    CHECK(userInL1(r.u3));
    CHECK(LogCapture::count(log::Level::Warn, kEveryTarget) == 0);
    dropUsers(env, r);
}

TEST_CASE("cross: a page touched since it was stored is passed over",
          "[coherence][settle][cross][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    LogCapture logs;
    auto r = resetCross(env, "coh-x-touched");
    auto moved = movedPurchase(env, r.pid, r.u2);
    REQUIRE(purchasePageHolds<CohPurchaseX>(env, r.u1, r.pid));
    ::entity::generated::TestPurchaseEntity sibling(moved);
    sibling.id = 0;
    sibling.user_id = r.u1;
    REQUIRE(driveBounded(env.io, CohPurchaseX::insert(sibling)));  // touches the page
    std::optional<RowLock> lock;
    lock.emplace("relais_test_purchases", "id=" + std::to_string(r.pid));

    settleAfterRefill(env, lock, CohPurchaseX::update(r.pid, moved),
                      [&] { everyUserUntilSettled(env, r); });
    CHECK(LogCapture::count(log::Level::Warn, kEveryTarget) == 1);  // no source
    usersKeptOnceSettled(env, r);
    dropUsers(env, r);
}

TEST_CASE("cross: without a copy, every target the row cannot locate otherwise",
          "[coherence][settle][cross][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    LogCapture logs;
    auto r = resetCross(env, "coh-x-none");
    auto moved = movedPurchase(env, r.pid, r.u2);
    auto p3 = seedPurchase(env, r.u3);
    REQUIRE(purchasePageHolds<CohPurchaseListL1>(env, r.u1, r.pid));
    REQUIRE_FALSE(purchasePageHolds<CohPurchaseListL1>(env, r.u2, r.pid));
    REQUIRE(purchasePageHolds<CohPurchaseListL1>(env, r.u3, p3));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_purchases", "id=" + std::to_string(r.pid));

    settleAfterRefill(env, lock, CohPurchaseX::update(r.pid, moved),
                      [&] { everyUserUntilSettled(env, r); });
    CHECK(LogCapture::count(log::Level::Warn, kEveryTarget) == 1);
    usersKeptOnceSettled(env, r);
    // The list repo over the same rows: the row left the page holding it,
    // entered the one of its committed position, and no other page went.
    auto hidden = seedPurchase(env, r.u3);
    CHECK_FALSE(purchasePageHolds<CohPurchaseListL1>(env, r.u1, r.pid));
    CHECK(purchasePageHolds<CohPurchaseListL1>(env, r.u2, r.pid));
    CHECK_FALSE(purchasePageHolds<CohPurchaseListL1>(env, r.u3, hidden));
    dropUsers(env, r);
}

TEST_CASE("cross: a foreign key rewritten by a trigger invalidates the real target",
          "[coherence][settle][cross][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    LogCapture logs;
    auto r = resetCross(env, "coh-x-trigger");
    ArticleTrigger trigger(env, "coh_x_rewrite", "BEFORE UPDATE",
        "IF NEW.user_id = " + std::to_string(r.u2) + " THEN NEW.user_id := "
            + std::to_string(r.u3) + "; END IF;",
        "relais_test_purchases");
    auto moved = movedPurchase(env, r.pid, r.u2);
    REQUIRE(driveBounded(env.io, CohPurchaseX::find(r.pid)));  // the L1 copy keys u1
    std::optional<RowLock> lock;
    lock.emplace("relais_test_purchases", "id=" + std::to_string(r.pid));

    settleAfterRefill(env, lock, CohPurchaseX::update(r.pid, moved),
                      [&] { cacheUsers(env, {r.u1, r.u2, r.u3}); });
    REQUIRE(env.probe.query("SELECT user_id FROM relais_test_purchases WHERE id = "
                            + std::to_string(r.pid)).get(0, 0) == std::to_string(r.u3));
    CHECK_FALSE(userInL1(r.u1));
    CHECK_FALSE(userInL1(r.u3));  // the committed row's
    CHECK(userInL1(r.u2));        // the input's, evicted once, never written
    CHECK(LogCapture::count(log::Level::Warn, kEveryTarget) == 0);
    dropUsers(env, r);
}

TEST_CASE("cross: an uncertain predicate write reaches every target it cannot locate",
          "[coherence][settle][cross][predicate][list][integration]") {
    CohEnv env(300ms);
    LogCapture logs;
    auto r = resetCross(env, "coh-x-pred");
    auto p3 = seedPurchase(env, r.u3);
    cacheUsers(env, {r.u1, r.u3});
    REQUIRE(purchasePageHolds<CohPurchaseListL1>(env, r.u1, r.pid));
    REQUIRE(purchasePageHolds<CohPurchaseListL1>(env, r.u3, p3));
    const auto pred = en::when(en::eq<PurchaseF::user_id>(r.u1));
    const auto amount = en::set<PurchaseF::amount>(int32_t{5});
    std::optional<RowLock> lock;
    lock.emplace("relais_test_purchases", "id=" + std::to_string(r.pid));
    queueOf(env).pause();
    REQUIRE(uncertainTicket(env, CohPurchaseX::patchWhere(pred, amount)));

    CHECK_FALSE(userInL1(r.u1));
    CHECK_FALSE(userInL1(r.u3));  // every user
    CHECK(LogCapture::count(log::Level::Warn, kEveryTarget) == 1);
    auto hidden1 = seedPurchase(env, r.u1);
    auto hidden3 = seedPurchase(env, r.u3);
    CHECK(purchasePageHolds<CohPurchaseListL1>(env, r.u1, hidden1));        // read again
    CHECK_FALSE(purchasePageHolds<CohPurchaseListL1>(env, r.u3, hidden3));  // kept
    lock.reset();
    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    dropUsers(env, r);
}

TEST_CASE("cross: an uncertain eraseMany names the targets and pages of the rows it found",
          "[coherence][settle][cross][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    LogCapture logs;
    auto r = resetCross(env, "coh-x-many");
    auto p2 = seedPurchase(env, r.u2);
    auto p3 = seedPurchase(env, r.u3);
    REQUIRE(driveBounded(env.io, CohPurchaseX::find(r.pid)));  // an L1 copy
    REQUIRE(purchasePageHolds<CohPurchaseX>(env, r.u2, p2));    // only a page
    REQUIRE(purchasePageHolds<CohPurchaseX>(env, r.u3, p3));
    REQUIRE_FALSE(TI::getFromCache<CohPurchaseX>(p2));
    const std::vector<int64_t> ids{r.pid, p2};
    std::optional<RowLock> lock;
    lock.emplace("relais_test_purchases", "id=" + std::to_string(r.pid));

    settleAfterRefill(env, lock, CohPurchaseX::eraseMany(std::span<const int64_t>(ids)),
                      [&] { cacheUsers(env, {r.u1, r.u2, r.u3}); });
    REQUIRE(env.probe.query("SELECT count(*) FROM relais_test_purchases WHERE id IN ("
                            + std::to_string(r.pid) + ", " + std::to_string(p2) + ")")
                .get(0, 0) == "0");  // committed late
    CHECK_FALSE(userInL1(r.u1));  // from the L1 copy
    CHECK_FALSE(userInL1(r.u2));  // from the page
    CHECK(userInL1(r.u3));
    CHECK(LogCapture::count(log::Level::Warn, kEveryTarget) == 0);
    auto hidden3 = seedPurchase(env, r.u3);
    CHECK_FALSE(purchasePageHolds<CohPurchaseX>(env, r.u2, p2));
    CHECK_FALSE(purchasePageHolds<CohPurchaseX>(env, r.u3, hidden3));  // kept
    dropUsers(env, r);
}

TEST_CASE("cross: an uncertain eraseMany of rows nobody holds reaches every target it cannot locate",
          "[coherence][settle][cross][list][integration]") {
    CohEnv env(300ms, io::UncertainSettle::Await);
    LogCapture logs;
    auto r = resetCross(env, "coh-x-many-none");
    auto p3 = seedPurchase(env, r.u3);
    REQUIRE(purchasePageHolds<CohPurchaseListL1>(env, r.u1, r.pid));
    REQUIRE(purchasePageHolds<CohPurchaseListL1>(env, r.u3, p3));
    const std::vector<int64_t> ids{r.pid};
    std::optional<RowLock> lock;
    lock.emplace("relais_test_purchases", "id=" + std::to_string(r.pid));

    settleAfterRefill(env, lock, CohPurchaseX::eraseMany(std::span<const int64_t>(ids)),
                      [&] { everyUserUntilSettled(env, r); });
    CHECK(LogCapture::count(log::Level::Warn, kEveryTarget) == 1);
    usersKeptOnceSettled(env, r);
    // The list repo over the same rows: only the page holding the row went.
    auto hidden3 = seedPurchase(env, r.u3);
    CHECK_FALSE(purchasePageHolds<CohPurchaseListL1>(env, r.u1, r.pid));
    CHECK_FALSE(purchasePageHolds<CohPurchaseListL1>(env, r.u3, hidden3));
    dropUsers(env, r);
}

// =============================================================================
// Aborted at settle — a write the server terminated before it committed left
// the database as it was: the cache, evicted once when the write failed, is
// already right. Nothing runs again: no second eviction, no row read again, no
// page invalidated as a last resort, and a note is dropped without a pass.
// =============================================================================

namespace {

// Lets the paused queue settle `t`, and requires the write found aborted.
void settleAborted(CohEnv& env, const std::shared_ptr<io::SettleTicket>& t) {
    queueOf(env).resume();
    REQUIRE(pumpUntil(env, [&] { return settledAll(env); }, 7s));
    REQUIRE(t->outcome == io::WriteOutcome::Aborted);
}

}  // namespace

TEST_CASE("outcome: an aborted update is neither evicted again nor read again",
          "[coherence][settle][outcome][list][integration]") {
    CohEnv env(300ms);
    LogCapture logs;
    RereadFaultReset faults;
    resetArticleCaches<CohArticleL1>(env);
    auto warm = seedArticle(env, "coh-oa-upd-warm");
    driveBounded(env.io, CohArticleL1::update(warm, articleIn(warm, "coh-oa-upd-warm")));
    auto id = seedArticle(env, "coh-oa-upd-a");
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(id));
    // A row read again once settled would fail, and invalidate every page.
    relais_test::TestInternals::failRereads<CohArticleL1>(1);

    queueOf(env).pause();
    auto t = uncertainTicket(env, CohArticleL1::update(id, articleIn(id, "coh-oa-upd-b")));
    REQUIRE(t);
    REQUIRE(driveBounded(env.io, CohArticleL1::find(id)));  // read again after the eviction
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-oa-upd-a", id));
    const auto rows_left = TI::pendingRowLeftCount<CohArticleL1>();

    settleAborted(env, t);
    CHECK(inL1Article(id));
    CHECK(TI::pendingRowLeftCount<CohArticleL1>() == rows_left);
    CHECK(LogCapture::count(log::Level::Warn, "row unreadable") == 0);
    auto hidden = seedArticle(env, "coh-oa-upd-a");
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-oa-upd-a", hidden));  // page kept
    lock.reset();
    REQUIRE(articleCategory(env, id) == "coh-oa-upd-a");
    resetArticleCaches<CohArticleL1>(env);
}

TEST_CASE("outcome: an aborted update leaves its cross targets as read meanwhile",
          "[coherence][settle][outcome][integration]") {
    CohEnv env(300ms);
    env.probe.query("DELETE FROM relais_test_purchases");
    relais_test::TestInternals::resetEntityCacheState<CohUserL1>();
    relais_test::TestInternals::resetEntityCacheState<CohPurchaseL1>();
    auto u1 = seedUser(env, "coh-oa-u1");
    auto u2 = seedUser(env, "coh-oa-u2");
    auto pid = seedPurchase(env, u1);
    auto before = driveBounded(env.io, CohPurchaseL1::find(pid));
    REQUIRE(before);
    auto moved = ::entity::generated::TestPurchaseEntity(*before);
    moved.user_id = u2;
    driveBounded(env.io, CohPurchaseL1::update(pid, *before));  // prepares the UPDATE
    REQUIRE(driveBounded(env.io, CohPurchaseL1::find(pid)));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_purchases", "id=" + std::to_string(pid));
    auto inL1User = [](int64_t u) {
        return static_cast<bool>(relais_test::TestInternals::getFromCache<CohUserL1>(u));
    };

    queueOf(env).pause();
    auto t = uncertainTicket(env, CohPurchaseL1::update(pid, moved));
    REQUIRE(t);
    REQUIRE(driveBounded(env.io, CohUserL1::find(u1)));
    REQUIRE(driveBounded(env.io, CohUserL1::find(u2)));

    settleAborted(env, t);
    CHECK(inL1User(u1));  // the row before: not evicted again
    CHECK(inL1User(u2));  // the row as committed: not read again
    lock.reset();
    env.probe.query("DELETE FROM relais_test_purchases WHERE id = " + std::to_string(pid));
    env.probe.query("DELETE FROM relais_test_users WHERE id IN (" + std::to_string(u1) + ", "
                    + std::to_string(u2) + ")");
}

TEST_CASE("outcome: an aborted patchWhere drops its note without a pass",
          "[coherence][settle][outcome][predicate][integration]") {
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    TI::holdRowNoteDrain<CohArticleL1>(true);
    auto src = seedAuthored(env, "coh-oa-pw", 77);
    REQUIRE(driveBounded(env.io, CohArticleL1::find(src)));
    std::optional<RowLock> lock;
    lock.emplace("relais_test_articles", "id=" + std::to_string(src));
    // The task refers to its predicate and updates: they outlive it.
    const auto pred = byAuthor(77);
    const auto title = en::set<ArticleF::title>(std::string("patched"));

    queueOf(env).pause();
    auto t = uncertainTicket(env, CohArticleL1::patchWhere(pred, title));
    REQUIRE(t);
    REQUIRE(TI::rowNoteCount<CohArticleL1>() == 1);

    settleAborted(env, t);
    CHECK(TI::rowNoteCount<CohArticleL1>() == 0);  // dropped, though no pass may run
    CHECK_FALSE(TI::rowNotesActive<CohArticleL1>());
    lock.reset();
    auto copy = driveBounded(env.io, CohArticleL1::find(src));
    REQUIRE(copy);
    CHECK(copy->title == "pred");
    env.probe.query("UPDATE relais_test_articles SET title = 'behind' WHERE id = "
                    + std::to_string(src));
    copy = driveBounded(env.io, CohArticleL1::find(src));
    REQUIRE(copy);
    CHECK(copy->title == "pred");  // served
    resetArticleCaches<CohArticleL1>(env);
}

TEST_CASE("outcome: an aborted insert with a database-assigned key invalidates no other page",
          "[coherence][settle][outcome][list][integration]") {
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    // Waits once its row is written: the write has a transaction id.
    ArticleTrigger trigger(env, "coh_oa_ins_sleep", "AFTER INSERT",
        "IF NEW.category = 'coh-oa-ins' THEN PERFORM pg_sleep(30); END IF;");
    driveBounded(env.io, CohArticleL1::insert(articleIn(0, "coh-oa-ins-warm")));
    auto other = seedArticle(env, "coh-oa-ins-other");
    REQUIRE(pageHolds<CohArticleL1>(env, "coh-oa-ins-other", other));

    queueOf(env).pause();
    auto t = uncertainTicket(env, CohArticleL1::insert(articleIn(0, "coh-oa-ins")));
    REQUIRE(t);

    settleAborted(env, t);
    auto hidden = seedArticle(env, "coh-oa-ins-other");
    CHECK_FALSE(pageHolds<CohArticleL1>(env, "coh-oa-ins-other", hidden));  // page kept
    CHECK(pageCount<CohArticleL1>(env, "coh-oa-ins") == 0);
    resetArticleCaches<CohArticleL1>(env);
}

// =============================================================================
// An L1 repo reads the catalog at its first use, in the background, on the
// pool's only connection (see detail::ColumnFacts). Read once per process
// (the facts are static), before any case: a case that withholds the
// connection's results never withholds that read instead of its own.
// =============================================================================

namespace {

template<typename... Repos>
void primeColumnFacts(CohEnv& env) {
    ([&] {
        (void)TI::columnFacts<Repos>();  // first use: the read starts
        if (!pumpUntil(env, [] { return TI::columnFacts<Repos>().known(); }, 5s))
            throw std::runtime_error(std::string("column facts unread: ") + Repos::name());
    }(), ...);
}

class PrimeColumnFacts : public Catch::EventListenerBase {
public:
    using Catch::EventListenerBase::EventListenerBase;
    void testRunStarting(const Catch::TestRunInfo&) override {
        CohEnv env(1s);
        primeColumnFacts<CohItemL1, CohAkeyL1, CohArticleL1, CohUserL1, CohPurchaseL1,
                         CohGuardL1, CohPurchaseListL1, CohPurchaseX>(env);
    }
};

}  // namespace

CATCH_REGISTER_LISTENER(PrimeColumnFacts)
