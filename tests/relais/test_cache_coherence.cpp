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

#include <jcailloux/relais/repository/Repo.h>
#include <jcailloux/relais/config/CacheConfig.h>
#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/pg/SettleQueue.h>
#include <jcailloux/relais/Log.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/batch/TimingEstimator.h>
#include <jcailloux/relais/io/WhenAll.h>

#include <fixtures/ControllableIoContext.h>
#include <fixtures/PgProbe.h>
#include <fixtures/RelaisTestAccessors.h>
#include "fixtures/generated/TestItemEntity.h"
#include "fixtures/generated/TestAssignedKeyEntity.h"
#include "fixtures/generated/TestArticleEntity.h"
#include "fixtures/generated/TestFilterOnlyEntity.h"

#include <array>
#include <chrono>
#include <coroutine>
#include <exception>
#include <memory>
#include <optional>
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

    // `conninfo_extra` is appended to the pool's conninfo (the settle
    // connection uses it too).
    explicit CohEnv(std::chrono::milliseconds query_timeout,
                    io::UncertainSettle settle = io::UncertainSettle::Terminate,
                    std::chrono::milliseconds settle_delay = 0ms,
                    const std::string& conninfo_extra = "")
        : probe(probeConnInfo()), q_timeout_(query_timeout)
    {
        // min=max=1: a single connection serves every op, so holdReads(pg_fd)
        // deterministically withholds the result of whatever the repo runs.
        pool = driveBounded(io, io::PgPool<Io>::create(io, poolConnInfo() + conninfo_extra,
            {.min_connections = 1, .max_connections = 1,
             .acquire_timeout = 2s, .query_timeout = query_timeout,
             .uncertain_settle = settle, .settle_delay = settle_delay}));
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
// the pages are then keyed on the copy L1 still holds.
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
void uncertainEraseKeysOnL1Copy(CohEnv& env) {
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

TEST_CASE("erase with lists: an uncertain erase keys the pages on the L1 copy",
          "[coherence][list][erase][scenarioA][integration]") {
    CohEnv env(300ms);
    uncertainEraseKeysOnL1Copy<CohArticleL1>(env);
}

TEST_CASE("erase with lists: an uncertain erase with no cached copy only throws",
          "[coherence][list][erase][scenarioA][integration]") {
    // The row is never read, so L1 holds no copy of it. Nothing tells which
    // pages held the row: they are left to their TTL. The caller still gets the
    // uncertain error, and nothing else.
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    driveBounded(env.io, CohArticleL1::erase(seedArticle(env, "coh-unc-nocopy")));
    auto id = seedArticle(env, "coh-unc-nocopy");

    env.holdPg();
    CHECK_THROWS_AS(driveBounded(env.io, CohArticleL1::erase(id)), io::PgQueryTimeout);
    env.releasePg();
    CHECK_FALSE(articleExists(env, id));
}

// =============================================================================
// Update of an entity with lists — the UPDATE locks the row in a CTE and
// returns it before and after: the pages of both positions are keyed on the
// versions PostgreSQL replaced and committed, with no read before the write. An
// uncertain update returns nothing: the old position is then keyed on the copy
// L1 still holds, the new one on the input. A patch already returned both
// versions; an uncertain one now invalidates the old position from that copy.
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
void uncertainUpdateKeysOnL1Copy(CohEnv& env) {
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
void uncertainPatchKeysOnL1Copy(CohEnv& env) {
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

TEST_CASE("update with lists: an uncertain update keys the old position on the L1 copy",
          "[coherence][list][update][scenarioA][integration]") {
    CohEnv env(300ms);
    uncertainUpdateKeysOnL1Copy<CohArticleL1>(env);
}

TEST_CASE("update with lists: an uncertain update with no cached copy keys the new position",
          "[coherence][list][update][scenarioA][integration]") {
    // The row is never read, so L1 holds no copy of it. The old position is
    // unknown and left to its TTL; the new one is the input. The caller still
    // gets the uncertain error, and nothing else.
    CohEnv env(300ms);
    resetArticleCaches<CohArticleL1>(env);
    auto warm = seedArticle(env, "coh-upd-nocopy-warm");
    driveBounded(env.io, CohArticleL1::update(warm, articleIn(warm, "coh-upd-nocopy-warm")));
    auto id = seedArticle(env, "coh-upd-nocopy-a");
    REQUIRE_FALSE(pageHolds<CohArticleL1>(env, "coh-upd-nocopy-b", id));

    env.holdPg();
    CHECK_THROWS_AS(driveBounded(env.io, CohArticleL1::update(
                        id, articleIn(id, "coh-upd-nocopy-b"))),
                    io::PgQueryTimeout);
    env.releasePg();
    CHECK(articleCategory(env, id) == "coh-upd-nocopy-b");
    CHECK(pageHolds<CohArticleL1>(env, "coh-upd-nocopy-b", id));
}

TEST_CASE("patch with lists: an uncertain patch invalidates the old position from the L1 copy",
          "[coherence][list][patch][scenarioA][integration]") {
    CohEnv env(300ms);
    uncertainPatchKeysOnL1Copy<CohArticleL1>(env);
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

bool settledAll(CohEnv& env) { return queueOf(env).pending() == 0; }

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
    t->replays.push_back([&ran]() -> io::Task<void> { ran.push_back(1); co_return; });
    t->replays.push_back([&ran]() -> io::Task<void> {
        ran.push_back(2);
        throw std::runtime_error("replay failure");
        co_return;
    });
    t->replays.push_back([&ran]() -> io::Task<void> { ran.push_back(3); co_return; });

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
                   " options='-c statement_timeout=700'");
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
        t->replays.push_back([]() -> io::Task<void> { co_return; });

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
                   " options='-c statement_timeout=700'");
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
        (void)blockedUpdate(env, id, 2);
    }
    CHECK(LogCapture::count(log::Level::Warn, "abandoned unsettled") == 1);
}
