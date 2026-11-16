// Liveness / timeout suite for the I/O layer.
//
// Every test here drives a fault into the loop — a withheld read, a blackholed
// connect, a socket replaced mid-handshake, an injected RST, a poisoned pool
// slot — and asserts the worker stays live: a bounded timeout fires, the
// poisoned resource is reclaimed, no slot or waiter leaks, and nothing
// double-resumes or dangles. Run it under ASan (.build/asan) and TSan
// (.build/tsan); the bugs these guard against are exactly the kind those
// sanitizers catch.
//
// Fault injection lives entirely in tests/fixtures (ControllableIoContext,
// PgProbe, the fake servers of LivenessHarness) — nothing in include/ knows it
// is under test.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <jcailloux/relais/io/pg/PgConnection.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/IoPool.h>
#include <jcailloux/relais/io/WhenAll.h>

#include <fixtures/ControllableIoContext.h>
#include <fixtures/EpollIoContext.h>
#include <fixtures/LivenessHarness.h>
#include <fixtures/PgProbe.h>
#include <fixtures/TestConnInfo.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::detail;
using namespace jcailloux::relais::io::batch;
using namespace jcailloux::relais::io::batch::detail;
using namespace jcailloux::relais::io::test;
using namespace std::chrono_literals;

using Io = ControllableIoContext;

// =============================================================================
// Environment + helpers
// =============================================================================

static const char* getConnInfo() {
    return relais_test::kConnInfo;
}

// =============================================================================
// Connection-level timeout (drive a single PgConnection)
// =============================================================================

TEST_CASE("liveness: a withheld read trips query_timeout (PgQueryTimeout)",
          "[io][liveness][integration]") {
    Io io;
    auto conn = driveBounded(io, PgConnection<Io>::connect(io, getConnInfo(), 2s, 200ms));
    REQUIRE(conn->connected());

    // Hold the read side before the query: the send proceeds (write-ready is not
    // masked), the response is withheld, the query_timeout fires.
    io.holdReads(conn->socket());
    REQUIRE_THROWS_AS(
        driveBounded(io, conn->query("SELECT 1")),
        PgQueryTimeout);

    // The connection is poisoned, not silently reusable.
    REQUIRE_FALSE(conn->connected());
}

TEST_CASE("liveness: a late result after teardown is dropped, not re-injected",
          "[io][liveness][integration]") {
    Io io;
    int fd = -1;
    {
        auto conn = driveBounded(io, PgConnection<Io>::connect(io, getConnInfo(), 2s, 150ms));
        fd = conn->socket();
        io.holdReads(fd);
        REQUIRE_THROWS_AS(driveBounded(io, conn->query("SELECT 1")), PgQueryTimeout);
        // conn destroyed here: ~PgConnection removeCurrentWatch + PQfinish(fd).
    }
    // The buffered result is now "released" — but the watch is gone and the fd is
    // closed, so nothing can deliver it. No callback, no double-resume, no UAF.
    io.releaseReads(fd);
    pump(io);
    SUCCEED("released read after teardown produced no callback");
}

// =============================================================================
// Pool-level: poisoned slot reclamation, no leak
// =============================================================================

TEST_CASE("liveness: a poisoned connection is discarded and the slot reclaimed",
          "[io][liveness][integration]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 2, .acquire_timeout = 2s,
         .query_timeout = 150ms}));
    REQUIRE(pool->totalConnections() == 1);

    {
        auto guard = driveBounded(io, pool->acquire());
        int fd = guard.conn().socket();
        io.holdReads(fd);
        REQUIRE_THROWS_AS(driveBounded(io, guard.conn().query("SELECT 1")), PgQueryTimeout);
        io.releaseReads(fd);
        // guard released here: release() sees !connected() → --total_, not re-idled.
    }
    REQUIRE(pool->totalConnections() == 0);
    REQUIRE(pool->idleCount() == 0);
    REQUIRE(pool->waiterCount() == 0);

    // Pool is reusable: a fresh acquire mints a new connection.
    auto guard2 = driveBounded(io, pool->acquire());
    REQUIRE(guard2.conn().connected());
}

TEST_CASE("liveness: N consecutive timeouts leak no slot and no waiter",
          "[io][liveness][integration]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 4, .acquire_timeout = 2s,
         .query_timeout = 120ms}));

    for (int i = 0; i < 5; ++i) {
        auto guard = driveBounded(io, pool->acquire());
        int fd = guard.conn().socket();
        io.holdReads(fd);
        REQUIRE_THROWS_AS(driveBounded(io, guard.conn().query("SELECT 1")), PgQueryTimeout);
        io.releaseReads(fd);
    }
    REQUIRE(pool->totalConnections() <= 4);
    REQUIRE(pool->waiterCount() == 0);

    // Still healthy.
    auto guard = driveBounded(io, pool->acquire());
    auto r = driveBounded(io, guard.conn().query("SELECT 1"));
    REQUIRE(r.ok());
}

TEST_CASE("liveness: a poisoned connection is not handed back as idle",
          "[io][liveness][integration]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 1, .acquire_timeout = 2s,
         .query_timeout = 120ms}));
    {
        auto guard = driveBounded(io, pool->acquire());
        int fd = guard.conn().socket();
        io.holdReads(fd);
        REQUIRE_THROWS_AS(driveBounded(io, guard.conn().query("SELECT 1")), PgQueryTimeout);
        REQUIRE_FALSE(guard.conn().connected());  // dead_, even though PQstatus==OK
        io.releaseReads(fd);
    }
    // Discarded, not re-idled; the dtor's timer teardown fired no stray callback.
    REQUIRE(pool->idleCount() == 0);
    REQUIRE(pool->totalConnections() == 0);
    pump(io);
}

TEST_CASE("liveness: an idle connection the server closed is not lent",
          "[io][liveness][integration]") {
    Io io;
    const std::string conninfo = std::string(getConnInfo())
                               + " application_name=relais_liveness_idle_pool";
    auto pool = driveBounded(io, PgPool<Io>::create(io, conninfo,
        {.min_connections = 1, .max_connections = 1, .acquire_timeout = 2s,
         .query_timeout = 2s}));

    PgProbe probe(getConnInfo());
    REQUIRE(probe.terminateBackends("application_name = 'relais_liveness_idle_pool'") == 1);
    pump(io, 200ms);  // the idle connection reads the server's farewell

    auto guard = driveBounded(io, pool->acquire());
    CHECK(guard.conn().connected());
    CHECK(driveBounded(io, guard.conn().query("SELECT 1")).ok());
    CHECK(pool->totalConnections() == 1);
}

// =============================================================================
// Pool-level: acquire timeout (queue wait) and connect-hang
// =============================================================================

TEST_CASE("liveness: acquire on a saturated pool trips PgPoolTimeout, no leaked waiter",
          "[io][liveness][integration]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 1, .acquire_timeout = 300ms}));

    auto held = driveBounded(io, pool->acquire());  // the only slot, kept
    REQUIRE(pool->waiterCount() == 0);

    // Second acquire queues then times out on the queue wait.
    REQUIRE_THROWS_AS(driveBounded(io, pool->acquire(), 2s), PgPoolTimeout);
    REQUIRE(pool->waiterCount() == 0);  // waiter dequeued, not stranded
}

TEST_CASE("liveness: a connection that dies under a queued acquire is replaced for it",
          "[io][liveness][integration]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 1, .acquire_timeout = 3s,
         .query_timeout = 150ms}));

    // The only connection times out under a query while another acquire waits
    // for it: the slot it frees must serve that acquire, not its acquire_timeout.
    bool first_timed_out = false;
    bool second_ok = false;
    auto first = [&]() -> Task<void> {
        auto guard = co_await pool->acquire();
        const int fd = guard.conn().socket();
        io.holdReads(fd);
        try {
            co_await guard.conn().query("SELECT 1");
        } catch (const PgQueryTimeout&) {
            first_timed_out = true;
        }
        io.releaseReads(fd);  // the replacement may reuse the fd number
    };
    auto second = [&]() -> Task<void> {
        auto guard = co_await pool->acquire();
        second_ok = (co_await guard.conn().query("SELECT 1")).ok();
    };
    std::vector<Task<void>> both;
    both.push_back(first());
    both.push_back(second());

    const auto t0 = std::chrono::steady_clock::now();
    driveBounded(io, [](std::vector<Task<void>> tasks) -> Task<void> {
        co_await whenAll(std::move(tasks));
    }(std::move(both)));
    const auto elapsed = std::chrono::steady_clock::now() - t0;

    REQUIRE(first_timed_out);
    CHECK(second_ok);
    CHECK(elapsed < 1500ms);  // query_timeout plus a connect, not acquire_timeout
    CHECK(pool->totalConnections() == 1);
    CHECK(pool->idleCount() == 1);
    CHECK(pool->waiterCount() == 0);
}

TEST_CASE("liveness: a blackholed connect (path b) trips PgPoolTimeout and reclaims the slot",
          "[io][liveness][integration]") {
    SilentTcpServer silent;
    Io io;
    // min_connections=0 → create does no boot connect; the acquire below is the
    // one that connects, into the silent server, and must be bounded.
    auto pool = driveBounded(io, PgPool<Io>::create(io, silent.conninfo(),
        {.min_connections = 0, .max_connections = 2, .acquire_timeout = 300ms}));

    auto t0 = std::chrono::steady_clock::now();
    REQUIRE_THROWS_AS(driveBounded(io, pool->acquire(), 3s), PgPoolTimeout);
    auto elapsed = std::chrono::steady_clock::now() - t0;

    // Bounded near acquire_timeout: the single timer armed at the first watch
    // registration survived the connect's per-step re-watch (the whole
    // multi-step handshake stayed bounded).
    REQUIRE(elapsed < 2s);
    // Path (b): the timeout is on the connect, never on the queue — no waiter was
    // ever enqueued — and the ++total_ slot is unwound by its scope guard.
    REQUIRE(pool->waiterCount() == 0);
    REQUIRE(pool->totalConnections() == 0);
}

TEST_CASE("liveness: repeated connect failures never freeze the pool",
          "[io][liveness][integration]") {
    SilentTcpServer silent;
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, silent.conninfo(),
        {.min_connections = 0, .max_connections = 2, .acquire_timeout = 250ms}));

    for (int i = 0; i < 4; ++i)
        REQUIRE_THROWS_AS(driveBounded(io, pool->acquire(), 2s), PgPoolTimeout);

    // total_ returns to 0 each time (scope guard), so `total_ < max` stays true
    // and the pool is never wedged at max with dead slots.
    REQUIRE(pool->totalConnections() == 0);
    REQUIRE(pool->waiterCount() == 0);
}

TEST_CASE("liveness: a connect follows the socket libpq replaces mid-handshake",
          "[io][liveness][integration]") {
    TlsDropServer srv;
    Io io;
    const std::string conninfo = srv.conninfo();

    // Whatever the entry point, the connect ends with the server's error, long
    // before acquire_timeout (3 s).
    auto expectFollowed = [&](auto connect) {
        const auto t0 = std::chrono::steady_clock::now();
        std::string error;
        try {
            connect();
            FAIL("connected to a server that only answers with an error");
        } catch (const PgPoolTimeout&) {
            FAIL("the connect waited for acquire_timeout on the socket libpq replaced");
        } catch (const PgConnectionError& e) {
            error = e.what();
        }
        const auto elapsed = std::chrono::steady_clock::now() - t0;

        if (!srv.saw_ssl_request)
            SKIP("libpq built without TLS: it never replaces its socket here");
        CHECK(srv.saw_plain_startup);
        CHECK_THAT(error, Catch::Matchers::ContainsSubstring(TlsDropServer::kError));
        CHECK(elapsed < 1s);
    };

    SECTION("PgConnection::connect") {
        expectFollowed([&] {
            driveBounded(io, PgConnection<Io>::connect(io, conninfo.c_str(), 3s));
        });
    }
    SECTION("PgPool::create, boot connection") {
        expectFollowed([&] {
            driveBounded(io, PgPool<Io>::create(io, conninfo,
                {.min_connections = 1, .max_connections = 1, .acquire_timeout = 3s}));
        });
    }
}

TEST_CASE("liveness: a connect changes its watches outside any event dispatch",
          "[io][liveness][integration]") {
    Io io;
    const int fds_before = countOpenFds();

    SECTION("a handshake that succeeds") {
        auto conn = driveBounded(io, PgConnection<Io>::connect(io, getConnInfo(), 2s));
        REQUIRE(conn->connected());
    }
    SECTION("a handshake libpq restarts on a new socket") {
        TlsDropServer srv;
        REQUIRE_THROWS_AS(
            driveBounded(io, PgConnection<Io>::connect(io, srv.conninfo().c_str(), 3s)),
            PgConnectionError);
    }
    SECTION("a handshake cut by acquire_timeout") {
        SilentTcpServer silent;
        REQUIRE_THROWS_AS(
            driveBounded(io, PgConnection<Io>::connect(io, silent.conninfo().c_str(), 200ms)),
            PgPoolTimeout);
    }

    pump(io, 50ms);  // runs the posted removal of the last watch
    CHECK(io.watchChangesInDispatch() == 0);
    CHECK(countOpenFds() <= fds_before);  // libpq's socket and every duplicate closed
}

TEST_CASE("liveness: requests keep the connection's one watch",
          "[io][liveness][integration]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 1, .acquire_timeout = 2s,
         .query_timeout = 2s}));
    const auto added = io.addedWatches();

    for (int i = 0; i < 3; ++i) {
        auto guard = driveBounded(io, pool->acquire());
        REQUIRE(driveBounded(io, guard.conn().query("SELECT 1")).ok());
        REQUIRE(driveBounded(io, guard.conn().queryParams(
            "SELECT $1::int", PgParams::make(int32_t{i}))).ok());
    }
    {
        auto guard = driveBounded(io, pool->acquire());
        auto& conn = guard.conn();
        std::vector<PgPool<Io>::ConnectionType::PipelineResult> results;
        driveBounded(io, [&]() -> Task<void> {
            conn.enterPipelineMode();
            for (int i = 0; i < 4; ++i) {
                if (conn.ensurePreparedPipelined("SELECT $1::int + 1", 1)) conn.pipelineSync();
                conn.sendPreparedPipelined("SELECT $1::int + 1", PgParams::make(int32_t{i}));
                conn.pipelineSync();
            }
            co_await conn.flushPipeline();
            co_await conn.readPipelineResults(4, results);
            conn.exitPipelineMode();
        }());
        REQUIRE(results.size() == 4);
    }

    CHECK(io.addedWatches() == added);
    CHECK(io.watchChangesInDispatch() == 0);
}

TEST_CASE("liveness: a pipeline flush reads what the server answers meanwhile",
          "[io][liveness][integration]") {
    Io io;
    auto conn = driveBounded(io, PgConnection<Io>::connect(io, getConnInfo(), 2s, 5s));

    // The first statement answers 32 MiB, more than both socket buffers hold;
    // the server reads the 16 MiB of statements behind it only once that answer
    // is out. Until the client reads it, the server blocks writing, stops
    // reading, and the flush can never finish.
    constexpr int kTail = 256;
    const std::string payload(64 * 1024, 'x');
    std::vector<PgConnection<Io>::PipelineResult> results;
    driveBounded(io, [&]() -> Task<void> {
        auto send = [&](const char* sql, const PgParams& params) {
            if (conn->ensurePreparedPipelined(sql, params.count())) conn->pipelineSync();
            conn->sendPreparedPipelined(sql, params);
            conn->pipelineSync();
        };
        conn->enterPipelineMode();
        send("SELECT repeat('x', $1::int)", PgParams::make(int32_t{32 << 20}));
        for (int i = 0; i < kTail; ++i)
            send("SELECT length($1::text)", PgParams::make(payload));
        co_await conn->flushPipeline();
        co_await conn->readPipelineResults(kTail + 1, results);
        conn->exitPipelineMode();
    }(), 30s);

    REQUIRE(results.size() == kTail + 1);
    for (const auto& r : results) REQUIRE(r.result.ok());
}

TEST_CASE("liveness: an idle connection the server closes is let go at once",
          "[io][liveness][integration]") {
    Io io;
    const std::string conninfo = std::string(getConnInfo())
                               + " application_name=relais_liveness_idle";
    auto conn = driveBounded(io, PgConnection<Io>::connect(io, conninfo.c_str(), 2s, 2s));
    REQUIRE(conn->connected());
    PgProbe probe(getConnInfo());
    const int fds_connected = countOpenFds();

    REQUIRE(probe.terminateBackends("application_name = 'relais_liveness_idle'") == 1);

    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (conn->connected() && std::chrono::steady_clock::now() < deadline)
        io.runOnce(10);
    REQUIRE_FALSE(conn->connected());
    pump(io, 50ms);

    // Neither libpq's socket nor its duplicate lingers half-closed.
    CHECK(conn->socket() == -1);
    CHECK(countOpenFds() == fds_connected - 2);
    CHECK(io.watchChangesInDispatch() == 0);
}

// =============================================================================
// Drain error with the watch-timer armed (injected RST)
// =============================================================================

TEST_CASE("liveness: a mid-query RST resolves in error without a stray timer",
          "[io][liveness][integration]") {
    Io io;
    // Tag the session so the probe can find and kill exactly this backend.
    std::string conninfo = std::string(getConnInfo()) +
                           " application_name=relais_liveness_rst";
    auto conn = driveBounded(io, PgConnection<Io>::connect(io, conninfo.c_str(), 2s, 5s));
    REQUIRE(conn->connected());

    PgProbe probe(getConnInfo());

    // Run a slow query; while it is in flight (watch + query_timer armed), the
    // probe terminates the backend → PQconsumeInput sees the RST → the awaiter
    // resolves in error, routing through removeCurrentWatch (cancel timer +
    // removeWatch). The timer must not fire later on the released connection.
    bool killed = false;
    bool threw = false;
    auto runner = [&]() -> Task<void> {
        // Defer the kill to the loop so it lands while the read is suspended.
        io.post([&] {
            probe.terminateBackends("application_name = 'relais_liveness_rst'");
            killed = true;
        });
        try {
            co_await conn->query("SELECT pg_sleep(2)");
        } catch (const PgError&) {
            // A terminated backend sends a FATAL result, so this is a deterministic
            // "query failed" — distinct from a raw socket drop, which surfaces as
            // the uncertain PgConnectionLost (covered by the static_assert + the
            // scenario-A tests). The point here is liveness, not the exact type.
            threw = true;
        }
    };
    REQUIRE_NOTHROW(driveBounded(io, runner(), 4s));
    REQUIRE(killed);
    REQUIRE(threw);
    pump(io);  // a stray timer would fire (and UAF under ASan) here
    SUCCEED("error-path resolution cancelled the watch-bound timer");
}

// =============================================================================
// Pipeline / batched paths
// =============================================================================

TEST_CASE("liveness: a fast multi-result pipeline re-arms with no spurious timeout",
          "[io][liveness][integration]") {
    Io io;
    // Short query_timeout, but each round-trip is fast → every wait disarms its
    // deadline in time; zero spurious PgQueryTimeout and no timer accumulation.
    auto conn = driveBounded(io, PgConnection<Io>::connect(io, getConnInfo(), 2s, 500ms));

    for (int i = 0; i < 5; ++i) {
        auto r = driveBounded(io, conn->query("SELECT generate_series(1, 50)"));
        REQUIRE(r.ok());
        REQUIRE(r.rows() == 50);
    }
    REQUIRE(conn->connected());
    // At most the timer of each of the connection's two queues.
    REQUIRE(io.pendingTimerCount() <= 2);
    REQUIRE(io.cancelledTimers() == 0);
}

TEST_CASE("liveness: a pool's requests arm no loop timer",
          "[io][liveness][integration]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 1, .acquire_timeout = 5s,
         .query_timeout = 30s}));
    const auto armed_at_start = io.armedTimers();  // the handshake's acquire_timeout

    // Four requests at a time on one connection: three queue on acquire.
    constexpr int kRequests = 40;
    int done = 0;
    auto request = [&]() -> DetachedTask {
        auto guard = co_await pool->acquire();
        if ((co_await guard.conn().query("SELECT 1")).ok()) ++done;
    };
    for (int round = 0; round < kRequests / 4; ++round) {
        const int target = done + 4;
        for (int i = 0; i < 4; ++i) request();
        const auto end = std::chrono::steady_clock::now() + 5s;
        while (done < target && std::chrono::steady_clock::now() < end) io.runOnce(10);
    }

    REQUIRE(done == kRequests);
    // The query_timeout queue's first timer, nothing per request or per waiter.
    CHECK(io.armedTimers() - armed_at_start <= 1);
    CHECK(io.cancelledTimers() == 0);
    CHECK(pool->waiterCount() == 0);
}

TEST_CASE("liveness: query_timeout counts from each wait, not from the pool's timer",
          "[io][liveness][integration]") {
    Io io;
    auto conn = driveBounded(io, PgConnection<Io>::connect(io, getConnInfo(), 2s, 300ms));
    // Arms the query_timeout queue's timer, 300 ms from now.
    REQUIRE(driveBounded(io, conn->query("SELECT 1")).ok());
    pump(io, 200ms);

    const auto t0 = std::chrono::steady_clock::now();
    REQUIRE_THROWS_AS(driveBounded(io, conn->query("SELECT pg_sleep(2)")), PgQueryTimeout);
    const auto waited = std::chrono::steady_clock::now() - t0;
    CHECK(waited >= 300ms);  // not when the timer armed by the first query fired
    CHECK(waited < 1500ms);
}

TEST_CASE("liveness: a batched fire that cannot acquire a connection fails its waiters",
          "[io][liveness][integration]") {
    SilentTcpServer silent;
    Io io;
    // Pool that can never connect (silent server), short acquire bound.
    auto pool = driveBounded(io, PgPool<Io>::create(io, silent.conninfo(),
        {.min_connections = 0, .max_connections = 2, .acquire_timeout = 300ms}));
    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

    // The submit batches and fires; the detached fire's acquire blackholes and
    // times out, and the fire's catch must propagate PgPoolTimeout to the waiter
    // (not strand it forever).
    REQUIRE_THROWS_AS(
        driveBounded(io, batcher->submitQueryRead("SELECT 1", PgParams{}), 3s),
        PgPoolTimeout);
}

TEST_CASE("liveness: a withheld pipeline read times out via the pipeline awaiter",
          "[io][liveness][integration]") {
    Io io;
    // One connection, so the two concurrent reads pipeline on it (not single-send).
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 1, .acquire_timeout = 2s,
         .query_timeout = 300ms}));

    // Learn the lone connection's fd, hand it back, then withhold its reads — the
    // batch fire re-acquires that same idle connection.
    int fd = -1;
    { auto g = driveBounded(io, pool->acquire()); fd = g.conn().socket(); }
    io.holdReads(fd);

    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

    // Two distinct (non-coalescing) reads → one pipeline, two segments. The
    // withheld response trips query_timeout inside readPipelineResults, so the
    // failure surfaces through the pipeline Read/WriteAwaiter and is stamped on
    // every batched waiter — distinct from the single-send ResultAwaiter path.
    auto runner = [&]() -> Task<void> {
        co_await whenAll(
            batcher->submitQueryRead("SELECT 1", PgParams{}),
            batcher->submitQueryRead("SELECT 2", PgParams{}));
    };
    REQUIRE_THROWS_AS(driveBounded(io, runner(), 3s), PgQueryTimeout);
}

namespace relais_bench {
// Befriended by BatchScheduler: lets a case set how long a read batch waits.
struct BatchBenchAccessor {
    static void setNetworkTime(BatchScheduler<Io>& b, std::chrono::nanoseconds t) {
        b.estimator_.pg_network_time_ns = static_cast<double>(t.count());
    }
};
}  // namespace relais_bench

TEST_CASE("liveness: a read batch that leaves early lets its departure timer fire for nothing",
          "[io][liveness][integration]") {
    using Clock = std::chrono::steady_clock;
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 2, .max_connections = 2}));
    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
    // Past bootstrap, a read sent while another is in flight joins a batch.
    for (int i = 0; i < 6; ++i)
        REQUIRE(driveBounded(io, batcher->submitQueryRead("SELECT 1", PgParams{})).ok());
    // A batch leaves 400 ms after it forms, or with its leader's answer.
    relais_bench::BatchBenchAccessor::setNetworkTime(*batcher, 400ms);
    const auto cancelled_at_start = io.cancelledTimers();

    Clock::time_point follower_done{};
    auto follower = [&](const char* sql) -> Task<PgResult> {
        auto r = co_await batcher->submitQueryRead(sql, PgParams{});
        follower_done = Clock::now();
        co_return r;
    };
    auto pair = [&](const char* leader_sql, const char* follower_sql) -> Task<void> {
        auto [lead, follow] = co_await whenAll(
            batcher->submitQueryRead(leader_sql, PgParams{}), follower(follower_sql));
        REQUIRE(lead.ok());
        REQUIRE(follow.ok());
    };

    // A fast leader: its batch leaves with its answer, its timer stays pending.
    const auto t0 = Clock::now();
    driveBounded(io, pair("SELECT 1", "SELECT 2"), 3s);
    REQUIRE(follower_done - t0 < 200ms);

    // A slow leader: the next batch waits for its own departure; the first
    // batch's timer, which fires meanwhile, does not send it.
    pump(io, std::chrono::duration_cast<std::chrono::milliseconds>(t0 + 200ms - Clock::now()));
    const auto t1 = Clock::now();
    driveBounded(io, pair("SELECT 1 FROM pg_sleep(0.8)", "SELECT 3"), 3s);
    CHECK(follower_done - t1 >= 350ms);

    // A batch leaves early again, then the scheduler goes: its timer fires on
    // nothing.
    driveBounded(io, pair("SELECT 1", "SELECT 2"), 3s);
    std::weak_ptr<BatchScheduler<Io>> weak = batcher;
    batcher.reset();
    REQUIRE(weak.expired());
    weak.reset();  // or the scheduler's memory outlives it, out of ASan's sight
    pump(io, 500ms);

    CHECK(io.cancelledTimers() == cancelled_at_start);
}

// =============================================================================
// Teardown safety
// =============================================================================

TEST_CASE("liveness: destroying the scheduler inside a detached fire does not UAF",
          "[io][liveness][integration][teardown]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 2}));

    std::weak_ptr<BatchScheduler<Io>> weak;
    {
        auto b = std::make_shared<BatchScheduler<Io>>(io, pool, 8);
        weak = b;

        bool done = false;
        std::exception_ptr ex;
        // The continuation owns a scheduler ref (by-value param) and drops it
        // AFTER the await — i.e. inside the fire's resume loop. Once the test's
        // own ref is gone, that drop is the last ref: pre-fix it destroys the
        // scheduler mid-drain (UAF on the drain-or-chain); the fire's self-anchor
        // defers destruction to frame end.
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
        auto runner = [&](std::shared_ptr<BatchScheduler<Io>> bb) -> Starter {
            try {
                co_await bb->submitPgWrite(
                    "UPDATE relais_test_items SET value = value WHERE id = $1",
                    PgParams::make(int64_t{0}));
            } catch (...) { ex = std::current_exception(); }
            auto last = std::move(bb);
            done = true;
            last.reset();  // drop the (now last) ref during the fire's resume
        };
        auto s = runner(b);
        b.reset();  // only the suspended runner frame holds the scheduler now

        auto deadline = std::chrono::steady_clock::now() + 5s;
        while (!done && std::chrono::steady_clock::now() < deadline)
            io.runOnce(20);
        if (s.handle) s.handle.destroy();
        REQUIRE(done);
        if (ex) std::rethrow_exception(ex);
    }
    REQUIRE(weak.expired());  // cleanly destroyed exactly once
    pump(io);
}

// A self-driving starter that runs a Task<void> without the test thread blocking
// on its result — so an in-flight detached fire can be observed mid-teardown.
namespace {
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
}  // namespace

TEST_CASE("liveness: ordered teardown drains an in-flight detached fire, no leak",
          "[io][liveness][integration][teardown]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 2, .acquire_timeout = 2s,
         .query_timeout = 400ms}));

    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, 8);

    // Warm the estimator past bootstrap so submitPgRead takes the batching path
    // (a bootstrapping batcher sends every read direct, never firing a detached
    // batch). Six successful reads clear the threshold of five.
    for (int i = 0; i < 6; ++i) {
        auto r = driveBounded(io, batcher->submitQueryRead("SELECT 1", PgParams{}));
        REQUIRE(r.ok());
    }

    // Two concurrent reads: the leader goes direct on one connection; the follower
    // accumulates and, when its departure timer fires, runs as a DETACHED fire on
    // the other. Both statements outlast query_timeout on the server, so each read
    // hangs until its bound: withholding read-readiness would not do, since a
    // pipelined batch reads its socket before waiting on it and, on a slow client
    // (sanitizers), finds the answer already there. Drive it without awaiting the
    // result so the in-flight fire is observable mid-teardown.
    bool done = false;
    std::exception_ptr ex;
    auto runner = [&]() -> Task<void> {
        try {
            co_await whenAll(
                batcher->submitQueryRead("SELECT 1 FROM pg_sleep(2)", PgParams{}),
                batcher->submitQueryRead("SELECT 2 FROM pg_sleep(2)", PgParams{}));
        } catch (...) { ex = std::current_exception(); }
        done = true;
    };
    auto starter = [&](Task<void> t) -> Starter { co_await std::move(t); };
    auto s = starter(runner());

    // The departure timer fires the follower as a detached fire; it suspends on the
    // pending answer, well before the 400ms query bound.
    pump(io, 100ms);
    REQUIRE(batcher->inflightDetached() >= 1);
    REQUIRE_FALSE(batcher->quiescentForTeardown());

    // Ordered teardown: pump until the in-flight fire has drained (its read
    // times out, it unwinds in error, the DetachedTask self-destructs). Bounded —
    // the loop never hangs on it.
    auto deadline = std::chrono::steady_clock::now() + 3s;
    while (!batcher->quiescentForTeardown() &&
           std::chrono::steady_clock::now() < deadline)
        io.runOnce(20);

    REQUIRE(batcher->quiescentForTeardown());
    REQUIRE(batcher->inflightDetached() == 0);
    REQUIRE(done);  // both waiters saw PgQueryTimeout; the driver frame completed

    if (s.handle) s.handle.destroy();
    pump(io);  // a leaked fire frame / stray timer would surface here under ASan
}

TEST_CASE("liveness: a request destroyed mid-wait poisons its connection",
          "[io][liveness][integration][teardown]") {
    Io io;
    auto pool = driveBounded(io, PgPool<Io>::create(io, getConnInfo(),
        {.min_connections = 1, .max_connections = 1, .acquire_timeout = 2s,
         .query_timeout = 2s}));

    // Suspended on its result, withheld; then its frame is destroyed.
    int fd = -1;
    auto op = [&]() -> Task<void> {
        auto guard = co_await pool->acquire();
        fd = guard.conn().socket();
        io.holdReads(fd);
        co_await guard.conn().query("SELECT 1");
    };
    auto starter = [](Task<void> t) -> Starter { co_await std::move(t); };
    auto s = starter(op());
    pump(io, 100ms);
    REQUIRE(fd >= 0);
    s.handle.destroy();

    // The result still on its way answers nobody: the connection is dropped.
    CHECK(pool->totalConnections() == 0);
    CHECK(pool->idleCount() == 0);
    io.releaseReads(fd);
    pump(io);

    auto guard = driveBounded(io, pool->acquire());
    CHECK(driveBounded(io, guard.conn().query("SELECT 1")).ok());
}

TEST_CASE("liveness: fail-fast startup reclaims an in-flight connect, no fd leak",
          "[io][liveness][integration][teardown]") {
    SilentTcpServer silent;

    int fds_before = countOpenFds();
    // PG points at the silent server with acquire_timeout=0 (unbounded connect),
    // so the boot connect suspends; startup_timeout is the only backstop and
    // fires fast, destroying the io/pool while that connect is still suspended.
    IoPoolConfig cfg;
    cfg.num_workers = 1;
    cfg.pg_conninfo = silent.conninfo();
    cfg.pin_to_cores = false;
    cfg.acquire_timeout = 0ms;     // unbounded boot connect
    cfg.query_timeout = 0ms;
    cfg.startup_timeout = 400ms;

    REQUIRE_THROWS_AS(IoPool::create(cfg), IoPoolStartupError);

    // The suspended connect frame and its socket fd are reclaimed at teardown
    // (BootTask ownership), so a failed startup does not leak an fd.
    int fds_after = countOpenFds();
    REQUIRE(fds_after <= fds_before);
}

TEST_CASE("liveness: an unreachable dependency fails startup within a bound, never hangs",
          "[io][liveness][integration]") {
    SilentTcpServer silent;
    IoPoolConfig cfg;
    cfg.num_workers = 1;
    cfg.pg_conninfo = silent.conninfo();
    cfg.pin_to_cores = false;
    cfg.acquire_timeout = 300ms;   // bound the boot connect itself
    cfg.startup_timeout = 5s;

    auto t0 = std::chrono::steady_clock::now();
    REQUIRE_THROWS(IoPool::create(cfg));
    auto elapsed = std::chrono::steady_clock::now() - t0;
    REQUIRE(elapsed < 4s);  // bounded boot, no deadlock on ready_cv
}

// =============================================================================
// IoPool boot and teardown: clean start, no leaked fd
// =============================================================================

TEST_CASE("liveness: an L1-only IoPool starts clean",
          "[io][iopool][integration]") {
    IoPoolConfig cfg;
    cfg.num_workers = 1;
    cfg.pg_conninfo = getConnInfo();
    cfg.pin_to_cores = false;
    cfg.pg_min_conns_per_worker = 1;
    cfg.pg_max_conns_per_worker = 1;

    auto pool = IoPool::create(cfg);
    REQUIRE(pool->numWorkers() == 1);
    pool->stop();
}

TEST_CASE("liveness: an L1-only IoPool tears down with no fd leak",
          "[io][iopool][integration][teardown]") {
    // Teardown must dismantle every worker cleanly. A before/after fd delta
    // would surface any leaked socket or pipe.
    int fds_before = countOpenFds();
    {
        IoPoolConfig cfg;
        cfg.num_workers = 2;
        cfg.pg_conninfo = getConnInfo();
        cfg.pin_to_cores = false;
        cfg.pg_min_conns_per_worker = 1;
        cfg.pg_max_conns_per_worker = 1;

        auto pool = IoPool::create(cfg);
        REQUIRE(pool->numWorkers() == 2);
        pool->stop();
    }
    int fds_after = countOpenFds();
    REQUIRE(fds_after <= fds_before);
}

TEST_CASE("liveness: IoPool teardown waits for a request in flight",
          "[io][iopool][integration][teardown]") {
    // A request still running at stop() holds a connection of the pool: the
    // teardown waits for its result, so that neither its frame nor the pool and
    // its connections are left behind.
    int fds_before = countOpenFds();
    std::atomic<bool> started{false};
    bool finished = false;  // written on the worker, read after the join
    {
        IoPoolConfig cfg;
        cfg.num_workers = 1;
        cfg.pg_conninfo = getConnInfo();
        cfg.pin_to_cores = false;
        cfg.pg_min_conns_per_worker = 1;
        cfg.pg_max_conns_per_worker = 1;
        auto pool = IoPool::create(cfg);

        pool->workerIo(0).post([&] {
            [](std::atomic<bool>& started, bool& finished) -> DetachedTask {
                started = true;
                (void)co_await jcailloux::relais::PgProvider::query("SELECT pg_sleep(0.3)");
                finished = true;
            }(started, finished);
        });
        while (!started) std::this_thread::yield();
        pool->stop();
    }
    CHECK(finished);
    CHECK(countOpenFds() <= fds_before);
}

TEST_CASE("liveness: IoPool teardown waits for relais's detached work",
          "[io][iopool][integration][teardown]") {
    // Work relais detaches on a loop and that holds no connection, such as
    // draining the rows an uncertain write may have changed: the teardown runs
    // it to its end too.
    struct Sleep {
        IoPool::Io& io;
        std::chrono::milliseconds delay;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            io.postDelayed(delay, [h] { h.resume(); });
        }
        void await_resume() const noexcept {}
    };
    std::atomic<bool> started{false};
    bool finished = false;  // written on the worker, read after the join
    {
        IoPoolConfig cfg;
        cfg.num_workers = 1;
        cfg.pg_conninfo = getConnInfo();
        cfg.pin_to_cores = false;
        cfg.pg_min_conns_per_worker = 1;
        cfg.pg_max_conns_per_worker = 1;
        auto pool = IoPool::create(cfg);

        auto& io = pool->workerIo(0);
        io.post([&] {
            [](IoPool::Io& io, std::atomic<bool>& started, bool& finished) -> TrackedTask {
                started = true;
                co_await Sleep{io, 300ms};
                finished = true;
            }(io, started, finished);
        });
        while (!started) std::this_thread::yield();
        pool->stop();
    }
    CHECK(finished);
}
