// relais on adversarial event loops.
//
// Each case drives PostgreSQL through relais — a connect, a socket libpq
// replaces mid-handshake, requests, a pool, pipelines, batches, timeouts, a
// connection the server closes, attach() and its startup check — on an
// AdversarialIoContext, once per
// adversity: none, events split into one callback per bit, calls for nothing,
// coarse timers, then all of them. Every variant frees a watch synchronously and has
// no cancelTimer. relais must give the same answers as on EpollIoContext, never
// breach its side of the contract, and leave no watch behind. Under ASan, a
// watch freed inside its own dispatch and then called again (the next bit of a
// split event) is also a use-after-free.
//
// Built with RELAIS_TEST_TRANTOR_LOOP, the same cases run on trantor instead,
// through the shipped TrantorIoContext (test_trantor_loops).

#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <jcailloux/relais/io/pg/PgConnection.h>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/WhenAll.h>
#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/runtime/Attach.h>

#include <fixtures/AdversarialIoContext.h>
#include <fixtures/LivenessHarness.h>
#include <fixtures/PgProbe.h>
#include <fixtures/TestConnInfo.h>
#ifdef RELAIS_TEST_TRANTOR_LOOP
#include <fixtures/TrantorTestLoop.h>
#endif

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::detail;
using namespace jcailloux::relais::io::batch::detail;
using namespace jcailloux::relais::io::test;
using namespace std::chrono_literals;

using Plain       = AdversarialIoContext<kNone>;
using Split       = AdversarialIoContext<kSplitEvents>;
using Spurious    = AdversarialIoContext<kSpurious>;
using Coarse      = AdversarialIoContext<kCoarseTimers>;
using Adversarial = AdversarialIoContext<kAll>;

static_assert(IoContext<Plain> && IoContext<Adversarial>,
              "an adapter without cancelTimer satisfies the concept");

#ifdef RELAIS_TEST_TRANTOR_LOOP
#define ADVERSARIAL_LOOPS TrantorTestLoop
#else
#define ADVERSARIAL_LOOPS Plain, Split, Spurious, Coarse, Adversarial
#endif

namespace {

const char* conninfo() { return relais_test::kConnInfo; }

Task<void> together(std::vector<Task<void>> tasks) {
    co_await whenAll(std::move(tasks));
}

// What every case ends with: the posted removals run, relais kept its side of
// the contract, and the variant did what it is there for.
template<typename Loop>
void requireContractKept(Loop& io) {
    pump(io, 50ms);
    CHECK(io.breaches() == 0);
    CHECK(io.liveWatches() == 0);
}

}  // namespace

TEMPLATE_TEST_CASE("adversarial loop: a connection serves requests",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    TestType io;
    const int fds_before = countOpenFds();
    {
        auto conn = driveBounded(io, PgConnection<TestType>::connect(io, conninfo(), 2s, 2s));
        REQUIRE(conn->connected());
        for (int i = 0; i < 5; ++i) {
            auto r = driveBounded(io, conn->queryParams("SELECT $1::int + 1",
                                                        PgParams::make(int32_t{i})));
            REQUIRE(r.ok());
            REQUIRE(r[0].template get<int32_t>(0) == i + 1);
        }
        auto r = driveBounded(io, conn->query("SELECT generate_series(1, 5000)"));
        REQUIRE(r.rows() == 5000);
    }
    requireContractKept(io);
    CHECK(countOpenFds() <= fds_before);
    if constexpr (std::is_same_v<TestType, Spurious> || std::is_same_v<TestType, Adversarial>)
        CHECK(io.spuriousCalls() > 0);
}

TEMPLATE_TEST_CASE("adversarial loop: a pool serves concurrent requests",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    TestType io;
    {
        auto pool = driveBounded(io, PgPool<TestType>::create(io, conninfo(),
            {.min_connections = 2, .max_connections = 3, .acquire_timeout = 2s,
             .query_timeout = 2s}));
        int ok = 0;
        auto request = [&](int i) -> Task<void> {
            auto guard = co_await pool->acquire();
            auto r = co_await guard.conn().queryParams(
                "SELECT $1::int * 2 FROM pg_sleep(0.01)", PgParams::make(int32_t{i}));
            if (r.ok() && r[0].template get<int32_t>(0) == 2 * i) ++ok;
        };
        std::vector<Task<void>> requests;
        for (int i = 0; i < 24; ++i) requests.push_back(request(i));
        driveBounded(io, together(std::move(requests)));
        CHECK(ok == 24);
        CHECK(pool->waiterCount() == 0);
    }
    requireContractKept(io);
}

TEMPLATE_TEST_CASE("adversarial loop: pipelines, one of them blocking its flush",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    TestType io;
    {
        auto conn = driveBounded(io, PgConnection<TestType>::connect(io, conninfo(), 2s, 5s));
        using Conn = PgConnection<TestType>;
        auto send = [&](const char* sql, const PgParams& params) {
            if (conn->ensurePreparedPipelined(sql, params.count())) conn->pipelineSync();
            conn->sendPreparedPipelined(sql, params);
            conn->pipelineSync();
        };

        std::vector<typename Conn::PipelineResult> results;
        driveBounded(io, [&]() -> Task<void> {
            conn->enterPipelineMode();
            for (int i = 0; i < 8; ++i)
                send("SELECT $1::int + 1", PgParams::make(int32_t{i}));
            co_await conn->flushPipeline();
            co_await conn->readPipelineResults(8, results);
            conn->exitPipelineMode();
        }());
        REQUIRE(results.size() == 8);
        for (int i = 0; i < 8; ++i)
            CHECK(results[i].result[0].template get<int32_t>(0) == i + 1);

        // An answer larger than both socket buffers ahead of statements the
        // server reads only once it is out: the flush waits for Read|Write and
        // reads meanwhile.
        constexpr int kTail = 256;
        const std::string payload(64 * 1024, 'x');
        results.clear();
        driveBounded(io, [&]() -> Task<void> {
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

        // A flush waiting while the server sleeps, and a loop that stops turning
        // while the server answers and reads: its next event carries Read and
        // Write at once, which a split loop delivers as two calls.
        results.clear();
        bool done = false;
        std::exception_ptr failure;
        auto sleepy = [&]() -> DetachedTask {
            try {
                conn->enterPipelineMode();
                send("SELECT pg_sleep(0.2)", PgParams{});
                for (int i = 0; i < kTail; ++i)
                    send("SELECT length($1::text)", PgParams::make(payload));
                co_await conn->flushPipeline();
                co_await conn->readPipelineResults(kTail + 1, results);
                conn->exitPipelineMode();
            } catch (...) {
                failure = std::current_exception();
            }
            done = true;
        };
        sleepy();
        pump(io, 50ms);
        std::this_thread::sleep_for(500ms);
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (!done && std::chrono::steady_clock::now() < deadline) io.runOnce(20);
        REQUIRE(done);
        if (failure) std::rethrow_exception(failure);
        REQUIRE(results.size() == kTail + 1);
        for (const auto& r : results) REQUIRE(r.result.ok());
    }
    requireContractKept(io);
    if constexpr (std::is_same_v<TestType, Split> || std::is_same_v<TestType, Adversarial>)
        CHECK(io.splitEvents() > 0);
}

TEMPLATE_TEST_CASE("adversarial loop: batched reads and writes",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    TestType io;
    {
        auto pool = driveBounded(io, PgPool<TestType>::create(io, conninfo(),
            {.min_connections = 1, .max_connections = 1, .acquire_timeout = 2s,
             .query_timeout = 2s}));
        auto batcher = std::make_shared<BatchScheduler<TestType>>(io, pool, 8);
        REQUIRE(driveBounded(io, batcher->directQuery(
            "CREATE TEMP TABLE adversarial_batch (v int)")).ok());

        int writes = 0, reads = 0;
        auto write = [&](int i) -> Task<void> {
            auto [r, coalesced] = co_await batcher->submitPgWrite(
                "INSERT INTO adversarial_batch VALUES ($1)", PgParams::make(int32_t{i}));
            if (r.ok()) ++writes;
        };
        auto read = [&](int i) -> Task<void> {
            auto r = co_await batcher->submitQueryRead("SELECT $1::int",
                                                       PgParams::make(int32_t{i}));
            if (r.ok() && r[0].template get<int32_t>(0) == i) ++reads;
        };
        for (int round = 0; round < 3; ++round) {
            std::vector<Task<void>> tasks;
            for (int i = 0; i < 16; ++i) {
                tasks.push_back(write(round * 16 + i));
                tasks.push_back(read(i));
            }
            driveBounded(io, together(std::move(tasks)));
        }
        CHECK(writes == 48);
        CHECK(reads == 48);
        auto count = driveBounded(io, batcher->submitQueryRead(
            "SELECT count(*)::int FROM adversarial_batch", PgParams{}));
        CHECK(count[0].template get<int32_t>(0) == 48);
    }
    requireContractKept(io);
    CHECK(io.armedTimers() > 0);  // the batches' departure timers, never cancelled
}

TEMPLATE_TEST_CASE("adversarial loop: a connect follows the socket libpq replaces",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    TlsDropServer srv;
    TestType io;
    const auto t0 = std::chrono::steady_clock::now();
    std::string error;
    try {
        driveBounded(io, PgConnection<TestType>::connect(io, srv.conninfo().c_str(), 3s));
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
    requireContractKept(io);
}

TEMPLATE_TEST_CASE("adversarial loop: a TCP connect still pending ends at acquire_timeout",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    PendingConnectServer srv;
    TestType io;
    const auto t0 = std::chrono::steady_clock::now();
    REQUIRE_THROWS_AS(
        driveBounded(io, PgConnection<TestType>::connect(io, srv.conninfo().c_str(), 300ms)),
        PgPoolTimeout);
    const auto elapsed = std::chrono::steady_clock::now() - t0;
    CHECK(elapsed >= 300ms);
    CHECK(elapsed < 2s);
    requireContractKept(io);
}

TEMPLATE_TEST_CASE("adversarial loop: query_timeout and acquire_timeout bound their waits",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    TestType io;
    {
        auto conn = driveBounded(io, PgConnection<TestType>::connect(io, conninfo(), 2s, 200ms));
        const auto t0 = std::chrono::steady_clock::now();
        REQUIRE_THROWS_AS(driveBounded(io, conn->query("SELECT pg_sleep(2)")), PgQueryTimeout);
        CHECK(std::chrono::steady_clock::now() - t0 < 1s);
        CHECK_FALSE(conn->connected());
    }
    {
        SilentTcpServer silent;
        auto pool = driveBounded(io, PgPool<TestType>::create(io, silent.conninfo(),
            {.min_connections = 0, .max_connections = 1, .acquire_timeout = 250ms}));
        REQUIRE_THROWS_AS(driveBounded(io, pool->acquire(), 2s), PgPoolTimeout);
        CHECK(pool->totalConnections() == 0);
    }
    {
        auto pool = driveBounded(io, PgPool<TestType>::create(io, conninfo(),
            {.min_connections = 1, .max_connections = 1, .acquire_timeout = 250ms}));
        auto held = driveBounded(io, pool->acquire());
        REQUIRE_THROWS_AS(driveBounded(io, pool->acquire(), 2s), PgPoolTimeout);
        CHECK(pool->waiterCount() == 0);
    }
    requireContractKept(io);
}

TEMPLATE_TEST_CASE("adversarial loop: the server closes an idle connection",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    TestType io;
    const std::string app = "relais_adversarial_idle";
    const std::string info = std::string(conninfo()) + " application_name=" + app;
    PgProbe probe(conninfo());
    {
        auto conn = driveBounded(io, PgConnection<TestType>::connect(io, info.c_str(), 2s, 2s));
        REQUIRE(conn->connected());
        const int fds_connected = countOpenFds();
        REQUIRE(probe.terminateBackends("application_name = '" + app + "'") == 1);
        const auto deadline = std::chrono::steady_clock::now() + 3s;
        while (conn->connected() && std::chrono::steady_clock::now() < deadline)
            io.runOnce(10);
        REQUIRE_FALSE(conn->connected());
        pump(io, 50ms);
        CHECK(countOpenFds() == fds_connected - 2);  // libpq's socket and its duplicate
    }
    {
        auto pool = driveBounded(io, PgPool<TestType>::create(io, info,
            {.min_connections = 1, .max_connections = 1, .acquire_timeout = 2s,
             .query_timeout = 2s}));
        REQUIRE(probe.terminateBackends("application_name = '" + app + "'") == 1);
        pump(io, 200ms);
        auto guard = driveBounded(io, pool->acquire());
        CHECK(driveBounded(io, guard.conn().query("SELECT 1")).ok());
    }
    requireContractKept(io);
}

TEMPLATE_TEST_CASE("adversarial loop: the server ends a request mid-wait",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    TestType io;
    const std::string app = "relais_adversarial_rst";
    const std::string info = std::string(conninfo()) + " application_name=" + app;
    PgProbe probe(conninfo());
    {
        auto conn = driveBounded(io, PgConnection<TestType>::connect(io, info.c_str(), 2s, 5s));
        bool threw = false;
        driveBounded(io, [&]() -> Task<void> {
            io.post([&] { probe.terminateBackends("application_name = '" + app + "'"); });
            try {
                co_await conn->query("SELECT pg_sleep(2)");
            } catch (const PgError&) {
                threw = true;
            }
        }(), 4s);
        CHECK(threw);
        CHECK_FALSE(conn->connected());
    }
    requireContractKept(io);
}

TEMPLATE_TEST_CASE("adversarial loop: attach checks the loop, binds it, and detach lets go",
                   "[io][adversarial][integration]", ADVERSARIAL_LOOPS) {
    LoopThread<TestType> t;  // the adapter belongs to the thread that runs it
    {
        auto relais = jcailloux::relais::attach(t.loop(), conninfo(),
            {.pool = {.min_connections = 2, .max_connections = 2}});
        const auto value = t.run([]() -> Task<std::string> {
            auto result = co_await jcailloux::relais::PgProvider::query("SELECT 6 * 7");
            co_return std::string(result[0].rawValue(0));
        });
        CHECK(value == "42");
    }
    t.settle();  // the connections' posted removals
    CHECK(t.call([&] { return t.loop().breaches(); }) == 0);
    CHECK(t.call([&] { return t.loop().liveWatches(); }) == 0);
}
