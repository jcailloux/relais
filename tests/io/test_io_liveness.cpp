// Liveness / timeout suite for the I/O layer.
//
// Every test here drives a fault into the loop — a withheld read, a blackholed
// connect, an injected RST, a poisoned pool slot — and asserts the worker stays
// live: a bounded timeout fires, the poisoned resource is reclaimed, no slot or
// waiter leaks, and nothing double-resumes or dangles. Run it under ASan
// (.build/asan) and TSan (.build/tsan); the bugs these guard against are exactly
// the kind those sanitizers catch.
//
// Fault injection lives entirely in tests/fixtures (ControllableIoContext,
// PgProbe) plus the in-test SilentTcpServer — nothing in include/ knows it is
// under test.

#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/pg/PgConnection.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/io/redis/RedisConnection.h>
#include <jcailloux/relais/io/redis/RedisPool.h>
#include <jcailloux/relais/io/redis/RedisError.h>
#include <jcailloux/relais/io/batch/BatchScheduler.h>
#include <jcailloux/relais/io/IoPool.h>
#include <jcailloux/relais/io/WhenAll.h>

#include <fixtures/ControllableIoContext.h>
#include <fixtures/EpollIoContext.h>
#include <fixtures/PgProbe.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <coroutine>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::batch;
using namespace jcailloux::relais::io::test;
using namespace std::chrono_literals;

using Io = ControllableIoContext;

// =============================================================================
// Environment + helpers
// =============================================================================

static const char* getConnInfo() {
    return "host=localhost port=5432 dbname=relais_test "
           "user=relais_test password=relais_test";
}

// A TCP listener that accepts the kernel handshake (so connect() completes) but
// never reads or replies. To a PG client the socket is writable for the startup
// packet, then never readable — the exact silent hang the connect/query bound
// must catch. To a Redis client the TCP connect simply completes and the first
// reply never comes.
struct SilentTcpServer {
    int fd = -1;
    int port_ = 0;

    SilentTcpServer() {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        REQUIRE(fd >= 0);
        int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;  // ephemeral
        REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        REQUIRE(::listen(fd, 16) == 0);
        socklen_t len = sizeof(addr);
        REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
        port_ = ntohs(addr.sin_port);
    }
    ~SilentTcpServer() { if (fd >= 0) ::close(fd); }
    SilentTcpServer(const SilentTcpServer&) = delete;
    SilentTcpServer& operator=(const SilentTcpServer&) = delete;

    int port() const { return port_; }
    std::string conninfo() const {
        return "host=127.0.0.1 port=" + std::to_string(port_) +
               " dbname=relais_test user=relais_test password=x";
    }
};

// Count process fds. Used as a leak tripwire: the offset (., .., the opendir fd)
// is constant, so a before/after delta is meaningful.
static int countOpenFds() {
    DIR* d = ::opendir("/proc/self/fd");
    if (!d) return -1;
    int n = 0;
    while (::readdir(d)) ++n;
    ::closedir(d);
    return n;
}

// Bounded synchronous driver. A liveness suite must never hang the runner, so
// every Task is driven against a wall-clock budget; blowing the budget is a test
// failure, not a hang. Mirrors the fixtures' runTask but caps the loop and works
// over any IoContext (so ControllableIoContext's masking layer sees the watches).
template<typename T>
T driveBounded(Io& io, Task<T> task, std::chrono::milliseconds budget = 5s) {
    std::variant<std::monostate, T, std::exception_ptr> result;
    bool done = false;

    auto wrapper = [&](Task<T> t) -> Task<void> {
        try {
            result.template emplace<1>(co_await std::move(t));
        } catch (...) {
            result.template emplace<2>(std::current_exception());
        }
        done = true;
    };

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
    auto starter = [&](Task<void> t) -> Starter { co_await std::move(t); };
    auto s = starter(wrapper(std::move(task)));

    auto deadline = std::chrono::steady_clock::now() + budget;
    while (!done && std::chrono::steady_clock::now() < deadline)
        io.runOnce(20);
    if (s.handle) s.handle.destroy();

    REQUIRE(done);  // never silently time out
    if (auto* ex = std::get_if<2>(&result))
        std::rethrow_exception(*ex);
    return std::move(std::get<1>(result));
}

inline void driveBounded(Io& io, Task<void> task, std::chrono::milliseconds budget = 5s) {
    std::exception_ptr ex;
    bool done = false;

    auto wrapper = [&](Task<void> t) -> Task<void> {
        try { co_await std::move(t); }
        catch (...) { ex = std::current_exception(); }
        done = true;
    };
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
    auto starter = [&](Task<void> t) -> Starter { co_await std::move(t); };
    auto s = starter(wrapper(std::move(task)));

    auto deadline = std::chrono::steady_clock::now() + budget;
    while (!done && std::chrono::steady_clock::now() < deadline)
        io.runOnce(20);
    if (s.handle) s.handle.destroy();

    REQUIRE(done);
    if (ex) std::rethrow_exception(ex);
}

// Spin the loop for a fixed window so any stray posted resume / timer / late
// readiness would surface (under ASan/TSan) instead of going unobserved.
static void pump(Io& io, std::chrono::milliseconds window = 100ms) {
    auto deadline = std::chrono::steady_clock::now() + window;
    while (std::chrono::steady_clock::now() < deadline)
        io.runOnce(10);
}

// =============================================================================
// Connection-level timeout (drive a single PgConnection)
// =============================================================================

TEST_CASE("liveness: a withheld read trips query_timeout (PgQueryTimeout)",
          "[io][liveness][integration]") {
    Io io;
    auto conn = driveBounded(io, PgConnection<Io>::connect(io, getConnInfo(), 2s, 200ms));
    REQUIRE(conn.connected());

    // Hold the read side before the query: the send proceeds (write-ready is not
    // masked), the response is withheld, the query_timeout fires.
    io.holdReads(conn.socket());
    REQUIRE_THROWS_AS(
        driveBounded(io, conn.query("SELECT 1")),
        PgQueryTimeout);

    // The connection is poisoned, not silently reusable.
    REQUIRE_FALSE(conn.connected());
}

TEST_CASE("liveness: a late result after teardown is dropped, not re-injected",
          "[io][liveness][integration]") {
    Io io;
    int fd = -1;
    {
        auto conn = driveBounded(io, PgConnection<Io>::connect(io, getConnInfo(), 2s, 150ms));
        fd = conn.socket();
        io.holdReads(fd);
        REQUIRE_THROWS_AS(driveBounded(io, conn.query("SELECT 1")), PgQueryTimeout);
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
    // registration survived the connect's WRITING→READING mask flips (the whole
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
    // and the pool is never wedged at max with phantom slots.
    REQUIRE(pool->totalConnections() == 0);
    REQUIRE(pool->waiterCount() == 0);
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
    REQUIRE(conn.connected());

    PgProbe probe(getConnInfo());

    // Run a slow query; while it is in flight (watch + query_timer armed), the
    // probe terminates the backend → PQconsumeInput sees the RST → the awaiter
    // resolves in error, routing through removeCurrentWatch (cancel timer +
    // removeWatch). The timer must not fire later on the released connection.
    bool killed = false;
    auto runner = [&]() -> Task<void> {
        // Defer the kill to the loop so it lands while the read is suspended.
        io.post([&] {
            probe.terminateBackends("application_name = 'relais_liveness_rst'");
            killed = true;
        });
        try {
            co_await conn.query("SELECT pg_sleep(2)");
        } catch (...) {
            // PgError on RST — the point is liveness, not the exact type.
        }
    };
    REQUIRE_NOTHROW(driveBounded(io, runner(), 4s));
    REQUIRE(killed);
    pump(io);  // a stray timer would fire (and UAF under ASan) here
    SUCCEED("error-path resolution cancelled the watch-bound timer");
}

// =============================================================================
// Redis silent hang: query bound + lock release
// =============================================================================

TEST_CASE("liveness: a silent Redis trips RedisQueryTimeout and releases the lock",
          "[io][liveness][integration]") {
    SilentTcpServer silent;
    Io io;
    auto client = driveBounded(io,
        RedisClient<Io>::connect(io, "127.0.0.1", silent.port(), 200ms));
    REQUIRE(client->connected());  // TCP completed; the reply never will

    // First command: send ok, reply withheld → RedisQueryTimeout.
    REQUIRE_THROWS_AS(driveBounded(io, client->exec("PING")), RedisQueryTimeout);

    // Second command on the SAME client must not deadlock on the coroutine mutex
    // (execArgv's catch released it). If the lock had leaked, this would never
    // start its own read and driveBounded would blow its budget.
    REQUIRE_THROWS_AS(driveBounded(io, client->exec("PING"), 2s), RedisQueryTimeout);
}

// =============================================================================
// RedisPool: dead-slot avoidance + reviveDeadClients
// =============================================================================

// Poison a fresh client by timing it out against the silent server; it ends
// dead_ and quiescent (no holder, no waiter).
static std::shared_ptr<RedisClient<Io>> poisonedClient(Io& io, int silent_port) {
    auto c = driveBounded(io,
        RedisClient<Io>::connect(io, "127.0.0.1", silent_port, 150ms));
    try { driveBounded(io, c->exec("PING")); } catch (const RedisQueryTimeout&) {}
    REQUIRE_FALSE(c->connected());
    REQUIRE(c->isQuiescent());
    return c;
}

TEST_CASE("liveness: next() steers off dead slots; reviveDeadClients rebuilds them",
          "[io][liveness][integration]") {
    SilentTcpServer silent;
    Io io;

    auto healthy = driveBounded(io, RedisClient<Io>::connect(io, "127.0.0.1", 6379));
    REQUIRE(healthy->connected());
    auto dead = poisonedClient(io, silent.port());

    // Reconnect factory rebuilds a dead slot from the real Redis.
    RedisPool<Io>::ReconnectFactory factory =
        [&io](std::size_t) -> Task<std::shared_ptr<RedisClient<Io>>> {
            co_return co_await RedisClient<Io>::connect(io, "127.0.0.1", 6379);
        };
    std::vector<std::shared_ptr<RedisClient<Io>>> clients{healthy, dead};
    auto pool = RedisPool<Io>::fromClients(std::move(clients), factory);

    // next() never hands out the dead slot while a live one exists.
    for (int i = 0; i < 8; ++i)
        REQUIRE(pool.next().connected());

    // Cold-path revive rebuilds the (quiescent) dead slot; the healthy one is
    // skipped at the connected() gate.
    auto revived = driveBounded(io, pool.reviveDeadClients());
    REQUIRE(revived == 1);
    REQUIRE(pool.at(0).connected());
    REQUIRE(pool.at(1).connected());
}

TEST_CASE("liveness: two dead quiescent slots are rebuilt in one pass without UAF",
          "[io][liveness][integration]") {
    SilentTcpServer silent;
    Io io;

    auto deadA = poisonedClient(io, silent.port());
    auto deadB = poisonedClient(io, silent.port());

    RedisPool<Io>::ReconnectFactory factory =
        [&io](std::size_t) -> Task<std::shared_ptr<RedisClient<Io>>> {
            co_return co_await RedisClient<Io>::connect(io, "127.0.0.1", 6379);
        };
    std::vector<std::shared_ptr<RedisClient<Io>>> clients{deadA, deadB};
    auto pool = RedisPool<Io>::fromClients(std::move(clients), factory);

    // Both dead slots are quiescent → both revived in a single pass; the swap of
    // two slots in one cycle is the ASan-relevant path (no use-after-free on the
    // replaced clients, which the pool solely owns).
    auto revived = driveBounded(io, pool.reviveDeadClients());
    REQUIRE(revived == 2);
    REQUIRE(pool.at(0).connected());
    REQUIRE(pool.at(1).connected());
}

// =============================================================================
// Pipeline / batched paths
// =============================================================================

TEST_CASE("liveness: a fast multi-result pipeline re-arms with no spurious timeout",
          "[io][liveness][integration]") {
    Io io;
    // Short query_timeout, but each round-trip is fast → every re-registered
    // watch cancels its predecessor's timer; zero spurious PgQueryTimeout and no
    // timer accumulation.
    auto conn = driveBounded(io, PgConnection<Io>::connect(io, getConnInfo(), 2s, 500ms));

    for (int i = 0; i < 5; ++i) {
        auto r = driveBounded(io, conn.query("SELECT generate_series(1, 50)"));
        REQUIRE(r.ok());
        REQUIRE(r.rows() == 50);
    }
    REQUIRE(conn.connected());
    REQUIRE(io.pendingTimerCount() == 0);  // no timer leaked across re-arms
}

TEST_CASE("liveness: a batched fire that cannot acquire a connection fails its waiters",
          "[io][liveness][integration]") {
    SilentTcpServer silent;
    Io io;
    // Pool that can never connect (silent server), short acquire bound.
    auto pool = driveBounded(io, PgPool<Io>::create(io, silent.conninfo(),
        {.min_connections = 0, .max_connections = 2, .acquire_timeout = 300ms}));
    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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

    auto batcher = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);

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
        auto b = std::make_shared<BatchScheduler<Io>>(io, pool, nullptr, 8);
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
