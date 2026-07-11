#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/IoPool.h>
#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/Task.h>

#include <chrono>
#include <future>
#include <stdexcept>

// Exercises IoPool — relais's reference shared-nothing N-loop runtime. Verifies
// that each worker binds its OWN thread_local PgProvider on its OWN thread and
// that a query co_awaited on a worker routes through that worker's pool.
//
// Requires PostgreSQL (relais_test) + Redis on localhost.

using namespace jcailloux::relais;
using namespace jcailloux::relais::io;

namespace {

const char* connInfo() {
    return "host=localhost port=5432 dbname=relais_test "
           "user=relais_test password=relais_test";
}

struct Probe {
    bool bound = false;     // PgProvider bound on the worker thread (thread_local)
    bool ok = false;        // query routed and returned a row
    int value = 0;
    std::thread::id tid;    // which thread the query ran on
    bool has_redis = false;         // PgProvider::hasRedis() on this worker thread
    bool redis_unconfigured = false;// redis() threw std::logic_error (redis_exec_ null)
    bool redis_ok = false;          // a Redis command round-tripped, no exception
};

// Run a probe ON worker `w`'s loop thread (post → executed during its runOnce).
Probe probeWorker(IoPool& pool, int w) {
    std::promise<Probe> p;
    auto fut = p.get_future();
    pool.workerIo(w).post([&p] {
        [](std::promise<Probe>& pr) -> DetachedTask {
            Probe res;
            res.tid = std::this_thread::get_id();
            res.bound = PgProvider::initialized();  // this thread's thread_local
            res.has_redis = PgProvider::hasRedis();
            try {
                auto r = co_await PgProvider::query("SELECT 1");
                res.ok = (r.rows() == 1);
                if (res.ok) res.value = r[0].get<int>(0);
            } catch (...) {
                res.ok = false;
            }
            // Probe the Redis binding. Unbound surfaces as the std::logic_error
            // from PgProvider::redis (redis_exec_ null) — NOT io::RedisError from
            // an empty batcher pool, which is the OLD bug's signature (hasRedis()
            // wrongly true over a zero-connection pool). A live Redis round-trips
            // with no exception.
            try {
                co_await PgProvider::redis("PING");
                res.redis_ok = true;
            } catch (const std::logic_error&) {
                res.redis_unconfigured = true;
            } catch (...) {
                // io::RedisError or other — leave both flags false (old-bug shape).
            }
            pr.set_value(res);
        }(p);
    });
    REQUIRE(fut.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    return fut.get();
}

}  // namespace

// A — public defaults contract. No I/O; a runtime REQUIRE (not static_assert:
// both structs hold std::string members, unusable in a persistent constant
// expression). Guards the shipped defaults against silent drift.
TEST_CASE("IoPool: RedisWorkerConfig / IoPoolConfig defaults", "[io][iopool]") {
    RedisWorkerConfig r;
    REQUIRE(r.unix_path.empty());
    REQUIRE(r.host == "127.0.0.1");
    REQUIRE(r.port == 6379);
    REQUIRE(r.conns_per_worker == 4);

    IoPoolConfig cfg;
    // Default is an enabled localhost pool — preserves the v2.0.0 behaviour. The
    // L1-only path is the explicit std::nullopt opt-in, never the default.
    REQUIRE(cfg.redis.has_value());
    REQUIRE(cfg.redis->host == "127.0.0.1");
    REQUIRE(cfg.redis->port == 6379);

    cfg.redis = std::nullopt;  // discoverable, unambiguous opt-out
    REQUIRE_FALSE(cfg.redis.has_value());
}

// C — default (Redis enabled): each worker binds its own provider on its own
// thread, PG routes locally, hasRedis() is true, and a Redis command round-trips
// on the worker loop. Non-regression guard for the nominal path.
TEST_CASE("IoPool: each worker binds its own provider and routes PG + Redis locally",
          "[io][iopool][integration]") {
    IoPoolConfig cfg;
    cfg.num_workers = 2;
    cfg.pg_conninfo = connInfo();
    cfg.redis = RedisWorkerConfig{.host = "127.0.0.1", .port = 6379,
                                  .conns_per_worker = 1};
    cfg.pin_to_cores = false;  // don't fight the test runner for cores
    cfg.pg_min_conns_per_worker = 1;
    cfg.pg_max_conns_per_worker = 2;

    auto pool = IoPool::create(cfg);
    REQUIRE(pool->numWorkers() == 2);

    auto p0 = probeWorker(*pool, 0);
    auto p1 = probeWorker(*pool, 1);

    // Each worker bound its own provider on its own thread and served the query.
    REQUIRE(p0.bound);
    REQUIRE(p0.ok);
    REQUIRE(p0.value == 1);

    REQUIRE(p1.bound);
    REQUIRE(p1.ok);
    REQUIRE(p1.value == 1);

    // Redis is enabled: hasRedis() is true and PING round-trips with no exception.
    REQUIRE(p0.has_redis);
    REQUIRE(p0.redis_ok);
    REQUIRE(p1.has_redis);
    REQUIRE(p1.redis_ok);

    // Shared-nothing: the two probes ran on two distinct loop threads.
    REQUIRE(p0.tid != p1.tid);

    pool->stop();
}

// B — L1-only (redis=nullopt): startup succeeds with no Redis, PG still routes,
// and every worker reports hasRedis()==false. The Redis probe throws the
// std::logic_error of an unbound provider (redis_exec_ null), NOT the
// io::RedisError of an empty batcher pool — that distinction is the regression
// guard for the old always-true hasRedis() bug. (Subsumes test F1.)
TEST_CASE("IoPool: L1-only (redis=nullopt) routes PG, reports hasRedis()==false",
          "[io][iopool][integration]") {
    IoPoolConfig cfg;
    cfg.num_workers = 2;
    cfg.pg_conninfo = connInfo();
    cfg.redis = std::nullopt;  // L1-only: no pool, no boot connect
    cfg.pin_to_cores = false;
    cfg.pg_min_conns_per_worker = 1;
    cfg.pg_max_conns_per_worker = 2;

    auto pool = IoPool::create(cfg);  // must succeed with no reachable Redis
    REQUIRE(pool->numWorkers() == 2);

    auto p0 = probeWorker(*pool, 0);
    auto p1 = probeWorker(*pool, 1);

    for (const auto& p : {p0, p1}) {
        REQUIRE(p.bound);
        REQUIRE(p.ok);
        REQUIRE(p.value == 1);
        REQUIRE_FALSE(p.has_redis);
        REQUIRE(p.redis_unconfigured);   // std::logic_error, not io::RedisError
        REQUIRE_FALSE(p.redis_ok);
    }
    REQUIRE(p0.tid != p1.tid);

    pool->stop();
}

// H — the Unix-socket branch is reachable through the optional. A nonexistent
// socket path fails the connect fast (ENOENT), failing startup — proving
// RedisWorkerConfig.unix_path routes to createUnix, not the TCP branch.
TEST_CASE("IoPool: a Unix-socket Redis endpoint routes through the optional",
          "[io][iopool][integration]") {
    IoPoolConfig cfg;
    cfg.num_workers = 1;
    cfg.pg_conninfo = connInfo();
    cfg.redis = RedisWorkerConfig{.unix_path = "/nonexistent-relais-test.sock",
                                  .conns_per_worker = 1};
    cfg.pin_to_cores = false;
    cfg.pg_min_conns_per_worker = 1;
    cfg.pg_max_conns_per_worker = 1;
    cfg.query_timeout = std::chrono::milliseconds(300);

    REQUIRE_THROWS(IoPool::create(cfg));
}
