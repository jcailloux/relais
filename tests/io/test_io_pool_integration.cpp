#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/IoPool.h>
#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/Task.h>

#include <future>

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
            try {
                auto r = co_await PgProvider::query("SELECT 1");
                res.ok = (r.rows() == 1);
                if (res.ok) res.value = r[0].get<int>(0);
            } catch (...) {
                res.ok = false;
            }
            pr.set_value(res);
        }(p);
    });
    REQUIRE(fut.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    return fut.get();
}

}  // namespace

TEST_CASE("IoPool: each worker binds its own provider and routes locally",
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

    // Shared-nothing: the two probes ran on two distinct loop threads.
    REQUIRE(p0.tid != p1.tid);

    pool->stop();
}
