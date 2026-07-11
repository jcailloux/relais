// iopool_nloop.cpp — relais's built-in shared-nothing N-loop runtime.
//
// IoPool spins up N epoll loops (one per core), each pinned with its OWN
// connection pools + BatchScheduler, each binding its OWN thread_local
// PgProvider. A query co_awaited on a worker routes to THAT worker's pool — no
// cross-thread hop. Throughput scales ~linearly with loops at unchanged latency.
//
// This program runs a query on each worker and prints the thread it ran on plus
// the PostgreSQL backend PID it used — distinct per worker, demonstrating that
// the loops share nothing.
//
// Requires PostgreSQL + Redis reachable. No table needed (it queries
// pg_backend_pid()). conninfo is empty → libpq reads the PG* environment.
//
// Build: cmake -B .build/dev -DRELAIS_BUILD_EXAMPLES=ON
//        cmake --build .build/dev --target example_iopool_nloop
// Run:   PGHOST=localhost PGDATABASE=relais_test PGUSER=relais_test \
//        PGPASSWORD=relais_test ./.build/dev/example_iopool_nloop

#include <jcailloux/relais/io/IoPool.h>
#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/Task.h>

#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

using namespace jcailloux::relais;
using namespace jcailloux::relais::io;

// Probe worker `w`: run a query ON its loop thread (posted there) and report
// which thread + PG backend served it.
static std::string probe(IoPool& pool, int w) {
    std::promise<std::string> p;
    auto fut = p.get_future();
    pool.workerIo(w).post([w, &p] {
        [](int w, std::promise<std::string>& pr) -> DetachedTask {
            try {
                auto r = co_await PgProvider::query("SELECT pg_backend_pid()");
                std::ostringstream os;
                os << "worker " << w
                   << "  thread=" << std::this_thread::get_id()
                   << "  pg_backend_pid=" << r[0].get<int>(0);
                pr.set_value(os.str());
            } catch (const std::exception& e) {
                pr.set_value("worker " + std::to_string(w) + ": " + e.what());
            }
        }(w, p);
    });
    return fut.get();
}

int main() {
    IoPoolConfig cfg;
    cfg.num_workers = 3;
    cfg.pg_conninfo = "";          // empty → libpq reads PG* env
    cfg.redis = RedisWorkerConfig{.host = "127.0.0.1", .port = 6379,
                                  .conns_per_worker = 1};
    cfg.pin_to_cores = false;      // don't pin in a demo
    cfg.pg_min_conns_per_worker = 1;
    cfg.pg_max_conns_per_worker = 2;

    std::cout << "Starting IoPool with " << cfg.num_workers << " loops...\n";
    std::unique_ptr<IoPool> pool;
    try {
        pool = IoPool::create(cfg);  // blocks until all workers connect + bind
    } catch (const std::exception& e) {
        std::cerr << "Could not start (is PostgreSQL/Redis reachable?): "
                  << e.what() << '\n';
        return 1;
    }
    std::cout << "All " << pool->numWorkers() << " loops up.\n\n";

    for (int w = 0; w < pool->numWorkers(); ++w)
        std::cout << probe(*pool, w) << '\n';

    std::cout << "\nDistinct threads + PG backends per worker → shared-nothing.\n";
    pool->stop();
    return 0;
}