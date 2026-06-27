#ifndef JCX_RELAIS_IO_POOL_H
#define JCX_RELAIS_IO_POOL_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <pthread.h>
#include <sched.h>

#include "jcailloux/relais/io/EpollIoContext.h"
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgPool.h"
#include "jcailloux/relais/io/redis/RedisPool.h"
#include "jcailloux/relais/io/batch/BatchScheduler.h"
#include "jcailloux/relais/PgProvider.h"

namespace jcailloux::relais::io {

// Thrown by IoPool::create() when a worker fails to report ready within
// startup_timeout — e.g. a connection that hangs at boot with no client-side
// query bound. A worker whose initialization throws surfaces its own
// exception instead (rethrown verbatim).
struct IoPoolStartupError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// IoPoolConfig — configuration for the multi-core I/O pool.

struct IoPoolConfig {
    int num_workers = 1;
    std::string pg_conninfo;

    // Redis: prefer Unix socket, fall back to TCP
    std::string redis_unix_path;              // Empty = use TCP
    std::string redis_host = "127.0.0.1";
    int redis_port = 6379;

    // PG pool sizing per worker
    size_t pg_min_conns_per_worker = 2;
    size_t pg_max_conns_per_worker = 8;

    // Redis pool sizing per worker
    size_t redis_conns_per_worker = 4;

    // Shared I/O budget per worker (PG + Redis combined)
    int max_concurrent_per_worker = 8;

    // Liveness timeouts threaded into the per-worker pools.
    //   - acquire_timeout bounds each PG acquire, including the warm-up connect
    //     at boot, so an unreachable PostgreSQL fails startup instead of hanging.
    //   - query_timeout bounds each per-connection I/O wait (PG and Redis). On
    //     Redis it also bounds the boot connect handshake. 0 = no client bound
    //     (PG delegates to the server's statement_timeout; Redis is unbounded —
    //     then startup_timeout is the only backstop for a blackholed boot).
    //   - startup_timeout bounds the wait for all workers to report ready, so a
    //     worker stuck in a connect that never returns cannot freeze create().
    std::chrono::milliseconds acquire_timeout{5000};
    std::chrono::milliseconds query_timeout{0};
    std::chrono::milliseconds startup_timeout{30000};

    // Core pinning
    bool pin_to_cores = true;
    int first_core = 1;  // Avoid core 0 (OS/IRQ)
};

// IoPool — N event loops pinned on N cores, each with its own resources.
//
// Each worker owns:
// - An EpollIoContext (event loop)
// - A PgPool (PostgreSQL connection pool)
// - A RedisPool (Redis connection pool)
// - A BatchScheduler (adaptive batching)
// - A std::jthread (the actual OS thread)
//
// Each worker binds PgProvider's thread_local providers to its OWN
// BatchScheduler, on its own thread, during create(). A coroutine running on a
// worker thus routes to that worker's resources with no cross-thread hop
// (shared-nothing). This is the same per-loop-thread init() contract any
// external router (e.g. a Drogon/trantor adapter) uses.

class IoPool {
public:
    using Io = EpollIoContext;

    IoPool() = default;
    ~IoPool() { stop(); }

    IoPool(const IoPool&) = delete;
    IoPool& operator=(const IoPool&) = delete;
    IoPool(IoPool&&) = default;
    IoPool& operator=(IoPool&&) = default;

    /// Create and start the IoPool. This blocks the calling thread until
    /// all workers have initialized their resources.
    /// Must be called from outside the event loop (e.g., main thread).
    static std::unique_ptr<IoPool> create(const IoPoolConfig& config) {
        auto pool = std::make_unique<IoPool>();
        pool->config_ = config;
        pool->workers_.resize(config.num_workers);

        // Startup barrier: every worker reports exactly once — on success AND on
        // failure — so the main thread can never deadlock waiting on a worker
        // whose initialization threw.
        std::atomic<int> done_count{0};
        std::mutex ready_mutex;
        std::condition_variable ready_cv;

        for (int i = 0; i < config.num_workers; ++i) {
            auto& w = pool->workers_[i];
            w.io = std::make_unique<Io>();
            w.worker_id = i;

            w.thread = std::jthread([&w, &config, &done_count,
                                     &ready_mutex, &ready_cv, i]
                                    (std::stop_token stop) {
                // Pin to core
                if (config.pin_to_cores) {
                    int core = config.first_core + i;
                    cpu_set_t cpuset;
                    CPU_ZERO(&cpuset);
                    CPU_SET(core, &cpuset);
                    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
                }

                // Initialize resources on the event loop
                auto initTask = [&]() -> DetachedTask {
                    try {
                        // PG pool — acquire_timeout bounds the warm-up connect.
                        w.pg_pool = co_await PgPool<Io>::create(
                            *w.io, config.pg_conninfo,
                            {.min_connections = config.pg_min_conns_per_worker,
                             .max_connections = config.pg_max_conns_per_worker,
                             .acquire_timeout = config.acquire_timeout,
                             .query_timeout = config.query_timeout});

                        // Redis pool — query_timeout bounds the connect handshake.
                        if (!config.redis_unix_path.empty()) {
                            w.redis_pool = std::make_shared<RedisPool<Io>>(
                                co_await RedisPool<Io>::createUnix(
                                    *w.io, config.redis_unix_path.c_str(),
                                    config.redis_conns_per_worker,
                                    {.query_timeout = config.query_timeout}));
                        } else {
                            w.redis_pool = std::make_shared<RedisPool<Io>>(
                                co_await RedisPool<Io>::create(
                                    *w.io, config.redis_host.c_str(),
                                    config.redis_port,
                                    config.redis_conns_per_worker,
                                    {.query_timeout = config.query_timeout}));
                        }

                        // Batch scheduler — owned per worker (shared so the
                        // thread_local providers can co-own it).
                        w.batcher = std::make_shared<batch::BatchScheduler<Io>>(
                            *w.io, w.pg_pool, w.redis_pool,
                            config.max_concurrent_per_worker);

                        // Bind THIS worker thread's providers to its own batcher.
                        PgProvider::bindBatcher<Io>(w.batcher, w.redis_pool != nullptr);
                    } catch (...) {
                        // Capture the failure: a bare DetachedTask would swallow
                        // it silently. The main thread rethrows it after the
                        // barrier (fail-fast).
                        w.init_error = std::current_exception();
                    }

                    // Report done on BOTH paths — failure still advances the
                    // barrier, so create() wakes and surfaces the error instead
                    // of hanging. Increment AND notify under the lock: the woken
                    // main thread re-checks the predicate by re-acquiring this
                    // mutex, so it can neither miss the wakeup nor return and
                    // destroy ready_cv while this notify is still in flight (a
                    // one-shot startup barrier — the extra contention is moot).
                    {
                        std::lock_guard lock(ready_mutex);
                        done_count.fetch_add(1, std::memory_order_release);
                        ready_cv.notify_one();
                    }
                };

                initTask();

                // Run event loop until stop is requested
                w.io->runUntil([&stop, &w] {
                    return stop.stop_requested();
                });
            });
        }

        // Wait for every worker to report, bounded by startup_timeout so a
        // worker stuck in a connect that never returns (e.g. a blackholed Redis
        // at boot when query_timeout is unset) cannot freeze startup forever.
        bool all_done;
        {
            std::unique_lock lock(ready_mutex);
            all_done = ready_cv.wait_for(lock, config.startup_timeout, [&] {
                return done_count.load(std::memory_order_acquire) == config.num_workers;
            });
        }

        // Fail-fast on a stuck worker: join the workers (stop()) BEFORE
        // unwinding, while the barrier's mutex/cv are still alive — a worker
        // finishing its connect a hair after the timeout would otherwise touch a
        // destroyed primitive (these locals are torn down before `pool`).
        if (!all_done) {
            pool->stop();
            throw IoPoolStartupError("IoPool startup exceeded startup_timeout");
        }
        // Every worker reported; surface the first init failure. All workers are
        // now in their event loop (past the barrier), so unwinding `pool` to join
        // them via ~IoPool is safe.
        for (auto& w : pool->workers_) {
            if (w.init_error)
                std::rethrow_exception(w.init_error);
        }

        return pool;
    }

    /// Stop all workers.
    void stop() {
        for (auto& w : workers_) {
            if (w.io) w.io->stop();
        }
        for (auto& w : workers_) {
            if (w.thread.joinable()) {
                w.thread.request_stop();
                w.thread.join();
            }
        }
    }

    /// Get the number of workers.
    [[nodiscard]] int numWorkers() const noexcept {
        return static_cast<int>(workers_.size());
    }

    /// Access a worker's event loop (for testing).
    [[nodiscard]] Io& workerIo(int idx) noexcept { return *workers_[idx].io; }

private:
    struct Worker {
        std::unique_ptr<Io> io;
        std::shared_ptr<PgPool<Io>> pg_pool;
        std::shared_ptr<RedisPool<Io>> redis_pool;
        std::shared_ptr<batch::BatchScheduler<Io>> batcher;
        std::jthread thread;
        std::exception_ptr init_error;  // set if this worker's init threw
        int worker_id = 0;
    };

    IoPoolConfig config_;
    std::vector<Worker> workers_;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_POOL_H
