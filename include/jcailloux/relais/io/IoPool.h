#ifndef JCX_RELAIS_IO_POOL_H
#define JCX_RELAIS_IO_POOL_H

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
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

// BootTask — an eager, owned coroutine for per-worker startup.
//
// Unlike DetachedTask (which self-destroys at final_suspend and hands back no
// handle), BootTask starts immediately, parks its frame at final_suspend, and
// gives the handle to its owner. That ownership is what lets fail-fast teardown
// reclaim a boot that is still suspended on a connect which never completed
// (a dependency blackholed at boot, past startup_timeout): destroying the frame
// runs its in-frame connection destructors — PgConnection/RedisConnection both
// cancel the watch-bound timer, removeWatch, and close/PQfinish the fd — so
// neither the coroutine frame nor its socket fd leaks. With a self-destroying
// DetachedTask the suspended frame and its fd would be abandoned, leaking one fd
// per failed startup; a service that retries create() in a loop would exhaust
// them.
//
// Destroy-only contract: the owner destroys the frame ONLY after the worker
// thread is joined (nothing is mid-resume) and BEFORE the worker's IoContext is
// torn down (removeWatch still reaches a live loop). The frame is never resumed
// after the thread stops, so a boot suspended inside the startup lambda never
// dereferences the (by then destroyed) lambda closure — destruction only runs
// the frame's local destructors.
struct BootTask {
    struct promise_type {
        BootTask get_return_object() noexcept {
            return BootTask{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept {}  // boot body try/catches; unreached
    };

    std::coroutine_handle<promise_type> handle{};

    BootTask() noexcept = default;
    explicit BootTask(std::coroutine_handle<promise_type> h) noexcept : handle(h) {}
    BootTask(BootTask&& o) noexcept : handle(std::exchange(o.handle, {})) {}
    BootTask& operator=(BootTask&& o) noexcept {
        if (this != &o) {
            if (handle) handle.destroy();
            handle = std::exchange(o.handle, {});
        }
        return *this;
    }
    BootTask(const BootTask&) = delete;
    BootTask& operator=(const BootTask&) = delete;
    ~BootTask() { if (handle) handle.destroy(); }
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

        // Drain window for ordered teardown (phase 2 in each worker). Sized to let
        // a single fire blocked on a bounded query/acquire time out and
        // self-destruct, so the common case — a handful of in-flight fires —
        // drains with no leak. It is NOT a hard ceiling on total drain time: fires
        // queued behind a saturated pool against a blackholed server time out
        // serially (~ceil(N/conns) x query_timeout), and a 0 (unbounded) timeout
        // never times out at all. Past this window the pump gives up and the
        // still-suspended frame(s) are abandoned — a bounded, best-effort residual,
        // far better than before ordered teardown (which abandoned every in-flight
        // fire unconditionally), and no finite grace can cover the serial cascade.
        // Captured BY VALUE into each worker: a temporary config argument must not
        // be read at teardown, long after create() has returned.
        const auto teardown_grace =
            std::max(config.acquire_timeout, config.query_timeout)
            + std::chrono::milliseconds(500);

        for (int i = 0; i < config.num_workers; ++i) {
            auto& w = pool->workers_[i];
            w.io = std::make_unique<Io>();
            w.worker_id = i;

            w.thread = std::jthread([&w, &config, &done_count,
                                     &ready_mutex, &ready_cv, i, teardown_grace]
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
                auto initTask = [&]() -> BootTask {
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

                // Own the boot frame: on fail-fast, ~Worker destroys a frame
                // still suspended on a blackholed connect instead of leaking it
                // (frame + socket fd). Eager start runs the body to its first
                // suspend before returning.
                w.init_task = initTask();

                // Phase 1: serve until stop is requested.
                w.io->runUntil([&stop] { return stop.stop_requested(); });

                // Phase 2: ordered teardown. A detached fire or self-heal drain
                // still suspended on a blackholed dependency would otherwise leak
                // its frame — and, for a fire, the pool via its in-frame
                // ConnectionGuard. Stop the self-heal queue re-arming, then pump
                // the loop until every detached coroutine has completed (each is
                // bounded by the per-operation query/acquire timeouts: a hung fire
                // times out, unwinds in error, and self-destroys) or the grace
                // window elapses (the fallback when a timeout is 0 or the serial
                // drain outruns the window — the residual frame is then abandoned,
                // bounded best-effort; see teardown_grace). The join in stop()
                // blocks on this, so teardown waits for the drain rather than
                // tearing the loop out from under it.
                if (w.batcher) w.batcher->beginShutdown();
                auto drain_deadline = Io::Clock::now() + teardown_grace;
                w.io->runUntil([&w, drain_deadline] {
                    return (!w.batcher || w.batcher->quiescentForTeardown())
                           || Io::Clock::now() >= drain_deadline;
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
        // Request stop on every worker first, then join — so the per-worker
        // ordered-teardown drains (phase 2) overlap instead of serializing into
        // N x teardown_grace.
        for (auto& w : workers_) {
            if (w.thread.joinable()) w.thread.request_stop();
        }
        for (auto& w : workers_) {
            if (w.thread.joinable()) w.thread.join();
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
        // Owns the startup coroutine frame. Declared right after `io` so member
        // destruction (reverse order) reclaims it BEFORE `io`: a boot still
        // suspended on a connect at fail-fast is destroyed while its loop is
        // alive, so its connection dtors' removeWatch lands on a live IoContext.
        BootTask init_task;
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
