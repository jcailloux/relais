#ifndef JCX_RELAIS_IO_POOL_H
#define JCX_RELAIS_IO_POOL_H

#include <chrono>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <pthread.h>
#include <sched.h>

#include "jcailloux/relais/io/EpollIoContext.h"
#include "jcailloux/relais/io/pg/PgPool.h"
#include "jcailloux/relais/runtime/Attach.h"

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

    // PG pool sizing per worker
    size_t pg_min_conns_per_worker = 2;
    size_t pg_max_conns_per_worker = 8;

    // PG sends in flight per worker (reads and writes together)
    int max_concurrent_per_worker = 8;

    // Liveness timeouts threaded into the per-worker pools.
    //   - acquire_timeout bounds each PG acquire, including the warm-up connect
    //     at boot, so an unreachable PostgreSQL fails startup instead of hanging.
    //   - query_timeout bounds each per-connection I/O wait. A generous liveness
    //     backstop, not an SLA: it bounds a silently blackholed socket, which
    //     statement_timeout misses. 0 disables it (discouraged); set tighter for
    //     an SLA.
    //   - startup_timeout bounds the wait for all workers to report ready, so a
    //     worker stuck in a connect that never returns cannot freeze create().
    std::chrono::milliseconds acquire_timeout{5000};
    std::chrono::milliseconds query_timeout{30000};
    std::chrono::milliseconds startup_timeout{30000};

    // How an uncertain write is settled; see PgPoolConfig.
    UncertainSettle uncertain_settle = UncertainSettle::Terminate;
    std::chrono::milliseconds settle_delay{0};
    // Sent as each session's client_connection_check_interval; see PgPoolConfig.
    std::chrono::milliseconds client_check_interval{1000};

    // Core pinning
    bool pin_to_cores = true;
    int first_core = 1;  // Avoid core 0 (OS/IRQ)
};

// IoPool — N event loops pinned on N cores, each with its own resources.
//
// Each worker owns an EpollIoContext run by its own std::jthread, to which
// create() binds relais as attach() does: the loop is checked, a PgPool and a
// BatchScheduler are created on it, and the worker thread's providers are bound
// to them. A coroutine running on a worker thus routes to that worker's
// resources with no cross-thread hop (shared-nothing).

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

        for (int i = 0; i < config.num_workers; ++i) {
            auto& w = pool->workers_[i];
            w.io = std::make_unique<Io>();
            w.thread = std::jthread([io = w.io.get(), pin = config.pin_to_cores,
                                     core = config.first_core + i](std::stop_token stop) {
                if (pin) {
                    cpu_set_t cpuset;
                    CPU_ZERO(&cpuset);
                    CPU_SET(core, &cpuset);
                    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
                }
                io->runUntil([&stop] { return stop.stop_requested(); });
            });
        }

        // Every worker at once: checked, then booted, each wait bounded.
        const AttachConfig attach_cfg{
            .pool = {.min_connections = config.pg_min_conns_per_worker,
                     .max_connections = config.pg_max_conns_per_worker,
                     .acquire_timeout = config.acquire_timeout,
                     .query_timeout = config.query_timeout,
                     .uncertain_settle = config.uncertain_settle,
                     .settle_delay = config.settle_delay,
                     .client_check_interval = config.client_check_interval},
            .max_concurrent = config.max_concurrent_per_worker};
        const auto deadline = std::chrono::steady_clock::now() + config.startup_timeout;
        for (auto& w : pool->workers_)
            w.binding = relais::detail::LoopBinding<Io>::start(*w.io, config.pg_conninfo, attach_cfg);

        std::exception_ptr error;
        for (auto& w : pool->workers_) {
            try {
                w.binding->awaitCheck();
            } catch (...) {
                if (!error) error = std::current_exception();
            }
        }
        bool timed_out = false;
        if (!error) {
            for (auto& w : pool->workers_) w.binding->startBoot();
            for (auto& w : pool->workers_) {
                try {
                    // acquire_timeout bounds each connect; startup_timeout
                    // bounds them all, a connect with no bound included.
                    if (!w.binding->awaitBoot(deadline)) timed_out = true;
                } catch (...) {
                    if (!error) error = std::current_exception();
                }
            }
        }

        // Fail fast: the teardown ends a boot still connecting, on its loop.
        if (timed_out || error) {
            pool->stop();
            if (timed_out) throw IoPoolStartupError("IoPool startup exceeded startup_timeout");
            std::rethrow_exception(error);
        }
        return pool;
    }

    /// Stop all workers: relais leaves every loop at once, each waiting for the
    /// requests in flight on it, then the loops stop.
    void stop() {
        for (auto& w : workers_) {
            if (w.binding) w.binding->beginRelease();
        }
        for (auto& w : workers_) {
            if (w.binding) (void)w.binding->awaitRelease();
            w.binding.reset();
        }
        for (auto& w : workers_) {
            if (w.thread.joinable()) {
                w.thread.request_stop();
                w.io->stop();  // wakes the loop to see it
            }
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
        std::shared_ptr<relais::detail::LoopBinding<Io>> binding;
        std::jthread thread;
    };

    IoPoolConfig config_;
    std::vector<Worker> workers_;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_POOL_H
