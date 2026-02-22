#ifndef JCX_RELAIS_IO_POOL_H
#define JCX_RELAIS_IO_POOL_H

#include <atomic>
#include <cstdint>
#include <memory>
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
#include "jcailloux/relais/DbProvider.h"

namespace jcailloux::relais::io {

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
// The IoPool configures DbProvider with thread_local dispatch so that
// coroutines running on a worker thread automatically route to that
// worker's BatchScheduler.

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

        // Synchronization: each worker signals when initialized
        std::atomic<int> ready_count{0};
        std::mutex ready_mutex;
        std::condition_variable ready_cv;

        for (int i = 0; i < config.num_workers; ++i) {
            auto& w = pool->workers_[i];
            w.io = std::make_unique<Io>();
            w.worker_id = i;

            w.thread = std::jthread([&pool, &w, &config, &ready_count,
                                     &ready_mutex, &ready_cv, i]
                                    (std::stop_token stop) {
                // Set thread_local worker ID
                tl_worker_id_ = i;
                tl_pool_ = pool.get();

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
                    // PG pool
                    w.pg_pool = co_await PgPool<Io>::create(
                        *w.io, config.pg_conninfo,
                        config.pg_min_conns_per_worker,
                        config.pg_max_conns_per_worker);

                    // Redis pool
                    if (!config.redis_unix_path.empty()) {
                        w.redis_pool = std::make_shared<RedisPool<Io>>(
                            co_await RedisPool<Io>::createUnix(
                                *w.io, config.redis_unix_path.c_str(),
                                config.redis_conns_per_worker));
                    } else {
                        w.redis_pool = std::make_shared<RedisPool<Io>>(
                            co_await RedisPool<Io>::create(
                                *w.io, config.redis_host.c_str(),
                                config.redis_port,
                                config.redis_conns_per_worker));
                    }

                    // Batch scheduler
                    w.batcher = std::make_unique<batch::BatchScheduler<Io>>(
                        *w.io, w.pg_pool, w.redis_pool,
                        config.max_concurrent_per_worker);

                    // Signal ready
                    ready_count.fetch_add(1, std::memory_order_release);
                    ready_cv.notify_one();
                };

                initTask();

                // Run event loop until stop is requested
                w.io->runUntil([&stop, &w] {
                    return stop.stop_requested();
                });
            });
        }

        // Wait for all workers to be initialized
        {
            std::unique_lock lock(ready_mutex);
            ready_cv.wait(lock, [&] {
                return ready_count.load(std::memory_order_acquire) == config.num_workers;
            });
        }

        return pool;
    }

    /// Register this IoPool as the DbProvider backend.
    /// After this call, DbProvider::queryParams() etc. route through the
    /// BatchScheduler of the calling thread's worker.
    void registerAsProvider() {
        // PG query (simple)
        DbProvider::pg_query_ = [this](const char* sql) -> Task<PgResult> {
            auto* batcher = getBatcher();
            if (batcher) {
                co_return co_await batcher->directQuery(sql);
            }
            // Fallback: post to worker 0
            // TODO: implement cross-thread dispatch
            co_return co_await workers_[0].batcher->directQuery(sql);
        };

        // PG queryParams
        DbProvider::pg_query_params_ = [this](const char* sql,
                                               const PgParams& params)
            -> Task<PgResult>
        {
            auto* batcher = getBatcher();
            if (batcher) {
                co_return co_await batcher->submitQueryRead(sql, PgParams{params});
            }
            co_return co_await workers_[0].batcher->submitQueryRead(sql, PgParams{params});
        };

        // PG execute
        DbProvider::pg_execute_ = [this](const char* sql,
                                          const PgParams& params)
            -> Task<std::pair<int, bool>>
        {
            auto* batcher = getBatcher();
            if (batcher) {
                co_return co_await batcher->submitPgExecute(sql, PgParams{params});
            }
            co_return co_await workers_[0].batcher->submitPgExecute(sql, PgParams{params});
        };

        // Redis
        DbProvider::redis_exec_ = [this](int argc, const char** argv,
                                          const size_t* argvlen)
            -> Task<RedisResult>
        {
            auto* batcher = getBatcher();
            if (batcher) {
                co_return co_await batcher->submitRedis(argc, argv, argvlen);
            }
            co_return co_await workers_[0].batcher->submitRedis(argc, argv, argvlen);
        };
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
        std::unique_ptr<batch::BatchScheduler<Io>> batcher;
        std::jthread thread;
        int worker_id = 0;
    };

    /// Get the BatchScheduler for the current thread (nullptr if not a worker thread).
    [[nodiscard]] batch::BatchScheduler<Io>* getBatcher() const noexcept {
        if (tl_pool_ != this) return nullptr;
        int id = tl_worker_id_;
        if (id < 0 || id >= static_cast<int>(workers_.size())) return nullptr;
        return workers_[id].batcher.get();
    }

    IoPoolConfig config_;
    std::vector<Worker> workers_;

    static inline thread_local int tl_worker_id_ = -1;
    static inline thread_local IoPool* tl_pool_ = nullptr;

    // DbProvider needs access to the static function members
    friend class jcailloux::relais::DbProvider;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_POOL_H
