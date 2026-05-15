#ifndef JCX_RELAIS_IO_REDIS_POOL_H
#define JCX_RELAIS_IO_REDIS_POOL_H

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/redis/RedisClient.h"

namespace jcailloux::relais::io {

// RedisPool — fixed-size pool of RedisClient instances with round-robin dispatch.
//
// Each RedisClient has its own connection and coroutine mutex. The pool
// distributes requests across connections via an atomic counter (zero contention
// on the counter itself, rare mutex collision per-client with round-robin).

template<IoContext Io>
class RedisPool {
public:
    RedisPool() noexcept = default;

    RedisPool(RedisPool&& o) noexcept
        : clients_(std::move(o.clients_))
        , counter_(o.counter_.load(std::memory_order_relaxed))
    {}

    RedisPool& operator=(RedisPool&& o) noexcept {
        if (this != &o) {
            clients_ = std::move(o.clients_);
            counter_.store(o.counter_.load(std::memory_order_relaxed),
                           std::memory_order_relaxed);
        }
        return *this;
    }

    RedisPool(const RedisPool&) = delete;
    RedisPool& operator=(const RedisPool&) = delete;

    /// Create a pool from existing clients (no new connections).
    static RedisPool fromClients(std::vector<std::shared_ptr<RedisClient<Io>>> clients) {
        RedisPool pool;
        pool.clients_ = std::move(clients);
        return pool;
    }

    /// Create a pool with `size` TCP connections.
    static Task<RedisPool> create(
        Io& io,
        const char* host = "127.0.0.1",
        int port = 6379,
        size_t size = 4)
    {
        RedisPool pool;
        pool.clients_.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            auto client = co_await RedisClient<Io>::connect(io, host, port);
            pool.clients_.push_back(std::move(client));
        }
        co_return std::move(pool);
    }

    /// Create a pool with `size` Unix socket connections.
    static Task<RedisPool> createUnix(
        Io& io,
        const char* path = "/var/run/redis/redis-server.sock",
        size_t size = 4)
    {
        RedisPool pool;
        pool.clients_.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            auto client = co_await RedisClient<Io>::connectUnix(io, path);
            pool.clients_.push_back(std::move(client));
        }
        co_return std::move(pool);
    }

    /// Get the next client via round-robin. Thread-safe (atomic counter).
    [[nodiscard]] RedisClient<Io>& next() noexcept {
        auto idx = counter_.fetch_add(1, std::memory_order_relaxed) % clients_.size();
        return *clients_[idx];
    }

    /// Get a client by explicit index.
    [[nodiscard]] RedisClient<Io>& at(size_t idx) noexcept {
        return *clients_[idx % clients_.size()];
    }

    [[nodiscard]] size_t size() const noexcept { return clients_.size(); }
    [[nodiscard]] bool empty() const noexcept { return clients_.empty(); }

private:
    std::vector<std::shared_ptr<RedisClient<Io>>> clients_;
    std::atomic<uint32_t> counter_{0};
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_POOL_H
