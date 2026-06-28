#ifndef JCX_RELAIS_IO_REDIS_POOL_H
#define JCX_RELAIS_IO_REDIS_POOL_H

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/redis/RedisClient.h"

namespace jcailloux::relais::io {

// Runtime Redis pool configuration. Unlike PgPool there is no acquire wait
// (round-robin over a fixed vector, no blocking acquire), so the only client-side
// bound is the per-operation I/O timeout, threaded to each RedisConnection's ctor,
// exactly like PgPoolConfig::query_timeout on the PG side.
//
//   - query_timeout bounds each Redis I/O wait. 0 = no client bound (discouraged —
//     a silent Redis blackhole then hangs the worker forever).
//
// Reconnection is configured on the pool, not here: create()/createUnix() know the
// address and self-derive a reconnect factory; fromClients() takes one explicitly
// (a typed std::function can't live in this address-agnostic struct without
// templating it onto Io and polluting every {.query_timeout=} call site).
struct RedisPoolConfig {
    std::chrono::milliseconds query_timeout{0};
};

// RedisPool — fixed-size pool of RedisClient instances with round-robin dispatch.
//
// Each RedisClient has its own connection and coroutine mutex. The pool
// distributes requests across connections via an atomic counter (zero contention
// on the counter itself, rare mutex collision per-client with round-robin).

template<IoContext Io>
class RedisPool {
public:
    /// Builds a fresh client for a dead slot off the hot path. Returns a new,
    /// connected client. May be empty (opaque construction) — then the pool cannot
    /// self-heal and reviveDeadClients() is a no-op.
    using ReconnectFactory =
        std::function<Task<std::shared_ptr<RedisClient<Io>>>(size_t idx)>;

    RedisPool() noexcept = default;

    RedisPool(RedisPool&& o) noexcept
        : clients_(std::move(o.clients_))
        , reconnect_factory_(std::move(o.reconnect_factory_))
        , counter_(o.counter_.load(std::memory_order_relaxed))
    {}

    RedisPool& operator=(RedisPool&& o) noexcept {
        if (this != &o) {
            clients_ = std::move(o.clients_);
            reconnect_factory_ = std::move(o.reconnect_factory_);
            counter_.store(o.counter_.load(std::memory_order_relaxed),
                           std::memory_order_relaxed);
        }
        return *this;
    }

    RedisPool(const RedisPool&) = delete;
    RedisPool& operator=(const RedisPool&) = delete;

    /// Create a pool from existing clients (no new connections). Pass a reconnect
    /// factory to enable self-heal after a Redis outage; without one (opaque
    /// pre-connected clients) the pool cannot rebuild dead slots and degrades on
    /// l1_ttl — a deployment choice, not a hang.
    static RedisPool fromClients(
        std::vector<std::shared_ptr<RedisClient<Io>>> clients,
        ReconnectFactory reconnect_factory = {})
    {
        RedisPool pool;
        pool.clients_ = std::move(clients);
        pool.reconnect_factory_ = std::move(reconnect_factory);
        return pool;
    }

    /// Create a pool with `size` TCP connections.
    static Task<RedisPool> create(
        Io& io,
        const char* host = "127.0.0.1",
        int port = 6379,
        size_t size = 4,
        RedisPoolConfig cfg = {})
    {
        RedisPool pool;
        pool.clients_.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            // ms → ns is an exact widening (implicit). query_timeout is stored on
            // each connection for its I/O waits.
            auto client = co_await RedisClient<Io>::connect(
                io, host, port, cfg.query_timeout);
            pool.clients_.push_back(std::move(client));
        }
        // Self-derived reconnect factory: create() knows the address, so the pool
        // rebuilds any dead slot off the hot path with nothing more from the caller.
        // host is copied — the const char* may not outlive the pool.
        pool.reconnect_factory_ =
            [io_ptr = &io, host = std::string(host), port, qt = cfg.query_timeout]
            (size_t) -> Task<std::shared_ptr<RedisClient<Io>>> {
                co_return co_await RedisClient<Io>::connect(
                    *io_ptr, host.c_str(), port, qt);
            };
        co_return std::move(pool);
    }

    /// Create a pool with `size` Unix socket connections.
    static Task<RedisPool> createUnix(
        Io& io,
        const char* path = "/var/run/redis/redis-server.sock",
        size_t size = 4,
        RedisPoolConfig cfg = {})
    {
        RedisPool pool;
        pool.clients_.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            auto client = co_await RedisClient<Io>::connectUnix(
                io, path, cfg.query_timeout);
            pool.clients_.push_back(std::move(client));
        }
        // Self-derived reconnect factory (Unix path copied, see create()).
        pool.reconnect_factory_ =
            [io_ptr = &io, path = std::string(path), qt = cfg.query_timeout]
            (size_t) -> Task<std::shared_ptr<RedisClient<Io>>> {
                co_return co_await RedisClient<Io>::connectUnix(
                    *io_ptr, path.c_str(), qt);
            };
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

    /// Whether this pool can rebuild a dead connection (has a reconnect factory).
    [[nodiscard]] bool canSelfHeal() const noexcept {
        return static_cast<bool>(reconnect_factory_);
    }

    /// Recreate poisoned connection slots off the hot path. For each client whose
    /// connection is dead (connected() == false: a silent hang flagged by the
    /// watch-bound timeout, or an fd closed by RST), co_awaits the reconnect
    /// factory and swaps a fresh client into its place. Returns the number revived.
    ///
    /// Without a factory (opaque fromClients construction) it is a no-op returning
    /// 0; the caller then degrades on l1_ttl + log rather than converging.
    ///
    /// Cold path only — never from next()/exec, so steady-state traffic pays
    /// nothing. The replacement is built into a local first, then swapped only if
    /// the dead client is QUIESCENT — no lock holder, no queued waiter, i.e. no
    /// coroutine suspended inside it. This is the safety gate: a poisoned client can
    /// still host a suspended frame (its timeout resume not yet drained, or queued
    /// lock waiters whose resumes the unwinding holder will post), and the reconnect
    /// can complete SYNCHRONOUSLY (a Unix connect() returning 0 never yields), so a
    /// blind swap could destroy a connection out from under a live frame. The gate
    /// is re-checked AFTER the connect (a user may have grabbed the dead slot while
    /// it ran); a slot still busy then is left for a later pass and the freshly-built
    /// client drops safely (idle, no armed timer). A reconnect that itself fails
    /// (Redis not back) leaves the slot dead for the next pass; its handshake is
    /// watch-bound, so this cannot hang.
    ///
    /// Postcondition: revives only the dead slots that are quiescent. A dead slot
    /// still draining a suspended frame is deferred, and under traffic that keeps
    /// dispatching to it (next() is dispatch-blind) it can stay non-quiescent across
    /// passes — so a revived count below the dead count is a structural wait, not a
    /// transient error. Convergence requires the caller to also steer dispatch away
    /// from dead slots; on its own this method does not guarantee progress.
    Task<size_t> reviveDeadClients() {
        size_t revived = 0;
        if (!reconnect_factory_)
            co_return revived;
        for (size_t i = 0; i < clients_.size(); ++i) {
            if (clients_[i] && clients_[i]->connected())
                continue;
            // Pre-gate: a dead slot still draining a suspended frame can't be swapped
            // yet — skip without burning a connect (revisited on a later pass).
            if (clients_[i] && !clients_[i]->isQuiescent())
                continue;
            std::shared_ptr<RedisClient<Io>> fresh;
            try {
                fresh = co_await reconnect_factory_(i);
            } catch (const std::exception& e) {
                RELAIS_LOG_WARN << "RedisPool: reconnect of dead slot " << i
                                << " failed - " << e.what();
                continue;
            }
            // Re-gate after the connect: it may have yielded, letting a user grab the
            // dead slot. Mandatory (the pre-gate is only an optimization) — never
            // destroy a client with a suspended frame. Drop `fresh`, retry later.
            if (clients_[i] && !clients_[i]->isQuiescent())
                continue;
            clients_[i] = std::move(fresh);
            ++revived;
        }
        co_return revived;
    }

private:
    std::vector<std::shared_ptr<RedisClient<Io>>> clients_;
    ReconnectFactory reconnect_factory_;
    std::atomic<uint32_t> counter_{0};
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_POOL_H
