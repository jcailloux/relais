#ifndef JCX_RELAIS_IO_PG_POOL_H
#define JCX_RELAIS_IO_PG_POOL_H

#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/pg/PgConnection.h"
#include "jcailloux/relais/io/pg/PgError.h"

namespace jcailloux::relais::io {

// Runtime pool configuration — deployment tuning, NOT the compile-time CacheConfig
// NTTP. Struct + designated initializers so fields grow without breaking the
// positional order at call sites.
//
// Separation of concerns:
//   - conninfo carries what libpq/the server enforce (connect_timeout, keepalives,
//     statement_timeout).
//   - PgPoolConfig carries what only relais can guarantee client-side:
//     - acquire_timeout bounds acquire(): the queue wait (Waiter timer, §6.2 a)
//       and, from a later step, the connect handshake (§6.2 b). 0 = unbounded
//       (discouraged — reintroduces the silent deadlock this config prevents).
//     - query_timeout bounds each per-connection I/O wait (wired into PgConnection
//       in a later step). 0 = delegated to the server's statement_timeout.
struct PgPoolConfig {
    size_t min_connections = 2;
    size_t max_connections = 16;
    std::chrono::milliseconds acquire_timeout{5000};
    std::chrono::milliseconds query_timeout{0};
};

// PgPool — bounded connection pool with coroutine wait queue

template<IoContext Io>
class PgPool : public std::enable_shared_from_this<PgPool<Io>> {
public:
    using ConnectionType = PgConnection<Io>;

    // ConnectionGuard — RAII: returns connection to pool on destruction

    class ConnectionGuard {
    public:
        ConnectionGuard() noexcept = default;

        ConnectionGuard(std::shared_ptr<PgPool> pool, ConnectionType conn) noexcept
            : pool_(std::move(pool)), conn_(std::move(conn)) {}

        ~ConnectionGuard() {
            if (pool_ && conn_)
                pool_->release(std::move(*conn_));
        }

        ConnectionGuard(ConnectionGuard&& o) noexcept
            : pool_(std::move(o.pool_)), conn_(std::move(o.conn_))
        {
            o.conn_.reset();
        }

        ConnectionGuard& operator=(ConnectionGuard&& o) noexcept {
            if (this != &o) {
                if (pool_ && conn_) pool_->release(std::move(*conn_));
                pool_ = std::move(o.pool_);
                conn_ = std::move(o.conn_);
                o.conn_.reset();
            }
            return *this;
        }
        ConnectionGuard(const ConnectionGuard&) = delete;
        ConnectionGuard& operator=(const ConnectionGuard&) = delete;

        [[nodiscard]] ConnectionType& conn() noexcept { return *conn_; }
        [[nodiscard]] const ConnectionType& conn() const noexcept { return *conn_; }

    private:
        std::shared_ptr<PgPool> pool_;
        std::optional<ConnectionType> conn_;
    };

    // Factory: create pool with initial connections.
    //
    // First line of defence against silent I/O hangs is the conninfo string —
    // strongly recommended (these bound liveness without any client-side timer):
    //   - connect_timeout=N         bounds the handshake (libpq, native).
    //   - keepalives=1 keepalives_idle=.. keepalives_interval=.. keepalives_count=..
    //                               bounds silent network death at the TCP layer.
    //   - statement_timeout=..      the server kills an over-long query (lock/scan).
    // These cover the cases where the server is reachable and/or the TCP stack
    // cooperates. The deterministic, stack-independent bound is the client-side
    // timeout (PgPoolConfig::acquire_timeout/query_timeout).
    static Task<std::shared_ptr<PgPool>> create(
        Io& io,
        std::string conninfo,
        PgPoolConfig cfg = {}
    ) {
        auto pool = std::shared_ptr<PgPool>(
            new PgPool(io, std::move(conninfo), cfg));

        for (size_t i = 0; i < cfg.min_connections; ++i) {
            auto conn = co_await ConnectionType::connect(pool->io_, pool->conninfo_.c_str());
            pool->idle_.push_back(std::move(conn));
            ++pool->total_;
        }

        co_return pool;
    }

    // Acquire a connection (may suspend if pool exhausted)

    Task<ConnectionGuard> acquire() {
        if (!idle_.empty()) {
            auto conn = std::move(idle_.back());
            idle_.pop_back();
            co_return ConnectionGuard(this->shared_from_this(), std::move(conn));
        }

        if (total_ < cfg_.max_connections) {
            // PGLIVE-2: ++total_ must be undone if connect throws. The
            // ConnectionGuard — the sole owner of the release → --total_ path — is
            // built only on success below, so a failed connect (a future
            // PgPoolTimeout on connect, or a plain PgConnectionError today) would
            // otherwise leak the slot permanently; after max_connections failures
            // `total_ < max_connections` is false forever and the pool freezes.
            // The scope guard decrements on any exception out of co_await connect.
            ++total_;
            struct SlotGuard {
                size_t* total;
                bool committed = false;
                ~SlotGuard() { if (!committed) --*total; }
            } slot{&total_};
            auto conn = co_await ConnectionType::connect(io_, conninfo_.c_str());
            slot.committed = true;
            co_return ConnectionGuard(this->shared_from_this(), std::move(conn));
        }

        Waiter waiter{this};
        co_return co_await waiter;
    }

private:
    // Forward-declared: onWaiterTimeout() names Waiter as a parameter type, and a
    // parameter type is not in complete-class context (unlike a member body), so
    // it must be visible before that declaration.
    struct Waiter;

    PgPool(Io& io, std::string conninfo, PgPoolConfig cfg)
        : io_(io)
        , conninfo_(std::move(conninfo))
        , cfg_(cfg)
    {}

    void release(ConnectionType conn) {
        if (!waiters_.empty()) {
            // Validate connection before handing to a waiter
            if (!conn.connected()) {
                --total_;
                // Waiter stays queued — next release() will serve it,
                // or acquire() will create a new connection.
                return;
            }
            auto* waiter = waiters_.front();
            waiters_.pop_front();
            // Win the acquire_timeout race (§6.2 a): cancel the waiter's timer
            // before posting its resume. Single loop thread, so once cancelled the
            // expiry callback (onWaiterTimeout) cannot run. No-op if unarmed
            // (acquire_timeout == 0).
            if (waiter->armed_) io_.cancelTimer(waiter->timer_);
            waiter->conn.emplace(std::move(conn));
            io_.post([h = waiter->continuation] { h.resume(); });
            return;
        }

        if (conn.connected()) {
            idle_.push_back(std::move(conn));
        } else {
            --total_;
        }
    }

    // acquire_timeout expiry for a queued Waiter (§6.2 a). Single loop thread, so
    // this and release() cannot interleave: whichever runs first removes the
    // waiter — release pops it and cancels this timer; this erases it from the
    // queue — and the other finds nothing to do. The resume is posted, never run
    // inline, to keep the unwind out of fireExpiredTimers (§6.1/§6.3 rationale).
    void onWaiterTimeout(Waiter* w) {
        for (auto it = waiters_.begin(); it != waiters_.end(); ++it) {
            if (*it == w) {
                waiters_.erase(it);  // linear — the wait queue is short
                w->timed_out_ = true;
                io_.post([h = w->continuation] { h.resume(); });
                return;
            }
        }
        // Not found: release() already served this waiter and cancelled the
        // timer — this callback should not even run; defensive no-op.
    }

    struct Waiter {
        PgPool* pool;
        std::optional<ConnectionType> conn;
        std::coroutine_handle<> continuation;
        typename Io::TimerToken timer_{};
        bool armed_ = false;      // timer_ holds a live token
        bool timed_out_ = false;  // acquire_timeout fired before a connection arrived

        bool await_ready() const noexcept { return false; }

        // noexcept preserved: under OOM both postDelayed and waiters_.push_back may
        // throw bad_alloc, which terminates — the same fatal-allocation contract as
        // the original push_back-only body, and it avoids any half-armed/half-queued
        // intermediate state.
        void await_suspend(std::coroutine_handle<> h) noexcept {
            continuation = h;
            pool->waiters_.push_back(this);
            // Bound the queue wait by acquire_timeout (§6.2 a). 0 = unbounded.
            // The timer callback captures the raw `self`; it stays valid because a
            // queued acquire() frame is never destroyed while suspended here — the
            // same invariant the waiters_-stored `continuation` already relies on.
            // The frame is torn down only after a posted resume, which both
            // release() (timer cancelled) and onWaiterTimeout() (timer consumed)
            // emit strictly before the timer could dangle. A future cancellable
            // acquire() caller would have to disarm on cancellation.
            auto to = pool->cfg_.acquire_timeout;
            if (to.count() > 0) {
                timer_ = pool->io_.postDelayed(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(to),
                    [pool = pool, self = this] { pool->onWaiterTimeout(self); });
                armed_ = true;
            }
        }

        ConnectionGuard await_resume() {
            if (timed_out_)
                throw PgPoolTimeout("queue wait exceeded acquire_timeout");
            assert(conn.has_value());
            return ConnectionGuard(pool->shared_from_this(), std::move(*conn));
        }
    };

    Io& io_;
    std::string conninfo_;
    PgPoolConfig cfg_;
    size_t total_ = 0;

    std::vector<ConnectionType> idle_;
    std::deque<Waiter*> waiters_;
};

// PgClient — high-level query interface using a PgPool

template<IoContext Io>
class PgClient {
public:
    using Pool = PgPool<Io>;

    explicit PgClient(std::shared_ptr<Pool> pool) noexcept
        : pool_(std::move(pool)) {}

    Task<PgResult> query(const char* sql) {
        auto guard = co_await pool_->acquire();
        co_return co_await guard.conn().query(sql);
    }

    Task<PgResult> queryParams(const char* sql, const PgParams& params) {
        auto guard = co_await pool_->acquire();
        co_return co_await guard.conn().queryParams(sql, params);
    }

    template<typename... Args>
    Task<PgResult> queryArgs(const char* sql, Args&&... args) {
        auto params = PgParams::make(std::forward<Args>(args)...);
        co_return co_await queryParams(sql, params);
    }

    Task<int> execute(const char* sql, const PgParams& params) {
        auto guard = co_await pool_->acquire();
        co_return co_await guard.conn().execute(sql, params);
    }

    template<typename... Args>
    Task<int> executeArgs(const char* sql, Args&&... args) {
        auto params = PgParams::make(std::forward<Args>(args)...);
        co_return co_await execute(sql, params);
    }

    [[nodiscard]] Pool& pool() noexcept { return *pool_; }
    [[nodiscard]] const Pool& pool() const noexcept { return *pool_; }

private:
    std::shared_ptr<Pool> pool_;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_POOL_H
