#ifndef JCX_RELAIS_IO_PG_POOL_H
#define JCX_RELAIS_IO_PG_POOL_H

#include <algorithm>
#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "jcailloux/relais/io/DeadlineQueue.h"
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
//     - acquire_timeout bounds acquire(): the queue wait (Waiter timer) and the
//       connect handshake. 0 = unbounded
//       (discouraged — reintroduces the silent deadlock this config prevents).
//     - query_timeout bounds each per-connection I/O wait. A generous liveness
//       backstop (not an SLA): it bounds a blackholed socket that statement_timeout
//       misses. 0 delegates entirely to the server (discouraged); set tighter for an SLA.
//     - uncertain_settle decides how a write that failed uncertainly (timeout or
//       lost connection after sending) is settled: Terminate stops its backend,
//       Await waits for it to end, Delay waits settle_delay (behind a transaction
//       pooler, where the backend is not stable; settle_delay must exceed the
//       server's statement_timeout, and 0 takes that timeout plus a margin), Off
//       settles nothing. Settling runs over a connection of its own, outside the
//       pool, with the same conninfo.
//     - client_check_interval is set as the session's client_connection_check_interval
//       (PostgreSQL 14+, Linux server): the server aborts a statement whose client
//       has closed its socket, often before settling gets to it. Never sent under
//       Delay, where a session setting would leak through the pooler to other
//       clients; ignored by a server that refuses it. 0 sends nothing.
struct PgPoolConfig {
    size_t min_connections = 2;
    size_t max_connections = 16;
    std::chrono::milliseconds acquire_timeout{5000};
    std::chrono::milliseconds query_timeout{30000};
    UncertainSettle uncertain_settle = UncertainSettle::Terminate;
    std::chrono::milliseconds settle_delay{0};
    std::chrono::milliseconds client_check_interval{1000};
};

// PgPool — bounded connection pool with coroutine wait queue
//
// acquire_timeout and query_timeout bound operations through two deadline
// queues the pool's connections share (DeadlineQueue): one loop timer each for
// the whole pool, none armed or cancelled per request.

template<IoContext Io>
class PgPool : public std::enable_shared_from_this<PgPool<Io>> {
public:
    using ConnectionType = detail::PgConnection<Io>;

    // ConnectionGuard — RAII: lends a connection of the pool, returns it on
    // destruction. The connection itself never moves; the pool owns it.

    class ConnectionGuard {
    public:
        ConnectionGuard() noexcept = default;

        ConnectionGuard(std::shared_ptr<PgPool> pool, ConnectionType* conn) noexcept
            : pool_(std::move(pool)), conn_(conn) {}

        ~ConnectionGuard() {
            if (conn_) pool_->release(conn_);
        }

        ConnectionGuard(ConnectionGuard&& o) noexcept
            : pool_(std::move(o.pool_)), conn_(std::exchange(o.conn_, nullptr)) {}

        ConnectionGuard& operator=(ConnectionGuard&& o) noexcept {
            if (this != &o) {
                if (conn_) pool_->release(conn_);
                pool_ = std::move(o.pool_);
                conn_ = std::exchange(o.conn_, nullptr);
            }
            return *this;
        }
        ConnectionGuard(const ConnectionGuard&) = delete;
        ConnectionGuard& operator=(const ConnectionGuard&) = delete;

        [[nodiscard]] ConnectionType& conn() noexcept { return *conn_; }
        [[nodiscard]] const ConnectionType& conn() const noexcept { return *conn_; }

    private:
        std::shared_ptr<PgPool> pool_;
        ConnectionType* conn_ = nullptr;  // non-null ⟹ pool_ non-null
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
        auto pool = make(io, std::move(conninfo), cfg);
        co_await pool->warmUp();
        co_return pool;
    }

    // A pool that has no connection yet: acquire() waits until warmUp() hands
    // it one. Touches nothing of the loop.
    static std::shared_ptr<PgPool> make(Io& io, std::string conninfo, PgPoolConfig cfg = {}) {
        return std::shared_ptr<PgPool>(new PgPool(io, std::move(conninfo), cfg));
    }

    // Connects min_connections, one after another, each handed to the oldest
    // acquire() waiting, if any; then acquire() connects as needed, up to
    // max_connections, and connections are opened for the acquires still
    // waiting. On failure, throws, as failWarmUp() does. Once.
    Task<void> warmUp() {
        try {
            for (size_t i = 0; i < cfg_.min_connections; ++i) {
                auto conn = co_await connectOne();
                auto* adopted = adopt(std::move(conn));
                ++total_;
                release(adopted);
            }
        } catch (...) {
            failWarmUp(std::current_exception());
            throw;
        }
        limit_ = cfg_.max_connections;
        for (size_t queued = waiters_.size(); queued > 0 && total_ < limit_; --queued) {
            ++total_;
            connectReplacement(this->shared_from_this());
        }

        if (cfg_.uncertain_settle == UncertainSettle::Delay && !conns_.empty()
            && conns_.front()->settleDelay().count() == 0) {
            RELAIS_LOG_WARN << "relais: uncertain_settle is Delay, but neither settle_delay "
                               "nor the server's statement_timeout bounds a write: "
                               "uncertain writes will not be settled";
        }
    }

    // Ends a pool that is not warmed up: the acquires waiting throw `error`,
    // and so does every acquire() that finds no connection idle from then on.
    void failWarmUp(std::exception_ptr error) {
        assert(error && limit_ == 0);
        failure_ = std::move(error);
        while (!waiters_.empty()) {
            auto* waiter = waiters_.front();
            waiters_.pop_front();
            waiter->deadline.disarm();
            waiter->error = failure_;
            wake(waiter->continuation);
        }
    }

    // Acquire a connection (may suspend if pool exhausted)

    Task<ConnectionGuard> acquire() {
        while (!idle_.empty()) {
            auto* conn = idle_.back();
            idle_.pop_back();
            if (conn->connected())
                co_return ConnectionGuard(this->shared_from_this(), conn);
            // The server closed it while idle; its socket is already let go.
            --total_;
            destroy(conn);
        }

        if (total_ < limit_) {
            // ++total_ must be undone if connect throws. The
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
            // acquire_timeout bounds this connect: a hanging handshake
            // (DNS, SYN blackhole) throws PgPoolTimeout, the SlotGuard unwinds
            // ++total_, and acquire() propagates instead of freezing.
            auto conn = co_await connectOne();
            auto* adopted = adopt(std::move(conn));
            slot.committed = true;
            co_return ConnectionGuard(this->shared_from_this(), adopted);
        }

        if (failure_) [[unlikely]] std::rethrow_exception(failure_);
        Waiter waiter{this};
        co_return co_await waiter;
    }

    /// Diagnostics — connections counted against max_connections (idle + in-use +
    /// in-flight connects), connections currently idle, and coroutines queued on
    /// acquire_timeout. Loop-thread only (no lock): the pool mutates these solely
    /// on its own loop. Used to assert no slot/waiter leak after a timeout storm.
    [[nodiscard]] size_t totalConnections() const noexcept { return total_; }
    [[nodiscard]] size_t idleCount() const noexcept { return idle_.size(); }
    [[nodiscard]] size_t waiterCount() const noexcept { return waiters_.size(); }

    /// No connection lent or connecting, no acquire waiting or about to
    /// resume: no request is in flight on the pool, whoever awaits it.
    /// Loop-thread only, like the above.
    [[nodiscard]] bool quiescent() const noexcept {
        return idle_.size() == total_ && waiters_.empty() && waking_ == 0;
    }

    [[nodiscard]] const std::string& conninfo() const noexcept { return conninfo_; }
    [[nodiscard]] const PgPoolConfig& config() const noexcept { return cfg_; }

private:
    // Forward-declared: onWaiterTimeout() names Waiter as a parameter type, and a
    // parameter type is not in complete-class context (unlike a member body), so
    // it must be visible before that declaration.
    struct Waiter;

    using Deadlines = detail::DeadlineQueue<Io>;

    PgPool(Io& io, std::string conninfo, PgPoolConfig cfg)
        : io_(io)
        , conninfo_(std::move(conninfo))
        , cfg_(cfg)
        , acquire_deadlines_(Deadlines::make(io, cfg.acquire_timeout))
        , query_deadlines_(Deadlines::make(io, cfg.query_timeout))
    {}

    [[nodiscard]] detail::SettleConfig settleConfig() const noexcept {
        return {cfg_.uncertain_settle, cfg_.settle_delay, cfg_.client_check_interval};
    }

    // A new connection, bounded by the pool's queues.
    Task<std::unique_ptr<ConnectionType>> connectOne() {
        return ConnectionType::connect(io_, conninfo_.c_str(), acquire_deadlines_,
                                       query_deadlines_, settleConfig());
    }

    // Takes ownership of a new connection, which stays at its address until
    // destroy(). Returns that address.
    ConnectionType* adopt(std::unique_ptr<ConnectionType> conn) {
        conns_.push_back(std::move(conn));
        return conns_.back().get();
    }

    // Linear in the pool size, off the hot path: only a dead connection is
    // destroyed while the pool lives.
    void destroy(ConnectionType* conn) noexcept {
        auto it = std::find_if(conns_.begin(), conns_.end(),
                               [conn](const auto& c) { return c.get() == conn; });
        assert(it != conns_.end());
        std::swap(*it, conns_.back());
        conns_.pop_back();
    }

    void release(ConnectionType* conn) {
        // A dead connection is dropped, never handed to a waiter.
        if (!conn->connected()) {
            --total_;
            replenish();
            destroy(conn);
            return;
        }

        if (!waiters_.empty()) {
            auto* waiter = waiters_.front();
            waiters_.pop_front();
            // Win the acquire_timeout race: disarm the waiter's deadline before
            // posting its resume. Single loop thread, so once disarmed its expiry
            // (onWaiterTimeout) cannot run. No-op if unarmed (acquire_timeout == 0).
            waiter->deadline.disarm();
            waiter->conn = conn;
            io_.post([h = waiter->continuation] { h.resume(); });
            return;
        }

        idle_.push_back(conn);
    }

    // A dead connection freed a slot while acquires wait: nothing else would
    // connect for them (only a new acquire() does), so connect a replacement
    // for the slot and hand it over as a release does. A failed connect gives
    // the slot back; the waiters keep their acquire_timeout.
    void replenish() {
        if (waiters_.empty() || total_ >= limit_) return;
        ++total_;
        connectReplacement(this->shared_from_this());
    }

    static detail::TrackedTask connectReplacement(std::shared_ptr<PgPool> self) {
        try {
            auto conn = co_await self->connectOne();
            self->release(self->adopt(std::move(conn)));
        } catch (...) {
            --self->total_;
        }
    }

    // acquire_timeout expiry for a queued Waiter. Single loop thread, so
    // this and release() cannot interleave: whichever runs first removes the
    // waiter — release pops it and disarms its deadline; this erases it from the
    // queue — and the other finds nothing to do. The resume is posted, never run
    // inline, to keep the unwind out of the timer's dispatch.
    void onWaiterTimeout(Waiter* w) {
        for (auto it = waiters_.begin(); it != waiters_.end(); ++it) {
            if (*it == w) {
                waiters_.erase(it);  // linear — the wait queue is short
                w->timed_out_ = true;
                wake(w->continuation);
                return;
            }
        }
        // Not found: release() already served this waiter and disarmed its
        // deadline — this should not even run; defensive no-op.
    }

    // Resumes an acquire taken off the queue without a connection, on a turn
    // of its own. Until it runs, the acquire is still in flight (quiescent()):
    // a teardown must not destroy what it is about to unwind through.
    void wake(std::coroutine_handle<> h) {
        ++waking_;
        io_.post([this, h] {
            --waking_;
            h.resume();
        });
    }

    struct Waiter {
        PgPool* pool;
        ConnectionType* conn = nullptr;  // handed over by release()
        std::coroutine_handle<> continuation{};
        bool timed_out_ = false;  // acquire_timeout fired before a connection arrived
        std::exception_ptr error{};  // what failed the warm-up it waited for
        // Bounds the queue wait by acquire_timeout. It lives in the suspended
        // acquire() frame, which is never destroyed while queued here — the same
        // invariant the waiters_-stored `continuation` relies on — and disarms
        // itself with the frame.
        detail::Deadline deadline{this, &Waiter::expire};

        static void expire(void* w) {
            auto* self = static_cast<Waiter*>(w);
            self->pool->onWaiterTimeout(self);
        }

        bool await_ready() const noexcept { return false; }

        // noexcept preserved: under OOM both waiters_.push_back and the queue's
        // first timer may throw bad_alloc, which terminates, rather than leave a
        // half-armed, half-queued waiter.
        void await_suspend(std::coroutine_handle<> h) noexcept {
            continuation = h;
            pool->waiters_.push_back(this);
            if (auto* q = pool->acquire_deadlines_.get()) q->arm(deadline);  // 0 = unbounded
        }

        ConnectionGuard await_resume() {
            if (timed_out_)
                throw PgPoolTimeout("queue wait exceeded acquire_timeout");
            if (error) std::rethrow_exception(error);
            assert(conn);
            return ConnectionGuard(pool->shared_from_this(), conn);
        }
    };

    Io& io_;
    std::string conninfo_;
    PgPoolConfig cfg_;
    size_t total_ = 0;
    // Connections acquire() may open: none until warmUp() succeeds, so that
    // every acquire waits for it; then max_connections.
    size_t limit_ = 0;
    std::exception_ptr failure_;  // what failed the warm-up, if it failed
    size_t waking_ = 0;  // acquires taken off waiters_ whose resume is posted

    // acquire_timeout bounds queued acquires and connect handshakes;
    // query_timeout, every I/O wait of the pool's connections. Null = unbounded.
    std::shared_ptr<Deadlines> acquire_deadlines_;
    std::shared_ptr<Deadlines> query_deadlines_;

    // Every connection the pool owns, idle or lent out. Its I/O callbacks hold
    // its address, so the pool keeps it in a node of its own, never moved.
    std::vector<std::unique_ptr<ConnectionType>> conns_;
    std::vector<ConnectionType*> idle_;
    std::deque<Waiter*> waiters_;
};

namespace detail {

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

    template<PgArg... Args>
    Task<PgResult> queryArgs(const char* sql, Args&&... args) {
        auto params = PgParams::make(std::forward<Args>(args)...);
        co_return co_await queryParams(sql, params);
    }

    Task<int> execute(const char* sql, const PgParams& params) {
        auto guard = co_await pool_->acquire();
        co_return co_await guard.conn().execute(sql, params);
    }

    template<PgArg... Args>
    Task<int> executeArgs(const char* sql, Args&&... args) {
        auto params = PgParams::make(std::forward<Args>(args)...);
        co_return co_await execute(sql, params);
    }

    [[nodiscard]] Pool& pool() noexcept { return *pool_; }
    [[nodiscard]] const Pool& pool() const noexcept { return *pool_; }

private:
    std::shared_ptr<Pool> pool_;
};

}  // namespace detail

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_POOL_H
