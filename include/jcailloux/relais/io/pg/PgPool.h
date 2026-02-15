#ifndef JCX_RELAIS_IO_PG_POOL_H
#define JCX_RELAIS_IO_PG_POOL_H

#include <cassert>
#include <coroutine>
#include <cstddef>
#include <deque>
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

    // Factory: create pool with initial connections

    static Task<std::shared_ptr<PgPool>> create(
        Io& io,
        std::string conninfo,
        size_t min_connections = 2,
        size_t max_connections = 16
    ) {
        auto pool = std::shared_ptr<PgPool>(
            new PgPool(io, std::move(conninfo), min_connections, max_connections));

        for (size_t i = 0; i < min_connections; ++i) {
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

        if (total_ < max_connections_) {
            ++total_;
            auto conn = co_await ConnectionType::connect(io_, conninfo_.c_str());
            co_return ConnectionGuard(this->shared_from_this(), std::move(conn));
        }

        Waiter waiter{this};
        co_return co_await waiter;
    }

private:
    PgPool(Io& io, std::string conninfo, size_t min_conn, size_t max_conn)
        : io_(io)
        , conninfo_(std::move(conninfo))
        , min_connections_(min_conn)
        , max_connections_(max_conn)
    {}

    void release(ConnectionType conn) {
        if (!waiters_.empty()) {
            auto* waiter = waiters_.front();
            waiters_.pop_front();
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

    struct Waiter {
        PgPool* pool;
        std::optional<ConnectionType> conn;
        std::coroutine_handle<> continuation;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) noexcept {
            continuation = h;
            pool->waiters_.push_back(this);
        }

        ConnectionGuard await_resume() {
            assert(conn.has_value());
            return ConnectionGuard(pool->shared_from_this(), std::move(*conn));
        }
    };

    Io& io_;
    std::string conninfo_;
    size_t min_connections_;
    size_t max_connections_;
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
