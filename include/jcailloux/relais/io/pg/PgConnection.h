#ifndef JCX_RELAIS_IO_PG_CONNECTION_H
#define JCX_RELAIS_IO_PG_CONNECTION_H

#include <cassert>
#include <coroutine>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <libpq-fe.h>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/pg/PgResult.h"

namespace jcailloux::relais::io {

// PgConnection — RAII async PostgreSQL connection using libpq + IoContext

template<IoContext Io>
class PgConnection {
public:
    PgConnection(Io& io, PGconn* conn) noexcept
        : io_(&io), conn_(conn) {
        assert(conn_);
        PQsetnonblocking(conn_, 1);
    }

    ~PgConnection() {
        if (conn_) {
            if (watch_active_) {
                io_->removeWatch(watch_);
                watch_active_ = false;
            }
            PQfinish(conn_);
        }
    }

    // Move only
    PgConnection(PgConnection&& o) noexcept
        : io_(o.io_)
        , conn_(std::exchange(o.conn_, nullptr))
        , watch_(std::exchange(o.watch_, {}))
        , watch_active_(std::exchange(o.watch_active_, false))
        , prepared_(std::move(o.prepared_))
    {}

    PgConnection& operator=(PgConnection&& o) noexcept {
        if (this != &o) {
            if (conn_) {
                if (watch_active_) io_->removeWatch(watch_);
                PQfinish(conn_);
            }
            io_ = o.io_;
            conn_ = std::exchange(o.conn_, nullptr);
            watch_ = std::exchange(o.watch_, {});
            watch_active_ = std::exchange(o.watch_active_, false);
            prepared_ = std::move(o.prepared_);
        }
        return *this;
    }

    PgConnection(const PgConnection&) = delete;
    PgConnection& operator=(const PgConnection&) = delete;

    // Connection state

    [[nodiscard]] bool connected() const noexcept {
        return conn_ && PQstatus(conn_) == CONNECTION_OK;
    }

    [[nodiscard]] int socket() const noexcept {
        return conn_ ? PQsocket(conn_) : -1;
    }

    // Async connect (static factory)

    static Task<PgConnection> connect(Io& io, const char* conninfo) {
        PGconn* conn = PQconnectStart(conninfo);
        if (!conn)
            throw PgConnectionError("PQconnectStart returned null");

        if (PQstatus(conn) == CONNECTION_BAD) {
            std::string err = PQerrorMessage(conn);
            PQfinish(conn);
            throw PgConnectionError("connect failed: " + err);
        }

        PgConnection pgconn(io, conn);
        co_await pgconn.awaitConnect();
        co_return std::move(pgconn);
    }

    // Async query with parameters

    Task<PgResult> query(const char* sql) {
        if (!PQsendQueryParams(conn_, sql, 0, nullptr, nullptr, nullptr, nullptr, 0))
            throw PgError(std::string("PQsendQueryParams failed: ") + PQerrorMessage(conn_));
        co_return co_await awaitResult();
    }

    Task<PgResult> queryParams(const char* sql, const PgParams& params) {
        const int n = params.count();
        co_await ensurePrepared(sql, n);
        auto& name = prepared_[sql];

        // Stack-allocated arrays for <=16 params (covers 99.9% of queries)
        static constexpr int kInlineMax = 16;
        const char* val_buf[kInlineMax]; int len_buf[kInlineMax]; int fmt_buf[kInlineMax];
        std::vector<const char*> val_vec; std::vector<int> len_vec, fmt_vec;
        const char** values; int* lengths; int* formats;

        if (n <= kInlineMax) {
            values = val_buf; lengths = len_buf; formats = fmt_buf;
        } else {
            val_vec.resize(n); len_vec.resize(n); fmt_vec.resize(n);
            values = val_vec.data(); lengths = len_vec.data(); formats = fmt_vec.data();
        }
        params.fillArrays(values, lengths, formats);

        if (!PQsendQueryPrepared(conn_, name.c_str(),
                n, values, lengths, formats, 0))
        {
            throw PgError(std::string("PQsendQueryPrepared failed: ") + PQerrorMessage(conn_));
        }
        co_return co_await awaitResult();
    }

    // Async execute (no result rows expected)

    Task<int> execute(const char* sql, const PgParams& params) {
        auto result = co_await queryParams(sql, params);
        co_return result.affectedRows();
    }

private:
    // Async connect polling

    struct ConnectAwaiter {
        PgConnection* self;
        std::coroutine_handle<> continuation;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            continuation = h;
            auto poll = PQconnectPoll(self->conn_);
            IoEvent events = IoEvent::None;

            switch (poll) {
                case PGRES_POLLING_READING:
                    events = IoEvent::Read;
                    break;
                case PGRES_POLLING_WRITING:
                    events = IoEvent::Write;
                    break;
                case PGRES_POLLING_OK:
                    self->io_->post([h] { h.resume(); });
                    return;
                default:
                    self->io_->post([h] { h.resume(); });
                    return;
            }

            self->registerWatch(events, [this](IoEvent) {
                pollConnect();
            });
        }

        void await_resume() {
            if (PQstatus(self->conn_) != CONNECTION_OK)
                throw PgConnectionError(
                    std::string("async connect failed: ") + PQerrorMessage(self->conn_));
        }

    private:
        void pollConnect() {
            auto poll = PQconnectPoll(self->conn_);

            switch (poll) {
                case PGRES_POLLING_READING:
                    self->updateWatchEvents(IoEvent::Read);
                    break;
                case PGRES_POLLING_WRITING:
                    self->updateWatchEvents(IoEvent::Write);
                    break;
                case PGRES_POLLING_OK:
                case PGRES_POLLING_FAILED:
                    self->removeCurrentWatch();
                    continuation.resume();
                    break;
                default:
                    self->removeCurrentWatch();
                    continuation.resume();
                    break;
            }
        }
    };

    ConnectAwaiter awaitConnect() {
        return ConnectAwaiter{this, {}};
    }

    // Auto-prepare: first call PREPAREs the statement, subsequent calls reuse it

    Task<void> ensurePrepared(const char* sql, int nParams) {
        auto it = prepared_.find(sql);
        if (it != prepared_.end())
            co_return;

        auto name = "s" + std::to_string(prepared_.size());
        if (!PQsendPrepare(conn_, name.c_str(), sql, nParams, nullptr))
            throw PgError(std::string("PQsendPrepare failed: ") + PQerrorMessage(conn_));

        co_await awaitResult();
        prepared_.emplace(sql, std::move(name));
    }

    // Async result reading

    struct ResultAwaiter {
        PgConnection* self;
        std::coroutine_handle<> continuation;
        PgResult result;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            continuation = h;
            self->registerWatch(IoEvent::Read, [this](IoEvent) {
                consumeInput();
            });
        }

        PgResult await_resume() {
            if (!result.ok())
                throw PgError("query failed");
            return std::move(result);
        }

    private:
        void consumeInput() {
            if (!PQconsumeInput(self->conn_)) {
                self->removeCurrentWatch();
                result = PgResult{};
                continuation.resume();
                return;
            }

            if (PQisBusy(self->conn_))
                return;

            PGresult* last = nullptr;
            while (PGresult* r = PQgetResult(self->conn_)) {
                if (last) PQclear(last);
                last = r;
            }

            self->removeCurrentWatch();
            result = PgResult(last);
            continuation.resume();
        }
    };

    ResultAwaiter awaitResult() noexcept {
        return ResultAwaiter{this, {}, {}};
    }

    // Watch management helpers

    void registerWatch(IoEvent events, std::function<void(IoEvent)> cb) {
        if (watch_active_) io_->removeWatch(watch_);
        watch_ = io_->addWatch(socket(), events, std::move(cb));
        watch_active_ = true;
    }

    void updateWatchEvents(IoEvent events) {
        if (watch_active_)
            io_->updateWatch(watch_, events);
    }

    void removeCurrentWatch() {
        if (watch_active_) {
            io_->removeWatch(watch_);
            watch_active_ = false;
        }
    }

    Io* io_;
    PGconn* conn_ = nullptr;
    typename Io::WatchHandle watch_{};
    bool watch_active_ = false;
    std::unordered_map<std::string, std::string> prepared_;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_CONNECTION_H
