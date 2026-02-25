#ifndef JCX_RELAIS_IO_PG_CONNECTION_H
#define JCX_RELAIS_IO_PG_CONNECTION_H

#include <cassert>
#include <chrono>
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

    // =========================================================================
    // Pipeline mode — batch multiple queries on a single connection
    // =========================================================================

    /// Enter pipeline mode. Must be called before sendPreparedPipelined().
    void enterPipelineMode() {
        if (!PQenterPipelineMode(conn_))
            throw PgError(std::string("PQenterPipelineMode failed: ") + PQerrorMessage(conn_));
    }

    /// Exit pipeline mode. Call after all pipeline results have been read.
    void exitPipelineMode() {
        if (!PQexitPipelineMode(conn_))
            throw PgError(std::string("PQexitPipelineMode failed: ") + PQerrorMessage(conn_));
    }

    /// Ensure a statement is prepared in pipeline mode (non-blocking).
    /// If the statement is not yet prepared, queues a PQsendPrepare into the pipeline.
    /// Returns true if a prepare was queued (caller must account for an extra result).
    bool ensurePreparedPipelined(const char* sql, int nParams) {
        auto it = prepared_.find(sql);
        if (it != prepared_.end())
            return false;

        auto name = "s" + std::to_string(prepared_.size());
        if (!PQsendPrepare(conn_, name.c_str(), sql, nParams, nullptr))
            throw PgError(std::string("PQsendPrepare (pipeline) failed: ") + PQerrorMessage(conn_));

        prepared_.emplace(sql, std::move(name));
        return true;
    }

    /// Send a prepared query into the pipeline without waiting for the result.
    void sendPreparedPipelined(const char* sql, const PgParams& params) {
        auto it = prepared_.find(sql);
        assert(it != prepared_.end() && "statement not prepared");

        const int n = params.count();
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

        if (!PQsendQueryPrepared(conn_, it->second.c_str(),
                n, values, lengths, formats, 0))
        {
            throw PgError(std::string("PQsendQueryPrepared (pipeline) failed: ") + PQerrorMessage(conn_));
        }
    }

    /// Insert a sync point in the pipeline. Separates segments for error isolation.
    void pipelineSync() {
        if (!PQpipelineSync(conn_))
            throw PgError(std::string("PQpipelineSync failed: ") + PQerrorMessage(conn_));
    }

    /// Flush the pipeline output buffer to the server.
    Task<void> flushPipeline() {
        while (true) {
            int ret = PQflush(conn_);
            if (ret == 0) co_return;         // All flushed
            if (ret < 0)
                throw PgError(std::string("PQflush failed: ") + PQerrorMessage(conn_));
            // ret == 1: need to wait for socket write-ready, then retry
            co_await awaitWriteReady();
        }
    }

    /// Result from a single pipeline segment (query result + processing time).
    struct PipelineResult {
        PgResult result;
        int64_t processing_time_us = 0;  // inter-result interval for GDSF cost
    };

    /// Read n pipeline segment results (one per query, between syncs).
    /// Each segment: read PQgetResult until NULL (= one query's result),
    /// then read the sync result (PGRES_PIPELINE_SYNC).
    /// Returns exactly n PipelineResults in pipeline order.
    Task<std::vector<PipelineResult>> readPipelineResults(int n) {
        std::vector<PipelineResult> results;
        results.reserve(n);

        auto prev = std::chrono::steady_clock::now();

        for (int i = 0; i < n; ++i) {
            // Read the query result for this segment
            PgResult query_result = co_await awaitPipelineResult();
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - prev);
            prev = now;

            results.emplace_back(std::move(query_result), elapsed.count());

            // Consume the sync point result (PGRES_PIPELINE_SYNC)
            co_await consumePipelineSync();
        }

        co_return results;
    }

private:
    // Write-ready awaiter for pipeline flushing
    struct WriteAwaiter {
        PgConnection* self;
        std::coroutine_handle<> continuation;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            continuation = h;
            self->registerWatch(IoEvent::Write, [this](IoEvent) {
                // Save before removeCurrentWatch destroys this lambda (and its captures)
                auto coro = continuation;
                self->removeCurrentWatch();
                coro.resume();
            });
        }

        void await_resume() noexcept {}
    };

    WriteAwaiter awaitWriteReady() { return {this, {}}; }

    // Read a single query's result from the pipeline (everything up to NULL).
    Task<PgResult> awaitPipelineResult() {
        while (true) {
            if (!PQconsumeInput(conn_))
                throw PgError(std::string("PQconsumeInput failed: ") + PQerrorMessage(conn_));

            if (PQisBusy(conn_)) {
                co_await awaitReadReady();
                continue;
            }

            // Read all PGresults for this query (last non-NULL before NULL)
            PGresult* last = nullptr;
            while (PGresult* r = PQgetResult(conn_)) {
                if (last) PQclear(last);
                last = r;

                // In pipeline mode, check if we hit a PIPELINE_SYNC or PIPELINE_ABORTED
                auto status = PQresultStatus(last);
                if (status == PGRES_PIPELINE_SYNC) {
                    // Put back for consumePipelineSync — but we can't put it back.
                    // This shouldn't happen here since sync comes after the NULL.
                    // Actually in pipeline mode: query results end with NULL,
                    // then the sync result is a separate PQgetResult call.
                    break;
                }

                // Check if the next result is available without blocking
                if (PQisBusy(conn_)) {
                    co_await awaitReadReady();
                    if (!PQconsumeInput(conn_))
                        throw PgError(std::string("PQconsumeInput failed: ") + PQerrorMessage(conn_));
                }
            }

            co_return PgResult(last);
        }
    }

    // Consume the pipeline sync point result.
    Task<void> consumePipelineSync() {
        while (true) {
            if (!PQconsumeInput(conn_))
                throw PgError(std::string("PQconsumeInput failed: ") + PQerrorMessage(conn_));

            if (PQisBusy(conn_)) {
                co_await awaitReadReady();
                continue;
            }

            PGresult* r = PQgetResult(conn_);
            if (r) {
                auto status = PQresultStatus(r);
                PQclear(r);
                if (status == PGRES_PIPELINE_SYNC)
                    co_return;
                // If not sync, keep reading
                continue;
            }
            // NULL without sync — unexpected, but don't loop forever
            co_return;
        }
    }

    // Read-ready awaiter (shared between pipeline and normal mode)
    struct ReadAwaiter {
        PgConnection* self;
        std::coroutine_handle<> continuation;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            continuation = h;
            self->registerWatch(IoEvent::Read, [this](IoEvent) {
                // Save before removeCurrentWatch destroys this lambda (and its captures)
                auto coro = continuation;
                self->removeCurrentWatch();
                coro.resume();
            });
        }

        void await_resume() noexcept {}
    };

    ReadAwaiter awaitReadReady() { return {this, {}}; }
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
                case PGRES_POLLING_FAILED: {
                    // Save before removeCurrentWatch destroys this lambda (and its captures)
                    auto coro = continuation;
                    self->removeCurrentWatch();
                    coro.resume();
                    break;
                }
                default: {
                    auto coro = continuation;
                    self->removeCurrentWatch();
                    coro.resume();
                    break;
                }
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
                // Save before removeCurrentWatch destroys this lambda (and its captures)
                auto coro = continuation;
                self->removeCurrentWatch();
                // result is on the awaiter (stack-allocated), safe to access after removeWatch
                result = PgResult{};
                coro.resume();
                return;
            }

            if (PQisBusy(self->conn_))
                return;

            PGresult* last = nullptr;
            while (PGresult* r = PQgetResult(self->conn_)) {
                if (last) PQclear(last);
                last = r;
            }

            // Save before removeCurrentWatch destroys this lambda (and its captures)
            auto coro = continuation;
            self->removeCurrentWatch();
            result = PgResult(last);
            coro.resume();
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
