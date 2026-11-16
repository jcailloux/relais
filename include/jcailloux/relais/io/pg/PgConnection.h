#ifndef JCX_RELAIS_IO_PG_CONNECTION_H
#define JCX_RELAIS_IO_PG_CONNECTION_H

#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <libpq-fe.h>
#include <poll.h>
#include <unistd.h>

#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/io/DeadlineQueue.h"
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/io/pg/SettleTicket.h"

namespace jcailloux::relais::io::detail {

// PgConnection — RAII async PostgreSQL connection using libpq + IoContext
//
// One watch serves the connection for its whole life, on relais's own
// duplicate of libpq's socket: a request changes no watch, at most its mask.
// Watches are added and removed from posted tasks, never inside an event
// dispatch.
//
// Its handshake and its I/O waits are bounded by a deadline in a queue
// (DeadlineQueue), one loop timer serving every connection that shares the
// queue: a request arms no timer of the loop.
//
// Neither copyable nor movable: its watch callback and its deadline hold its
// address, so a connection stays where connect() built it, on the heap, until
// destroyed.

template<IoContext Io>
class PgConnection {
public:
    using Deadlines = DeadlineQueue<Io>;

    ~PgConnection() {
        deadline_.disarm();
        // A request that loses its connection may destroy it inside an event
        // dispatch: the watch is retired, its removal posted.
        retireSocketWatch();
        if (conn_) PQfinish(conn_);
    }

    PgConnection(const PgConnection&) = delete;
    PgConnection& operator=(const PgConnection&) = delete;

    // Connection state

    [[nodiscard]] bool connected() const noexcept {
        // dead_ first: a timed-out connection is poisoned even though
        // PQstatus stays CONNECTION_OK on a *silent* hang (socket retained, no
        // RST). release() must see false → throw the connection, not reuse it.
        return !dead_ && conn_ && PQstatus(conn_) == CONNECTION_OK;
    }

    /// The descriptor the loop watches for this connection: relais's duplicate
    /// of libpq's socket, -1 once no longer watched.
    [[nodiscard]] int socket() const noexcept {
        return socket_watch_ ? socket_watch_->fd : -1;
    }

    // Async connect (static factory)

    // acquire_timeout bounds the connect handshake and the setup queries below;
    // query_timeout bounds each later I/O wait. Both default to 0 = unbounded
    // (server/conninfo-delegated). The connection gets queues of its own; a
    // pool's connections share the pool's, through the overload below.
    //
    // `settle` decides what a write failing uncertainly on this connection must
    // wait for (settleTicket()). Terminate and Await name the backend, and Delay
    // without a delay reads the server's statement_timeout, both here within
    // acquire_timeout; a given delay and Off add no round-trip. A non-zero
    // client_check_interval adds one, except under Delay.
    static Task<std::unique_ptr<PgConnection>> connect(
        Io& io, const char* conninfo,
        std::chrono::nanoseconds acquire_timeout = {},
        std::chrono::nanoseconds query_timeout = {},
        SettleConfig settle = {})
    {
        return connect(io, conninfo, Deadlines::make(io, acquire_timeout),
                       Deadlines::make(io, query_timeout), settle);
    }

    // The same, bounded by the given queues (null = unbounded): the durations
    // are theirs, and their timers serve every connection that shares them.
    static Task<std::unique_ptr<PgConnection>> connect(
        Io& io, const char* conninfo,
        std::shared_ptr<Deadlines> acquire_deadlines,
        std::shared_ptr<Deadlines> query_deadlines,
        SettleConfig settle = {})
    {
        // Allocated before libpq's connection, which it then owns: a failure
        // below frees both.
        std::unique_ptr<PgConnection> pgconn(new PgConnection(
            io, std::move(acquire_deadlines), std::move(query_deadlines)));
        pgconn->conn_ = PQconnectStart(conninfo);
        if (!pgconn->conn_)
            throw PgConnectionError("PQconnectStart returned null");

        if (PQstatus(pgconn->conn_) == CONNECTION_BAD)
            throw PgConnectionError(std::string("connect failed: ")
                                    + PQerrorMessage(pgconn->conn_));
        PQsetnonblocking(pgconn->conn_, 1);

        co_await pgconn->awaitConnect();
        pgconn->settle_ = settle;
        if (settle.mode == UncertainSettle::Terminate || settle.mode == UncertainSettle::Await)
            co_await pgconn->readBackendId();
        else if (settle.mode == UncertainSettle::Delay && settle.delay.count() <= 0)
            co_await pgconn->readStatementTimeout();
        if (settle.mode != UncertainSettle::Delay && settle.client_check_interval.count() > 0)
            co_await pgconn->setClientCheckInterval(settle.client_check_interval);
        co_return pgconn;
    }

    /// The backend serving this connection, read at connect; unknown when the
    /// settle mode does not need it or the read failed.
    [[nodiscard]] const BackendId& backendId() const noexcept { return backend_; }

    /// The wait that settles an uncertain write in mode Delay: the configured
    /// delay, else the server's statement_timeout plus kSettleMargin; 0 when
    /// nothing bounds a write.
    [[nodiscard]] std::chrono::milliseconds settleDelay() const noexcept {
        return settle_.mode == UncertainSettle::Delay ? settle_.delay
                                                       : std::chrono::milliseconds{0};
    }

    /// What a write that failed uncertainly on this connection must wait for
    /// before its outcome is final. Null when there is nothing to settle: mode
    /// Off, Delay with nothing to bound a write, or a backend that could not be named.
    [[nodiscard]] std::shared_ptr<SettleTicket> settleTicket() const {
        switch (settle_.mode) {
            case UncertainSettle::Terminate:
            case UncertainSettle::Await:
                if (!backend_.known()) return nullptr;
                return std::make_shared<SettleTicket>(
                    SettleTicket{.mode = settle_.mode, .backend = backend_});
            case UncertainSettle::Delay:
                if (settle_.delay.count() <= 0) return nullptr;
                return std::make_shared<SettleTicket>(
                    SettleTicket{.mode = settle_.mode, .delay = settle_.delay});
            case UncertainSettle::Off:
                break;
        }
        return nullptr;
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
        resetPipelineBookkeeping();
        if (!PQenterPipelineMode(conn_))
            throw PgError(std::string("PQenterPipelineMode failed: ") + PQerrorMessage(conn_));
    }

    /// Exit pipeline mode. Call after all pipeline results have been read.
    void exitPipelineMode() {
        resetPipelineBookkeeping();
        if (!PQexitPipelineMode(conn_))
            throw PgError(std::string("PQexitPipelineMode failed: ") + PQerrorMessage(conn_));
    }

    /// Ensure a statement is prepared in pipeline mode (non-blocking).
    /// If the statement is not yet prepared, queues a PQsendPrepare into the pipeline.
    /// Returns true if a prepare was queued: the caller must follow it with
    /// pipelineSync(). The prepare's result is then consumed by
    /// readPipelineResults() together with the next sendPreparedPipelined(), so
    /// callers only ever count executions.
    bool ensurePreparedPipelined(const char* sql, int nParams) {
        auto it = prepared_.find(sql);
        if (it != prepared_.end())
            return false;

        auto name = "s" + std::to_string(prepared_.size());
        if (!PQsendPrepare(conn_, name.c_str(), sql, nParams, nullptr))
            throw PgError(std::string("PQsendPrepare (pipeline) failed: ") + PQerrorMessage(conn_));

        prepared_.emplace(sql, std::move(name));
        pipeline_prepare_queued_ = true;
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
        // Prepares and executions interleave on the wire ([P S] E S per
        // segment); remember which executions carry a prepare ahead of them so
        // the reader consumes results in send order.
        pipeline_prepares_.push_back(pipeline_prepare_queued_);
        pipeline_prepare_queued_ = false;
    }

    /// Insert a sync point in the pipeline. Separates segments for error isolation.
    void pipelineSync() {
        if (!PQpipelineSync(conn_))
            // Flushes the send buffer; earlier batch commands may already be
            // executing server-side, so a failure here is uncertain.
            throw PgConnectionLost(std::string("PQpipelineSync failed: ") + PQerrorMessage(conn_));
    }

    /// Flush the pipeline output buffer to the server.
    Task<void> flushPipeline() {
        while (true) {
            int ret = PQflush(conn_);
            if (ret == 0) co_return;         // All flushed
            if (ret < 0)
                // Bytes may already have reached the server → uncertain.
                throw PgConnectionLost(std::string("PQflush failed: ") + PQerrorMessage(conn_));
            // ret == 1: need to wait for socket write-ready, then retry
            co_await awaitWriteReady();
        }
    }

    /// Result from a single pipeline segment (query result + processing time).
    struct PipelineResult {
        PgResult result;
        int64_t processing_time_us = 0;  // inter-result interval for GDSF cost
    };

    /// Read n pipeline segment results (one per sendPreparedPipelined, between syncs).
    /// Each segment: first discard the prepare queued ahead of it, if any, then
    /// read PQgetResult until NULL (= one query's result), then read the sync
    /// result (PGRES_PIPELINE_SYNC).
    /// Appends to `results` in pipeline order, a segment once its sync is read:
    /// its implicit transaction has then ended, committed or not. On a throw,
    /// `results` holds the segments read in full; the others are unresolved.
    Task<void> readPipelineResults(int n, std::vector<PipelineResult>& results) {
        results.reserve(results.size() + n);

        auto prev = std::chrono::steady_clock::now();

        for (int i = 0; i < n; ++i) {
            assert(pipeline_read_pos_ < pipeline_prepares_.size()
                   && "more results read than executions sent");
            if (pipeline_prepares_[pipeline_read_pos_++]) {
                co_await awaitPipelineResult();
                co_await consumePipelineSync();
                // One-off prepare cost stays out of the execution's timing.
                prev = std::chrono::steady_clock::now();
            }

            // Read the query result for this segment
            PgResult query_result = co_await awaitPipelineResult();
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - prev);
            prev = now;

            // The sync commits the segment's implicit transaction: a refused
            // commit fails the segment, whatever its statement answered.
            if (auto failed = co_await consumePipelineSync())
                query_result = std::move(*failed);

            results.emplace_back(std::move(query_result), elapsed.count());
        }
    }

private:
    PgConnection(Io& io, std::shared_ptr<Deadlines> acquire_deadlines,
                 std::shared_ptr<Deadlines> query_deadlines)
        : io_(io)
        , socket_watch_(std::make_shared<SocketWatch>(this, io))
        , acquire_deadlines_(std::move(acquire_deadlines))
        , query_deadlines_(std::move(query_deadlines))
        , wait_deadlines_(query_deadlines_.get()) {}

    // Part of acquiring the connection, so bounded by acquire_timeout and failing
    // as an acquire does: the request changes nothing, its failure is never
    // uncertain. A server-side refusal leaves the backend unknown and the
    // connection usable.
    Task<void> readBackendId() {
        static constexpr const char* kSql =
            "SELECT pid, backend_start::text FROM pg_stat_activity "
            "WHERE pid = pg_backend_pid()";
        std::optional<PgResult> r = co_await settleQuery(kSql,
            "cannot name the backend of a connection, its uncertain writes will not be settled");
        if (r && r->rows() == 1) {
            backend_.pid = (*r)[0].get<int32_t>(0);
            backend_.backend_start = (*r)[0].get<std::string>(1);
        }
    }

    // Delay without a configured delay: the server's statement_timeout bounds
    // a write. Read in milliseconds; 0 (no bound) leaves nothing to settle.
    Task<void> readStatementTimeout() {
        static constexpr const char* kSql =
            "SELECT setting::bigint FROM pg_settings WHERE name = 'statement_timeout'";
        std::optional<PgResult> r = co_await settleQuery(kSql,
            "cannot read statement_timeout, uncertain writes will not be settled");
        if (r && r->rows() == 1) {
            const auto ms = (*r)[0].get<int64_t>(0);
            if (ms > 0) settle_.delay = std::chrono::milliseconds{ms} + kSettleMargin;
        }
    }

    // Refused by a server before 14 (unknown parameter) or not on Linux: the
    // statement then runs to its end after the client left, as before.
    Task<void> setClientCheckInterval(std::chrono::milliseconds interval) {
        const std::string sql = "SET client_connection_check_interval = "
            + std::to_string(interval.count());
        (void)co_await settleQuery(sql.c_str(),
            "client_connection_check_interval not set", log::Level::Debug);
    }

    // Runs a settle setup query, bounded by acquire_timeout; nullopt when the
    // server refuses it.
    Task<std::optional<PgResult>> settleQuery(const char* sql, const char* refused,
                                              log::Level level = log::Level::Warn) {
        auto* const query_deadlines = std::exchange(wait_deadlines_, acquire_deadlines_.get());
        std::optional<PgResult> r;
        try {
            r = co_await query(sql);
        } catch (const PgQueryTimeout&) {
            throw PgPoolTimeout("settle setup exceeded acquire_timeout");
        } catch (const PgConnectionLost& e) {
            throw PgConnectionError(e.what());
        } catch (const PgError& e) {
            log::LogStream(level) << "relais: " << refused << ": " << e.what();
        }
        wait_deadlines_ = query_deadlines;
        co_return r;
    }

    void resetPipelineBookkeeping() noexcept {
        pipeline_prepare_queued_ = false;
        pipeline_prepares_.clear();  // keeps capacity: no allocation per batch
        pipeline_read_pos_ = 0;
    }

    // What a wait needs from the socket before it ends.
    enum class Want : uint8_t {
        Readable,  // readable or failed; the caller reads
        Result,    // a request's whole result, read here
        Flush,     // writable or failed, reading meanwhile
    };

    // The I/O wait in progress, held by its awaiter.
    struct Wait {
        Want want;
        bool lost = false;  // reading failed: the connection is gone
        PgResult result{};  // Result: what the request answered
    };

    // Waits until the socket takes more of the pipeline, reading meanwhile: a
    // server blocked on results nobody reads stops reading the pipeline, and
    // the socket would never become writable.
    struct WriteAwaiter {
        PgConnection* self;
        Wait wait{Want::Flush};

        ~WriteAwaiter() { self->abandon(wait); }

        bool await_ready() const noexcept { return false; }

        bool await_suspend(std::coroutine_handle<> h) { return self->beginWait(wait, h); }

        // Not noexcept: on a query_timeout the timer posts the resume with dead_
        // set, which must surface as a throw.
        void await_resume() {
            if (self->dead_)
                throw PgQueryTimeout("write wait exceeded query_timeout");
            if (wait.lost)
                // Bytes may already have reached the server → uncertain.
                throw PgConnectionLost(std::string("connection lost flushing the pipeline: ")
                                       + PQerrorMessage(self->conn_));
        }
    };

    WriteAwaiter awaitWriteReady() { return {this}; }

    // Read a single query's result from the pipeline (everything up to NULL).
    Task<PgResult> awaitPipelineResult() {
        while (true) {
            if (!consumeInput())
                // Post-send (reading the result): a dropped socket is uncertain.
                throw PgConnectionLost(std::string("PQconsumeInput failed: ") + PQerrorMessage(conn_));

            if (PQisBusy(conn_)) {
                co_await awaitReadReady();
                continue;
            }

            // Read all PGresults for this query (last non-NULL before NULL)
            std::unique_ptr<PGresult, decltype(&PQclear)> last(nullptr, &PQclear);
            while (PGresult* r = PQgetResult(conn_)) {
                last.reset(r);

                // In pipeline mode, check if we hit a PIPELINE_SYNC or PIPELINE_ABORTED
                auto status = PQresultStatus(r);
                if (status == PGRES_PIPELINE_SYNC) {
                    // Put back for consumePipelineSync — but we can't put it back.
                    // This shouldn't happen here since sync comes after the NULL.
                    // Actually in pipeline mode: query results end with NULL,
                    // then the sync result is a separate PQgetResult call.
                    break;
                }

                // PQgetResult blocks while the next result is incomplete: wait
                // for it, however many wakeups it takes.
                while (PQisBusy(conn_)) {
                    co_await awaitReadReady();
                    if (!consumeInput())
                        // Post-send (reading the result): a dropped socket is uncertain.
                        throw PgConnectionLost(std::string("PQconsumeInput failed: ") + PQerrorMessage(conn_));
                }
            }

            co_return PgResult(last.release());
        }
    }

    // Consume the pipeline sync point result (PGRES_PIPELINE_SYNC). The server
    // commits the implicit transaction when it processes the sync: a refused
    // commit (deferred constraint, serialization failure) comes as an error
    // result, then the NULL ending it, ahead of the sync. That error is returned.
    Task<std::optional<PgResult>> consumePipelineSync() {
        std::optional<PgResult> failed;
        bool after_null = false;
        while (true) {
            if (!consumeInput())
                // Post-send (reading the result): a dropped socket is uncertain.
                throw PgConnectionLost(std::string("PQconsumeInput failed: ") + PQerrorMessage(conn_));

            if (PQisBusy(conn_)) {
                co_await awaitReadReady();
                continue;
            }

            PGresult* r = PQgetResult(conn_);
            if (!r) {
                // Two NULLs in a row: nothing is queued, no sync will come.
                if (after_null) co_return failed;
                after_null = true;
                continue;
            }
            after_null = false;
            if (PQresultStatus(r) == PGRES_PIPELINE_SYNC) {
                PQclear(r);
                co_return failed;
            }
            if (!failed && PQresultStatus(r) == PGRES_FATAL_ERROR)
                failed.emplace(r);
            else
                PQclear(r);
        }
    }

    // Waits until the socket is readable or failed; the caller then reads, and
    // waits again if what it read is not enough.
    struct ReadAwaiter {
        PgConnection* self;
        Wait wait{Want::Readable};

        ~ReadAwaiter() { self->abandon(wait); }

        bool await_ready() const noexcept { return false; }

        bool await_suspend(std::coroutine_handle<> h) { return self->beginWait(wait, h); }

        // Not noexcept: throws PgQueryTimeout when resumed after a timeout. A
        // lost connection is reported by the caller's next read.
        void await_resume() {
            if (self->dead_)
                throw PgQueryTimeout("read wait exceeded query_timeout");
        }
    };

    ReadAwaiter awaitReadReady() { return {this}; }
    // Async connect polling

    // Every handshake step runs from a posted task, never inside an event
    // dispatch: a step removes the previous watch and adds the next one.
    struct ConnectAwaiter {
        PgConnection* self;
        bool watch_failed = false;  // the socket could not be watched mid-handshake
        bool step_posted = false;   // a step is pending: further events add none
        // What libpq waits for before its next step; Write before the first.
        IoEvent want = IoEvent::Write;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            // acquire_timeout bounds the whole handshake: armed once here,
            // disarmed when the handshake ends.
            if (auto* q = self->acquire_deadlines_.get()) q->arm(self->deadline_);
            self->current_cont_ = h;
            self->connect_ = this;
            self->postConnectStep();
        }

        void await_resume() {
            // dead_ first: the connect exceeded acquire_timeout → a connect-phase
            // pool timeout, distinct from a server-refused connection.
            if (self->dead_)
                throw PgPoolTimeout("connect handshake exceeded acquire_timeout");
            if (watch_failed)
                throw PgConnectionError(
                    "async connect failed: could not watch the connection's socket");
            if (PQstatus(self->conn_) != CONNECTION_OK)
                throw PgConnectionError(
                    std::string("async connect failed: ") + PQerrorMessage(self->conn_));
        }
    };

    ConnectAwaiter awaitConnect() { return ConnectAwaiter{this}; }

    // Any number of events before the step runs post it once.
    void postConnectStep() {
        if (connect_->step_posted) return;
        connect_->step_posted = true;
        io_.post([w = socket_watch_] {
            if (w->owner) w->owner->connectStep();
        });
    }

    // PQconnectPoll may close the socket and open another one (sslmode=prefer
    // falling back to plaintext, next host or address, target_session_attrs),
    // even under the same number: every step watches a duplicate of the socket
    // it returns on. A step woken before the socket is ready keeps watching it.
    void connectStep() {
        auto* step = connect_;
        if (!step) return;  // acquire_timeout ended the handshake
        step->step_posted = false;

        bool done = false;
        if (socketReady(step->want)) {
            unwatchSocket();
            switch (PQconnectPoll(conn_)) {
                case PGRES_POLLING_READING: step->want = IoEvent::Read; break;
                case PGRES_POLLING_WRITING: step->want = IoEvent::Write; break;
                case PGRES_POLLING_OK:  // the watch the connection keeps
                    step->want = IoEvent::Read;
                    done = true;
                    break;
                default: endConnect(); return;  // FAILED
            }
        }
        if (socket_watch_->fd < 0) {
            try {
                watchSocket(step->want);
            } catch (...) {
                step->watch_failed = true;
                done = true;
            }
        }
        if (done) endConnect();
    }

    // libpq takes the readiness it asked for as given: polled before a TCP
    // connect completes, it takes the connect for made and fails sending its
    // first packet. Ready also when the socket failed or poll() cannot tell:
    // libpq then reports what happened.
    bool socketReady(IoEvent want) const noexcept {
        pollfd p{PQsocket(conn_), static_cast<short>(want == IoEvent::Write ? POLLOUT : POLLIN), 0};
        return p.fd < 0 || ::poll(&p, 1, 0) != 0;
    }

    // From a posted task: the connect resumes outside any event dispatch.
    void endConnect() {
        connect_ = nullptr;
        deadline_.disarm();  // acquire_timeout
        std::exchange(current_cont_, {}).resume();
    }

    // Watches a fresh duplicate of libpq's current socket. Outside any event
    // dispatch only. Throws, watching nothing, if the socket cannot be
    // duplicated or watched.
    void watchSocket(IoEvent events) {
        auto& w = *socket_watch_;
        assert(w.fd < 0);
        const int fd = ::fcntl(PQsocket(conn_), F_DUPFD_CLOEXEC, 0);
        if (fd < 0)
            throw PgConnectionError("cannot duplicate the connection's socket");
        try {
            w.handle = io_.addWatch(fd, events, [p = &w](IoEvent ev) {
                if (p->owner) p->owner->onSocketEvent(ev);
            });
        } catch (...) {
            ::close(fd);
            throw;
        }
        w.fd = fd;
        watch_mask_ = events;
    }

    // Removes the watch, then closes the duplicate. Outside any event dispatch only.
    void unwatchSocket() {
        auto& w = *socket_watch_;
        if (w.fd < 0) return;
        io_.removeWatch(w.handle);
        ::close(std::exchange(w.fd, -1));
    }

    // Detaches the connection from its watch, from anywhere, an event dispatch
    // included: no event reaches the connection anymore, and a posted task
    // removes the watch, then closes the duplicate. Should the loop be
    // destroyed before running that task, destroying the task closes it.
    void retireSocketWatch() {
        if (!socket_watch_) return;
        socket_watch_->owner = nullptr;
        if (socket_watch_->fd >= 0) {
            io_.post([w = std::move(socket_watch_)] {
                w->io.removeWatch(w->handle);
                ::close(std::exchange(w->fd, -1));
            });
        }
        socket_watch_.reset();
    }

    // Every event on the watched socket lands here. A wait ends only once it
    // has what it needs: an adapter that splits an event into several
    // callbacks, or wakes the watch for nothing, costs at most a read that
    // finds nothing.
    void onSocketEvent(IoEvent ev) {
        if (connect_) {
            postConnectStep();
            return;
        }
        const bool readable = hasEvent(ev, IoEvent::Read) || hasEvent(ev, IoEvent::Error);
        Wait* w = wait_;
        if (!w) {
            // Idle, or lent but not waiting: a notice, a parameter change, or
            // the server closing the connection, which the failed read retires.
            if (readable) consumeInput();
            return;
        }
        switch (w->want) {
            case Want::Readable:
                if (!readable) return;
                break;
            case Want::Result:
                if (!readable) return;
                if (!consumeInput()) {
                    w->lost = true;
                    break;
                }
                if (PQisBusy(conn_)) return;
                w->result = PgResult(lastResult());
                break;
            case Want::Flush:
                if (readable && !consumeInput()) {
                    w->lost = true;
                    break;
                }
                if (!hasEvent(ev, IoEvent::Write) && !hasEvent(ev, IoEvent::Error)) return;
                break;
        }
        endWait();
        // The request resumed here may destroy the connection: nothing touches
        // it afterwards.
        std::exchange(current_cont_, {}).resume();
    }

    // False, without waiting, when the connection is no longer watched (lost
    // or poisoned): the awaiter then reports why.
    bool beginWait(Wait& w, std::coroutine_handle<> h) {
        if (!socket_watch_) {
            w.lost = true;
            return false;
        }
        // No queue = unbounded (delegated to conninfo connect_timeout/statement_timeout).
        if (wait_deadlines_) wait_deadlines_->arm(deadline_);
        current_cont_ = h;
        wait_ = &w;
        if (w.want == Want::Flush) setWatchMask(IoEvent::Read | IoEvent::Write);
        return true;
    }

    void endWait() {
        wait_ = nullptr;
        deadline_.disarm();
        setWatchMask(IoEvent::Read);
    }

    // An awaiter destroyed before its wait ended, its coroutine frame destroyed
    // while suspended: what the server sends next answers nobody, so the
    // connection is poisoned.
    void abandon(const Wait& w) {
        if (wait_ != &w) return;
        wait_ = nullptr;
        current_cont_ = {};
        deadline_.disarm();
        dead_ = true;
        retireSocketWatch();
    }

    // A failed read loses the connection. Its watch is retired at once: epoll
    // keeps signalling a broken socket until the watch is removed.
    bool consumeInput() {
        if (PQconsumeInput(conn_)) return true;
        retireSocketWatch();
        return false;
    }

    // The last of a request's results, once PQisBusy is false.
    PGresult* lastResult() noexcept {
        PGresult* last = nullptr;
        while (PGresult* r = PQgetResult(conn_)) {
            if (last) PQclear(last);
            last = r;
        }
        return last;
    }

    // Allowed inside an event dispatch, unlike adding or removing a watch.
    void setWatchMask(IoEvent mask) {
        if (!socket_watch_ || socket_watch_->fd < 0 || watch_mask_ == mask) return;
        io_.updateWatch(socket_watch_->handle, mask);
        watch_mask_ = mask;
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

    // Waits for a request's whole result, read as it arrives.
    struct ResultAwaiter {
        PgConnection* self;
        Wait wait{Want::Result};

        ~ResultAwaiter() { self->abandon(wait); }

        bool await_ready() const noexcept { return false; }

        bool await_suspend(std::coroutine_handle<> h) { return self->beginWait(wait, h); }

        PgResult await_resume() {
            // dead_ before result.ok(): a timed-out read leaves no result;
            // surface the timeout, not "query failed".
            if (self->dead_)
                throw PgQueryTimeout("result wait exceeded query_timeout");
            // lost before result.ok(): a dropped socket also leaves no result,
            // but it is uncertain, not a deterministic server error.
            if (wait.lost)
                throw PgConnectionLost("connection lost reading result");
            if (!wait.result.ok())
                throw PgError("query failed");
            return std::move(wait.result);
        }
    };

    ResultAwaiter awaitResult() noexcept {
        return ResultAwaiter{this};
    }

    // The wait's deadline expired: the connection is poisoned (dead_) and no
    // longer watched, and the coroutine resumes from a posted task, never
    // inside the timer's dispatch. Its awaiter sees dead_ and throws
    // (PgQueryTimeout for a request, PgPoolTimeout for the handshake).
    static void expireWait(void* self) {
        static_cast<PgConnection*>(self)->timeoutCurrentWait();
    }

    void timeoutCurrentWait() {
        dead_ = true;
        connect_ = nullptr;    // a step already posted finds the handshake over
        wait_ = nullptr;
        // Posted before the resume, so it runs first.
        retireSocketWatch();
        if (auto h = std::exchange(current_cont_, {}))
            io_.post([h] { h.resume(); });
    }

    // The loop's watch on relais's own duplicate of libpq's socket. libpq may
    // close or replace its descriptor at any time (a failed read, a connect
    // step, PQfinish); the duplicate keeps the watched file open until the
    // watch is removed, so no watched descriptor is ever closed or reused under
    // the loop. Shared with the posted tasks that may outlive the connection:
    // once `owner` is null, they and the watch callback do nothing.
    struct SocketWatch {
        PgConnection* owner;
        Io& io;
        typename Io::WatchHandle handle{};
        int fd = -1;  // the duplicate; -1 while nothing is watched

        SocketWatch(PgConnection* o, Io& i) noexcept : owner(o), io(i) {}
        ~SocketWatch() { if (fd >= 0) ::close(fd); }
        SocketWatch(const SocketWatch&) = delete;
        SocketWatch& operator=(const SocketWatch&) = delete;
    };

    Io& io_;
    PGconn* conn_ = nullptr;
    std::shared_ptr<SocketWatch> socket_watch_;  // null once retired
    IoEvent watch_mask_ = IoEvent::None;
    ConnectAwaiter* connect_ = nullptr;          // the handshake in progress
    Wait* wait_ = nullptr;                       // the request's I/O wait in progress
    std::unordered_map<std::string, std::string> prepared_;

    // Pipeline bookkeeping: one entry per execution sent, true when a prepare
    // precedes it on the wire. Reset on pipeline entry and exit, so an aborted
    // batch leaves nothing behind.
    bool pipeline_prepare_queued_ = false;
    std::vector<uint8_t> pipeline_prepares_;
    size_t pipeline_read_pos_ = 0;

    // Timeout machinery. current_cont_ is the single in-flight continuation
    // per connection (the pipeline chains co_awaits sequentially), set by the
    // handshake and every wait so timeoutCurrentWait can resume it without
    // knowing which awaiter is parked. One deadline serves them all, armed in
    // acquire_deadlines_ for the handshake and its setup queries, in
    // query_deadlines_ afterwards; a null queue bounds nothing.
    bool dead_ = false;
    std::coroutine_handle<> current_cont_{};
    std::shared_ptr<Deadlines> acquire_deadlines_;
    std::shared_ptr<Deadlines> query_deadlines_;
    Deadlines* wait_deadlines_ = nullptr;  // what bounds the next I/O wait
    Deadline deadline_{this, &PgConnection::expireWait};

    SettleConfig settle_{};
    BackendId backend_{};
};

}  // namespace jcailloux::relais::io::detail

#endif // JCX_RELAIS_IO_PG_CONNECTION_H
