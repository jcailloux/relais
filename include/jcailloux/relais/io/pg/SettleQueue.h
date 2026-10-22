#ifndef JCX_RELAIS_IO_PG_SETTLE_QUEUE_H
#define JCX_RELAIS_IO_PG_SETTLE_QUEUE_H

#include <algorithm>
#include <array>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/WhenAll.h"
#include "jcailloux/relais/io/pg/PgConnection.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/pg/PgPool.h"
#include "jcailloux/relais/io/pg/SettleTicket.h"

namespace jcailloux::relais::io::detail {

// SettleQueue — settles the uncertain writes of one event loop.
//
// A write abandoned on a timeout or a lost connection may still commit
// server-side. For each ticket, the queue waits until it no longer can — its
// backend terminated (Terminate) or gone (Await), or its delay elapsed (Delay) —
// then marks the ticket settled and runs its replays.
//
// On the way, it reads what became of the write when the server can tell: the
// transaction its backend was running, then whether that transaction committed
// or aborted. Only when the ticket is the backend's sole unresolved write is
// that transaction known to be the write's; otherwise, or when the backend was
// never seen running one, the outcome stays Unknown.
//
// Off the hot path: the queue works only while a ticket waits. It talks to
// PostgreSQL over a connection of its own, outside the pool: the pool may be
// exhausted by connections blocked on the very lock the uncertain write holds,
// and settling is what frees them. That connection is closed once the queue is
// empty. Tickets naming the same backend are settled by one statement.
//
// Loop-thread only. Destroying the queue abandons its pending tickets (logged);
// it must be destroyed before its loop, like the pool.
template<IoContext Io>
class SettleQueue {
    using Clock = std::chrono::steady_clock;

public:
    SettleQueue(Io& io, std::shared_ptr<PgPool<Io>> pool)
        : io_(io), pool_(std::move(pool)) {}

    ~SettleQueue() {
        if (const auto n = pending())
            RELAIS_LOG_WARN << "relais: " << n << " uncertain write(s) abandoned unsettled "
                               "at shutdown";
        // A posted wake-up may still be queued: it finds no waiter.
        alarm_->waiting = {};
        ++alarm_->seq;
        if (alarm_armed_) io_.cancelTimer(alarm_token_);
        // Destroying a suspended worker runs its connection's destructor, which
        // drops the watch and the timer before closing the socket.
        if (worker_) worker_.destroy();
    }

    SettleQueue(const SettleQueue&) = delete;
    SettleQueue& operator=(const SettleQueue&) = delete;

    /// Settle `ticket` (null is ignored). Called from the handler of the failed
    /// write: the work starts on a later turn of the loop.
    void enqueue(std::shared_ptr<SettleTicket> ticket) {
        if (!ticket || ticket->state == SettleTicket::State::Settled) return;
        if (ticket->mode != UncertainSettle::Delay) {
            auto joins = [&](Job& job) {
                if (job.mode == UncertainSettle::Delay
                    || job.backend.pid != ticket->backend.pid
                    || job.backend.backend_start != ticket->backend.backend_start)
                    return false;
                if (std::find(job.tickets.begin(), job.tickets.end(), ticket) == job.tickets.end())
                    job.tickets.push_back(ticket);
                return true;
            };
            if (current_ && joins(*current_)) return;
            for (auto& job : jobs_)
                if (joins(job)) return;
        }
        Job job;
        job.mode = ticket->mode;
        job.backend = ticket->backend;
        job.next = Clock::now()
            + (ticket->mode == UncertainSettle::Delay ? Clock::duration{ticket->delay}
                                                       : Clock::duration{});
        job.tickets.push_back(std::move(ticket));
        jobs_.push_back(std::move(job));
        kick();
    }

    /// Tickets not settled yet, the one being worked included.
    [[nodiscard]] size_t pending() const noexcept {
        size_t n = current_ ? current_->tickets.size() : 0;
        for (const auto& job : jobs_) n += job.tickets.size();
        return n;
    }
    /// Replays of settled tickets still running (a replay may read the database).
    [[nodiscard]] size_t replaying() const noexcept { return replaying_; }
    /// Tickets settled so far.
    [[nodiscard]] uint64_t settled() const noexcept { return settled_; }
    /// Tickets given up: settling was refused and no statement_timeout bounds the write.
    [[nodiscard]] uint64_t abandoned() const noexcept { return abandoned_; }
    /// Statements sent to settle a backend (terminate, or presence check).
    [[nodiscard]] uint64_t statements() const noexcept { return statements_; }
    /// Tickets settled with this outcome.
    [[nodiscard]] uint64_t settledAs(WriteOutcome o) const noexcept {
        return outcomes_[static_cast<size_t>(o)];
    }

#ifdef RELAIS_BUILDING_TESTS
    /// Makes the next `count` attempts fail. Unreachable: PostgreSQL cannot be
    /// reached. Refused: the server refuses the settling statement.
    struct Fault {
        enum class Kind : uint8_t { Unreachable, Refused };
        Kind kind = Kind::Unreachable;
        int count = 0;
    };
    static inline Fault fault{};

    /// Holds the tickets without working them until resume().
    void pause() noexcept { paused_ = true; }
    void resume() {
        paused_ = false;
        wake();
    }
#endif

private:
    // First wait before another attempt, then doubled up to the cap.
    static constexpr Clock::duration kRetryFirst = std::chrono::milliseconds{50};
    static constexpr Clock::duration kRetryMax = std::chrono::seconds{2};
    // Server-side wait of pg_terminate_backend for the backend to exit.
    static constexpr std::chrono::milliseconds kTerminateWait{5000};

    // pid alone may name a later backend: backend_start pins the one that ran
    // the write. No row = that backend is gone. Both also read the transaction
    // the backend runs (NULL before its first change), as read before the
    // termination. backend_xid is 32 bits; pg_xact_status takes an xid8: the
    // widened xid is the one nearest the current snapshot's xmax, which a
    // running transaction may be just above or below.
    static constexpr const char* kTerminateSql =
        "SELECT pg_terminate_backend(a.pid, 5000), "
        "v.e - ((((v.e - v.x) & 4294967295) # 2147483648) - 2147483648) "
        "FROM pg_stat_activity a, LATERAL (SELECT "
        "pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS e, "
        "a.backend_xid::text::bigint AS x) v "
        "WHERE a.pid = $1 AND a.backend_start = $2::timestamptz";
    static constexpr const char* kPresentSql =
        "SELECT v.e - ((((v.e - v.x) & 4294967295) # 2147483648) - 2147483648) "
        "FROM pg_stat_activity a, LATERAL (SELECT "
        "pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS e, "
        "a.backend_xid::text::bigint AS x) v "
        "WHERE a.pid = $1 AND a.backend_start = $2::timestamptz";
    static constexpr const char* kXactStatusSql =
        "SELECT pg_xact_status($1::bigint::text::xid8)";
    static constexpr const char* kStatementTimeoutSql =
        "SELECT setting::bigint FROM pg_settings WHERE name = 'statement_timeout'";

    // The tickets of one backend (Terminate, Await) or one ticket (Delay).
    struct Job {
        UncertainSettle mode = UncertainSettle::Terminate;
        BackendId backend;
        Clock::time_point next{};         // next attempt, or the end of the delay
        Clock::duration retry = kRetryFirst;
        std::vector<std::shared_ptr<SettleTicket>> tickets;
        std::optional<int64_t> xid;       // a transaction seen running on the backend
    };

    enum class Step : uint8_t { Settled, Retry, Refused };

    // Owned, eager: the queue destroys the frame, suspended or finished.
    struct Worker {
        struct promise_type {
            Worker get_return_object() noexcept {
                return Worker{std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_never initial_suspend() noexcept { return {}; }
            std::suspend_always final_suspend() noexcept { return {}; }
            void return_void() noexcept {}
            void unhandled_exception() noexcept {}  // the next enqueue starts a new worker
        };
        std::coroutine_handle<promise_type> handle;
    };

    // The worker's sleep. Timer and wake-up both go through a shared alarm with
    // a sequence number: a superseded or late timer finds another number and does
    // nothing, and the resume is posted, never run inside the timer dispatch.
    struct Alarm {
        std::coroutine_handle<> waiting{};
        uint64_t seq = 0;
    };

    struct Nap {
        SettleQueue* q;
        Clock::duration d;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            q->alarm_->waiting = h;
            q->arm(d);
        }
        void await_resume() noexcept { q->alarm_armed_ = false; }
    };

    void arm(Clock::duration d) {
        if (alarm_armed_) io_.cancelTimer(alarm_token_);
        const auto seq = ++alarm_->seq;
        alarm_token_ = io_.postDelayed(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::max(d, Clock::duration{})),
            [a = alarm_, seq, io = &io_] {
                io->post([a, seq] {
                    if (a->seq != seq || !a->waiting) return;
                    std::exchange(a->waiting, {}).resume();
                });
            });
        alarm_armed_ = true;
    }

    void wake() {
        if (alarm_->waiting) arm(Clock::duration{});
    }

    void kick() {
        if (worker_ && !worker_.done()) {
            wake();
            return;
        }
        if (worker_) worker_.destroy();
        worker_ = run().handle;
    }

    Worker run() {
        // Leave the handler of the failed write before any work.
        co_await Nap{this, Clock::duration{}};
        std::optional<PgConnection<Io>> conn;
        while (!jobs_.empty()) {
#ifdef RELAIS_BUILDING_TESTS
            if (paused_) {
                co_await Nap{this, std::chrono::hours{1}};
                continue;
            }
#endif
            auto due = std::min_element(jobs_.begin(), jobs_.end(),
                [](const Job& a, const Job& b) { return a.next < b.next; });
            const auto now = Clock::now();
            if (due->next > now) {
                co_await Nap{this, due->next - now};
                continue;
            }
            current_ = std::move(*due);
            jobs_.erase(due);

            auto step = co_await attempt(*current_, conn);
            if (step == Step::Refused)
                step = co_await fallBack(*current_, conn);

            if (step == Step::Settled) {
                co_await settle(conn);
                continue;
            }
            if (step == Step::Retry) {
                current_->next = Clock::now() + current_->retry;
                current_->retry = std::min(current_->retry * 2, kRetryMax);
            }
            // A refused job comes back as a delay, or empty once abandoned.
            if (!current_->tickets.empty()) jobs_.push_back(std::move(*current_));
            current_.reset();
        }
    }

    // One step on the job's backend. A connection failure, or a statement that
    // may not have run, is retried; a server refusal is reported.
    Task<Step> attempt(Job& job, std::optional<PgConnection<Io>>& conn) {
        if (job.mode == UncertainSettle::Delay) co_return Step::Settled;
        try {
            co_await connect(conn);
#ifdef RELAIS_BUILDING_TESTS
            if (fault.count > 0 && fault.kind == Fault::Kind::Refused) {
                --fault.count;
                throw PgError("injected: settling refused");
            }
#endif
            const bool terminate = job.mode == UncertainSettle::Terminate;
            ++statements_;
            PgResult r = co_await conn->queryParams(terminate ? kTerminateSql : kPresentSql,
                PgParams::make(job.backend.pid, job.backend.backend_start));
            if (r.rows() == 0) co_return Step::Settled;            // gone
            if (auto xid = r[0].getOpt<int64_t>(terminate ? 1 : 0)) job.xid = xid;
            if (terminate && r[0].get<bool>(0)) co_return Step::Settled;
            co_return Step::Retry;  // still there (Await), or not out after the wait
        } catch (const PgUncertainError& e) {
            RELAIS_LOG_DEBUG << "relais: settling postponed: " << e.what();
        } catch (const PgError& e) {
            // Still connected: the server answered, and refused.
            if (conn && conn->connected()) {
                RELAIS_LOG_DEBUG << "relais: settling refused: " << e.what();
                co_return Step::Refused;
            }
            RELAIS_LOG_DEBUG << "relais: settling postponed: " << e.what();
        } catch (const std::exception& e) {
            RELAIS_LOG_DEBUG << "relais: settling postponed: " << e.what();
        }
        conn.reset();
        co_return Step::Retry;
    }

    // The server refuses to settle the backend: the write is still bounded by
    // the statement_timeout of the role, if any. The job then waits that long.
    Task<Step> fallBack(Job& job, std::optional<PgConnection<Io>>& conn) {
        int64_t ms = 0;
        try {
            PgResult r = co_await conn->query(kStatementTimeoutSql);
            if (r.rows() == 1) ms = r[0].get<int64_t>(0);
        } catch (const PgUncertainError&) {
            conn.reset();
            co_return Step::Retry;
        } catch (const PgError&) {
            // Refused as well: nothing bounds the write that relais can read.
            if (!conn->connected()) {
                conn.reset();
                co_return Step::Retry;
            }
        } catch (const std::exception&) {
            conn.reset();
            co_return Step::Retry;
        }
        if (!refusal_logged_) {
            refusal_logged_ = true;
            RELAIS_LOG_ERROR << "relais: the server refuses to terminate the backend of an "
                                "uncertain write; "
                             << (ms > 0 ? "settling waits statement_timeout instead"
                                        : "no statement_timeout bounds it, it stays unsettled");
        }
        if (ms > 0) {
            job.mode = UncertainSettle::Delay;
            job.next = Clock::now() + std::chrono::milliseconds{ms} + kSettleMargin;
            co_return Step::Refused;  // requeued as is, until its delay ends
        }
        abandoned_ += job.tickets.size();
        for (auto& t : job.tickets) t->replays.clear();
        job.tickets.clear();
        co_return Step::Refused;
    }

    // The backend is gone, or its delay elapsed. With one ticket, itself the
    // backend's sole unresolved write, the transaction seen running was the
    // write's: the server tells whether it committed. Anything else is Unknown.
    Task<WriteOutcome> readOutcome(const Job& job, std::optional<PgConnection<Io>>& conn) {
        if (job.mode == UncertainSettle::Delay || !job.xid || job.tickets.size() != 1
            || job.tickets.front()->unresolved != 1 || !conn || !conn->connected())
            co_return WriteOutcome::Unknown;
        try {
            PgResult r = co_await conn->queryParams(kXactStatusSql, PgParams::make(*job.xid));
            const auto status = r.rows() == 1 ? r[0].getOpt<std::string>(0) : std::nullopt;
            if (status == "aborted") co_return WriteOutcome::Aborted;
            if (status == "committed") co_return WriteOutcome::Committed;
        } catch (const std::exception& e) {
            RELAIS_LOG_DEBUG << "relais: the outcome of an uncertain write is unknown: "
                             << e.what();
            if (!conn->connected()) conn.reset();
        }
        co_return WriteOutcome::Unknown;
    }

    // In one synchronous step, every ticket of the job takes the outcome,
    // becomes Settled and gives up its replays; a replay registered from now on
    // runs at once. Then the replays start in registration order, together: one
    // that reads the database does not hold back the evictions registered after
    // it. A failure is logged without stopping the others.
    Task<void> settle(std::optional<PgConnection<Io>>& conn) {
        auto outcome = co_await readOutcome(*current_, conn);
        // A ticket joined while the status was read: the transaction may be its.
        if (current_->tickets.size() != 1) outcome = WriteOutcome::Unknown;
        std::vector<std::function<Task<void>(WriteOutcome)>> replays;
        for (auto& t : current_->tickets) {
            if (t->state == SettleTicket::State::Settled) continue;
            t->outcome = outcome;
            t->state = SettleTicket::State::Settled;
            ++settled_;
            ++outcomes_[static_cast<size_t>(outcome)];
            for (auto& r : t->replays) replays.push_back(std::move(r));
            t->replays.clear();
        }
        current_.reset();
        std::vector<Task<void>> runs;
        runs.reserve(replays.size());
        for (auto& replay : replays) runs.push_back(runReplay(std::move(replay), outcome));
        replaying_ += runs.size();
        co_await whenAll(std::span<Task<void>>(runs));
    }

    Task<void> runReplay(std::function<Task<void>(WriteOutcome)> replay, WriteOutcome outcome) {
        try {
            co_await replay(outcome);
        } catch (const std::exception& e) {
            RELAIS_LOG_WARN << "relais: a replayed eviction failed: " << e.what();
        } catch (...) {
            RELAIS_LOG_WARN << "relais: a replayed eviction failed";
        }
        --replaying_;
    }

    Task<void> connect(std::optional<PgConnection<Io>>& conn) {
#ifdef RELAIS_BUILDING_TESTS
        if (fault.count > 0 && fault.kind == Fault::Kind::Unreachable) {
            --fault.count;
            conn.reset();
            throw PgConnectionError("injected: PostgreSQL unreachable");
        }
#endif
        if (conn && conn->connected()) co_return;
        conn.reset();
        const auto& cfg = pool_->config();
        // The statements wait up to kTerminateWait server-side: the I/O bound
        // leaves them that time whatever the pool's query_timeout.
        const auto query_timeout = std::max<std::chrono::milliseconds>(
            cfg.query_timeout, kTerminateWait + std::chrono::seconds{5});
        conn.emplace(co_await PgConnection<Io>::connect(
            io_, pool_->conninfo().c_str(), cfg.acquire_timeout, query_timeout));
    }

    Io& io_;
    std::shared_ptr<PgPool<Io>> pool_;

    std::vector<Job> jobs_;
    std::optional<Job> current_;  // the job being worked, out of jobs_

    std::coroutine_handle<typename Worker::promise_type> worker_{};
    std::shared_ptr<Alarm> alarm_ = std::make_shared<Alarm>();
    typename Io::TimerToken alarm_token_{};
    bool alarm_armed_ = false;

    bool refusal_logged_ = false;
    uint64_t settled_ = 0;
    size_t replaying_ = 0;
    uint64_t abandoned_ = 0;
    uint64_t statements_ = 0;
    std::array<uint64_t, 3> outcomes_{};
#ifdef RELAIS_BUILDING_TESTS
    bool paused_ = false;
#endif
};

}  // namespace jcailloux::relais::io::detail

#endif // JCX_RELAIS_IO_PG_SETTLE_QUEUE_H
