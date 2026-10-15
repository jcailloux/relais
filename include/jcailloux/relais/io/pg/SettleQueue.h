#ifndef JCX_RELAIS_IO_PG_SETTLE_QUEUE_H
#define JCX_RELAIS_IO_PG_SETTLE_QUEUE_H

#include <algorithm>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgConnection.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/pg/PgPool.h"
#include "jcailloux/relais/io/pg/SettleTicket.h"

namespace jcailloux::relais::io {

// SettleQueue — settles the uncertain writes of one event loop.
//
// A write abandoned on a timeout or a lost connection may still commit
// server-side. For each ticket, the queue waits until it no longer can — its
// backend terminated (Terminate) or gone (Await), or its delay elapsed (Delay) —
// then marks the ticket settled and runs its replays. Settling never decides
// whether the write committed.
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
    /// Tickets settled so far.
    [[nodiscard]] uint64_t settled() const noexcept { return settled_; }
    /// Tickets given up: settling was refused and no statement_timeout bounds the write.
    [[nodiscard]] uint64_t abandoned() const noexcept { return abandoned_; }
    /// Statements sent to settle a backend (terminate, or presence check).
    [[nodiscard]] uint64_t statements() const noexcept { return statements_; }

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
    // the write. No row = that backend is gone.
    static constexpr const char* kTerminateSql =
        "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity "
        "WHERE pid = $1 AND backend_start = $2::timestamptz";
    static constexpr const char* kPresentSql =
        "SELECT 1 FROM pg_stat_activity "
        "WHERE pid = $1 AND backend_start = $2::timestamptz";
    static constexpr const char* kStatementTimeoutSql =
        "SELECT setting::bigint FROM pg_settings WHERE name = 'statement_timeout'";

    // The tickets of one backend (Terminate, Await) or one ticket (Delay).
    struct Job {
        UncertainSettle mode = UncertainSettle::Terminate;
        BackendId backend;
        Clock::time_point next{};         // next attempt, or the end of the delay
        Clock::duration retry = kRetryFirst;
        std::vector<std::shared_ptr<SettleTicket>> tickets;
    };

    enum class Outcome : uint8_t { Settled, Retry, Refused };

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

            auto outcome = co_await attempt(*current_, conn);
            if (outcome == Outcome::Refused)
                outcome = co_await fallBack(*current_, conn);

            if (outcome == Outcome::Settled) {
                co_await settle();
                continue;
            }
            if (outcome == Outcome::Retry) {
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
    Task<Outcome> attempt(Job& job, std::optional<PgConnection<Io>>& conn) {
        if (job.mode == UncertainSettle::Delay) co_return Outcome::Settled;
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
            if (r.rows() == 0) co_return Outcome::Settled;            // gone
            if (terminate && r[0].get<bool>(0)) co_return Outcome::Settled;
            co_return Outcome::Retry;  // still there (Await), or not out after the wait
        } catch (const PgUncertainError& e) {
            RELAIS_LOG_DEBUG << "relais: settling postponed: " << e.what();
        } catch (const PgError& e) {
            // Still connected: the server answered, and refused.
            if (conn && conn->connected()) {
                RELAIS_LOG_DEBUG << "relais: settling refused: " << e.what();
                co_return Outcome::Refused;
            }
            RELAIS_LOG_DEBUG << "relais: settling postponed: " << e.what();
        } catch (const std::exception& e) {
            RELAIS_LOG_DEBUG << "relais: settling postponed: " << e.what();
        }
        conn.reset();
        co_return Outcome::Retry;
    }

    // The server refuses to settle the backend: the write is still bounded by
    // the statement_timeout of the role, if any. The job then waits that long.
    Task<Outcome> fallBack(Job& job, std::optional<PgConnection<Io>>& conn) {
        int64_t ms = 0;
        try {
            PgResult r = co_await conn->query(kStatementTimeoutSql);
            if (r.rows() == 1) ms = r[0].get<int64_t>(0);
        } catch (const PgUncertainError&) {
            conn.reset();
            co_return Outcome::Retry;
        } catch (const PgError&) {
            // Refused as well: nothing bounds the write that relais can read.
            if (!conn->connected()) {
                conn.reset();
                co_return Outcome::Retry;
            }
        } catch (const std::exception&) {
            conn.reset();
            co_return Outcome::Retry;
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
            co_return Outcome::Refused;  // requeued as is, until its delay ends
        }
        abandoned_ += job.tickets.size();
        for (auto& t : job.tickets) t->replays.clear();
        job.tickets.clear();
        co_return Outcome::Refused;
    }

    // In one synchronous step, every ticket of the job becomes Settled and gives
    // up its replays; a replay registered from now on runs at once. Then the
    // replays run, each failure logged without stopping the next ones.
    Task<void> settle() {
        std::vector<std::function<Task<void>()>> replays;
        for (auto& t : current_->tickets) {
            if (t->state == SettleTicket::State::Settled) continue;
            t->state = SettleTicket::State::Settled;
            ++settled_;
            for (auto& r : t->replays) replays.push_back(std::move(r));
            t->replays.clear();
        }
        current_.reset();
        for (auto& replay : replays) {
            try {
                co_await replay();
            } catch (const std::exception& e) {
                RELAIS_LOG_WARN << "relais: a replayed eviction failed: " << e.what();
            } catch (...) {
                RELAIS_LOG_WARN << "relais: a replayed eviction failed";
            }
        }
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
    uint64_t abandoned_ = 0;
    uint64_t statements_ = 0;
#ifdef RELAIS_BUILDING_TESTS
    bool paused_ = false;
#endif
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_SETTLE_QUEUE_H
