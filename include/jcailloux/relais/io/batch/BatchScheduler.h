#ifndef JCX_RELAIS_IO_BATCH_SCHEDULER_H
#define JCX_RELAIS_IO_BATCH_SCHEDULER_H

#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/EpollIoContext.h"
#include "jcailloux/relais/io/pg/PgPool.h"
#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/redis/RedisPool.h"
#include "jcailloux/relais/io/redis/RedisResult.h"
#include "jcailloux/relais/io/batch/TimingEstimator.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_bench { struct BatchBenchAccessor; }
#endif

namespace jcailloux::relais::io::batch {

// BatchScheduler — single-threaded adaptive batching for PG and Redis.
//
// This scheduler is mono-thread: one instance per event loop worker.
// No internal mutexes — the DbProvider thread_local dispatch guarantees
// that all calls come from the same event loop.
//
// Batching strategy:
// - Reads: SELECT ... WHERE id = ANY($1) for entities of same repo,
//   pipelining for different repos/lists, with sync between segments.
// - Writes: pipelined with sync after each, preserving sequence order.
// - Redis: reads + writes in the same pipeline (Redis guarantees order).
// - Coalescing: identical queries share the same result.
//
// Budget: ConcurrencyGate limits total in-flight PG + Redis sends.

template<IoContext Io>
class BatchScheduler {
    using Clock = std::chrono::steady_clock;
    using TimerToken = typename EpollIoContext::TimerToken;

public:
    BatchScheduler(Io& io,
                   std::shared_ptr<PgPool<Io>> pg_pool,
                   std::shared_ptr<RedisPool<Io>> redis_pool,
                   int max_concurrent = 8)
        : io_(io)
        , pg_pool_(std::move(pg_pool))
        , redis_pool_(std::move(redis_pool))
        , gate_{max_concurrent}
    {}

    ~BatchScheduler() = default;

    BatchScheduler(const BatchScheduler&) = delete;
    BatchScheduler& operator=(const BatchScheduler&) = delete;

    // =========================================================================
    // Public API — submit queries for batching
    // =========================================================================

    /// Submit an entity read (will be batched via ANY array if batch_sql != nullptr).
    /// batch_sql: SELECT ... WHERE pk = ANY($1) — null means use single_sql.
    /// single_sql: SELECT ... WHERE pk = $1 (fallback / prepare).
    /// key_params: parameters for the primary key.
    Task<PgResult> submitEntityRead(const char* batch_sql, const char* single_sql,
                                     PgParams key_params) {
        if (!batch_sql) {
            // No batch SQL available — submit as a regular query read
            co_return co_await submitQueryRead(single_sql, std::move(key_params));
        }

        PgReadEntry entry;
        entry.batch_sql = batch_sql;
        entry.single_sql = single_sql;
        entry.params = std::move(key_params);
        entry.is_entity = true;

        co_return co_await submitPgRead(std::move(entry));
    }

    /// Submit a list/custom query read (pipelined, not batched via ANY).
    Task<PgResult> submitQueryRead(const char* sql, PgParams params) {
        PgReadEntry entry;
        entry.batch_sql = nullptr;
        entry.single_sql = sql;
        entry.params = std::move(params);
        entry.is_entity = false;

        co_return co_await submitPgRead(std::move(entry));
    }

    /// Result of a pipelined PG write, with coalescing indicator.
    struct WriteResult {
        PgResult result;
        bool coalesced = false;
    };

    /// Submit a PG write (INSERT/UPDATE/DELETE RETURNING).
    /// Returns {PgResult, coalesced}. coalesced=true means an identical
    /// write (same SQL + same params) was already in the batch and this
    /// caller received the leader's result without a DB round-trip.
    Task<WriteResult> submitPgWrite(const char* sql, PgParams params) {
        PgWriteEntry entry;
        entry.sql = sql;
        entry.params = std::move(params);
        entry.seq = next_write_seq_++;

        co_return co_await submitPgWriteEntry(std::move(entry));
    }

    /// Submit a PG execute (DELETE, affected rows).
    /// Returns {affected_rows, coalesced}.
    Task<std::pair<int, bool>> submitPgExecute(const char* sql, PgParams params) {
        auto [result, coalesced] = co_await submitPgWrite(sql, std::move(params));
        co_return std::pair{result.affectedRows(), coalesced};
    }

    /// Submit a Redis command (read or write — Redis pipeline handles both).
    Task<RedisResult> submitRedis(int argc, const char** argv, const size_t* argvlen) {
        // Build owned copies of args for the coroutine frame
        RedisEntry entry;
        entry.args.reserve(argc);
        for (int i = 0; i < argc; ++i) {
            entry.args.emplace_back(argv[i], argvlen[i]);
        }

        co_return co_await submitRedisEntry(std::move(entry));
    }

    /// Direct query bypass — for BEGIN/COMMIT/ROLLBACK/SET.
    /// Acquires a connection and executes directly, no batching.
    Task<PgResult> directQuery(const char* sql) {
        auto guard = co_await pg_pool_->acquire();
        co_return co_await guard.conn().query(sql);
    }

    Task<PgResult> directQueryParams(const char* sql, const PgParams& params) {
        auto guard = co_await pg_pool_->acquire();
        co_return co_await guard.conn().queryParams(sql, params);
    }

    Task<int> directExecute(const char* sql, const PgParams& params) {
        auto guard = co_await pg_pool_->acquire();
        co_return co_await guard.conn().execute(sql, params);
    }

    /// Access the timing estimator (for testing/diagnostics).
    [[nodiscard]] const TimingEstimator& estimator() const noexcept { return estimator_; }

private:
    // =========================================================================
    // Internal data structures
    // =========================================================================

    struct PgReadEntry {
        const char* batch_sql = nullptr;   // ANY batch SQL (null if not entity)
        const char* single_sql = nullptr;  // Single-row / list SQL
        PgParams params;
        bool is_entity = false;

        // Set by submitPgRead when the entry is added to a batch
        std::coroutine_handle<> continuation{};
        PgResult result;
        int64_t processing_time_us = 0;
    };

    struct PgWriteEntry {
        const char* sql = nullptr;
        PgParams params;
        uint64_t seq = 0;

        std::coroutine_handle<> continuation{};
        PgResult result;
        int64_t processing_time_us = 0;
        bool coalesced = false;
        std::vector<PgWriteEntry*> followers;
    };

    struct RedisEntry {
        std::vector<std::string> args;   // Owned copies

        std::coroutine_handle<> continuation{};
        RedisResult result;
    };

    struct PgReadBatch {
        std::vector<PgReadEntry*> entries;
        double cost_ns = 0;
        TimerToken timer{0};
        bool timer_active = false;
    };

    struct PgWriteBatch {
        std::vector<PgWriteEntry*> entries;
        double cost_ns = 0;
        TimerToken timer{0};
        bool timer_active = false;
    };

    struct RedisBatch {
        std::vector<RedisEntry*> entries;
        TimerToken timer{0};
        bool timer_active = false;
    };

    // =========================================================================
    // ConcurrencyGate — coroutine semaphore for shared PG+Redis budget
    // =========================================================================

    struct ConcurrencyGate {
        int max_concurrent;
        int inflight = 0;
        std::deque<std::coroutine_handle<>> waiters;

        struct Awaiter {
            ConcurrencyGate* gate;

            bool await_ready() const noexcept {
                return gate->inflight < gate->max_concurrent;
            }

            void await_suspend(std::coroutine_handle<> h) {
                gate->waiters.push_back(h);
            }

            void await_resume() noexcept {
                ++gate->inflight;
            }
        };

        Awaiter acquire() { return {this}; }

        void release() {
            --inflight;
            if (!waiters.empty()) {
                auto next = waiters.front();
                waiters.pop_front();
                next.resume(); // await_resume() does ++inflight
            }
        }
    };

    // =========================================================================
    // Submit helpers — add entry to batch, schedule departure
    // =========================================================================

    struct PgReadAwaiter {
        BatchScheduler* self;
        PgReadEntry entry;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            entry.continuation = h;
            self->addToPgReadBatch(&entry);
        }

        PgResult await_resume() {
            return std::move(entry.result);
        }
    };

    Task<PgResult> submitPgRead(PgReadEntry entry) {
        if (estimator_.isPgBootstrapping() || estimator_.isPgStale()) {
            co_return co_await sendSinglePgRead(std::move(entry));
        }

        // Nagle: first query goes direct, subsequent accumulate during RTT
        if (!pg_read_inflight_) {
            pg_read_inflight_ = true;
            PgResult result;
            try {
                result = co_await sendSinglePgRead(std::move(entry));
            } catch (...) {
                pg_read_inflight_ = false;
                firePgReadBatchNow();
                throw;
            }
            pg_read_inflight_ = false;
            firePgReadBatchNow(); // flush accumulated during RTT
            co_return result;
        }

        PgReadAwaiter awaiter{this, std::move(entry)};
        co_return co_await awaiter;
    }

    struct PgWriteAwaiter {
        BatchScheduler* self;
        PgWriteEntry entry;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            entry.continuation = h;
            self->addToPgWriteBatch(&entry);
        }

        WriteResult await_resume() {
            return {std::move(entry.result), entry.coalesced};
        }
    };

    Task<WriteResult> submitPgWriteEntry(PgWriteEntry entry) {
        if (estimator_.isPgBootstrapping() || estimator_.isPgStale()) {
            auto result = co_await sendSinglePgWrite(std::move(entry));
            co_return WriteResult{std::move(result), false};
        }

        // Nagle: first query goes direct, subsequent accumulate during RTT
        if (!pg_write_inflight_) {
            pg_write_inflight_ = true;
            PgResult result;
            try {
                result = co_await sendSinglePgWrite(std::move(entry));
            } catch (...) {
                pg_write_inflight_ = false;
                firePgWriteBatchNow();
                throw;
            }
            pg_write_inflight_ = false;
            firePgWriteBatchNow(); // flush accumulated during RTT
            co_return WriteResult{std::move(result), false};
        }

        PgWriteAwaiter awaiter{this, std::move(entry)};
        co_return co_await awaiter;
    }

    struct RedisAwaiter {
        BatchScheduler* self;
        RedisEntry entry;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            entry.continuation = h;
            self->addToRedisBatch(&entry);
        }

        RedisResult await_resume() {
            return std::move(entry.result);
        }
    };

    Task<RedisResult> submitRedisEntry(RedisEntry entry) {
        if (!redis_pool_ || redis_pool_->empty())
            throw RedisError("Redis pool not configured");

        if (estimator_.isRedisBootstrapping() || estimator_.isRedisStale()) {
            co_return co_await sendSingleRedis(std::move(entry));
        }

        // Nagle: first query goes direct, subsequent accumulate during RTT
        if (!redis_inflight_) {
            redis_inflight_ = true;
            RedisResult result;
            try {
                result = co_await sendSingleRedis(std::move(entry));
            } catch (...) {
                redis_inflight_ = false;
                fireRedisBatchNow();
                throw;
            }
            redis_inflight_ = false;
            fireRedisBatchNow(); // flush accumulated during RTT
            co_return result;
        }

        // In-flight → accumulate in batch
        RedisAwaiter awaiter{this, std::move(entry)};
        co_return co_await awaiter;
    }

    // =========================================================================
    // Batch management
    // =========================================================================

    void addToPgReadBatch(PgReadEntry* entry) {
        double entry_cost = estimator_.getRequestTime(
            entry->batch_sql ? entry->batch_sql : entry->single_sql);

        if (pg_read_batch_.entries.empty()) {
            // First entry — schedule departure timer
            pg_read_batch_.cost_ns = entry_cost;
            pg_read_batch_.entries.push_back(entry);
            schedulePgReadDeparture();
        } else {
            pg_read_batch_.cost_ns += entry_cost;
            pg_read_batch_.entries.push_back(entry);
            checkPgReadBatchReady();
        }
    }

    void addToPgWriteBatch(PgWriteEntry* entry) {
        // Write coalescing: if an identical write (same SQL + same params)
        // is already in the batch, attach as follower instead of adding
        // a new entry. The follower will receive the leader's result.
        for (auto* existing : pg_write_batch_.entries) {
            if (existing->sql == entry->sql && existing->params == entry->params) {
                entry->coalesced = true;
                existing->followers.push_back(entry);
                return;
            }
        }

        if (pg_write_batch_.entries.empty()) {
            pg_write_batch_.entries.push_back(entry);
            schedulePgWriteDeparture();
        } else {
            pg_write_batch_.entries.push_back(entry);
            checkPgWriteBatchReady();
        }
    }

    void addToRedisBatch(RedisEntry* entry) {
        if (redis_batch_.entries.empty()) {
            redis_batch_.entries.push_back(entry);
            scheduleRedisDeparture();
        } else {
            redis_batch_.entries.push_back(entry);
            checkRedisBatchReady();
        }
    }

    // =========================================================================
    // Batch readiness checks
    // =========================================================================

    static constexpr int kMaxBatchEntries = 512;

    void checkPgReadBatchReady() {
        if (pg_read_batch_.cost_ns >= estimator_.pg_network_time_ns ||
            static_cast<int>(pg_read_batch_.entries.size()) >= kMaxBatchEntries)
        {
            firePgReadBatchNow();
        }
    }

    void checkPgWriteBatchReady() {
        if (static_cast<int>(pg_write_batch_.entries.size()) >= kMaxBatchEntries) {
            firePgWriteBatchNow();
        }
    }

    void checkRedisBatchReady() {
        if (static_cast<int>(redis_batch_.entries.size()) >= kMaxBatchEntries) {
            fireRedisBatchNow();
        }
    }

    // =========================================================================
    // Timer scheduling
    // =========================================================================

    void schedulePgReadDeparture() {
        auto delay_ns = static_cast<int64_t>(estimator_.pg_network_time_ns);
        if (delay_ns <= 0) delay_ns = 100'000; // 100us minimum
        auto delay = std::chrono::nanoseconds(delay_ns);

        pg_read_batch_.timer = static_cast<EpollIoContext&>(io_).postDelayed(delay, [this] {
            pg_read_batch_.timer_active = false;
            firePgReadBatchNow();
        });
        pg_read_batch_.timer_active = true;
    }

    void schedulePgWriteDeparture() {
        auto delay_ns = static_cast<int64_t>(estimator_.pg_network_time_ns);
        if (delay_ns <= 0) delay_ns = 100'000;
        auto delay = std::chrono::nanoseconds(delay_ns);

        pg_write_batch_.timer = static_cast<EpollIoContext&>(io_).postDelayed(delay, [this] {
            pg_write_batch_.timer_active = false;
            firePgWriteBatchNow();
        });
        pg_write_batch_.timer_active = true;
    }

    void scheduleRedisDeparture() {
        auto delay_ns = static_cast<int64_t>(estimator_.redis_network_time_ns);
        if (delay_ns <= 0) delay_ns = 50'000; // 50us minimum
        auto delay = std::chrono::nanoseconds(delay_ns);

        redis_batch_.timer = static_cast<EpollIoContext&>(io_).postDelayed(delay, [this] {
            redis_batch_.timer_active = false;
            fireRedisBatchNow();
        });
        redis_batch_.timer_active = true;
    }

    // =========================================================================
    // Batch firing
    // =========================================================================

    void firePgReadBatchNow() {
        if (pg_read_batch_.entries.empty()) return;

        // Cancel timer if still active
        if (pg_read_batch_.timer_active) {
            static_cast<EpollIoContext&>(io_).cancelTimer(pg_read_batch_.timer);
            pg_read_batch_.timer_active = false;
        }

        // Move batch out and reset
        auto entries = std::move(pg_read_batch_.entries);
        pg_read_batch_ = {};

        // Launch fire coroutine as detached task
        firePgReadBatch(std::move(entries));
    }

    void firePgWriteBatchNow() {
        if (pg_write_batch_.entries.empty()) return;

        if (pg_write_batch_.timer_active) {
            static_cast<EpollIoContext&>(io_).cancelTimer(pg_write_batch_.timer);
            pg_write_batch_.timer_active = false;
        }

        auto entries = std::move(pg_write_batch_.entries);
        pg_write_batch_ = {};

        firePgWriteBatch(std::move(entries));
    }

    void fireRedisBatchNow() {
        if (redis_batch_.entries.empty()) return;

        if (redis_batch_.timer_active) {
            static_cast<EpollIoContext&>(io_).cancelTimer(redis_batch_.timer);
            redis_batch_.timer_active = false;
        }

        auto entries = std::move(redis_batch_.entries);
        redis_batch_ = {};

        fireRedisBatch(std::move(entries));
    }

    // =========================================================================
    // PG Read batch execution
    // =========================================================================

    DetachedTask firePgReadBatch(std::vector<PgReadEntry*> entries) {
        co_await gate_.acquire();
        auto guard = co_await pg_pool_->acquire();
        auto& conn = guard.conn();

        try {
            conn.enterPipelineMode();

            // Group entity reads by batch_sql, send list/query reads as individual segments
            struct Segment {
                const char* sql;       // SQL for this segment
                PgParams params;       // Combined params (for entity: ANY array; for list: original)
                std::vector<PgReadEntry*> waiters;  // Entries waiting for this segment's result
                bool is_any = false;   // True if this is an ANY-batch segment
            };
            std::vector<Segment> segments;

            // Group entity reads by batch_sql pointer
            std::unordered_map<const char*, std::vector<PgReadEntry*>> entity_groups;
            for (auto* e : entries) {
                if (e->is_entity && e->batch_sql) {
                    entity_groups[e->batch_sql].push_back(e);
                } else {
                    // List/query: individual segment
                    Segment seg;
                    seg.sql = e->single_sql;
                    seg.params = std::move(e->params);
                    seg.waiters.push_back(e);
                    seg.is_any = false;
                    segments.push_back(std::move(seg));
                }
            }

            // Build ANY segments from entity groups
            for (auto& [sql, group] : entity_groups) {
                // Build the ANY array as a PG array literal: {val1,val2,...}
                // For simple keys: single param as PG array text format
                // For now: send each entity as individual pipelined queries
                // (ANY batching requires generated batch SQL — Phase 5)
                for (auto* e : group) {
                    Segment seg;
                    seg.sql = e->single_sql;
                    seg.params = std::move(e->params);
                    seg.waiters.push_back(e);
                    seg.is_any = false;
                    segments.push_back(std::move(seg));
                }
            }

            // Pipeline all segments with sync between each
            int n_prepares = 0;
            for (auto& seg : segments) {
                if (conn.ensurePreparedPipelined(seg.sql, seg.params.count())) {
                    conn.pipelineSync();
                    ++n_prepares;
                }
                conn.sendPreparedPipelined(seg.sql, seg.params);
                conn.pipelineSync();
            }

            co_await conn.flushPipeline();

            // Read prepare results
            for (int i = 0; i < n_prepares; ++i) {
                co_await readAndDiscardPipelineResult(conn);
            }

            // Read segment results
            auto results = co_await conn.readPipelineResults(
                static_cast<int>(segments.size()));

            conn.exitPipelineMode();

            // Distribute results to waiters
            for (size_t i = 0; i < segments.size(); ++i) {
                auto& seg = segments[i];
                auto& pr = results[i];

                for (auto* waiter : seg.waiters) {
                    waiter->result = std::move(pr.result);
                    waiter->processing_time_us = pr.processing_time_us;
                }

                // Update timing estimator
                if (seg.is_any && !seg.waiters.empty()) {
                    auto* first_waiter = seg.waiters[0];
                    estimator_.updateSqlTimingPerKey(
                        first_waiter->batch_sql ? first_waiter->batch_sql : first_waiter->single_sql,
                        static_cast<int>(seg.waiters.size()),
                        static_cast<double>(pr.processing_time_us) * 1000.0);
                }
            }

            // Update network time if single-entry batch
            if (entries.size() == 1 && !results.empty()) {
                estimator_.updatePgNetworkTime(
                    static_cast<double>(results[0].processing_time_us) * 1000.0,
                    estimator_.getRequestTime(entries[0]->single_sql));
            }

        } catch (...) {
            // On error, try to exit pipeline mode gracefully
            try { conn.exitPipelineMode(); } catch (...) {}

            // Resume all waiters with empty results
            for (auto* e : entries) {
                e->result = PgResult{};
            }
        }

        gate_.release();

        // Resume all waiting coroutines
        for (auto* e : entries) {
            if (e->continuation) {
                e->continuation.resume();
            }
        }

        // Chain: fire next accumulated batch or clear inflight
        if (!pg_read_batch_.entries.empty()) {
            firePgReadBatchNow();
        } else {
            pg_read_inflight_ = false;
        }
    }

    // =========================================================================
    // PG Write batch execution
    // =========================================================================

    DetachedTask firePgWriteBatch(std::vector<PgWriteEntry*> entries) {
        // Sort by sequence number
        std::sort(entries.begin(), entries.end(),
            [](const auto* a, const auto* b) { return a->seq < b->seq; });

        co_await gate_.acquire();
        auto guard = co_await pg_pool_->acquire();
        auto& conn = guard.conn();

        try {
            conn.enterPipelineMode();

            int n_prepares = 0;
            for (auto* e : entries) {
                if (conn.ensurePreparedPipelined(e->sql, e->params.count())) {
                    conn.pipelineSync();
                    ++n_prepares;
                }
                conn.sendPreparedPipelined(e->sql, e->params);
                conn.pipelineSync();
            }

            co_await conn.flushPipeline();

            // Read prepare results
            for (int i = 0; i < n_prepares; ++i) {
                co_await readAndDiscardPipelineResult(conn);
            }

            // Read write results
            auto results = co_await conn.readPipelineResults(
                static_cast<int>(entries.size()));

            conn.exitPipelineMode();

            // Distribute results to leaders and their followers
            for (size_t i = 0; i < entries.size(); ++i) {
                entries[i]->result = std::move(results[i].result);
                entries[i]->processing_time_us = results[i].processing_time_us;
                for (auto* f : entries[i]->followers) {
                    f->result = entries[i]->result; // shared_ptr copy
                }
            }

        } catch (...) {
            try { conn.exitPipelineMode(); } catch (...) {}
            for (auto* e : entries) {
                e->result = PgResult{};
                for (auto* f : e->followers) {
                    f->result = PgResult{};
                }
            }
        }

        gate_.release();

        // Collect all continuation handles BEFORE resuming any.
        // Resuming a leader can destroy its coroutine frame (symmetric
        // transfer chain), which would make e->followers a dangling pointer.
        std::vector<std::coroutine_handle<>> to_resume;
        to_resume.reserve(entries.size() * 2);
        for (auto* e : entries) {
            if (e->continuation) to_resume.push_back(e->continuation);
            for (auto* f : e->followers) {
                if (f->continuation) to_resume.push_back(f->continuation);
            }
        }
        for (auto h : to_resume) {
            h.resume();
        }

        // Chain: fire next accumulated batch or clear inflight
        if (!pg_write_batch_.entries.empty()) {
            firePgWriteBatchNow();
        } else {
            pg_write_inflight_ = false;
        }
    }

    // =========================================================================
    // Redis batch execution
    // =========================================================================

    DetachedTask fireRedisBatch(std::vector<RedisEntry*> entries) {
        co_await gate_.acquire();

        try {
            auto& client = redis_pool_->next();

            // Build argv arrays for each entry
            struct CmdBuf {
                std::vector<const char*> argv;
                std::vector<size_t> argvlen;
            };
            std::vector<CmdBuf> bufs;
            bufs.reserve(entries.size());
            for (auto* e : entries) {
                CmdBuf buf;
                buf.argv.reserve(e->args.size());
                buf.argvlen.reserve(e->args.size());
                for (const auto& a : e->args) {
                    buf.argv.push_back(a.data());
                    buf.argvlen.push_back(a.size());
                }
                bufs.push_back(std::move(buf));
            }

            // Build pipeline command descriptors
            using PCmd = typename RedisClient<Io>::PipelineCmd;
            std::vector<PCmd> cmds;
            cmds.reserve(entries.size());
            for (auto& buf : bufs) {
                cmds.push_back({static_cast<int>(buf.argv.size()),
                                buf.argv.data(), buf.argvlen.data()});
            }

            // Pipeline: one lock, one flush, one read
            auto start = Clock::now();
            auto results = co_await client.pipelineExec(
                cmds.data(), static_cast<int>(cmds.size()));
            auto elapsed_ns = static_cast<double>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now() - start).count());

            // Distribute results
            for (size_t i = 0; i < entries.size(); ++i)
                entries[i]->result = std::move(results[i]);

            // Update network time estimator on single-entry batches
            if (entries.size() == 1)
                estimator_.updateRedisNetworkTime(elapsed_ns);

        } catch (...) {
            for (auto* e : entries) {
                e->result = RedisResult{};
            }
        }

        gate_.release();

        for (auto* e : entries) {
            if (e->continuation) {
                e->continuation.resume();
            }
        }

        // Chain: fire next accumulated batch or clear inflight
        if (!redis_batch_.entries.empty()) {
            fireRedisBatchNow();
        } else {
            redis_inflight_ = false;
        }
    }

    // =========================================================================
    // Single query execution (bootstrap / staleness / fallback)
    // =========================================================================

    Task<PgResult> sendSinglePgRead(PgReadEntry entry) {
        co_await gate_.acquire();

        PgResult result;
        auto start = Clock::now();

        try {
            auto guard = co_await pg_pool_->acquire();
            result = co_await guard.conn().queryParams(entry.single_sql, entry.params);
        } catch (...) {
            gate_.release();
            throw;
        }

        auto elapsed_ns = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now() - start).count());

        // Update timing estimator
        estimator_.updatePgNetworkTime(
            elapsed_ns, estimator_.getRequestTime(entry.single_sql));
        estimator_.updateSqlTiming(entry.single_sql, 1, 1, elapsed_ns);

        gate_.release();
        co_return result;
    }

    Task<PgResult> sendSinglePgWrite(PgWriteEntry entry) {
        co_await gate_.acquire();

        PgResult result;
        auto start = Clock::now();

        try {
            auto guard = co_await pg_pool_->acquire();
            result = co_await guard.conn().queryParams(entry.sql, entry.params);
        } catch (...) {
            gate_.release();
            throw;
        }

        auto elapsed_ns = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now() - start).count());

        // Update timing estimator
        estimator_.updatePgNetworkTime(
            elapsed_ns, estimator_.getRequestTime(entry.sql));
        estimator_.updateSqlTiming(entry.sql, 1, 1, elapsed_ns);

        gate_.release();
        co_return result;
    }

    Task<RedisResult> sendSingleRedis(RedisEntry entry) {
        co_await gate_.acquire();

        RedisResult result;
        auto start = Clock::now();

        try {
            auto& client = redis_pool_->next();

            std::vector<const char*> argv;
            std::vector<size_t> argvlen;
            argv.reserve(entry.args.size());
            argvlen.reserve(entry.args.size());
            for (const auto& a : entry.args) {
                argv.push_back(a.data());
                argvlen.push_back(a.size());
            }

            result = co_await client.execArgv(
                static_cast<int>(argv.size()),
                argv.data(), argvlen.data());
        } catch (...) {
            gate_.release();
            throw;
        }

        auto elapsed_ns = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now() - start).count());

        estimator_.updateRedisNetworkTime(elapsed_ns);

        gate_.release();
        co_return result;
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    Task<void> readAndDiscardPipelineResult(PgConnection<Io>& conn) {
        // Read and discard a single pipeline result (prepare result)
        auto results = co_await conn.readPipelineResults(1);
        // Discard
    }

    // =========================================================================
    // State
    // =========================================================================

    Io& io_;
    std::shared_ptr<PgPool<Io>> pg_pool_;
    std::shared_ptr<RedisPool<Io>> redis_pool_;
    ConcurrencyGate gate_;
    TimingEstimator estimator_;

    // Current accumulating batches (one of each at a time)
    PgReadBatch pg_read_batch_;
    PgWriteBatch pg_write_batch_;
    RedisBatch redis_batch_;

    // Nagle inflight flags — true while a direct send is in-flight,
    // causing subsequent queries to accumulate in the batch.
    bool pg_read_inflight_ = false;
    bool pg_write_inflight_ = false;
    bool redis_inflight_ = false;

    // Write sequence counter
    uint64_t next_write_seq_ = 0;

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_bench::BatchBenchAccessor;
#endif
};

} // namespace jcailloux::relais::io::batch

#endif // JCX_RELAIS_IO_BATCH_SCHEDULER_H
