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
#include "jcailloux/relais/io/IoContext.h"
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
// No internal mutexes — the PgProvider thread_local dispatch guarantees
// that all calls come from the same event loop.
//
// Batching strategy:
// - Reads: SELECT ... WHERE id = ANY($1) for entities of same repo,
//   pipelining for different repos/lists, with sync between segments.
// - Writes: pipelined with sync after each, preserving sequence order.
// - Redis: reads + writes in the same pipeline (Redis guarantees order).
// - Coalescing: identical queries share the same result.
//
// Budget: separate ConcurrencyGates cap in-flight PG and Redis sends
// independently, so neither tier starves the other under mixed load.

/// Result of a pipelined PG write, with coalescing indicator.
/// Non-templated (independent of Io) so it can be named in the type-erased
/// PgProvider provider signatures (pg_write_) — enables return-direct binding
/// with zero added coroutine frame.
struct PgWriteResult {
    PgResult result;
    bool coalesced = false;
};

template<IoContext Io>
class BatchScheduler {
    using Clock = std::chrono::steady_clock;
    using TimerToken = typename Io::TimerToken;

public:
    BatchScheduler(Io& io,
                   std::shared_ptr<PgPool<Io>> pg_pool,
                   std::shared_ptr<RedisPool<Io>> redis_pool,
                   int max_concurrent = 8)
        : io_(io)
        , pg_pool_(std::move(pg_pool))
        , redis_pool_(std::move(redis_pool))
        , pg_gate_{max_concurrent}
        , redis_gate_{max_concurrent}
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

    /// Submit a multi-key entity read (findMany): ONE entry carrying N keys.
    /// When it lands in a batch, its keys join the same deduplicated ANY array
    /// as concurrent single finds of the same table — K segments collapse to one
    /// `pk = ANY($1)`. As the Nagle leader it sends its own ANY direct (the shape
    /// findManyRaw used before fusion). The returned PgResult holds the rows for
    /// this entry's keys, unordered; the caller re-aligns by primary key.
    /// `keys` must be non-empty (callers short-circuit the empty case).
    Task<PgResult> submitEntityReadMany(const char* batch_sql, const char* single_sql,
                                        std::vector<PgParams> keys) {
        PgReadEntry entry;
        entry.batch_sql = batch_sql;
        entry.single_sql = single_sql;
        entry.is_entity = true;
        entry.batch_keys = std::move(keys);

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

    /// Result of a pipelined PG write, with coalescing indicator (alias of the
    /// namespace-scope PgWriteResult — see above for why it lives outside).
    using WriteResult = PgWriteResult;

    /// Submit a PG write (INSERT/UPDATE/DELETE RETURNING).
    /// Returns {PgResult, coalesced}. coalesced=true means an identical
    /// write (same SQL + same params) was already in the batch and this
    /// caller received the leader's result without a DB round-trip.
    ///
    /// Ordering: each write takes a monotonic `seq` here, at submit time, and a
    /// batch fires its writes in `seq` order (firePgWriteBatch's sort). So two
    /// writes submitted in a flow — e.g. INSERT then UPDATE of the same PK —
    /// execute in submission order even when they land in the same batch. Order
    /// BETWEEN batches is the caller's responsibility, carried by `co_await`
    /// (read-your-writes intra-flow). The scheduler does NOT track write
    /// dependencies across concurrent coroutines. See docs/runtime-and-threading.md.
    Task<WriteResult> submitPgWrite(const char* sql, PgParams params) {
        PgWriteEntry entry;
        entry.sql = sql;
        entry.params = std::move(params);
        entry.seq = next_write_seq_++;

        co_return co_await submitPgWriteEntry(std::move(entry));
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

    // -----------------------------------------------------------------------
    // Entry lifetime
    // -----------------------------------------------------------------------
    // Entries are heap-allocated (unique_ptr) by their awaiter, transferred
    // to the batch on await_suspend, and deleted *after* their continuation
    // resumes (the awaiter's destructor cleans up via reclaim).
    //
    // cancelled  : set by awaiter's destructor when the caller coroutine is
    //              destroyed before await_resume runs. The fire-functions
    //              skip resume and delete the entry instead.
    // error      : set by fire-functions when the pipeline as a whole fails
    //              (or a per-segment exception was caught). Rethrown by
    //              await_resume so callers see a proper exception, not a
    //              silently-empty PgResult.
    // followers  : peer entries coalesced onto this one (identical SQL +
    //              params). They share leader's result/error on completion.

    struct PgReadEntry {
        const char* batch_sql = nullptr;   // ANY batch SQL (null if not entity)
        const char* single_sql = nullptr;  // Single-row / list SQL
        PgParams params;
        bool is_entity = false;

        // Set by submitPgRead when the entry is added to a batch
        std::coroutine_handle<> continuation{};
        PgResult result;
        int64_t processing_time_us = 0;

        // Key values for ANY-batch result matching (populated before ANY send)
        std::vector<std::string> key_values;

        // Multi-key entity entry (findMany): N keys carried by ONE entry. When
        // non-empty, this entry stands in for all these keys in the shared ANY
        // array, and its result is the SUBSET of rows matching any of them.
        // `params`/`key_values` (the single-key fields above) are unused then.
        std::vector<PgParams> batch_keys;
        // Per-key match values, populated at fire time (mirror of key_values).
        std::vector<std::vector<std::string>> batch_key_values;

        // Lifecycle / error / coalescing
        bool cancelled = false;
        std::exception_ptr error;
        std::vector<PgReadEntry*> followers;  // coalesced peers
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

        // Lifecycle / error
        bool cancelled = false;
        std::exception_ptr error;
    };

    struct RedisEntry {
        std::vector<std::string> args;   // Owned copies

        std::coroutine_handle<> continuation{};
        RedisResult result;

        // Lifecycle / error
        bool cancelled = false;
        std::exception_ptr error;
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
    // ConcurrencyGate — coroutine semaphore for a per-tier in-flight budget
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

    // -----------------------------------------------------------------------
    // Awaiters
    //
    // Each awaiter owns its Entry on the heap (unique_ptr) until await_suspend
    // transfers ownership to the batch. On normal completion (await_resume
    // runs), we reclaim ownership via unique_ptr and delete after extracting
    // the result. On cancellation (caller coroutine destroyed before
    // await_resume), the destructor flags `cancelled` so the fire-function
    // detects and deletes the entry instead of resuming a dead continuation.
    // -----------------------------------------------------------------------

    struct PgReadAwaiter {
        BatchScheduler* self;
        std::unique_ptr<PgReadEntry> entry_owner;
        PgReadEntry* entry_ptr;
        bool resumed = false;

        PgReadAwaiter(BatchScheduler* s, PgReadEntry e)
            : self(s)
            , entry_owner(std::make_unique<PgReadEntry>(std::move(e)))
            , entry_ptr(entry_owner.get()) {}

        PgReadAwaiter(const PgReadAwaiter&) = delete;
        PgReadAwaiter(PgReadAwaiter&&) = delete;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            entry_ptr->continuation = h;
            self->addToPgReadBatch(entry_owner.release());
        }

        PgResult await_resume() {
            resumed = true;
            std::unique_ptr<PgReadEntry> entry(entry_ptr);
            if (entry->error) std::rethrow_exception(entry->error);
            return std::move(entry->result);
        }

        ~PgReadAwaiter() {
            // Three exit paths:
            //   1. never suspended → entry_owner still holds → unique_ptr cleans up
            //   2. resumed normally → entry_owner already released, await_resume deleted
            //   3. cancelled mid-await → entry still owned by batch → mark cancelled
            if (entry_owner)   return;            // (1)
            if (resumed)       return;            // (2)
            entry_ptr->cancelled = true;          // (3)
        }
    };

    Task<PgResult> submitPgRead(PgReadEntry entry) {
        if (estimator_.isPgBootstrapping() || estimator_.isPgStale()) {
            co_return co_await sendSinglePgRead(std::move(entry));
        }

        // Nagle: first query goes direct, subsequent accumulate during its RTT.
        // This branch is the leader's set + clear half of the pg_read_inflight_
        // latch invariant (proven at its declaration).
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
        std::unique_ptr<PgWriteEntry> entry_owner;
        PgWriteEntry* entry_ptr;
        bool resumed = false;

        PgWriteAwaiter(BatchScheduler* s, PgWriteEntry e)
            : self(s)
            , entry_owner(std::make_unique<PgWriteEntry>(std::move(e)))
            , entry_ptr(entry_owner.get()) {}

        PgWriteAwaiter(const PgWriteAwaiter&) = delete;
        PgWriteAwaiter(PgWriteAwaiter&&) = delete;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            entry_ptr->continuation = h;
            self->addToPgWriteBatch(entry_owner.release());
        }

        WriteResult await_resume() {
            resumed = true;
            std::unique_ptr<PgWriteEntry> entry(entry_ptr);
            if (entry->error) std::rethrow_exception(entry->error);
            return {std::move(entry->result), entry->coalesced};
        }

        ~PgWriteAwaiter() {
            if (entry_owner)   return;
            if (resumed)       return;
            entry_ptr->cancelled = true;
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
        std::unique_ptr<RedisEntry> entry_owner;
        RedisEntry* entry_ptr;
        bool resumed = false;

        RedisAwaiter(BatchScheduler* s, RedisEntry e)
            : self(s)
            , entry_owner(std::make_unique<RedisEntry>(std::move(e)))
            , entry_ptr(entry_owner.get()) {}

        RedisAwaiter(const RedisAwaiter&) = delete;
        RedisAwaiter(RedisAwaiter&&) = delete;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            entry_ptr->continuation = h;
            self->addToRedisBatch(entry_owner.release());
        }

        RedisResult await_resume() {
            resumed = true;
            std::unique_ptr<RedisEntry> entry(entry_ptr);
            if (entry->error) std::rethrow_exception(entry->error);
            return std::move(entry->result);
        }

        ~RedisAwaiter() {
            if (entry_owner)   return;
            if (resumed)       return;
            entry_ptr->cancelled = true;
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
        // Coalescing: identical reads share the leader's result. Two reads
        // coalesce iff same SQL (both batch_sql and single_sql pointers) and
        // identical params. Applies to query reads and entity reads with the
        // same PK — saves a duplicate row fetch when N coroutines request
        // the same key concurrently.
        for (auto* existing : pg_read_batch_.entries) {
            if (existing->batch_sql == entry->batch_sql &&
                existing->single_sql == entry->single_sql &&
                existing->params == entry->params &&
                existing->batch_keys == entry->batch_keys)
            {
                // batch_keys is empty for single finds/list reads (compares
                // equal → params decides); for findMany it guards against
                // coalescing two different key sets onto one entry.
                existing->followers.push_back(entry);
                return;
            }
        }

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
        //
        // Coalescing is sound ONLY for idempotent (absolute-SET) writes:
        // dropping N-1 identical writes is equivalent to keeping one iff the
        // write yields the same final state applied once or N times. The entity
        // generator guarantees this — it only emits absolute `col=$n` SETs,
        // never self-referential `col=col+$n`. Locked by test_relais_gen_sql
        // ("no self-referential SET"). Do not coalesce relative writes here.
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

        pg_read_batch_.timer = io_.postDelayed(delay, [this] {
            pg_read_batch_.timer_active = false;
            firePgReadBatchNow();
        });
        pg_read_batch_.timer_active = true;
    }

    void schedulePgWriteDeparture() {
        auto delay_ns = static_cast<int64_t>(estimator_.pg_network_time_ns);
        if (delay_ns <= 0) delay_ns = 100'000;
        auto delay = std::chrono::nanoseconds(delay_ns);

        pg_write_batch_.timer = io_.postDelayed(delay, [this] {
            pg_write_batch_.timer_active = false;
            firePgWriteBatchNow();
        });
        pg_write_batch_.timer_active = true;
    }

    void scheduleRedisDeparture() {
        auto delay_ns = static_cast<int64_t>(estimator_.redis_network_time_ns);
        if (delay_ns <= 0) delay_ns = 50'000; // 50us minimum
        auto delay = std::chrono::nanoseconds(delay_ns);

        redis_batch_.timer = io_.postDelayed(delay, [this] {
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
            io_.cancelTimer(pg_read_batch_.timer);
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
            io_.cancelTimer(pg_write_batch_.timer);
            pg_write_batch_.timer_active = false;
        }

        auto entries = std::move(pg_write_batch_.entries);
        pg_write_batch_ = {};

        firePgWriteBatch(std::move(entries));
    }

    void fireRedisBatchNow() {
        if (redis_batch_.entries.empty()) return;

        if (redis_batch_.timer_active) {
            io_.cancelTimer(redis_batch_.timer);
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
        co_await pg_gate_.acquire();
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
                int n_keys = 0;        // Distinct keys folded into an ANY segment
            };
            std::vector<Segment> segments;

            // Group entity reads by batch_sql pointer. An entry carries one key
            // (find) or N keys (findMany); both feed the same ANY fusion.
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

            // One segment per entity group. Gather every key across all finds and
            // findManys in the group, DEDUPLICATE, and emit a single ANY array;
            // distributeAnyResults later fans each row out to every waiter that
            // asked for that key. A group reducing to a single distinct key skips
            // ANY entirely (single_sql). Same batch_sql ⇒ same repo ⇒ same
            // single_sql across the group (constexpr pointers), so group[0] picks it.
            for (auto& [sql, group] : entity_groups) {
                std::vector<PgParams> unique_keys;
                std::vector<std::vector<std::string>> unique_vals;  // parallel match values
                auto addKey = [&](std::vector<std::string> vals, const PgParams& p) {
                    for (const auto& uv : unique_vals)
                        if (uv == vals) return;  // already in the ANY array
                    unique_keys.push_back(p);
                    unique_vals.push_back(std::move(vals));
                };
                for (auto* e : group) {
                    if (!e->batch_keys.empty()) {
                        // findMany: precompute per-key match values once, dedup each.
                        e->batch_key_values.clear();
                        e->batch_key_values.reserve(e->batch_keys.size());
                        for (auto& kp : e->batch_keys) {
                            auto vals = kp.keyValues();
                            addKey(vals, kp);
                            e->batch_key_values.push_back(std::move(vals));
                        }
                    } else {
                        e->key_values = e->params.keyValues();
                        addKey(e->key_values, e->params);
                    }
                }

                Segment seg;
                seg.waiters = group;
                seg.n_keys = static_cast<int>(unique_keys.size());
                if (unique_keys.size() == 1) {
                    // Single distinct key: no ANY, fan the lone row to all waiters.
                    seg.sql = group[0]->single_sql;
                    seg.params = std::move(unique_keys[0]);
                    seg.is_any = false;
                } else {
                    seg.sql = sql;  // batch_sql: SELECT ... WHERE pk = ANY($1)
                    seg.params = PgParams::buildArrayLiteral(unique_keys);
                    seg.is_any = true;
                }
                segments.push_back(std::move(seg));
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

            // Distribute results to waiters (+ propagate to coalesced followers)
            for (size_t i = 0; i < segments.size(); ++i) {
                auto& seg = segments[i];
                auto& pr = results[i];

                if (!seg.is_any) {
                    // Non-ANY segment: typically one waiter, plus its followers.
                    for (auto* waiter : seg.waiters) {
                        waiter->result = pr.result;  // shared_ptr copy — keep pr.result usable for followers
                        waiter->processing_time_us = pr.processing_time_us;
                        for (auto* f : waiter->followers) {
                            f->result = pr.result;
                            f->processing_time_us = pr.processing_time_us;
                        }
                    }
                } else {
                    // ANY segment: match result rows to waiters by PK values.
                    distributeAnyResults(seg.waiters, pr.result,
                                         pr.processing_time_us);
                    // Propagate each waiter's matched slice to its followers.
                    for (auto* waiter : seg.waiters) {
                        for (auto* f : waiter->followers) {
                            f->result = waiter->result;
                            f->processing_time_us = waiter->processing_time_us;
                        }
                    }

                    // Update timing estimator for per-key cost. seg.n_keys is the
                    // distinct-key count (not the waiter count): findMany folds
                    // many keys into one waiter, so waiters.size() would undercount.
                    if (seg.n_keys > 0) {
                        estimator_.updateSqlTimingPerKey(
                            seg.sql, seg.n_keys,
                            static_cast<double>(pr.processing_time_us) * 1000.0);
                    }
                }
            }

            // Update network time if single-entry batch
            if (entries.size() == 1 && !results.empty()) {
                estimator_.updatePgNetworkTime(
                    static_cast<double>(results[0].processing_time_us) * 1000.0,
                    estimator_.getRequestTime(entries[0]->single_sql));
            }

        } catch (...) {
            // On pipeline-wide failure, propagate exception to ALL waiters
            // (leaders + coalesced followers) so callers see a real error
            // instead of a silently-empty PgResult.
            auto eptr = std::current_exception();
            try { conn.exitPipelineMode(); } catch (...) {}
            for (auto* e : entries) {
                e->error = eptr;
                for (auto* f : e->followers) f->error = eptr;
            }
        }

        pg_gate_.release();

        // Collect handles BEFORE resuming any — resuming a leader can destroy
        // its coroutine frame (symmetric transfer), invalidating followers
        // pointers on the leader's entry. Cancelled entries are deleted here
        // since their continuation will never run.
        std::vector<std::coroutine_handle<>> to_resume;
        std::vector<PgReadEntry*> to_delete;
        to_resume.reserve(entries.size() * 2);
        for (auto* e : entries) {
            if (e->cancelled) {
                to_delete.push_back(e);
            } else if (e->continuation) {
                to_resume.push_back(e->continuation);
            }
            for (auto* f : e->followers) {
                if (f->cancelled) {
                    to_delete.push_back(f);
                } else if (f->continuation) {
                    to_resume.push_back(f->continuation);
                }
            }
        }
        for (auto h : to_resume) h.resume();
        for (auto* e : to_delete) delete e;

        // Drain-or-chain — the clear-or-keep half of the pg_read_inflight_
        // latch invariant (proven at its declaration). More accumulated → fire
        // the next batch and KEEP the latch set; otherwise clear it. This runs
        // on the loop thread, so this read-modify-write of the latch cannot
        // race the leader's own clear in submitPgRead.
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
        // Sort by submission sequence: restores submission order regardless of
        // how entries were accumulated/coalesced, so intra-batch write→write
        // order == seq order == the order the caller submitted them. This is the
        // write-ordering contract (docs/runtime-and-threading.md).
        std::sort(entries.begin(), entries.end(),
            [](const auto* a, const auto* b) { return a->seq < b->seq; });

        co_await pg_gate_.acquire();
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
            // Propagate exception to all waiters instead of silent PgResult{}.
            auto eptr = std::current_exception();
            try { conn.exitPipelineMode(); } catch (...) {}
            for (auto* e : entries) {
                e->error = eptr;
                for (auto* f : e->followers) f->error = eptr;
            }
        }

        pg_gate_.release();

        // Collect all continuation handles BEFORE resuming any.
        // Resuming a leader can destroy its coroutine frame (symmetric
        // transfer chain), which would make e->followers a dangling pointer.
        // Cancelled entries are deleted here since their continuation will
        // never run.
        std::vector<std::coroutine_handle<>> to_resume;
        std::vector<PgWriteEntry*> to_delete;
        to_resume.reserve(entries.size() * 2);
        for (auto* e : entries) {
            if (e->cancelled) {
                to_delete.push_back(e);
            } else if (e->continuation) {
                to_resume.push_back(e->continuation);
            }
            for (auto* f : e->followers) {
                if (f->cancelled) {
                    to_delete.push_back(f);
                } else if (f->continuation) {
                    to_resume.push_back(f->continuation);
                }
            }
        }
        for (auto h : to_resume) h.resume();
        for (auto* e : to_delete) delete e;

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
        co_await redis_gate_.acquire();

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
            // Propagate exception to all waiters instead of silent RedisResult{}.
            auto eptr = std::current_exception();
            for (auto* e : entries) {
                e->error = eptr;
            }
        }

        redis_gate_.release();

        // Collect handles BEFORE resuming any — symmetric transfer can destroy
        // the resumed coroutine's frame, and we'd be iterating `entries` after
        // the entry pointer has been deleted (await_resume reclaims via
        // unique_ptr). Cancelled entries are deleted here.
        std::vector<std::coroutine_handle<>> to_resume;
        std::vector<RedisEntry*> to_delete;
        to_resume.reserve(entries.size());
        for (auto* e : entries) {
            if (e->cancelled) {
                to_delete.push_back(e);
            } else if (e->continuation) {
                to_resume.push_back(e->continuation);
            }
        }
        for (auto h : to_resume) h.resume();
        for (auto* e : to_delete) delete e;

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
        co_await pg_gate_.acquire();

        PgResult result;
        auto start = Clock::now();
        const char* timed_sql = entry.single_sql;
        int n_keys = 1;

        try {
            auto guard = co_await pg_pool_->acquire();
            if (!entry.batch_keys.empty()) {
                // Multi-key entry sent direct (Nagle leader or estimator
                // bootstrap): one ANY query carrying all its keys — exactly the
                // shape findManyRaw issued before fusion. A lone key skips the
                // array and uses single_sql. `arr` is a local that outlives the
                // co_await below.
                if (entry.batch_keys.size() == 1) {
                    result = co_await guard.conn().queryParams(
                        entry.single_sql, entry.batch_keys[0]);
                } else {
                    timed_sql = entry.batch_sql;
                    n_keys = static_cast<int>(entry.batch_keys.size());
                    auto arr = PgParams::buildArrayLiteral(entry.batch_keys);
                    result = co_await guard.conn().queryParams(entry.batch_sql, arr);
                }
            } else {
                result = co_await guard.conn().queryParams(
                    entry.single_sql, entry.params);
            }
        } catch (...) {
            pg_gate_.release();
            throw;
        }

        auto elapsed_ns = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now() - start).count());

        // Update timing estimator
        estimator_.updatePgNetworkTime(
            elapsed_ns, estimator_.getRequestTime(timed_sql));
        if (n_keys > 1)
            estimator_.updateSqlTimingPerKey(timed_sql, n_keys, elapsed_ns);
        else
            estimator_.updateSqlTiming(timed_sql, 1, 1, elapsed_ns);

        pg_gate_.release();
        co_return result;
    }

    Task<PgResult> sendSinglePgWrite(PgWriteEntry entry) {
        co_await pg_gate_.acquire();

        PgResult result;
        auto start = Clock::now();

        try {
            auto guard = co_await pg_pool_->acquire();
            result = co_await guard.conn().queryParams(entry.sql, entry.params);
        } catch (...) {
            pg_gate_.release();
            throw;
        }

        auto elapsed_ns = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now() - start).count());

        // Update timing estimator
        estimator_.updatePgNetworkTime(
            elapsed_ns, estimator_.getRequestTime(entry.sql));
        estimator_.updateSqlTiming(entry.sql, 1, 1, elapsed_ns);

        pg_gate_.release();
        co_return result;
    }

    Task<RedisResult> sendSingleRedis(RedisEntry entry) {
        co_await redis_gate_.acquire();

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
            redis_gate_.release();
            throw;
        }

        auto elapsed_ns = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now() - start).count());

        estimator_.updateRedisNetworkTime(elapsed_ns);

        redis_gate_.release();
        co_return result;
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /// Distribute an ANY-batch PgResult to individual waiters by matching PK
    /// column values. A single-key waiter (find) gets a zero-copy single-row
    /// sliceRow of its matching row (empty PgResult if absent). A multi-key
    /// waiter (findMany) gets a sliceRows view over EVERY row matching one of its
    /// keys. Matching is per-waiter, so a shared key's row fans out to each waiter
    /// that asked for it — and no waiter ever sees another caller's rows.
    void distributeAnyResults(std::vector<PgReadEntry*>& waiters,
                               PgResult& batch_result,
                               int64_t processing_time_us) {
        int n_rows = batch_result.valid()
            ? PQntuples(batch_result.raw()) : 0;

        // INVARIANT: the first vals.size() result columns are the PK columns in
        // key order — select_by_pk_batch must SELECT them first, matching the
        // column order keyValues()/buildArrayLiteral fold the keys in. The
        // single-key path relied on this already; multi-key fan-out now does too.
        // True iff row r equals the PK tuple `vals` (column-wise text compare).
        auto rowMatches = [&](int r, const std::vector<std::string>& vals) {
            for (size_t c = 0; c < vals.size(); ++c) {
                if (batch_result[r].rawValue(static_cast<int>(c))
                        != std::string_view(vals[c])) {
                    return false;
                }
            }
            return true;
        };

        // O(rows * keys * pk_cols) per waiter — typically small (< 100 total).
        for (auto* waiter : waiters) {
            waiter->processing_time_us = processing_time_us;

            if (!waiter->batch_key_values.empty()) {
                // findMany: collect every row matching any of its keys.
                std::vector<int> idx;
                for (int r = 0; r < n_rows; ++r) {
                    for (const auto& vals : waiter->batch_key_values) {
                        if (rowMatches(r, vals)) { idx.push_back(r); break; }
                    }
                }
                waiter->result = PgResult::sliceRows(batch_result, std::move(idx));
            } else {
                // find: first matching row, or empty.
                bool matched = false;
                for (int r = 0; r < n_rows && !matched; ++r) {
                    if (rowMatches(r, waiter->key_values)) {
                        waiter->result = PgResult::sliceRow(batch_result, r);
                        matched = true;
                    }
                }
                if (!matched) waiter->result = PgResult{};  // not found
            }
        }
    }

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
    // Per-tier concurrency budgets. Split so a burst of in-flight Redis sends
    // can never steal PG's connection slots (and vice-versa): under mixed
    // PG+Redis load PG keeps its full budget. Both reads and writes share the
    // PG gate — writes still pipeline on a single connection, so the gate only
    // bounds concurrent read fires against the write fire, never reorders them.
    ConcurrencyGate pg_gate_;
    ConcurrencyGate redis_gate_;
    TimingEstimator estimator_;

    // Current accumulating batches (one of each at a time)
    PgReadBatch pg_read_batch_;
    PgWriteBatch pg_write_batch_;
    RedisBatch redis_batch_;

    // Nagle inflight latches — true while a direct send is in-flight, so
    // subsequent same-tier queries accumulate into the batch instead of each
    // paying its own round-trip.
    //
    // Invariant (pg_read_inflight_, mono-thread): a plain bool, never atomic.
    // Every read and write of it runs on the one loop thread, only at points
    // where no other coroutine can interleave — coroutines hand off
    // cooperatively at co_await, they never preempt — so there is no data race
    // and no memory ordering to enforce. Lifecycle:
    //   - Set true by the *leader*: the read that finds it false. The leader
    //     sends one query direct (bypassing the batch) and suspends on its RTT.
    //   - Reads arriving during that RTT find it true and accumulate into
    //     pg_read_batch_ (departure timer / size check fires the batch).
    //   - Cleared on the leader's resume (submitPgRead, success or throw) and,
    //     independently, at firePgReadBatch's tail once accumulation drains; a
    //     non-empty tail chains the next batch WITHOUT clearing, keeping the
    //     latch set across back-to-back batches.
    // The latch only ever *under*-gates: clearing it while work is still
    // pending costs at most one extra direct send on another pooled connection,
    // and reads are independent + idempotent, so that is correctness-neutral.
    // It can never stick true (no clear-site is skippable) nor deadlock.
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
