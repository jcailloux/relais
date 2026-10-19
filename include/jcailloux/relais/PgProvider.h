#ifndef JCX_RELAIS_PG_PROVIDER_H
#define JCX_RELAIS_PG_PROVIDER_H

#include <cassert>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgPool.h"
#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/batch/BatchScheduler.h"

namespace jcailloux::relais::io { class IoPool; }

namespace jcailloux::relais {

// =============================================================================
// PgProvider — Type-erased service locator for PostgreSQL
//
// Wraps the per-loop io::batch::BatchScheduler<Io> behind std::function to
// decouple relais from the concrete IoContext type. The application
// initializes once at startup with the appropriate IoContext.
//
// The overhead of std::function indirection (one pointer-chase per call) is
// negligible compared to the network I/O latency of database operations.
//
// Initialization (in application startup):
//   auto pool = co_await PgPool<MyIo>::create(io, conninfo,
//                  {.min_connections = 4, .max_connections = 16});
//   PgProvider::init(io, pool);   // on the loop thread it serves
//
// Usage in repositories:
//   auto result = co_await PgProvider::queryParams(sql, params);
//
// Lifetime: params and sql must remain valid until the co_await completes.
// This is naturally satisfied when params is a local variable in the calling
// coroutine and the result is co_awaited immediately.
// =============================================================================

class PgProvider {
public:
    // =========================================================================
    // PostgreSQL operations
    // =========================================================================

    /// Execute a simple SQL query (no parameters).
    /// @note sql must remain valid until the co_await completes.
    static io::Task<io::PgResult> query(const char* sql) {
        if (!pg_query_) throw std::logic_error("PgProvider::query() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
        return pg_query_(sql);
    }

    /// Execute a parameterized SQL query.
    /// @note sql and params must remain valid until the co_await completes.
    static io::Task<io::PgResult> queryParams(
        const char* sql, const io::PgParams& params)
    {
        if (!pg_query_params_) throw std::logic_error("PgProvider::queryParams() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
        return pg_query_params_(sql, params);
    }

    /// Execute an entity read query — routed through submitEntityRead for
    /// ANY-array batching. batch_sql is the ANY variant (may be null).
    /// @note sql pointers and params must remain valid until co_await completes.
    static io::Task<io::PgResult> entityQueryParams(
        const char* batch_sql, const char* single_sql,
        const io::PgParams& params)
    {
        if (!pg_entity_query_) throw std::logic_error("PgProvider::entityQueryParams() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
        return pg_entity_query_(batch_sql, single_sql, params);
    }

    /// Execute a multi-key entity read (findMany) — one entry carrying N keys,
    /// fused with concurrent single finds into a single deduplicated `pk = ANY`.
    /// `keys` is moved into the scheduler entry (lives until the read completes).
    /// @note sql pointers must remain valid until the co_await completes.
    static io::Task<io::PgResult> entityQueryParamsMany(
        const char* batch_sql, const char* single_sql,
        std::vector<io::PgParams> keys)
    {
        if (!pg_entity_query_many_) throw std::logic_error("PgProvider::entityQueryParamsMany() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
        return pg_entity_query_many_(batch_sql, single_sql, std::move(keys));
    }

    /// Submit a write (INSERT/UPDATE/DELETE) on the seq-ordered write path,
    /// returning {PgResult, coalesced}. Like queryParams but write-batched: the
    /// returned PgResult carries RETURNING rows; affectedRows() gives the count.
    /// coalesced=true means an identical write was already batched and this
    /// caller received the leader's result without a DB round-trip.
    /// `mode` declares whether the write may be coalesced at all: pass
    /// WriteMode::Exclusive for any write that is not idempotent (relative SET,
    /// guarded write, claim, `now()`-dependent value).
    /// Sole write entry point — read counts/rows from the returned PgResult.
    /// @note sql and params must remain valid until the co_await completes.
    static io::Task<io::batch::PgWriteResult> queryWrite(
        const char* sql, const io::PgParams& params,
        const io::batch::WriteMode mode = io::batch::WriteMode::Idempotent)
    {
        return queryWrite(sql, params, mode, io::batch::kOrdered);
    }

    /// queryWrite naming the row the write targets (internal: the repositories
    /// know it, raw SQL does not). See io::batch::WriteAffinity.
    static io::Task<io::batch::PgWriteResult> queryWrite(
        const char* sql, const io::PgParams& params,
        const io::batch::WriteMode mode, const io::batch::WriteAffinity affinity)
    {
        if (!pg_write_) throw std::logic_error("PgProvider::queryWrite() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
        return pg_write_(sql, params, mode, affinity);
    }

    /// Execute a parameterized SQL query with inline args.
    /// The PgParams object is kept alive in the coroutine frame.
    template<io::PgArg... Args>
    static io::Task<io::PgResult> queryArgs(const char* sql, Args&&... args) {
        auto params = io::PgParams::make(std::forward<Args>(args)...);
        co_return co_await queryParams(sql, params);
    }

    /// Resume the awaiting coroutine on a later turn of this thread's loop, so
    /// that long work hands the loop back between its steps. At once on a
    /// thread with no bound loop.
    static auto yieldToLoop() noexcept {
        struct Awaiter {
            bool await_ready() const noexcept { return !post_; }
            void await_suspend(std::coroutine_handle<> h) const {
                post_([h] { h.resume(); });
            }
            void await_resume() const noexcept {}
        };
        return Awaiter{};
    }

    /// Check if PgProvider has been initialized.
    [[nodiscard]] static bool initialized() noexcept {
        return pg_query_ != nullptr;
    }

    // =========================================================================
    // Initialization (call once PER LOOP THREAD at startup)
    // =========================================================================

    /// Bind this thread's providers to a freshly built BatchScheduler. The
    /// IoContext type is erased — callers don't need to know it.
    ///
    /// Thread affinity: the providers are thread_local. Call init() ON the loop
    /// thread whose pool you pass — mono-loop: once on that loop; N-loop: once
    /// per loop, each with that loop's own pool. Building the BatchScheduler also
    /// touches `io` (timers), which must happen on the loop thread.
    template<typename Io>
    static void init(
        Io& io,
        std::shared_ptr<io::PgPool<Io>> pool,
        int max_concurrent = 8)
    {
        // Debug guard: providers are thread_local, so init() MUST run on the loop
        // thread whose pool it binds. Adapters exposing isInLoopThread() (e.g.
        // EpollIoContext, a trantor shim) get this checked; others opt out.
        if constexpr (requires(Io& i) {
                          { i.isInLoopThread() } -> std::convertible_to<bool>;
                      }) {
            assert(io.isInLoopThread() &&
                "PgProvider::init must run ON the loop thread (providers are "
                "thread_local — call it once per loop thread for N-loop)");
        }

        bindBatcher<Io>(std::make_shared<io::batch::BatchScheduler<Io>>(
            io, std::move(pool), max_concurrent));
    }

    /// Bind the calling thread's providers to an already-built BatchScheduler.
    /// Used by init() and by IoPool (which builds one batcher per worker). MUST
    /// run on the loop thread that owns `batcher`. The thread_local function
    /// objects co-own `batcher` via shared_ptr — it lives until reset() or the
    /// thread exits.
    template<typename Io>
    static void bindBatcher(std::shared_ptr<io::batch::BatchScheduler<Io>> batcher) {
        pg_query_ = [batcher](const char* sql) {
            return batcher->directQuery(sql);
        };
        pg_query_params_ = [batcher](const char* sql, const io::PgParams& params)
            -> io::Task<io::PgResult>
        {
            co_return co_await batcher->submitQueryRead(sql, io::PgParams{params});
        };
        pg_entity_query_ = [batcher](const char* batch_sql, const char* single_sql,
                                      const io::PgParams& params)
            -> io::Task<io::PgResult>
        {
            co_return co_await batcher->submitEntityRead(
                batch_sql, single_sql, io::PgParams{params});
        };
        pg_entity_query_many_ = [batcher](const char* batch_sql, const char* single_sql,
                                          std::vector<io::PgParams> keys)
            -> io::Task<io::PgResult>
        {
            co_return co_await batcher->submitEntityReadMany(
                batch_sql, single_sql, std::move(keys));
        };
        // Return-direct: submitPgWrite already returns Task<io::batch::PgWriteResult>,
        // so no co_await/co_return wrapper — zero added coroutine frame on the write path.
        pg_write_ = [batcher](const char* sql, const io::PgParams& params,
                              io::batch::WriteMode mode,
                              io::batch::WriteAffinity affinity) {
            return batcher->submitPgWrite(sql, io::PgParams{params}, mode, affinity);
        };
        post_ = [batcher](std::function<void()> cb) { batcher->post(std::move(cb)); };
    }

    /// Reset all providers (for testing).
    static void reset() noexcept {
        pg_query_ = nullptr;
        pg_query_params_ = nullptr;
        pg_entity_query_ = nullptr;
        pg_entity_query_many_ = nullptr;
        pg_write_ = nullptr;
        post_ = nullptr;
    }

    // =========================================================================
    // Type-erased function storage (accessible to IoPool for registration)
    // =========================================================================

    using PgQueryFn = std::function<io::Task<io::PgResult>(const char*)>;
    using PgQueryParamsFn = std::function<io::Task<io::PgResult>(
        const char*, const io::PgParams&)>;
    using PgEntityQueryFn = std::function<io::Task<io::PgResult>(
        const char*, const char*, const io::PgParams&)>;
    using PgEntityQueryManyFn = std::function<io::Task<io::PgResult>(
        const char*, const char*, std::vector<io::PgParams>)>;
    using PgWriteFn = std::function<io::Task<io::batch::PgWriteResult>(
        const char*, const io::PgParams&, io::batch::WriteMode,
        io::batch::WriteAffinity)>;
    using PostFn = std::function<void(std::function<void()>)>;

    // thread_local: each event-loop thread binds its OWN pool/batcher by calling
    // init() on that thread (shared-nothing). A Task co_awaited on loop K then
    // dispatches to loop K's batcher with no cross-thread hop. Mono-loop is N=1;
    // N-loop scales throughput by calling init() once per loop thread.
    static inline thread_local PgQueryFn pg_query_;
    static inline thread_local PgQueryParamsFn pg_query_params_;
    static inline thread_local PgEntityQueryFn pg_entity_query_;
    static inline thread_local PgEntityQueryManyFn pg_entity_query_many_;
    static inline thread_local PgWriteFn pg_write_;
    static inline thread_local PostFn post_;

    // IoPool needs to set these directly
    friend class io::IoPool;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_PG_PROVIDER_H
