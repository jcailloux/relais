#ifndef JCX_RELAIS_PG_PROVIDER_H
#define JCX_RELAIS_PG_PROVIDER_H

#include <cassert>
#include <concepts>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgPool.h"
#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/redis/RedisClient.h"
#include "jcailloux/relais/io/redis/RedisResult.h"
#include "jcailloux/relais/io/batch/BatchScheduler.h"

namespace jcailloux::relais::io { class IoPool; }

namespace jcailloux::relais {

// =============================================================================
// PgProvider — Type-erased service locator for PostgreSQL and Redis
//
// Wraps io::PgClient<Io> and io::RedisClient<Io> behind std::function
// to decouple relais from the concrete IoContext type. The application
// initializes once at startup with the appropriate IoContext.
//
// The overhead of std::function indirection (one pointer-chase per call) is
// negligible compared to the network I/O latency of database/Redis operations.
//
// Initialization (in application startup):
//   auto pool = co_await PgPool<MyIo>::create(io, conninfo, 4, 16);
//   auto redis = co_await RedisClient<MyIo>::connect(io, "127.0.0.1", 6379);
//   PgProvider::init(io, pool, redis);   // on the loop thread it serves
//
// Usage in repositories:
//   auto result = co_await PgProvider::queryParams(sql, params);
//   auto reply = co_await PgProvider::redis("GET", key);
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
        assert(pg_query_ && "PgProvider::query() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
        return pg_query_(sql);
    }

    /// Execute a parameterized SQL query.
    /// @note sql and params must remain valid until the co_await completes.
    static io::Task<io::PgResult> queryParams(
        const char* sql, const io::PgParams& params)
    {
        assert(pg_query_params_ && "PgProvider::queryParams() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
        return pg_query_params_(sql, params);
    }

    /// Execute an entity read query — routed through submitEntityRead for
    /// ANY-array batching. batch_sql is the ANY variant (may be null).
    /// @note sql pointers and params must remain valid until co_await completes.
    static io::Task<io::PgResult> entityQueryParams(
        const char* batch_sql, const char* single_sql,
        const io::PgParams& params)
    {
        assert(pg_entity_query_ && "PgProvider::entityQueryParams() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
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
        assert(pg_entity_query_many_ && "PgProvider::entityQueryParamsMany() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
        return pg_entity_query_many_(batch_sql, single_sql, std::move(keys));
    }

    /// Submit a write (INSERT/UPDATE/DELETE) on the seq-ordered write path,
    /// returning {PgResult, coalesced}. Like queryParams but write-batched: the
    /// returned PgResult carries RETURNING rows; affectedRows() gives the count.
    /// coalesced=true means an identical write was already batched and this
    /// caller received the leader's result without a DB round-trip.
    /// Sole write entry point — read counts/rows from the returned PgResult.
    /// @note sql and params must remain valid until the co_await completes.
    static io::Task<io::batch::PgWriteResult> queryWrite(
        const char* sql, const io::PgParams& params)
    {
        assert(pg_write_ && "PgProvider::queryWrite() called before init() on this thread (providers are thread_local — init() must run on each loop thread)");
        return pg_write_(sql, params);
    }

    /// Execute a parameterized SQL query with inline args.
    /// The PgParams object is kept alive in the coroutine frame.
    template<typename... Args>
    static io::Task<io::PgResult> queryArgs(const char* sql, Args&&... args) {
        auto params = io::PgParams::make(std::forward<Args>(args)...);
        co_return co_await queryParams(sql, params);
    }

    // =========================================================================
    // Redis operations
    // =========================================================================

    /// Execute a Redis command with variadic arguments.
    /// Arguments are converted to strings. Binary data can be passed as
    /// std::string_view (including embedded NUL bytes — all args are
    /// binary-safe since RESP2 uses length-prefixed strings).
    ///
    /// Usage:
    ///   co_await PgProvider::redis("SET", "key", "value");
    ///   co_await PgProvider::redis("SETEX", key, std::to_string(ttl), value);
    ///   co_await PgProvider::redis("EVAL", lua_script, "1", tracking_key);
    ///   co_await PgProvider::redis("SETEX", key, ttl_str,
    ///       std::string_view(reinterpret_cast<const char*>(bin.data()), bin.size()));
    template<typename... Args>
    static io::Task<io::RedisResult> redis(Args&&... args) {
        assert(redis_exec_ && "PgProvider::redis() called before init() or Redis not configured");

        // Build argv in the coroutine frame — lifetime extends until co_await completes.
        std::vector<std::string> arg_strs;
        arg_strs.reserve(sizeof...(args));
        (arg_strs.push_back(toStr(std::forward<Args>(args))), ...);

        std::vector<const char*> argv;
        std::vector<size_t> argvlen;
        argv.reserve(arg_strs.size());
        argvlen.reserve(arg_strs.size());
        for (const auto& s : arg_strs) {
            argv.push_back(s.data());
            argvlen.push_back(s.size());
        }

        co_return co_await redis_exec_(
            static_cast<int>(argv.size()), argv.data(), argvlen.data());
    }

    /// Execute a Redis command whose argument list is sized at runtime
    /// (e.g. MGET over N keys). `args` is the full argv — verb first — taken by
    /// value and kept alive in the coroutine frame. Binary-safe (length-prefixed).
    static io::Task<io::RedisResult> redisDynamic(std::vector<std::string> args) {
        assert(redis_exec_ && "PgProvider::redisDynamic() called before init() or Redis not configured");

        std::vector<const char*> argv;
        std::vector<size_t> argvlen;
        argv.reserve(args.size());
        argvlen.reserve(args.size());
        for (const auto& s : args) {
            argv.push_back(s.data());
            argvlen.push_back(s.size());
        }

        co_return co_await redis_exec_(
            static_cast<int>(argv.size()), argv.data(), argvlen.data());
    }

    /// Check if Redis is configured.
    [[nodiscard]] static bool hasRedis() noexcept {
        return redis_exec_ != nullptr;
    }

    /// Check if PgProvider has been initialized.
    [[nodiscard]] static bool initialized() noexcept {
        return pg_query_ != nullptr;
    }

    // =========================================================================
    // Initialization (call once PER LOOP THREAD at startup)
    // =========================================================================

    /// Bind this thread's providers to a freshly built BatchScheduler (PG +
    /// Redis pipelining). The IoContext type is erased — callers don't need to
    /// know it.
    ///
    /// Thread affinity: the providers are thread_local. Call init() ON the loop
    /// thread whose pool you pass — mono-loop: once on that loop; N-loop: once
    /// per loop, each with that loop's own pool. Building the BatchScheduler also
    /// touches `io` (timers), which must happen on the loop thread.
    template<typename Io>
    static void init(
        Io& io,
        std::shared_ptr<io::PgPool<Io>> pool,
        // type_identity_t → non-deduced: Io comes from io/pool only, so passing
        // an explicit `nullptr` here doesn't break deduction.
        std::type_identity_t<std::shared_ptr<io::RedisClient<Io>>> redisClient = nullptr,
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

        // Wrap single RedisClient into a RedisPool for the BatchScheduler
        std::shared_ptr<io::RedisPool<Io>> redis_pool;
        if (redisClient) {
            redis_pool = std::make_shared<io::RedisPool<Io>>(
                io::RedisPool<Io>::fromClients({std::move(redisClient)}));
        }

        auto batcher = std::make_shared<io::batch::BatchScheduler<Io>>(
            io, std::move(pool), redis_pool, max_concurrent);

        bindBatcher<Io>(std::move(batcher), redis_pool != nullptr);
    }

    /// Bind the calling thread's providers to an already-built BatchScheduler.
    /// Used by init() and by IoPool (which builds one batcher per worker). MUST
    /// run on the loop thread that owns `batcher`. The thread_local function
    /// objects co-own `batcher` via shared_ptr — it lives until reset() or the
    /// thread exits.
    template<typename Io>
    static void bindBatcher(
        std::shared_ptr<io::batch::BatchScheduler<Io>> batcher, bool with_redis)
    {
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
        pg_write_ = [batcher](const char* sql, const io::PgParams& params) {
            return batcher->submitPgWrite(sql, io::PgParams{params});
        };

        if (with_redis) {
            // Route Redis through the BatchScheduler for pipelining.
            // The batcher owns the redis_pool via shared_ptr.
            redis_exec_ = [batcher](
                int argc, const char** argv, const size_t* argvlen)
            {
                return batcher->submitRedis(argc, argv, argvlen);
            };
        } else {
            redis_exec_ = nullptr;
        }
    }

    /// Reset all providers (for testing).
    static void reset() noexcept {
        pg_query_ = nullptr;
        pg_query_params_ = nullptr;
        pg_entity_query_ = nullptr;
        pg_entity_query_many_ = nullptr;
        pg_write_ = nullptr;
        redis_exec_ = nullptr;
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
        const char*, const io::PgParams&)>;
    using RedisExecFn = std::function<io::Task<io::RedisResult>(
        int, const char**, const size_t*)>;

    // thread_local: each event-loop thread binds its OWN pool/batcher by calling
    // init() on that thread (shared-nothing). A Task co_awaited on loop K then
    // dispatches to loop K's batcher with no cross-thread hop. Mono-loop is N=1;
    // N-loop scales throughput by calling init() once per loop thread.
    static inline thread_local PgQueryFn pg_query_;
    static inline thread_local PgQueryParamsFn pg_query_params_;
    static inline thread_local PgEntityQueryFn pg_entity_query_;
    static inline thread_local PgEntityQueryManyFn pg_entity_query_many_;
    static inline thread_local PgWriteFn pg_write_;
    static inline thread_local RedisExecFn redis_exec_;

    // IoPool needs to set these directly
    friend class io::IoPool;

private:

    // =========================================================================
    // String conversion helpers for Redis args
    // =========================================================================

    static std::string toStr(const char* s) { return s; }
    static std::string toStr(std::string_view s) { return std::string(s); }
    static std::string toStr(const std::string& s) { return s; }
    static std::string toStr(std::string&& s) { return std::move(s); }

    template<typename T> requires std::integral<T>
    static std::string toStr(T v) { return std::to_string(v); }

    static std::string toStr(double v) { return std::to_string(v); }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_PG_PROVIDER_H
