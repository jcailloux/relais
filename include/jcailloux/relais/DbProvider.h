#ifndef JCX_RELAIS_DB_PROVIDER_H
#define JCX_RELAIS_DB_PROVIDER_H

#include <cassert>
#include <concepts>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
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
// DbProvider — Type-erased service locator for PostgreSQL and Redis
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
//   DbProvider::init(pool, redis);
//
// Usage in repositories:
//   auto result = co_await DbProvider::queryParams(sql, params);
//   auto reply = co_await DbProvider::redis("GET", key);
//
// Lifetime: params and sql must remain valid until the co_await completes.
// This is naturally satisfied when params is a local variable in the calling
// coroutine and the result is co_awaited immediately.
// =============================================================================

class DbProvider {
public:
    // =========================================================================
    // PostgreSQL operations
    // =========================================================================

    /// Execute a simple SQL query (no parameters).
    /// @note sql must remain valid until the co_await completes.
    static io::Task<io::PgResult> query(const char* sql) {
        assert(pg_query_ && "DbProvider::query() called before init()");
        return pg_query_(sql);
    }

    /// Execute a parameterized SQL query.
    /// @note sql and params must remain valid until the co_await completes.
    static io::Task<io::PgResult> queryParams(
        const char* sql, const io::PgParams& params)
    {
        assert(pg_query_params_ && "DbProvider::queryParams() called before init()");
        return pg_query_params_(sql, params);
    }

    /// Execute a command (INSERT/UPDATE/DELETE), returning {affected_rows, coalesced}.
    /// coalesced=true means an identical write was already batched and this
    /// caller received the leader's result without a DB round-trip.
    /// @note sql and params must remain valid until the co_await completes.
    static io::Task<std::pair<int, bool>> execute(
        const char* sql, const io::PgParams& params)
    {
        assert(pg_execute_ && "DbProvider::execute() called before init()");
        return pg_execute_(sql, params);
    }

    /// Execute a parameterized SQL query with inline args.
    /// The PgParams object is kept alive in the coroutine frame.
    template<typename... Args>
    static io::Task<io::PgResult> queryArgs(const char* sql, Args&&... args) {
        auto params = io::PgParams::make(std::forward<Args>(args)...);
        co_return co_await queryParams(sql, params);
    }

    /// Execute a command with inline args, returning {affected_rows, coalesced}.
    /// The PgParams object is kept alive in the coroutine frame.
    template<typename... Args>
    static io::Task<std::pair<int, bool>> executeArgs(const char* sql, Args&&... args) {
        auto params = io::PgParams::make(std::forward<Args>(args)...);
        co_return co_await execute(sql, params);
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
    ///   co_await DbProvider::redis("SET", "key", "value");
    ///   co_await DbProvider::redis("SETEX", key, std::to_string(ttl), value);
    ///   co_await DbProvider::redis("EVAL", lua_script, "1", tracking_key);
    ///   co_await DbProvider::redis("SETEX", key, ttl_str,
    ///       std::string_view(reinterpret_cast<const char*>(bin.data()), bin.size()));
    template<typename... Args>
    static io::Task<io::RedisResult> redis(Args&&... args) {
        assert(redis_exec_ && "DbProvider::redis() called before init() or Redis not configured");

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

    /// Check if Redis is configured.
    [[nodiscard]] static bool hasRedis() noexcept {
        return redis_exec_ != nullptr;
    }

    /// Check if DbProvider has been initialized.
    [[nodiscard]] static bool initialized() noexcept {
        return pg_query_ != nullptr;
    }

    // =========================================================================
    // Initialization (call once at startup)
    // =========================================================================

    /// Initialize with a BatchScheduler (PG pipelining + Redis pipelining).
    /// The IoContext type is erased — callers don't need to know it.
    /// Redis commands are routed through the BatchScheduler for pipelining.
    template<typename Io>
    static void init(
        Io& io,
        std::shared_ptr<io::PgPool<Io>> pool,
        std::shared_ptr<io::RedisClient<Io>> redisClient = nullptr,
        int max_concurrent = 8)
    {
        // Wrap single RedisClient into a RedisPool for the BatchScheduler
        std::shared_ptr<io::RedisPool<Io>> redis_pool;
        if (redisClient) {
            redis_pool = std::make_shared<io::RedisPool<Io>>(
                io::RedisPool<Io>::fromClients({std::move(redisClient)}));
        }

        auto batcher = std::make_shared<io::batch::BatchScheduler<Io>>(
            io, std::move(pool), redis_pool, max_concurrent);

        pg_query_ = [batcher](const char* sql) {
            return batcher->directQuery(sql);
        };
        pg_query_params_ = [batcher](const char* sql, const io::PgParams& params)
            -> io::Task<io::PgResult>
        {
            co_return co_await batcher->submitQueryRead(sql, io::PgParams{params});
        };
        pg_execute_ = [batcher](const char* sql, const io::PgParams& params)
            -> io::Task<std::pair<int, bool>>
        {
            co_return co_await batcher->submitPgExecute(sql, io::PgParams{params});
        };

        if (redis_pool) {
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
        pg_execute_ = nullptr;
        redis_exec_ = nullptr;
    }

    // =========================================================================
    // Type-erased function storage (accessible to IoPool for registration)
    // =========================================================================

    using PgQueryFn = std::function<io::Task<io::PgResult>(const char*)>;
    using PgQueryParamsFn = std::function<io::Task<io::PgResult>(
        const char*, const io::PgParams&)>;
    using PgExecuteFn = std::function<io::Task<std::pair<int, bool>>(
        const char*, const io::PgParams&)>;
    using RedisExecFn = std::function<io::Task<io::RedisResult>(
        int, const char**, const size_t*)>;

    static inline PgQueryFn pg_query_;
    static inline PgQueryParamsFn pg_query_params_;
    static inline PgExecuteFn pg_execute_;
    static inline RedisExecFn redis_exec_;

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

#endif  // JCX_RELAIS_DB_PROVIDER_H
