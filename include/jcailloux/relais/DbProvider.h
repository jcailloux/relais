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

#include "pqcoro/Task.h"
#include "pqcoro/pg/PgPool.h"
#include "pqcoro/pg/PgResult.h"
#include "pqcoro/pg/PgParams.h"
#include "pqcoro/redis/RedisClient.h"
#include "pqcoro/redis/RedisResult.h"

namespace jcailloux::relais {

// =============================================================================
// DbProvider — Type-erased service locator for PostgreSQL and Redis
//
// Wraps pqcoro::PgClient<Io> and pqcoro::RedisClient<Io> behind std::function
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
    static pqcoro::Task<pqcoro::PgResult> query(const char* sql) {
        assert(pg_query_ && "DbProvider::query() called before init()");
        return pg_query_(sql);
    }

    /// Execute a parameterized SQL query.
    /// @note sql and params must remain valid until the co_await completes.
    static pqcoro::Task<pqcoro::PgResult> queryParams(
        const char* sql, const pqcoro::PgParams& params)
    {
        assert(pg_query_params_ && "DbProvider::queryParams() called before init()");
        return pg_query_params_(sql, params);
    }

    /// Execute a command (INSERT/UPDATE/DELETE), returning affected row count.
    /// @note sql and params must remain valid until the co_await completes.
    static pqcoro::Task<int> execute(
        const char* sql, const pqcoro::PgParams& params)
    {
        assert(pg_execute_ && "DbProvider::execute() called before init()");
        return pg_execute_(sql, params);
    }

    /// Execute a parameterized SQL query with inline args.
    /// The PgParams object is kept alive in the coroutine frame.
    template<typename... Args>
    static pqcoro::Task<pqcoro::PgResult> queryArgs(const char* sql, Args&&... args) {
        auto params = pqcoro::PgParams::make(std::forward<Args>(args)...);
        co_return co_await queryParams(sql, params);
    }

    /// Execute a command with inline args, returning affected row count.
    /// The PgParams object is kept alive in the coroutine frame.
    template<typename... Args>
    static pqcoro::Task<int> executeArgs(const char* sql, Args&&... args) {
        auto params = pqcoro::PgParams::make(std::forward<Args>(args)...);
        co_return co_await execute(sql, params);
    }

    // =========================================================================
    // Redis operations
    // =========================================================================

    /// Execute a Redis command with variadic arguments.
    /// Arguments are converted to strings. Binary data can be passed as
    /// std::string_view (including embedded NUL bytes — all args are
    /// binary-safe since hiredis uses argvlen).
    ///
    /// Usage:
    ///   co_await DbProvider::redis("SET", "key", "value");
    ///   co_await DbProvider::redis("SETEX", key, std::to_string(ttl), value);
    ///   co_await DbProvider::redis("EVAL", lua_script, "1", tracking_key);
    ///   co_await DbProvider::redis("SETEX", key, ttl_str,
    ///       std::string_view(reinterpret_cast<const char*>(bin.data()), bin.size()));
    template<typename... Args>
    static pqcoro::Task<pqcoro::RedisResult> redis(Args&&... args) {
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

    /// Initialize with a PgPool and optionally a RedisClient.
    /// The IoContext type is erased — callers don't need to know it.
    template<typename Io>
    static void init(
        std::shared_ptr<pqcoro::PgPool<Io>> pool,
        std::shared_ptr<pqcoro::RedisClient<Io>> redisClient = nullptr)
    {
        auto pg = std::make_shared<pqcoro::PgClient<Io>>(std::move(pool));

        pg_query_ = [pg](const char* sql) {
            return pg->query(sql);
        };
        pg_query_params_ = [pg](const char* sql, const pqcoro::PgParams& params) {
            return pg->queryParams(sql, params);
        };
        pg_execute_ = [pg](const char* sql, const pqcoro::PgParams& params) {
            return pg->execute(sql, params);
        };

        if (redisClient) {
            redis_exec_ = [r = std::move(redisClient)](
                int argc, const char** argv, const size_t* argvlen)
            {
                return r->execArgv(argc, argv, argvlen);
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

private:
    // =========================================================================
    // Type-erased function storage
    // =========================================================================

    using PgQueryFn = std::function<pqcoro::Task<pqcoro::PgResult>(const char*)>;
    using PgQueryParamsFn = std::function<pqcoro::Task<pqcoro::PgResult>(
        const char*, const pqcoro::PgParams&)>;
    using PgExecuteFn = std::function<pqcoro::Task<int>(
        const char*, const pqcoro::PgParams&)>;
    using RedisExecFn = std::function<pqcoro::Task<pqcoro::RedisResult>(
        int, const char**, const size_t*)>;

    static inline PgQueryFn pg_query_;
    static inline PgQueryParamsFn pg_query_params_;
    static inline PgExecuteFn pg_execute_;
    static inline RedisExecFn redis_exec_;

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
