#ifndef JCX_RELAIS_IO_REDIS_CLIENT_H
#define JCX_RELAIS_IO_REDIS_CLIENT_H

#include <chrono>
#include <concepts>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "jcailloux/relais/detail/NumberText.h"
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/redis/RedisError.h"
#include "jcailloux/relais/io/redis/RedisResult.h"
#include "jcailloux/relais/io/redis/RedisConnection.h"

namespace jcailloux::relais::io {

// RedisArg — the closed set of types a Redis command argument accepts
//
//   number (any arithmetic type but bool and characters)  exact decimal text
//   std::string, std::string_view, const char*            bytes as is
//
// Redis has no boolean and no null: such an argument must be spelled out.

template<typename T>
concept RedisArg =
    !std::is_enum_v<std::decay_t<T>>  // pass an enum explicitly: std::to_underlying(e) for its integer, or its codec's toDb(e) for its text
    && (relais::detail::TextNumber<std::decay_t<T>> || std::same_as<std::decay_t<T>, std::string>
        || std::same_as<std::decay_t<T>, std::string_view> || std::same_as<std::decay_t<T>, const char*>
        || std::same_as<std::decay_t<T>, char*>);

namespace detail {

/// The bytes a RedisArg is sent as.
inline std::string redisArgText(std::string&& s) noexcept { return std::move(s); }
inline std::string redisArgText(const std::string& s) { return s; }
inline std::string redisArgText(std::string_view s) { return std::string(s); }
inline std::string redisArgText(const char* s) { return s; }
template<relais::detail::TextNumber T>
inline std::string redisArgText(T v) { return relais::detail::toText(v); }

}  // namespace detail

// RedisClient — async Redis client using custom RESP2 protocol + IoContext.
//
// Commands are serialized via a coroutine mutex. Multiple coroutines can
// call exec() concurrently; they will queue and execute one at a time.
// Uses a coroutine mutex to serialize concurrent access to the connection.

template<IoContext Io>
class RedisClient : public std::enable_shared_from_this<RedisClient<Io>> {
public:
    ~RedisClient() = default;

    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    // Async TCP connect

    static Task<std::shared_ptr<RedisClient>> connect(
        Io& io,
        const char* host = "127.0.0.1",
        int port = 6379,
        std::chrono::nanoseconds query_timeout = {}
    ) {
        auto conn = co_await RedisConnection<Io>::connectTcp(io, host, port, query_timeout);
        co_return std::shared_ptr<RedisClient>(
            new RedisClient(io, std::move(conn)));
    }

    // Async Unix socket connect

    static Task<std::shared_ptr<RedisClient>> connectUnix(
        Io& io,
        const char* path = "/var/run/redis/redis-server.sock",
        std::chrono::nanoseconds query_timeout = {}
    ) {
        auto conn = co_await RedisConnection<Io>::connectUnix(io, path, query_timeout);
        co_return std::shared_ptr<RedisClient>(
            new RedisClient(io, std::move(conn)));
    }

    // Execute a Redis command (variadic, converts args to strings)

    template<RedisArg... Args>
    Task<RedisResult> exec(Args&&... args) {
        std::vector<std::string> arg_strs;
        arg_strs.reserve(sizeof...(args));
        (arg_strs.push_back(detail::redisArgText(std::forward<Args>(args))), ...);

        std::vector<const char*> argv;
        std::vector<size_t> argvlen;
        argv.reserve(arg_strs.size());
        argvlen.reserve(arg_strs.size());
        for (auto& s : arg_strs) {
            argv.push_back(s.data());
            argvlen.push_back(s.size());
        }

        co_return co_await execArgv(
            static_cast<int>(argv.size()),
            argv.data(),
            argvlen.data());
    }

    // Execute with pre-built argv

    /// Descriptor for a single command in a pipeline (non-owning).
    struct PipelineCmd {
        int argc;
        const char** argv;
        const size_t* argvlen;
    };

    /// Execute N commands as a single pipeline.
    /// Acquires the lock once, queues all commands, flushes once, reads N results.
    Task<std::vector<RedisResult>> pipelineExec(const PipelineCmd* cmds, int count) {
        co_await acquireLock();

        std::vector<RedisResult> results;
        results.reserve(count);

        try {
            for (int i = 0; i < count; ++i)
                conn_.queueCommand(cmds[i].argc, cmds[i].argv, cmds[i].argvlen);

            co_await conn_.flushPipeline();

            auto parsers = co_await conn_.readPipelineResults(count);
            for (auto& p : parsers)
                results.emplace_back(std::move(p));
        } catch (...) {
            releaseLock();
            throw;
        }

        releaseLock();
        co_return results;
    }

    Task<RedisResult> execArgv(int argc, const char** argv, const size_t* argvlen) {
        // Serialize access: wait if another command is in progress
        co_await acquireLock();

        RedisResult result;
        try {
            co_await conn_.sendCommand(argc, argv, argvlen);

            bool ok = co_await conn_.readResponse();
            if (!ok)
                throw RedisError("Redis connection closed");

            // Move parsed data into a shared_ptr for RedisResult ownership
            auto parser = std::make_shared<RespParser>();
            std::swap(*parser, conn_.parser());

            result = RedisResult(std::move(parser));
        } catch (...) {
            releaseLock();
            throw;
        }

        releaseLock();

        if (result.isError())
            throw RedisError(result.errorMessage());

        co_return result;
    }

    [[nodiscard]] bool connected() const noexcept {
        return conn_.connected();
    }

    /// True when no coroutine is suspended inside this client: no lock holder
    /// (busy_ — a holder mid-command is suspended in the connection's awaiter) and
    /// no queued waiter. The pool checks this before destroying a dead client —
    /// tearing one down while a frame is suspended in its connection or lock queue
    /// would be a use-after-free.
    [[nodiscard]] bool isQuiescent() const noexcept {
        return !busy_ && waiters_.empty();
    }

private:
    explicit RedisClient(Io& io, RedisConnection<Io> conn) noexcept
        : io_(&io), conn_(std::move(conn)) {}

    // Coroutine mutex — serializes command execution

    struct LockAwaiter {
        RedisClient* self;

        bool await_ready() const noexcept { return !self->busy_; }

        void await_suspend(std::coroutine_handle<> h) {
            self->waiters_.push_back(h);
        }

        void await_resume() noexcept {
            self->busy_ = true;
        }
    };

    LockAwaiter acquireLock() { return {this}; }

    void releaseLock() {
        if (!waiters_.empty()) {
            auto next = waiters_.front();
            waiters_.pop_front();
            // Resume via post to avoid deep stack recursion
            io_->post([next] { next.resume(); });
        } else {
            busy_ = false;
        }
    }

    Io* io_;
    RedisConnection<Io> conn_;
    bool busy_ = false;
    std::deque<std::coroutine_handle<>> waiters_;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_CLIENT_H
