#ifndef JCX_RELAIS_IO_REDIS_CLIENT_H
#define JCX_RELAIS_IO_REDIS_CLIENT_H

#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/redis/RedisError.h"
#include "jcailloux/relais/io/redis/RedisResult.h"
#include "jcailloux/relais/io/redis/RedisConnection.h"

namespace jcailloux::relais::io {

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
        int port = 6379
    ) {
        auto conn = co_await RedisConnection<Io>::connectTcp(io, host, port);
        co_return std::shared_ptr<RedisClient>(
            new RedisClient(io, std::move(conn)));
    }

    // Async Unix socket connect

    static Task<std::shared_ptr<RedisClient>> connectUnix(
        Io& io,
        const char* path = "/var/run/redis/redis-server.sock"
    ) {
        auto conn = co_await RedisConnection<Io>::connectUnix(io, path);
        co_return std::shared_ptr<RedisClient>(
            new RedisClient(io, std::move(conn)));
    }

    // Execute a Redis command (variadic, converts args to strings)

    template<typename... Args>
    Task<RedisResult> exec(Args&&... args) {
        std::vector<std::string> arg_strs;
        arg_strs.reserve(sizeof...(args));
        (arg_strs.push_back(toString(std::forward<Args>(args))), ...);

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

private:
    explicit RedisClient(Io& io, RedisConnection<Io> conn) noexcept
        : io_(&io), conn_(std::move(conn)) {}

    static std::string toString(const char* s) { return s; }
    static std::string toString(std::string_view s) { return std::string(s); }
    static std::string toString(const std::string& s) { return s; }
    static std::string toString(int64_t v) { return std::to_string(v); }
    static std::string toString(int32_t v) { return std::to_string(v); }
    static std::string toString(double v) { return std::to_string(v); }

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
