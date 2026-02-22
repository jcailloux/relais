#ifndef JCX_RELAIS_IO_REDIS_CONNECTION_H
#define JCX_RELAIS_IO_REDIS_CONNECTION_H

#include <coroutine>
#include <cstring>
#include <functional>
#include <string>
#include <utility>

#include <cerrno>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/redis/RedisError.h"
#include "jcailloux/relais/io/redis/RespWriter.h"
#include "jcailloux/relais/io/redis/RespParser.h"

namespace jcailloux::relais::io {

// RedisConnection — async TCP/Unix socket connection with RESP2 protocol.
//
// Uses IoContext for async I/O. Manages send/receive buffers and
// incremental RESP2 parsing.

template<IoContext Io>
class RedisConnection {
public:
    ~RedisConnection() {
        if (fd_ >= 0) {
            if (watch_active_) io_->removeWatch(watch_);
            ::close(fd_);
        }
    }

    RedisConnection(RedisConnection&& o) noexcept
        : io_(o.io_)
        , fd_(std::exchange(o.fd_, -1))
        , watch_(std::exchange(o.watch_, {}))
        , watch_active_(std::exchange(o.watch_active_, false))
    {}

    RedisConnection& operator=(RedisConnection&& o) noexcept {
        if (this != &o) {
            if (fd_ >= 0) {
                if (watch_active_) io_->removeWatch(watch_);
                ::close(fd_);
            }
            io_ = o.io_;
            fd_ = std::exchange(o.fd_, -1);
            watch_ = std::exchange(o.watch_, {});
            watch_active_ = std::exchange(o.watch_active_, false);
        }
        return *this;
    }

    RedisConnection(const RedisConnection&) = delete;
    RedisConnection& operator=(const RedisConnection&) = delete;

    [[nodiscard]] bool connected() const noexcept { return fd_ >= 0; }
    [[nodiscard]] int fd() const noexcept { return fd_; }

    // Async TCP connect

    static Task<RedisConnection> connectTcp(Io& io, const char* host, int port) {
        struct addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        auto portStr = std::to_string(port);
        struct addrinfo* res = nullptr;
        int err = ::getaddrinfo(host, portStr.c_str(), &hints, &res);
        if (err != 0)
            throw RedisConnectionError(
                std::string("getaddrinfo failed: ") + gai_strerror(err));

        // RAII guard for addrinfo
        struct AddrGuard {
            struct addrinfo* p;
            ~AddrGuard() { if (p) freeaddrinfo(p); }
        } guard{res};

        int fd = ::socket(res->ai_family,
            res->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC, res->ai_protocol);
        if (fd < 0)
            throw RedisConnectionError("socket() failed: " + std::string(strerror(errno)));

        int ret = ::connect(fd, res->ai_addr, res->ai_addrlen);
        if (ret < 0 && errno != EINPROGRESS) {
            ::close(fd);
            throw RedisConnectionError("connect() failed: " + std::string(strerror(errno)));
        }

        RedisConnection conn(io, fd);

        if (ret < 0) {
            // EINPROGRESS — await write-ready, then check SO_ERROR
            co_await conn.awaitWriteReady();

            int so_err = 0;
            socklen_t len = sizeof(so_err);
            ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &len);
            if (so_err != 0) {
                throw RedisConnectionError(
                    "async connect failed: " + std::string(strerror(so_err)));
            }
        }

        co_return std::move(conn);
    }

    // Async Unix socket connect

    static Task<RedisConnection> connectUnix(Io& io, const char* path) {
        int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0)
            throw RedisConnectionError("socket() failed: " + std::string(strerror(errno)));

        struct sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);

        int ret = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
        if (ret < 0 && errno != EINPROGRESS) {
            ::close(fd);
            throw RedisConnectionError(
                "Unix connect failed: " + std::string(strerror(errno)));
        }

        RedisConnection conn(io, fd);

        if (ret < 0) {
            co_await conn.awaitWriteReady();

            int so_err = 0;
            socklen_t len = sizeof(so_err);
            ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &len);
            if (so_err != 0) {
                throw RedisConnectionError(
                    "async Unix connect failed: " + std::string(strerror(so_err)));
            }
        }

        co_return std::move(conn);
    }

    // Send a command and read the response.
    // Serializes via RespWriter, sends, then reads and parses the response.

    Task<void> sendCommand(int argc, const char** argv, const size_t* argvlen) {
        writer_.writeCommand(argc, argv, argvlen);
        co_await flushWrite();
    }

    Task<bool> readResponse() {
        // Try parsing what we already have in the read buffer
        if (!readBuf_.empty()) {
            size_t consumed = parser_.parse(readBuf_.data(), readBuf_.size());
            if (consumed > 0) {
                readBuf_.erase(0, consumed);
                co_return true;
            }
        }

        // Need more data
        while (true) {
            co_await awaitReadReady();

            char buf[8192];
            ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
            if (n <= 0) {
                if (n == 0) co_return false; // Connection closed
                if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                if (errno == EINTR) continue;
                throw RedisError("recv failed: " + std::string(strerror(errno)));
            }

            readBuf_.append(buf, static_cast<size_t>(n));

            size_t consumed = parser_.parse(readBuf_.data(), readBuf_.size());
            if (consumed > 0) {
                readBuf_.erase(0, consumed);
                co_return true;
            }
        }
    }

    [[nodiscard]] RespParser& parser() noexcept { return parser_; }
    [[nodiscard]] const RespParser& parser() const noexcept { return parser_; }

    // =========================================================================
    // Pipeline mode — queue multiple commands, flush once, read N responses
    // =========================================================================

    /// Queue a command into the write buffer without flushing.
    void queueCommand(int argc, const char** argv, const size_t* argvlen) {
        writer_.writeCommand(argc, argv, argvlen);
    }

    /// Flush the entire write buffer (all queued commands) to the server.
    Task<void> flushPipeline() {
        co_await flushWrite();
    }

    /// Read N pipeline responses sequentially.
    /// Returns a vector of shared RespParsers, one per response.
    Task<std::vector<std::shared_ptr<RespParser>>> readPipelineResults(int n) {
        std::vector<std::shared_ptr<RespParser>> results;
        results.reserve(n);

        for (int i = 0; i < n; ++i) {
            parser_.reset();
            bool ok = co_await readResponse();
            if (!ok)
                throw RedisError("Redis connection closed during pipeline read");

            auto p = std::make_shared<RespParser>();
            std::swap(*p, parser_);
            results.push_back(std::move(p));
        }

        co_return results;
    }

private:
    explicit RedisConnection(Io& io, int fd) noexcept : io_(&io), fd_(fd) {}

    // Flush write buffer
    Task<void> flushWrite() {
        while (!writer_.empty()) {
            ssize_t n = ::send(fd_, writer_.data(), writer_.size(), MSG_NOSIGNAL);
            if (n > 0) {
                writer_.consume(static_cast<size_t>(n));
                continue;
            }
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    co_await awaitWriteReady();
                    continue;
                }
                if (errno == EINTR) continue;
                throw RedisError("send failed: " + std::string(strerror(errno)));
            }
        }
    }

    // Await events helpers

    struct EventAwaiter {
        RedisConnection* self;
        IoEvent events;
        std::coroutine_handle<> continuation;

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            continuation = h;
            self->registerWatch(events, [this](IoEvent) {
                self->removeCurrentWatch();
                continuation.resume();
            });
        }

        void await_resume() noexcept {}
    };

    EventAwaiter awaitReadReady() { return {this, IoEvent::Read, {}}; }
    EventAwaiter awaitWriteReady() { return {this, IoEvent::Write, {}}; }

    // Watch management (same pattern as PgConnection)

    void registerWatch(IoEvent events, std::function<void(IoEvent)> cb) {
        if (watch_active_) io_->removeWatch(watch_);
        watch_ = io_->addWatch(fd_, events, std::move(cb));
        watch_active_ = true;
    }

    void removeCurrentWatch() {
        if (watch_active_) {
            io_->removeWatch(watch_);
            watch_active_ = false;
        }
    }

    Io* io_;
    int fd_ = -1;
    typename Io::WatchHandle watch_{};
    bool watch_active_ = false;

    RespWriter writer_;
    RespParser parser_;
    std::string readBuf_;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_CONNECTION_H
