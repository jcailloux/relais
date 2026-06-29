#ifndef JCX_RELAIS_IO_REDIS_CONNECTION_H
#define JCX_RELAIS_IO_REDIS_CONNECTION_H

#include <cassert>
#include <chrono>
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
            // Cancel any armed watch-bound timer BEFORE close(fd_): a pending timer
            // callback captures `this`, so firing it after the object is gone would
            // be a use-after-free. removeCurrentWatch does both (timer + watch).
            removeCurrentWatch();
            ::close(fd_);
        }
    }

    RedisConnection(RedisConnection&& o) noexcept
        : io_(o.io_)
        , fd_(std::exchange(o.fd_, -1))
        , watch_(std::exchange(o.watch_, {}))
        , watch_active_(std::exchange(o.watch_active_, false))
        // dead_ + timer token + stored continuation must travel with the connection.
        // RedisPool/RedisClient move connections during construction; dropping dead_
        // would resurrect a poisoned connection (connected() == fd_>=0 on a silent
        // hang), and dropping the timer token would orphan it.
        , dead_(o.dead_)
        , current_cont_(std::exchange(o.current_cont_, {}))
        , timer_(std::exchange(o.timer_, {}))
        , timer_armed_(std::exchange(o.timer_armed_, false))
        , query_timeout_(o.query_timeout_)
    {
        // Never move mid-wait: the watch callback (and timer) capture a raw `this`
        // that move does not patch → UAF. watch_active_ catches the query_timeout=0
        // case too (watch, no timer). Holds by run-to-completion; assert guards it.
        assert(!watch_active_);
    }

    RedisConnection& operator=(RedisConnection&& o) noexcept {
        if (this != &o) {
            assert(!o.watch_active_);  // never move mid-wait (watch/timer → UAF)
            if (fd_ >= 0) {
                removeCurrentWatch();  // cancel timer before close (timer → UAF)
                ::close(fd_);
            }
            io_ = o.io_;
            fd_ = std::exchange(o.fd_, -1);
            watch_ = std::exchange(o.watch_, {});
            watch_active_ = std::exchange(o.watch_active_, false);
            dead_ = o.dead_;
            current_cont_ = std::exchange(o.current_cont_, {});
            timer_ = std::exchange(o.timer_, {});
            timer_armed_ = std::exchange(o.timer_armed_, false);
            query_timeout_ = o.query_timeout_;
        }
        return *this;
    }

    RedisConnection(const RedisConnection&) = delete;
    RedisConnection& operator=(const RedisConnection&) = delete;

    [[nodiscard]] bool connected() const noexcept {
        // dead_ first: a timed-out connection is poisoned even though the fd stays
        // valid on a *silent* hang (socket retained, no RST). Callers must see false
        // and recreate it, not reuse it.
        return !dead_ && fd_ >= 0;
    }
    [[nodiscard]] int fd() const noexcept { return fd_; }

    // Async TCP connect

    // query_timeout is stored on the connection as the bound for subsequent I/O
    // waits — and also bounds this connect handshake's awaitWriteReady. 0 =
    // unbounded (delegated to the OS), which keeps direct callers compiling.
    static Task<RedisConnection> connectTcp(Io& io, const char* host, int port,
                                            std::chrono::nanoseconds query_timeout = {}) {
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

        RedisConnection conn(io, fd, query_timeout);

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

    static Task<RedisConnection> connectUnix(Io& io, const char* path,
                                             std::chrono::nanoseconds query_timeout = {}) {
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

        RedisConnection conn(io, fd, query_timeout);

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
    explicit RedisConnection(Io& io, int fd,
                             std::chrono::nanoseconds query_timeout = {}) noexcept
        : io_(&io), fd_(fd), query_timeout_(query_timeout) {}

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

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) {
            self->current_cont_ = h;
            self->registerWatch(events, [this](IoEvent) {
                // Save before removeCurrentWatch destroys this lambda (and its captures)
                auto coro = self->current_cont_;
                self->removeCurrentWatch();
                coro.resume();
            }, self->query_timeout_);
        }

        // No longer noexcept: on a query timeout the timer posts the resume with
        // dead_ set — returning normally would swallow the timeout (resume on a dead
        // Redis connection); a noexcept body + throw would std::terminate. The single
        // awaiter covers read AND write waits, so this is the only "hang → exception"
        // link Redis needs.
        void await_resume() {
            if (self->dead_)
                throw RedisQueryTimeout("redis I/O wait exceeded query_timeout");
        }
    };

    EventAwaiter awaitReadReady() { return {this, IoEvent::Read}; }
    EventAwaiter awaitWriteReady() { return {this, IoEvent::Write}; }

    // Watch management (same pattern as PgConnection)

    void registerWatch(IoEvent events, std::function<void(IoEvent)> cb,
                       std::chrono::nanoseconds timeout) {
        // Route the existing-watch removal through removeCurrentWatch so a leftover
        // timer from the previous wait is cancelled too — a bare removeWatch here
        // would leak the predecessor's timer on every awaiter chaining.
        removeCurrentWatch();
        watch_ = io_->addWatch(fd_, events, std::move(cb));
        watch_active_ = true;
        // Arm the watch-bound timeout AFTER the watch is posted. Invariant: timer
        // armed ⟺ watch active ⟺ connection waiting on I/O. 0 = unbounded.
        if (timeout.count() > 0) {
            timer_ = io_->postDelayed(timeout, [this] { timeoutCurrentWait(); });
            timer_armed_ = true;
        }
    }

    void removeCurrentWatch() {
        // cancelTimer BEFORE removeWatch: once the timer is gone the expiry callback
        // cannot run, so the socket-success path that calls this fully neutralises a
        // co-resident timeout. Single point that does both.
        if (timer_armed_) {
            io_->cancelTimer(timer_);
            timer_armed_ = false;
        }
        if (watch_active_) {
            io_->removeWatch(watch_);
            watch_active_ = false;
        }
    }

    // Watch-bound timer expiry (mirrors PgConnection). Neutralise the co-resident
    // socket event (removeCurrentWatch), poison the connection (dead_), and resume
    // the stored continuation DEFERRED via io_.post — never inline, so the unwind
    // lands in drainPosted() instead of fireExpiredTimers(): no re-entrant
    // cancelTimer, and the fd is never closed mid-timer-dispatch.
    // EventAwaiter::await_resume sees dead_ and throws RedisQueryTimeout.
    void timeoutCurrentWait() {
        // The token is already removed from the timer subsystem (it just fired);
        // clearing the flag first keeps removeCurrentWatch from re-cancelling it.
        timer_armed_ = false;
        removeCurrentWatch();
        dead_ = true;
        if (current_cont_) {
            auto coro = current_cont_;
            io_->post([coro] { coro.resume(); });
        }
    }

    Io* io_;
    int fd_ = -1;
    typename Io::WatchHandle watch_{};
    bool watch_active_ = false;

    RespWriter writer_;
    RespParser parser_;
    std::string readBuf_;

    // Timeout machinery — mirrors PgConnection. current_cont_ is the single
    // in-flight continuation per connection (RedisClient serialises commands via its
    // coroutine mutex), set by EventAwaiter::await_suspend so timeoutCurrentWait can
    // resume it without knowing which wait is parked.
    bool dead_ = false;
    std::coroutine_handle<> current_cont_{};
    typename Io::TimerToken timer_{};
    bool timer_armed_ = false;
    std::chrono::nanoseconds query_timeout_{};  // bound for I/O waits
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_CONNECTION_H
