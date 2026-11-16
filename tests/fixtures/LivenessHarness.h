#ifndef JCX_RELAIS_TEST_LIVENESS_HARNESS_H
#define JCX_RELAIS_TEST_LIVENESS_HARNESS_H

// What the suites that drive PostgreSQL through a fault share: fake servers
// that stall or reshape a connect, a descriptor count, and drivers that bound
// every wait, over any loop. Catch2 test translation units only (REQUIRE).

#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/Task.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <string>
#include <thread>
#include <utility>
#include <variant>

namespace jcailloux::relais::io::test {

// A TCP listener that accepts the kernel handshake (so connect() completes) but
// never reads or replies. To a PG client the socket is writable for the startup
// packet, then never readable — the exact silent hang the connect/query bound
// must catch.
struct SilentTcpServer {
    int fd = -1;
    int port_ = 0;

    SilentTcpServer() {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        REQUIRE(fd >= 0);
        int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;  // ephemeral
        REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        REQUIRE(::listen(fd, 16) == 0);
        socklen_t len = sizeof(addr);
        REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
        port_ = ntohs(addr.sin_port);
    }
    ~SilentTcpServer() { if (fd >= 0) ::close(fd); }
    SilentTcpServer(const SilentTcpServer&) = delete;
    SilentTcpServer& operator=(const SilentTcpServer&) = delete;

    int port() const { return port_; }
    std::string conninfo() const {
        return "host=127.0.0.1 port=" + std::to_string(port_) +
               " dbname=relais_test user=relais_test password=x";
    }
};

// A listener whose accept queue is full: the kernel drops the SYN of any
// further connect, which stays pending (SYN_SENT) until the client gives up. Its
// socket is neither readable nor writable meanwhile; libpq, polled anyway,
// would take the connect for made.
struct PendingConnectServer {
    PendingConnectServer() {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        REQUIRE(fd >= 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;  // ephemeral
        REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        REQUIRE(::listen(fd, 0) == 0);  // one connection fills the queue
        socklen_t len = sizeof(addr);
        REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
        port_ = ntohs(addr.sin_port);
        filler = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        REQUIRE(filler >= 0);
        REQUIRE(::connect(filler, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    }
    ~PendingConnectServer() {
        ::close(filler);
        ::close(fd);
    }
    PendingConnectServer(const PendingConnectServer&) = delete;
    PendingConnectServer& operator=(const PendingConnectServer&) = delete;

    std::string conninfo() const {
        return "host=127.0.0.1 port=" + std::to_string(port_) +
               " dbname=d user=u sslmode=disable gssencmode=disable";
    }

private:
    int fd = -1;
    int filler = -1;  // accepted by the kernel, never by us: the queue stays full
    int port_ = 0;
};

// A fake PG server that makes libpq replace its socket inside one PQconnectPoll
// call. The first connection accepts TLS ('S' to the SSLRequest), then drops as
// the ClientHello arrives; under sslmode=prefer libpq retries in plaintext on a
// new socket. The second connection answers the StartupMessage with a FATAL
// ErrorResponse. A client that follows the new socket fails fast with kError; one
// still watching the old socket waits for acquire_timeout. One thread, blocking
// I/O bounded by poll, two connections served once.
struct TlsDropServer {
    static constexpr const char* kError = "fake: plaintext reached";

    std::atomic<bool> saw_ssl_request{false};
    std::atomic<bool> saw_plain_startup{false};

    TlsDropServer() {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        REQUIRE(fd >= 0);
        int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;  // ephemeral
        REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        REQUIRE(::listen(fd, 4) == 0);
        socklen_t len = sizeof(addr);
        REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { serve(); });
    }
    ~TlsDropServer() {
        thread_.join();
        ::close(fd);
    }
    TlsDropServer(const TlsDropServer&) = delete;
    TlsDropServer& operator=(const TlsDropServer&) = delete;

    std::string conninfo() const {
        // gssencmode=disable: no GSSENCRequest ahead of the SSLRequest, even
        // with a Kerberos credential cache around.
        return "host=127.0.0.1 port=" + std::to_string(port_) +
               " user=u dbname=d sslmode=prefer gssencmode=disable";
    }

private:
    int fd = -1;
    int port_ = 0;
    std::thread thread_;

    static bool readFull(int s, void* buf, size_t n) {
        auto* p = static_cast<char*>(buf);
        while (n > 0) {
            pollfd pfd{s, POLLIN, 0};
            if (::poll(&pfd, 1, 2000) <= 0) return false;
            ssize_t r = ::read(s, p, n);
            if (r <= 0) return false;
            p += r;
            n -= static_cast<size_t>(r);
        }
        return true;
    }

    int acceptOne() const {
        pollfd pfd{fd, POLLIN, 0};
        if (::poll(&pfd, 1, 3000) <= 0) return -1;
        return ::accept4(fd, nullptr, nullptr, SOCK_CLOEXEC);
    }

    static uint32_t be32(const unsigned char* b) {
        return (uint32_t{b[0]} << 24) | (uint32_t{b[1]} << 16) | (uint32_t{b[2]} << 8) | b[3];
    }

    void serve() {
        // 1st connection: SSLRequest → 'S', wait for the ClientHello's record
        // header, then drop it mid-handshake.
        int c1 = acceptOne();
        if (c1 < 0) return;
        unsigned char req[8];
        if (readFull(c1, req, sizeof(req)) && be32(req) == 8 && be32(req + 4) == 80877103u) {
            saw_ssl_request = true;
            [[maybe_unused]] ssize_t sent = ::write(c1, "S", 1);
            unsigned char hello[5];
            readFull(c1, hello, sizeof(hello));
        }
        ::close(c1);
        if (!saw_ssl_request) return;  // libpq without TLS: no plaintext retry to serve

        // 2nd connection: plaintext StartupMessage (protocol 3.x) → FATAL error.
        int c2 = acceptOne();
        if (c2 < 0) return;
        unsigned char hdr[8];
        if (readFull(c2, hdr, sizeof(hdr)) && be32(hdr) >= 8 && (be32(hdr + 4) >> 16) == 3) {
            saw_plain_startup = true;
            std::string rest(be32(hdr) - 8, '\0');
            readFull(c2, rest.data(), rest.size());

            const std::pair<char, const char*> fields[] = {
                {'S', "FATAL"}, {'V', "FATAL"}, {'C', "28000"}, {'M', kError}};
            std::string body;
            for (auto [code, value] : fields) {
                body += code;
                body += value;
                body += '\0';
            }
            body += '\0';
            const uint32_t len = htonl(static_cast<uint32_t>(body.size() + 4));
            std::string msg = "E";
            msg.append(reinterpret_cast<const char*>(&len), sizeof(len));
            msg += body;
            [[maybe_unused]] ssize_t sent = ::write(c2, msg.data(), msg.size());
        }
        ::close(c2);
    }
};


// Count process fds. Used as a leak tripwire: the offset (., .., the opendir fd)
// is constant, so a before/after delta is meaningful.
inline int countOpenFds() {
    DIR* d = ::opendir("/proc/self/fd");
    if (!d) return -1;
    int n = 0;
    while (::readdir(d)) ++n;
    ::closedir(d);
    return n;
}

// Bounded synchronous driver. A liveness suite must never hang the runner, so
// every Task is driven against a wall-clock budget; blowing the budget is a test
// failure, not a hang. Mirrors the fixtures' runTask but caps the loop and works
// over any IoContext (so ControllableIoContext's masking layer sees the watches).
template<typename T, typename Loop>
T driveBounded(Loop& io, Task<T> task, std::chrono::milliseconds budget = std::chrono::seconds(5)) {
    std::variant<std::monostate, T, std::exception_ptr> result;
    bool done = false;

    auto wrapper = [&](Task<T> t) -> Task<void> {
        try {
            result.template emplace<1>(co_await std::move(t));
        } catch (...) {
            result.template emplace<2>(std::current_exception());
        }
        done = true;
    };

    struct Starter {
        struct promise_type {
            Starter get_return_object() noexcept {
                return Starter{std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_never initial_suspend() noexcept { return {}; }
            std::suspend_always final_suspend() noexcept { return {}; }
            void return_void() noexcept {}
            void unhandled_exception() noexcept {}
        };
        std::coroutine_handle<promise_type> handle;
    };
    auto starter = [&](Task<void> t) -> Starter { co_await std::move(t); };
    auto s = starter(wrapper(std::move(task)));

    auto deadline = std::chrono::steady_clock::now() + budget;
    while (!done && std::chrono::steady_clock::now() < deadline)
        io.runOnce(20);
    if (s.handle) s.handle.destroy();

    REQUIRE(done);  // never silently time out
    if (auto* ex = std::get_if<2>(&result))
        std::rethrow_exception(*ex);
    return std::move(std::get<1>(result));
}

template<typename Loop>
void driveBounded(Loop& io, Task<void> task, std::chrono::milliseconds budget = std::chrono::seconds(5)) {
    std::exception_ptr ex;
    bool done = false;

    auto wrapper = [&](Task<void> t) -> Task<void> {
        try { co_await std::move(t); }
        catch (...) { ex = std::current_exception(); }
        done = true;
    };
    struct Starter {
        struct promise_type {
            Starter get_return_object() noexcept {
                return Starter{std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_never initial_suspend() noexcept { return {}; }
            std::suspend_always final_suspend() noexcept { return {}; }
            void return_void() noexcept {}
            void unhandled_exception() noexcept {}
        };
        std::coroutine_handle<promise_type> handle;
    };
    auto starter = [&](Task<void> t) -> Starter { co_await std::move(t); };
    auto s = starter(wrapper(std::move(task)));

    auto deadline = std::chrono::steady_clock::now() + budget;
    while (!done && std::chrono::steady_clock::now() < deadline)
        io.runOnce(20);
    if (s.handle) s.handle.destroy();

    REQUIRE(done);
    if (ex) std::rethrow_exception(ex);
}

// Spin the loop for a fixed window so any stray posted resume / timer / late
// readiness would surface (under ASan/TSan) instead of going unobserved.
template<typename Loop>
void pump(Loop& io, std::chrono::milliseconds window = std::chrono::milliseconds(100)) {
    auto deadline = std::chrono::steady_clock::now() + window;
    while (std::chrono::steady_clock::now() < deadline)
        io.runOnce(10);
}

}  // namespace jcailloux::relais::io::test

#endif  // JCX_RELAIS_TEST_LIVENESS_HARNESS_H
