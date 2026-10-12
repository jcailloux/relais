#pragma once

/**
 * IoCounter.h
 *
 * Work counters of the benchmark process, read around a measurement window:
 *
 *   pg_exec    statements sent to PostgreSQL (PQsendQueryParams, PQsendQueryPrepared)
 *   pg_prepare statements prepared (PQsendPrepare)
 *   redis      commands sent to Redis (RESP arrays written to a Redis socket)
 *   heap       allocations of every thread (only with AllocCounter.h)
 *
 * They count this process only, at the client: another process using the same
 * database or Redis (a test run) does not change them, nor does CPU load.
 * Concurrent coalescing still makes the counts of a multi-coroutine case vary
 * slightly from run to run.
 *
 * libpq's entry points and the C library's send/close are interposed: this
 * executable defines them, counts, and forwards to the real ones. Include in
 * exactly one translation unit of a benchmark executable (BenchEngine.h does).
 *
 * BENCH_REDIS_PORT=N — port whose connections are Redis (default 6379)
 */

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include <dlfcn.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <libpq-fe.h>

namespace relais_bench::io_count {

inline std::atomic<uint64_t> pg_exec{0};
inline std::atomic<uint64_t> pg_prepare{0};
inline std::atomic<uint64_t> redis{0};

/// Allocations of every thread, installed by AllocCounter.h; null without it.
inline uint64_t (*heap_source)() noexcept = nullptr;

template<typename Fn>
Fn real(const char* name) {
    void* f = dlsym(RTLD_NEXT, name);
    if (!f) std::abort();
    return reinterpret_cast<Fn>(f);
}

inline uint16_t redisPort() {
    static const uint16_t port = [] {
        if (auto* env = std::getenv("BENCH_REDIS_PORT"))
            if (int v = std::atoi(env); v > 0 && v < 65536) return static_cast<uint16_t>(v);
        return uint16_t{6379};
    }();
    return port;
}

// Per socket: whether it is Redis, and where its outgoing RESP stream stands.
// A socket is written by one thread at a time (one event loop owns it).
struct RespStream {
    enum : uint8_t { kUnknown, kRedis, kOther } kind = kUnknown;
    enum : uint8_t { kArray, kArrayLen, kBulk, kBulkLen, kPayload } state = kArray;
    int64_t number = 0;   // digits read so far (array or bulk length)
    int64_t args = 0;     // bulk strings left in the current command
    int64_t skip = 0;     // payload bytes (+ CRLF) left to skip
};

inline constexpr int kMaxFd = 65536;
inline RespStream streams[kMaxFd];

inline bool isRedis(int fd) {
    auto& s = streams[fd];
    if (s.kind == RespStream::kUnknown) {
        sockaddr_storage peer{};
        socklen_t len = sizeof(peer);
        uint16_t port = 0;
        if (getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &len) == 0) {
            if (peer.ss_family == AF_INET)
                port = ntohs(reinterpret_cast<const sockaddr_in&>(peer).sin_port);
            else if (peer.ss_family == AF_INET6)
                port = ntohs(reinterpret_cast<const sockaddr_in6&>(peer).sin6_port);
        }
        s = {};
        s.kind = port == redisPort() ? RespStream::kRedis : RespStream::kOther;
    }
    return s.kind == RespStream::kRedis;
}

/// Count the commands that start in `data`, carrying the parse across writes.
/// Payloads are skipped by their length, not scanned.
inline uint64_t countCommands(RespStream& s, const char* data, size_t n) {
    uint64_t commands = 0;
    size_t i = 0;
    while (i < n) {
        switch (s.state) {
        case RespStream::kArray:
            if (data[i++] == '*') { ++commands; s.number = 0; s.state = RespStream::kArrayLen; }
            break;
        case RespStream::kArrayLen:
        case RespStream::kBulkLen: {
            const char c = data[i++];
            if (c >= '0' && c <= '9') { s.number = s.number * 10 + (c - '0'); break; }
            if (c != '\n') break;   // '\r'
            if (s.state == RespStream::kArrayLen) {
                s.args = s.number;
                s.state = s.args > 0 ? RespStream::kBulk : RespStream::kArray;
            } else {
                s.skip = s.number + 2;
                s.state = RespStream::kPayload;
            }
            break;
        }
        case RespStream::kBulk:
            if (data[i++] == '$') { s.number = 0; s.state = RespStream::kBulkLen; }
            break;
        case RespStream::kPayload: {
            const auto take = static_cast<int64_t>(n - i) < s.skip
                ? static_cast<int64_t>(n - i) : s.skip;
            i += static_cast<size_t>(take);
            s.skip -= take;
            if (s.skip == 0)
                s.state = --s.args > 0 ? RespStream::kBulk : RespStream::kArray;
            break;
        }
        }
    }
    return commands;
}

struct Counts {
    uint64_t pg_exec = 0;
    uint64_t pg_prepare = 0;
    uint64_t redis = 0;
    uint64_t heap = 0;
};

/// A few relaxed loads: cheap enough to take around every operation.
inline Counts snapshot() noexcept {
    return {pg_exec.load(std::memory_order_relaxed),
            pg_prepare.load(std::memory_order_relaxed),
            redis.load(std::memory_order_relaxed),
            heap_source ? heap_source() : 0};
}

/// Work per operation over a window.
struct PerOp {
    double pg_exec = 0;
    double pg_prepare = 0;
    double redis = 0;
    double heap = 0;
    bool valid = false;
};

inline PerOp perOp(const Counts& before, const Counts& after, double ops) {
    if (ops <= 0) return {};
    return {static_cast<double>(after.pg_exec - before.pg_exec) / ops,
            static_cast<double>(after.pg_prepare - before.pg_prepare) / ops,
            static_cast<double>(after.redis - before.redis) / ops,
            static_cast<double>(after.heap - before.heap) / ops,
            true};
}

}  // namespace relais_bench::io_count

// Interposition: count, then forward.
extern "C" {

int PQsendQueryParams(PGconn* conn, const char* command, int nParams,
                      const Oid* paramTypes, const char* const* paramValues,
                      const int* paramLengths, const int* paramFormats, int resultFormat) {
    using Fn = int (*)(PGconn*, const char*, int, const Oid*, const char* const*,
                       const int*, const int*, int);
    static const Fn f = relais_bench::io_count::real<Fn>("PQsendQueryParams");
    relais_bench::io_count::pg_exec.fetch_add(1, std::memory_order_relaxed);
    return f(conn, command, nParams, paramTypes, paramValues, paramLengths,
             paramFormats, resultFormat);
}

int PQsendQueryPrepared(PGconn* conn, const char* stmtName, int nParams,
                        const char* const* paramValues, const int* paramLengths,
                        const int* paramFormats, int resultFormat) {
    using Fn = int (*)(PGconn*, const char*, int, const char* const*,
                       const int*, const int*, int);
    static const Fn f = relais_bench::io_count::real<Fn>("PQsendQueryPrepared");
    relais_bench::io_count::pg_exec.fetch_add(1, std::memory_order_relaxed);
    return f(conn, stmtName, nParams, paramValues, paramLengths, paramFormats,
             resultFormat);
}

int PQsendPrepare(PGconn* conn, const char* stmtName, const char* query,
                  int nParams, const Oid* paramTypes) {
    using Fn = int (*)(PGconn*, const char*, const char*, int, const Oid*);
    static const Fn f = relais_bench::io_count::real<Fn>("PQsendPrepare");
    relais_bench::io_count::pg_prepare.fetch_add(1, std::memory_order_relaxed);
    return f(conn, stmtName, query, nParams, paramTypes);
}

// send and close go straight to the kernel: resolving the C library's own
// through dlsym could re-enter them while it runs.
ssize_t send(int fd, const void* buf, size_t n, int flags) {
    const ssize_t sent = syscall(SYS_sendto, fd, buf, n, flags, nullptr, 0);
    namespace ic = relais_bench::io_count;
    if (sent > 0 && fd >= 0 && fd < ic::kMaxFd && ic::isRedis(fd)) {
        if (auto c = ic::countCommands(ic::streams[fd], static_cast<const char*>(buf),
                                       static_cast<size_t>(sent)))
            ic::redis.fetch_add(c, std::memory_order_relaxed);
    }
    return sent;
}

int close(int fd) {
    if (fd >= 0 && fd < relais_bench::io_count::kMaxFd)
        relais_bench::io_count::streams[fd] = {};
    return static_cast<int>(syscall(SYS_close, fd));
}

}  // extern "C"
