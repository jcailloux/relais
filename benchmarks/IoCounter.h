#pragma once

/**
 * IoCounter.h
 *
 * Work counters of the benchmark process, read around a measurement window:
 *
 *   pg_exec    statements sent to PostgreSQL (PQsendQueryParams, PQsendQueryPrepared)
 *   pg_prepare statements prepared (PQsendPrepare)
 *   heap       allocations of every thread (only with AllocCounter.h)
 *
 * They count this process only, at the client: another process using the same
 * database (a test run) does not change them, nor does CPU load.
 * Concurrent coalescing still makes the counts of a multi-coroutine case vary
 * slightly from run to run.
 *
 * libpq's entry points are interposed: this executable defines them, counts,
 * and forwards to the real ones. Include in exactly one translation unit of a
 * benchmark executable (BenchEngine.h does).
 */

#include <atomic>
#include <cstdint>
#include <cstdlib>

#include <dlfcn.h>

#include <libpq-fe.h>

namespace relais_bench::io_count {

inline std::atomic<uint64_t> pg_exec{0};
inline std::atomic<uint64_t> pg_prepare{0};

/// Allocations of every thread, installed by AllocCounter.h; null without it.
inline uint64_t (*heap_source)() noexcept = nullptr;

template<typename Fn>
Fn real(const char* name) {
    void* f = dlsym(RTLD_NEXT, name);
    if (!f) std::abort();
    return reinterpret_cast<Fn>(f);
}

struct Counts {
    uint64_t pg_exec = 0;
    uint64_t pg_prepare = 0;
    uint64_t heap = 0;
};

/// A few relaxed loads: cheap enough to take around every operation.
inline Counts snapshot() noexcept {
    return {pg_exec.load(std::memory_order_relaxed),
            pg_prepare.load(std::memory_order_relaxed),
            heap_source ? heap_source() : 0};
}

/// Work per operation over a window.
struct PerOp {
    double pg_exec = 0;
    double pg_prepare = 0;
    double heap = 0;
    bool valid = false;
};

inline PerOp perOp(const Counts& before, const Counts& after, double ops) {
    if (ops <= 0) return {};
    return {static_cast<double>(after.pg_exec - before.pg_exec) / ops,
            static_cast<double>(after.pg_prepare - before.pg_prepare) / ops,
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

}  // extern "C"
