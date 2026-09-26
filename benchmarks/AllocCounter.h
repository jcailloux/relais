#pragma once

/**
 * AllocCounter.h
 *
 * Counting replacement of the global operator new/delete, for benchmarks that
 * report allocations per operation next to their latency.
 *
 * Replaces the global allocation functions: include in exactly one translation
 * unit of a benchmark executable. Each thread counts its own allocations in a
 * plain thread_local (no shared write), so the counter adds no contention to
 * multi-threaded measurements.
 */

#include <cstdint>
#include <cstdlib>
#include <new>

namespace relais_bench {

constinit thread_local uint64_t tl_alloc_count = 0;

/// Allocations performed by the calling thread since it started.
inline uint64_t threadAllocCount() noexcept { return tl_alloc_count; }

namespace alloc_detail {

inline void* countedMalloc(std::size_t n) noexcept {
    ++tl_alloc_count;
    return std::malloc(n ? n : 1);
}

inline void* countedAlignedMalloc(std::size_t n, std::align_val_t al) noexcept {
    ++tl_alloc_count;
    auto align = static_cast<std::size_t>(al);
    if (align < sizeof(void*)) align = sizeof(void*);
    void* p = nullptr;
    return posix_memalign(&p, align, n ? n : 1) == 0 ? p : nullptr;
}

}  // namespace alloc_detail
}  // namespace relais_bench

void* operator new(std::size_t n) {
    if (void* p = relais_bench::alloc_detail::countedMalloc(n)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (void* p = relais_bench::alloc_detail::countedMalloc(n)) return p;
    throw std::bad_alloc();
}
void* operator new(std::size_t n, std::align_val_t al) {
    if (void* p = relais_bench::alloc_detail::countedAlignedMalloc(n, al)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n, std::align_val_t al) {
    if (void* p = relais_bench::alloc_detail::countedAlignedMalloc(n, al)) return p;
    throw std::bad_alloc();
}
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    return relais_bench::alloc_detail::countedMalloc(n);
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    return relais_bench::alloc_detail::countedMalloc(n);
}
void* operator new(std::size_t n, std::align_val_t al, const std::nothrow_t&) noexcept {
    return relais_bench::alloc_detail::countedAlignedMalloc(n, al);
}
void* operator new[](std::size_t n, std::align_val_t al, const std::nothrow_t&) noexcept {
    return relais_bench::alloc_detail::countedAlignedMalloc(n, al);
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept { std::free(p); }
