#pragma once

/**
 * AllocCounter.h
 *
 * Counting replacement of the global operator new/delete, for benchmarks that
 * report allocations per operation next to their latency.
 *
 * Replaces the global allocation functions: include in exactly one translation
 * unit of a benchmark executable. Each thread counts its own allocations in a
 * plain thread_local and in a slot of its own (no shared write), so the counter
 * adds no contention to multi-threaded measurements.
 */

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>

#include "IoCounter.h"

namespace relais_bench {

constinit thread_local uint64_t tl_alloc_count = 0;

/// Allocations performed by the calling thread since it started.
inline uint64_t threadAllocCount() noexcept { return tl_alloc_count; }

namespace alloc_detail {

/// One cache line per thread: the owner alone writes it, any thread sums.
struct alignas(64) Slot { std::atomic<uint64_t> count{0}; };
inline constexpr size_t kSlots = 1024;
inline Slot slots[kSlots];
inline std::atomic<size_t> next_slot{0};

inline void countOne() noexcept {
    ++tl_alloc_count;
    constinit thread_local Slot* mine = nullptr;
    constinit thread_local bool shared = false;
    if (!mine) [[unlikely]] {
        const size_t i = next_slot.fetch_add(1, std::memory_order_relaxed);
        shared = i >= kSlots - 1;
        mine = &slots[shared ? kSlots - 1 : i];
    }
    // A slot of its own: a plain load and store, no locked instruction. The
    // last slot is shared past kSlots - 1 threads: an atomic add there.
    if (!shared) [[likely]]
        mine->count.store(mine->count.load(std::memory_order_relaxed) + 1,
                          std::memory_order_relaxed);
    else
        mine->count.fetch_add(1, std::memory_order_relaxed);
}

inline uint64_t processAllocCount() noexcept {
    uint64_t total = 0;
    for (auto& s : slots) total += s.count.load(std::memory_order_relaxed);
    return total;
}

inline const bool heap_source_installed =
    (io_count::heap_source = &processAllocCount, true);

inline void* countedMalloc(std::size_t n) noexcept {
    countOne();
    return std::malloc(n ? n : 1);
}

inline void* countedAlignedMalloc(std::size_t n, std::align_val_t al) noexcept {
    countOne();
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
