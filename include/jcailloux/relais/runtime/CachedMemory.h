#ifndef JCX_RELAIS_RUNTIME_CACHED_MEMORY_H
#define JCX_RELAIS_RUNTIME_CACHED_MEMORY_H

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>

#include <fcntl.h>
#include <unistd.h>

// Allocator backend: detect jemalloc at runtime via weak symbol,
// fall back to glibc mallinfo2, then RSS-only.
//
// Weak mallctl: resolves to jemalloc if linked (-ljemalloc) or LD_PRELOAD'd,
// null otherwise. No jemalloc-devel header required at build time.
// When jemalloc intercepts malloc, glibc's mallinfo2 reports ~0 (glibc sees
// no allocations), so runtime detection is required for correctness.
extern "C" {
__attribute__((weak)) int mallctl(const char*, void*, size_t*, void*, size_t);
}

#ifdef __GLIBC__
    #include <malloc.h>
#endif

namespace jcailloux::relais::runtime {

/// CachedMemory — background-refreshed live process memory for hot paths.
///
/// Updated by RuntimeThread every ~100ms via tick().
/// Reads are a single relaxed atomic load (~1ns, zero contention).
///
/// Measurement strategy: min(allocator_heap, RSS).
///   - Allocator heap is responsive to free() but can overestimate
///     (mmap'd pages not faulted in, arena fragmentation).
///   - RSS (/proc/self/statm) is exact physical memory but lags after free()
///     until the kernel reclaims pages.
///   min() picks the tighter bound: heap wins post-eviction (instant drop),
///   RSS wins at steady state (no mmap inflation).
///
/// Allocator backends (detected at runtime/compile-time):
///   - jemalloc: mallctl("stats.allocated") — ~50ns, single atomic read.
///               Detected via weak symbol at runtime (link or LD_PRELOAD).
///   - glibc:    mallinfo2() — ~2-6µs, iterates arenas.
///   - fallback: RSS only (no heap measurement available).
///
/// Budget should represent total process memory target (not heap-only).
struct CachedMemory {
    /// Hot path: current live memory in bytes. Single mov from L1 cache, ~1ns.
    static uint64_t bytes() noexcept {
        return bytes_.load(std::memory_order_relaxed);
    }

    /// Called by RuntimeThread every ~100ms and by sweep after eviction.
    static void tick() noexcept {
        auto heap = readHeap();
        auto rss = readRSS();
        bytes_.store(std::min(heap, rss), std::memory_order_relaxed);
    }

private:
    /// Read allocated bytes from the process allocator.
    /// Returns UINT64_MAX if unavailable (min() falls back to RSS).
    static uint64_t readHeap() noexcept {
        // Runtime detection: prefer jemalloc if linked/LD_PRELOAD'd.
        if (::mallctl) {
            // mallctl("stats.allocated"): total bytes in active allocations.
            // ~50ns — single atomic read inside jemalloc, no arena iteration.
            // Epoch must be advanced for stats to refresh.
            uint64_t epoch = 1;
            size_t epoch_len = sizeof(epoch);
            ::mallctl("epoch", &epoch, &epoch_len, &epoch, epoch_len);

            uint64_t allocated = 0;
            size_t len = sizeof(allocated);
            if (::mallctl("stats.allocated", &allocated, &len, nullptr, 0) == 0)
                return allocated;
        }
#ifdef __GLIBC__
        auto info = ::mallinfo2();
        return static_cast<uint64_t>(info.uordblks + info.hblkhd);
#else
        return UINT64_MAX;
#endif
    }

    /// Read RSS from /proc/self/statm (field 1 = resident pages).
    /// fd kept open across ticks — single background thread, no contention.
    /// Returns UINT64_MAX on failure (min() falls back to heap).
    static uint64_t readRSS() noexcept {
        static const int fd = ::open("/proc/self/statm", O_RDONLY);
        static const long page_size = ::sysconf(_SC_PAGESIZE);
        if (fd < 0) [[unlikely]] return UINT64_MAX;

        char buf[64];
        ::lseek(fd, 0, SEEK_SET);
        auto n = ::read(fd, buf, sizeof(buf) - 1);
        if (n <= 0) [[unlikely]] return UINT64_MAX;
        buf[n] = '\0';

        // Skip VmSize (field 0), parse VmRSS (field 1).
        const char* p = buf;
        while (*p > ' ') ++p;
        return std::strtoull(p, nullptr, 10)
             * static_cast<uint64_t>(page_size);
    }

    alignas(64) static inline std::atomic<uint64_t> bytes_{0};
};

}  // namespace jcailloux::relais::runtime

#endif  // JCX_RELAIS_RUNTIME_CACHED_MEMORY_H
