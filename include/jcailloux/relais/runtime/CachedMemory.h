#ifndef JCX_RELAIS_RUNTIME_CACHED_MEMORY_H
#define JCX_RELAIS_RUNTIME_CACHED_MEMORY_H

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>

#include <fcntl.h>
#include <malloc.h>
#include <unistd.h>

namespace jcailloux::relais::runtime {

/// CachedMemory — background-refreshed live process memory for hot paths.
///
/// Updated by RuntimeThread every ~100ms via tick().
/// Reads are a single relaxed atomic load (~1ns, zero contention).
///
/// Measurement: min(mallinfo2, RSS).
///   - mallinfo2 (uordblks + hblkhd): responsive to free() but overestimates
///     when mmap'd pages aren't faulted in (hash table over-provisioning).
///   - RSS (/proc/self/statm): exact physical memory but lags after free()
///     until the kernel reclaims pages.
///   min() picks the tighter bound in both cases: mallinfo2 wins after
///   eviction (instant drop), RSS wins at steady state (no mmap inflation).
///   Budget should represent total process memory target (not heap-only).
///
/// Cost per tick: ~2-6µs (mallinfo2 arena iteration + procfs read).
struct CachedMemory {
    /// Hot path: current live memory in bytes. Single mov from L1 cache, ~1ns.
    static uint64_t bytes() noexcept {
        return bytes_.load(std::memory_order_relaxed);
    }

    /// Called by RuntimeThread every ~100ms and by sweep after eviction.
    static void tick() noexcept {
        auto info = ::mallinfo2();
        auto heap = static_cast<uint64_t>(info.uordblks + info.hblkhd);
        auto rss = readRSS();
        bytes_.store(std::min(heap, rss), std::memory_order_relaxed);
    }

private:
    /// Read RSS from /proc/self/statm (field 1 = resident pages).
    /// fd kept open across ticks — single background thread, no contention.
    /// Returns UINT64_MAX on failure (min() falls back to mallinfo2).
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
