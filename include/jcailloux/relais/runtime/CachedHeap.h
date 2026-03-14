#ifndef JCX_RELAIS_RUNTIME_CACHED_HEAP_H
#define JCX_RELAIS_RUNTIME_CACHED_HEAP_H

#include <atomic>
#include <cstdint>

#include <malloc.h>

namespace jcailloux::relais::runtime {

/// CachedHeap — background-refreshed live heap usage for hot paths.
///
/// Updated by RuntimeThread every ~100ms via tick().
/// Reads are a single relaxed atomic load (~1ns, zero contention).
///
/// Implementation: mallinfo2() — live heap bytes (uordblks + hblkhd).
/// Unlike RSS, this reflects actual allocated memory: free() reduces it
/// immediately.  Cost per tick: ~1-5µs (iterates malloc arenas).
struct CachedHeap {
    /// Hot path: current live heap in bytes. Single mov from L1 cache, ~1ns.
    static uint64_t bytes() noexcept {
        return bytes_.load(std::memory_order_relaxed);
    }

    /// Called by RuntimeThread every ~100ms.
    static void tick() noexcept {
        auto info = ::mallinfo2();
        auto heap = static_cast<uint64_t>(info.uordblks + info.hblkhd);
        bytes_.store(heap, std::memory_order_relaxed);
    }

private:
    alignas(64) static inline std::atomic<uint64_t> bytes_{0};
};

}  // namespace jcailloux::relais::runtime

#endif  // JCX_RELAIS_RUNTIME_CACHED_HEAP_H