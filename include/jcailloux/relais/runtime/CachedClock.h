#ifndef JCX_RELAIS_RUNTIME_CACHED_CLOCK_H
#define JCX_RELAIS_RUNTIME_CACHED_CLOCK_H

#include <atomic>
#include <chrono>
#include <cstdint>

namespace jcailloux::relais::runtime {

/// CachedClock — background-refreshed steady_clock for hot paths.
///
/// Updated by RuntimeThread every ~100ms via tick().
/// Reads are a single relaxed atomic load (~1ns, zero contention).
///
/// Stores uint32_t seconds since steady_clock epoch.
/// Precision: 1 second (sufficient for TTL checks with minute+ granularity).
/// Overflow: ~136 years from steady_clock epoch.
struct CachedClock {
    using Clock = std::chrono::steady_clock;

    /// Hot path: single mov from L1 cache, ~1ns.
    static uint32_t now() noexcept {
        return sec_.load(std::memory_order_relaxed);
    }

    /// Called by RuntimeThread every ~100ms.
    static void tick() noexcept {
        sec_.store(currentSec(), std::memory_order_relaxed);
    }

private:
    static uint32_t currentSec() noexcept {
        return static_cast<uint32_t>(
            std::chrono::duration_cast<std::chrono::seconds>(
                Clock::now().time_since_epoch()).count());
    }

    // Own cache line: writer invalidates every ~100ms,
    // readers only read → no reader↔reader bouncing.
    alignas(64) static inline std::atomic<uint32_t> sec_{currentSec()};
};

}  // namespace jcailloux::relais::runtime

#endif  // JCX_RELAIS_RUNTIME_CACHED_CLOCK_H