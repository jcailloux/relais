#ifndef JCX_RELAIS_CONFIG_CACHED_CLOCK_H
#define JCX_RELAIS_CONFIG_CACHED_CLOCK_H

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>

namespace jcailloux::relais::config {

/// CachedClock — background-refreshed steady_clock for hot paths.
///
/// A dedicated jthread updates the cached time every 100ms.
/// Reads are a single relaxed atomic load (~1ns, zero contention).
/// The atomic sits on its own cache line: the writer invalidates it,
/// but readers only read → no reader↔reader bouncing.
///
/// Stores uint32_t seconds since steady_clock epoch.
/// Precision: 1 second (sufficient for TTL checks with minute+ granularity).
/// Overflow: ~136 years from steady_clock epoch.
struct CachedClock {
    using Clock = std::chrono::steady_clock;

    /// Hot path: single mov from L1 cache, ~1ns.
    /// Returns seconds since steady_clock epoch as uint32_t.
    static uint32_t now() noexcept {
        return sec_.load(std::memory_order_relaxed);
    }

    /// Start the background refresh thread (idempotent via call_once).
    static void start() {
        std::call_once(start_flag_, [] {
            thread_ = std::jthread{[](std::stop_token st) {
                while (!st.stop_requested()) {
                    sec_.store(currentSec(), std::memory_order_relaxed);
                    std::this_thread::sleep_for(kInterval);
                }
            }};
        });
    }

    /// Ensure the background thread is running (call from init paths).
    static void ensureStarted() { start(); }

private:
    static constexpr auto kInterval = std::chrono::milliseconds{100};

    /// Convert steady_clock::now() to uint32_t seconds.
    static uint32_t currentSec() noexcept {
        return static_cast<uint32_t>(
            std::chrono::duration_cast<std::chrono::seconds>(
                Clock::now().time_since_epoch()).count());
    }

    // Own cache line: writer invalidates this line every 100ms,
    // but readers only read → no reader↔reader bouncing.
    alignas(64) static inline std::atomic<uint32_t> sec_{currentSec()};

    static inline std::jthread thread_;
    static inline std::once_flag start_flag_;
};

}  // namespace jcailloux::relais::config

#endif  // JCX_RELAIS_CONFIG_CACHED_CLOCK_H
