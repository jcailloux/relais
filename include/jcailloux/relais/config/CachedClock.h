#ifndef JCX_RELAIS_CONFIG_CACHED_CLOCK_H
#define JCX_RELAIS_CONFIG_CACHED_CLOCK_H

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>

namespace jcailloux::relais::config {

/// CachedClock — background-refreshed steady_clock for hot paths.
///
/// A dedicated jthread updates the cached time point every 100ms.
/// Reads are a single relaxed atomic load (~1ns, zero contention).
/// The atomic sits on its own cache line: the writer invalidates it,
/// but readers only read → no reader↔reader bouncing.
///
/// For 1-hour TTL checks, 1ms precision is more than sufficient.
/// Write paths that need precise timing (e.g., GDSF construction
/// cost measurement) should use Clock::now() directly.
struct CachedClock {
    using Clock      = std::chrono::steady_clock;
    using time_point = Clock::time_point;
    using duration   = Clock::duration;
    using rep        = duration::rep;

    /// Hot path: single mov from L1 cache, ~1ns.
    static time_point now() noexcept {
        return time_point{duration{rep_.load(std::memory_order_relaxed)}};
    }

    /// Start the background refresh thread (idempotent via call_once).
    static void start() {
        std::call_once(start_flag_, [] {
            thread_ = std::jthread{[](std::stop_token st) {
                while (!st.stop_requested()) {
                    rep_.store(Clock::now().time_since_epoch().count(),
                               std::memory_order_relaxed);
                    std::this_thread::sleep_for(kInterval);
                }
            }};
        });
    }

    /// Stop the background thread (for clean shutdown).
    static void stop() { thread_ = {}; }

    /// Ensure the background thread is running (call from init paths).
    static void ensureStarted() { start(); }

private:
    static constexpr auto kInterval = std::chrono::milliseconds{100};

    // Own cache line: writer invalidates this line every 100ms,
    // but readers only read → no reader↔reader bouncing.
    alignas(64) static inline std::atomic<rep> rep_{
        Clock::now().time_since_epoch().count()};

    static inline std::jthread thread_;
    static inline std::once_flag start_flag_;
};

}  // namespace jcailloux::relais::config

#endif  // JCX_RELAIS_CONFIG_CACHED_CLOCK_H
