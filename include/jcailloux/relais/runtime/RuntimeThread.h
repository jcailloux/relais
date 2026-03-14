#ifndef JCX_RELAIS_RUNTIME_THREAD_H
#define JCX_RELAIS_RUNTIME_THREAD_H

#include <chrono>
#include <mutex>
#include <thread>

#include "jcailloux/relais/runtime/CachedClock.h"
#include "jcailloux/relais/runtime/CachedHeap.h"

namespace jcailloux::relais::runtime {

/// RuntimeThread — single background thread for periodic runtime tasks.
///
/// One jthread ticks every 100ms, calling each task directly.
/// New tasks: add a call in the loop body.
struct RuntimeThread {
    /// Tick interval in microseconds (constexpr, usable for normalization).
    static constexpr float kIntervalUs =
        static_cast<float>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::milliseconds{100}).count());

    /// Optional callback invoked after each CachedHeap::tick().
    /// Set by GDSFPolicy to reset its admitted counter on heap refresh.
    static inline void (*on_heap_refresh)() noexcept = nullptr;

    /// Ensure the background thread is running (idempotent via call_once).
    static void ensureStarted() {
        std::call_once(start_flag_, [] {
            thread_ = std::jthread{[](std::stop_token st) {
                while (!st.stop_requested()) {
                    CachedClock::tick();
                    CachedHeap::tick();
                    if (on_heap_refresh) on_heap_refresh();
                    std::this_thread::sleep_for(kInterval);
                }
            }};
        });
    }

private:
    static constexpr auto kInterval = std::chrono::milliseconds{100};

    static inline std::jthread thread_;
    static inline std::once_flag start_flag_;
};

}  // namespace jcailloux::relais::runtime

#endif  // JCX_RELAIS_RUNTIME_THREAD_H