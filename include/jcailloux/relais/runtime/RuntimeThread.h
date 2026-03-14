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

    /// Ensure the background thread is running (idempotent via call_once).
    static void ensureStarted() {
        std::call_once(start_flag_, [] {
            thread_ = std::jthread{[](std::stop_token st) {
                while (!st.stop_requested()) {
                    CachedClock::tick();
                    CachedHeap::tick();
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