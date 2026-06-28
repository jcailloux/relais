#ifndef JCX_RELAIS_RUNTIME_THREAD_H
#define JCX_RELAIS_RUNTIME_THREAD_H

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>

#include "jcailloux/relais/runtime/CachedClock.h"
#include "jcailloux/relais/runtime/CachedMemory.h"

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

    using HeapRefreshHook = void (*)() noexcept;

    /// Optional callback invoked after each CachedMemory::tick().
    /// Set by GDSFPolicy to reset its admitted counter on heap refresh.
    ///
    /// Atomic because it is written once by the GDSFPolicy singleton's
    /// constructor (which runs lazily, possibly after this thread has started)
    /// and read on every tick by the background thread. The release/acquire pair
    /// also publishes the GDSFPolicy state the hook touches: a reader that sees
    /// the non-null hook sees a fully-constructed policy.
    static inline std::atomic<HeapRefreshHook> on_heap_refresh{nullptr};

    /// Ensure the background thread is running (idempotent via call_once).
    static void ensureStarted() {
        std::call_once(start_flag_, [] {
            thread_ = std::jthread{[](std::stop_token st) {
                while (!st.stop_requested()) {
                    CachedClock::tick();
                    CachedMemory::tick();
                    if (auto hook = on_heap_refresh.load(std::memory_order_acquire))
                        hook();
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