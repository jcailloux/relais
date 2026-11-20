#ifndef JCX_RELAIS_RUNTIME_THREAD_H
#define JCX_RELAIS_RUNTIME_THREAD_H

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>

#include "jcailloux/relais/runtime/CachedClock.h"

namespace jcailloux::relais::runtime::detail {

/// RuntimeThread — single background thread for periodic runtime tasks.
///
/// One jthread ticks every 100ms: it refreshes the cached clock, then runs the
/// tick hook, if one is set. It never asks the allocator anything.
/// New tasks: add a call in the loop body.
struct RuntimeThread {
    /// Tick interval in microseconds (constexpr, usable for normalization).
    static constexpr float kIntervalUs =
        static_cast<float>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::milliseconds{100}).count());

    using TickHook = void (*)() noexcept;

    /// Optional task run on each tick, after the clock. Set by GDSFPolicy, in
    /// a build with GDSF, to read the bytes the L1 holds for its budget.
    ///
    /// Atomic because it is written once by the GDSFPolicy singleton's
    /// constructor (which runs lazily, possibly after this thread has started)
    /// and read on every tick by the background thread. The release/acquire pair
    /// also publishes the GDSFPolicy state the hook touches: a reader that sees
    /// the non-null hook sees a fully-constructed policy.
    static inline std::atomic<TickHook> on_tick{nullptr};

    /// Ensure the background thread is running (idempotent via call_once).
    static void ensureStarted() {
        std::call_once(start_flag_, [] {
            thread_ = std::jthread{[](std::stop_token st) {
                while (!st.stop_requested()) {
                    CachedClock::tick();
                    if (auto hook = on_tick.load(std::memory_order_acquire))
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

}  // namespace jcailloux::relais::runtime::detail

#endif  // JCX_RELAIS_RUNTIME_THREAD_H