#ifndef JCX_RELAIS_IO_CONTEXT_H
#define JCX_RELAIS_IO_CONTEXT_H

#include <chrono>
#include <concepts>
#include <cstdint>
#include <functional>

namespace jcailloux::relais::io {

enum class IoEvent : uint8_t {
    None    = 0,
    Read    = 1 << 0,
    Write   = 1 << 1,
    Error   = 1 << 2,
};

[[nodiscard]] constexpr IoEvent operator|(IoEvent a, IoEvent b) noexcept {
    return static_cast<IoEvent>(
        static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
}

[[nodiscard]] constexpr IoEvent operator&(IoEvent a, IoEvent b) noexcept {
    return static_cast<IoEvent>(
        static_cast<uint8_t>(a) & static_cast<uint8_t>(b));
}

constexpr IoEvent& operator|=(IoEvent& a, IoEvent b) noexcept {
    return a = a | b;
}

[[nodiscard]] constexpr bool hasEvent(IoEvent set, IoEvent flag) noexcept {
    return (set & flag) != IoEvent::None;
}

// IoContext — the event-loop extension point relais binds I/O onto.
//
// EpollIoContext is the bundled model, but PgPool/RedisPool and the awaiter
// machinery are generic over this concept: any epoll-family loop (e.g. a
// trantor::EventLoop shim to run relais inline on Drogon's threads) can satisfy
// it. The concept fixes only the signatures below; the *semantic* contract an
// adapter must honor is:
//
//   - addWatch(fd, mask, cb): cb runs ON THE LOOP THREAD whenever fd is ready
//     for an event in `mask`, with the matching IoEvent bits set. Returns a
//     handle for later update/remove.
//   - updateWatch(handle, mask): changes the active mask on that handle.
//   - removeWatch(handle): no further callbacks for that handle.
//   - post(cb): runs cb exactly once, on the loop thread, FIFO with other posts;
//     thread-safe AND wakes a blocked loop promptly when called from another
//     thread. The loop's wait must be bounded so posts cannot stall.
//   - postDelayed(delay, cb): runs cb once on the loop thread after `delay`;
//     returns a TimerToken. Thread-safe. (Used by BatchScheduler to flush a
//     batch after an adaptive deadline.)
//   - cancelTimer(token): cancels a pending postDelayed; no-op if it already
//     fired or the token is unknown. (Used to cancel the flush timer when a
//     batch departs early because it filled up.)
//
// These rules are encoded as runnable checks in
// testing/IoContextConformance.h — instantiate the harness against any adapter
// to verify it before wiring relais pools onto it.
template<typename T>
concept IoContext = requires(
    T& ctx,
    int fd,
    IoEvent events,
    std::function<void(IoEvent)> io_cb,
    std::function<void()> cb,
    typename T::WatchHandle handle,
    typename T::TimerToken token,
    std::chrono::nanoseconds delay
) {
    { ctx.addWatch(fd, events, std::move(io_cb)) } -> std::same_as<typename T::WatchHandle>;
    { ctx.removeWatch(handle) } -> std::same_as<void>;
    { ctx.updateWatch(handle, events) } -> std::same_as<void>;
    { ctx.post(std::move(cb)) } -> std::same_as<void>;
    { ctx.postDelayed(delay, std::move(cb)) } -> std::same_as<typename T::TimerToken>;
    { ctx.cancelTimer(token) } -> std::same_as<void>;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_CONTEXT_H
