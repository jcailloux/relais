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

// IoContext — the event loop relais runs its I/O on.
//
// EpollIoContext is the bundled one. Any loop that offers fd watches, posted
// callbacks and one-shot timers can be adapted to it: trantor (Channel,
// queueInLoop, runAfter), libuv (uv_poll, uv_async, uv_timer), asio
// (async_wait, post, steady_timer). The concept fixes the signatures; the
// contract below fixes their meaning, and testing/IoContextConformance.h checks
// it (runAll).
//
// What relais guarantees the adapter:
//   - every call but post() comes from the loop thread: the one that runs the
//     loop, or, after attachHere() before the loop starts, the one that will
//     (postDelayed alone, until the loop's first turn);
//   - addWatch and removeWatch are never called inside an event dispatch (a
//     watch or timer callback): relais makes them from tasks it posts;
//   - a watched fd stays open, on the same file, until its removeWatch;
//   - postDelayed is rare: once per batch of reads at most, never per request;
//   - no timer is ever cancelled.
//
// What the adapter must do:
//   - addWatch(fd, mask, cb): call cb on the loop thread, with the IoEvent bits
//     of what is ready among `mask`, for as long as the watch lasts; may throw
//     if the fd cannot be watched. One event split into several calls (Read,
//     then Write) and calls for nothing are tolerated: relais reads what is
//     there and waits again. A failed fd may be reported as Error, which relais
//     takes as Read.
//   - updateWatch(handle, mask): watch for `mask` from then on. relais may call
//     it from a watch callback, that watch's own included.
//   - removeWatch(handle): no call for that handle once it returns; the watch,
//     its callback included, may be freed right away. A later addWatch of the
//     same fd number is a new watch, which the old one must not affect.
//   - post(cb): run cb once on the loop thread, in posting order, never before
//     the callback that posted it has returned; callable from any thread, and
//     wakes the loop if it is waiting.
//   - postDelayed(delay, cb): run cb once on the loop thread, `delay` from now
//     or later. What it returns is ignored.
//
// Optional:
//   - isInLoopThread(): true on the loop thread. attach() then refuses that
//     thread, which it would wait on forever.
//   - pinThread(std::thread::id): called by attachHere() on the loop's first
//     turn, with the loop thread: relais's callbacks run there from then on,
//     which the adapter may check.
//   - detached(): called on the loop thread once relais has left the loop
//     (detach) and nothing of it is still running there: the postDelayed
//     callbacks still pending would do nothing, and the adapter may drop them,
//     for a loop that runs until it has no work left.
template<typename T>
concept IoContext = requires(
    T& ctx,
    int fd,
    IoEvent events,
    std::function<void(IoEvent)> io_cb,
    std::function<void()> cb,
    typename T::WatchHandle handle,
    std::chrono::nanoseconds delay
) {
    { ctx.addWatch(fd, events, std::move(io_cb)) } -> std::same_as<typename T::WatchHandle>;
    { ctx.removeWatch(handle) } -> std::same_as<void>;
    { ctx.updateWatch(handle, events) } -> std::same_as<void>;
    { ctx.post(std::move(cb)) } -> std::same_as<void>;
    ctx.postDelayed(delay, std::move(cb));
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_CONTEXT_H
