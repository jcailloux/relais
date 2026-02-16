#ifndef JCX_RELAIS_IO_CONTEXT_H
#define JCX_RELAIS_IO_CONTEXT_H

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

template<typename T>
concept IoContext = requires(
    T& ctx,
    int fd,
    IoEvent events,
    std::function<void(IoEvent)> io_cb,
    std::function<void()> cb,
    typename T::WatchHandle handle
) {
    { ctx.addWatch(fd, events, std::move(io_cb)) } -> std::same_as<typename T::WatchHandle>;
    { ctx.removeWatch(handle) } -> std::same_as<void>;
    { ctx.updateWatch(handle, events) } -> std::same_as<void>;
    { ctx.post(std::move(cb)) } -> std::same_as<void>;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_CONTEXT_H
