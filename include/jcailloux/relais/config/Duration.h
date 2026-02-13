#ifndef JCX_DROGON_DURATION_H
#define JCX_DROGON_DURATION_H

#include <chrono>
#include <cstdint>

namespace jcailloux::relais::config {

/// Structural duration wrapper for use as NTTP field.
/// std::chrono::duration has private members and is not structural,
/// so it cannot appear in a template parameter aggregate.
///
/// Usage:
///   constexpr CacheConfig cfg{ .l1_ttl = 30min, .l2_ttl = 4h };
///   auto ns = std::chrono::nanoseconds(cfg.l1_ttl);  // explicit conversion
struct Duration {
    int64_t ns = 0;

    constexpr Duration() = default;

    /// Implicit conversion from any std::chrono::duration.
    template<typename Rep, typename Period>
    constexpr Duration(std::chrono::duration<Rep, Period> d)
        : ns(std::chrono::duration_cast<std::chrono::nanoseconds>(d).count()) {}

    /// Explicit conversion to std::chrono::nanoseconds.
    constexpr explicit operator std::chrono::nanoseconds() const {
        return std::chrono::nanoseconds{ns};
    }

    /// Convenience conversion to any duration type.
    template<typename Duration_t>
    constexpr Duration_t as() const {
        return std::chrono::duration_cast<Duration_t>(std::chrono::nanoseconds{ns});
    }

    constexpr auto operator<=>(const Duration&) const = default;
};

}  // namespace jcailloux::relais::config

#endif //JCX_DROGON_DURATION_H
