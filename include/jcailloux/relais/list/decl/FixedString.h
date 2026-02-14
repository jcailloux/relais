#ifndef CODIBOT_CACHE_LIST_DECL_FIXEDSTRING_H
#define CODIBOT_CACHE_LIST_DECL_FIXEDSTRING_H

#include <algorithm>
#include <cstddef>
#include <string_view>

namespace jcailloux::relais::cache::list::decl {

/// Compile-time string for use as Non-Type Template Parameter (NTTP)
template<size_t N>
struct FixedString {
    char data[N]{};

    constexpr FixedString() = default;

    constexpr FixedString(const char (&str)[N]) {
        std::copy_n(str, N, data);
    }

    [[nodiscard]] constexpr std::string_view view() const noexcept {
        return {data, N - 1};  // Exclude null terminator
    }

    [[nodiscard]] constexpr size_t size() const noexcept {
        return N - 1;
    }

    [[nodiscard]] constexpr const char* c_str() const noexcept {
        return data;
    }

    constexpr bool operator==(const FixedString&) const = default;

    template<size_t M>
    constexpr bool operator==(const FixedString<M>&) const noexcept {
        return false;  // Different sizes can't be equal
    }
};

// Deduction guide
template<size_t N>
FixedString(const char (&)[N]) -> FixedString<N>;

}  // namespace jcailloux::relais::cache::list::decl

#endif  // CODIBOT_CACHE_LIST_DECL_FIXEDSTRING_H
