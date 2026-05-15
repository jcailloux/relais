#ifndef JCX_RELAIS_FIXEDSTRING_H
#define JCX_RELAIS_FIXEDSTRING_H

#include <algorithm>
#include <cstddef>
#include <string_view>

namespace jcailloux::relais::config {

/// Structural string wrapper for use as NTTP (Non-Type Template Parameter).
/// Allows passing string literals directly as template arguments:
///   template<FixedString Name> struct Foo {};
///   Foo<"hello"> f;
template<size_t N>
struct FixedString {
    char value[N]{};

    constexpr FixedString() = default;

    constexpr FixedString(const char (&str)[N]) {
        std::copy_n(str, N, value);
    }

    [[nodiscard]] constexpr std::string_view view() const noexcept {
        return {value, N - 1};  // Exclude null terminator
    }

    [[nodiscard]] constexpr size_t size() const noexcept {
        return N - 1;
    }

    [[nodiscard]] constexpr const char* c_str() const noexcept {
        return value;
    }

    constexpr operator const char*() const { return value; }

    constexpr bool operator==(const FixedString&) const = default;

    template<size_t M>
    constexpr bool operator==(const FixedString<M>&) const noexcept {
        return false;  // Different sizes can't be equal
    }

    constexpr auto operator<=>(const FixedString&) const = default;
};

// Deduction guide
template<size_t N>
FixedString(const char (&)[N]) -> FixedString<N>;

}  // namespace jcailloux::relais::config

#endif //JCX_RELAIS_FIXEDSTRING_H
