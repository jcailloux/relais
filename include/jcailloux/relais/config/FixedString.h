#ifndef JCX_RELAIS_FIXEDSTRING_H
#define JCX_RELAIS_FIXEDSTRING_H

#include <algorithm>
#include <cstddef>

namespace jcailloux::relais::config {

/// Structural string wrapper for use as NTTP (Non-Type Template Parameter).
/// Allows passing string literals directly as template arguments:
///   template<FixedString Name> struct Foo {};
///   Foo<"hello"> f;
template<size_t N>
struct FixedString {
    char value[N]{};

    constexpr FixedString(const char (&str)[N]) {
        std::copy_n(str, N, value);
    }

    constexpr operator const char*() const { return value; }

    constexpr auto operator<=>(const FixedString&) const = default;
};

}  // namespace jcailloux::relais::config

#endif //JCX_RELAIS_FIXEDSTRING_H
