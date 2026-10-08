#ifndef JCX_RELAIS_DETAIL_NUMBER_TEXT_H
#define JCX_RELAIS_DETAIL_NUMBER_TEXT_H

#include <algorithm>
#include <charconv>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>

namespace jcailloux::relais::detail {

/// Character types are arithmetic but denote text, not numbers: `char`,
/// `wchar_t` and the `charN_t` family. `signed char` / `unsigned char` (hence
/// `int8_t` / `uint8_t`) are small integers and stay numbers.
template<typename T>
inline constexpr bool is_character_v =
    std::same_as<T, char> || std::same_as<T, wchar_t> || std::same_as<T, char8_t>
    || std::same_as<T, char16_t> || std::same_as<T, char32_t>;

/// A type written and read as a decimal number: every arithmetic type except
/// `bool` (which has its own `t`/`f` text form) and the character types.
template<typename T>
concept TextNumber = std::is_arithmetic_v<std::remove_cv_t<T>>
                     && !std::same_as<std::remove_cv_t<T>, bool>
                     && !is_character_v<std::remove_cv_t<T>>;

namespace number_text {

constexpr std::size_t decimalDigits(unsigned long long v) noexcept {
    std::size_t n = 1;
    while (v >= 10) {
        v /= 10;
        ++n;
    }
    return n;
}

template<TextNumber T>
constexpr std::size_t maxChars() noexcept {
    using L = std::numeric_limits<T>;
    if constexpr (std::is_integral_v<T>) {
        return static_cast<std::size_t>(L::digits10) + 1 + (L::is_signed ? 1 : 0);
    } else {
        // Shortest round-trip form is never longer than its scientific form:
        // sign, max_digits10 significant digits, '.', 'e', exponent sign and
        // digits. The smallest subnormal has the largest exponent magnitude,
        // bounded by -min_exponent10 + max_digits10.
        const std::size_t exponent = decimalDigits(
            static_cast<unsigned long long>(-L::min_exponent10 + L::max_digits10));
        const std::size_t scientific =
            1 + static_cast<std::size_t>(L::max_digits10) + 1 + 1 + 1 + exponent;
        return std::max<std::size_t>(scientific, sizeof("-Infinity") - 1);
    }
}

}  // namespace number_text

/// Upper bound on the text length of any value of `T` produced by
/// `appendNumber`. Exact for the standard integer types, `float` (15) and
/// `double` (24, e.g. "-2.2250738585072014e-308").
template<TextNumber T>
inline constexpr std::size_t kMaxNumberChars = number_text::maxChars<std::remove_cv_t<T>>();

/// Append the decimal text of `v` to `out`, independent of the C locale.
///
/// Integers are written exactly. Floating-point values use the shortest text
/// that parses back to the same value (`std::to_chars` with neither format nor
/// precision), so a value survives a write/read round-trip bit for bit. The
/// non-finite values use PostgreSQL's spelling: `NaN` (whatever the sign bit),
/// `Infinity`, `-Infinity`.
template<TextNumber T>
void appendNumber(std::string& out, T v) {
    if constexpr (std::is_floating_point_v<T>) {
        if (!std::isfinite(v)) [[unlikely]] {
            out += std::isnan(v) ? "NaN" : (v < 0 ? "-Infinity" : "Infinity");
            return;
        }
    }
    char buf[kMaxNumberChars<T>];
    // Cannot fail: buf holds the longest representation of T.
    const auto res = std::to_chars(buf, buf + sizeof(buf), v);
    out.append(buf, res.ptr);
}

/// `appendNumber` into a fresh string.
template<TextNumber T>
[[nodiscard]] std::string toText(T v) {
    std::string out;
    appendNumber(out, v);
    return out;
}

/// Parse `text` as a `T`, strictly: the whole token must be consumed and the
/// value must fit in `T`, otherwise `nullopt`. No leading whitespace or `+`.
/// Floating-point types also accept `NaN`, `Infinity` and `-Infinity`
/// (case-insensitive), so every `appendNumber` output parses back.
template<TextNumber T>
[[nodiscard]] std::optional<T> parseNumber(std::string_view text) noexcept {
    T v{};
    const char* last = text.data() + text.size();
    const auto res = std::from_chars(text.data(), last, v);
    if (res.ec != std::errc{} || res.ptr != last) return std::nullopt;
    return v;
}

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_DETAIL_NUMBER_TEXT_H
