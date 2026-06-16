#ifndef JCX_RELAIS_LIST_SPEC_PARSE_UTILS_H
#define JCX_RELAIS_LIST_SPEC_PARSE_UTILS_H

#include <charconv>
#include <cstdint>
#include <optional>
#include <string_view>

namespace jcailloux::relais::list::spec::parse {

/// Fast int64_t parsing using std::from_chars
[[nodiscard]] inline int64_t toInt64(std::string_view str) noexcept {
    int64_t result = 0;
    std::from_chars(str.data(), str.data() + str.size(), result);
    return result;
}

/// Fast int parsing using std::from_chars
[[nodiscard]] inline int toInt(std::string_view str) noexcept {
    int result = 0;
    std::from_chars(str.data(), str.data() + str.size(), result);
    return result;
}

/// Parse a boolean from the standard HTTP / HTML-form query representations,
/// case-insensitively. Truthy: true/1/t/yes/y/on. Falsy: false/0/f/no/n/off.
/// Anything else -> nullopt (the filter stays inactive, mirroring the strict
/// reject discipline of integral IN elements). Covers the conventions emitted
/// by browsers (checkbox "on"), curl/forms ("1"/"0"), and JSON-ish clients
/// ("true"/"false").
[[nodiscard]] inline std::optional<bool> toBool(std::string_view str) noexcept {
    // Case-insensitive equality against an already-lowercase literal. Tokens are
    // <=5 chars, so this is cheaper than allocating a lowercased copy.
    constexpr auto ieq = [](std::string_view a, std::string_view lower) noexcept {
        if (a.size() != lower.size()) return false;
        for (size_t i = 0; i < a.size(); ++i) {
            char c = a[i];
            if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
            if (c != lower[i]) return false;
        }
        return true;
    };
    for (std::string_view t : {"true", "1", "t", "yes", "y", "on"})
        if (ieq(str, t)) return true;
    for (std::string_view t : {"false", "0", "f", "no", "n", "off"})
        if (ieq(str, t)) return false;
    return std::nullopt;
}

/// Maximum allowed string length for filter values (security)
inline constexpr size_t MAX_STRING_LEN = 256;

/// Check string length is within safe bounds
[[nodiscard]] inline bool isSafeLength(std::string_view str) noexcept {
    return str.size() <= MAX_STRING_LEN;
}

}  // namespace jcailloux::relais::list::spec::parse

#endif  // JCX_RELAIS_LIST_SPEC_PARSE_UTILS_H
