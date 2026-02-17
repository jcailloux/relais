#ifndef JCX_RELAIS_PARSE_UTILS_H
#define JCX_RELAIS_PARSE_UTILS_H

#include <charconv>
#include <cstdint>
#include <string_view>

namespace jcailloux::relais::cache::parse {

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

/// Clamp limit to [1, 100]
[[nodiscard]] inline int clampLimit(int value) noexcept {
    return value < 1 ? 1 : (value > 100 ? 100 : value);
}

/// Maximum allowed string length for filter values (security)
inline constexpr size_t MAX_STRING_LEN = 256;

/// Check string length is within safe bounds
[[nodiscard]] inline bool isSafeLength(std::string_view str) noexcept {
    return str.size() <= MAX_STRING_LEN;
}

}  // namespace jcailloux::relais::cache::parse

#endif  // JCX_RELAIS_PARSE_UTILS_H
