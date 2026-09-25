#ifndef JCX_RELAIS_ENTITY_ENUM_CODEC_H
#define JCX_RELAIS_ENTITY_ENUM_CODEC_H

#include <array>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace jcailloux::relais::entity {

// =============================================================================
// Mapped enum codec: C++ enumerator <-> database string
//
// The generator emits one codec per enum field stored as text. The derived
// struct only declares `pairs`, the single source of the mapping; every
// conversion is derived from it:
//
//   struct StateCodec : MappedEnumCodec<StateCodec, State> {
//       static constexpr std::array<std::pair<enum_type, std::string_view>, 2>
//           pairs{{{enum_type::Open, "open"}, {enum_type::Closed, "closed"}}};
//   };
//
// Order follows the underlying value, as `operator<` does on the enum. SQL
// compares the rank expression `CASE "col" WHEN 'open' THEN 5 ... END`, built
// at compile time from `pairs`; a value outside the mapping, or NULL, ranks
// NULL. An index serves ordering only when it is built on that same
// expression.
// =============================================================================

namespace detail {

template<typename T>
constexpr std::size_t decimalWidth(T v) noexcept {
    std::size_t n = 1;
    if constexpr (std::is_signed_v<T>) n += v < 0;
    for (v /= 10; v != 0; v /= 10) ++n;
    return n;
}

/// Writes `v` in decimal ending just before `end`; returns the new end.
template<typename T>
constexpr char* writeDecimal(char* out, T v) noexcept {
    char* end = out + decimalWidth(v);
    char* p = end;
    do {
        const int d = static_cast<int>(v % 10);
        *--p = static_cast<char>('0' + (d < 0 ? -d : d));
        v /= 10;
    } while (v != 0);
    if (p != out) *--p = '-';
    return end;
}

inline constexpr std::string_view kRankWhen = " WHEN '";
inline constexpr std::string_view kRankThen = "' THEN ";
inline constexpr std::string_view kRankEnd = " END";

template<typename Codec>
consteval std::size_t rankCasesSize() {
    std::size_t n = kRankEnd.size();
    for (const auto& [e, s] : Codec::pairs) {
        n += kRankWhen.size() + kRankThen.size() + decimalWidth(std::to_underlying(e));
        for (char c : s) n += c == '\'' ? 2 : 1;
    }
    return n;
}

/// ` WHEN 'db' THEN u ... END`, quotes doubled.
template<typename Codec>
inline constexpr auto rank_cases = [] {
    std::array<char, rankCasesSize<Codec>()> out{};
    char* p = out.data();
    auto put = [&](std::string_view s) { for (char c : s) *p++ = c; };
    for (const auto& [e, s] : Codec::pairs) {
        put(kRankWhen);
        for (char c : s) {
            if (c == '\'') *p++ = '\'';
            *p++ = c;
        }
        put(kRankThen);
        p = writeDecimal(p, std::to_underlying(e));
    }
    put(kRankEnd);
    return out;
}();

}  // namespace detail

template<typename Derived, typename E>
struct MappedEnumCodec {
    using enum_type = E;

    /// Database string of an enumerator; empty when the mapping omits it.
    static constexpr std::string_view toDb(enum_type v) noexcept {
        for (const auto& [e, s] : Derived::pairs)
            if (e == v) return s;
        return {};
    }

    /// Enumerator stored as `s`; nullopt when no pair matches.
    static constexpr std::optional<enum_type> fromDb(std::string_view s) noexcept {
        for (const auto& [e, db] : Derived::pairs)
            if (db == s) return e;
        return std::nullopt;
    }

    /// Tail of the rank expression, after `CASE "col"`.
    static constexpr std::string_view rankCases() noexcept {
        const auto& cases = detail::rank_cases<Derived>;
        return {cases.data(), cases.size()};
    }

    /// Appends the rank expression of `column`, already quoted and qualified.
    static void appendRank(std::string& sql, std::string_view qual, std::string_view column) {
        sql += "CASE ";
        sql += qual;
        sql += column;
        sql += rankCases();
    }
};

}  // namespace jcailloux::relais::entity

#endif  // JCX_RELAIS_ENTITY_ENUM_CODEC_H
