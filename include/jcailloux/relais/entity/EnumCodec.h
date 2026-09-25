#ifndef JCX_RELAIS_ENTITY_ENUM_CODEC_H
#define JCX_RELAIS_ENTITY_ENUM_CODEC_H

#include <array>
#include <optional>
#include <string_view>
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
// =============================================================================

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
};

}  // namespace jcailloux::relais::entity

#endif  // JCX_RELAIS_ENTITY_ENUM_CODEC_H
