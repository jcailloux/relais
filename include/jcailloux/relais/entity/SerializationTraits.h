#ifndef JCX_RELAIS_ENTITY_SERIALIZATION_TRAITS_H
#define JCX_RELAIS_ENTITY_SERIALIZATION_TRAITS_H

#include <concepts>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace jcailloux::relais {

// =============================================================================
// Serialization Capability Concepts
//
// These check what an entity CAN do (capability), not how it's configured.
// =============================================================================

/// E supports JSON serialization (json/fromJson).
/// json() returns std::string by value (on-demand serialization).
/// fromJson(string_view) is the canonical input form.
template<typename E>
concept HasJsonSerialization = requires(const E& e, std::string_view json) {
    { e.json() } -> std::convertible_to<std::string>;
    { E::fromJson(json) } -> std::convertible_to<std::optional<E>>;
};

/// E supports binary serialization (binary/fromBinary).
/// binary() returns std::vector<uint8_t> by value (on-demand serialization).
/// fromBinary(span) is the canonical input form.
template<typename E>
concept HasBinarySerialization = requires(const E& e, std::span<const uint8_t> data) {
    { e.binary() } -> std::convertible_to<std::vector<uint8_t>>;
    { E::fromBinary(data) } -> std::convertible_to<std::optional<E>>;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_ENTITY_SERIALIZATION_TRAITS_H
