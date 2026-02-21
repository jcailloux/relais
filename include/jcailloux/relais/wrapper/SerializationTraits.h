#ifndef JCX_RELAIS_WRAPPER_SERIALIZATION_TRAITS_H
#define JCX_RELAIS_WRAPPER_SERIALIZATION_TRAITS_H

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

/// Entity supports JSON serialization (json/fromJson).
/// json() returns const string* — epoch-guarded, nullable, zero-copy.
/// fromJson(string_view) is the canonical input form.
template<typename Entity>
concept HasJsonSerialization = requires(const Entity& e, std::string_view json) {
    { e.json() } -> std::convertible_to<const std::string*>;
    { Entity::fromJson(json) } -> std::convertible_to<std::optional<Entity>>;
};

/// Entity supports binary serialization (binary/fromBinary).
/// binary() returns const vector<uint8_t>* — epoch-guarded, nullable, zero-copy.
/// fromBinary(span) is the canonical input form.
template<typename Entity>
concept HasBinarySerialization = requires(const Entity& e, std::span<const uint8_t> data) {
    { e.binary() } -> std::convertible_to<const std::vector<uint8_t>*>;
    { Entity::fromBinary(data) } -> std::convertible_to<std::optional<Entity>>;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_WRAPPER_SERIALIZATION_TRAITS_H