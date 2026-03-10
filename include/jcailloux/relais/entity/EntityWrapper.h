#ifndef JCX_RELAIS_ENTITY_ENTITY_WRAPPER_H
#define JCX_RELAIS_ENTITY_ENTITY_WRAPPER_H

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <glaze/glaze.hpp>

#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/entity/Format.h"

namespace jcailloux::relais::entity {

/// Heap capacity of a std::string, excluding SSO buffer (already in sizeof).
/// Portable: checks if data() points outside the string object.
inline size_t heapCapacity(const std::string& s) {
    const char* p = s.data();
    const char* obj = reinterpret_cast<const char*>(&s);
    return (p < obj || p >= obj + sizeof(std::string)) ? s.capacity() : 0;
}

// =============================================================================
// EntityWrapper<Struct, Mapping> — API-side wrapper for pure data structs
//
// Inherits from Struct (pure declarative data) and adds:
// - On-demand BEVE/JSON serialization via Glaze (no caching)
// - SQL row mapping (fromRow/toInsertParams) delegated to Mapping
// - Primary key access delegated to Mapping
//
// Struct is framework-agnostic and can be shared across projects.
// Mapping is generated and contains SQL column definitions.
//
// Satisfies: HasBinarySerialization, HasJsonSerialization, HasFormat
// =============================================================================

template<typename Struct, typename Mapping>
class EntityWrapper : public Struct {
public:
    using Format = jcailloux::relais::StructFormat;
    using TraitsType = typename Mapping::TraitsType;
    using Field = typename TraitsType::Field;
    static constexpr bool read_only = Mapping::read_only;

    EntityWrapper() = default;
    explicit EntityWrapper(const Struct& s) : Struct(s) {}
    explicit EntityWrapper(Struct&& s) noexcept : Struct(std::move(s)) {}

    ~EntityWrapper() = default;

    // No serialization caches — copies/moves are trivial (Struct-only).
    EntityWrapper(const EntityWrapper& o) : Struct(static_cast<const Struct&>(o)) {}
    EntityWrapper(EntityWrapper&& o) noexcept : Struct(static_cast<Struct&&>(std::move(o))) {}
    EntityWrapper& operator=(const EntityWrapper& o) {
        if (this != &o) Struct::operator=(static_cast<const Struct&>(o));
        return *this;
    }
    EntityWrapper& operator=(EntityWrapper&& o) noexcept {
        if (this != &o) Struct::operator=(static_cast<Struct&&>(std::move(o)));
        return *this;
    }

    // =========================================================================
    // Primary key — delegated to Mapping
    // =========================================================================

    [[nodiscard]] auto key() const noexcept {
        return Mapping::key(*this);
    }

    // =========================================================================
    // SQL row mapping — delegated to Mapping
    // =========================================================================

    static std::optional<EntityWrapper> fromRow(const io::PgResult::Row& row) {
        return Mapping::template fromRow<EntityWrapper>(row);
    }

    static io::PgParams toInsertParams(const EntityWrapper& e) {
        return Mapping::template toInsertParams<EntityWrapper>(e);
    }

    static io::PgParams toUpdateParams(const EntityWrapper& e)
        requires requires { Mapping::template toUpdateParams<EntityWrapper>(e); }
    {
        return Mapping::template toUpdateParams<EntityWrapper>(e);
    }

    // =========================================================================
    // Mapping type — exposed for concept detection
    // =========================================================================

    using MappingType = Mapping;

    /// Approximate heap memory used by this entity (struct + dynamic fields).
    [[nodiscard]] size_t memoryUsage() const {
        size_t size = sizeof(*this);
        if constexpr (requires(const Struct& s) { Mapping::dynamicSize(s); }) {
            size += Mapping::dynamicSize(static_cast<const Struct&>(*this));
        }
        return size;
    }

    // =========================================================================
    // Binary serialization (Glaze BEVE, on-demand)
    // =========================================================================

    [[nodiscard]] std::vector<uint8_t> binary() const {
        std::vector<uint8_t> buf;
        if (glz::write_beve(*this, buf)) buf.clear();
        return buf;
    }

    static std::optional<EntityWrapper> fromBinary(std::span<const uint8_t> data) {
        if (data.empty()) return std::nullopt;
        EntityWrapper entity;
        if (glz::read_beve(entity, std::string_view{
            reinterpret_cast<const char*>(data.data()), data.size()}))
            return std::nullopt;
        return entity;
    }

    // =========================================================================
    // JSON serialization (Glaze JSON, on-demand)
    // =========================================================================

    [[nodiscard]] std::string json() const {
        std::string buf;
        buf.reserve(256);
        if (glz::write_json(*this, buf)) buf = "{}";
        return buf;
    }

    static std::optional<EntityWrapper> fromJson(std::string_view json) {
        if (json.empty()) return std::nullopt;
        EntityWrapper entity;
        if (glz::read_json(entity, json)) return std::nullopt;
        return entity;
    }

};

}  // namespace jcailloux::relais::entity

// =============================================================================
// Glaze metadata: prefer glz::meta<Struct> when available, else use Mapping
//
// If the shared struct header defines a glz::meta<Struct> specialization
// (e.g., with custom JSON field names), EntityWrapper automatically uses it.
// This ensures both the API (via EntityWrapper) and BEVE consumers (via the
// raw struct) share the same field naming contract.
//
// If no glz::meta<Struct> exists, falls back to Mapping::glaze_value (which
// uses C++ member names as JSON keys — the default for generated entities).
// =============================================================================

template<typename Struct, typename Mapping>
struct glz::meta<jcailloux::relais::entity::EntityWrapper<Struct, Mapping>> {
    using T = jcailloux::relais::entity::EntityWrapper<Struct, Mapping>;
    static constexpr auto value = [] {
        if constexpr (requires { glz::meta<Struct>::value; }) {
            return glz::meta<Struct>::value;
        } else {
            return Mapping::template glaze_value<T>;
        }
    }();
};

#endif  // JCX_RELAIS_ENTITY_ENTITY_WRAPPER_H