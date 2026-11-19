#ifndef JCX_RELAIS_ENTITY_H
#define JCX_RELAIS_ENTITY_H

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
#include "jcailloux/relais/HeapBytes.h"

namespace jcailloux::relais {

// =============================================================================
// Entity<Struct, Mapping> — ORM entity inheriting from pure data struct
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
class Entity : public Struct {
public:
    using Format = jcailloux::relais::StructFormat;
    using TraitsType = typename Mapping::TraitsType;
    using Field = typename TraitsType::Field;
    static constexpr bool read_only = Mapping::read_only;

    Entity() = default;
    explicit Entity(const Struct& s) : Struct(s) {}
    explicit Entity(Struct&& s) noexcept : Struct(std::move(s)) {}

    ~Entity() = default;

    // No serialization caches — copies/moves are trivial (Struct-only).
    Entity(const Entity& o) : Struct(static_cast<const Struct&>(o)) {}
    Entity(Entity&& o) noexcept : Struct(static_cast<Struct&&>(std::move(o))) {}
    Entity& operator=(const Entity& o) {
        if (this != &o) Struct::operator=(static_cast<const Struct&>(o));
        return *this;
    }
    Entity& operator=(Entity&& o) noexcept {
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

    static std::optional<Entity> fromRow(const io::PgResult::Row& row) {
        return Mapping::template fromRow<Entity>(row);
    }

    static io::PgParams toInsertParams(const Entity& e) {
        return Mapping::template toInsertParams<Entity>(e);
    }

    static io::PgParams toUpdateParams(const Entity& e)
        requires requires { Mapping::template toUpdateParams<Entity>(e); }
    {
        return Mapping::template toUpdateParams<Entity>(e);
    }

    // =========================================================================
    // Mapping type — exposed for concept detection
    // =========================================================================

    using MappingType = Mapping;

    /// The bytes this entity holds: the object, plus the room its members'
    /// heap blocks take at the allocator. Generated mappings count member by
    /// member; a hand-written one without heapBytes is counted through glaze's
    /// reflection of the struct.
    [[nodiscard]] size_t memoryUsage() const {
        const auto& s = static_cast<const Struct&>(*this);
        if constexpr (requires { Mapping::heapBytes(s); }) {
            return sizeof(*this) + Mapping::heapBytes(s);
        } else {
            return sizeof(*this) + ::jcailloux::relais::detail::heapBytes(s);
        }
    }

    // =========================================================================
    // Binary serialization (Glaze BEVE, on-demand)
    // =========================================================================
    //
    // On-demand serialization (allocates each call). For cached serialization,
    // use Repo::findJson() / Repo::findBinary() which leverage the L1 cache.

    [[nodiscard]] std::vector<uint8_t> binary() const {
        std::vector<uint8_t> buf;
        if (glz::write_beve(*this, buf)) buf.clear();
        return buf;
    }

    static std::optional<Entity> fromBinary(std::span<const uint8_t> data) {
        if (data.empty()) return std::nullopt;
        Entity entity;
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

    static std::optional<Entity> fromJson(std::string_view json) {
        if (json.empty()) return std::nullopt;
        Entity entity;
        if (glz::read_json(entity, json)) return std::nullopt;
        return entity;
    }

};

}  // namespace jcailloux::relais

// =============================================================================
// Glaze metadata: prefer glz::meta<Struct> when available, else use Mapping
//
// If the shared struct header defines a glz::meta<Struct> specialization
// (e.g., with custom JSON field names), Entity automatically uses it.
// This ensures both the API (via Entity) and BEVE consumers (via the
// raw struct) share the same field naming contract.
//
// If no glz::meta<Struct> exists, falls back to Mapping::glaze_value (which
// uses C++ member names as JSON keys — the default for generated entities).
// =============================================================================

template<typename Struct, typename Mapping>
struct glz::meta<jcailloux::relais::Entity<Struct, Mapping>> {
    using T = jcailloux::relais::Entity<Struct, Mapping>;
    static constexpr auto value = [] {
        if constexpr (requires { glz::meta<Struct>::value; }) {
            return glz::meta<Struct>::value;
        } else {
            return Mapping::template glaze_value<T>;
        }
    }();
};

#endif  // JCX_RELAIS_ENTITY_H
