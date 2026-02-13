#ifndef JCX_DROGON_WRAPPER_ENTITY_WRAPPER_H
#define JCX_DROGON_WRAPPER_ENTITY_WRAPPER_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <glaze/glaze.hpp>

#include "jcailloux/relais/wrapper/Format.h"

namespace jcailloux::drogon::wrapper {

// =============================================================================
// EntityWrapper<Struct, Mapping> — API-side wrapper for pure data structs
//
// Inherits from Struct (pure declarative data) and adds:
// - Thread-safe lazy BEVE/JSON serialization via std::call_once
// - ORM mapping (fromModel/toModel) delegated to Mapping
// - Primary key access delegated to Mapping
//
// Struct is framework-agnostic and can be shared across projects.
// Mapping is generated and Drogon-specific (API-only).
//
// Satisfies: HasBinarySerialization, HasJsonSerialization, HasFormat
// =============================================================================

template<typename Struct, typename Mapping>
class EntityWrapper : public Struct {
public:
    using Format = jcailloux::relais::StructFormat;
    using Model = typename Mapping::Model;
    using TraitsType = typename Mapping::TraitsType;
    using Field = typename TraitsType::Field;
    static constexpr bool read_only = Mapping::read_only;

    EntityWrapper() = default;
    explicit EntityWrapper(const Struct& s) : Struct(s) {}
    explicit EntityWrapper(Struct&& s) noexcept : Struct(std::move(s)) {}

    // std::once_flag is non-copyable/non-movable — caches are transient
    // and will be lazily recomputed after copy/move.
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

    [[nodiscard]] auto getPrimaryKey() const noexcept {
        return Mapping::getPrimaryKey(*this);
    }

    // =========================================================================
    // ORM mapping — delegated to Mapping
    // =========================================================================

    static std::optional<EntityWrapper> fromModel(const Model& model) {
        return Mapping::template fromModel<EntityWrapper>(model);
    }

    static Model toModel(const EntityWrapper& e) {
        return Mapping::template toModel<EntityWrapper>(e);
    }

    // =========================================================================
    // Partial key — conditionally delegated to Mapping
    // =========================================================================

    template<typename M>
    static auto makeKeyCriteria(const auto& key)
        requires requires { Mapping::template makeKeyCriteria<M>(key); }
    {
        return Mapping::template makeKeyCriteria<M>(key);
    }

    // =========================================================================
    // Mapping type — exposed for concept detection
    // =========================================================================

    using MappingType = Mapping;

    // =========================================================================
    // Binary serialization (Glaze BEVE, thread-safe lazy)
    // =========================================================================

    [[nodiscard]] std::shared_ptr<const std::vector<uint8_t>> toBinary() const {
        std::call_once(beve_flag_, [this] {
            auto buf = std::make_shared<std::vector<uint8_t>>();
            if (glz::write_beve(*this, *buf))
                buf->clear();
            beve_cache_ = std::move(buf);
        });
        return beve_cache_;
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
    // JSON serialization (Glaze JSON, thread-safe lazy)
    // =========================================================================

    [[nodiscard]] std::shared_ptr<const std::string> toJson() const {
        std::call_once(json_flag_, [this] {
            auto json = std::make_shared<std::string>();
            json->reserve(256);
            if (glz::write_json(*this, *json))
                json_cache_ = std::make_shared<std::string>("{}");
            else
                json_cache_ = std::move(json);
        });
        return json_cache_;
    }

    static std::optional<EntityWrapper> fromJson(std::string_view json) {
        if (json.empty()) return std::nullopt;
        EntityWrapper entity;
        if (glz::read_json(entity, json)) return std::nullopt;
        return entity;
    }

    // =========================================================================
    // Cache management
    // =========================================================================

    /// Release serialization caches. After this call, toBinary()/toJson()
    /// return nullptr. Callers who previously obtained shared_ptrs from
    /// toBinary()/toJson() retain valid data through reference counting.
    void releaseCaches() const noexcept {
        beve_cache_.reset();
        json_cache_.reset();
    }

private:
    mutable std::once_flag beve_flag_;
    mutable std::once_flag json_flag_;
    mutable std::shared_ptr<const std::vector<uint8_t>> beve_cache_;
    mutable std::shared_ptr<const std::string> json_cache_;
};

}  // namespace jcailloux::drogon::wrapper

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
struct glz::meta<jcailloux::drogon::wrapper::EntityWrapper<Struct, Mapping>> {
    using T = jcailloux::drogon::wrapper::EntityWrapper<Struct, Mapping>;
    static constexpr auto value = [] {
        if constexpr (requires { glz::meta<Struct>::value; }) {
            return glz::meta<Struct>::value;
        } else {
            return Mapping::template glaze_value<T>;
        }
    }();
};

#endif  // JCX_DROGON_WRAPPER_ENTITY_WRAPPER_H
