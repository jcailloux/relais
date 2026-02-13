#ifndef CODIBOT_CACHE_LIST_DECL_SORTDESCRIPTOR_H
#define CODIBOT_CACHE_LIST_DECL_SORTDESCRIPTOR_H

#include <cstdint>
#include <string>
#include <type_traits>

#include "FixedString.h"
#include "FilterDescriptor.h"  // For detail::member_pointer_type_t

namespace jcailloux::drogon::cache::list::decl {

// =============================================================================
// Sort direction
// =============================================================================

enum class SortDirection : uint8_t {
    Asc,
    Desc
};

// =============================================================================
// Sort value type constraint
// =============================================================================

/// A sort field must be encodable as int64_t for cursor pagination and
/// sort bounds range checks.
///
/// Supported types:
///   - Integral types (int64_t, uint32_t, etc.)
///   - Enum types (cast to underlying)
///   - Date types with microSecondsSinceEpoch() (trantor::Date)
///
/// String types are NOT supported. Declaring a Sort on a string field
/// will produce a compile error.
template<typename T>
concept CursorEncodable =
    std::is_integral_v<std::remove_cvref_t<T>> ||
    std::is_enum_v<std::remove_cvref_t<T>> ||
    requires(const std::remove_cvref_t<T>& v) {
        { v.microSecondsSinceEpoch() } -> std::convertible_to<int64_t>;
    };

// =============================================================================
// Sort declaration template
// =============================================================================

/// Declares a sortable field for list queries
///
/// @tparam Name            Field name (for HTTP query param: ?sort=name)
/// @tparam EntityMemberPtr Pointer to entity member (&Entity::field)
/// @tparam ColumnPtr       Pointer to Drogon model column (&Model::Cols::_field)
/// @tparam ModelMemberPtr  Pointer to Drogon model getter (&Model::getValueOfField)
/// @tparam DefaultDir      Default sort direction (default: Asc)
///
/// Example:
///   Sort<"created_at", &Entity::created_at, &Cols::_created_at, &Model::getValueOfCreatedAt, SortDirection::Desc>{}
///   Sort<"id", &Entity::id, &Cols::_id, &Model::getValueOfId>{}
///
template<
    FixedString Name,
    auto EntityMemberPtr,
    const std::string* ColumnPtr,
    auto ModelMemberPtr,
    SortDirection DefaultDir = SortDirection::Asc
>
struct Sort {
    /// The value type for cursor encoding, deduced from the Model getter.
    /// We use ModelMemberPtr rather than EntityMemberPtr because FlatBuffer
    /// entities may store dates as strings, while the Model always returns
    /// the correct semantic type (e.g. trantor::Date for timestamps).
    using value_type = detail::member_pointer_type_t<decltype(ModelMemberPtr)>;

    static_assert(CursorEncodable<value_type>,
        "Sort field type must be integral, enum, or a date type with "
        "microSecondsSinceEpoch(). String types cannot be encoded as "
        "int64_t for cursor pagination.");

    /// Field name (from HTTP query param)
    static constexpr auto name = Name;

    /// Pointer to entity member
    static constexpr auto entity_ptr = EntityMemberPtr;

    /// Pointer to Drogon column string
    static constexpr auto column_ptr = ColumnPtr;

    /// Pointer to Drogon model getter
    static constexpr auto model_ptr = ModelMemberPtr;

    /// Default sort direction
    static constexpr SortDirection default_direction = DefaultDir;

    /// Get the Drogon column name (at runtime)
    [[nodiscard]] static const std::string& column() noexcept {
        return *column_ptr;
    }
};

// =============================================================================
// Sort specification (runtime)
// =============================================================================

/// Runtime sort specification with field index
template<typename Descriptor>
struct SortSpec {
    size_t field_index{0};  // Index into Descriptor::sorts tuple
    SortDirection direction{SortDirection::Asc};

    bool operator==(const SortSpec&) const = default;
};

}  // namespace jcailloux::drogon::cache::list::decl

#endif  // CODIBOT_CACHE_LIST_DECL_SORTDESCRIPTOR_H
