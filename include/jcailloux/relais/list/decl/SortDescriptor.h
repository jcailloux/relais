#ifndef CODIBOT_CACHE_LIST_DECL_SORTDESCRIPTOR_H
#define CODIBOT_CACHE_LIST_DECL_SORTDESCRIPTOR_H

#include <cstdint>
#include <string_view>
#include <type_traits>

#include "FixedString.h"
#include "FilterDescriptor.h"  // For detail::member_pointer_type_t

namespace jcailloux::relais::cache::list::decl {

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
///   - std::optional<T> where T is integral or enum
///
/// String types are NOT supported. Declaring a Sort on a string field
/// will produce a compile error. Use an integer timestamp field instead.
template<typename T>
concept CursorEncodable =
    std::is_integral_v<detail::unwrap_optional_t<std::remove_cvref_t<T>>> ||
    std::is_enum_v<detail::unwrap_optional_t<std::remove_cvref_t<T>>>;

// =============================================================================
// Sort declaration template
// =============================================================================

/// Declares a sortable field for list queries
///
/// @tparam Name            Field name (for HTTP query param: ?sort=name)
/// @tparam EntityMemberPtr Pointer to entity member (&Entity::field)
/// @tparam ColumnName      SQL column name as FixedString ("column_name")
/// @tparam DefaultDir      Default sort direction (default: Asc)
///
/// Example:
///   Sort<"created_at", &Entity::created_at_us, "created_at">{}
///   Sort<"id", &Entity::id, "id">{}
///
template<
    FixedString Name,
    auto EntityMemberPtr,
    FixedString ColumnName,
    SortDirection DefaultDir = SortDirection::Asc
>
struct Sort {
    /// The value type for cursor encoding, deduced from the Entity member.
    using value_type = detail::member_pointer_type_t<decltype(EntityMemberPtr)>;

    static_assert(CursorEncodable<value_type>,
        "Sort field type must be integral or enum. String types cannot be "
        "encoded as int64_t for cursor pagination. Use an integer timestamp "
        "field (e.g. microseconds since epoch) instead of a string date.");

    /// Field name (from HTTP query param)
    static constexpr auto name = Name;

    /// Pointer to entity member
    static constexpr auto entity_ptr = EntityMemberPtr;

    /// SQL column name
    static constexpr auto column_name = ColumnName;

    /// Default sort direction
    static constexpr SortDirection default_direction = DefaultDir;

    /// Get the SQL column name
    [[nodiscard]] static constexpr std::string_view column() noexcept {
        return column_name.view();
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

}  // namespace jcailloux::relais::cache::list::decl

#endif  // CODIBOT_CACHE_LIST_DECL_SORTDESCRIPTOR_H
