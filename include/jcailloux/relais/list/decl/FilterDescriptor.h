#ifndef CODIBOT_CACHE_LIST_DECL_FILTERDESCRIPTOR_H
#define CODIBOT_CACHE_LIST_DECL_FILTERDESCRIPTOR_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "FixedString.h"

namespace jcailloux::relais::cache::list::decl {

// =============================================================================
// Comparison operators for filters
// =============================================================================

enum class Op : uint8_t {
    EQ,  // Equal
    NE,  // Not equal
    GT,  // Greater than
    GE,  // Greater than or equal
    LT,  // Less than
    LE   // Less than or equal
};

// =============================================================================
// Invalidation strategy for filters
// =============================================================================

enum class InvalidationStrategy : uint8_t {
    PreComputed,  // Hash pre-computed on modification (for EQ, NE)
    Lazy,         // Checked lazily on cache access (for range ops: GT, GE, LT, LE)
    Disabled      // Never invalidates (e.g., pagination-only fields)
};

/// Get default invalidation strategy for an operator
constexpr InvalidationStrategy defaultInvalidationStrategy(Op op) noexcept {
    switch (op) {
        case Op::EQ:
        case Op::NE:
            return InvalidationStrategy::PreComputed;
        case Op::GT:
        case Op::GE:
        case Op::LT:
        case Op::LE:
            return InvalidationStrategy::Lazy;
    }
    return InvalidationStrategy::Lazy;  // Fallback
}

// =============================================================================
// Converter tag types - how to transform value for DB query
// =============================================================================

/// No conversion needed (default)
struct NoConvert {};

/// Convert enum to string via toString() (using ADL)
struct AsString {};

// =============================================================================
// Type traits helpers
// =============================================================================

namespace detail {

/// Check if T is std::optional<U>
template<typename T>
struct is_optional : std::false_type {};

template<typename T>
struct is_optional<std::optional<T>> : std::true_type {};

template<typename T>
inline constexpr bool is_optional_v = is_optional<T>::value;

/// Unwrap std::optional<T> to T, otherwise keep T
template<typename T>
struct unwrap_optional { using type = T; };

template<typename T>
struct unwrap_optional<std::optional<T>> { using type = T; };

template<typename T>
using unwrap_optional_t = typename unwrap_optional<T>::type;

/// Extract the class type from a member pointer (data or function)
template<typename T>
struct member_pointer_class;

template<typename C, typename M>
struct member_pointer_class<M C::*> { using type = C; };

// Member function pointers (const)
template<typename C, typename R, typename... Args>
struct member_pointer_class<R (C::*)(Args...) const> { using type = C; };

// Member function pointers (non-const)
template<typename C, typename R, typename... Args>
struct member_pointer_class<R (C::*)(Args...)> { using type = C; };

// Member function pointers (const noexcept)
template<typename C, typename R, typename... Args>
struct member_pointer_class<R (C::*)(Args...) const noexcept> { using type = C; };

// Member function pointers (non-const noexcept)
template<typename C, typename R, typename... Args>
struct member_pointer_class<R (C::*)(Args...) noexcept> { using type = C; };

template<typename T>
using member_pointer_class_t = typename member_pointer_class<T>::type;

/// Extract the member/return type from a member pointer
template<typename T>
struct member_pointer_type;

// Data member pointer
template<typename C, typename M>
struct member_pointer_type<M C::*> { using type = M; };

// Member function pointer (const) - extract return type
template<typename C, typename R, typename... Args>
struct member_pointer_type<R (C::*)(Args...) const> { using type = R; };

// Member function pointer (non-const) - extract return type
template<typename C, typename R, typename... Args>
struct member_pointer_type<R (C::*)(Args...)> { using type = R; };

// Member function pointer (const noexcept) - extract return type
template<typename C, typename R, typename... Args>
struct member_pointer_type<R (C::*)(Args...) const noexcept> { using type = R; };

// Member function pointer (non-const noexcept) - extract return type
template<typename C, typename R, typename... Args>
struct member_pointer_type<R (C::*)(Args...) noexcept> { using type = R; };

template<typename T>
using member_pointer_type_t = typename member_pointer_type<T>::type;

// =============================================================================
// Member value extraction - works with both data members and member functions
// =============================================================================

/// Extract value from entity using either data member or member function pointer
template<auto MemberPtr, typename Entity>
[[nodiscard]] decltype(auto) extractMemberValue(const Entity& entity) noexcept {
    if constexpr (std::is_member_function_pointer_v<decltype(MemberPtr)>) {
        // Member function: call it
        return (entity.*MemberPtr)();
    } else {
        // Data member: access directly
        return entity.*MemberPtr;
    }
}

// =============================================================================
// Entity ID extraction - works with both id member and id() method
// =============================================================================

/// Concept: Entity has id() method
template<typename Entity>
concept HasIdMethod = requires(const Entity& e) {
    { e.id() } -> std::convertible_to<int64_t>;
};

/// Concept: Entity has id data member
template<typename Entity>
concept HasIdMember = requires(const Entity& e) {
    { e.id } -> std::convertible_to<int64_t>;
};

/// Extract entity ID (supports both data member and method)
template<typename Entity>
[[nodiscard]] int64_t extractEntityId(const Entity& entity) noexcept {
    if constexpr (HasIdMethod<Entity>) {
        return entity.id();
    } else {
        return entity.id;
    }
}

}  // namespace detail

// =============================================================================
// Filter declaration template
// =============================================================================

/// Declares a filter field for list queries
///
/// @tparam Name            Field name (for HTTP query param and debugging)
/// @tparam EntityMemberPtr Pointer to entity member (&Entity::field)
/// @tparam ColumnName      SQL column name as FixedString ("column_name")
/// @tparam Operator        Comparison operator (default: EQ)
/// @tparam Converter       Value conversion type (default: NoConvert)
/// @tparam Invalidation    How to handle cache invalidation (default: based on Op)
///
/// Example:
///   Filter<"guild_id", &Entity::guild_id, "guild_id">{}
///   Filter<"severity", &Entity::severity, "severity", Op::EQ, AsString>{}
///   Filter<"date_from", &Entity::created_at, "created_at", Op::GE>{}
///
template<
    FixedString Name,
    auto EntityMemberPtr,
    FixedString ColumnName,
    Op Operator = Op::EQ,
    typename Converter = NoConvert,
    InvalidationStrategy Invalidation = defaultInvalidationStrategy(Operator)
>
struct Filter {
    /// Field name (from HTTP query param)
    static constexpr auto name = Name;

    /// Pointer to entity member
    static constexpr auto entity_ptr = EntityMemberPtr;

    /// SQL column name
    static constexpr auto column_name = ColumnName;

    /// Comparison operator
    static constexpr Op op = Operator;

    /// Invalidation strategy
    static constexpr InvalidationStrategy invalidation = Invalidation;

    /// Converter type
    using converter = Converter;

    /// Get the SQL column name
    [[nodiscard]] static constexpr std::string_view column() noexcept {
        return column_name.view();
    }

    /// The raw member type from the entity
    using member_type = detail::member_pointer_type_t<decltype(EntityMemberPtr)>;

    /// The unwrapped type (optional<T> -> T)
    using value_type = detail::unwrap_optional_t<member_type>;

    /// The filter value type (always optional for filter activation check)
    using filter_type = std::optional<value_type>;

    /// Whether the entity member is optional
    static constexpr bool is_optional_member = detail::is_optional_v<member_type>;

    /// Whether this filter uses pre-computed invalidation
    static constexpr bool is_precomputed = (invalidation == InvalidationStrategy::PreComputed);

    /// Whether this filter uses lazy invalidation
    static constexpr bool is_lazy = (invalidation == InvalidationStrategy::Lazy);
};

}  // namespace jcailloux::relais::cache::list::decl

#endif  // CODIBOT_CACHE_LIST_DECL_FILTERDESCRIPTOR_H
