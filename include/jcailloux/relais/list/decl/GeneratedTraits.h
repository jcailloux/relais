#ifndef CODIBOT_CACHE_LIST_DECL_GENERATEDTRAITS_H
#define CODIBOT_CACHE_LIST_DECL_GENERATEDTRAITS_H

#include <array>
#include <concepts>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "FilterDescriptor.h"
#include "SortDescriptor.h"
#include "ListDescriptor.h"
#include "GeneratedFilters.h"

namespace jcailloux::relais::cache::list::decl {

// =============================================================================
// Cursor for keyset pagination
// =============================================================================

struct Cursor {
    std::vector<uint8_t> data;

    [[nodiscard]] bool empty() const noexcept { return data.empty(); }
    void clear() noexcept { data.clear(); }
};

// =============================================================================
// matchesFilters - Check if entity matches all active filters
// =============================================================================

namespace detail {

/// Compare values with the given operator
template<Op op, typename T>
[[nodiscard]] constexpr bool compareWithOp(const T& entity_val, const T& filter_val) noexcept {
    if constexpr (op == Op::EQ) return entity_val == filter_val;
    else if constexpr (op == Op::NE) return entity_val != filter_val;
    else if constexpr (op == Op::GT) return entity_val > filter_val;
    else if constexpr (op == Op::GE) return entity_val >= filter_val;
    else if constexpr (op == Op::LT) return entity_val < filter_val;
    else if constexpr (op == Op::LE) return entity_val <= filter_val;
    else return true;
}

}  // namespace detail

/// Check if entity matches all active filters
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] bool matchesFilters(
    const typename Descriptor::Entity& entity,
    const Filters<Descriptor>& filters
) noexcept {
    bool result = true;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        result = (([&] {
            using FilterType = filter_at<Descriptor, Is>;
            const auto& filter_value = filters.template get<Is>();

            // Filter not active -> matches
            if (!filter_value.has_value()) return true;

            // Get entity value (supports both data members and member functions)
            const auto entity_value = detail::extractMemberValue<FilterType::entity_ptr>(entity);

            // Handle optional entity members
            if constexpr (FilterType::is_optional_member) {
                if (!entity_value.has_value()) {
                    return FilterType::op == Op::NE;
                }
                return detail::compareWithOp<FilterType::op>(*entity_value, *filter_value);
            } else {
                return detail::compareWithOp<FilterType::op>(entity_value, *filter_value);
            }
        }()) && ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    return result;
}

// =============================================================================
// compare - Compare two entities for sorting
// =============================================================================

namespace detail {

/// Compare two values, returns <0, 0, >0
template<typename T>
[[nodiscard]] int compareValues(const T& a, const T& b) noexcept {
    if constexpr (std::is_integral_v<std::remove_cvref_t<T>>) {
        return (a < b) ? -1 : (a > b) ? 1 : 0;
    } else if constexpr (std::is_enum_v<std::remove_cvref_t<T>>) {
        using U = std::underlying_type_t<std::remove_cvref_t<T>>;
        return static_cast<int>(static_cast<U>(a)) - static_cast<int>(static_cast<U>(b));
    } else if constexpr (requires { a.compare(b); }) {
        return a.compare(b);
    } else {
        return (a < b) ? -1 : (a > b) ? 1 : 0;
    }
}

}  // namespace detail

/// Compare two entities based on sort specification
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] int compare(
    const typename Descriptor::Entity& a,
    const typename Descriptor::Entity& b,
    const SortSpec<Descriptor>& sort
) noexcept {
    int result = 0;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((sort.field_index == Is ? [&] {
            using SortType = sort_at<Descriptor, Is>;
            const auto va = detail::extractMemberValue<SortType::entity_ptr>(a);
            const auto vb = detail::extractMemberValue<SortType::entity_ptr>(b);
            result = detail::compareValues(va, vb);
            return true;
        }() : false) || ...);
    }(std::make_index_sequence<sort_count<Descriptor>>{});

    return (sort.direction == SortDirection::Desc) ? -result : result;
}

// =============================================================================
// extractCursor / isBeforeOrAtCursor - Cursor-based pagination
// =============================================================================

namespace detail {

/// Extract sort value as int64 for cursor encoding
template<typename T>
[[nodiscard]] int64_t toInt64ForCursor(const T& value) noexcept {
    if constexpr (std::is_enum_v<std::remove_cvref_t<T>>) {
        return static_cast<int64_t>(value);
    } else if constexpr (std::is_integral_v<std::remove_cvref_t<T>>) {
        return static_cast<int64_t>(value);
    } else if constexpr (requires { value.has_value(); *value; }) {
        return value.has_value() ? toInt64ForCursor(*value) : 0;
    } else {
        return 0;
    }
}

}  // namespace detail

/// Extract cursor from entity for pagination
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] Cursor extractCursor(
    const typename Descriptor::Entity& entity,
    const SortSpec<Descriptor>& sort
) noexcept {
    Cursor cursor;
    cursor.data.resize(sizeof(int64_t) * 2);  // sort_value + id

    int64_t sort_value = 0;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((sort.field_index == Is ? [&] {
            using SortType = sort_at<Descriptor, Is>;
            const auto value = detail::extractMemberValue<SortType::entity_ptr>(entity);
            sort_value = detail::toInt64ForCursor(value);
            return true;
        }() : false) || ...);
    }(std::make_index_sequence<sort_count<Descriptor>>{});

    // Encode: sort_value, then entity id (supports both member and method)
    std::memcpy(cursor.data.data(), &sort_value, sizeof(sort_value));
    int64_t id = detail::extractEntityId(entity);
    std::memcpy(cursor.data.data() + sizeof(sort_value), &id, sizeof(id));

    return cursor;
}

/// Check if entity is at or before cursor position
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] bool isBeforeOrAtCursor(
    const typename Descriptor::Entity& entity,
    const Cursor& cursor,
    const SortSpec<Descriptor>& sort
) noexcept {
    if (cursor.empty()) return true;
    if (cursor.data.size() < sizeof(int64_t) * 2) return true;

    // Decode cursor
    int64_t cursor_sort_value = 0;
    int64_t cursor_id = 0;
    std::memcpy(&cursor_sort_value, cursor.data.data(), sizeof(cursor_sort_value));
    std::memcpy(&cursor_id, cursor.data.data() + sizeof(cursor_sort_value), sizeof(cursor_id));

    // Get entity sort value
    int64_t entity_sort_value = 0;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((sort.field_index == Is ? [&] {
            using SortType = sort_at<Descriptor, Is>;
            const auto value = detail::extractMemberValue<SortType::entity_ptr>(entity);
            entity_sort_value = detail::toInt64ForCursor(value);
            return true;
        }() : false) || ...);
    }(std::make_index_sequence<sort_count<Descriptor>>{});

    // Get entity id (supports both member and method)
    int64_t entity_id = detail::extractEntityId(entity);

    // Compare based on direction
    if (sort.direction == SortDirection::Desc) {
        if (entity_sort_value > cursor_sort_value) return true;
        if (entity_sort_value < cursor_sort_value) return false;
        return entity_id >= cursor_id;
    } else {
        if (entity_sort_value < cursor_sort_value) return true;
        if (entity_sort_value > cursor_sort_value) return false;
        return entity_id <= cursor_id;
    }
}

// =============================================================================
// parseSortField / sortFieldName - String <-> index conversion
// =============================================================================

/// Parse sort field name to index
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] std::optional<size_t> parseSortField(std::string_view field) noexcept {
    std::optional<size_t> result;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((sort_at<Descriptor, Is>::name.view() == field ? (result = Is, true) : false) || ...);
    }(std::make_index_sequence<sort_count<Descriptor>>{});

    return result;
}

/// Get sort field name from index
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] std::string_view sortFieldName(size_t field_index) noexcept {
    std::string_view result;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((field_index == Is ? (result = sort_at<Descriptor, Is>::name.view(), true) : false) || ...);
    }(std::make_index_sequence<sort_count<Descriptor>>{});

    return result;
}

/// Get sort column name from index (for DB query)
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] std::string_view sortColumnName(size_t field_index) noexcept {
    std::string_view result;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((field_index == Is ? (result = sort_at<Descriptor, Is>::column(), true) : false) || ...);
    }(std::make_index_sequence<sort_count<Descriptor>>{});

    return result;
}

// =============================================================================
// Default sort specification
// =============================================================================

/// Get the default sort specification (first sort field with its default direction)
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] constexpr SortSpec<Descriptor> defaultSort() noexcept {
    using FirstSort = sort_at<Descriptor, 0>;
    return SortSpec<Descriptor>{0, FirstSort::default_direction};
}

// =============================================================================
// Limit normalization
// =============================================================================

/// Normalize requested limit to allowed step
template<typename Descriptor>
[[nodiscard]] constexpr uint16_t normalizeLimit(
    uint16_t requested,
    const std::array<uint16_t, 4>& steps = {10, 25, 50, 100},
    uint16_t max_limit = 100
) noexcept {
    for (auto step : steps) {
        if (requested <= step) return step;
    }
    return max_limit;
}

// =============================================================================
// extractSortValue - Extract sort field value from entity
// =============================================================================

/// Extract sort field value from entity as int64_t
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] int64_t extractSortValue(
    const typename Descriptor::Entity& entity,
    size_t field_index
) noexcept {
    int64_t result = 0;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((field_index == Is ? [&] {
            using SortType = sort_at<Descriptor, Is>;
            const auto value = detail::extractMemberValue<SortType::entity_ptr>(entity);
            result = detail::toInt64ForCursor(value);
            return true;
        }() : false) || ...);
    }(std::make_index_sequence<sort_count<Descriptor>>{});

    return result;
}

// =============================================================================
// compareSortValues - Compare sort values with direction
// =============================================================================

[[nodiscard]] inline int compareSortValues(int64_t a, int64_t b, SortDirection dir) noexcept {
    int cmp = (a < b) ? -1 : (a > b) ? 1 : 0;
    return (dir == SortDirection::Desc) ? -cmp : cmp;
}

// =============================================================================
// isInSortRange - Check if entity falls within sort range of a list
// =============================================================================

[[nodiscard]] inline bool isInSortRange(
    int64_t entity_sort_value,
    int64_t first_sort_value,
    int64_t last_sort_value,
    SortDirection direction
) noexcept {
    int cmp_first = compareSortValues(entity_sort_value, first_sort_value, direction);
    int cmp_last = compareSortValues(entity_sort_value, last_sort_value, direction);
    return cmp_first >= 0 && cmp_last <= 0;
}

template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] bool isInSortRange(
    const typename Descriptor::Entity& entity,
    int64_t first_sort_value,
    int64_t last_sort_value,
    const SortSpec<Descriptor>& sort
) noexcept {
    int64_t entity_sort_value = extractSortValue<Descriptor>(entity, sort.field_index);
    return isInSortRange(entity_sort_value, first_sort_value, last_sort_value, sort.direction);
}

// =============================================================================
// Query Validation
// =============================================================================

struct QueryValidationError {
    enum class Type : uint8_t {
        InvalidFilter,
        InvalidSort,
        InvalidLimit
    };

    Type type;
    std::string field;
    uint16_t limit{0};

    [[nodiscard]] std::string message() const {
        switch (type) {
            case Type::InvalidFilter:
                return "Invalid filter: " + field;
            case Type::InvalidSort:
                return "Invalid sort field: " + field;
            case Type::InvalidLimit:
                return "Invalid limit: " + std::to_string(limit);
        }
        return "Unknown validation error";
    }
};

template<typename Descriptor>
concept HasAllowedLimits = requires {
    { Descriptor::allowedLimits } -> std::convertible_to<decltype(Descriptor::allowedLimits)>;
};

template<typename Descriptor>
[[nodiscard]] bool isLimitAllowed(uint16_t limit) noexcept {
    if constexpr (HasAllowedLimits<Descriptor>) {
        for (auto allowed : Descriptor::allowedLimits) {
            if (limit == allowed) return true;
        }
        return false;
    } else {
        constexpr std::array<uint16_t, 4> default_limits = {10, 25, 50, 100};
        for (auto allowed : default_limits) {
            if (limit == allowed) return true;
        }
        return false;
    }
}

template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] bool isSortFieldValid(size_t field_index) noexcept {
    return field_index < sort_count<Descriptor>;
}

template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] std::optional<QueryValidationError> validateSortField(std::string_view field_name) noexcept {
    if (parseSortField<Descriptor>(field_name).has_value()) {
        return std::nullopt;
    }
    return QueryValidationError{
        .type = QueryValidationError::Type::InvalidSort,
        .field = std::string(field_name),
        .limit = 0
    };
}

template<typename Descriptor>
[[nodiscard]] std::optional<QueryValidationError> validateLimit(uint16_t limit) noexcept {
    if (isLimitAllowed<Descriptor>(limit)) {
        return std::nullopt;
    }
    return QueryValidationError{
        .type = QueryValidationError::Type::InvalidLimit,
        .field = {},
        .limit = limit
    };
}

template<typename Descriptor>
[[nodiscard]] std::string getAllowedLimitsString() {
    std::string result;
    if constexpr (HasAllowedLimits<Descriptor>) {
        for (size_t i = 0; i < Descriptor::allowedLimits.size(); ++i) {
            if (i > 0) result += ", ";
            result += std::to_string(Descriptor::allowedLimits[i]);
        }
    } else {
        result = "10, 25, 50, 100";
    }
    return result;
}

}  // namespace jcailloux::relais::cache::list::decl

#endif  // CODIBOT_CACHE_LIST_DECL_GENERATEDTRAITS_H
