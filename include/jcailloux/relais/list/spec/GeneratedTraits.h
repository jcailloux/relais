#ifndef JCX_RELAIS_LIST_SPEC_GENERATEDTRAITS_H
#define JCX_RELAIS_LIST_SPEC_GENERATEDTRAITS_H

#include <algorithm>
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
#include "jcailloux/relais/list/ListQuery.h"  // list::Cursor (std::byte)

namespace jcailloux::relais::list::spec {

// Unified cursor type — same as list::Cursor (std::byte based)
using Cursor = list::Cursor;

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
    requires ValidFilterSet<Descriptor>
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

            if constexpr (FilterType::op == Op::IN) {
                // IN: entity scalar ∈ query set. filter_value is optional<vector>,
                // *filter_value the set. compareWithOp is never instantiated here.
                if constexpr (FilterType::is_optional_member) {
                    if (!entity_value.has_value()) return false;  // null ∉ any set
                    return std::ranges::find(*filter_value, *entity_value) != filter_value->end();
                } else {
                    return std::ranges::find(*filter_value, entity_value) != filter_value->end();
                }
            } else if constexpr (FilterType::op == Op::NIN) {
                // NIN: entity scalar ∉ query set. Dedicated branch — never falls
                // through to compareWithOp (whose `else return true` would match all).
                if constexpr (FilterType::is_optional_member) {
                    if (!entity_value.has_value()) return false;  // null excluded (NOT the negation of IN, §1.1)
                    return std::ranges::find(*filter_value, *entity_value) == filter_value->end();
                } else {
                    return std::ranges::find(*filter_value, entity_value) == filter_value->end();
                }
            } else if constexpr (FilterType::is_optional_member) {
                // Handle optional entity members
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
    constexpr size_t N = detail::keyComponentCount<typename Descriptor::Entity>;
    Cursor cursor;
    cursor.data.resize(sizeof(int64_t) * (1 + N));  // sort_value + N key components

    int64_t sort_value = 0;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((sort.field_index == Is ? [&] {
            using SortType = sort_at<Descriptor, Is>;
            const auto value = detail::extractMemberValue<SortType::entity_ptr>(entity);
            sort_value = detail::toInt64ForCursor(value);
            return true;
        }() : false) || ...);
    }(std::make_index_sequence<sort_count<Descriptor>>{});

    // Encode: sort_value, then the entity's primary-key components (one for a
    // scalar key — byte-identical to the legacy id cursor — N for a tuple key).
    std::memcpy(cursor.data.data(), &sort_value, sizeof(sort_value));
    int64_t keys[N];
    detail::extractKeyComponents(entity, keys);
    for (size_t i = 0; i < N; ++i) {
        std::memcpy(cursor.data.data() + sizeof(int64_t) * (1 + i),
                    &keys[i], sizeof(int64_t));
    }

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
    constexpr size_t N = detail::keyComponentCount<typename Descriptor::Entity>;
    if (cursor.empty()) return true;
    if (cursor.data.size() < sizeof(int64_t) * (1 + N)) return true;

    // Decode cursor: sort_value + N key components
    int64_t cursor_sort_value = 0;
    std::memcpy(&cursor_sort_value, cursor.data.data(), sizeof(cursor_sort_value));
    int64_t cursor_keys[N];
    for (size_t i = 0; i < N; ++i) {
        std::memcpy(&cursor_keys[i], cursor.data.data() + sizeof(int64_t) * (1 + i),
                    sizeof(int64_t));
    }

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

    // Lexicographic tiebreak over the key components (one for a scalar key)
    int64_t entity_keys[N];
    detail::extractKeyComponents(entity, entity_keys);
    int kcmp = 0;
    for (size_t i = 0; i < N && kcmp == 0; ++i) {
        kcmp = (entity_keys[i] < cursor_keys[i]) ? -1
             : (entity_keys[i] > cursor_keys[i]) ? 1 : 0;
    }

    // Compare based on direction
    if (sort.direction == SortDirection::Desc) {
        if (entity_sort_value > cursor_sort_value) return true;
        if (entity_sort_value < cursor_sort_value) return false;
        return kcmp >= 0;
    } else {
        if (entity_sort_value < cursor_sort_value) return true;
        if (entity_sort_value > cursor_sort_value) return false;
        return kcmp <= 0;
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

/// Fallback page-size grid for descriptors without a `limits=` annotation.
inline constexpr std::array<uint16_t, 4> kDefaultLimits = {10, 25, 50, 100};

template<typename Descriptor>
concept HasAllowedLimits = requires {
    { Descriptor::allowedLimits } -> std::convertible_to<decltype(Descriptor::allowedLimits)>;
};

/// Round a requested limit up to the first allowed page size; the largest
/// allowed size if it exceeds the grid. Reads the descriptor's `allowedLimits`
/// (sorted ascending by the generator) when present, else falls back to
/// kDefaultLimits. The grid drives the canonical cache key, so this must stay
/// deterministic per descriptor.
template<typename Descriptor>
[[nodiscard]] constexpr uint16_t normalizeLimit(uint16_t requested) noexcept {
    if constexpr (HasAllowedLimits<Descriptor>) {
        for (auto step : Descriptor::allowedLimits) {
            if (requested <= step) return step;
        }
        return Descriptor::allowedLimits.back();
    } else {
        for (auto step : kDefaultLimits) {
            if (requested <= step) return step;
        }
        return kDefaultLimits.back();
    }
}

template<typename Descriptor>
concept HasDefaultLimit = requires {
    { Descriptor::defaultLimit } -> std::convertible_to<uint16_t>;
};

/// Default page size when a request omits the `limit` param: the descriptor's
/// declared default (allowedLimits.front(), emitted by the generator), else 20 —
/// the ListQuery struct default (ListQuery.h) — for hand-written descriptors
/// without a `limits=` grid.
template<typename Descriptor>
[[nodiscard]] constexpr uint16_t defaultLimit() noexcept {
    if constexpr (HasDefaultLimit<Descriptor>) {
        return Descriptor::defaultLimit;
    } else {
        return 20;
    }
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
        InvalidLimit,
        ConflictingPagination
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
            case Type::ConflictingPagination:
                return "Cannot use both 'after' (cursor) and 'offset' simultaneously";
        }
        return "Unknown validation error";
    }
};

template<typename Descriptor>
[[nodiscard]] bool isLimitAllowed(uint16_t limit) noexcept {
    if constexpr (HasAllowedLimits<Descriptor>) {
        for (auto allowed : Descriptor::allowedLimits) {
            if (limit == allowed) return true;
        }
        return false;
    } else {
        for (auto allowed : kDefaultLimits) {
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
        for (size_t i = 0; i < kDefaultLimits.size(); ++i) {
            if (i > 0) result += ", ";
            result += std::to_string(kDefaultLimits[i]);
        }
    }
    return result;
}

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_GENERATEDTRAITS_H
