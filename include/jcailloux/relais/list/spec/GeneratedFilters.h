#ifndef JCX_RELAIS_LIST_SPEC_GENERATEDFILTERS_H
#define JCX_RELAIS_LIST_SPEC_GENERATEDFILTERS_H

#include <algorithm>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

#include "FilterDescriptor.h"
#include "ListDescriptor.h"

namespace jcailloux::relais::list::spec {

// =============================================================================
// Generated Filters struct from declaration
// =============================================================================

namespace detail {

/// Build a tuple type of filter_type from each Filter in the tuple
template<typename Descriptor, typename Seq>
struct FiltersTupleImpl;

template<typename Descriptor, size_t... Is>
struct FiltersTupleImpl<Descriptor, std::index_sequence<Is...>> {
    using type = std::tuple<typename filter_at<Descriptor, Is>::filter_type...>;
};

template<typename Descriptor>
using FiltersTuple = typename FiltersTupleImpl<
    Descriptor,
    std::make_index_sequence<filter_count<Descriptor>>
>::type;

/// Build a tuple type of tag_type from each Filter in the tuple.
/// tag_type is always a scalar optional (optional<element>), even for IN —
/// an entity holds a single value, matched against the cached group's set.
/// This decouples FilterTags storage from filter_type (optional<vector> for IN).
template<typename Descriptor, typename Seq>
struct TagsTupleImpl;

template<typename Descriptor, size_t... Is>
struct TagsTupleImpl<Descriptor, std::index_sequence<Is...>> {
    using type = std::tuple<typename filter_at<Descriptor, Is>::tag_type...>;
};

template<typename Descriptor>
using TagsTuple = typename TagsTupleImpl<
    Descriptor,
    std::make_index_sequence<filter_count<Descriptor>>
>::type;

/// Find index of filter by name (compile-time)
template<typename Descriptor, FixedString Name, size_t I = 0>
constexpr size_t find_filter_index() {
    if constexpr (I >= filter_count<Descriptor>) {
        // Static assert with helpful message
        static_assert(I < filter_count<Descriptor>, "Filter name not found in declaration");
        return I;
    } else if constexpr (filter_at<Descriptor, I>::name == Name) {
        return I;
    } else {
        return find_filter_index<Descriptor, Name, I + 1>();
    }
}

}  // namespace detail

// Forward declaration — Filters::matchesFilters takes entity-side tags, which
// are stored in the decoupled FilterTags struct (scalar tag_type slots).
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
struct FilterTags;

/// Generated Filters struct that holds filter values
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
struct Filters {
    /// Tuple storing all filter values (each as std::optional<T>)
    detail::FiltersTuple<Descriptor> values{};

    /// Access filter by name (compile-time lookup)
    template<FixedString Name>
    [[nodiscard]] auto& get() noexcept {
        constexpr size_t idx = detail::find_filter_index<Descriptor, Name>();
        return std::get<idx>(values);
    }

    template<FixedString Name>
    [[nodiscard]] const auto& get() const noexcept {
        constexpr size_t idx = detail::find_filter_index<Descriptor, Name>();
        return std::get<idx>(values);
    }

    /// Check if any filter is active
    [[nodiscard]] bool hasAnyFilter() const noexcept {
        return std::apply([](const auto&... opts) {
            return (opts.has_value() || ...);
        }, values);
    }

    /// Count active filters
    [[nodiscard]] size_t activeFilterCount() const noexcept {
        return std::apply([](const auto&... opts) {
            return (static_cast<size_t>(opts.has_value()) + ...);
        }, values);
    }

    /// Check whether an entity (its extracted tags) matches this query's filters.
    /// `this` holds the query filters; `tags` holds the entity-side scalar values.
    /// Returns true if the entity satisfies every active filter.
    [[nodiscard]] bool matchesFilters(const FilterTags<Descriptor>& tags) const noexcept {
        return [&]<size_t... Is>(std::index_sequence<Is...>) {
            return (matchesFilterAt<Is>(tags) && ...);
        }(std::make_index_sequence<std::tuple_size_v<detail::FiltersTuple<Descriptor>>>{});
    }

private:
    /// Check if entity tag at index I satisfies the query filter at same index.
    template<size_t I>
    [[nodiscard]] bool matchesFilterAt(const FilterTags<Descriptor>& tags) const noexcept {
        const auto& tag_value = std::get<I>(tags.values);     // optional<element>
        const auto& filter_value = std::get<I>(values);       // optional<element | vector<element>>

        // If filter not set, always matches
        if (!filter_value.has_value()) return true;

        // If filter set but tag has no value, doesn't match
        if (!tag_value.has_value()) return false;

        // Compare values based on filter operator
        using FilterType = filter_at<Descriptor, I>;
        constexpr Op op = FilterType::op;

        if constexpr (op == Op::IN) {
            // Membership: entity scalar ∈ query set
            return std::ranges::find(*filter_value, *tag_value) != filter_value->end();
        } else if constexpr (op == Op::NIN) {
            // Anti-membership: entity scalar ∉ query set (null already rejected above)
            return std::ranges::find(*filter_value, *tag_value) == filter_value->end();
        } else if constexpr (op == Op::EQ) {
            return *tag_value == *filter_value;
        } else if constexpr (op == Op::NE) {
            return *tag_value != *filter_value;
        } else if constexpr (op == Op::GT) {
            return *tag_value > *filter_value;
        } else if constexpr (op == Op::GE) {
            return *tag_value >= *filter_value;
        } else if constexpr (op == Op::LT) {
            return *tag_value < *filter_value;
        } else if constexpr (op == Op::LE) {
            return *tag_value <= *filter_value;
        } else {
            return true;  // Unknown op - assume match
        }
    }

public:
    bool operator==(const Filters&) const = default;
};

// =============================================================================
// Named filter access helper macro
// =============================================================================

/// Helper to access filter by string name (for use in controllers)
/// Usage: FILTER(filters, "guild_id") = 123;
#define FILTER(filters, name) (filters).template get<name>()

// =============================================================================
// FilterTags - extracted from entity for fast matching
// =============================================================================

/// FilterTags holds the scalar values extracted from an entity for O(1) filter
/// matching during cache invalidation checks. Distinct from Filters: its tuple is
/// built on tag_type (always optional<element>) so an IN filter's optional<vector>
/// slot does not leak into entity-side tag storage — an entity holds one value.
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
struct FilterTags {
    /// Tuple storing the entity's scalar tag values (each as std::optional<element>)
    detail::TagsTuple<Descriptor> values{};

    /// Access tag by name (compile-time lookup)
    template<FixedString Name>
    [[nodiscard]] auto& get() noexcept {
        constexpr size_t idx = detail::find_filter_index<Descriptor, Name>();
        return std::get<idx>(values);
    }

    template<FixedString Name>
    [[nodiscard]] const auto& get() const noexcept {
        constexpr size_t idx = detail::find_filter_index<Descriptor, Name>();
        return std::get<idx>(values);
    }

    bool operator==(const FilterTags&) const = default;
};

/// Extract filter tags from an entity
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
[[nodiscard]] FilterTags<Descriptor> extractTags(const typename Descriptor::Entity& entity) noexcept {
    FilterTags<Descriptor> tags;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((std::get<Is>(tags.values) = [&] {
            using FilterType = filter_at<Descriptor, Is>;
            // Use extractMemberValue to support both data members and member functions
            const auto value = detail::extractMemberValue<FilterType::entity_ptr>(entity);

            // Handle optional vs non-optional entity members
            if constexpr (FilterType::is_optional_member) {
                return value;  // Already optional
            } else {
                return std::make_optional(value);
            }
        }()), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    return tags;
}

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_GENERATEDFILTERS_H
