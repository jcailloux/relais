#ifndef JCX_RELAIS_LIST_SPEC_LISTDESCRIPTOR_H
#define JCX_RELAIS_LIST_SPEC_LISTDESCRIPTOR_H

#include <concepts>
#include <tuple>
#include <type_traits>

#include "jcailloux/relais/entity/EntityConcepts.h"

namespace jcailloux::relais::list::spec {

// =============================================================================
// Concepts for validating list descriptors
// =============================================================================

/// Check if Descriptor has required Entity type alias
template<typename Descriptor>
concept HasEntity = requires {
    typename Descriptor::Entity;
};

/// Check if Descriptor has a filters tuple
template<typename Descriptor>
concept HasFilters = requires {
    { Descriptor::filters } -> std::convertible_to<decltype(Descriptor::filters)>;
    requires std::tuple_size_v<std::remove_cvref_t<decltype(Descriptor::filters)>> >= 0;
};

/// Check if Descriptor has a sorts tuple with at least one element
template<typename Descriptor>
concept HasSorts = requires {
    { Descriptor::sorts } -> std::convertible_to<decltype(Descriptor::sorts)>;
    requires std::tuple_size_v<std::remove_cvref_t<decltype(Descriptor::sorts)>> >= 1;
};

/// A filter set: an entity + a tuple of filters, no sort/pagination required.
/// This is the predicate-only substrate the filter core operates on (Filters,
/// FilterTags, buildWhereClause, groupKey, encodeEntityFilterBlob, extractTags).
/// A ListDescriptor is one consumer; eraseWhere/invalidateWhere is another. An
/// entity may declare filters WITHOUT a cached list and still satisfy this.
template<typename Descriptor>
concept ValidFilterSet =
    HasEntity<Descriptor> &&
    HasFilters<Descriptor> &&
    relais::Readable<typename Descriptor::Entity>;

/// A valid list descriptor is a FilterSet that ALSO declares a sort dimension
/// (+ pagination/cache). Composition, not a parallel definition: every list
/// descriptor is a filter set, so retargeting the core onto ValidFilterSet only
/// widens what it accepts — existing list call sites stay valid byte-for-byte.
template<typename Descriptor>
concept ValidListDescriptor =
    ValidFilterSet<Descriptor> &&
    HasSorts<Descriptor>;

// =============================================================================
// Helper to count filters and sorts
// =============================================================================

template<typename Descriptor>
inline constexpr size_t filter_count =
    std::tuple_size_v<std::remove_cvref_t<decltype(Descriptor::filters)>>;

template<typename Descriptor>
inline constexpr size_t sort_count =
    std::tuple_size_v<std::remove_cvref_t<decltype(Descriptor::sorts)>>;

// =============================================================================
// Helper to get filter/sort by index
// =============================================================================

template<typename Descriptor, size_t I>
using filter_at = std::tuple_element_t<I, std::remove_cvref_t<decltype(Descriptor::filters)>>;

template<typename Descriptor, size_t I>
using sort_at = std::tuple_element_t<I, std::remove_cvref_t<decltype(Descriptor::sorts)>>;

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_LISTDESCRIPTOR_H
