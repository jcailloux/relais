#ifndef CODIBOT_CACHE_LIST_DECL_LISTDESCRIPTOR_H
#define CODIBOT_CACHE_LIST_DECL_LISTDESCRIPTOR_H

#include <concepts>
#include <tuple>
#include <type_traits>

#include "jcailloux/relais/config/repository_config.h"
#include "jcailloux/relais/wrapper/EntityConcepts.h"

namespace jcailloux::relais::cache::list::decl {

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

/// Combined concept for a valid list descriptor
template<typename Descriptor>
concept ValidListDescriptor =
    HasEntity<Descriptor> &&
    HasFilters<Descriptor> &&
    HasSorts<Descriptor> &&
    relais::Readable<typename Descriptor::Entity>;

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

}  // namespace jcailloux::relais::cache::list::decl

#endif  // CODIBOT_CACHE_LIST_DECL_LISTDESCRIPTOR_H
