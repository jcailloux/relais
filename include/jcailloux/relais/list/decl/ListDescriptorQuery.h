#ifndef JCX_RELAIS_LIST_DECL_LISTDESCRIPTORQUERY_H
#define JCX_RELAIS_LIST_DECL_LISTDESCRIPTORQUERY_H

#include <cstdint>
#include <optional>
#include <string>

#include "GeneratedFilters.h"
#include "jcailloux/relais/list/ListQuery.h"

namespace jcailloux::relais::cache::list::decl {

// =============================================================================
// ListDescriptorQuery - Query type for the declarative list system
// =============================================================================

template<typename Descriptor>
using DescriptorSortSpec = cache::list::SortSpec<size_t>;  // Use index instead of enum

template<typename Descriptor>
struct ListDescriptorQuery {
    Filters<Descriptor> filters;
    std::optional<DescriptorSortSpec<Descriptor>> sort;
    uint16_t limit{20};
    cache::list::Cursor cursor;
    std::string group_key;   ///< Canonical key for filters+sort (Redis group tracking)
    std::string cache_key;   ///< Full canonical key: group_key + limit + cursor

    [[nodiscard]] const std::string& cacheKey() const noexcept { return cache_key; }

    bool operator==(const ListDescriptorQuery&) const = default;
};

}  // namespace jcailloux::relais::cache::list::decl

#endif  // JCX_RELAIS_LIST_DECL_LISTDESCRIPTORQUERY_H
