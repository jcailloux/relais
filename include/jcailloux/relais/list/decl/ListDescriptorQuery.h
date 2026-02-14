#ifndef JCX_RELAIS_LIST_DECL_LISTDESCRIPTORQUERY_H
#define JCX_RELAIS_LIST_DECL_LISTDESCRIPTORQUERY_H

#include <cstdint>
#include <optional>

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
    size_t query_hash{0};

    [[nodiscard]] size_t hash() const noexcept { return query_hash; }

    bool operator==(const ListDescriptorQuery&) const = default;
};

}  // namespace jcailloux::relais::cache::list::decl

#endif  // JCX_RELAIS_LIST_DECL_LISTDESCRIPTORQUERY_H
