#ifndef JCX_DROGON_LIST_DECL_LISTDESCRIPTORQUERY_H
#define JCX_DROGON_LIST_DECL_LISTDESCRIPTORQUERY_H

#include <cstdint>
#include <optional>

#include "GeneratedFilters.h"
#include "jcailloux/drogon/list/ListQuery.h"

namespace jcailloux::drogon::cache::list::decl {

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

}  // namespace jcailloux::drogon::cache::list::decl

#endif  // JCX_DROGON_LIST_DECL_LISTDESCRIPTORQUERY_H
