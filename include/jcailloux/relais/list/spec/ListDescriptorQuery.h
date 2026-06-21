#ifndef JCX_RELAIS_LIST_SPEC_LISTDESCRIPTORQUERY_H
#define JCX_RELAIS_LIST_SPEC_LISTDESCRIPTORQUERY_H

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "GeneratedFilters.h"
#include "jcailloux/relais/list/ListQuery.h"

namespace jcailloux::relais::list::spec {

// =============================================================================
// ListQueryParams / ListQuery — type-state for the declarative list system
//
// Two types, one invariant: a query passed to query() ALWAYS carries cache
// keys that reflect its contents.
//
//   ListQueryParams<D> — mutable form (filters/sort/limit/cursor/offset). No
//                        keys. This is what you fill, by hand or via the builder.
//   ListQuery<D>       — sealed, immutable form. Carries group_key + cache_key,
//                        produced ONLY by seal() (declared here, defined in
//                        CanonicalEncoding.h) or Builder::build(). The sole type
//                        query()/queryJson()/queryBinary() accept.
//
// seal() computes both keys once from the FINAL params, then the type is
// immutable — the "cache_key that doesn't reflect the query" bug is
// inexpressible, and there is no empty-key branch on the hot path.
// =============================================================================

template<typename Descriptor>
using DescriptorSortSpec = list::SortSpec<size_t>;  // Use index instead of enum

template<typename Descriptor>
struct ListQueryParams {
    Filters<Descriptor> filters;
    std::optional<DescriptorSortSpec<Descriptor>> sort;
    uint16_t limit{20};
    list::Cursor cursor;
    uint32_t offset{0};      ///< Offset for traditional offset+limit pagination

    bool operator==(const ListQueryParams&) const = default;
};

// Forward declarations so ListQuery can befriend seal() by name.
template<typename Descriptor>
class ListQuery;

template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
ListQuery<Descriptor> seal(ListQueryParams<Descriptor> params);

/// Sealed, immutable list query. Constructible only via seal() — every
/// instance carries cache keys consistent with its params by construction.
template<typename Descriptor>
class ListQuery {
public:
    ListQuery() = delete;

    [[nodiscard]] const Filters<Descriptor>& filters() const noexcept { return params_.filters; }
    [[nodiscard]] const std::optional<DescriptorSortSpec<Descriptor>>& sort() const noexcept { return params_.sort; }
    [[nodiscard]] uint16_t limit() const noexcept { return params_.limit; }
    [[nodiscard]] const list::Cursor& cursor() const noexcept { return params_.cursor; }
    [[nodiscard]] uint32_t offset() const noexcept { return params_.offset; }
    [[nodiscard]] const ListQueryParams<Descriptor>& params() const noexcept { return params_; }

    [[nodiscard]] const std::string& groupKey() const noexcept { return group_key_; }
    [[nodiscard]] const std::string& cacheKey() const noexcept { return cache_key_; }

    bool operator==(const ListQuery&) const = default;

private:
    ListQuery(ListQueryParams<Descriptor> params, std::string group_key, std::string cache_key)
        : params_(std::move(params)),
          group_key_(std::move(group_key)),
          cache_key_(std::move(cache_key)) {}

    friend ListQuery<Descriptor> seal<Descriptor>(ListQueryParams<Descriptor>);

    ListQueryParams<Descriptor> params_;
    std::string group_key_;   ///< Canonical key for filters+sort (Redis group tracking)
    std::string cache_key_;   ///< Full canonical key: group_key + limit + cursor + offset
};

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_LISTDESCRIPTORQUERY_H
