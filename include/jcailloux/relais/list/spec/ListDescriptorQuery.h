#ifndef JCX_RELAIS_LIST_SPEC_LISTDESCRIPTORQUERY_H
#define JCX_RELAIS_LIST_SPEC_LISTDESCRIPTORQUERY_H

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "GeneratedFilters.h"
#include "TypedCursor.h"
#include "jcailloux/relais/list/ListQuery.h"

namespace jcailloux::relais::list::spec {

// =============================================================================
// ListQueryParams / ListQuery — type-state for the declarative list system
//
// Two types, one invariant: a query passed to query() ALWAYS carries validated,
// canonical params.
//
//   ListQueryParams<D> — mutable form (filters/sort/limit/cursor/offset). This
//                        is what you fill, by hand or via the builder.
//   ListQuery<D>       — sealed, immutable form, produced ONLY by seal()
//                        (declared here, defined in CanonicalEncoding.h) or
//                        Builder::build(). The sole type
//                        query()/queryJson()/queryBinary() accept.
//
// seal() sorts and deduplicates every IN/NIN set (filters() returns them in
// that canonical form), then the type is immutable. It stores no key: the
// canonical keys are encoded from the params where they are needed —
// groupKey()/cacheKey() compute them on demand, and a cache lookup encodes the
// page key into a reused per-thread buffer, so an L1 hit allocates nothing.
// =============================================================================

template<typename Descriptor>
using DescriptorSortSpec = list::SortSpec<size_t>;  // Use index instead of enum

template<typename Descriptor>
struct ListQueryParams {
    Filters<Descriptor> filters;
    std::optional<DescriptorSortSpec<Descriptor>> sort;
    uint16_t limit{20};
    TypedCursor<Descriptor> cursor;
    uint32_t offset{0};      ///< Offset for traditional offset+limit pagination

    bool operator==(const ListQueryParams&) const = default;
};

// Forward declarations so ListQuery can befriend seal() by name.
template<typename Descriptor>
class ListQuery;

template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
ListQuery<Descriptor> seal(ListQueryParams<Descriptor> params);

template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
void appendGroupKey(std::string& out, const ListQuery<Descriptor>& query);

template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
void appendPageKey(std::string& out, const ListQuery<Descriptor>& query);

/// Sealed, immutable list query. Constructible only via seal() — every
/// instance carries canonical params, from which its keys are encoded.
template<typename Descriptor>
class ListQuery {
public:
    ListQuery() = delete;

    [[nodiscard]] const Filters<Descriptor>& filters() const noexcept { return params_.filters; }
    [[nodiscard]] const std::optional<DescriptorSortSpec<Descriptor>>& sort() const noexcept { return params_.sort; }
    [[nodiscard]] uint16_t limit() const noexcept { return params_.limit; }
    [[nodiscard]] const TypedCursor<Descriptor>& cursor() const noexcept { return params_.cursor; }
    [[nodiscard]] uint32_t offset() const noexcept { return params_.offset; }
    [[nodiscard]] const ListQueryParams<Descriptor>& params() const noexcept { return params_; }

    /// Canonical key for filters+sort (Redis group tracking), computed on each call.
    [[nodiscard]] std::string groupKey() const {
        std::string key;
        appendGroupKey<Descriptor>(key, *this);
        return key;
    }

    /// Full canonical key: group key + limit + cursor + offset, computed on each call.
    [[nodiscard]] std::string cacheKey() const {
        std::string key;
        appendPageKey<Descriptor>(key, *this);
        return key;
    }

    /// Equal params, hence equal keys: the sets are canonical.
    bool operator==(const ListQuery&) const = default;

private:
    explicit ListQuery(ListQueryParams<Descriptor> params) : params_(std::move(params)) {}

    friend ListQuery<Descriptor> seal<Descriptor>(ListQueryParams<Descriptor>);

    ListQueryParams<Descriptor> params_;
};

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_LISTDESCRIPTORQUERY_H
