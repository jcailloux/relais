#ifndef JCX_RELAIS_LIST_SPEC_LISTQUERYBUILDER_H
#define JCX_RELAIS_LIST_SPEC_LISTQUERYBUILDER_H

#include <cstdint>
#include <utility>

#include "GeneratedFilters.h"
#include "GeneratedTraits.h"
#include "ListDescriptorQuery.h"
#include "CanonicalEncoding.h"
#include "jcailloux/relais/list/ListQuery.h"
#include "jcailloux/relais/list/SortDirection.h"

namespace jcailloux::relais::list::spec {

// =============================================================================
// ListQueryBuilder — fluent construction path and the single point of sealing.
//
// Accumulates a mutable ListQueryParams<Descriptor>; build() seals it into the
// immutable ListQuery<Descriptor> — the sole type query()/queryJson()/
// queryBinary() accept. A query reaching query() therefore always carries cache
// keys consistent with its contents: there is exactly one sealing moment,
// terminal by construction, and the mutable form holds no keys to go stale.
//
// Filters and sort are set BY NAME, verified at compile time (find_filter_index
// / find_sort_index static_assert on an unknown name) — no positional index, no
// silently-wrong column on a Sort<> reorder. limit()/after()/offset() carry the
// trusted-path pagination: NO limit grid is applied (decision 3), the caller
// owns cache_key cardinality.
//
// Chaining: every setter returns `*this` by reference. Repo::queryBuilder()
// yields a prvalue; the first setter binds the materialized temporary, the rest
// chain on the returned lvalue ref, and build() seals before the full-expression
// temporary dies.
// =============================================================================

template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
class ListQueryBuilder {
public:
    ListQueryBuilder() = default;

    /// Set a filter by name. Delegates to Filters::get<Name>() so the exact slot
    /// type is assigned — optional<element> for a scalar, optional<vector<element>>
    /// for IN/NIN — without reimplementing the set-op slot shape (point #1).
    template<FixedString Name, typename V>
    ListQueryBuilder& filter(V&& value) {
        params_.filters.template get<Name>() = std::forward<V>(value);
        return *this;
    }

    /// Set the sort by field name + direction, resolved and checked at compile
    /// time (sortBy<>, decision 1). Mono-key — the PK tie-break is injected lower
    /// (keyset level).
    template<FixedString Name, SortDirection Dir = SortDirection::Asc>
    ListQueryBuilder& sortBy() {
        params_.sort = spec::sortBy<Descriptor, Name, Dir>();
        return *this;
    }

    template<FixedString Name>
    ListQueryBuilder& sortAsc() {
        return sortBy<Name, SortDirection::Asc>();
    }

    template<FixedString Name>
    ListQueryBuilder& sortDesc() {
        return sortBy<Name, SortDirection::Desc>();
    }

    /// Set a pre-resolved sort spec (e.g. from defaultSort or parseSortField).
    /// Prefer the by-name sortBy<>/sortAsc/sortDesc — this is the escape hatch
    /// for an index resolved at runtime.
    ListQueryBuilder& sort(DescriptorSortSpec<Descriptor> spec) {
        params_.sort = spec;
        return *this;
    }

    /// Exact page size — no grid normalization (trusted path, decision 3).
    ListQueryBuilder& limit(uint16_t n) {
        params_.limit = n;
        return *this;
    }

    /// Set the page cursor for keyset pagination (decision 4). The cursor is an
    /// opaque, server-minted token (decision 6): the caller decodes the token it
    /// previously emitted via list::Cursor::decode (runtime validation at the
    /// trust boundary) and passes the decoded cursor here. Mutually exclusive
    /// with offset() — the cursor wins in cacheKey().
    ListQueryBuilder& after(list::Cursor cursor) {
        params_.cursor = std::move(cursor);
        return *this;
    }

    /// Offset for traditional offset+limit pagination. Ignored once a cursor is
    /// set (cursor takes precedence in cacheKey()).
    ListQueryBuilder& offset(uint32_t n) {
        params_.offset = n;
        return *this;
    }

    /// Seal into the immutable ListQuery — the single sealing point of the fluent
    /// path. Computes both canonical keys once from the final params.
    [[nodiscard]] ListQuery<Descriptor> build() const {
        return seal<Descriptor>(params_);
    }

    /// The params accumulated so far — to inspect or seal manually.
    [[nodiscard]] const ListQueryParams<Descriptor>& params() const noexcept {
        return params_;
    }

private:
    ListQueryParams<Descriptor> params_;
};

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_LISTQUERYBUILDER_H
