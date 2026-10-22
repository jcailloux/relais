#ifndef JCX_RELAIS_LIST_SPEC_CANONICALENCODING_H
#define JCX_RELAIS_LIST_SPEC_CANONICALENCODING_H

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "FilterDescriptor.h"
#include "SortDescriptor.h"
#include "ListDescriptor.h"
#include "GeneratedFilters.h"
#include "ListDescriptorQuery.h"

// =============================================================================
// CanonicalEncoding — parser-agnostic binary encoders for the list cache.
//
// These produce the cache identity (group_key / cache_key). They are pure
// mechanics over `Filters` (+ optional sort / pagination) — no HTTP, no string
// parsing. The HTTP adapter (HttpQueryParser.h) and the fluent builder both
// seal their query through the same core here.
// =============================================================================

namespace jcailloux::relais::list::spec {

namespace detail {

/// Byte sink that only counts: running an encoder into it gives the exact size
/// the same encoder writes into a std::string, so the key is reserved once.
struct ByteCounter {
    size_t size = 0;
    void push_back(char) noexcept { ++size; }
    void append(const char*, size_t n) noexcept { size += n; }
};

/// Append a value to a key buffer. Every supported type emits at least one
/// byte; an unsupported type is a compile error, never an empty encoding (two
/// distinct values would otherwise share a cache key).
template<typename T, typename Out>
void appendToBuffer(Out& out, const T& value) {
    if constexpr (std::is_arithmetic_v<T>) {
        out.append(reinterpret_cast<const char*>(&value), sizeof(value));
    } else if constexpr (std::is_enum_v<T>) {
        // Same bytes as the underlying integer.
        appendToBuffer(out, static_cast<std::underlying_type_t<T>>(value));
    } else if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>) {
        // Length + data
        appendToBuffer(out, static_cast<uint32_t>(value.size()));
        out.append(value.data(), value.size());
    } else {
        static_assert(sizeof(T) == 0, "appendToBuffer: unsupported filter value type");
    }
}

/// Append an optional value: [presence][value].
template<typename T, typename Out>
void appendOptional(Out& out, const std::optional<T>& opt) {
    out.push_back(opt.has_value() ? 1 : 0);
    if (opt) {
        appendToBuffer(out, *opt);
    }
}

/// Append the filter portion of a value set, in declaration order. This is the
/// byte-exact prefix of every group key — set ops emit
/// [presence][count:u32][elem×count], scalars emit [presence][value].
/// Precondition: every set is canonical (see canonicalize()); the elements are
/// written in their stored order.
template<typename Descriptor, typename Out>
void appendFilterSet(Out& out, const Filters<Descriptor>& filters) {
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            const auto& filter_value = std::get<Is>(filters.values);
            if constexpr (FilterType::is_set_op) {
                // Set op (IN/NIN): [presence][count:u32][elem×count]. Encoding is
                // byte-identical for both — only the match verdict differs, never
                // the key.
                out.push_back(filter_value.has_value() ? 1 : 0);
                if (filter_value) {
                    // Pin the element type: iterating std::vector<bool> yields a
                    // proxy reference, not bool — appendToBuffer would deduce the
                    // proxy and hit its unsupported-type assertion. Explicit T forces
                    // the proxy to materialize and keeps strings copy-free.
                    using ElemT = typename std::decay_t<decltype(*filter_value)>::value_type;
                    appendToBuffer(out, static_cast<uint32_t>(filter_value->size()));
                    for (const auto& e : *filter_value) appendToBuffer<ElemT>(out, e);
                }
            } else {
                appendOptional(out, filter_value);
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});
}

/// Append the sort suffix of a group key: [presence][field:size_t][direction:u8].
template<typename Descriptor, typename Out>
void appendSort(Out& out, const std::optional<DescriptorSortSpec<Descriptor>>& sort) {
    out.push_back(sort.has_value() ? 1 : 0);
    if (sort) {
        appendToBuffer(out, sort->field);
        out.push_back(static_cast<char>(static_cast<uint8_t>(sort->direction)));
    }
}

/// Append the pagination suffix of a page key: [limit][cursor | offset].
template<typename Descriptor, typename Out>
void appendPagination(Out& out, const ListQueryParams<Descriptor>& params) {
    appendToBuffer(out, params.limit);

    // Cursor — the opaque byte token behind the descriptor tag: [len:u32][bytes].
    // Its width is checked at decode, so every byte keyed here is read by the
    // keyset SQL.
    const auto& cursor_data = params.cursor.raw().data;
    if (!cursor_data.empty()) {
        appendToBuffer(out, static_cast<uint32_t>(cursor_data.size()));
        out.append(reinterpret_cast<const char*>(cursor_data.data()), cursor_data.size());
    }

    // Offset (mutually exclusive with cursor — cursor takes precedence)
    if (params.offset > 0 && cursor_data.empty()) {
        out.push_back(0x4F);  // 'O' — distinguishes from cursor data
        appendToBuffer(out, params.offset);
    }
}

/// Sort and deduplicate one set in place. An already canonical set is left
/// untouched after a single scan. Moving a std::string costs a buffer copy, so
/// a small set of non-trivially-copyable elements is sorted through an index
/// array on the stack and then permuted into place with at most n-1 swaps,
/// instead of the O(n²) element moves of the insertion sort std::sort runs on
/// small ranges.
template<typename Set>
void canonicalizeSet(Set& set) {
    using T = typename Set::value_type;
    if (std::adjacent_find(set.begin(), set.end(), std::greater_equal<>{}) == set.end()) return;

    constexpr size_t kIndexedMax = 32;
    if constexpr (!std::is_trivially_copyable_v<T>) {
        if (const size_t n = set.size(); n <= kIndexedMax) {
            // from[i]: position of the element that belongs at i.
            std::array<uint8_t, kIndexedMax> from;
            for (size_t i = 0; i < n; ++i) from[i] = static_cast<uint8_t>(i);
            std::sort(from.begin(), from.begin() + n,
                      [&](uint8_t a, uint8_t b) { return set[a] < set[b]; });
            for (size_t i = 0; i < n; ++i) {
                size_t hole = i;
                while (from[hole] != i) {
                    const size_t src = from[hole];
                    std::swap(set[hole], set[src]);
                    from[hole] = static_cast<uint8_t>(hole);
                    hole = src;
                }
                from[hole] = static_cast<uint8_t>(hole);
            }
            set.erase(std::unique(set.begin(), set.end()), set.end());
            return;
        }
    }
    std::sort(set.begin(), set.end());
    set.erase(std::unique(set.begin(), set.end()), set.end());
}

/// Encode into a string sized exactly once: `encode(sink)` runs first on a
/// counter, then on the reserved string.
template<typename Encode>
std::string encodeReserved(Encode&& encode) {
    ByteCounter counter;
    encode(counter);
    std::string out;
    out.reserve(counter.size);
    encode(out);
    return out;
}

// =============================================================================
// Canonical Cache Key Computation — deterministic binary encoding from values
// =============================================================================

/// Sort and deduplicate every IN/NIN set in place. A set is a set: its order
/// and repetitions must not split one group into several keys.
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
void canonicalize(Filters<Descriptor>& filters) {
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            if constexpr (filter_at<Descriptor, Is>::is_set_op) {
                if (auto& set = std::get<Is>(filters.values)) detail::canonicalizeSet(*set);
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});
}

/// Append the group-level canonical key (filters + sort) of a sealed query to
/// `out`. Same filters+sort = same group, regardless of pagination.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
void appendGroupKey(std::string& out, const ListQuery<Descriptor>& query) {
    // Filters in declaration order (alphabetically sorted by generator)
    detail::appendFilterSet<Descriptor>(out, query.filters());
    detail::appendSort<Descriptor>(out, query.sort());
}

/// Append the full page-level canonical key (group key + limit + cursor/offset)
/// of a sealed query to `out`. Uniquely identifies a specific page within a group.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
void appendPageKey(std::string& out, const ListQuery<Descriptor>& query) {
    appendGroupKey<Descriptor>(out, query);
    detail::appendPagination<Descriptor>(out, query.params());
}

/// Build the group-level canonical key (filters + sort) from the filter values
/// and the optional sort directly, without a sealed query. Canonicalizes its own
/// copy of the sets.
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
std::string groupKey(
    Filters<Descriptor> filters,
    const std::optional<DescriptorSortSpec<Descriptor>>& sort
) {
    canonicalize<Descriptor>(filters);
    return detail::encodeReserved([&](auto& out) {
        detail::appendFilterSet<Descriptor>(out, filters);
        detail::appendSort<Descriptor>(out, sort);
    });
}

/// Build the full page-level canonical key from a `group_key` plus the
/// pagination params.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
std::string cacheKey(const std::string& group_key, const ListQueryParams<Descriptor>& params) {
    ByteCounter suffix;
    detail::appendPagination<Descriptor>(suffix, params);
    std::string key;
    key.reserve(group_key.size() + suffix.size);
    key.append(group_key);
    detail::appendPagination<Descriptor>(key, params);
    return key;
}

}  // namespace detail

/// Seal a mutable params bundle into an immutable ListQuery: canonicalizes the
/// IN/NIN sets in place, so that equal queries encode equal keys. The sole
/// producer of a ListQuery outside the fluent builder — query() accepts
/// nothing else.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
ListQuery<Descriptor> seal(ListQueryParams<Descriptor> params) {
    detail::canonicalize<Descriptor>(params.filters);
    return ListQuery<Descriptor>(std::move(params));
}

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_CANONICALENCODING_H
