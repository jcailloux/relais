#ifndef JCX_RELAIS_LIST_SPEC_CANONICALENCODING_H
#define JCX_RELAIS_LIST_SPEC_CANONICALENCODING_H

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "FilterDescriptor.h"
#include "SortDescriptor.h"
#include "ListDescriptor.h"
#include "GeneratedFilters.h"
#include "ListDescriptorQuery.h"

// =============================================================================
// CanonicalEncoding — parser-agnostic binary encoders for the list cache.
//
// These produce the cache identity (group_key / cache_key) and the blob/schema
// consumed by the Lua `fmatch` matcher. They are pure mechanics over `Filters`
// (+ optional sort / pagination) — no HTTP, no string parsing. The HTTP adapter
// (HttpQueryParser.h) and the programmatic predicate builder (eraseWhere) both
// build a `Filters` and feed it to the same core here.
// =============================================================================

namespace jcailloux::relais::list::spec {

namespace detail {

/// Append a value to a hash buffer
template<typename T>
void appendToBuffer(std::vector<uint8_t>& buf, const T& value) {
    if constexpr (std::is_arithmetic_v<T>) {
        const auto* ptr = reinterpret_cast<const uint8_t*>(&value);
        buf.insert(buf.end(), ptr, ptr + sizeof(value));
    } else if constexpr (std::is_same_v<T, std::string>) {
        // Append length + data
        uint32_t len = static_cast<uint32_t>(value.size());
        appendToBuffer(buf, len);
        buf.insert(buf.end(), value.begin(), value.end());
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        uint32_t len = static_cast<uint32_t>(value.size());
        appendToBuffer(buf, len);
        buf.insert(buf.end(), value.begin(), value.end());
    }
}

/// Append an optional value to buffer
template<typename T>
void appendOptional(std::vector<uint8_t>& buf, const std::optional<T>& opt) {
    uint8_t has_value = opt.has_value() ? 1 : 0;
    buf.push_back(has_value);
    if (opt) {
        appendToBuffer(buf, *opt);
    }
}

/// Append the filter portion of a value set to buf, in declaration order. This
/// is the byte-exact prefix shared by every group key, the predicate blob, and
/// the canonical hash — set ops emit [presence][count:u32][elem×count]
/// (sorted+unique), scalars emit [presence][value]. Extracted so groupKey and
/// encodeFilterSet (eraseWhere predicate) stay byte-identical: the Lua matcher
/// compares a group's bytes against the predicate's, so any divergence desyncs.
template<typename Descriptor>
void appendFilterSet(std::vector<uint8_t>& buf, const Filters<Descriptor>& filters) {
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            const auto& filter_value = std::get<Is>(filters.values);
            if constexpr (FilterType::is_set_op) {
                // Set op (IN/NIN): [presence][count:u32][elem×count]. Encoding is
                // byte-identical for both — only the match verdict differs (L1/L2/
                // L3), never the key. Canonicalize (sort+unique) defensively here,
                // not only in the parser, so filters built programmatically also
                // hash to a stable group_key.
                buf.push_back(filter_value.has_value() ? 1 : 0);
                if (filter_value) {
                    auto sorted = *filter_value;
                    std::sort(sorted.begin(), sorted.end());
                    sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
                    appendToBuffer(buf, static_cast<uint32_t>(sorted.size()));
                    // Pin the element type: iterating std::vector<bool> yields a
                    // proxy reference, not bool — appendToBuffer would deduce the
                    // proxy, match no branch and emit zero bytes, desyncing the
                    // group set from the (scalar) entity blob. Explicit T forces
                    // the proxy to materialize and keeps strings copy-free.
                    using ElemT = typename std::decay_t<decltype(sorted)>::value_type;
                    for (const auto& e : sorted) appendToBuffer<ElemT>(buf, e);
                }
            } else {
                appendOptional(buf, filter_value);
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});
}

}  // namespace detail

// =============================================================================
// Canonical Cache Key Computation — deterministic binary buffer from values
// =============================================================================

/// Build the group-level canonical key (filters + sort).
/// Same filters+sort = same group, regardless of pagination. Takes the filter
/// values and the optional sort directly — invalidation has no cursor/offset to
/// fabricate, so it must not be forced to build a full ListDescriptorQuery.
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
std::string groupKey(
    const Filters<Descriptor>& filters,
    const std::optional<DescriptorSortSpec<Descriptor>>& sort
) {
    std::vector<uint8_t> buf;
    buf.reserve(128);

    // Filters in declaration order (alphabetically sorted by generator)
    detail::appendFilterSet<Descriptor>(buf, filters);

    // Sort specification
    uint8_t has_sort = sort.has_value() ? 1 : 0;
    buf.push_back(has_sort);
    if (sort) {
        detail::appendToBuffer(buf, sort->field);
        uint8_t dir = static_cast<uint8_t>(sort->direction);
        buf.push_back(dir);
    }

    return std::string(reinterpret_cast<const char*>(buf.data()), buf.size());
}

/// Build the full page-level canonical key (group_key + limit + cursor).
/// Uniquely identifies a specific page within a group. Takes the precomputed
/// `group_key` plus the pagination params — pure mechanics, no key storage.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
std::string cacheKey(const std::string& group_key, const ListQueryParams<Descriptor>& params) {
    // Start from the group key
    std::string key = group_key;

    std::vector<uint8_t> buf;
    buf.reserve(32);

    // Limit
    detail::appendToBuffer(buf, params.limit);

    // Cursor — read the opaque byte token behind the descriptor tag.
    const auto& cursor_data = params.cursor.raw().data;
    if (!cursor_data.empty()) {
        uint32_t cursor_len = static_cast<uint32_t>(cursor_data.size());
        detail::appendToBuffer(buf, cursor_len);
        buf.insert(buf.end(),
            reinterpret_cast<const uint8_t*>(cursor_data.data()),
            reinterpret_cast<const uint8_t*>(cursor_data.data()) + cursor_data.size());
    }

    // Offset (mutually exclusive with cursor — cursor takes precedence)
    if (params.offset > 0 && cursor_data.empty()) {
        uint8_t offset_marker = 0x4F;  // 'O' — distinguishes from cursor data
        buf.push_back(offset_marker);
        detail::appendToBuffer(buf, params.offset);
    }

    key.append(reinterpret_cast<const char*>(buf.data()), buf.size());
    return key;
}

/// Seal a mutable params bundle into an immutable ListQuery, computing both
/// canonical keys exactly once from the final params. The sole producer of a
/// ListQuery outside the fluent builder — query() accepts nothing else.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
ListQuery<Descriptor> seal(ListQueryParams<Descriptor> params) {
    auto gk = groupKey<Descriptor>(params.filters, params.sort);
    auto ck = cacheKey<Descriptor>(gk, params);
    return ListQuery<Descriptor>(std::move(params), std::move(gk), std::move(ck));
}

// =============================================================================
// Entity Filter Blob — binary encoding of entity filter values for Lua matching
// =============================================================================

/// Encode entity filter values as a binary blob in the same format as groupKey().
/// For each filter: [0x01][value_bytes] if entity has a value, [0x00] if optional and null.
/// Lua compares this blob against the binary portion of the group key for filter matching.
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
std::string encodeEntityFilterBlob(const typename Descriptor::Entity& entity) {
    std::vector<uint8_t> buf;
    buf.reserve(64);

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            const auto value = detail::extractMemberValue<FilterType::entity_ptr>(entity);

            if constexpr (FilterType::is_optional_member) {
                detail::appendOptional(buf, value);
            } else {
                buf.push_back(0x01);
                detail::appendToBuffer(buf, value);
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    return std::string(reinterpret_cast<const char*>(buf.data()), buf.size());
}

/// Encode a predicate's filter values as the byte-exact group-key filter prefix
/// (set ops as canonical sets, scalars as [presence][value]) — NO sort suffix.
/// This is the predicate blob the Lua `pmatch` compares, position by position,
/// against each group's stored filter bytes (`bin`). It uses the SAME encoding
/// as groupKey's filter portion, so a present predicate value and a present
/// group value at the same filter are directly comparable; an absent predicate
/// value (presence 0) is a wildcard that never prunes the group.
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
std::string encodeFilterSet(const Filters<Descriptor>& filters) {
    std::vector<uint8_t> buf;
    buf.reserve(64);
    detail::appendFilterSet<Descriptor>(buf, filters);
    return std::string(reinterpret_cast<const char*>(buf.data()), buf.size());
}

/// Generate a compact filter schema string for Lua binary parsing.
/// 2 characters per filter: type char + operator char.
/// Type: 's'=string, '8'=int64_t, '4'=int32_t, '1'=bool/uint8_t.
/// Operator: '='=EQ, '!'=NE, '>'=GT, 'G'=GE, '<'=LT, 'L'=LE, '@'=IN, '#'=NIN.
template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
std::string filterSchema() {
    std::string schema;
    schema.reserve(filter_count<Descriptor> * 2);

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            using ValueType = typename FilterType::value_type;

            if constexpr (std::is_same_v<ValueType, std::string>)
                schema += 's';
            else if constexpr (sizeof(ValueType) == 8) schema += '8';
            else if constexpr (sizeof(ValueType) == 4) schema += '4';
            else schema += '1';

            constexpr Op op = FilterType::op;
            if constexpr (op == Op::EQ) schema += '=';
            else if constexpr (op == Op::NE) schema += '!';
            else if constexpr (op == Op::GT) schema += '>';
            else if constexpr (op == Op::GE) schema += 'G';
            else if constexpr (op == Op::LT) schema += '<';
            else if constexpr (op == Op::LE) schema += 'L';
            else if constexpr (op == Op::IN) schema += '@';
            else if constexpr (op == Op::NIN) schema += '#';
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    return schema;
}

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_CANONICALENCODING_H
