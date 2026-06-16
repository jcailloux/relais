#ifndef JCX_RELAIS_LIST_SPEC_HTTPQUERYPARSER_H
#define JCX_RELAIS_LIST_SPEC_HTTPQUERYPARSER_H

#include <algorithm>
#include <charconv>
#include <expected>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <vector>

#include "FilterDescriptor.h"
#include "SortDescriptor.h"
#include "ListDescriptor.h"
#include "GeneratedFilters.h"
#include "GeneratedTraits.h"
#include "ListDescriptorQuery.h"
#include "ParseUtils.h"

namespace jcailloux::relais::list::spec {

// =============================================================================
// HTTP Query Parser - Auto-parse filters from HTTP request
// =============================================================================

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

/// Parse a single filter value from string based on its type
template<typename T>
std::optional<T> parseValue(const std::string& str) {
    if constexpr (std::is_same_v<T, int64_t>) {
        return jcailloux::relais::list::spec::parse::toInt64(str);
    } else if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int>) {
        return jcailloux::relais::list::spec::parse::toInt(str);
    } else if constexpr (std::is_same_v<T, bool>) {
        return jcailloux::relais::list::spec::parse::toBool(str);
    } else if constexpr (std::is_same_v<T, std::string>) {
        if (jcailloux::relais::list::spec::parse::isSafeLength(str)) {
            return str;
        }
        return std::nullopt;
    } else if constexpr (std::is_enum_v<T>) {
        // Use ADL to find parseEnum
        return parseEnum(str, static_cast<T*>(nullptr));
    } else {
        return std::nullopt;
    }
}

/// Hard cap on IN-set cardinality parsed from a query string (abuse mitigation).
inline constexpr size_t kMaxInListElements = 256;

/// Parse one IN-set element with STRICT validation. Unlike scalar parseValue
/// (which maps a malformed int to 0 via parse::toInt64), an integral element is
/// accepted only if from_chars consumes the whole token — otherwise a junk token
/// like "abc" would pollute the set with a spurious 0. Non-integral types fall
/// back to parseValue: bool via parse::toBool (true/1/t/yes/y/on vs false/0/f/
/// no/n/off, case-insensitive), strings via the length check.
template<typename T>
std::optional<T> parseInElement(const std::string& str) {
    if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
        T result{};
        const char* first = str.data();
        const char* last = first + str.size();
        auto [ptr, ec] = std::from_chars(first, last, result);
        if (ec != std::errc{} || ptr != last) return std::nullopt;
        return result;
    } else {
        return parseValue<T>(str);
    }
}

/// Parse a comma-separated query value into a canonical set for an IN filter:
/// split on ',' → parseInElement<T> per element (silently dropping invalid ones)
/// → sort → unique → truncate to kMaxInListElements. The truncation happens AFTER
/// sort+unique so the resulting group key is deterministic regardless of the
/// arrival order or duplicate count — `tech,science` and `science,tech,tech`
/// yield byte-identical keys.
template<typename T>
std::vector<T> parseInList(const std::string& str) {
    std::vector<T> out;
    size_t start = 0;
    while (true) {
        size_t comma = str.find(',', start);
        size_t end = (comma == std::string::npos) ? str.size() : comma;
        if (auto val = parseInElement<T>(str.substr(start, end - start))) {
            out.push_back(std::move(*val));
        }
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    if (out.size() > kMaxInListElements) {
        out.erase(out.begin() + kMaxInListElements, out.end());
    }
    return out;
}

}  // namespace detail

// =============================================================================
// Canonical Cache Key Computation — deterministic binary buffer from parsed values
// =============================================================================

/// Build the group-level canonical key (filters + sort).
/// Same filters+sort = same group, regardless of pagination.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
std::string groupCacheKey(const ListDescriptorQuery<Descriptor>& query) {
    std::vector<uint8_t> buf;
    buf.reserve(128);

    // Filters in declaration order (alphabetically sorted by generator)
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            const auto& filter_value = query.filters.template get<Is>();
            if constexpr (FilterType::op == Op::IN) {
                // IN set: [presence][count:u32][elem×count]. Canonicalize (sort+
                // unique) defensively here, not only in the parser, so filters
                // built programmatically also hash to a stable group_key.
                buf.push_back(filter_value.has_value() ? 1 : 0);
                if (filter_value) {
                    auto sorted = *filter_value;
                    std::sort(sorted.begin(), sorted.end());
                    sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
                    detail::appendToBuffer(buf, static_cast<uint32_t>(sorted.size()));
                    // Pin the element type: iterating std::vector<bool> yields a
                    // proxy reference, not bool — appendToBuffer would deduce the
                    // proxy, match no branch and emit zero bytes, desyncing the
                    // group set from the (scalar) entity blob. Explicit T forces
                    // the proxy to materialize and keeps strings copy-free.
                    using ElemT = typename std::decay_t<decltype(sorted)>::value_type;
                    for (const auto& e : sorted) detail::appendToBuffer<ElemT>(buf, e);
                }
            } else {
                detail::appendOptional(buf, filter_value);
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    // Sort specification
    uint8_t has_sort = query.sort.has_value() ? 1 : 0;
    buf.push_back(has_sort);
    if (query.sort) {
        detail::appendToBuffer(buf, query.sort->field);
        uint8_t dir = static_cast<uint8_t>(query.sort->direction);
        buf.push_back(dir);
    }

    return std::string(reinterpret_cast<const char*>(buf.data()), buf.size());
}

/// Build the full page-level canonical key (group_key + limit + cursor).
/// Uniquely identifies a specific page within a group.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
std::string cacheKey(const ListDescriptorQuery<Descriptor>& query) {
    // Start from the group key
    std::string key = query.group_key;

    std::vector<uint8_t> buf;
    buf.reserve(32);

    // Limit
    detail::appendToBuffer(buf, query.limit);

    // Cursor
    if (!query.cursor.data.empty()) {
        uint32_t cursor_len = static_cast<uint32_t>(query.cursor.data.size());
        detail::appendToBuffer(buf, cursor_len);
        buf.insert(buf.end(),
            reinterpret_cast<const uint8_t*>(query.cursor.data.data()),
            reinterpret_cast<const uint8_t*>(query.cursor.data.data()) + query.cursor.data.size());
    }

    // Offset (mutually exclusive with cursor — cursor takes precedence)
    if (query.offset > 0 && query.cursor.data.empty()) {
        uint8_t offset_marker = 0x4F;  // 'O' — distinguishes from cursor data
        buf.push_back(offset_marker);
        detail::appendToBuffer(buf, query.offset);
    }

    key.append(reinterpret_cast<const char*>(buf.data()), buf.size());
    return key;
}

// =============================================================================
// Entity Filter Blob — binary encoding of entity filter values for Lua matching
// =============================================================================

/// Encode entity filter values as a binary blob in the same format as groupCacheKey().
/// For each filter: [0x01][value_bytes] if entity has a value, [0x00] if optional and null.
/// Lua compares this blob against the binary portion of the group key for filter matching.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
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

/// Generate a compact filter schema string for Lua binary parsing.
/// 2 characters per filter: type char + operator char.
/// Type: 's'=string, '8'=int64_t, '4'=int32_t, '1'=bool/uint8_t.
/// Operator: '='=EQ, '!'=NE, '>'=GT, 'G'=GE, '<'=LT, 'L'=LE, '@'=IN.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
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
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    return schema;
}

// =============================================================================
// HTTP Query Parser - Auto-parse filters from HTTP request
// =============================================================================

/// Parse ListQuery from a parameter map (e.g. req->getParameters())
template<typename Descriptor, typename Map = std::unordered_map<std::string, std::string>>
    requires ValidListDescriptor<Descriptor>
ListDescriptorQuery<Descriptor> parseListQuery(const Map& params) {
    using Query = ListDescriptorQuery<Descriptor>;

    Query query;

    // Parse each filter by iterating over the declaration
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            auto name = std::string(FilterType::name.view());

            if (auto it = params.find(name); it != params.end()) {
                if constexpr (FilterType::op == Op::IN) {
                    // IN: comma-separated set; empty (no valid element) leaves the
                    // filter inactive → list stays unfiltered (no HTTP empty-set).
                    auto vals = detail::parseInList<typename FilterType::element_type>(it->second);
                    if (!vals.empty()) {
                        query.filters.template get<Is>() = std::move(vals);
                    }
                } else {
                    using ValueType = typename FilterType::value_type;
                    if (auto val = detail::parseValue<ValueType>(it->second)) {
                        query.filters.template get<Is>() = std::move(*val);
                    }
                }
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    // Parse sort
    if (auto it = params.find("sort"); it != params.end()) {
        size_t colon = it->second.find(':');
        if (colon != std::string::npos) {
            auto field_str = std::string_view(it->second).substr(0, colon);
            auto dir_str = std::string_view(it->second).substr(colon + 1);

            if (auto field = parseSortField<Descriptor>(field_str)) {
                list::SortDirection dir =
                    (dir_str == "asc") ? list::SortDirection::Asc
                                       : list::SortDirection::Desc;
                query.sort = list::SortSpec<size_t>{*field, dir};
            }
        }
    }

    // Parse limit
    if (auto it = params.find("limit"); it != params.end()) {
        query.limit = normalizeLimit<Descriptor>(
            static_cast<uint16_t>(jcailloux::relais::list::spec::parse::toInt(it->second)));
    } else {
        // No limit param → the descriptor's declared default page size.
        query.limit = defaultLimit<Descriptor>();
    }

    // Parse cursor
    if (auto it = params.find("after"); it != params.end()) {
        if (auto cursor = list::Cursor::decode(it->second)) {
            query.cursor = std::move(*cursor);
        }
    }

    // Parse offset (ignored if cursor is present — cursor takes precedence)
    if (query.cursor.data.empty()) {
        if (auto it = params.find("offset"); it != params.end()) {
            query.offset = static_cast<uint32_t>(jcailloux::relais::list::spec::parse::toInt(it->second));
        }
    }

    // Build canonical cache keys from parsed values
    query.group_key = groupCacheKey<Descriptor>(query);
    query.cache_key = cacheKey<Descriptor>(query);

    return query;
}

// =============================================================================
// Strict Query Parser - Validates all parameters
// =============================================================================

/// Parse and validate ListQuery from HTTP request parameters
/// Returns error if any parameter is invalid (unknown filter, invalid sort, bad limit)
template<typename Descriptor, typename Map = std::unordered_map<std::string, std::string>>
    requires ValidListDescriptor<Descriptor>
std::expected<ListDescriptorQuery<Descriptor>, QueryValidationError> parseListQueryStrict(
    const Map& params
) {
    using Query = ListDescriptorQuery<Descriptor>;

    Query query;

    // Collect declared filter names for validation
    std::vector<std::string_view> declared_filters;
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((declared_filters.push_back(filter_at<Descriptor, Is>::name.view())), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    // Parse and validate each parameter
    for (const auto& [key, value] : params) {
        // Skip known non-filter parameters
        if (key == "sort" || key == "limit" || key == "after" || key == "cursor" || key == "offset") {
            continue;
        }

        // Check if this is a declared filter
        bool is_declared = false;
        for (auto filter_name : declared_filters) {
            if (key == filter_name) {
                is_declared = true;
                break;
            }
        }

        if (!is_declared) {
            return std::unexpected(QueryValidationError{
                .type = QueryValidationError::Type::InvalidFilter,
                .field = key,
                .limit = 0
            });
        }
    }

    // Parse filters (we know they're all valid now)
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            auto name = std::string(FilterType::name.view());

            if (auto it = params.find(name); it != params.end()) {
                if constexpr (FilterType::op == Op::IN) {
                    auto vals = detail::parseInList<typename FilterType::element_type>(it->second);
                    if (!vals.empty()) {
                        query.filters.template get<Is>() = std::move(vals);
                    }
                } else {
                    using ValueType = typename FilterType::value_type;
                    if (auto val = detail::parseValue<ValueType>(it->second)) {
                        query.filters.template get<Is>() = std::move(*val);
                    }
                }
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    // Parse and validate sort
    if (auto it = params.find("sort"); it != params.end()) {
        size_t colon = it->second.find(':');
        std::string_view field_str;
        std::string_view dir_str = "desc";

        if (colon != std::string::npos) {
            field_str = std::string_view(it->second).substr(0, colon);
            dir_str = std::string_view(it->second).substr(colon + 1);
        } else {
            field_str = it->second;
        }

        auto field = parseSortField<Descriptor>(field_str);
        if (!field) {
            return std::unexpected(QueryValidationError{
                .type = QueryValidationError::Type::InvalidSort,
                .field = std::string(field_str),
                .limit = 0
            });
        }

        list::SortDirection dir =
            (dir_str == "asc") ? list::SortDirection::Asc
                               : list::SortDirection::Desc;
        query.sort = list::SortSpec<size_t>{*field, dir};
    }

    // Parse and validate limit
    if (auto it = params.find("limit"); it != params.end()) {
        auto parsed_limit = static_cast<uint16_t>(jcailloux::relais::list::spec::parse::toInt(it->second));

        if (!isLimitAllowed<Descriptor>(parsed_limit)) {
            return std::unexpected(QueryValidationError{
                .type = QueryValidationError::Type::InvalidLimit,
                .field = {},
                .limit = parsed_limit
            });
        }

        query.limit = parsed_limit;
    } else {
        // No limit param → the descriptor's declared default page size.
        query.limit = defaultLimit<Descriptor>();
    }

    // Parse cursor (no validation needed, just decoding)
    if (auto it = params.find("after"); it != params.end()) {
        if (auto cursor = list::Cursor::decode(it->second)) {
            query.cursor = std::move(*cursor);
        }
    }

    // Parse offset
    if (auto it = params.find("offset"); it != params.end()) {
        query.offset = static_cast<uint32_t>(jcailloux::relais::list::spec::parse::toInt(it->second));
    }

    // Reject conflicting pagination: cursor + offset
    if (!query.cursor.data.empty() && query.offset > 0) {
        return std::unexpected(QueryValidationError{
            .type = QueryValidationError::Type::ConflictingPagination,
            .field = {},
            .limit = 0
        });
    }

    // Build canonical cache keys from parsed values
    query.group_key = groupCacheKey<Descriptor>(query);
    query.cache_key = cacheKey<Descriptor>(query);

    return query;
}

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_HTTPQUERYPARSER_H
