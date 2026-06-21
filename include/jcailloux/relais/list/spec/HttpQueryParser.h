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
#include "CanonicalEncoding.h"
#include "ParseUtils.h"

namespace jcailloux::relais::list::spec {

// =============================================================================
// HTTP Query Parser - Auto-parse filters from HTTP request
// =============================================================================

namespace detail {

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
// HTTP Query Parser - Auto-parse filters from HTTP request
//
// Canonical encoders (groupKey/cacheKey/encodeEntityFilterBlob/filterSchema)
// live in CanonicalEncoding.h — this header is the HTTP adapter only.
// =============================================================================

/// Parse ListQuery from a parameter map (e.g. req->getParameters())
template<typename Descriptor, typename Map = std::unordered_map<std::string, std::string>>
    requires ValidListDescriptor<Descriptor>
ListQuery<Descriptor> parseListQuery(const Map& params) {
    ListQueryParams<Descriptor> query;

    // Parse each filter by iterating over the declaration
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            auto name = std::string(FilterType::name.view());

            if (auto it = params.find(name); it != params.end()) {
                if constexpr (FilterType::is_set_op) {
                    // Set op (IN/NIN): comma-separated set; empty (no valid element)
                    // leaves the filter inactive → list stays unfiltered. For NIN
                    // this coincides with the intended "NOT IN {} = universe"
                    // (§1.2): inactive ≡ unfiltered ≡ universe. No HTTP empty-set.
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

    // Seal: compute both canonical keys once, return the immutable query.
    return seal<Descriptor>(std::move(query));
}

// =============================================================================
// Strict Query Parser - Validates all parameters
// =============================================================================

/// Parse and validate ListQuery from HTTP request parameters
/// Returns error if any parameter is invalid (unknown filter, invalid sort, bad limit)
template<typename Descriptor, typename Map = std::unordered_map<std::string, std::string>>
    requires ValidListDescriptor<Descriptor>
std::expected<ListQuery<Descriptor>, QueryValidationError> parseListQueryStrict(
    const Map& params
) {
    ListQueryParams<Descriptor> query;

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
                if constexpr (FilterType::is_set_op) {
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

    // Seal: compute both canonical keys once, return the immutable query.
    return seal<Descriptor>(std::move(query));
}

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_HTTPQUERYPARSER_H
