#ifndef CODIBOT_CACHE_LIST_DECL_HTTPQUERYPARSER_H
#define CODIBOT_CACHE_LIST_DECL_HTTPQUERYPARSER_H

#include <drogon/HttpRequest.h>
#include <xxhash.h>
#include <expected>

#include "FilterDescriptor.h"
#include "SortDescriptor.h"
#include "ListDescriptor.h"
#include "GeneratedFilters.h"
#include "GeneratedTraits.h"
#include "ListDescriptorQuery.h"
#include "jcailloux/drogon/cache/QueryParser.h"

namespace jcailloux::drogon::cache::list::decl {

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
        return cache::parse::toInt64(str);
    } else if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int>) {
        return cache::parse::toInt(str);
    } else if constexpr (std::is_same_v<T, std::string>) {
        if (cache::parse::isSafeLength(str)) {
            return str;
        }
        return std::nullopt;
    } else if constexpr (std::is_same_v<T, trantor::Date>) {
        return trantor::Date(cache::parse::toInt64(str));
    } else if constexpr (std::is_enum_v<T>) {
        // Use ADL to find parseEnum
        return parseEnum(str, static_cast<T*>(nullptr));
    } else {
        return std::nullopt;
    }
}

}  // namespace detail

// =============================================================================
// Query Hash Computation - Hash from parsed values, not raw query string
// =============================================================================

/// Compute a canonical hash from parsed query values
/// This ensures that queries with the same effective parameters produce the same hash,
/// regardless of parameter order or invalid parameters in the original request.
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
size_t computeQueryHash(const ListDescriptorQuery<Descriptor>& query) {
    std::vector<uint8_t> buf;
    buf.reserve(128);  // Pre-allocate reasonable size

    // Hash each filter value in declaration order
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            const auto& filter_value = query.filters.template get<Is>();
            detail::appendOptional(buf, filter_value);
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    // Hash sort specification
    uint8_t has_sort = query.sort.has_value() ? 1 : 0;
    buf.push_back(has_sort);
    if (query.sort) {
        detail::appendToBuffer(buf, query.sort->field);
        uint8_t dir = static_cast<uint8_t>(query.sort->direction);
        buf.push_back(dir);
    }

    // Hash limit
    detail::appendToBuffer(buf, query.limit);

    // Hash cursor
    if (!query.cursor.data.empty()) {
        uint32_t cursor_len = static_cast<uint32_t>(query.cursor.data.size());
        detail::appendToBuffer(buf, cursor_len);
        buf.insert(buf.end(),
            reinterpret_cast<const uint8_t*>(query.cursor.data.data()),
            reinterpret_cast<const uint8_t*>(query.cursor.data.data()) + query.cursor.data.size());
    }

    return XXH3_64bits(buf.data(), buf.size());
}

/// Parse ListQuery from HTTP request parameters
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
ListDescriptorQuery<Descriptor> parseListQuery(const ::drogon::HttpRequestPtr& req) {
    using Query = ListDescriptorQuery<Descriptor>;

    const auto& params = req->getParameters();
    Query query;

    // Parse each filter by iterating over the declaration
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            auto name = std::string(FilterType::name.view());

            if (auto it = params.find(name); it != params.end()) {
                using ValueType = typename FilterType::value_type;
                if (auto val = detail::parseValue<ValueType>(it->second)) {
                    query.filters.template get<Is>() = std::move(*val);
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
                cache::list::SortDirection dir =
                    (dir_str == "asc") ? cache::list::SortDirection::Asc
                                       : cache::list::SortDirection::Desc;
                query.sort = cache::list::SortSpec<size_t>{*field, dir};
            }
        }
    }

    // Parse limit
    if (auto it = params.find("limit"); it != params.end()) {
        query.limit = normalizeLimit<Descriptor>(
            static_cast<uint16_t>(cache::parse::toInt(it->second)));
    }

    // Parse cursor
    if (auto it = params.find("after"); it != params.end()) {
        if (auto cursor = cache::list::Cursor::decode(it->second)) {
            query.cursor = std::move(*cursor);
        }
    }

    // Compute hash from parsed values (not raw query string)
    query.query_hash = computeQueryHash<Descriptor>(query);

    return query;
}

// =============================================================================
// Strict Query Parser - Validates all parameters
// =============================================================================

/// Parse and validate ListQuery from HTTP request parameters
/// Returns error if any parameter is invalid (unknown filter, invalid sort, bad limit)
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
std::expected<ListDescriptorQuery<Descriptor>, QueryValidationError> parseListQueryStrict(
    const ::drogon::HttpRequestPtr& req
) {
    using Query = ListDescriptorQuery<Descriptor>;

    const auto& params = req->getParameters();
    Query query;

    // Collect declared filter names for validation
    std::vector<std::string_view> declared_filters;
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((declared_filters.push_back(filter_at<Descriptor, Is>::name.view())), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    // Parse and validate each parameter
    for (const auto& [key, value] : params) {
        // Skip known non-filter parameters
        if (key == "sort" || key == "limit" || key == "after" || key == "cursor") {
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
                using ValueType = typename FilterType::value_type;
                if (auto val = detail::parseValue<ValueType>(it->second)) {
                    query.filters.template get<Is>() = std::move(*val);
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

        cache::list::SortDirection dir =
            (dir_str == "asc") ? cache::list::SortDirection::Asc
                               : cache::list::SortDirection::Desc;
        query.sort = cache::list::SortSpec<size_t>{*field, dir};
    }

    // Parse and validate limit
    if (auto it = params.find("limit"); it != params.end()) {
        auto parsed_limit = static_cast<uint16_t>(cache::parse::toInt(it->second));

        if (!isLimitAllowed<Descriptor>(parsed_limit)) {
            return std::unexpected(QueryValidationError{
                .type = QueryValidationError::Type::InvalidLimit,
                .field = {},
                .limit = parsed_limit
            });
        }

        query.limit = parsed_limit;
    }

    // Parse cursor (no validation needed, just decoding)
    if (auto it = params.find("after"); it != params.end()) {
        if (auto cursor = cache::list::Cursor::decode(it->second)) {
            query.cursor = std::move(*cursor);
        }
    }

    // Compute hash from parsed values (not raw query string)
    query.query_hash = computeQueryHash<Descriptor>(query);

    return query;
}

}  // namespace jcailloux::drogon::cache::list::decl

#endif  // CODIBOT_CACHE_LIST_DECL_HTTPQUERYPARSER_H
