#ifndef JCX_DROGON_QUERY_PARSER_H
#define JCX_DROGON_QUERY_PARSER_H

#include "jcailloux/relais/cache/QueryCacheKey.h"
#include <algorithm>
#include <array>
#include <charconv>
#include <string_view>

namespace jcailloux::drogon::cache {

// =============================================================================
// Parsing utilities
// =============================================================================

namespace parse {

/// Fast int64_t parsing using std::from_chars
[[nodiscard]] inline int64_t toInt64(std::string_view str) noexcept {
    int64_t result = 0;
    std::from_chars(str.data(), str.data() + str.size(), result);
    return result;
}

/// Fast int parsing using std::from_chars
[[nodiscard]] inline int toInt(std::string_view str) noexcept {
    int result = 0;
    std::from_chars(str.data(), str.data() + str.size(), result);
    return result;
}

/// Clamp limit to [1, 100]
[[nodiscard]] inline int clampLimit(int value) noexcept {
    return value < 1 ? 1 : (value > 100 ? 100 : value);
}

/// Maximum allowed string length for filter values (security)
inline constexpr size_t MAX_STRING_LEN = 256;

/// Check string length is within safe bounds
[[nodiscard]] inline bool isSafeLength(std::string_view str) noexcept {
    return str.size() <= MAX_STRING_LEN;
}

}  // namespace parse

// =============================================================================
// QueryParser - Template-based parser for custom filter types
// =============================================================================

/**
 * Parser for HTTP query parameters with custom filter types.
 *
 * Usage:
 *   // Define parser for your filters
 *   struct MessageQueryParser {
 *       static constexpr std::array VALID_SORTS = {
 *           std::string_view{"created_at:asc"},
 *           std::string_view{"created_at:desc"}
 *       };
 *
 *       template<typename Map>
 *       static MessageFilters parseFilters(const Map& params) noexcept {
 *           MessageFilters f;
 *           if (auto it = params.find("user_id"); it != params.end()) {
 *               f.user_id = parse::toInt64(it->second);
 *           }
 *           // ... other filters
 *           return f;
 *       }
 *   };
 *
 *   // Use with QueryParser::parse
 *   auto key = QueryParser::parse<MessageQueryParser, MessageFilters>(params);
 */
class QueryParser {
public:
    /**
     * Parse query parameters into QueryCacheKey with custom filters.
     *
     * @tparam Parser  Custom parser with VALID_SORTS and parseFilters()
     * @tparam Filters Custom filter struct implementing HashableFilters
     * @param params   Query parameters map (from Drogon)
     * @return Populated QueryCacheKey<Filters>
     */
    template<typename Parser, HashableFilters Filters, typename Map>
    [[nodiscard]] static QueryCacheKey<Filters> parse(const Map& params) noexcept {
        QueryCacheKey<Filters> key;

        // Parse custom filters
        key.filters = Parser::parseFilters(params);

        // Parse sort with validation
        if (auto it = params.find("sort"); it != params.end()) {
            if (SortParam::isValid(it->second, Parser::VALID_SORTS)) {
                key.sort.value = it->second;
            }
        }

        // Parse limit with clamping
        if (auto it = params.find("limit"); it != params.end()) {
            key.limit = parse::clampLimit(parse::toInt(it->second));
        }

        // Parse pagination cursors (NOT in hash)
        if (auto it = params.find("after"); it != params.end()) {
            key.after_cursor = parse::toInt64(it->second);
        }
        if (auto it = params.find("before"); it != params.end()) {
            key.before_cursor = parse::toInt64(it->second);
        }

        return key;
    }

    /**
     * Simple parse with GenericFilters (backwards compatible).
     */
    template<typename Map>
    [[nodiscard]] static DefaultQueryCacheKey parseGeneric(const Map& params) noexcept {
        return parse<GenericQueryParser, GenericFilters>(params);
    }

private:
    /// Parser for GenericFilters
    struct GenericQueryParser {
        static constexpr std::array VALID_SORTS = {
            std::string_view{"created_at:asc"},
            std::string_view{"created_at:desc"},
            std::string_view{"updated_at:asc"},
            std::string_view{"updated_at:desc"},
            std::string_view{"id:asc"},
            std::string_view{"id:desc"}
        };

        template<typename Map>
        static GenericFilters parseFilters(const Map& params) noexcept {
            GenericFilters f;

            if (auto it = params.find("user_id"); it != params.end()) {
                f.user_id = parse::toInt64(it->second);
            }

            if (auto it = params.find("category"); it != params.end()) {
                if (parse::isSafeLength(it->second)) {
                    f.category = it->second;
                }
            }

            if (auto it = params.find("date_from"); it != params.end()) {
                f.date_from = parse::toInt64(it->second);
            }

            if (auto it = params.find("date_to"); it != params.end()) {
                f.date_to = parse::toInt64(it->second);
            }

            return f;
        }
    };
};

}  // namespace jcailloux::drogon::cache

#endif  // JCX_DROGON_QUERY_PARSER_H
