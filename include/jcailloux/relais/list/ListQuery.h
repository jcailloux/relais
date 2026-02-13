#ifndef CODIBOT_LISTQUERY_H
#define CODIBOT_LISTQUERY_H

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <glaze/glaze.hpp>

namespace jcailloux::drogon::cache::list {

// =============================================================================
// SortDirection - Ascending or Descending
// =============================================================================

enum class SortDirection : uint8_t {
    Asc,
    Desc
};

// =============================================================================
// SortSpec - Sort field and direction
// =============================================================================

template<typename SortFieldEnum>
struct SortSpec {
    SortFieldEnum field;
    SortDirection direction;

    bool operator==(const SortSpec&) const = default;
};

// =============================================================================
// Cursor - Opaque pagination token for keyset pagination
// =============================================================================

struct Cursor {
    std::vector<std::byte> data;

    [[nodiscard]] bool empty() const noexcept { return data.empty(); }
    [[nodiscard]] size_t size() const noexcept { return data.size(); }

    [[nodiscard]] std::string encode() const {
        if (data.empty()) return "";
        // Base64 encoding
        static constexpr char alphabet[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string result;
        result.reserve((data.size() + 2) / 3 * 4);

        size_t i = 0;
        while (i < data.size()) {
            uint32_t octet_a = static_cast<uint8_t>(data[i++]);
            uint32_t octet_b = i < data.size() ? static_cast<uint8_t>(data[i++]) : 0;
            uint32_t octet_c = i < data.size() ? static_cast<uint8_t>(data[i++]) : 0;

            uint32_t triple = (octet_a << 16) | (octet_b << 8) | octet_c;

            result += alphabet[(triple >> 18) & 0x3F];
            result += alphabet[(triple >> 12) & 0x3F];
            result += (i > data.size() + 1) ? '=' : alphabet[(triple >> 6) & 0x3F];
            result += (i > data.size()) ? '=' : alphabet[triple & 0x3F];
        }
        return result;
    }

    static std::optional<Cursor> decode(std::string_view token) {
        if (token.empty()) return Cursor{};

        static constexpr uint8_t decode_table[256] = {
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,62,64,64,64,63,
            52,53,54,55,56,57,58,59,60,61,64,64,64,64,64,64,
            64, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
            15,16,17,18,19,20,21,22,23,24,25,64,64,64,64,64,
            64,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
            41,42,43,44,45,46,47,48,49,50,51,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
            64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64
        };

        // Remove padding and validate
        size_t len = token.size();
        while (len > 0 && token[len - 1] == '=') --len;

        Cursor result;
        result.data.reserve(len * 3 / 4);

        uint32_t buffer = 0;
        int bits = 0;

        for (size_t i = 0; i < len; ++i) {
            uint8_t c = decode_table[static_cast<uint8_t>(token[i])];
            if (c == 64) return std::nullopt;  // Invalid character

            buffer = (buffer << 6) | c;
            bits += 6;

            if (bits >= 8) {
                bits -= 8;
                result.data.push_back(static_cast<std::byte>((buffer >> bits) & 0xFF));
            }
        }

        return result;
    }

    bool operator==(const Cursor&) const = default;
};

// =============================================================================
// ListQuery - Complete list query representation
// =============================================================================

/**
 * ListQuery holds both the structured query parameters and a pre-computed hash.
 *
 * The hash is computed from the raw HTTP query string by QueryHashFilter
 * using XXH3 for maximum performance. This avoids re-hashing on every cache
 * lookup since the hash is computed once at request entry.
 *
 * Usage:
 *   // In controller, after QueryHashFilter has run:
 *   auto query_hash = req->attributes()->get<size_t>("query_hash");
 *   ListQuery<Filters, SortField> query{
 *       .filters = parseFilters(req),
 *       .sort = parseSort(req),
 *       .limit = parseLimit(req),
 *       .cursor = parseCursor(req),
 *       .query_hash = query_hash
 *   };
 */
template<typename FilterSet, typename SortFieldEnum>
struct ListQuery {
    using Filters = FilterSet;
    using SortField = SortFieldEnum;
    using Sort = SortSpec<SortFieldEnum>;

    FilterSet filters;
    std::optional<Sort> sort;
    uint16_t limit{20};
    Cursor cursor;
    size_t query_hash{0};  ///< Pre-computed XXH3 hash from QueryHashFilter

    /// Returns the pre-computed hash (from HTTP query string)
    [[nodiscard]] size_t hash() const noexcept { return query_hash; }

    [[nodiscard]] std::shared_ptr<const std::string> toJson() const {
        auto buffer = std::make_shared<std::string>();
        glz::write_json(*this, *buffer);
        return buffer;
    }

    static std::optional<ListQuery> fromJson(std::string_view json) {
        ListQuery result;
        if (glz::read_json(result, json)) {
            return std::nullopt;
        }
        return result;
    }

    bool operator==(const ListQuery&) const = default;
};

// =============================================================================
// CachedListResult - Result stored in cache
// =============================================================================

template<typename Entity>
struct CachedListResult {
    using EntityPtr = std::shared_ptr<const Entity>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    std::vector<EntityPtr> items;
    std::optional<Cursor> next_cursor;  // nullopt if no more pages
    TimePoint cached_at;

    [[nodiscard]] bool empty() const noexcept { return items.empty(); }
    [[nodiscard]] size_t size() const noexcept { return items.size(); }
};

}  // namespace jcailloux::drogon::cache::list

// =============================================================================
// Glaze metadata for serialization
// =============================================================================

template<>
struct glz::meta<jcailloux::drogon::cache::list::SortDirection> {
    using enum jcailloux::drogon::cache::list::SortDirection;
    static constexpr auto value = enumerate(Asc, Desc);
};

template<>
struct glz::meta<jcailloux::drogon::cache::list::Cursor> {
    static constexpr auto value = object(
        "data", [](auto& self) -> auto& {
            // Serialize as base64 string
            static thread_local std::string encoded;
            encoded = self.encode();
            return encoded;
        }
    );
};

#endif  // CODIBOT_LISTQUERY_H
