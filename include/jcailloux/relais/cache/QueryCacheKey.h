#ifndef JCX_RELAIS_QUERY_CACHE_KEY_H
#define JCX_RELAIS_QUERY_CACHE_KEY_H

#include <array>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <xxhash.h>

namespace jcailloux::relais::cache {

// =============================================================================
// HashBuffer - Stack buffer for canonical hash building
// =============================================================================

/**
 * Fixed-size stack buffer for building canonical hash input.
 * Avoids heap allocation and provides safe append operations.
 */
class HashBuffer {
public:
    static constexpr size_t CAPACITY = 1024;

    /// Append presence flag (0 = absent, 1 = present)
    void appendFlag(bool present) noexcept {
        if (pos_ < CAPACITY) {
            buf_[pos_++] = present ? 1 : 0;
        }
    }

    /// Append raw bytes
    void append(const void* data, size_t len) noexcept {
        if (pos_ + len <= CAPACITY) {
            std::memcpy(buf_ + pos_, data, len);
            pos_ += len;
        }
    }

    /// Append string with length prefix (prevents collision)
    void appendString(std::string_view str) noexcept {
        const auto len = static_cast<uint16_t>(str.size() > 0xFFFF ? 0xFFFF : str.size());
        append(&len, sizeof(len));
        if (len > 0 && pos_ + len <= CAPACITY) {
            std::memcpy(buf_ + pos_, str.data(), len);
            pos_ += len;
        }
    }

    /// Append optional string
    void appendOptional(const std::optional<std::string>& opt) noexcept {
        appendFlag(opt.has_value());
        if (opt) {
            appendString(*opt);
        }
    }

    /// Append optional int64
    void appendOptional(const std::optional<int64_t>& opt) noexcept {
        appendFlag(opt.has_value());
        if (opt) {
            append(&*opt, sizeof(*opt));
        }
    }

    /// Append optional int32
    void appendOptional(const std::optional<int32_t>& opt) noexcept {
        appendFlag(opt.has_value());
        if (opt) {
            append(&*opt, sizeof(*opt));
        }
    }

    /// Append int
    void appendInt(int value) noexcept {
        append(&value, sizeof(value));
    }

    /// Append int64
    void appendInt64(int64_t value) noexcept {
        append(&value, sizeof(value));
    }

    /// Compute XXH3-64 hash of buffer contents
    [[nodiscard]] size_t hash() const noexcept {
        return XXH3_64bits(buf_, pos_);
    }

    [[nodiscard]] size_t size() const noexcept { return pos_; }

private:
    alignas(8) char buf_[CAPACITY]{};
    size_t pos_ = 0;
};

// =============================================================================
// Concepts for hashable filter structs
// =============================================================================

/**
 * Detect if T has a static hashFields tuple of member pointers.
 */
template<typename T>
concept HasHashFields = requires {
    { T::hashFields } -> std::convertible_to<decltype(T::hashFields)>;
};

/**
 * Detect if T has a custom appendToHash method.
 */
template<typename T>
concept HasAppendToHash = requires(const T& filters, HashBuffer& buf) {
    { filters.appendToHash(buf) } noexcept;
};

/**
 * A Filters struct must implement EITHER:
 * - static constexpr auto hashFields = std::tuple{&T::field1, &T::field2, ...}
 *   Fields listed in FIXED ORDER for canonical hash (any consistent order works)
 * - void appendToHash(HashBuffer& buf) const noexcept
 *   Manual implementation (legacy)
 */
template<typename T>
concept HashableFilters = HasHashFields<T> || HasAppendToHash<T>;

// =============================================================================
// Tuple-based automatic hashing
// =============================================================================

namespace detail {

/// Append a single value to HashBuffer (non-optional overloads)
template<typename T>
void appendValue(HashBuffer& buf, const T& value) noexcept {
    if constexpr (std::is_same_v<T, std::string>) {
        buf.appendFlag(true);
        buf.appendString(value);
    } else if constexpr (std::is_integral_v<T>) {
        buf.appendFlag(true);
        buf.append(&value, sizeof(value));
    }
}

/// Append an optional value to HashBuffer
template<typename T>
void appendValue(HashBuffer& buf, const std::optional<T>& opt) noexcept {
    buf.appendOptional(opt);
}

/// Iterate over hashFields tuple and append each field value
template<typename T, typename Tuple, size_t... Is>
void appendFieldsImpl(HashBuffer& buf, const T& obj, const Tuple& fields,
                      std::index_sequence<Is...>) noexcept {
    // Fold expression: append each field in tuple order
    (appendValue(buf, obj.*std::get<Is>(fields)), ...);
}

/// Entry point for tuple-based hashing
template<HasHashFields T>
void appendFields(HashBuffer& buf, const T& obj) noexcept {
    constexpr auto& fields = T::hashFields;
    constexpr auto size = std::tuple_size_v<std::remove_cvref_t<decltype(fields)>>;
    appendFieldsImpl(buf, obj, fields, std::make_index_sequence<size>{});
}

}  // namespace detail

/// Append filters to HashBuffer using either hashFields or appendToHash
template<HashableFilters T>
void appendFiltersToHash(HashBuffer& buf, const T& filters) noexcept {
    if constexpr (HasHashFields<T>) {
        detail::appendFields(buf, filters);
    } else {
        filters.appendToHash(buf);
    }
}

// =============================================================================
// SortParam - Validated sort parameter
// =============================================================================

struct SortParam {
    std::string value = "created_at:desc";

    /// Check if sort value is in whitelist
    template<size_t N>
    [[nodiscard]] static bool isValid(std::string_view sort,
                                       const std::array<std::string_view, N>& whitelist) noexcept {
        for (const auto& valid : whitelist) {
            if (sort == valid) return true;
        }
        return false;
    }
};

// =============================================================================
// QueryCacheKey<Filters> - Generic cache key with custom filters
// =============================================================================

/**
 * Template cache key supporting custom filter structures per endpoint.
 *
 * Usage (recommended - hashFields tuple):
 *   struct MessageFilters {
 *       std::optional<int64_t> user_id;
 *       std::optional<std::string> category;
 *
 *       // Fields in FIXED ORDER for canonical hash
 *       static constexpr auto hashFields = std::tuple{
 *           &MessageFilters::category,
 *           &MessageFilters::user_id
 *       };
 *   };
 *
 * Usage (legacy - appendToHash method):
 *   struct MessageFilters {
 *       std::optional<int64_t> user_id;
 *       std::optional<std::string> category;
 *
 *       void appendToHash(HashBuffer& buf) const noexcept {
 *           buf.appendOptional(category);  // alphabetical!
 *           buf.appendOptional(user_id);
 *       }
 *   };
 *
 * In controller:
 *   QueryCacheKey<MessageFilters> key;
 *   key.filters.user_id = 123;
 *   size_t h = key.hash();
 */
template<HashableFilters Filters>
struct QueryCacheKey {
    // ===== Custom filters (INCLUDED in hash) =====
    Filters filters{};

    // ===== Common params (INCLUDED in hash) =====
    SortParam sort{};
    int limit = 50;  // Clamped [1, 100]

    // ===== Pagination (NOT included in hash) =====
    std::optional<int64_t> after_cursor;
    std::optional<int64_t> before_cursor;

    /**
     * Compute canonical XXH3-64 hash.
     * Hash is computed in fixed order: filters -> limit -> sort
     * Canonicity is guaranteed by the fixed hashFields tuple order.
     */
    [[nodiscard]] size_t hash() const noexcept {
        HashBuffer buf;

        // 1. Filters (alphabetical order enforced by hashFields or appendToHash)
        appendFiltersToHash(buf, filters);

        // 2. Limit (always present)
        buf.appendInt(limit);

        // 3. Sort (always present)
        buf.appendString(sort.value);

        // NOT hashed: after_cursor, before_cursor

        return buf.hash();
    }

    bool operator==(const QueryCacheKey&) const = default;
};

// =============================================================================
// Pre-defined filter structs for common use cases
// =============================================================================

/**
 * Empty filters - for endpoints with only sort/limit/pagination.
 */
struct NoFilters {
    static constexpr auto hashFields = std::tuple{};
};

/**
 * Generic filters with user_id and category (common pattern).
 */
struct GenericFilters {
    std::optional<int64_t> user_id;
    std::optional<std::string> category;
    std::optional<int64_t> date_from;
    std::optional<int64_t> date_to;

    // Fields in FIXED ORDER for canonical hash (any consistent order works)
    static constexpr auto hashFields = std::tuple{
        &GenericFilters::category,
        &GenericFilters::date_from,
        &GenericFilters::date_to,
        &GenericFilters::user_id
    };

    bool operator==(const GenericFilters&) const = default;
};

// Type alias for backwards compatibility
using DefaultQueryCacheKey = QueryCacheKey<GenericFilters>;

}  // namespace jcailloux::relais::cache

// std::hash specialization
template<jcailloux::relais::cache::HashableFilters Filters>
struct std::hash<jcailloux::relais::cache::QueryCacheKey<Filters>> {
    size_t operator()(const jcailloux::relais::cache::QueryCacheKey<Filters>& key) const noexcept {
        return key.hash();
    }
};

#endif  // JCX_RELAIS_QUERY_CACHE_KEY_H
