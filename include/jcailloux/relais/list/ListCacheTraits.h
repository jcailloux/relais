#ifndef CODIBOT_LISTCACHETRAITS_H
#define CODIBOT_LISTCACHETRAITS_H

#include <concepts>

#include "ListQuery.h"

namespace jcailloux::relais::cache::list {

// =============================================================================
// Concepts for ListCacheTraits requirements
// =============================================================================

template<typename Traits, typename Entity>
concept HasMatchesFilters = requires(const Entity& e, const typename Traits::Filters& f) {
    { Traits::matchesFilters(e, f) } -> std::convertible_to<bool>;
};

template<typename Traits, typename Entity>
concept HasCompare = requires(const Entity& a, const Entity& b,
                              typename Traits::SortField field,
                              SortDirection dir) {
    { Traits::compare(a, b, field, dir) } -> std::convertible_to<int>;
};

template<typename Traits, typename Entity>
concept HasCursorOperations = requires(const Entity& e, const Cursor& c,
                                        SortSpec<typename Traits::SortField> sort) {
    { Traits::extractCursor(e, sort) } -> std::convertible_to<Cursor>;
    { Traits::isBeforeOrAtCursor(e, c, sort) } -> std::convertible_to<bool>;
};

template<typename Traits, typename Entity>
concept HasExtractTags = requires(const Entity& e) {
    { Traits::extractTags(e) } -> std::convertible_to<typename Traits::FilterTags>;
};

template<typename Traits, typename Entity>
concept HasDefaultSort = requires {
    { Traits::defaultSort() } -> std::convertible_to<SortSpec<typename Traits::SortField>>;
};

template<typename Traits, typename Entity>
concept HasLimitConfig = requires {
    { Traits::limitSteps } -> std::convertible_to<const std::array<uint16_t, 4>&>;
    { Traits::maxLimit } -> std::convertible_to<uint16_t>;
    { Traits::normalizeLimit(uint16_t{}) } -> std::convertible_to<uint16_t>;
};

// Combined concept for a valid ListCacheTraits
template<typename Traits, typename Entity>
concept ValidListCacheTraits =
    HasMatchesFilters<Traits, Entity> &&
    HasCompare<Traits, Entity> &&
    HasCursorOperations<Traits, Entity> &&
    HasExtractTags<Traits, Entity> &&
    HasDefaultSort<Traits, Entity>;

// =============================================================================
// ListCacheTraits - Primary template (must be specialized per entity)
// =============================================================================

template<typename Entity>
struct ListCacheTraits {
    // User must specialize this template for each entity type
    // See example specialization below
    static_assert(sizeof(Entity) == 0,
        "ListCacheTraits must be specialized for this entity type");
};

// =============================================================================
// FilterTags - Base for tag extraction and matching
// =============================================================================

// FilterTags stores the values of filterable fields extracted from an entity.
// This allows fast O(1) comparison with query filters during invalidation checks.
//
// Each entity specialization should define its own FilterTags struct inside
// the ListCacheTraits specialization.
//
// Example:
//   struct FilterTags {
//       std::optional<int64_t> user_id;
//       std::optional<std::string> category;
//
//       bool matchesFilters(const Filters& f) const {
//           if (f.user_id && user_id != f.user_id) return false;
//           if (f.category && category != f.category) return false;
//           return true;
//       }
//   };

// =============================================================================
// Example ListCacheTraits specialization (documentation only)
// =============================================================================

/*
template<>
struct ListCacheTraits<entity::MyEntity> {
    // Enumeration of filterable fields
    enum class FilterField {
        UserId,
        Category,
        DateRange
    };

    // Enumeration of sortable fields
    enum class SortField {
        CreatedAt,  // Usually the default
        Id,
        Name
    };

    // Filter values structure
    // Note: No hash() needed - hash is pre-computed from HTTP query string by QueryHashFilter
    struct Filters {
        std::optional<int64_t> user_id;
        std::optional<std::string> category;
        std::optional<std::pair<Date, Date>> date_range;
    };

    // Tags for fast filter matching during invalidation
    struct FilterTags {
        std::optional<int64_t> user_id;
        std::optional<std::string> category;
        std::optional<Date> created_at;  // For date range checks

        bool matchesFilters(const Filters& f) const {
            if (f.user_id && user_id != f.user_id) return false;
            if (f.category && category != f.category) return false;
            if (f.date_range) {
                const auto& [start, end] = *f.date_range;
                if (!created_at || *created_at < start || *created_at > end) {
                    return false;
                }
            }
            return true;
        }
    };

    // Check if entity matches all active filters
    static bool matchesFilters(const entity::MyEntity& e, const Filters& f) {
        if (f.user_id && e.user_id != *f.user_id) return false;
        if (f.category && e.category != *f.category) return false;
        if (f.date_range) {
            const auto& [start, end] = *f.date_range;
            if (e.created_at < start || e.created_at > end) return false;
        }
        return true;
    }

    // Compare two entities for sort order
    // Returns: <0 if a comes before b, 0 if equal, >0 if a comes after b
    static int compare(const entity::MyEntity& a,
                       const entity::MyEntity& b,
                       SortField field,
                       SortDirection dir) {
        int result = 0;
        switch (field) {
            case SortField::CreatedAt:
                result = (a.created_at < b.created_at) ? -1
                       : (a.created_at > b.created_at) ? 1 : 0;
                break;
            case SortField::Id:
                result = (a.id < b.id) ? -1 : (a.id > b.id) ? 1 : 0;
                break;
            case SortField::Name:
                result = a.name.compare(b.name);
                break;
        }
        return (dir == SortDirection::Desc) ? -result : result;
    }

    // Extract cursor position from entity (for pagination)
    static Cursor extractCursor(const entity::MyEntity& e,
                                const SortSpec<SortField>& sort) {
        Cursor c;
        // Encode (sort_value, primary_key) for stable keyset pagination
        // Implementation depends on sort field type
        return c;
    }

    // Check if entity would appear at or before cursor position
    static bool isBeforeOrAtCursor(const entity::MyEntity& e,
                                   const Cursor& cursor,
                                   const SortSpec<SortField>& sort) {
        if (cursor.empty()) return true;  // No cursor = first page
        // Decode cursor and compare with entity position
        return true;  // Implementation needed
    }

    // Extract filter tags from entity (for fast invalidation checks)
    static FilterTags extractTags(const entity::MyEntity& e) {
        return FilterTags{
            .user_id = e.user_id,
            .category = e.category,
            .created_at = e.created_at
        };
    }

    // Default sort specification
    static constexpr SortSpec<SortField> defaultSort() {
        return {SortField::CreatedAt, SortDirection::Desc};
    }

    // Allowed limit values (steps)
    static constexpr std::array<uint16_t, 4> limitSteps = {5, 10, 20, 50};
    static constexpr uint16_t maxLimit = 50;

    // Normalize requested limit to nearest allowed step
    static uint16_t normalizeLimit(uint16_t requested) {
        for (auto step : limitSteps) {
            if (requested <= step) return step;
        }
        return maxLimit;
    }
};
*/

}  // namespace jcailloux::relais::cache::list

#endif  // CODIBOT_LISTCACHETRAITS_H
