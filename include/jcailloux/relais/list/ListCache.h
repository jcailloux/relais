#ifndef CODIBOT_LISTCACHE_H
#define CODIBOT_LISTCACHE_H

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <type_traits>

#include <jcailloux/shardmap/ShardMap.h>
#include "ListQuery.h"
#include "ListCacheTraits.h"
#include "ModificationTracker.h"
#include "jcailloux/relais/wrapper/ListWrapper.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais::cache::list {

// =============================================================================
// PaginationMode - Distinguishes offset-based and cursor-based pagination
// =============================================================================

enum class PaginationMode : uint8_t {
    Offset = 0,  // Traditional offset+limit (cascade invalidation for insert/delete)
    Cursor = 1   // Keyset/cursor-based (localized invalidation)
};

// =============================================================================
// ListCacheConfig - Configuration for ListCache behavior
// =============================================================================

struct ListCacheConfig {
    size_t cleanup_every_n_gets = 1000;       // Trigger cleanup every N gets
    std::chrono::seconds default_ttl{3600};   // 1 hour
    bool accept_expired_on_get = false;       // Accept expired entries on get
    bool refresh_on_get = false;              // Refresh TTL on get
};

// =============================================================================
// SortBounds - Min/max values for O(1) range checking during invalidation
// =============================================================================

struct SortBounds {
    int64_t first_value{0};   // Sort field value for first item in page
    int64_t last_value{0};    // Sort field value for last item in page
    bool is_valid{false};     // True if bounds were extracted (non-empty page)

    /// Check if a sort value falls within this page's range
    [[nodiscard]] bool isValueInRange(
        int64_t value,
        bool is_first_page,
        bool is_incomplete,
        bool is_descending
    ) const noexcept {
        if (!is_valid) {
            return true;  // Empty page or no bounds - conservatively assume in range
        }

        if (is_descending) {
            // DESC: larger values come first
            if (is_first_page && is_incomplete) return true;
            if (is_first_page) return value >= last_value;
            if (is_incomplete) return value <= first_value;
            return value <= first_value && value >= last_value;
        } else {
            // ASC: smaller values come first
            if (is_first_page && is_incomplete) return true;
            if (is_first_page) return value <= last_value;
            if (is_incomplete) return value >= first_value;
            return value >= first_value && value <= last_value;
        }
    }
};

// =============================================================================
// ListBoundsHeader - 19-byte binary header for Redis L2 list cache values
// =============================================================================
//
// Prepended to cached list values in Redis for fine-grained invalidation.
// A Lua script reads the header (via GETRANGE) to decide whether each page
// should be invalidated, avoiding unnecessary deletes.
//
// Format (little-endian):
//   Offset  Size  Field
//   0       2     Magic bytes: 0x52 0x4C ("SR" = Relais)
//   2       8     first_value (int64_t LE)
//   10      8     last_value (int64_t LE)
//   18      1     flags:
//                   bit 0: sort_direction (0=ASC, 1=DESC)
//                   bit 1: is_first_page
//                   bit 2: is_incomplete
//                   bit 3: pagination_mode (0=Offset, 1=Cursor)
//                   bits 4-7: reserved
//

static constexpr size_t kListBoundsHeaderSize = 19;
static constexpr uint8_t kListBoundsHeaderMagic[2] = {0x53, 0x52};

struct ListBoundsHeader {
    SortBounds bounds;
    PaginationMode pagination_mode{PaginationMode::Offset};
    bool is_first_page{true};
    bool is_incomplete{false};
    SortDirection sort_direction{SortDirection::Desc};

    /// Serialize the header to 19 bytes at dst (little-endian).
    void writeTo(uint8_t* dst) const noexcept {
        dst[0] = kListBoundsHeaderMagic[0];
        dst[1] = kListBoundsHeaderMagic[1];

        // first_value (little-endian int64_t)
        auto first = static_cast<uint64_t>(bounds.first_value);
        std::memcpy(dst + 2, &first, 8);

        // last_value (little-endian int64_t)
        auto last = static_cast<uint64_t>(bounds.last_value);
        std::memcpy(dst + 10, &last, 8);

        // flags byte
        uint8_t flags = 0;
        if (sort_direction == SortDirection::Desc)      flags |= 0x01;
        if (is_first_page)                              flags |= 0x02;
        if (is_incomplete)                              flags |= 0x04;
        if (pagination_mode == PaginationMode::Cursor)  flags |= 0x08;
        dst[18] = flags;
    }

    /// Read a header from raw bytes. Returns nullopt if magic is invalid.
    static std::optional<ListBoundsHeader> readFrom(const uint8_t* src, size_t len) noexcept {
        if (len < kListBoundsHeaderSize) return std::nullopt;
        if (src[0] != kListBoundsHeaderMagic[0] || src[1] != kListBoundsHeaderMagic[1]) {
            return std::nullopt;
        }

        ListBoundsHeader h;

        uint64_t first_raw, last_raw;
        std::memcpy(&first_raw, src + 2, 8);
        std::memcpy(&last_raw, src + 10, 8);
        h.bounds.first_value = static_cast<int64_t>(first_raw);
        h.bounds.last_value = static_cast<int64_t>(last_raw);
        h.bounds.is_valid = true;

        uint8_t flags = src[18];
        h.sort_direction  = (flags & 0x01) ? SortDirection::Desc : SortDirection::Asc;
        h.is_first_page   = (flags & 0x02) != 0;
        h.is_incomplete   = (flags & 0x04) != 0;
        h.pagination_mode = (flags & 0x08) ? PaginationMode::Cursor : PaginationMode::Offset;

        return h;
    }

    /// Check if a insert or delete of an entity with this sort value affects this page.
    ///
    /// - Offset mode (cascade): the segment is affected if entity_val is within or above
    ///   its range, because inserting/deleting shifts all subsequent segments.
    /// - Cursor mode (localized): only the segment whose range contains entity_val is affected.
    [[nodiscard]] bool isAffectedByCreateOrDelete(int64_t entity_val) const noexcept {
        if (!bounds.is_valid) return true;

        bool is_desc = (sort_direction == SortDirection::Desc);

        if (pagination_mode == PaginationMode::Offset) {
            // CASCADE: affected if entity_val is in or above range
            if (is_incomplete) return true;
            return is_desc ? (entity_val >= bounds.last_value)
                           : (entity_val <= bounds.last_value);
        } else {
            // LOCALIZED: use existing range check
            return bounds.isValueInRange(entity_val, is_first_page, is_incomplete, is_desc);
        }
    }

    /// Check if an update moving sort value from old_val to new_val affects this page.
    ///
    /// - Offset mode: uses interval overlap between the page range and [min(old,new), max(old,new)].
    /// - Cursor mode: checks if old OR new value is in the page range (localized).
    [[nodiscard]] bool isAffectedByUpdate(int64_t old_val, int64_t new_val) const noexcept {
        if (!bounds.is_valid) return true;

        bool is_desc = (sort_direction == SortDirection::Desc);

        if (pagination_mode == PaginationMode::Offset) {
            // INTERVAL OVERLAP: [page_min, page_max] ∩ [range_min, range_max]
            int64_t page_min = is_desc ? bounds.last_value : bounds.first_value;
            int64_t page_max = is_desc ? bounds.first_value : bounds.last_value;
            int64_t range_min = std::min(old_val, new_val);
            int64_t range_max = std::max(old_val, new_val);

            if (is_incomplete) return (page_min <= range_max);
            return (page_min <= range_max) && (range_min <= page_max);
        } else {
            // LOCALIZED: old OR new in range
            return bounds.isValueInRange(old_val, is_first_page, is_incomplete, is_desc)
                || bounds.isValueInRange(new_val, is_first_page, is_incomplete, is_desc);
        }
    }
};

// =============================================================================
// ListCacheMetadataImpl - Stored behind shared_ptr alongside cache entries
// =============================================================================

template<typename FilterSet, typename SortFieldEnum>
struct ListCacheMetadataImpl {
    using Clock = std::chrono::steady_clock;

    ListQuery<FilterSet, SortFieldEnum> query;
    std::atomic<int64_t> cached_at_rep{0};  // Atomic for shared-lock safety
    SortBounds sort_bounds;
    uint16_t result_count{0};

    ListCacheMetadataImpl() = default;

    ListCacheMetadataImpl(ListQuery<FilterSet, SortFieldEnum> q,
                          Clock::time_point cached_at,
                          SortBounds bounds, uint16_t count)
        : query(std::move(q))
        , cached_at_rep(cached_at.time_since_epoch().count())
        , sort_bounds(bounds)
        , result_count(count)
    {}

    Clock::time_point cachedAt() const {
        return Clock::time_point{
            Clock::duration{cached_at_rep.load(std::memory_order_relaxed)}};
    }
    void setCachedAt(Clock::time_point tp) {
        cached_at_rep.store(tp.time_since_epoch().count(), std::memory_order_relaxed);
    }
};

// =============================================================================
// ListCache - L1 cache for paginated list queries with lazy validation
// =============================================================================
//
// Uses shardmap for storage with callback-based validation on get().
// Modifications are tracked by ModificationTracker and validated lazily.
//
// Template parameters:
//   - Entity: The entity type being cached
//   - ShardCountLog2: log2 of shard count (default: 3 = 8 shards, from CacheConfig NTTP)
//   - Key: The entity ID type (default: int64_t)
//   - Traits: Traits for filter matching, sorting, etc.
//

template<typename Entity, uint8_t ShardCountLog2 = 3,
         typename Key = int64_t, typename Traits = ListCacheTraits<Entity>>
class ListCache {
public:
    static constexpr size_t ShardCount = size_t{1} << ShardCountLog2;

    using FilterSet = typename Traits::Filters;
    using SortFieldEnum = typename Traits::SortField;
    using Query = ListQuery<FilterSet, SortFieldEnum>;
    using Result = jcailloux::relais::wrapper::ListWrapper<Entity>;
    using ResultPtr = std::shared_ptr<const Result>;
    using Modification = EntityModification<Entity>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;

    using ModTracker = ModificationTracker<Entity, ShardCount>;
    using BitmapType = typename ModTracker::BitmapType;

private:
    using CacheKey = std::string;  // Canonical binary buffer
    using MetadataImpl = ListCacheMetadataImpl<FilterSet, SortFieldEnum>;
    using MetadataPtr = std::shared_ptr<MetadataImpl>;

    static constexpr shardmap::ShardMapConfig kShardMapConfig{
        .shard_count_log2 = ShardCountLog2
    };

    // The ShardMap-based storage (Metadata = shared_ptr<MetadataImpl>)
    shardmap::ShardMap<CacheKey, ResultPtr, MetadataPtr, kShardMapConfig> cache_;

    // Tracks recently modified entities for lazy validation
    ModTracker modifications_;

    // Configuration
    ListCacheConfig config_;

    // Counter for cleanup triggers
    std::atomic<size_t> get_counter_{0};

public:
    explicit ListCache(ListCacheConfig config = {})
        : config_(std::move(config))
    {}

    ~ListCache() = default;

    // Non-copyable, non-movable
    ListCache(const ListCache&) = delete;
    ListCache& operator=(const ListCache&) = delete;
    ListCache(ListCache&&) = delete;
    ListCache& operator=(ListCache&&) = delete;

    // =========================================================================
    // Core API
    // =========================================================================

    /// Get cached result for a query (with lazy validation via callback)
    ResultPtr get(const Query& query) {
        // Trigger cleanup every N gets
        if (++get_counter_ % config_.cleanup_every_n_gets == 0) {
            trySweep();
        }

        const auto& key = query.cacheKey();
        const auto now = Clock::now();

        return cache_.get(key, [this, &query, now](const ResultPtr& result, MetadataPtr& meta, uint8_t shard_id) {
            const auto cached_at = meta->cachedAt();

            // 1. TTL check
            if (!config_.accept_expired_on_get) {
                if (cached_at + config_.default_ttl < now) {
                    return shardmap::GetAction::Invalidate;
                }
            }

            // 2. Lazy validation: check if modifications affect this result
            if (isAffectedByModifications(cached_at, meta->sort_bounds,
                                           *result, query, shard_id)) {
                return shardmap::GetAction::Invalidate;
            }

            // 3. Optionally refresh TTL (atomic store under shared lock)
            if (config_.refresh_on_get) {
                meta->setCachedAt(now);
            }

            return shardmap::GetAction::Accept;
        });
    }

    /// Store result for a query with optional sort bounds
    void put(const Query& query, ResultPtr result, SortBounds bounds = {}) {
        const auto& key = query.cacheKey();

        auto meta = std::make_shared<MetadataImpl>(
            query, Clock::now(), bounds,
            static_cast<uint16_t>(result->items.size()));

        cache_.put(key, std::move(result), std::move(meta));
    }

    /// Helper to extract sort bounds from a result
    template<typename SortValueExtractor>
    static SortBounds extractBounds(const Result& result, SortValueExtractor&& extractor) {
        if (result.items.empty()) {
            return SortBounds{.is_valid = false};
        }

        return SortBounds{
            .first_value = extractor(result.items.front()),
            .last_value = extractor(result.items.back()),
            .is_valid = true
        };
    }

    // =========================================================================
    // Modification tracking
    // =========================================================================

    /// Record entity creation for invalidation
    void onEntityCreated(std::shared_ptr<const Entity> entity) {
        modifications_.notifyCreated(std::move(entity));
    }

    /// Record entity update for invalidation
    void onEntityUpdated(std::shared_ptr<const Entity> old_entity,
                         std::shared_ptr<const Entity> new_entity) {
        modifications_.notifyUpdated(std::move(old_entity), std::move(new_entity));
    }

    /// Record entity deletion for invalidation
    void onEntityDeleted(std::shared_ptr<const Entity> entity) {
        modifications_.notifyDeleted(std::move(entity));
    }

    /// Invalidate a specific query
    void invalidate(const Query& query) {
        cache_.invalidate(query.cacheKey());
    }

    // =========================================================================
    // Cleanup API
    // =========================================================================

    /// Context passed to cleanup callbacks
    struct CleanupContext {
        TimePoint expiration_limit;
        const ModTracker& modifications;
        const ListCache& cache;
    };

    /// Try to sweep one shard.
    /// Returns immediately if a sweep is already in progress.
    bool trySweep() {
        // Snapshot time BEFORE shard cleanup so that modifications added
        // during cleanup are not counted (they weren't fully considered).
        const auto now = Clock::now();

        CleanupContext ctx{
            .expiration_limit = now - config_.default_ttl,
            .modifications = modifications_,
            .cache = *this
        };

        auto shard = cache_.try_cleanup(ctx,
            [](const CacheKey&, const ResultPtr& result, const MetadataPtr& meta,
               const CleanupContext& ctx, uint8_t shard_id) {
                // 1. TTL check
                if (meta->cachedAt() < ctx.expiration_limit) {
                    return true;  // Expired, erase
                }

                // 2. Check if affected by modifications (with bitmap skip)
                bool affected = false;
                ctx.modifications.forEachModificationWithBitmap(
                    [&](const Modification& mod, BitmapType pending_segments) {
                        if (affected) return;

                        // Skip: shard already cleaned for this modification
                        if ((pending_segments & (BitmapType{1} << shard_id)) == 0) return;

                        // Skip: data created after modification
                        if (mod.modified_at <= meta->cachedAt()) return;

                        if (ctx.cache.isModificationAffecting(mod, meta, *result)) {
                            affected = true;
                        }
                    });

                return affected;
            });

        if (shard) {
            modifications_.drainShard(now, *shard);
        }

        return shard.has_value();
    }

    /// Sweep one shard.
    /// Returns true if entries were removed.
    bool sweep() {
        const auto now = Clock::now();

        CleanupContext ctx{
            .expiration_limit = now - config_.default_ttl,
            .modifications = modifications_,
            .cache = *this
        };

        auto result = cache_.cleanup(ctx,
            [](const CacheKey&, const ResultPtr& result, const MetadataPtr& meta,
               const CleanupContext& ctx, uint8_t shard_id) {
                // 1. TTL check
                if (meta->cachedAt() < ctx.expiration_limit) {
                    return true;  // Expired, erase
                }

                // 2. Check if affected by modifications (with bitmap skip)
                bool affected = false;
                ctx.modifications.forEachModificationWithBitmap(
                    [&](const Modification& mod, BitmapType pending_segments) {
                        if (affected) return;

                        // Skip: shard already cleaned for this modification
                        if ((pending_segments & (BitmapType{1} << shard_id)) == 0) return;

                        // Skip: data created after modification
                        if (mod.modified_at <= meta->cachedAt()) return;

                        if (ctx.cache.isModificationAffecting(mod, meta, *result)) {
                            affected = true;
                        }
                    });

                return affected;
            });

        modifications_.drainShard(now, result.shard_id);

        return result.removed > 0;
    }

    /// Sweep all shards.
    size_t purge() {
        const auto now = Clock::now();

        CleanupContext ctx{
            .expiration_limit = now - config_.default_ttl,
            .modifications = modifications_,
            .cache = *this
        };

        size_t erased = cache_.full_cleanup(ctx,
            [](const CacheKey&, const ResultPtr& result, const MetadataPtr& meta,
               const CleanupContext& ctx) {
                if (meta->cachedAt() < ctx.expiration_limit) {
                    return true;
                }

                bool affected = false;
                ctx.modifications.forEachModification([&](const Modification& mod) {
                    if (affected) return;
                    if (mod.modified_at <= meta->cachedAt()) return;

                    if (ctx.cache.isModificationAffecting(mod, meta, *result)) {
                        affected = true;
                    }
                });

                return affected;
            });

        // All shards processed — drain modifications that existed before cleanup
        modifications_.drain(now);

        return erased;
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    [[nodiscard]] size_t size() const { return cache_.size(); }
    [[nodiscard]] static constexpr size_t shardCount() { return ShardCount; }
    [[nodiscard]] const ListCacheConfig& config() const { return config_; }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif

private:
    // =========================================================================
    // Validation logic
    // =========================================================================

    /// Check if any recent modifications affect the cached result (with bitmap skip)
    bool isAffectedByModifications(TimePoint cached_at,
                                    const SortBounds& bounds,
                                    const Result& result,
                                    const Query& query,
                                    uint8_t shard_id) const {
        // Short-circuit: if no modifications since cache creation, it's still valid
        if (!modifications_.hasModificationsSince(cached_at)) {
            return false;
        }

        bool affected = false;
        modifications_.forEachModificationWithBitmap(
            [&](const Modification& mod, BitmapType pending_segments) {
                if (affected) return;

                // 1. Skip: shard already cleaned for this modification
                if ((pending_segments & (BitmapType{1} << shard_id)) == 0) {
                    return;
                }

                // 2. Skip: data created after the modification
                if (mod.modified_at <= cached_at) {
                    return;
                }

                // 3+4. Filter match + range check
                if (isModificationAffecting(mod, query, bounds, result)) {
                    affected = true;
                }
            });

        return affected;
    }

    /// Check if a single modification affects a cached page (MetadataPtr overload)
    bool isModificationAffecting(const Modification& mod,
                                  const MetadataPtr& meta,
                                  const Result& result) const {
        return isModificationAffecting(mod, meta->query, meta->sort_bounds, result);
    }

    /// Check if a single modification affects a cached page
    bool isModificationAffecting(const Modification& mod,
                                  const Query& query,
                                  const SortBounds& bounds,
                                  const Result& result) const {
        const auto& filters = query.filters;
        const auto sort = query.sort.value_or(Traits::defaultSort());

        // Check old_entity if present (for update/delete)
        if (mod.old_entity && Traits::matchesFilters(*mod.old_entity, filters)) {
            if (isEntityInPageRange(*mod.old_entity, query, result, bounds, sort)) {
                return true;
            }
        }

        // Check new_entity if present (for insert/update)
        if (mod.new_entity && Traits::matchesFilters(*mod.new_entity, filters)) {
            if (isEntityInPageRange(*mod.new_entity, query, result, bounds, sort)) {
                return true;
            }
        }

        return false;
    }

    /// Check if an entity falls within this page's sort range
    bool isEntityInPageRange(const Entity& entity,
                              const Query& query,
                              const Result& result,
                              const SortBounds& bounds,
                              const SortSpec<SortFieldEnum>& sort) const {
        // If we have valid bounds, use fast O(1) check
        if (bounds.is_valid) {
            int64_t sort_value = Traits::extractSortValue(entity, sort.field);
            bool is_first_page = query.cursor.empty();
            bool is_incomplete = result.items.size() < query.limit;
            bool is_descending = (sort.direction == SortDirection::Desc);

            return bounds.isValueInRange(sort_value, is_first_page, is_incomplete, is_descending);
        }

        // Fallback: use entity comparison
        return isEntityInPageRangeSlow(entity, query, result, sort);
    }

    /// Slow path for range checking using entity comparison
    bool isEntityInPageRangeSlow(const Entity& entity,
                                  const Query& query,
                                  const Result& result,
                                  const SortSpec<SortFieldEnum>& sort) const {
        if (result.items.empty()) {
            return true;  // Empty page - any matching entity affects it
        }

        const bool is_first_page = query.cursor.empty();
        const bool is_incomplete = result.items.size() < query.limit;

        if (is_first_page && is_incomplete) {
            return true;  // Single incomplete page: always invalidate
        }

        const int cmp_last = Traits::compare(entity, result.items.back(),
                                              sort.field, sort.direction);

        if (is_first_page) {
            return cmp_last <= 0;  // value <= last
        }

        const int cmp_first = Traits::compare(entity, result.items.front(),
                                               sort.field, sort.direction);

        if (cmp_first < 0) {
            return false;  // value strictly before first
        }

        if (is_incomplete) {
            return true;  // Last page: value >= first is enough
        }

        return cmp_last <= 0;  // Middle page: value in [first, last]
    }
};

}  // namespace jcailloux::relais::cache::list

#endif  // CODIBOT_LISTCACHE_H
