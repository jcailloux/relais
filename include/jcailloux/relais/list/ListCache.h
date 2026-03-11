#ifndef JCX_RELAIS_LIST_LISTCACHE_H
#define JCX_RELAIS_LIST_LISTCACHE_H

#include <atomic>
#include <chrono>
#include <type_traits>

#include "ListQuery.h"
#include "ListCacheTraits.h"
#include "ModificationTracker.h"
#include "jcailloux/relais/list/ListWrapper.h"
#include "jcailloux/relais/cache/CacheView.h"
#include "jcailloux/relais/cache/CacheTier.h"
#include "jcailloux/relais/cache/CacheMetadata.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/cache/TaggedEntry.h"
#include "jcailloux/relais/runtime/CachedClock.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais::list {

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
    uint32_t default_ttl_sec{3600};   // 1 hour, in seconds
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
    [[nodiscard]] bool isAffectedByCreateOrDelete(int64_t entity_val) const noexcept {
        if (!bounds.is_valid) return true;

        bool is_desc = (sort_direction == SortDirection::Desc);

        if (pagination_mode == PaginationMode::Offset) {
            if (is_incomplete) return true;
            return is_desc ? (entity_val >= bounds.last_value)
                           : (entity_val <= bounds.last_value);
        } else {
            return bounds.isValueInRange(entity_val, is_first_page, is_incomplete, is_desc);
        }
    }

    /// Check if an update moving sort value from old_val to new_val affects this page.
    [[nodiscard]] bool isAffectedByUpdate(int64_t old_val, int64_t new_val) const noexcept {
        if (!bounds.is_valid) return true;

        bool is_desc = (sort_direction == SortDirection::Desc);

        if (pagination_mode == PaginationMode::Offset) {
            int64_t page_min = is_desc ? bounds.last_value : bounds.first_value;
            int64_t page_max = is_desc ? bounds.first_value : bounds.last_value;
            int64_t range_min = std::min(old_val, new_val);
            int64_t range_max = std::max(old_val, new_val);

            if (is_incomplete) return (page_min <= range_max);
            return (page_min <= range_max) && (range_min <= page_max);
        } else {
            return bounds.isValueInRange(old_val, is_first_page, is_incomplete, is_desc)
                || bounds.isValueInRange(new_val, is_first_page, is_incomplete, is_desc);
        }
    }
};

// =============================================================================
// ListCacheMetadataImpl - Stored inline in ChunkMap CacheEntry
// =============================================================================
//
// Inherits from CacheMetadata<true, true> for unified GDSF + TTL handling.
// Uses stored_generation (uint32_t) instead of cached_at_rep (int64_t) for
// modification tracking — exact precision, no clock issues.

template<typename FilterSet, typename SortFieldEnum>
struct ListCacheMetadataImpl : cache::CacheMetadata<true, true> {
    using Base = cache::CacheMetadata<true, true>;

    ListQuery<FilterSet, SortFieldEnum> query;
    uint32_t stored_generation{0};     // generation of the owning ListCache at cache time
    SortBounds sort_bounds;
    uint16_t result_count{0};
    float construction_time_us{0.0f};  // diagnostics only

    ListCacheMetadataImpl() = default;

    ListCacheMetadataImpl(ListQuery<FilterSet, SortFieldEnum> q,
                          uint32_t gen,
                          uint32_t ttl_expiration,
                          SortBounds bounds, uint16_t count,
                          float cost_us = 0.0f)
        : Base(cache::GDSFScoreData::kCountScale, ttl_expiration)
        , query(std::move(q))
        , stored_generation(gen)
        , sort_bounds(bounds)
        , result_count(count)
        , construction_time_us(cost_us)
    {}

    /// Merge access history from old entry on upsert (kUpdatePenalty applied).
    void mergeFrom(const ListCacheMetadataImpl& old) {
        Base::mergeFrom(old);
    }

    // Explicit move (GDSFScoreData has atomics requiring manual move)
    ListCacheMetadataImpl(ListCacheMetadataImpl&& o) noexcept
        : Base(std::move(o))
        , query(std::move(o.query))
        , stored_generation(o.stored_generation)
        , sort_bounds(o.sort_bounds)
        , result_count(o.result_count)
        , construction_time_us(o.construction_time_us)
    {}

    ListCacheMetadataImpl& operator=(ListCacheMetadataImpl&& o) noexcept {
        Base::operator=(std::move(o));
        query = std::move(o.query);
        stored_generation = o.stored_generation;
        sort_bounds = o.sort_bounds;
        result_count = o.result_count;
        construction_time_us = o.construction_time_us;
        return *this;
    }

    ListCacheMetadataImpl(const ListCacheMetadataImpl&) = delete;
    ListCacheMetadataImpl& operator=(const ListCacheMetadataImpl&) = delete;
};

// =============================================================================
// ListCache - L1 cache for paginated list queries with lazy validation
// =============================================================================
//
// Thin wrapper around CacheTier<string, ListWrapper, ListCacheMetadataImpl>.
// CacheTier handles: ChunkMap storage, GDSF scoring/admission, TTL, ghosts,
// inflight dedup, and cleanup sweep mechanics.
//
// ListCache adds domain-specific concerns:
// - ModificationTracker (lazy invalidation by filter/sort matching)
// - Generation counter (monotonic, for modification ordering)
// - Modification-based validation on get() and sweep extra predicate
//
// Template parameters:
//   - E: The entity type being cached
//   - ChunkCountLog2: log2 of chunk count (default: 3 = 8 chunks)
//   - Key: The entity ID type (default: int64_t)
//   - Traits: Traits for filter matching, sorting, etc.
//   - GDSF: Enable GDSF score tracking

template<typename E, uint8_t ChunkCountLog2 = 3,
         typename Key = int64_t, typename Traits = ListCacheTraits<E>,
         bool GDSF = true>
class ListCache {
public:
    static constexpr size_t ChunkCount = size_t{1} << ChunkCountLog2;

    using FilterSet = typename Traits::Filters;
    using SortFieldEnum = typename Traits::SortField;
    using Query = ListQuery<FilterSet, SortFieldEnum>;
    using Result = jcailloux::relais::list::ListWrapper<E>;
    using ResultView = jcailloux::relais::cache::CacheView<Result>;
    using Modification = EntityModification<E>;

    using ModTracker = ModificationTracker<E, ChunkCount>;
    using BitmapType = typename ModTracker::BitmapType;

private:
    using CacheKey = std::string;
    using MetadataImpl = ListCacheMetadataImpl<FilterSet, SortFieldEnum>;
    using Tier = cache::CacheTier<CacheKey, Result, MetadataImpl>;

    /// Backward-compat alias for test accessors.
    using L1Cache = typename Tier::Map;

    Tier tier_;
    ModTracker modifications_;
    ListCacheConfig config_;
    std::atomic<uint32_t> generation_{0};  // Monotonic mutation counter

public:
    explicit ListCache(ListCacheConfig config = {})
        : config_(std::move(config))
    {}

    ~ListCache() = default;

    ListCache(const ListCache&) = delete;
    ListCache& operator=(const ListCache&) = delete;
    ListCache(ListCache&&) = delete;
    ListCache& operator=(ListCache&&) = delete;

    // =========================================================================
    // Core API
    // =========================================================================

    /// Get cached result for a query (with lazy validation + GDSF score bump).
    /// Returns epoch-guarded ResultView (empty if miss or invalidated).
    ResultView get(const Query& query) {
        return getByKey(query.cacheKey());
    }

    /// Get cached result by pre-computed cache key.
    /// Delegates to CacheTier::find() for ghost/TTL/GDSF, then validates modifications.
    /// Single-hash: chunk_id computed from Hit::key_hash (no re-hash).
    ResultView getByKey(const std::string& key) {
        auto hit = tier_.find(key);
        if (!hit) return {};

        // Compute chunk_id from the hash already computed by find().
        long chunk_id = static_cast<long>(
            (hit.key_hash >> (48 - ChunkCountLog2)) & (ChunkCount - 1));

        // Modification validation — if entry is stale, evict and return miss.
        if (isAffectedByModificationsForChunk(*hit.meta, *hit.value, chunk_id)) {
            tier_.evictIfSame(key, hit.value);
            return {};
        }

        return ResultView(hit.value, std::move(hit.guard));
    }

    /// Store result for a query with optional sort bounds and construction cost.
    /// Returns epoch-guarded ResultView pointing to the cached entry.
    ResultView put(const Query& query, Result result, SortBounds bounds = {},
                   float construction_time_us = 0.0f) {
        const auto& key = query.cacheKey();
        uint32_t now_sec = runtime::CachedClock::now();
        uint32_t gen = generation_.load(std::memory_order_relaxed);

        MetadataImpl meta(
            query, gen, now_sec + config_.default_ttl_sec, bounds,
            static_cast<uint16_t>(result.items.size()),
            construction_time_us);

        auto hit = tier_.store(key, std::move(result), std::move(meta));

        // Record construction cost in EMA + trigger deterministic cleanup
        tier_.recordCost(construction_time_us);
        if constexpr (GDSF && cache::GDSFPolicy::enabled) {
            cache::GDSFPolicy::instance().tickInsertion();
        }

        return ResultView(hit.value, std::move(hit.guard));
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
    void onEntityCreated(const E& entity) {
        uint32_t gen = generation_.fetch_add(1, std::memory_order_relaxed) + 1;
        modifications_.notifyCreated(entity, gen);
    }

    /// Record entity update for invalidation
    void onEntityUpdated(const E& old_entity, const E& new_entity) {
        uint32_t gen = generation_.fetch_add(1, std::memory_order_relaxed) + 1;
        modifications_.notifyUpdated(old_entity, new_entity, gen);
    }

    /// Record entity deletion for invalidation
    void onEntityDeleted(const E& entity) {
        uint32_t gen = generation_.fetch_add(1, std::memory_order_relaxed) + 1;
        modifications_.notifyDeleted(entity, gen);
    }

    /// Invalidate a specific query.
    void invalidate(const Query& query) {
        tier_.evict(query.cacheKey());
    }

    // =========================================================================
    // Cleanup API — delegates to CacheTier with modification extra predicate
    // =========================================================================

    /// Sweep one chunk (lock-free, always succeeds).
    bool trySweep() {
        uint32_t cutoff_gen = generation_.load(std::memory_order_relaxed);
        auto result = tier_.sweepChunk(modificationPred());
        if (result.chunk_id >= 0) {
            modifications_.drainChunk(cutoff_gen, static_cast<uint8_t>(result.chunk_id));
        }
        return result.removed_any;
    }

    /// Sweep one chunk (identical to trySweep in lock-free design).
    bool sweep() {
        return trySweep();
    }

    /// Sweep all chunks.
    size_t purge() {
        uint32_t cutoff_gen = generation_.load(std::memory_order_relaxed);
        size_t removed = tier_.purgeAll(modificationPredFull());
        modifications_.drain(cutoff_gen);
        return removed;
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    [[nodiscard]] size_t size() { return tier_.size(); }
    [[nodiscard]] static constexpr size_t chunkCount() { return ChunkCount; }
    [[nodiscard]] const ListCacheConfig& config() const { return config_; }
    [[nodiscard]] Tier& tier() { return tier_; }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif

private:
    // =========================================================================
    // Modification extra predicates for CacheTier::sweepChunk/purgeAll
    // =========================================================================

    /// Extra predicate for sweepChunk: uses bitmap skip optimization.
    auto modificationPred() {
        return [this](const CacheKey&, const MetadataImpl& meta,
                      const Result& value, long chunk_id) -> bool {
            return isAffectedByModificationsForChunk(meta, value, chunk_id);
        };
    }

    /// Extra predicate for purgeAll: checks all modifications (no bitmap).
    auto modificationPredFull() {
        return [this](const CacheKey&, const MetadataImpl& meta,
                      const Result& value, long /*chunk_id*/) -> bool {
            return isAffectedByModifications(meta, value);
        };
    }

    // =========================================================================
    // Validation logic
    // =========================================================================

    /// Check if any recent modifications affect the cached result (no bitmap).
    bool isAffectedByModifications(const MetadataImpl& meta,
                                    const Result& result) const {
        uint32_t stored_gen = meta.stored_generation;
        if (!modifications_.hasModificationsSince(stored_gen)) {
            return false;
        }

        bool affected = false;
        modifications_.forEachModification(
            [&](const Modification& mod) {
                if (affected) return;
                if (mod.generation <= stored_gen) return;
                if (isModificationAffecting(mod, meta.query, meta.sort_bounds, result)) {
                    affected = true;
                }
            });

        return affected;
    }

    /// Check if any recent modifications affect the cached result (with bitmap skip).
    bool isAffectedByModificationsForChunk(const MetadataImpl& meta,
                                            const Result& result,
                                            long chunk_id) const {
        uint32_t stored_gen = meta.stored_generation;
        if (!modifications_.hasModificationsSince(stored_gen)) {
            return false;
        }

        bool affected = false;
        modifications_.forEachModificationWithBitmap(
            [&](const Modification& mod, BitmapType pending_chunks) {
                if (affected) return;

                // Skip: chunk already cleaned for this modification
                if ((pending_chunks & (BitmapType{1} << chunk_id)) == 0) return;

                // Skip: data created after modification
                if (mod.generation <= stored_gen) return;

                if (isModificationAffecting(mod, meta.query, meta.sort_bounds, result)) {
                    affected = true;
                }
            });

        return affected;
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
    bool isEntityInPageRange(const E& entity,
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
    bool isEntityInPageRangeSlow(const E& entity,
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

}  // namespace jcailloux::relais::list

#endif  // JCX_RELAIS_LIST_LISTCACHE_H
