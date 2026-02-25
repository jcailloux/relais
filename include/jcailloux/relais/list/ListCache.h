#ifndef CODIBOT_LISTCACHE_H
#define CODIBOT_LISTCACHE_H

#include <atomic>
#include <chrono>
#include <cstring>
#include <type_traits>

#include "jcailloux/relais/cache/ChunkMap.h"
#include "ListQuery.h"
#include "ListCacheTraits.h"
#include "ModificationTracker.h"
#include "jcailloux/relais/wrapper/ListWrapper.h"
#include "jcailloux/relais/wrapper/BufferView.h"
#include "jcailloux/relais/cache/GDSFMetadata.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/config/CachedClock.h"

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
    std::chrono::seconds default_ttl{3600};   // 1 hour
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

template<typename FilterSet, typename SortFieldEnum>
struct ListCacheMetadataImpl {
    using Clock = std::chrono::steady_clock;

    ListQuery<FilterSet, SortFieldEnum> query;
    int64_t cached_at_rep{0};  // steady_clock rep (immutable after construction)
    SortBounds sort_bounds;
    uint16_t result_count{0};
    cache::GDSFScoreData gdsf;              // GDSF access_count tracking (mutable atomic)
    float construction_time_us{0.0f};       // Measured cost for this page

    ListCacheMetadataImpl() = default;

    ListCacheMetadataImpl(ListQuery<FilterSet, SortFieldEnum> q,
                          Clock::time_point cached_at,
                          SortBounds bounds, uint16_t count,
                          float cost_us = 0.0f)
        : query(std::move(q))
        , cached_at_rep(cached_at.time_since_epoch().count())
        , sort_bounds(bounds)
        , result_count(count)
        , gdsf(cache::GDSFScoreData::kCountScale)
        , construction_time_us(cost_us)
    {}

    /// Merge access history from old entry on upsert (kUpdatePenalty applied).
    void mergeFrom(const ListCacheMetadataImpl& old) {
        gdsf.mergeFrom(old.gdsf);
    }

    // Explicit move (GDSFScoreData has atomics requiring manual move)
    ListCacheMetadataImpl(ListCacheMetadataImpl&& o) noexcept
        : query(std::move(o.query))
        , cached_at_rep(o.cached_at_rep)
        , sort_bounds(o.sort_bounds)
        , result_count(o.result_count)
        , gdsf(std::move(o.gdsf))
        , construction_time_us(o.construction_time_us)
    {}

    ListCacheMetadataImpl& operator=(ListCacheMetadataImpl&& o) noexcept {
        query = std::move(o.query);
        cached_at_rep = o.cached_at_rep;
        sort_bounds = o.sort_bounds;
        result_count = o.result_count;
        gdsf = std::move(o.gdsf);
        construction_time_us = o.construction_time_us;
        return *this;
    }

    ListCacheMetadataImpl(const ListCacheMetadataImpl&) = delete;
    ListCacheMetadataImpl& operator=(const ListCacheMetadataImpl&) = delete;

    Clock::time_point cachedAt() const {
        return Clock::time_point{Clock::duration{cached_at_rep}};
    }
};

// =============================================================================
// ListCache - L1 cache for paginated list queries with lazy validation
// =============================================================================
//
// Uses ChunkMap (lock-free ParlayHash) for storage with epoch-based reclamation.
// Modifications are tracked by ModificationTracker and validated lazily on get().
//
// Template parameters:
//   - Entity: The entity type being cached
//   - ChunkCountLog2: log2 of chunk count (default: 3 = 8 chunks, from CacheConfig NTTP)
//   - Key: The entity ID type (default: int64_t)
//   - Traits: Traits for filter matching, sorting, etc.
//   - GDSF: Enable GDSF score tracking
//

template<typename Entity, uint8_t ChunkCountLog2 = 3,
         typename Key = int64_t, typename Traits = ListCacheTraits<Entity>,
         bool GDSF = true>
class ListCache {
public:
    static constexpr size_t ChunkCount = size_t{1} << ChunkCountLog2;

    using FilterSet = typename Traits::Filters;
    using SortFieldEnum = typename Traits::SortField;
    using Query = ListQuery<FilterSet, SortFieldEnum>;
    using Result = jcailloux::relais::wrapper::ListWrapper<Entity>;
    using ResultView = jcailloux::relais::wrapper::BufferView<Result>;
    using Modification = EntityModification<Entity>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;

    using ModTracker = ModificationTracker<Entity, ChunkCount>;
    using BitmapType = typename ModTracker::BitmapType;

private:
    using CacheKey = std::string;  // Canonical binary buffer
    using MetadataImpl = ListCacheMetadataImpl<FilterSet, SortFieldEnum>;
    using L1Cache = cache::ChunkMap<CacheKey, Result, MetadataImpl>;

    L1Cache cache_;
    ModTracker modifications_;
    ListCacheConfig config_;
    std::atomic<long> cleanup_cursor_{0};  // Own cursor for chunk-based cleanup

public:
    explicit ListCache(ListCacheConfig config = {})
        : config_(std::move(config))
    {
        config::CachedClock::ensureStarted();
    }

    ~ListCache() = default;

    // Non-copyable, non-movable
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

    /// Get cached result by pre-computed cache key (avoids toCacheQuery overhead).
    /// Single-hash: hashes the key once for both lookup and chunk computation.
    ResultView getByKey(const std::string& key) {
        auto hk = L1Cache::make_key(key);
        auto result = cache_.find(hk);
        if (!result) return {};

        auto* ce = result.asReal();
        if (!ce) return {};
        auto& meta = ce->metadata;
        auto& value = ce->value;

        // Single-hash chunk computation: extract hash from pre-computed hashed_key
        size_t hash = L1Cache::get_hash(hk);
        long chunk_id = cache_.chunk_for_hash(hash, static_cast<long>(ChunkCount));

        if (isAffectedByModificationsForChunk(meta, value, config::CachedClock::now(), chunk_id)) {
            // Two-phase eviction: remove only if same entry (guards against concurrent Upsert)
            cache_.remove_if(key, [ce](auto* e) { return e == static_cast<typename L1Cache::EntryHeader*>(ce); });
            return {};
        }

        // GDSF: bump access count (simple fetch_add, no decay on read path)
        if constexpr (GDSF) {
            meta.gdsf.access_count.fetch_add(cache::GDSFScoreData::kCountScale,
                                              std::memory_order_relaxed);
        }

        return ResultView(&value, std::move(result.guard));
    }

    /// Store result for a query with optional sort bounds and construction cost.
    /// Returns epoch-guarded ResultView pointing to the cached entry.
    ResultView put(const Query& query, Result result, SortBounds bounds = {},
                   float construction_time_us = 0.0f) {
        const auto& key = query.cacheKey();
        const auto now = Clock::now();

        MetadataImpl meta(
            query, now, bounds,
            static_cast<uint16_t>(result.items.size()),
            construction_time_us);

        auto hk = L1Cache::make_key(key);
        auto find_result = cache_.upsert(hk, std::move(result), std::move(meta));

        // Hash-mask cleanup trigger (replaces modulo-based get counter)
        if constexpr (GDSF) {
            if ((L1Cache::get_hash(hk) & cache::GDSFPolicy::kCleanupMask) == 0) {
                cache::GDSFPolicy::instance().sweep();
            }
        } else {
            if ((L1Cache::get_hash(hk) & cache::GDSFPolicy::kCleanupMask) == 0) {
                trySweep();
            }
        }

        return ResultView(&find_result.asReal()->value, std::move(find_result.guard));
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
    void onEntityCreated(const Entity& entity) {
        modifications_.notifyCreated(entity);
    }

    /// Record entity update for invalidation
    void onEntityUpdated(const Entity& old_entity, const Entity& new_entity) {
        modifications_.notifyUpdated(old_entity, new_entity);
    }

    /// Record entity deletion for invalidation
    void onEntityDeleted(const Entity& entity) {
        modifications_.notifyDeleted(entity);
    }

    /// Invalidate a specific query
    void invalidate(const Query& query) {
        cache_.invalidate(query.cacheKey());
    }

    // =========================================================================
    // Cleanup API
    // =========================================================================

    /// Sweep one chunk (lock-free, always succeeds).
    bool trySweep() {
        // Snapshot time BEFORE chunk cleanup so that modifications added
        // during cleanup are not counted (they weren't fully considered).
        const auto now = Clock::now();
        long chunk = cleanup_cursor_.fetch_add(1, std::memory_order_relaxed)
                   % static_cast<long>(ChunkCount);

        float threshold = 0.0f;
        if constexpr (GDSF) {
            threshold = cache::GDSFPolicy::instance().threshold();
        }

        auto removed = cache_.cleanup_chunk(chunk, static_cast<long>(ChunkCount),
            [this, now, threshold, chunk](const CacheKey&, auto& header) {
                auto& entry = static_cast<typename L1Cache::CacheEntry&>(header);
                return cleanupPredicate(entry.metadata, entry.value, now, threshold, chunk);
            });

        modifications_.drainChunk(now, static_cast<uint8_t>(chunk));

        return removed > 0;
    }

    /// Sweep one chunk (identical to trySweep in lock-free design).
    bool sweep() {
        return trySweep();
    }

    /// Sweep all chunks.
    size_t purge() {
        const auto now = Clock::now();

        float threshold = 0.0f;
        if constexpr (GDSF) {
            threshold = cache::GDSFPolicy::instance().threshold();
        }

        size_t erased = cache_.full_cleanup(
            [this, now, threshold](const CacheKey&, auto& header) {
                auto& entry = static_cast<typename L1Cache::CacheEntry&>(header);
                return cleanupPredicateFull(entry.metadata, entry.value, now, threshold);
            });

        // All chunks processed — drain modifications that existed before cleanup
        modifications_.drain(now);

        return erased;
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    [[nodiscard]] size_t size() { return static_cast<size_t>(cache_.size()); }
    [[nodiscard]] static constexpr size_t chunkCount() { return ChunkCount; }
    [[nodiscard]] const ListCacheConfig& config() const { return config_; }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif

private:
    // =========================================================================
    // Validation logic
    // =========================================================================

    /// Check if any recent modifications affect the cached result.
    /// Used by get() — no chunk_id available, checks all modifications.
    bool isAffectedByModifications(const MetadataImpl& meta,
                                    const Result& result,
                                    const Query& query) const {
        auto cached_at = meta.cachedAt();
        if (!modifications_.hasModificationsSince(cached_at)) {
            return false;
        }

        bool affected = false;
        modifications_.forEachModification(
            [&](const Modification& mod) {
                if (affected) return;
                if (mod.modified_at <= cached_at) return;
                if (isModificationAffecting(mod, query, meta.sort_bounds, result)) {
                    affected = true;
                }
            });

        return affected;
    }

    /// Check if any recent modifications affect the cached result (with bitmap skip).
    /// Used by cleanup — chunk_id is known.
    bool isAffectedByModificationsForChunk(const MetadataImpl& meta,
                                            const Result& result,
                                            TimePoint now,
                                            long chunk_id) const {
        auto cached_at = meta.cachedAt();
        if (!modifications_.hasModificationsSince(cached_at)) {
            return false;
        }

        bool affected = false;
        modifications_.forEachModificationWithBitmap(
            [&](const Modification& mod, BitmapType pending_chunks) {
                if (affected) return;

                // Skip: chunk already cleaned for this modification
                if ((pending_chunks & (BitmapType{1} << chunk_id)) == 0) return;

                // Skip: data created after modification
                if (mod.modified_at <= cached_at) return;

                if (isModificationAffecting(mod, meta.query, meta.sort_bounds, result)) {
                    affected = true;
                }
            });

        return affected;
    }

    /// Estimate memory usage for a list entry (value + items vector capacity).
    static size_t estimateMemoryUsage(const Result& result) {
        return sizeof(Result) + result.items.capacity() * sizeof(Entity);
    }

    /// Cleanup predicate for chunk-based cleanup (with bitmap skip).
    bool cleanupPredicate(const MetadataImpl& meta, const Result& result,
                           TimePoint now, float threshold, long chunk_id) const {
        // 1. GDSF: inline decay + compute score on-the-fly + record in histogram
        if constexpr (GDSF) {
            // Decay: single writer per chunk during sweep, plain store (no CAS)
            float dr = cache::GDSFPolicy::instance().decayRate();
            uint32_t old_count = meta.gdsf.access_count.load(std::memory_order_relaxed);
            meta.gdsf.access_count.store(
                static_cast<uint32_t>(static_cast<float>(old_count) * dr),
                std::memory_order_relaxed);

            // Score = access_count x avg_cost / memoryUsage
            size_t mem = estimateMemoryUsage(result);
            float score = meta.gdsf.computeScore(meta.construction_time_us, mem);

            // Record in histogram (ALL entries, before eviction decision)
            cache::GDSFPolicy::instance().recordEntry(score, mem);

            if (score < threshold) return true;
        }

        // 2. TTL check (cached_at + default_ttl)
        if (now > meta.cachedAt() + config_.default_ttl) return true;

        // 3. Check if affected by modifications (with bitmap skip)
        return isAffectedByModificationsForChunk(meta, result, now, chunk_id);
    }

    /// Cleanup predicate for full cleanup (no bitmap).
    bool cleanupPredicateFull(const MetadataImpl& meta, const Result& result,
                               TimePoint now, float threshold) const {
        // 1. GDSF: inline decay + compute score on-the-fly + record in histogram
        if constexpr (GDSF) {
            float dr = cache::GDSFPolicy::instance().decayRate();
            uint32_t old_count = meta.gdsf.access_count.load(std::memory_order_relaxed);
            meta.gdsf.access_count.store(
                static_cast<uint32_t>(static_cast<float>(old_count) * dr),
                std::memory_order_relaxed);

            size_t mem = estimateMemoryUsage(result);
            float score = meta.gdsf.computeScore(meta.construction_time_us, mem);

            cache::GDSFPolicy::instance().recordEntry(score, mem);

            if (score < threshold) return true;
        }

        // 2. TTL check (cached_at + default_ttl)
        if (now > meta.cachedAt() + config_.default_ttl) return true;

        // 3. Modification check (without bitmap)
        auto cached_at = meta.cachedAt();
        bool affected = false;
        modifications_.forEachModification([&](const Modification& mod) {
            if (affected) return;
            if (mod.modified_at <= cached_at) return;
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
