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

    /// Range generalization of isValueInRange: does the closed predicate range
    /// [lo,hi] overlap this page's effective sort range? A page is invalidated by
    /// a predicate delete iff some value it could hold falls in [lo,hi]. The page
    /// extents mirror isValueInRange exactly (so the per-entity point check is the
    /// degenerate lo==hi case), with the open-ended first/incomplete pages
    /// extending to ±∞ on the unbounded side.
    [[nodiscard]] bool isRangeInRange(
        int64_t lo,
        int64_t hi,
        bool is_first_page,
        bool is_incomplete,
        bool is_descending
    ) const noexcept {
        if (!is_valid) {
            return true;  // Empty page or no bounds - conservatively assume overlap
        }

        if (is_descending) {
            // DESC page covers [last_value, first_value]; open ends to ±∞.
            if (is_first_page && is_incomplete) return true;
            if (is_first_page) return hi >= last_value;        // [last, +∞)
            if (is_incomplete) return lo <= first_value;       // (-∞, first]
            return lo <= first_value && hi >= last_value;      // [last, first]
        } else {
            // ASC page covers [first_value, last_value]; open ends to ±∞.
            if (is_first_page && is_incomplete) return true;
            if (is_first_page) return lo <= last_value;        // (-∞, last]
            if (is_incomplete) return hi >= first_value;       // [first, +∞)
            return lo <= last_value && hi >= first_value;      // [first, last]
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

    // The tracker carries predicate range modifications (eraseWhere fast-path)
    // as well as per-entity ones; the payload is a copy of the predicate filters.
    using ModTracker = ModificationTracker<E, ChunkCount, FilterSet, Key>;
    using BitmapType = typename ModTracker::BitmapType;

    /// Whether Traits exposes the predicate fast-path hooks (only ListMixin's
    /// adapter does). The range consumption path is compiled out otherwise.
    static constexpr bool kHasPredicateTraits = requires(const FilterSet& p, size_t i) {
        { Traits::predicateGroupCompatible(p, p) } -> std::convertible_to<bool>;
        Traits::predicateSortRange(p, i);
    };

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

    /// Generation to read before fetching a page, then to hand to put().
    [[nodiscard]] uint32_t generation() const {
        return generation_.load(std::memory_order_seq_cst);
    }

    /// Store result for a query with optional sort bounds and construction cost.
    /// `fetch_gen` is generation(), read before the page was fetched: every
    /// modification notified since must be able to invalidate it. A page read
    /// from the database reflects every modification notified before (they
    /// are notified after their commit). Returns epoch-guarded ResultView pointing to the stored
    /// entry (still readable if the entry is evicted again right away).
    ///
    /// Stamping the page at put time instead would let a modification notified
    /// during the fetch pass for older than the page. And the chunk sweep may
    /// have processed such a modification before the store (bit cleared, or
    /// entry drained), so lazy validation would never see it either. Hence the
    /// check after the store: if modifications were notified during the fetch,
    /// the page is kept only when each of them is still tracked and none affects
    /// it. A modification notified after the reload below is covered by lazy
    /// validation and by the sweep, which sees this store (seq_cst fence here,
    /// seq_cst generation bump, seq_cst cutoff read and fence in sweep()).
    ResultView put(const Query& query, Result result, uint32_t fetch_gen,
                   SortBounds bounds = {}, float construction_time_us = 0.0f) {
        const auto& key = query.cacheKey();
        uint32_t now_sec = runtime::CachedClock::now();

        MetadataImpl meta(
            query, fetch_gen, now_sec + config_.default_ttl_sec, bounds,
            static_cast<uint16_t>(result.items.size()),
            construction_time_us);

        auto hit = tier_.store(key, std::move(result), std::move(meta));

        std::atomic_thread_fence(std::memory_order_seq_cst);
        uint32_t current = generation_.load(std::memory_order_seq_cst);
        if (current != fetch_gen
            && straddledModificationsAffect(*hit.meta, *hit.value, fetch_gen, current)) {
            tier_.evictIfSame(key, hit.value);
        }

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
        modifications_.notifyCreated(entity, generation_);
    }

    /// Record entity update for invalidation
    void onEntityUpdated(const E& old_entity, const E& new_entity) {
        modifications_.notifyUpdated(old_entity, new_entity, generation_);
    }

    /// Record entity deletion for invalidation
    void onEntityDeleted(const E& entity) {
        modifications_.notifyDeleted(entity, generation_);
    }

    /// Record a predicate range delete (eraseWhere fast-path): a single tracker
    /// entry that invalidates every cached page a deleted row could have lived in,
    /// instead of one entity modification per deleted row (O(1) vs O(N) tracker).
    void onEntityRangeDeleted(const FilterSet& predicate) {
        static_assert(kHasPredicateTraits,
            "onEntityRangeDeleted requires Traits with predicate fast-path hooks");
        modifications_.notifyRangeDeleted(predicate, generation_);
    }

    /// Record that the row `key` left every page holding it, for a position
    /// that cannot be known (uncertain write): those pages, and only those,
    /// are invalidated lazily.
    void onRowLeft(const Key& key) {
        modifications_.notifyRowLeft(key, generation_);
    }

    /// Invalidate a specific query.
    void invalidate(const Query& query) {
        tier_.evict(query.cacheKey());
    }

    // =========================================================================
    // Cleanup API — delegates to CacheTier with modification extra predicate
    // =========================================================================

    /// Sweep given chunk (lock-free, always succeeds).
    /// The cutoff is read under the tracker's lock, so every modification it
    /// covers is already tracked when the scan runs; with the fence, a page
    /// stored before a modification is notified is seen by the sweep that
    /// drains it (see put()).
    bool sweep(long chunk_id) {
        uint32_t cutoff_gen = modifications_.cutoff(generation_);
        std::atomic_thread_fence(std::memory_order_seq_cst);
        auto result = tier_.sweepChunk(chunk_id, modificationPred());
        if (result.chunk_id >= 0) {
            modifications_.drainChunk(cutoff_gen, static_cast<uint8_t>(result.chunk_id));
        }
        return result.removed_any;
    }

    /// Sweep all chunks.
    size_t purge() {
        uint32_t cutoff_gen = modifications_.cutoff(generation_);
        std::atomic_thread_fence(std::memory_order_seq_cst);
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

    /// Modifications (fetch_gen, current] were notified while the page was
    /// fetched: true if one of them affects it, or cannot be checked. Every
    /// notification takes the next generation, so a modification not tracked
    /// yet or already drained shows as a short count.
    bool straddledModificationsAffect(const MetadataImpl& meta, const Result& result,
                                      uint32_t fetch_gen, uint32_t current) const {
        uint32_t seen = 0;
        bool affected = false;
        modifications_.forEachModification([&](const Modification& mod, uint32_t gen) {
            if (gen <= fetch_gen || gen > current) return;
            ++seen;
            if (!affected && isModificationAffecting(mod, meta.query, meta.sort_bounds, result))
                affected = true;
        });
        if constexpr (kHasPredicateTraits) {
            modifications_.forEachRange([&](const FilterSet& predicate, uint32_t gen) {
                if (gen <= fetch_gen || gen > current) return;
                ++seen;
                if (!affected && isRangeAffecting(predicate, meta.query, meta.sort_bounds, result))
                    affected = true;
            });
        }
        modifications_.forEachRowLeft([&](const Key& key, uint32_t gen) {
            if (gen <= fetch_gen || gen > current) return;
            ++seen;
            if (!affected && isRowLeftAffecting(key, meta.query, result)) affected = true;
        });
        return affected || seen != current - fetch_gen;
    }

    /// Check if any recent modifications affect the cached result (no bitmap).
    bool isAffectedByModifications(const MetadataImpl& meta,
                                    const Result& result) const {
        uint32_t stored_gen = meta.stored_generation;
        if (!modifications_.hasModificationsSince(stored_gen)) {
            return false;
        }

        bool affected = false;
        modifications_.forEachModification(
            [&](const Modification& mod, uint32_t gen) {
                if (affected) return;
                if (gen <= stored_gen) return;
                if (isModificationAffecting(mod, meta.query, meta.sort_bounds, result)) {
                    affected = true;
                }
            });

        if constexpr (kHasPredicateTraits) {
            modifications_.forEachRange(
                [&](const FilterSet& predicate, uint32_t gen) {
                    if (affected) return;
                    if (gen <= stored_gen) return;
                    if (isRangeAffecting(predicate, meta.query, meta.sort_bounds, result)) {
                        affected = true;
                    }
                });
        }

        modifications_.forEachRowLeft([&](const Key& key, uint32_t gen) {
            if (affected || gen <= stored_gen) return;
            if (isRowLeftAffecting(key, meta.query, result)) affected = true;
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
            [&](const Modification& mod, uint32_t gen, BitmapType pending_chunks) {
                if (affected) return;

                // Skip: chunk already cleaned for this modification
                if ((pending_chunks & (BitmapType{1} << chunk_id)) == 0) return;

                // Skip: data created after modification
                if (gen <= stored_gen) return;

                if (isModificationAffecting(mod, meta.query, meta.sort_bounds, result)) {
                    affected = true;
                }
            });

        if constexpr (kHasPredicateTraits) {
            modifications_.forEachRangeWithBitmap(
                [&](const FilterSet& predicate, uint32_t gen, BitmapType pending_chunks) {
                    if (affected) return;
                    if ((pending_chunks & (BitmapType{1} << chunk_id)) == 0) return;
                    if (gen <= stored_gen) return;
                    if (isRangeAffecting(predicate, meta.query, meta.sort_bounds, result)) {
                        affected = true;
                    }
                });
        }

        modifications_.forEachRowLeftWithBitmap(
            [&](const Key& key, uint32_t gen, BitmapType pending_chunks) {
                if (affected) return;
                if ((pending_chunks & (BitmapType{1} << chunk_id)) == 0) return;
                if (gen <= stored_gen) return;
                if (isRowLeftAffecting(key, meta.query, result)) affected = true;
            });

        return affected;
    }

    /// Does a row that left its pages affect this one? A keyset page holds
    /// the rows that follow its cursor: removing a row it does not hold moves
    /// nothing in it. So it is affected iff it holds the row, whatever version
    /// of the row it holds. An offset page past the first counts the rows
    /// before it, which may include this one: always affected.
    bool isRowLeftAffecting(const Key& key, const Query& query, const Result& result) const {
        if (query.offset > 0) return true;
        for (const auto& item : result.items)
            if (Key(item.key()) == key) return true;
        return false;
    }

    /// Check if a predicate range delete affects a cached page. Two gates, both
    /// never-miss: the page's group must be filter-compatible with the predicate
    /// (else no deleted row lived here), and the predicate's value range on the
    /// page's sort dimension must overlap the page's [first,last].
    bool isRangeAffecting(const FilterSet& predicate,
                          const Query& query,
                          const SortBounds& bounds,
                          const Result& result) const {
        if constexpr (kHasPredicateTraits) {
            if (!Traits::predicateGroupCompatible(predicate, query.filters)) {
                return false;
            }
            const auto sort = query.sort.value_or(Traits::defaultSort());
            const auto range = Traits::predicateSortRange(predicate, sort.field);
            if (!bounds.is_valid) return true;
            const bool is_first_page = query.cursor.empty();
            const bool is_incomplete = result.items.size() < query.limit;
            const bool is_descending = (sort.direction == SortDirection::Desc);
            return bounds.isRangeInRange(range.lo, range.hi,
                                         is_first_page, is_incomplete, is_descending);
        } else {
            return false;
        }
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
