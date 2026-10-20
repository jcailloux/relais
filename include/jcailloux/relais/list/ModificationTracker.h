#ifndef JCX_RELAIS_LIST_MODIFICATIONTRACKER_H
#define JCX_RELAIS_LIST_MODIFICATIONTRACKER_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "jcailloux/relais/cache/PendingLane.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais::list {

// =============================================================================
// EntityModification - Represents a modification to an entity
// =============================================================================

template<typename E>
struct EntityModification {
    using EntityPtr = std::unique_ptr<const E>;

    enum class Type : uint8_t {
        Created,
        Updated,
        Deleted
    };

    Type type;
    EntityPtr old_entity;   // nullptr for Created
    EntityPtr new_entity;   // nullptr for Deleted

    static EntityModification created(const E& entity) {
        return EntityModification{
            .type = Type::Created,
            .old_entity = nullptr,
            .new_entity = std::make_unique<const E>(entity)
        };
    }

    static EntityModification updated(const E& old_entity, const E& new_entity) {
        return EntityModification{
            .type = Type::Updated,
            .old_entity = std::make_unique<const E>(old_entity),
            .new_entity = std::make_unique<const E>(new_entity)
        };
    }

    static EntityModification deleted(const E& entity) {
        return EntityModification{
            .type = Type::Deleted,
            .old_entity = std::make_unique<const E>(entity),
            .new_entity = nullptr
        };
    }
};

namespace detail {
    /// Default range-payload — an entity-only tracker carries no predicate ranges.
    struct NoRangePayload {};
}

// =============================================================================
// ModificationTracker - Bitmap-based tracker for list cache invalidation
// =============================================================================
//
// Three pending lanes (see PendingLane) under one lock and one generation
// counter: entity modifications, predicate ranges, rows that left their pages.
// Each entry is drained once every chunk has been swept past it.
//
// Uses monotonic generation numbers instead of timestamps.
// Generation numbers come from the owning ListCache's atomic counter.
//
// TotalSegments = number of chunks, known at compile time (from ChunkMap config).
//

template<typename E, size_t TotalSegments,
         typename RangePayload = detail::NoRangePayload,
         typename RowLeft = int64_t>
class ModificationTracker {
public:
    using Modification = EntityModification<E>;

    /// Entity modifications.
    using ModificationLane = cache::PendingLane<Modification, TotalSegments>;
    /// Ranges of rows (predicate writes): one entry stands in for an unbounded
    /// set of rows located by what they hold, instead of N entity
    /// modifications.
    using RangeLane = cache::PendingLane<RangePayload, TotalSegments>;
    /// Rows that left the pages holding them, at a position nobody knows (the
    /// write that moved them was uncertain). An entry touches exactly the
    /// pages that hold one of its rows (the owner says which). Counted:
    /// readers skip it while it is empty, which it is unless an uncertain
    /// write happened.
    using RowLeftLane = cache::PendingLane<RowLeft, TotalSegments, true>;

    using BitmapType = typename ModificationLane::BitmapType;

private:
    ModificationLane modifications_;
    RangeLane ranges_;
    RowLeftLane rows_left_;
    mutable std::shared_mutex mutex_;
    std::atomic<uint32_t> latest_generation_{0};

    /// Atomic max into latest_generation_ (shared by every lane).
    void bumpLatest(uint32_t gen) {
        uint32_t current = latest_generation_.load(std::memory_order_relaxed);
        while (gen > current &&
               !latest_generation_.compare_exchange_weak(
                   current, gen,
                   std::memory_order_release, std::memory_order_relaxed)) {
        }
    }

    template<typename Lane, typename Payload>
    uint32_t track(Lane& lane, Payload payload, std::atomic<uint32_t>& counter) {
        std::unique_lock lock(mutex_);
        uint32_t gen = counter.fetch_add(1, std::memory_order_seq_cst) + 1;
        // Pushed before the latest generation is raised: a reader that sees
        // `gen` as latest also sees the entry, even in a lane it reads
        // without the lock while that lane looks empty.
        lane.push(std::move(payload), gen);
        bumpLatest(gen);
        return gen;
    }

public:
    explicit ModificationTracker() {
        modifications_.reserve(64);
    }

    ~ModificationTracker() = default;

    // Non-copyable, non-movable
    ModificationTracker(const ModificationTracker&) = delete;
    ModificationTracker& operator=(const ModificationTracker&) = delete;
    ModificationTracker(ModificationTracker&&) = delete;
    ModificationTracker& operator=(ModificationTracker&&) = delete;

    // =========================================================================
    // Track modifications
    // =========================================================================

    // Each notification takes the next value of `counter` (the owning cache's
    // generation) under the exclusive lock, and cutoff() reads it under the
    // shared lock: a generation a sweep reads as its cutoff always belongs to a
    // modification already in the tracker, so the sweep's scan evaluates it
    // before the drain clears its chunk bit. Returns the generation taken.

    uint32_t notifyCreated(const E& entity, std::atomic<uint32_t>& counter) {
        return track(modifications_, Modification::created(entity), counter);
    }

    uint32_t notifyUpdated(const E& old_entity, const E& new_entity,
                           std::atomic<uint32_t>& counter) {
        return track(modifications_, Modification::updated(old_entity, new_entity), counter);
    }

    uint32_t notifyDeleted(const E& entity, std::atomic<uint32_t>& counter) {
        return track(modifications_, Modification::deleted(entity), counter);
    }

    /// Record a range of rows (see RangeLane). One entry covers every row the
    /// payload locates; consumed lazily like entity modifications.
    uint32_t notifyRangeDeleted(RangePayload predicate, std::atomic<uint32_t>& counter) {
        return track(ranges_, std::move(predicate), counter);
    }

    /// Record rows that left their pages (see RowLeftLane).
    uint32_t notifyRowLeft(RowLeft rows, std::atomic<uint32_t>& counter) {
        return track(rows_left_, std::move(rows), counter);
    }

    /// The owning cache's generation, read as a sweep cutoff (see above).
    [[nodiscard]] uint32_t cutoff(const std::atomic<uint32_t>& counter) const {
        std::shared_lock lock(mutex_);
        return counter.load(std::memory_order_seq_cst);
    }

    // =========================================================================
    // Cleanup lifecycle
    // =========================================================================

    /// Called after each successful cleanup_chunk() of the cache.
    /// Clears the bit for chunk_id in each entry's bitmap.
    /// Erases entries whose bitmap becomes 0 (all chunks processed).
    ///
    /// Only entries with generation <= cutoff_gen are processed. The cutoff
    /// must be captured BEFORE the chunk cleanup, so that entries added
    /// during cleanup are excluded and not prematurely drained.
    ///
    /// Two-phase approach:
    /// - Phase 1 (shared_lock): clear bits via atomic_ref, collect fully-drained indices.
    ///   Concurrent with the forEach*WithBitmap readers — no conflict because
    ///   they read via atomic_ref too.
    /// - Phase 2 (unique_lock): erase expired entries via swap-with-last.
    ///   Only taken when there are actual removals.
    void drainChunk(uint32_t cutoff_gen, uint8_t chunk_id) {
        std::vector<size_t> to_erase;
        std::vector<size_t> to_erase_ranges;
        std::vector<size_t> to_erase_rows;
        const BitmapType chunk_bit = BitmapType{1} << chunk_id;

        {
            std::shared_lock lock(mutex_);
            modifications_.collectDrained(cutoff_gen, chunk_bit, to_erase);
            ranges_.collectDrained(cutoff_gen, chunk_bit, to_erase_ranges);
            rows_left_.collectDrained(cutoff_gen, chunk_bit, to_erase_rows);
        }

        if (to_erase.empty() && to_erase_ranges.empty() && to_erase_rows.empty()) return;

        std::unique_lock lock(mutex_);
        modifications_.eraseDrained(to_erase);
        ranges_.eraseDrained(to_erase_ranges);
        rows_left_.eraseDrained(to_erase_rows);
    }

    /// Erase all entries with generation <= cutoff_gen in one pass.
    /// Used by purge() after processing all chunks at once.
    void drain(uint32_t cutoff_gen) {
        std::unique_lock lock(mutex_);
        modifications_.drain(cutoff_gen);
        ranges_.drain(cutoff_gen);
        rows_left_.drain(cutoff_gen);
    }

    // =========================================================================
    // Iteration for lazy validation
    // =========================================================================
    //
    // Callback signatures: void(const Payload&, uint32_t generation) and, for
    // the *WithBitmap variants, void(const Payload&, uint32_t generation,
    // BitmapType pending_segments). Thread-safe: shared_lock allows
    // concurrent readers.

    template<typename Callback>
    void forEachModificationWithBitmap(Callback&& callback) const {
        std::shared_lock lock(mutex_);
        modifications_.forEachWithBitmap(callback);
    }

    template<typename Callback>
    void forEachModification(Callback&& callback) const {
        std::shared_lock lock(mutex_);
        modifications_.forEach(callback);
    }

    template<typename Callback>
    void forEachRangeWithBitmap(Callback&& callback) const {
        std::shared_lock lock(mutex_);
        ranges_.forEachWithBitmap(callback);
    }

    template<typename Callback>
    void forEachRange(Callback&& callback) const {
        std::shared_lock lock(mutex_);
        ranges_.forEach(callback);
    }

    /// Nothing to visit, and no lock taken, while the lane is empty.
    template<typename Callback>
    void forEachRowLeftWithBitmap(Callback&& callback) const {
        if (rows_left_.skippable()) return;
        std::shared_lock lock(mutex_);
        rows_left_.forEachWithBitmap(callback);
    }

    /// Nothing to visit, and no lock taken, while the lane is empty.
    template<typename Callback>
    void forEachRowLeft(Callback&& callback) const {
        if (rows_left_.skippable()) return;
        std::shared_lock lock(mutex_);
        rows_left_.forEach(callback);
    }

    /// Check if there are modifications since the given generation.
    /// Use this for short-circuit optimization before iterating.
    [[nodiscard]] bool hasModificationsSince(uint32_t since_gen) const {
        return latest_generation_.load(std::memory_order_acquire) > since_gen;
    }

    // =========================================================================
    // Query state
    // =========================================================================

    [[nodiscard]] bool empty() const {
        std::shared_lock lock(mutex_);
        return modifications_.empty() && ranges_.empty() && rows_left_.empty();
    }

    [[nodiscard]] size_t size() const {
        std::shared_lock lock(mutex_);
        return modifications_.size();
    }

    [[nodiscard]] size_t rangeCount() const {
        std::shared_lock lock(mutex_);
        return ranges_.size();
    }

    [[nodiscard]] size_t rowLeftCount() const {
        std::shared_lock lock(mutex_);
        return rows_left_.size();
    }

    [[nodiscard]] uint32_t latestGeneration() const {
        return latest_generation_.load(std::memory_order_acquire);
    }

    [[nodiscard]] static constexpr BitmapType initialBitmap() {
        return ModificationLane::initialBitmap();
    }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::relais::list

#endif  // JCX_RELAIS_LIST_MODIFICATIONTRACKER_H
