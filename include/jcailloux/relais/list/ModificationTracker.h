#ifndef JCX_RELAIS_LIST_MODIFICATIONTRACKER_H
#define JCX_RELAIS_LIST_MODIFICATIONTRACKER_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <type_traits>
#include <vector>

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
    uint32_t generation;    // monotonic generation number from the owning ListCache

    // Factory methods — caller provides the generation number
    static EntityModification created(const E& entity, uint32_t gen) {
        return EntityModification{
            .type = Type::Created,
            .old_entity = nullptr,
            .new_entity = std::make_unique<const E>(entity),
            .generation = gen
        };
    }

    static EntityModification updated(const E& old_entity, const E& new_entity, uint32_t gen) {
        return EntityModification{
            .type = Type::Updated,
            .old_entity = std::make_unique<const E>(old_entity),
            .new_entity = std::make_unique<const E>(new_entity),
            .generation = gen
        };
    }

    static EntityModification deleted(const E& entity, uint32_t gen) {
        return EntityModification{
            .type = Type::Deleted,
            .old_entity = std::make_unique<const E>(entity),
            .new_entity = nullptr,
            .generation = gen
        };
    }
};

// =============================================================================
// SmallestUintFor - Compile-time bitmap type selection
// =============================================================================

namespace detail {
    template<size_t N>
    using SmallestUintFor = std::conditional_t<(N <= 8), uint8_t,
                            std::conditional_t<(N <= 16), uint16_t,
                            std::conditional_t<(N <= 32), uint32_t, uint64_t>>>;

    /// Default range-payload — an entity-only tracker carries no predicate ranges.
    struct NoRangePayload {};
}

// =============================================================================
// ModificationTracker - Bitmap-based tracker for list cache invalidation
// =============================================================================
//
// Each modification tracks a bitmap of pending chunk identities.
// When a chunk is cleaned, its bit is cleared. When all bits are 0,
// all chunks have seen this modification and it can be erased.
//
// Uses monotonic generation numbers instead of timestamps.
// Generation numbers come from the owning ListCache's atomic counter.
//
// TotalSegments = number of chunks, known at compile time (from ChunkMap config).
//

template<typename E, size_t TotalSegments,
         typename RangePayload = detail::NoRangePayload>
class ModificationTracker {
public:
    static_assert(TotalSegments >= 2 && TotalSegments <= 64,
                  "TotalSegments must be between 2 and 64");

    using Modification = EntityModification<E>;
    using BitmapType = detail::SmallestUintFor<TotalSegments>;

    static constexpr BitmapType initial_bitmap_ =
        TotalSegments >= sizeof(BitmapType) * 8
            ? static_cast<BitmapType>(~BitmapType{0})
            : static_cast<BitmapType>((BitmapType{1} << TotalSegments) - 1);

    /// Wrapper that tracks which chunks have seen this modification via a bitmap.
    struct TrackedModification {
        Modification modification;
        alignas(std::atomic_ref<BitmapType>::required_alignment)
        mutable BitmapType pending_segments;
    };

    /// Predicate range modification (eraseWhere fast-path): one entry stands in
    /// for an unbounded set of deletes matching a predicate, instead of N entity
    /// modifications. Shares the generation counter and the per-chunk bitmap
    /// drain lifecycle with entity modifications.
    struct TrackedRange {
        RangePayload predicate;
        uint32_t generation;
        alignas(std::atomic_ref<BitmapType>::required_alignment)
        mutable BitmapType pending_segments;
    };

private:
    std::vector<TrackedModification> modifications_;
    std::vector<TrackedRange> ranges_;
    mutable std::shared_mutex mutex_;
    std::atomic<uint32_t> latest_generation_{0};

    /// Atomic max into latest_generation_ (shared by entity + range tracks).
    void bumpLatest(uint32_t gen) {
        uint32_t current = latest_generation_.load(std::memory_order_relaxed);
        while (gen > current &&
               !latest_generation_.compare_exchange_weak(
                   current, gen,
                   std::memory_order_release, std::memory_order_relaxed)) {
        }
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

    void notifyCreated(const E& entity, uint32_t gen) {
        track(Modification::created(entity, gen));
    }

    void notifyUpdated(const E& old_entity, const E& new_entity, uint32_t gen) {
        track(Modification::updated(old_entity, new_entity, gen));
    }

    void notifyDeleted(const E& entity, uint32_t gen) {
        track(Modification::deleted(entity, gen));
    }

    /// Record a predicate range delete (eraseWhere). One entry covers every row
    /// matching `predicate`; consumed lazily like entity modifications.
    void notifyRangeDeleted(RangePayload predicate, uint32_t gen) {
        bumpLatest(gen);
        std::unique_lock lock(mutex_);
        ranges_.push_back(TrackedRange{
            .predicate = std::move(predicate),
            .generation = gen,
            .pending_segments = initial_bitmap_
        });
    }

private:
    void track(Modification mod) {
        bumpLatest(mod.generation);

        {
            std::unique_lock lock(mutex_);
            modifications_.push_back(TrackedModification{
                .modification = std::move(mod),
                .pending_segments = initial_bitmap_
            });
        }
    }

public:
    // =========================================================================
    // Cleanup lifecycle
    // =========================================================================

    /// Called after each successful cleanup_chunk() of the cache.
    /// Clears the bit for chunk_id in each modification's bitmap.
    /// Erases modifications whose bitmap becomes 0 (all chunks processed).
    ///
    /// Only modifications with generation <= cutoff_gen are processed. The cutoff
    /// must be captured BEFORE the chunk cleanup, so that modifications added
    /// during cleanup are excluded and not prematurely drained.
    ///
    /// Two-phase approach:
    /// - Phase 1 (shared_lock): clear bits via atomic_ref, collect fully-drained indices.
    ///   Concurrent with forEachModificationWithBitmap — no conflict because
    ///   forEachModificationWithBitmap reads via atomic_ref too.
    /// - Phase 2 (unique_lock): erase expired entries via swap-with-last.
    ///   Only taken when there are actual removals.
    void drainChunk(uint32_t cutoff_gen, uint8_t chunk_id) {
        std::vector<size_t> to_erase;
        std::vector<size_t> to_erase_ranges;
        const BitmapType chunk_bit = BitmapType{1} << chunk_id;
        const auto clear = [&](BitmapType& slot) -> bool {
            std::atomic_ref<BitmapType> bitmap(slot);
            BitmapType remaining = bitmap.fetch_and(
                static_cast<BitmapType>(~chunk_bit), std::memory_order_relaxed)
                & static_cast<BitmapType>(~chunk_bit);
            return remaining == 0;
        };

        {
            std::shared_lock lock(mutex_);
            for (size_t i = 0; i < modifications_.size(); ++i) {
                if (modifications_[i].modification.generation > cutoff_gen) continue;
                if (clear(modifications_[i].pending_segments)) to_erase.push_back(i);
            }
            for (size_t i = 0; i < ranges_.size(); ++i) {
                if (ranges_[i].generation > cutoff_gen) continue;
                if (clear(ranges_[i].pending_segments)) to_erase_ranges.push_back(i);
            }
        }

        if (to_erase.empty() && to_erase_ranges.empty()) return;

        std::unique_lock lock(mutex_);
        for (auto it = to_erase.rbegin(); it != to_erase.rend(); ++it) {
            size_t idx = *it;
            if (idx < modifications_.size()) {
                if (idx != modifications_.size() - 1) {
                    std::swap(modifications_[idx], modifications_.back());
                }
                modifications_.pop_back();
            }
        }
        for (auto it = to_erase_ranges.rbegin(); it != to_erase_ranges.rend(); ++it) {
            size_t idx = *it;
            if (idx < ranges_.size()) {
                if (idx != ranges_.size() - 1) {
                    std::swap(ranges_[idx], ranges_.back());
                }
                ranges_.pop_back();
            }
        }
    }

    /// Erase all modifications with generation <= cutoff_gen in one pass.
    /// Used by purge() after processing all chunks at once.
    void drain(uint32_t cutoff_gen) {
        std::unique_lock lock(mutex_);
        std::erase_if(modifications_, [cutoff_gen](const TrackedModification& t) {
            return t.modification.generation <= cutoff_gen;
        });
        std::erase_if(ranges_, [cutoff_gen](const TrackedRange& t) {
            return t.generation <= cutoff_gen;
        });
    }

    // =========================================================================
    // Iteration for lazy validation
    // =========================================================================

    /// Execute a callback for each modification with its bitmap.
    /// Callback signature: void(const Modification&, BitmapType pending_segments)
    /// Thread-safe: shared_lock allows concurrent readers.
    template<typename Callback>
    void forEachModificationWithBitmap(Callback&& callback) const {
        std::shared_lock lock(mutex_);
        for (const auto& tracked : modifications_) {
            std::atomic_ref<BitmapType> bitmap(tracked.pending_segments);
            callback(tracked.modification, bitmap.load(std::memory_order_relaxed));
        }
    }

    /// Execute a callback for each modification (without bitmap).
    template<typename Callback>
    void forEachModification(Callback&& callback) const {
        std::shared_lock lock(mutex_);
        for (const auto& tracked : modifications_) {
            callback(tracked.modification);
        }
    }

    /// Execute a callback for each predicate range modification with its bitmap.
    /// Callback signature: void(const RangePayload&, uint32_t generation, BitmapType).
    template<typename Callback>
    void forEachRangeWithBitmap(Callback&& callback) const {
        std::shared_lock lock(mutex_);
        for (const auto& tracked : ranges_) {
            std::atomic_ref<BitmapType> bitmap(tracked.pending_segments);
            callback(tracked.predicate, tracked.generation,
                     bitmap.load(std::memory_order_relaxed));
        }
    }

    /// Execute a callback for each predicate range modification (without bitmap).
    template<typename Callback>
    void forEachRange(Callback&& callback) const {
        std::shared_lock lock(mutex_);
        for (const auto& tracked : ranges_) {
            callback(tracked.predicate, tracked.generation);
        }
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
        return modifications_.empty() && ranges_.empty();
    }

    [[nodiscard]] size_t size() const {
        std::shared_lock lock(mutex_);
        return modifications_.size();
    }

    [[nodiscard]] size_t rangeCount() const {
        std::shared_lock lock(mutex_);
        return ranges_.size();
    }

    [[nodiscard]] uint32_t latestGeneration() const {
        return latest_generation_.load(std::memory_order_acquire);
    }

    [[nodiscard]] static constexpr BitmapType initialBitmap() { return initial_bitmap_; }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::relais::list

#endif  // JCX_RELAIS_LIST_MODIFICATIONTRACKER_H
