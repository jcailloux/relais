#ifndef JCAILLOUX_DROGON_LIST_MODIFICATIONTRACKER_H
#define JCAILLOUX_DROGON_LIST_MODIFICATIONTRACKER_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <type_traits>
#include <vector>

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::drogon::cache::list {

// =============================================================================
// EntityModification - Represents a modification to an entity
// =============================================================================

template<typename Entity>
struct EntityModification {
    using EntityPtr = std::shared_ptr<const Entity>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    enum class Type : uint8_t {
        Created,
        Updated,
        Deleted
    };

    Type type;
    EntityPtr old_entity;   // nullptr for Created
    EntityPtr new_entity;   // nullptr for Deleted
    TimePoint modified_at;

    // Factory methods
    static EntityModification created(EntityPtr entity) {
        return EntityModification{
            .type = Type::Created,
            .old_entity = nullptr,
            .new_entity = std::move(entity),
            .modified_at = Clock::now()
        };
    }

    static EntityModification updated(EntityPtr old_entity, EntityPtr new_entity) {
        return EntityModification{
            .type = Type::Updated,
            .old_entity = std::move(old_entity),
            .new_entity = std::move(new_entity),
            .modified_at = Clock::now()
        };
    }

    static EntityModification deleted(EntityPtr entity) {
        return EntityModification{
            .type = Type::Deleted,
            .old_entity = std::move(entity),
            .new_entity = nullptr,
            .modified_at = Clock::now()
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
}

// =============================================================================
// ModificationTracker - Bitmap-based tracker for list cache invalidation
// =============================================================================
//
// Each modification tracks a bitmap of pending shard identities.
// When a shard is cleaned, its bit is cleared. When all bits are 0,
// all shards have seen this modification and it can be removed.
//
// TotalSegments = number of shards, known at compile time (from ShardMap config).
//

template<typename Entity, size_t TotalSegments>
class ModificationTracker {
public:
    static_assert(TotalSegments >= 2 && TotalSegments <= 64,
                  "TotalSegments must be between 2 and 64");

    using Modification = EntityModification<Entity>;
    using EntityPtr = typename Modification::EntityPtr;
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using BitmapType = detail::SmallestUintFor<TotalSegments>;

    static constexpr BitmapType initial_bitmap_ =
        TotalSegments >= sizeof(BitmapType) * 8
            ? static_cast<BitmapType>(~BitmapType{0})
            : static_cast<BitmapType>((BitmapType{1} << TotalSegments) - 1);

    /// Wrapper that tracks which segments have seen this modification via a bitmap.
    struct TrackedModification {
        Modification modification;
        alignas(std::atomic_ref<BitmapType>::required_alignment)
        mutable BitmapType pending_segments;
    };

private:
    std::vector<TrackedModification> modifications_;
    mutable std::shared_mutex mutex_;
    std::atomic<TimePoint> latest_modification_time_{TimePoint::min()};

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

    void notifyCreated(EntityPtr entity) {
        track(Modification::created(std::move(entity)));
    }

    void notifyUpdated(EntityPtr old_entity, EntityPtr new_entity) {
        track(Modification::updated(std::move(old_entity), std::move(new_entity)));
    }

    void notifyDeleted(EntityPtr entity) {
        track(Modification::deleted(std::move(entity)));
    }

private:
    void track(Modification mod) {
        // Update latest modification time (atomic max)
        TimePoint current_latest = latest_modification_time_.load(std::memory_order_relaxed);
        while (mod.modified_at > current_latest &&
               !latest_modification_time_.compare_exchange_weak(
                   current_latest, mod.modified_at,
                   std::memory_order_release, std::memory_order_relaxed)) {
        }

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

    /// Called after each successful try_cleanup() of the cache.
    /// Clears the bit for shard_id in each modification's bitmap.
    /// Removes modifications whose bitmap becomes 0 (all shards processed).
    ///
    /// Only modifications with modified_at <= cutoff are processed. The cutoff
    /// must be captured BEFORE the shard cleanup, so that modifications added
    /// during cleanup are excluded and not prematurely drained.
    ///
    /// Two-phase approach:
    /// - Phase 1 (shared_lock): clear bits via atomic_ref, collect fully-drained indices.
    ///   Concurrent with forEachModificationWithBitmap — no conflict because
    ///   forEachModificationWithBitmap reads via atomic_ref too.
    /// - Phase 2 (unique_lock): remove expired entries via swap-with-last.
    ///   Only taken when there are actual removals.
    void cleanup(TimePoint cutoff, uint8_t shard_id) {
        std::vector<size_t> to_remove;
        const BitmapType shard_bit = BitmapType{1} << shard_id;

        {
            std::shared_lock lock(mutex_);
            for (size_t i = 0; i < modifications_.size(); ++i) {
                if (modifications_[i].modification.modified_at > cutoff) continue;

                std::atomic_ref<BitmapType> bitmap(modifications_[i].pending_segments);
                BitmapType remaining = bitmap.fetch_and(
                    static_cast<BitmapType>(~shard_bit), std::memory_order_relaxed)
                    & static_cast<BitmapType>(~shard_bit);

                if (remaining == 0) {
                    to_remove.push_back(i);
                }
            }
        }

        if (!to_remove.empty()) {
            std::unique_lock lock(mutex_);
            for (auto it = to_remove.rbegin(); it != to_remove.rend(); ++it) {
                size_t idx = *it;
                if (idx < modifications_.size()) {
                    if (idx != modifications_.size() - 1) {
                        std::swap(modifications_[idx], modifications_.back());
                    }
                    modifications_.pop_back();
                }
            }
        }
    }

    /// Remove all modifications with modified_at <= cutoff in one pass.
    /// Used by fullCleanup() after processing all segments at once.
    void drain(TimePoint cutoff) {
        std::unique_lock lock(mutex_);
        std::erase_if(modifications_, [cutoff](const TrackedModification& t) {
            return t.modification.modified_at <= cutoff;
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

    /// Check if there are modifications since the given time.
    /// Use this for short-circuit optimization before iterating.
    [[nodiscard]] bool hasModificationsSince(TimePoint since) const {
        return latest_modification_time_.load(std::memory_order_acquire) > since;
    }

    // =========================================================================
    // Query state
    // =========================================================================

    [[nodiscard]] bool empty() const {
        std::shared_lock lock(mutex_);
        return modifications_.empty();
    }

    [[nodiscard]] size_t size() const {
        std::shared_lock lock(mutex_);
        return modifications_.size();
    }

    [[nodiscard]] TimePoint latestModificationTime() const {
        return latest_modification_time_.load(std::memory_order_acquire);
    }

    [[nodiscard]] static constexpr BitmapType initialBitmap() { return initial_bitmap_; }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::drogon::cache::list

#endif  // JCAILLOUX_DROGON_LIST_MODIFICATIONTRACKER_H
