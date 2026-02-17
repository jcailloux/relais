#pragma once

/**
 * RelaisTestAccessors.h
 *
 * Test-only accessor for relais internal state.
 * Compiled only when RELAIS_BUILDING_TESTS is defined.
 * Provides cache reset, modification count inspection, and forced cleanup
 * via friend access to CachedRepo, ListMixin, ListCache, and ModificationTracker.
 */

#include <shared_mutex>

namespace relais_test {

struct TestInternals {
    /// Reset entity L1 cache: unconditionally remove all entries.
    template<typename Repo>
    static void resetEntityCacheState() {
        auto& cache = Repo::cache();
        cache.full_cleanup(0,
            [](const auto&, const auto&, const auto&, const auto&) { return true; });
    }

    /// Reset list cache state: clear shardmap entries, modifications, and counter.
    template<typename Repo>
    static void resetListCacheState() {
        auto& cache = Repo::listCache();
        cache.cache_.full_cleanup(0,
            [](const auto&, const auto&, const auto&, const auto&) { return true; });
        resetModificationTracker(cache.modifications_);
        cache.get_counter_ = 0;
    }

    /// Get number of pending modifications in the modification tracker.
    template<typename Repo>
    static size_t pendingModificationCount() {
        return Repo::listCache().modifications_.size();
    }

    /// Number of shards in the list cache (= number of cleanup cycles to drain all bitmap bits).
    template<typename Repo>
    static constexpr size_t listCacheShardCount() {
        return std::remove_reference_t<decltype(Repo::listCache())>::ShardCount;
    }

    /// Force a modification tracker cleanup cycle (partial, one shard).
    template<typename Repo>
    static void forceModificationTrackerCleanup() {
        Repo::listCache().trySweep();
    }

    /// Full cleanup of list cache only (entity cache untouched).
    /// Processes all shards + drains modification tracker.
    template<typename Repo>
    static size_t forceFullListCleanup() {
        return Repo::listCache().purge();
    }

    /// Call ModificationTracker::cleanup() directly with a controlled cutoff and shard identity.
    /// Clears the bit for shard_id in modifications with modified_at <= cutoff.
    template<typename Repo>
    static void cleanupModificationsWithCutoff(
            std::chrono::steady_clock::time_point cutoff, uint8_t shard_id) {
        Repo::listCache().modifications_.drainShard(cutoff, shard_id);
    }

    /// Call ModificationTracker::drain() directly with a controlled cutoff.
    /// Removes all modifications with modified_at <= cutoff in one pass.
    template<typename Repo>
    static void drainModificationsWithCutoff(
            std::chrono::steady_clock::time_point cutoff) {
        Repo::listCache().modifications_.drain(cutoff);
    }

    /// Direct L1 cache get — bypasses coroutine overhead.
    /// Same path as find L1 hit, but synchronous (no sync_wait thread).
    template<typename Repo, typename Key>
    static auto getFromCache(const Key& key) {
        return Repo::getFromCache(key);
    }

    /// Direct L1 cache put — bypasses coroutine overhead.
    template<typename Repo, typename Key>
    static void putInCache(const Key& key, typename Repo::EntityPtr ptr) {
        Repo::putInCache(key, std::move(ptr));
    }

    /// Direct L1 cache invalidate.
    template<typename Repo, typename Key>
    static void evict(const Key& key) {
        Repo::evict(key);
    }

    /// Read the shard_id for a cached list entry (for bitmap skip testing).
    /// Computed by ShardMap from the key (deterministic). Returns nullopt if not in cache.
    template<typename Repo>
    static std::optional<uint8_t> getListEntryShardId(const std::string& cache_key) {
        auto& cache = Repo::listCache().cache_;
        uint8_t sid = 0;
        bool found = false;
        cache.get(cache_key, [&](const auto&, const auto&, uint8_t shard_id) {
            sid = shard_id;
            found = true;
            return jcailloux::shardmap::GetAction::Accept;
        });
        return found ? std::optional<uint8_t>{sid} : std::nullopt;
    }

private:
    static void resetModificationTracker(auto& t) {
        std::unique_lock lock(t.mutex_);
        t.modifications_.clear();
        t.latest_modification_time_.store(
            std::chrono::steady_clock::time_point::min(),
            std::memory_order_relaxed);
    }
};

} // namespace relais_test
