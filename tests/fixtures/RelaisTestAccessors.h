#pragma once

/**
 * RelaisTestAccessors.h
 *
 * Test-only accessor for relais internal state.
 * Compiled only when RELAIS_BUILDING_TESTS is defined.
 * Provides cache reset, modification count inspection, forced cleanup,
 * and GDSF policy access via friend access to CachedRepo, ListMixin,
 * ListCache, ModificationTracker, and GDSFPolicy.
 */

#include <optional>
#include <shared_mutex>
#include <jcailloux/relais/cache/GDSFPolicy.h>

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

    // =========================================================================
    // GDSF state access
    // =========================================================================

    /// Reset GDSF global state (generations, memory counters, correction).
    static void resetGDSF() {
        jcailloux::relais::cache::GDSFPolicy::instance().reset();
    }

    /// Reset per-repo GDSF state (avg_construction_time, repo_score).
    template<typename Repo>
    static void resetRepoGDSFState() {
        Repo::avg_construction_time_us_.store(0.0f, std::memory_order_relaxed);
        Repo::repo_score_.store(0.0f, std::memory_order_relaxed);
    }

    /// Set per-repo GDSF repo_score (for testing threshold-based eviction).
    template<typename Repo>
    static void setRepoScore(float score) {
        Repo::repo_score_.store(score, std::memory_order_relaxed);
    }

    /// Type-erased GDSF metadata for test assertions.
    struct GDSFTestMetadata {
        float score{0.0f};
        uint32_t last_generation{0};
        int64_t ttl_expiration_rep{0};
    };

    /// Get GDSF metadata for a cached entity (read-only, no score bump).
    /// Returns type-erased metadata compatible with all CacheMetadata variants.
    template<typename Repo, typename Key>
    static std::optional<GDSFTestMetadata> getEntityGDSFMetadata(const Key& key) {
        std::optional<GDSFTestMetadata> result;
        Repo::cache().get(key, [&](const auto&, auto& meta) {
            GDSFTestMetadata m;
            if constexpr (requires { meta.score; }) {
                m.score = meta.score.load(std::memory_order_relaxed);
                m.last_generation = meta.last_generation.load(std::memory_order_relaxed);
            }
            if constexpr (requires { meta.ttl_expiration_rep; }) {
                m.ttl_expiration_rep = meta.ttl_expiration_rep;
            }
            result = m;
            return jcailloux::shardmap::GetAction::Accept;
        });
        return result;
    }

    // =========================================================================
    // Synchronous notify helpers — L1 sync + L2 awaited (not fire-and-forget)
    // =========================================================================

    /// Synchronous notifyCreated: L1 inline + L2 co_await (not DetachedTask).
    /// Returns number of L2 pages deleted. Requires sync() from test_helper.h.
    template<typename Repo>
    static size_t notifyCreatedSync(typename Repo::WrapperPtrType entity) {
        constexpr auto level = Repo::config.cache_level;
        constexpr bool hasL1 = level == jcailloux::relais::config::CacheLevel::L1
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;
        constexpr bool hasL2 = level == jcailloux::relais::config::CacheLevel::L2
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;

        if constexpr (hasL1) { Repo::listCache().onEntityCreated(entity); }
        if constexpr (hasL2) { return sync(Repo::invalidateL2Created(*entity)); }
        return 0;
    }

    /// Synchronous notifyUpdated: L1 inline + L2 co_await.
    template<typename Repo>
    static size_t notifyUpdatedSync(typename Repo::WrapperPtrType old_e,
                                    typename Repo::WrapperPtrType new_e) {
        constexpr auto level = Repo::config.cache_level;
        constexpr bool hasL1 = level == jcailloux::relais::config::CacheLevel::L1
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;
        constexpr bool hasL2 = level == jcailloux::relais::config::CacheLevel::L2
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;

        if constexpr (hasL1) { Repo::listCache().onEntityUpdated(old_e, new_e); }
        if constexpr (hasL2) { return sync(Repo::invalidateL2Updated(*old_e, *new_e)); }
        return 0;
    }

    /// Synchronous notifyDeleted: L1 inline + L2 co_await.
    template<typename Repo>
    static size_t notifyDeletedSync(typename Repo::WrapperPtrType entity) {
        constexpr auto level = Repo::config.cache_level;
        constexpr bool hasL1 = level == jcailloux::relais::config::CacheLevel::L1
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;
        constexpr bool hasL2 = level == jcailloux::relais::config::CacheLevel::L2
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;

        if constexpr (hasL1) { Repo::listCache().onEntityDeleted(entity); }
        if constexpr (hasL2) { return sync(Repo::invalidateL2Deleted(*entity)); }
        return 0;
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
