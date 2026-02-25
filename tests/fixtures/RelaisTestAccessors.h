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
        cache.full_cleanup([](const auto&, const auto&) { return true; });
    }

    /// Reset list cache state: clear ChunkMap entries and modifications.
    template<typename Repo>
    static void resetListCacheState() {
        auto& cache = Repo::listCache();
        cache.cache_.full_cleanup([](const auto&, const auto&) { return true; });
        resetModificationTracker(cache.modifications_);
    }

    /// Get number of pending modifications in the modification tracker.
    template<typename Repo>
    static size_t pendingModificationCount() {
        return Repo::listCache().modifications_.size();
    }

    /// Number of chunks in the list cache (= number of cleanup cycles to drain all bitmap bits).
    template<typename Repo>
    static constexpr size_t listCacheChunkCount() {
        return std::remove_reference_t<decltype(Repo::listCache())>::ChunkCount;
    }

    /// Force a modification tracker cleanup cycle (partial, one chunk).
    template<typename Repo>
    static void forceModificationTrackerCleanup() {
        Repo::listCache().trySweep();
    }

    /// Full cleanup of list cache only (entity cache untouched).
    /// Processes all chunks + drains modification tracker.
    template<typename Repo>
    static size_t forceFullListCleanup() {
        return Repo::listCache().purge();
    }

    /// Call ModificationTracker::drainChunk() directly with a controlled cutoff and chunk identity.
    /// Clears the bit for chunk_id in modifications with modified_at <= cutoff.
    template<typename Repo>
    static void cleanupModificationsWithCutoff(
            std::chrono::steady_clock::time_point cutoff, uint8_t chunk_id) {
        Repo::listCache().modifications_.drainChunk(cutoff, chunk_id);
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
    static void putInCache(const Key& key, const typename Repo::EntityType& entity) {
        Repo::putInCache(key, entity);
    }

    /// Direct L1 cache invalidate.
    template<typename Repo, typename Key>
    static void evict(const Key& key) {
        Repo::evict(key);
    }

    /// Force epoch GC on entity cache pools (flushes deferred destructors).
    template<typename Repo>
    static void collectEntityCache() {
        Repo::cache().collect();
    }

    /// Read the chunk_id for a cached list entry (for bitmap skip testing).
    /// Computed by ChunkMap from the key (deterministic). Returns nullopt if not in cache.
    template<typename Repo>
    static std::optional<uint8_t> getListEntryChunkId(const std::string& cache_key) {
        auto& cache = Repo::listCache().cache_;
        constexpr long n_chunks = static_cast<long>(
            std::remove_reference_t<decltype(Repo::listCache())>::ChunkCount);
        long chunk = cache.key_chunk(cache_key, n_chunks);
        if (chunk < 0) return std::nullopt;
        return static_cast<uint8_t>(chunk);
    }

    // =========================================================================
    // GDSF state access
    // =========================================================================

    /// Reset GDSF global state (generations, memory counters, correction).
    static void resetGDSF() {
        jcailloux::relais::cache::GDSFPolicy::instance().reset();
    }

    /// Reset per-repo GDSF state (avg_construction_time).
    template<typename Repo>
    static void resetRepoGDSFState() {
        Repo::avg_construction_time_us_.store(0.0f, std::memory_order_relaxed);
    }

    /// Type-erased GDSF metadata for test assertions.
    struct GDSFTestMetadata {
        uint32_t access_count{0};
        int64_t ttl_expiration_rep{0};
    };

    /// Get GDSF metadata for a cached entity (read-only, no score bump).
    /// Returns type-erased metadata compatible with all CacheMetadata variants.
    template<typename Repo, typename Key>
    static std::optional<GDSFTestMetadata> getEntityGDSFMetadata(const Key& key) {
        auto result = Repo::cache().find(key);
        if (!result) return std::nullopt;

        auto& meta = result.entry->metadata;
        GDSFTestMetadata m;
        if constexpr (requires { meta.access_count; }) {
            m.access_count = meta.access_count.load(std::memory_order_relaxed);
        }
        if constexpr (requires { meta.ttl_expiration_rep; }) {
            m.ttl_expiration_rep = meta.ttl_expiration_rep;
        }
        return m;
    }

    /// Compute the GDSF score for a cached entity (access_count x avg_cost / memoryUsage).
    /// Returns nullopt if the entity is not in cache.
    template<typename Repo, typename Key>
    static std::optional<float> getEntityGDSFScore(const Key& key) {
        auto result = Repo::cache().find(key);
        if (!result) return std::nullopt;

        auto* ce = result.asReal();
        if (!ce) return std::nullopt;

        auto& meta = ce->metadata;
        auto& value = ce->value;
        if constexpr (requires { meta.access_count; }) {
            float avg_cost = Repo::avgConstructionTime();
            size_t mem = value.memoryUsage();
            return meta.computeScore(avg_cost, mem);
        } else {
            return 0.0f;
        }
    }

    // =========================================================================
    // Ghost entry access (GDSF admission control testing)
    // =========================================================================

    struct GhostTestData {
        uint32_t access_count{0};
        uint32_t estimated_bytes{0};
        uint8_t flags{0};
    };

    /// Check if an entry is a ghost (admission-control placeholder).
    template<typename Repo, typename Key>
    static bool isGhostEntry(const Key& key) {
        auto result = Repo::cache().find(key);
        if (!result) return false;
        return result.entry->metadata.isGhost();
    }

    /// Get ghost data for a cached ghost entry.
    template<typename Repo, typename Key>
    static std::optional<GhostTestData> getGhostData(const Key& key) {
        auto result = Repo::cache().find(key);
        if (!result) return std::nullopt;
        auto* ge = result.asGhost();
        if (!ge) return std::nullopt;
        GhostTestData d;
        d.access_count = ge->metadata.rawCount();
        d.estimated_bytes = ge->value.estimated_bytes.load(std::memory_order_relaxed);
        d.flags = ge->value.flags.load(std::memory_order_relaxed);
        return d;
    }

    /// Set the GDSF eviction threshold directly (test-only).
    static void setThreshold(float t) {
        jcailloux::relais::cache::GDSFPolicy::instance().cached_threshold_.store(
            t, std::memory_order_relaxed);
    }

    /// Seed the average construction time for a repo (test-only).
    template<typename Repo>
    static void seedAvgConstructionTime(float us) {
        Repo::avg_construction_time_us_.store(us, std::memory_order_relaxed);
    }

    /// Ghost overhead in bytes for a specific repo.
    template<typename Repo>
    static constexpr size_t ghostOverhead() {
        return Repo::kGhostOverhead;
    }

    // =========================================================================
    // Synchronous notify helpers — L1 sync + L2 awaited (not fire-and-forget)
    // =========================================================================

    /// Synchronous notifyCreated: L1 inline + L2 co_await (not DetachedTask).
    /// Returns number of L2 pages deleted. Requires sync() from test_helper.h.
    template<typename Repo>
    static size_t notifyCreatedSync(const typename Repo::EntityType& entity) {
        constexpr auto level = Repo::config.cache_level;
        constexpr bool hasL1 = level == jcailloux::relais::config::CacheLevel::L1
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;
        constexpr bool hasL2 = level == jcailloux::relais::config::CacheLevel::L2
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;

        if constexpr (hasL1) { Repo::listCache().onEntityCreated(entity); }
        if constexpr (hasL2) { return sync(Repo::invalidateL2Created(entity)); }
        return 0;
    }

    /// Synchronous notifyUpdated: L1 inline + L2 co_await.
    template<typename Repo>
    static size_t notifyUpdatedSync(const typename Repo::EntityType& old_e,
                                    const typename Repo::EntityType& new_e) {
        constexpr auto level = Repo::config.cache_level;
        constexpr bool hasL1 = level == jcailloux::relais::config::CacheLevel::L1
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;
        constexpr bool hasL2 = level == jcailloux::relais::config::CacheLevel::L2
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;

        if constexpr (hasL1) { Repo::listCache().onEntityUpdated(old_e, new_e); }
        if constexpr (hasL2) { return sync(Repo::invalidateL2Updated(old_e, new_e)); }
        return 0;
    }

    /// Synchronous notifyDeleted: L1 inline + L2 co_await.
    template<typename Repo>
    static size_t notifyDeletedSync(const typename Repo::EntityType& entity) {
        constexpr auto level = Repo::config.cache_level;
        constexpr bool hasL1 = level == jcailloux::relais::config::CacheLevel::L1
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;
        constexpr bool hasL2 = level == jcailloux::relais::config::CacheLevel::L2
                            || level == jcailloux::relais::config::CacheLevel::L1_L2;

        if constexpr (hasL1) { Repo::listCache().onEntityDeleted(entity); }
        if constexpr (hasL2) { return sync(Repo::invalidateL2Deleted(entity)); }
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
