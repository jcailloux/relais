#pragma once

/**
 * RelaisTestAccessors.h
 *
 * Test-only accessor for relais internal state.
 * Compiled only when RELAIS_BUILDING_TESTS is defined.
 * Provides cache reset, modification count inspection, forced cleanup,
 * and GDSF policy access via friend access to LocalRepo, ListMixin,
 * ListCache, ModificationTracker, and GDSFPolicy.
 */

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <vector>
#include <jcailloux/relais/cache/GDSFPolicy.h>
#include <jcailloux/relais/io/Task.h>
#include <jcailloux/relais/io/pg/SettleTicket.h>

namespace relais_test {

struct TestInternals {
    /// Reset entity L1 cache: unconditionally remove all entries.
    template<typename Repo>
    static void resetEntityCacheState() {
        auto& cache = Repo::cache();
        cache.full_cleanup([](const auto&, const auto&) { return true; });
        auto& notes = Repo::notes();
        std::unique_lock lock(notes.mutex_);
        notes.lane_.clear();
        notes.draining_ = false;
        notes.hold_drain_ = false;
        notes.hit_checks_.store(0, std::memory_order_relaxed);
    }

    /// Notes on the entity tier (rows an uncertain write may have changed).
    template<typename Repo>
    static size_t rowNoteCount() { return Repo::notes().size(); }

    /// Whether a hit pays more than the note count's load.
    template<typename Repo>
    static bool rowNotesActive() { return Repo::notes().active(); }

    /// Hits that checked a copy against the notes.
    template<typename Repo>
    static uint64_t rowNoteHitChecks() {
        return Repo::notes().hit_checks_.load(std::memory_order_relaxed);
    }

    /// Keep the drain pass from starting (false: start it if a note is settled).
    template<typename Repo>
    static void holdRowNoteDrain(bool hold) {
        {
            auto& notes = Repo::notes();
            std::unique_lock lock(notes.mutex_);
            notes.hold_drain_ = hold;
        }
        if (!hold) Repo::startNoteDrain();
    }

    /// Leave a note, as an uncertain write does.
    template<typename Repo, typename Pred>
    static void noteRowsChanged(const std::shared_ptr<jcailloux::relais::io::SettleTicket>& ticket,
                                Pred pred) {
        Repo::noteRowsChanged(ticket, std::move(pred));
    }

    /// What the repo's catalog read says of its table's columns.
    template<typename Repo>
    static const jcailloux::relais::detail::ColumnFacts& columnFacts() {
        return Repo::columnFacts();
    }

    /// Forget the column facts, so that the next readColumnFacts() reads the
    /// catalog again, and make its next `faults` reads fail.
    template<typename Repo>
    static void resetColumnFacts(int faults = 0) {
        const_cast<jcailloux::relais::detail::ColumnFacts&>(Repo::columnFacts()).reset();
        Repo::column_facts_faults = faults;
    }

    /// The tiers' response to an uncertain predicate write (rows it may have
    /// changed, values it set).
    template<typename Repo, typename Pred, typename... Updates>
    static void rowsMayHaveChanged(const std::shared_ptr<jcailloux::relais::io::SettleTicket>& ticket,
                                   Pred pred, const Updates&... updates) {
        Repo::rowsMayHaveChanged(ticket, std::move(pred), updates...);
    }

    /// Read the column facts now (on this loop thread) if they are not known.
    template<typename Repo>
    static void readColumnFacts() { Repo::readColumnFacts(); }

    /// Reset list cache state: clear ChunkMap entries and modifications.
    template<typename Repo>
    static void resetListCacheState() {
        auto& cache = Repo::listCache();
        cache.tier_.map().full_cleanup([](const auto&, const auto&) { return true; });
        resetModificationTracker(cache.modifications_);
    }

    /// Get number of pending modifications in the modification tracker.
    template<typename Repo>
    static size_t pendingModificationCount() {
        return Repo::listCache().modifications_.size();
    }

    /// The L1 list cache of a repo, to drive it directly.
    template<typename Repo>
    static auto& listCacheOf() {
        return Repo::listCache();
    }

    /// Rows that left their pages (uncertain writes) still tracked.
    template<typename Repo>
    static size_t pendingRowLeftCount() {
        return Repo::listCache().modifications_.rowLeftCount();
    }

    /// Makes the next `n` rereads of a settled row fail.
    template<typename Repo>
    static void failRereads(int n) {
        Repo::reread_faults = n;
    }

    /// Number of pages held by the L1 list cache.
    template<typename Repo>
    static size_t listCacheSize() {
        return Repo::listCache().size();
    }

    /// Number of pending predicate range modifications (eraseWhere fast-path).
    /// One eraseWhere should add exactly one — proves O(1), not N per-entity mods.
    template<typename Repo>
    static size_t pendingRangeCount() {
        return Repo::listCache().modifications_.rangeCount();
    }

    /// Number of chunks in the list cache (= number of cleanup cycles to drain all bitmap bits).
    template<typename Repo>
    static constexpr size_t listCacheChunkCount() {
        return std::remove_reference_t<decltype(Repo::listCache())>::ChunkCount;
    }

    /// Drain every chunk bit of the modification tracker for `Repo`.
    /// Deterministic: bypasses the global GDSF sweep cursor and sweep_flag_
    /// — does not interfere with background sweep threads.
    /// After this call, every modification with generation <= cutoff_gen is removed.
    template<typename Repo>
    static void drainAllModificationChunks(uint32_t cutoff_gen) {
        using Cache = std::remove_reference_t<decltype(Repo::listCache())>;
        auto& tracker = Repo::listCache().modifications_;
        for (uint8_t c = 0; c < Cache::ChunkCount; ++c) {
            tracker.drainChunk(cutoff_gen, c);
        }
    }

    /// Full cleanup of list cache only (entity cache untouched).
    /// Processes all chunks + drains modification tracker.
    template<typename Repo>
    static size_t forceFullListCleanup() {
        return Repo::listCache().purge();
    }

    /// Call ModificationTracker::drainChunk() directly with a controlled cutoff and chunk identity.
    /// Clears the bit for chunk_id in modifications with generation <= cutoff_gen.
    template<typename Repo>
    static void cleanupModificationsWithCutoff(uint32_t cutoff_gen, uint8_t chunk_id) {
        Repo::listCache().modifications_.drainChunk(cutoff_gen, chunk_id);
    }

    /// Call ModificationTracker::drain() directly with a controlled cutoff.
    /// Removes all modifications with generation <= cutoff_gen in one pass.
    template<typename Repo>
    static void drainModificationsWithCutoff(uint32_t cutoff_gen) {
        Repo::listCache().modifications_.drain(cutoff_gen);
    }

    /// Read the current generation counter of a list cache.
    /// Used by tests to capture a cutoff between modifications.
    template<typename Repo>
    static uint32_t listCacheGeneration() {
        return Repo::listCache().generation_.load(std::memory_order_relaxed);
    }

    /// Fetch a list page from the database, then store it as if its fetch had
    /// started at generation `fetch_gen`: every modification notified since
    /// straddles the fetch. Returns whether the page is left in the L1 list
    /// cache. Requires sync() from test_helper.h.
    template<typename Repo, typename Query>
    static bool putPageFetchedAt(const Query& query, uint32_t fetch_gen) {
        typename Repo::ListWrapperType wrapper;
        wrapper.items = sync(Repo::queryFromDb(query));
        auto cache_query = Repo::toCacheQuery(query, query.cacheKey());
        Repo::listCache().put(cache_query, std::move(wrapper), fetch_gen);
        return static_cast<bool>(Repo::listCache().get(cache_query));
    }

    /// The statement and parameters a list page read sends to PostgreSQL.
    template<typename Repo, typename Query>
    static auto listQuerySql(const Query& query) {
        return Repo::querySql(query);
    }

    /// Direct L1 cache get — bypasses coroutine overhead.
    /// Same path as find L1 hit, but synchronous (no sync_wait thread).
    template<typename Repo, typename Key>
    static auto getFromCache(const Key& key) {
        return Repo::getFromCache(key);
    }

    /// Reach the internal batched read (findManyRaw) — protected like findRaw,
    /// PgRepo befriends TestInternals. The returned Task is lazy: `ids` must
    /// outlive the sync() that drives it (a local test vector does).
    template<typename Repo, typename Key>
    static auto findManyRaw(std::span<const Key> ids) {
        return Repo::findManyRaw(ids);
    }

    /// Reach the internal batched erase (eraseManyRaw) — protected like eraseImpl.
    /// Returns the deleted entities (RETURNING). Lazy Task: `ids` must outlive
    /// the sync() that drives it.
    template<typename Repo, typename Key>
    static auto eraseManyRaw(std::span<const Key> ids) {
        return Repo::eraseManyRaw(ids);
    }

    /// Reach the internal predicate erase (eraseWhereRaw). Descriptor is the
    /// repo's augmented ListDescriptorType (carries the Entity alias the filter
    /// helpers resolve). FiltersT stays deduced so this header need not include
    /// GeneratedCriteria. Lazy Task: `filters` must outlive the sync().
    template<typename Repo, typename Descriptor, typename FiltersT>
    static auto eraseWhereRaw(const FiltersT& filters) {
        return Repo::template eraseWhereRaw<Descriptor>(filters);
    }

    /// Reach the batch invalidation cascade — the shared downstream of
    /// invalidateMany/eraseMany: L1 evict + list invalidation + deduplicated
    /// cross-inval. Drives BOTH passes (critical then deferred) awaited, so the
    /// test observes the full effect deterministically — the production facade
    /// awaits only the critical pass and fires the deferred one fire-and-forget.
    /// Takes the already-resolved affected set; the coroutine copies the span
    /// into its frame, so `entities` must outlive the sync().
    template<typename Repo, typename Ent>
    static jcailloux::relais::io::Task<void> invalidateManyImpl(
        std::span<const Ent> entities) {
        co_await Repo::template invalidateManyCritical<true>(entities);
        co_await Repo::invalidateManyDeferred(entities);
    }

    /// Detached counterpart of invalidateManyImpl — the exact production facade
    /// shape: await ONLY the critical pass, then fire the deferred pass
    /// (cross-target) fire-and-forget. Lets a benchmark measure caller
    /// time-to-return (critical only) against the drained invalidateManyImpl
    /// (critical + deferred). Takes the same borrowed span as invalidateManyImpl
    /// (symmetric caller-side cost); the fired deferred owns its own copy so the
    /// set stays valid across every suspend — mirroring the facade, which moves
    /// an already-owned vector into the DetachedTask.
    template<typename Repo, typename Ent>
    static jcailloux::relais::io::Task<void> invalidateManyDetached(
        std::span<const Ent> entities) {
        co_await Repo::template invalidateManyCritical<true>(entities);
        fireInvalidateManyDeferred<Repo, Ent>(
            std::vector<Ent>(entities.begin(), entities.end()));
    }

    template<typename Repo, typename Ent>
    static jcailloux::relais::io::DetachedTask fireInvalidateManyDeferred(
        std::vector<Ent> entities) {
        try {
            co_await Repo::invalidateManyDeferred(
                std::span<const Ent>(entities));
        } catch (...) {}
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

    /// Total entries in the cache (live + ghost). For size() vs ghost testing.
    template<typename Repo>
    static long totalEntityCacheEntries() {
        return Repo::cache().totalEntries();
    }

    /// Destroy all deferred entries in the epoch pool.
    /// Only safe after resetEntityCacheState (map is empty).
    template<typename Repo>
    static void clearEntityCachePools() {
        auto& cache = Repo::cache();
        cache.pool_.clear();
    }

    /// Read the chunk_id for a cached list entry (for bitmap skip testing).
    /// Computed by ChunkMap from the key (deterministic). Returns nullopt if not in cache.
    template<typename Repo>
    static std::optional<uint8_t> getListEntryChunkId(const std::string& cache_key) {
        auto& cache = Repo::listCache().tier_.map();
        constexpr long n_chunks = static_cast<long>(
            std::remove_reference_t<decltype(Repo::listCache())>::ChunkCount);
        long chunk = cache.key_chunk(cache_key, n_chunks);
        if (chunk < 0) return std::nullopt;
        return static_cast<uint8_t>(chunk);
    }

    /// The generation a cached page is checked from: modifications up to it
    /// are known not to touch it. nullopt if the page is not cached.
    template<typename Repo>
    static std::optional<uint32_t> listPageGeneration(const std::string& cache_key) {
        auto r = Repo::listCache().tier_.map().find(cache_key);
        if (!r || r.isGhost()) return std::nullopt;
        return r.asReal()->metadata.stored_generation;
    }

    /// The chunk of a cached page when the list table is split in `n_chunks`.
    template<typename Repo>
    static std::optional<long> listEntryChunkIn(const std::string& cache_key, long n_chunks) {
        long chunk = Repo::listCache().tier_.map().key_chunk(cache_key, n_chunks);
        if (chunk < 0) return std::nullopt;
        return chunk;
    }

    /// Full GDSF-safe cache reset: remove all entries,
    /// drain epoch pool, then reset all GDSF state (including memory counters).
    template<typename Repo>
    static void resetCacheForGDSF() {
        resetEntityCacheState<Repo>();              // remove all entries
        clearEntityCachePools<Repo>();              // drain epoch pool
        resetRepoGDSFState<Repo>();                 // reset avg_construction_time
        resetGDSF();                                // reset everything (histograms + memory)
    }

    // =========================================================================
    // GDSF state access
    // =========================================================================

    /// Reset GDSF global state (generations, memory counters, correction).
    static void resetGDSF() {
        jcailloux::relais::cache::GDSFPolicy::instance().reset();
    }

    /// Ticks counted toward the maintenance sweep since the last reset.
    static uint32_t sweepTicks() {
        return jcailloux::relais::cache::GDSFPolicy::instance()
            .insertion_counter_.load(std::memory_order_relaxed);
    }

    /// Ticks of one full sweep cycle: every chunk of the policy swept once.
    static size_t sweepCycleTicks() {
        using Policy = jcailloux::relais::cache::GDSFPolicy;
        return (Policy::kCleanupMask + 1)
            * static_cast<size_t>(Policy::instance().chunkCount());
    }

    /// Reset per-repo GDSF state (avg_construction_time).
    template<typename Repo>
    static void resetRepoGDSFState() {
        Repo::tier().avg_cost_us_.store(0.0f, std::memory_order_relaxed);
    }

    /// Type-erased GDSF metadata for test assertions.
    struct GDSFTestMetadata {
        uint32_t access_count{0};
        uint32_t ttl_expiration_sec{0};
    };

    /// Get GDSF metadata for a cached entity (read-only, no score bump).
    /// Returns type-erased metadata compatible with all CacheMetadata variants.
    /// Returns nullopt for ghosts and missing entries.
    template<typename Repo, typename Key>
    static std::optional<GDSFTestMetadata> getEntityGDSFMetadata(const Key& key) {
        auto result = Repo::cache().find(key);
        if (!result || result.isGhost()) return std::nullopt;

        auto* header = result.entry();
        auto& meta = header->metadata;
        GDSFTestMetadata m;
        if constexpr (requires { meta.access_count; }) {
            m.access_count = meta.access_count.load(std::memory_order_relaxed);
        }
        if constexpr (requires { meta.ttl_expiration_sec; }) {
            m.ttl_expiration_sec = meta.ttl_expiration_sec;
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
        return result.isGhost();
    }

    /// Get ghost data for a cached ghost entry.
    /// Ghost data is read inline from the TaggedEntry (zero pointer dereference).
    template<typename Repo, typename Key>
    static std::optional<GhostTestData> getGhostData(const Key& key) {
        auto result = Repo::cache().find(key);
        if (!result || !result.isGhost()) return std::nullopt;
        GhostTestData d;
        d.access_count = result.ghostCount();
        d.estimated_bytes = result.ghostBytes();
        d.flags = result.ghostFlags();
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
        Repo::tier().avg_cost_us_.store(us, std::memory_order_relaxed);
    }

    /// Bucket slot size in bytes for a specific repo.
    /// Ghost entries no longer carry a charge — the bucket slot is tracked
    /// by the ParlayHash memory hook. This constant is exposed for test
    /// assertions on histogram entries and score denominators.
    template<typename Repo>
    static constexpr size_t bucketSlotSize() {
        using TierType = std::remove_reference_t<decltype(Repo::tier())>;
        return TierType::kBucketSlotSize;
    }

private:
    static void resetModificationTracker(auto& t) {
        std::unique_lock lock(t.mutex_);
        t.modifications_.clear();
        t.ranges_.clear();  // predicate range deletes (eraseWhere fast-path)
        t.rows_left_.clear();
        t.latest_generation_.store(0, std::memory_order_relaxed);
    }
};

} // namespace relais_test
