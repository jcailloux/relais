#ifndef JCX_RELAIS_CACHE_CHUNK_MAP_H
#define JCX_RELAIS_CACHE_CHUNK_MAP_H

#include <algorithm>
#include <atomic>
#include <tuple>
#include <variant>
#include <vector>

#include <parlay_hash/unordered_map.h>
#include <utils/epoch.h>

namespace jcailloux::relais::cache {

// =============================================================================
// Hash support for std::tuple (needed for composite primary keys)
// =============================================================================

namespace detail {

inline size_t hash_combine(size_t seed, size_t h) {
    return seed ^ (h + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

template<typename T>
struct AutoHash : std::hash<T> {};

template<typename... Ts>
struct AutoHash<std::tuple<Ts...>> {
    size_t operator()(const std::tuple<Ts...>& t) const {
        size_t seed = 0;
        std::apply([&](const auto&... args) {
            ((seed = hash_combine(seed, std::hash<std::decay_t<decltype(args)>>{}(args))), ...);
        }, t);
        return seed;
    }
};

}  // namespace detail

// =============================================================================
// ChunkMap<K, V, Metadata> — lock-free hash map with epoch-based reclamation
//
// Wraps ParlayHash (lock-free concurrent hash map) with:
// - epoch::memory_pool<CacheEntry> for safe deferred destruction
// - epoch::EpochGuard (ticket-based) for thread-agnostic read protection
// - Chunk-based partial cleanup for incremental eviction
//
// ParlayHash stores pair<K, CacheEntry*> directly in bucket buffers (trivially
// copyable). Our memory_pool manages CacheEntry lifetime independently.
//
// Thread-safe: all public methods are safe to call concurrently.
// =============================================================================

template<typename K, typename V, typename Metadata = std::monostate,
         typename Hash = detail::AutoHash<K>>
class ChunkMap {
public:
    struct CacheEntry {
        V value;
        [[no_unique_address]] Metadata metadata;
    };

    struct FindResult {
        CacheEntry* entry = nullptr;
        epoch::EpochGuard guard;
        bool was_insert = false;
        explicit operator bool() const { return entry != nullptr; }
    };

    using MapType = parlay::parlay_unordered_map<K, CacheEntry*, Hash>;
    using hashed_key = typename MapType::hashed_key;

    static hashed_key make_key(const K& key) { return MapType::make_key(key); }
    static size_t get_hash(const hashed_key& k) { return MapType::get_hash(k); }

    explicit ChunkMap(long initial_size = 128)
        : map_(*new MapType(initial_size, false)) {}

    // ChunkMap instances are static singletons (CachedRepo::cache(),
    // ListCache::cache_). Their destruction happens during static cleanup
    // when dependent singletons (epoch, GDSFPolicy) may already be destroyed.
    // Both the ParlayHash map and the memory_pool are heap-allocated and
    // intentionally never freed — the OS reclaims all process memory at exit.
    // ParlayHash's internal pools call get_epoch() on destruction, which would
    // crash if the epoch singleton is already destroyed.
    ~ChunkMap() = default;

    ChunkMap(const ChunkMap&) = delete;
    ChunkMap& operator=(const ChunkMap&) = delete;

    // =========================================================================
    // Lookup
    // =========================================================================

    /// Find entry by key. Returns epoch-guarded result.
    /// The returned CacheEntry* is valid as long as FindResult lives.
    FindResult find(const K& key) {
        auto guard = epoch::EpochGuard::acquire();
        auto opt = map_.Find(key);
        if (!opt) return {};
        return {*opt, std::move(guard)};
    }

    // =========================================================================
    // Mutations
    // =========================================================================

    /// Insert or replace entry. Returns epoch-guarded result pointing to the
    /// NEW entry. EpochGuard is acquired BEFORE the Upsert to protect the new
    /// entry from concurrent Upsert + Retire by another thread.
    FindResult upsert(const K& key, V value, Metadata meta = {}) {
        auto guard = epoch::EpochGuard::acquire();
        auto* new_entry = pool_.New(std::move(value), std::move(meta));
        auto old = map_.Upsert(key, [&](std::optional<CacheEntry*>) -> CacheEntry* {
            return new_entry;
        });
        bool inserted = !old.has_value();
        if (!inserted) {
            pool_.Retire(*old);
        }
        return {new_entry, std::move(guard), inserted};
    }

    /// Insert or replace entry using a pre-computed hashed key (avoids re-hashing).
    FindResult upsert(const hashed_key& hk, V value, Metadata meta = {}) {
        auto guard = epoch::EpochGuard::acquire();
        auto* new_entry = pool_.New(std::move(value), std::move(meta));
        auto old = map_.Upsert(hk, [&](std::optional<CacheEntry*>) -> CacheEntry* {
            return new_entry;
        });
        bool inserted = !old.has_value();
        if (!inserted) {
            pool_.Retire(*old);
        }
        return {new_entry, std::move(guard), inserted};
    }

    /// Insert entry only if key doesn't exist.
    /// Returns true if inserted, false if key already existed.
    /// On failure, the new entry is destroyed immediately (never visible).
    bool insert(const K& key, V value, Metadata meta = {}) {
        auto* new_entry = pool_.New(std::move(value), std::move(meta));
        auto existing = map_.Insert(key, new_entry);
        if (existing.has_value()) {
            pool_.Delete(new_entry);
            return false;
        }
        return true;
    }

    /// Remove entry by key. Returns true if removed.
    bool remove(const K& key) {
        auto old = map_.Remove(key);
        if (!old.has_value()) return false;
        pool_.Retire(*old);
        return true;
    }

    /// Conditional remove: removes only if pred(entry) returns true.
    /// Used for eviction: prevents removing an entry that was concurrently
    /// replaced by Upsert between a Find and this Remove.
    ///
    /// Implementation: atomic Remove then check pred. If pred fails,
    /// re-Insert the entry (brief cache-miss window, acceptable for a cache).
    template<typename Pred>
    bool remove_if(const K& key, Pred&& pred) {
        auto old = map_.Remove(key);
        if (!old.has_value()) return false;
        CacheEntry* entry = *old;
        if (pred(entry)) {
            pool_.Retire(entry);
            return true;
        }
        // Predicate failed — re-insert (best-effort)
        auto existing = map_.Insert(key, entry);
        if (existing.has_value()) {
            // Race: another thread inserted between our Remove and Insert
            pool_.Retire(entry);
        }
        return false;
    }

    /// Convenience alias for remove().
    void invalidate(const K& key) { remove(key); }

    // =========================================================================
    // Size
    // =========================================================================

    long size() { return map_.size(); }
    long num_buckets() { return map_.num_buckets(); }

    /// Compute which chunk a key would fall into (uses ParlayHash's bucket mapping).
    /// Best-effort: may be briefly inconsistent during ParlayHash resize.
    long chunk_for_key(const K& key, long n_chunks) {
        long nb = map_.num_buckets();
        long chunk_size = (nb + n_chunks - 1) / n_chunks;
        long bucket = map_.bucket_for_key(key);
        return bucket / chunk_size;
    }

    // =========================================================================
    // Chunk-based cleanup
    // =========================================================================

    /// Cleanup a specific chunk of buckets. Pred: bool(const K&, CacheEntry&).
    /// Returns number of entries removed.
    template<typename Pred>
    size_t cleanup_chunk(long chunk, long n_chunks, Pred&& pred) {
        long nb = map_.num_buckets();
        long chunk_size = (nb + n_chunks - 1) / n_chunks;
        long start = chunk * chunk_size;
        long end = std::min(start + chunk_size, nb);

        std::vector<K> to_remove;
        {
            auto guard = epoch::EpochGuard::acquire();
            for (long i = start; i < end; ++i) {
                map_.for_each_bucket(i, [&](const K& key, CacheEntry* entry) {
                    if (pred(key, *entry)) to_remove.push_back(key);
                });
            }
        }

        size_t removed = 0;
        for (const auto& key : to_remove) {
            if (remove(key)) ++removed;
        }
        return removed;
    }

    /// Cleanup the next chunk (round-robin cursor). Returns entries removed.
    template<typename Pred>
    size_t cleanup_next_chunk(long n_chunks, Pred&& pred) {
        long chunk = cleanup_cursor_.fetch_add(1, std::memory_order_relaxed) % n_chunks;
        return cleanup_chunk(chunk, n_chunks, std::forward<Pred>(pred));
    }

    /// Cleanup all buckets. Returns total entries removed.
    template<typename Pred>
    size_t full_cleanup(Pred&& pred) {
        long nb = map_.num_buckets();
        std::vector<K> to_remove;
        {
            auto guard = epoch::EpochGuard::acquire();
            for (long i = 0; i < nb; ++i) {
                map_.for_each_bucket(i, [&](const K& key, CacheEntry* entry) {
                    if (pred(key, *entry)) to_remove.push_back(key);
                });
            }
        }
        size_t removed = 0;
        for (const auto& key : to_remove) {
            if (remove(key)) ++removed;
        }
        return removed;
    }

    /// Force a GC cycle on the epoch pool.
    void collect() { pool_.collect(); }

    /// Find which chunk a key belongs to (test-only, O(num_buckets)).
    /// Returns -1 if key is not found.
    long key_chunk(const K& key, long n_chunks) {
        auto guard = epoch::EpochGuard::acquire();
        long nb = map_.num_buckets();
        long chunk_size = (nb + n_chunks - 1) / n_chunks;
        for (long i = 0; i < nb; ++i) {
            bool found = false;
            map_.for_each_bucket(i, [&](const K& k, CacheEntry*) {
                if (k == key) found = true;
            });
            if (found) return i / chunk_size;
        }
        return -1;
    }

private:
    // memory_pool allocated on the heap and intentionally never freed.
    // ChunkMap instances are static singletons; during static destruction
    // the epoch singleton may already be destroyed, and ~memory_pool calls
    // get_epoch().update_epoch() which would crash.
    static epoch::memory_pool<CacheEntry>& shared_pool() {
        static auto* p = new epoch::memory_pool<CacheEntry>();
        return *p;
    }

    MapType& map_;
    epoch::memory_pool<CacheEntry>& pool_ = shared_pool();
    std::atomic<long> cleanup_cursor_{0};
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_CHUNK_MAP_H
