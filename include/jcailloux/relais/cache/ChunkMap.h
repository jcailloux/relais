#ifndef JCX_RELAIS_CACHE_CHUNK_MAP_H
#define JCX_RELAIS_CACHE_CHUNK_MAP_H

#include <algorithm>
#include <atomic>
#include <bit>
#include <concepts>
#include <cstring>
#include <tuple>
#include <type_traits>
#include <variant>
#include <vector>

#include <parlay_hash/unordered_map.h>
#include <utils/epoch.h>

#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/cache/TaggedEntry.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais::cache {

// =============================================================================
// Mergeable concept — metadata that preserves access history across upserts
// =============================================================================

template<typename T>
concept Mergeable = requires(T& a, const T& b) { a.mergeFrom(b); };

// =============================================================================
// Fast hashing — splitmix64 for integers, wyhash for strings/bytes
// =============================================================================

namespace detail {

// Splitmix64 finalizer — proven avalanche properties, ~3 cycles.
inline size_t mix(size_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

// wyhash core primitives (public domain, Wang Yi)
inline uint64_t wy_read64(const uint8_t* p) { uint64_t v; std::memcpy(&v, p, 8); return v; }
inline uint64_t wy_read32(const uint8_t* p) { uint32_t v; std::memcpy(&v, p, 4); return v; }

inline uint64_t wy_mum(uint64_t a, uint64_t b) {
    __uint128_t r = static_cast<__uint128_t>(a) * b;
    return static_cast<uint64_t>(r) ^ static_cast<uint64_t>(r >> 64);
}

inline uint64_t wyhash(const void* data, size_t len, uint64_t seed = 0) {
    static constexpr uint64_t s0 = 0xa0761d6478bd642fULL;
    static constexpr uint64_t s1 = 0xe7037ed1a0b428dbULL;
    static constexpr uint64_t s2 = 0x8ebc6af09c88c6e3ULL;
    static constexpr uint64_t s3 = 0x589965cc75374cc3ULL;
    const auto* p = static_cast<const uint8_t*>(data);
    seed ^= s0;
    uint64_t a, b;
    if (len <= 16) {
        if (len >= 4) {
            a = (wy_read32(p) << 32) | wy_read32(p + ((len >> 3) << 2));
            b = (wy_read32(p + len - 4) << 32) | wy_read32(p + len - 4 - ((len >> 3) << 2));
        } else if (len > 0) {
            a = (static_cast<uint64_t>(p[0]) << 16) | (static_cast<uint64_t>(p[len >> 1]) << 8) | p[len - 1];
            b = 0;
        } else { a = b = 0; }
    } else {
        size_t i = len;
        if (i > 48) {
            uint64_t s1_ = seed, s2_ = seed;
            do {
                seed = wy_mum(wy_read64(p) ^ s1, wy_read64(p + 8) ^ seed);
                s1_ = wy_mum(wy_read64(p + 16) ^ s2, wy_read64(p + 24) ^ s1_);
                s2_ = wy_mum(wy_read64(p + 32) ^ s3, wy_read64(p + 40) ^ s2_);
                p += 48; i -= 48;
            } while (i > 48);
            seed ^= s1_ ^ s2_;
        }
        while (i > 16) { seed = wy_mum(wy_read64(p) ^ s1, wy_read64(p + 8) ^ seed); p += 16; i -= 16; }
        a = wy_read64(p + i - 16);
        b = wy_read64(p + i - 8);
    }
    return wy_mum(s1 ^ len, wy_mum(a ^ s1, b ^ seed));
}

// AutoHash: fast hash dispatch per type
template<typename T>
struct AutoHash : std::hash<T> {};

// std::string — wyhash on raw bytes, skip ParlayHash rehash
template<>
struct AutoHash<std::string> {
    using is_avalanching = void;
    size_t operator()(const std::string& s) const {
        return wyhash(s.data(), s.size());
    }
};

// std::tuple — splitmix64 per element, skip ParlayHash rehash
template<typename... Ts>
struct AutoHash<std::tuple<Ts...>> {
    using is_avalanching = void;
    size_t operator()(const std::tuple<Ts...>& t) const {
        size_t seed = 0;
        std::apply([&](const auto&... args) {
            ((seed = seed ^ mix(std::hash<std::decay_t<decltype(args)>>{}(args) + seed)), ...);
        }, t);
        return seed;
    }
};

}  // namespace detail

// =============================================================================
// ChunkMap<K, V, Metadata, HasGhost> — lock-free hash map with epoch-based reclamation
//
// Wraps ParlayHash (lock-free concurrent hash map) with:
// - epoch::memory_pool<CacheEntry> for safe deferred destruction
// - epoch::EpochGuard (ticket-based) for thread-agnostic read protection
// - Chunk-based partial cleanup for incremental eviction
// - Optional ghost entries (HasGhost = true) via tagged pointers (zero allocation)
//
// ParlayHash stores pair<K, TaggedEntry> directly in bucket buffers (trivially
// copyable). TaggedEntry is 8 bytes: either a real pointer to a CacheEntry, or
// ghost data encoded inline (bit 0 = ghost flag).
//
// When HasGhost = false (default), all ghost code is eliminated at compile time.
//
// Thread-safe: all public methods are safe to call concurrently.
// =============================================================================

template<typename K, typename V, typename Metadata = std::monostate,
         bool HasGhost = false,
         typename Hash = detail::AutoHash<K>>
class ChunkMap {
public:
    // Base: metadata accessible without downcast
    struct EntryHeader {
        [[no_unique_address]] Metadata metadata;
    };

    // Real entry: inherits EntryHeader, adds value
    struct CacheEntry : EntryHeader {
        V value;
        CacheEntry(V v, Metadata m)
            : EntryHeader{std::move(m)}, value(std::move(v)) {}
    };

    struct FindResult {
        TaggedEntry tagged{};
        epoch::EpochGuard guard;
        bool was_insert = false;

        explicit operator bool() const { return !tagged.empty(); }

        bool isGhost() const { return tagged.isGhost(); }

        CacheEntry* asReal() const {
            auto* h = tagged.asReal<EntryHeader>();
            if (!h) return nullptr;
            return static_cast<CacheEntry*>(h);
        }

        EntryHeader* entry() const {
            return tagged.asReal<EntryHeader>();
        }

        uint32_t ghostCount() const { return tagged.ghostCount(); }
        uint32_t ghostBytes() const { return tagged.ghostBytes(); }
        uint8_t ghostFlags() const { return tagged.ghostFlags(); }
    };

    using MapType = parlay::parlay_unordered_map<K, TaggedEntry, Hash>;
    using hashed_key = typename MapType::hashed_key;

    static hashed_key make_key(const K& key) { return MapType::make_key(key); }
    static size_t get_hash(const hashed_key& k) { return MapType::get_hash(k); }

    static void memoryHook(int64_t delta) {
        if constexpr (GDSFPolicy::enabled) {
            GDSFPolicy::instance().charge(delta);
        }
    }

    explicit ChunkMap(long initial_size = 128)
        : map_(*new MapType(initial_size, false,
                GDSFPolicy::enabled ? &memoryHook : nullptr)) {}

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
    /// The returned TaggedEntry is valid as long as FindResult lives.
    /// Use asReal()/isGhost() to discriminate.
    FindResult find(const K& key) {
        auto hk = make_key(key);
        map_.prefetch_for(hk);
        auto guard = epoch::EpochGuard::acquire();
        auto opt = map_.Find_in_epoch(hk);
        if (!opt) return {};
        return {*opt, std::move(guard)};
    }

    /// Find entry by pre-computed hashed key (avoids re-hashing).
    FindResult find(const hashed_key& hk) {
        map_.prefetch_for(hk);
        auto guard = epoch::EpochGuard::acquire();
        auto opt = map_.Find_in_epoch(hk);
        if (!opt) return {};
        return {*opt, std::move(guard)};
    }

    // =========================================================================
    // Mutations
    // =========================================================================

    /// Insert or replace entry. Returns epoch-guarded result pointing to the
    /// NEW entry. EpochGuard is acquired BEFORE the Upsert to protect the new
    /// entry from concurrent Upsert + Retire by another thread.
    /// Old real entry is retired via pool. Old ghost is a no-op (inline data).
    FindResult upsert(const K& key, V value, Metadata meta = {}) {
        auto hk = make_key(key);
        map_.prefetch_for(hk);
        auto guard = epoch::EpochGuard::acquire();
        auto* new_entry = pool_.New(std::move(value), std::move(meta));
        auto new_te = TaggedEntry::fromReal(static_cast<EntryHeader*>(new_entry));
        auto old = map_.Upsert_in_epoch(hk,
            [&](std::optional<TaggedEntry> opt) -> TaggedEntry {
                if constexpr (Mergeable<Metadata>) {
                    if (opt && opt->isReal()) {
                        new_entry->metadata.mergeFrom(opt->template asReal<EntryHeader>()->metadata);
                    }
                }
                return new_te;
            });
        bool inserted = !old.has_value();
        if constexpr (HasGhost) {
            // new entry is always real: +1 if insert or replacing ghost
            if (inserted || old->isGhost())
                live_count_.fetch_add(1, std::memory_order_relaxed);
        }
        if (!inserted && old->isReal()) {
            pool_.Retire(static_cast<CacheEntry*>(old->template asReal<EntryHeader>()));
        }
        return {new_te, std::move(guard), inserted};
    }

    /// Insert or replace entry using a pre-computed hashed key (avoids re-hashing).
    FindResult upsert(const hashed_key& hk, V value, Metadata meta = {}) {
        map_.prefetch_for(hk);
        auto guard = epoch::EpochGuard::acquire();
        auto* new_entry = pool_.New(std::move(value), std::move(meta));
        auto new_te = TaggedEntry::fromReal(static_cast<EntryHeader*>(new_entry));
        auto old = map_.Upsert_in_epoch(hk,
            [&](std::optional<TaggedEntry> opt) -> TaggedEntry {
                if constexpr (Mergeable<Metadata>) {
                    if (opt && opt->isReal()) {
                        new_entry->metadata.mergeFrom(opt->template asReal<EntryHeader>()->metadata);
                    }
                }
                return new_te;
            });
        bool inserted = !old.has_value();
        if constexpr (HasGhost) {
            if (inserted || old->isGhost())
                live_count_.fetch_add(1, std::memory_order_relaxed);
        }
        if (!inserted && old->isReal()) {
            pool_.Retire(static_cast<CacheEntry*>(old->template asReal<EntryHeader>()));
        }
        return {new_te, std::move(guard), inserted};
    }

    /// Insert ghost entry (zero allocation — data encoded inline in TaggedEntry).
    /// Only if key doesn't exist (never replaces a real entry).
    /// Returns true if inserted, false if key already existed.
    bool insert_ghost(const K& key, uint32_t count, uint32_t bytes, uint8_t flags)
        requires (HasGhost)
    {
        auto te = TaggedEntry::fromGhost(count, bytes, flags);
        auto existing = map_.Insert(key, te);
        return !existing.has_value();
    }

    /// Update a ghost entry via read-modify-write (Upsert).
    /// No-op if the entry is not a ghost or doesn't exist.
    template<typename Mutator>
    void update_ghost(const K& key, Mutator&& mutator) requires (HasGhost) {
        auto hk = make_key(key);
        map_.prefetch_for(hk);
        auto old = map_.Upsert_in_epoch(hk,
            [&](std::optional<TaggedEntry> opt) -> TaggedEntry {
                if (opt && opt->isGhost()) return mutator(*opt);
                return opt.value_or(TaggedEntry{});
            });
        // If key didn't exist, we inserted an empty TaggedEntry — remove it
        if (!old.has_value()) map_.Remove(key);
    }

    /// Insert entry only if key doesn't exist.
    /// Returns true if inserted, false if key already existed.
    /// On failure, the new entry is destroyed immediately (never visible).
    bool insert(const K& key, V value, Metadata meta = {}) {
        auto* new_entry = pool_.New(std::move(value), std::move(meta));
        auto te = TaggedEntry::fromReal(static_cast<EntryHeader*>(new_entry));
        auto existing = map_.Insert(key, te);
        if (existing.has_value()) {
            pool_.Delete(new_entry);
            return false;
        }
        if constexpr (HasGhost) live_count_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    /// Remove entry by key. Retires real entries via pool; ghost removal is a no-op.
    /// Returns true if removed.
    bool remove(const K& key) {
        auto old = map_.Remove(key);
        if (!old.has_value()) return false;
        auto te = *old;
        if constexpr (HasGhost) {
            if (!te.isGhost())
                live_count_.fetch_sub(1, std::memory_order_relaxed);
        }
        if (te.isReal()) {
            pool_.Retire(static_cast<CacheEntry*>(te.template asReal<EntryHeader>()));
        }
        return true;
    }

    /// Conditional remove: removes only if pred(entry) returns true.
    /// Used for eviction: prevents removing an entry that was concurrently
    /// replaced by Upsert between a Find and this Remove.
    ///
    /// Implementation: atomic Remove then check pred. If pred fails,
    /// re-Insert the entry (brief cache-miss window, acceptable for a cache).
    /// Predicate receives EntryHeader* (only called for real entries).
    template<typename Pred>
    bool remove_if(const K& key, Pred&& pred) {
        auto old = map_.Remove(key);
        if (!old.has_value()) return false;
        auto te = *old;

        if (te.isReal()) {
            auto* entry = te.template asReal<EntryHeader>();
            if (pred(entry)) {
                if constexpr (HasGhost) {
                    live_count_.fetch_sub(1, std::memory_order_relaxed);
                }
                pool_.Retire(static_cast<CacheEntry*>(entry));
                return true;
            }
        }

        // Predicate failed or ghost entry — re-insert (best-effort)
        auto existing = map_.Insert(key, te);
        if (existing.has_value()) {
            // Race: another thread inserted between our Remove and Insert
            if (te.isReal()) {
                if constexpr (HasGhost) {
                    live_count_.fetch_sub(1, std::memory_order_relaxed);
                }
                pool_.Retire(static_cast<CacheEntry*>(te.template asReal<EntryHeader>()));
            }
        }
        return false;
    }

    /// Convenience alias for remove().
    void invalidate(const K& key) { remove(key); }

    // =========================================================================
    // Size
    // =========================================================================

    /// Number of live (non-ghost) entries. When HasGhost = false, equivalent
    /// to totalEntries(). Uses relaxed atomics (same approximation as totalMemory).
    long size() {
        if constexpr (HasGhost) return live_count_.load(std::memory_order_relaxed);
        else return map_.size();
    }

    /// Total entries in the map (live + ghost). For diagnostics/tests.
    long totalEntries() { return map_.size(); }

    long num_buckets() { return map_.num_buckets(); }

    /// Compute which chunk a key would fall into (uses ParlayHash's bucket mapping).
    /// Best-effort: may be briefly inconsistent during ParlayHash resize.
    long chunk_for_key(const K& key, long n_chunks) {
        long nb = map_.num_buckets();
        long chunk_size = (nb + n_chunks - 1) / n_chunks;
        long bucket = map_.bucket_for_key(key);
        return bucket / chunk_size;
    }

    /// Compute which chunk a pre-computed hash falls into (avoids re-hashing).
    /// Uses ParlayHash's high-bit bucket mapping: (hash >> (48 - log2(size))) & (size - 1).
    /// Use with get_hash(make_key(key)) for single-hash lookup + chunk computation.
    long chunk_for_hash(size_t hash, long n_chunks) {
        long nb = map_.num_buckets();
        long chunk_size = (nb + n_chunks - 1) / n_chunks;
        int num_bits = std::countr_zero(static_cast<unsigned long>(nb));
        long bucket = static_cast<long>(
            (hash >> (48 - num_bits)) & static_cast<size_t>(nb - 1));
        return bucket / chunk_size;
    }

    // =========================================================================
    // Chunk-based cleanup
    // =========================================================================

    /// Cleanup a specific chunk of buckets. Pred: bool(const K&, TaggedEntry).
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
                map_.prefetch_bucket(i + 1);
                map_.for_each_bucket(i, [&](const K& key, TaggedEntry te) {
                    if (pred(key, te)) to_remove.push_back(key);
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
                map_.prefetch_bucket(i + 1);
                map_.for_each_bucket(i, [&](const K& key, TaggedEntry te) {
                    if (pred(key, te)) to_remove.push_back(key);
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
    void collect() {
        pool_.collect();
    }

    /// Find which chunk a key belongs to (test-only, O(num_buckets)).
    /// Returns -1 if key is not found.
    long key_chunk(const K& key, long n_chunks) {
        auto guard = epoch::EpochGuard::acquire();
        long nb = map_.num_buckets();
        long chunk_size = (nb + n_chunks - 1) / n_chunks;
        for (long i = 0; i < nb; ++i) {
            bool found = false;
            map_.for_each_bucket(i, [&](const K& k, TaggedEntry) {
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
    std::atomic<long> live_count_{0};

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_CHUNK_MAP_H
