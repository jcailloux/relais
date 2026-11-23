#ifndef JCX_RELAIS_CACHE_CHUNK_MAP_H
#define JCX_RELAIS_CACHE_CHUNK_MAP_H

#include <algorithm>
#include <atomic>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <tuple>
#include <type_traits>
#include <variant>
#include <vector>

#include <parlay_hash/unordered_map.h>
#include <utils/epoch.h>

#include "jcailloux/relais/cache/EntryLayout.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/cache/L1Ledger.h"
#include "jcailloux/relais/cache/TaggedEntry.h"

#ifdef RELAIS_BUILDING_TESTS
#include <functional>
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais::cache::detail {

// =============================================================================
// Mergeable concept — metadata that preserves access history across upserts
// =============================================================================

template<typename T>
concept Mergeable = requires(T& a, const T& b) { a.mergeFrom(b); };

// =============================================================================
// Fast hashing — splitmix64 for integers, wyhash for strings/bytes
// =============================================================================

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

// Integers — ParlayHash's own rehash with is_avalanching (applied once, not twice)
template<std::integral T>
struct AutoHash<T> {
    using is_avalanching = void;
    size_t operator()(T x) const {
        size_t h = static_cast<size_t>(x) * UINT64_C(0xbf58476d1ce4e5b9);
        return h ^ (h >> 31);
    }
};

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


// =============================================================================
// ChunkMap<K, V, Metadata, HasGhost, CountsBytes> — lock-free hash map with epoch-based reclamation
//
// Wraps ParlayHash (lock-free concurrent hash map) with:
// - epoch::memory_pool<CacheEntry> for safe deferred destruction
// - epoch::EpochGuard (ticket-based) for thread-agnostic read protection
// - Chunk-based partial cleanup for incremental eviction
// - Optional ghost entries (HasGhost = true) via tagged pointers (zero allocation)
// - With CountsBytes, the L1 ledger's accounting of every block it holds
//
// ParlayHash stores pair<K, TaggedEntry> directly in bucket buffers when K is
// trivially copyable, and in a node behind a pointer otherwise (EntryLayout).
// TaggedEntry is 8 bytes: either a real pointer to a CacheEntry, or ghost data
// encoded inline (bit 0 = ghost flag).
//
// When HasGhost = false (default), all ghost code is eliminated at compile time.
//
// Thread-safe: all public methods are safe to call concurrently.
// =============================================================================

template<typename K, typename V, typename Metadata = std::monostate,
         bool HasGhost = false,
         bool CountsBytes = false,
         typename Hash = AutoHash<K>>
class ChunkMap {
    using Layout = EntryLayout<K, V, Metadata, CountsBytes, Hash>;

public:
    /// Whether entries are counted in bytes, and the L1 ledger told of every
    /// block the map holds: set by a GDSF tier (see EntryLayout).
    static constexpr bool kCountsBytes = Layout::kCountsBytes;
    static constexpr bool kStoresBytes = Layout::kStoresBytes;

    using EntryHeader = typename Layout::EntryHeader;
    using CacheEntry = typename Layout::CacheEntry;

    /// Bytes the pool asks for one entry beyond its value (see EntryLayout).
    static constexpr size_t kEntryOverhead = Layout::kEntryOverhead;

    /// Whether parlay keeps each key behind a pointer (see EntryLayout).
    static constexpr bool kIndirectKeys = Layout::kIndirectKeys;

    /// The bytes an entry holding `v` would take, before it exists (see
    /// EntryLayout). The entry's own count is CacheEntry::bytes().
    [[nodiscard]] static size_t entryBytes(const V& v) { return Layout::entryBytes(v); }

    /// The bytes `key` costs the table beside its entry (see EntryLayout).
    [[nodiscard]] static size_t slotBytes(const K& key) { return Layout::slotBytes(key); }

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

    using MapType = typename Layout::MapType;
    using hashed_key = typename MapType::hashed_key;

    static hashed_key make_key(const K& key) { return MapType::make_key(key); }
    static size_t get_hash(const hashed_key& k) { return MapType::get_hash(k); }

    explicit ChunkMap(long initial_size = 128)
        : map_(makeMap(initial_size)) {}

    // ChunkMap instances are static singletons (LocalRepo::cache(),
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
            // new entry is always real: +1 unless it replaces a real one (a
            // ghost or update_ghost's empty placeholder counts as absent)
            if (inserted || !old->isReal())
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
            if (inserted || !old->isReal())
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
        // If key didn't exist, we inserted an empty TaggedEntry — remove it, and
        // only it: a real entry stored meanwhile must stay (removing it would
        // also skip its Retire).
        if (!old.has_value()) {
            map_.Remove_if(key,
                [](const auto& kv) { return kv.second.empty(); },
                [](const auto&) { return true; });
        }
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
            if (te.isReal())
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
    /// Atomic: pred is evaluated on the bucket state the removal commits
    /// against, so the key never leaves the map when pred fails (a remove then
    /// re-insert would hide it from a concurrent evict, and re-insert a value
    /// that evict meant to drop). Predicate receives EntryHeader* (only called
    /// for real entries; ghosts are never removed here).
    template<typename Pred>
    bool remove_if(const K& key, Pred&& pred) {
        auto old = map_.Remove_if(key,
            [&](const auto& kv) {
                const TaggedEntry& te = kv.second;
                return te.isReal() && pred(te.template asReal<EntryHeader>());
            },
            [](const auto& kv) { return kv.second; });
        if (!old.has_value()) return false;
        if constexpr (HasGhost) {
            live_count_.fetch_sub(1, std::memory_order_relaxed);
        }
        pool_.Retire(static_cast<CacheEntry*>(old->template asReal<EntryHeader>()));
        return true;
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

    // A scan reads one table version, loaded once under its guard: the range
    // it computes on that version names the same keys until it ends, since a
    // bucket copied meanwhile to a larger version is followed into the buckets
    // it was split into (see MapType::current_version). A chunk is a prefix
    // of the key's hash as long as the version has n_chunks buckets or more.

    /// Cleanup a specific chunk of buckets. Pred: bool(const K&, TaggedEntry).
    /// Returns number of entries removed.
    template<typename Pred>
    size_t cleanup_chunk(long chunk, long n_chunks, Pred&& pred) {
        // The keys to remove, copied out of the table: counted while held.
        LedgerList<K, kCountsBytes> to_remove;
        {
            auto guard = epoch::EpochGuard::acquire();
            auto* tv = map_.current_version();
            const long nb = MapType::num_buckets(tv);
            const long chunk_size = (nb + n_chunks - 1) / n_chunks;
            const long start = chunk * chunk_size;
            const long end = std::min(start + chunk_size, nb);
            scanStarts(chunk);
            for (long i = start; i < end; ++i) {
                MapType::prefetch_bucket(tv, i + 1);
                MapType::for_each_bucket(tv, i, [&](const K& key, TaggedEntry te) {
                    if (pred(key, te)) to_remove.push_back(key);
                });
            }
        }

        return removeAll(to_remove);
    }

    /// Advance the round-robin cursor and return the chunk index.
    /// Use with cleanup_chunk() when work must be inserted between
    /// cursor advance and cleanup (e.g., flushing thread-local counters).
    long advance_cleanup_cursor(long n_chunks) {
        return cleanup_cursor_.fetch_add(1, std::memory_order_relaxed) % n_chunks;
    }

    /// Cleanup the next chunk (round-robin cursor). Returns entries removed.
    template<typename Pred>
    size_t cleanup_next_chunk(long n_chunks, Pred&& pred) {
        long chunk = advance_cleanup_cursor(n_chunks);
        return cleanup_chunk(chunk, n_chunks, std::forward<Pred>(pred));
    }

    /// Cleanup all buckets. Returns total entries removed.
    template<typename Pred>
    size_t full_cleanup(Pred&& pred) {
        LedgerList<K, kCountsBytes> to_remove;
        {
            auto guard = epoch::EpochGuard::acquire();
            auto* tv = map_.current_version();
            const long nb = MapType::num_buckets(tv);
            scanStarts(-1);
            for (long i = 0; i < nb; ++i) {
                MapType::prefetch_bucket(tv, i + 1);
                MapType::for_each_bucket(tv, i, [&](const K& key, TaggedEntry te) {
                    if (pred(key, te)) to_remove.push_back(key);
                });
            }
        }
        return removeAll(to_remove);
    }

    /// Visit every entry, read-only, under one epoch guard.
    /// Fn: void(const K&, TaggedEntry). Linear in the table.
    template<typename Fn>
    void for_each(Fn&& fn) {
        auto guard = epoch::EpochGuard::acquire();
        auto* tv = map_.current_version();
        const long nb = MapType::num_buckets(tv);
        scanStarts(-1);
        for (long i = 0; i < nb; ++i) {
            MapType::prefetch_bucket(tv, i + 1);
            MapType::for_each_bucket(tv, i, fn);
        }
    }

    /// While it lives, what the calling thread retires from this map's pools
    /// (its entries, parlay's links and nodes) is no longer cached, but held
    /// until the epoch frees it, which a reader that holds a guard can delay
    /// past the next sweeps: counted in transit until then (see L1Ledger).
    /// For what a sweep gives up: its evictions, and the buckets its ghost
    /// pass rewrites. Each block retired meanwhile is read once more; no
    /// retire outside its scope pays for it.
    class RetiresInTransit {
    public:
        explicit RetiresInTransit(ChunkMap& map) : map_(map) {
            if constexpr (kCountsBytes) {
                entries_ = map_.pool_.weigh_retires(true);
                links_ = map_.map_.m.link_pool->weigh_retires(true);
                if constexpr (kIndirectKeys) nodes_ = map_.map_.entries_.data_pool->weigh_retires(true);
            }
        }
        ~RetiresInTransit() {
            if constexpr (kCountsBytes) {
                map_.pool_.weigh_retires(entries_);
                map_.map_.m.link_pool->weigh_retires(links_);
                if constexpr (kIndirectKeys) map_.map_.entries_.data_pool->weigh_retires(nodes_);
            }
        }
        RetiresInTransit(const RetiresInTransit&) = delete;
        RetiresInTransit& operator=(const RetiresInTransit&) = delete;

    private:
        ChunkMap& map_;
        // The settings it found, restored as it ends: scopes nest.
        bool entries_ = false, links_ = false, nodes_ = false;
    };

    [[nodiscard]] RetiresInTransit retiresInTransit() { return RetiresInTransit{*this}; }

    /// Force a GC cycle on the epoch pool.
    void collect() {
        pool_.collect();
    }

    /// Immediately reclaim all retired entries safe to free.
    void reclaim() {
        pool_.reclaim();
    }

    /// Find which chunk a key belongs to (test-only, O(num_buckets)).
    /// Returns -1 if key is not found.
    long key_chunk(const K& key, long n_chunks) {
        auto guard = epoch::EpochGuard::acquire();
        auto* tv = map_.current_version();
        const long nb = MapType::num_buckets(tv);
        const long chunk_size = (nb + n_chunks - 1) / n_chunks;
        for (long i = 0; i < nb; ++i) {
            bool found = false;
            MapType::for_each_bucket(tv, i, [&](const K& k, TaggedEntry) {
                if (k == key) found = true;
            });
            if (found) return i / chunk_size;
        }
        return -1;
    }

private:
    // What the map's pools report to the L1 ledger: each entry at its own
    // count, each node of a key kept behind a pointer, each overflow link;
    // and what a sweep gives up, in transit until freed (see
    // RetiresInTransit).
    static constexpr epoch::pool_accounting<CacheEntry> kEntryAccounting{
        &kL1Ledger, &Layout::weighEntry, &kL1Transit};
    static constexpr epoch::pool_accounting<typename Layout::Node> kNodeAccounting{
        &kL1Ledger, &Layout::weighNode, &kL1Transit};
    static constexpr epoch::pool_accounting<typename Layout::Link> kLinkAccounting{
        &kL1Ledger, &Layout::weighLink, &kL1Transit};

    /// Removes the keys a cleanup selected; returns how many it removed. What
    /// it retires is in transit until freed (see RetiresInTransit): the bytes
    /// the L1 caches drop as the keys go.
    size_t removeAll(const LedgerList<K, kCountsBytes>& keys) {
        auto in_transit = retiresInTransit();
        size_t removed = 0;
        for (const auto& key : keys) {
            if (remove(key)) ++removed;
        }
        return removed;
    }

    // The table, which reports its versions to the L1 ledger when entries are
    // counted. Every map of one key type shares parlay's link and node pools;
    // in a build with GDSF every tier counts, so the first map of a key type
    // gives them their accounting before any map of that type allocates from
    // them (a later map sets the same again). Allocated on the heap and never
    // freed, like the entry pool (see ~ChunkMap).
    static MapType& makeMap(long initial_size) {
        if constexpr (kCountsBytes) {
            auto& map = *new MapType(initial_size, false, &kL1Ledger);
            map.m.link_pool->set_accounting(&kLinkAccounting);
            if constexpr (kIndirectKeys) map.entries_.data_pool->set_accounting(&kNodeAccounting);
            return map;
        } else {
            return *new MapType(initial_size, false, nullptr);
        }
    }

    // memory_pool allocated on the heap and intentionally never freed.
    // ChunkMap instances are static singletons; during static destruction
    // the epoch singleton may already be destroyed, and ~memory_pool calls
    // get_epoch().update_epoch() which would crash. With CountsBytes, it
    // reports each entry to the L1 ledger from its construction to its
    // destruction, retired or not.
    static epoch::memory_pool<CacheEntry>& shared_pool() {
        static auto* p = [] {
            auto* pool = new epoch::memory_pool<CacheEntry>();
            if constexpr (kCountsBytes) pool->set_accounting(&kEntryAccounting);
            return pool;
        }();
        return *p;
    }

    // Test builds: runs once a scan holds its version and its range, before
    // it reads a bucket; `chunk` is the chunk scanned, -1 for the whole table.
    void scanStarts([[maybe_unused]] long chunk) {
#ifdef RELAIS_BUILDING_TESTS
        if (const auto* hook = scan_hook_.load(std::memory_order_acquire))
            (*hook)(this, chunk);
#endif
    }

    MapType& map_;
    epoch::memory_pool<CacheEntry>& pool_ = shared_pool();
    std::atomic<long> cleanup_cursor_{0};
    // Approximate diagnostic counter (relaxed atomics, no cross-thread sync guarantee)
    std::atomic<long> live_count_{0};

#ifdef RELAIS_BUILDING_TESTS
    // One hook for every map of this type, given the map scanned: outside
    // the object, so that a test build lays the map out as a release build.
    using ScanHook = std::function<void(const ChunkMap*, long)>;
    static inline std::atomic<const ScanHook*> scan_hook_{nullptr};
    friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::relais::cache::detail

#endif  // JCX_RELAIS_CACHE_CHUNK_MAP_H
