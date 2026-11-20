#ifndef JCX_RELAIS_CACHE_ENTRY_LAYOUT_H
#define JCX_RELAIS_CACHE_ENTRY_LAYOUT_H

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

#include <parlay_hash/unordered_map.h>
#include <utils/epoch.h>

#include "jcailloux/relais/cache/TaggedEntry.h"
#include "jcailloux/relais/BlockBytes.h"
#include "jcailloux/relais/HeapBytes.h"

namespace jcailloux::relais::cache::detail {

/// An entry's byte count, kept in its header when that costs no space.
struct StoredBytes { uint32_t bytes = 0; };
struct NoStoredBytes {};

/// What an epoch pool allocates for one value: the value, plus guard words
/// around it when parlay checks its pools (EpochMemCheck, builds without
/// NDEBUG). The layout of epoch::memory_pool's wrapper, which is private.
#ifdef EpochMemCheck
template<typename T>
struct PoolBlock { long pad; std::atomic<long> head; T value; std::atomic<long> tail; };
#else
template<typename T>
struct PoolBlock { T value; };
#endif

// =============================================================================
// EntryLayout — the room one key and its entry take in a ChunkMap
// =============================================================================
//
// ChunkMap stores, for each key:
//   - the entry (CacheEntry: metadata, an optional byte count, the value) in
//     a block of the entry pool;
//   - a slot in parlay's table: the key and the entry's tagged pointer side by
//     side in a bucket when both are trivially copyable; otherwise a pointer
//     to a node (the key and the tagged pointer) in a block of parlay's node
//     pool. A ghost takes the slot alone.
// This describes those blocks, for the bytes an entry is counted at (its own
// count, the L1 ledger) and the bytes it is scored at (CacheTier).

template<typename K, typename V, typename Metadata, bool CountsBytes, typename Hash>
struct EntryLayout {
private:
    // The entry's layout with and without a stored byte count, to keep the
    // count only where it fits in padding.
    struct HeaderWithoutBytes { [[no_unique_address]] Metadata metadata; };
    struct EntryWithoutBytes : HeaderWithoutBytes { V value; };
    struct HeaderWithBytes {
        [[no_unique_address]] Metadata metadata;
        [[no_unique_address]] StoredBytes stored_bytes;
    };
    struct EntryWithBytes : HeaderWithBytes { V value; };

public:
    /// Whether entries are counted in bytes: set by a GDSF tier, the only
    /// kind whose bytes the eviction budget reads.
    static constexpr bool kCountsBytes = CountsBytes;

    /// The byte count is stored in the header when that leaves the entry's
    /// size unchanged, and recomputed from the value otherwise.
    static constexpr bool kStoresBytes =
        kCountsBytes && sizeof(EntryWithBytes) == sizeof(EntryWithoutBytes);

    // Base: metadata accessible without downcast
    struct EntryHeader {
        [[no_unique_address]] Metadata metadata;
        [[no_unique_address]] std::conditional_t<kStoresBytes, StoredBytes, NoStoredBytes>
            stored_bytes{};
    };

    // Real entry: inherits EntryHeader, adds value. Built only by the entry
    // pool, inside its pool block (PooledEntry).
    struct CacheEntry : EntryHeader {
        V value;
        CacheEntry(V v, Metadata m)
            : EntryHeader{std::move(m)}, value(std::move(v)) {
            if constexpr (kStoresBytes) {
                this->stored_bytes.bytes = static_cast<uint32_t>(std::min<size_t>(
                    heldBytes(), std::numeric_limits<uint32_t>::max()));
            }
        }

        /// The bytes this entry takes: read from the header, or recomputed by
        /// heldBytes(). A cached value never changes (a write stores a new
        /// entry), and a block keeps its size, so both give the same count for
        /// the entry's whole life.
        [[nodiscard]] size_t bytes() const {
            if constexpr (kStoresBytes) return this->stored_bytes.bytes;
            else return heldBytes();
        }

        /// The room the entry's blocks take, read on them: its pool block, and
        /// the blocks its value and its metadata own (a list page's query; a
        /// CacheMetadata owns none).
        [[nodiscard]] size_t heldBytes() const {
            return relais::detail::blockBytes(sizeof(PooledEntry), block())
                + relais::detail::heapBytes(value)
                + relais::detail::heapBytes(this->metadata);
        }

        /// The pool block that holds this entry.
        [[nodiscard]] const void* block() const {
            return reinterpret_cast<const char*>(this) - kEntryOffset;
        }
    };

    static_assert(sizeof(CacheEntry) == sizeof(EntryWithoutBytes),
                  "a stored byte count must not make the entry bigger");

    /// The entry pool's block for one entry.
    using PooledEntry = PoolBlock<CacheEntry>;

private:
    // Where the entry sits in its pool block: past the guard words, if any.
    // CacheEntry has members in a base and in itself, so PooledEntry is not
    // standard-layout, where offsetof is conditionally supported: GCC and Clang
    // compute it, and warn.
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
    static constexpr size_t kEntryOffset = offsetof(PooledEntry, value);
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

public:
    /// Bytes the pool asks for one entry beyond its value, one malloc of
    /// sizeof(PooledEntry) per entry:
    ///   - the header: the metadata, and the stored byte count when it fits;
    ///   - the padding that aligns the value after the header;
    ///   - the pool's guard words, when parlay checks its pool.
    static constexpr size_t kEntryOverhead = sizeof(PooledEntry) - sizeof(V);

    /// The bytes an entry holding `v` would take, before it exists: the class
    /// of its pool block (one malloc of sizeof(PooledEntry)), and the blocks
    /// the value owns, which move into the entry with it. Admission and ghosts
    /// weigh a value with it, before its metadata is built: what the metadata
    /// will own is left out (a list page's query). The entry's own count, which
    /// the sweep, the score and the eviction histogram read, is
    /// CacheEntry::bytes(): it reads the pool block itself, which the
    /// allocator may have made larger than its class.
    [[nodiscard]] static size_t entryBytes(const V& v) {
        return relais::detail::blockBytes(sizeof(PooledEntry)) + relais::detail::heapBytes(v);
    }

    // =========================================================================
    // The key's slot in parlay's table
    // =========================================================================

    using MapType = parlay::parlay_unordered_map<K, TaggedEntry, Hash>;
    using Table = typename MapType::map;
    using Entries = decltype(MapType::entries_);

    /// Whether parlay keeps each key behind a pointer, in a node of its own:
    /// it stores a key in the bucket only when the key and the tagged pointer
    /// are trivially copyable (std::string and std::tuple keys are not).
    static constexpr bool kIndirectKeys = !Table::Entry::Direct;

    /// An overflow link: the slots past a bucket's inline buffer.
    using Link = typename Table::link;

    /// A key's share of the bucket array, at parlay's expected load of
    /// 2^log_bucket_size keys per bucket. The bucket (a 64-byte line) holds
    /// buffer_size slots inline. The real share varies between two growths of
    /// the table, each 4 times larger, and parlay keeps the earlier versions:
    /// the ledger counts the real table, the score this nominal share, so that
    /// entries of two maps compare the same through the table's growths.
    static constexpr size_t kTableShare = sizeof(typename Table::bucket) >> Table::log_bucket_size;
    static_assert(kTableShare >= sizeof(typename Table::Entry),
                  "a key's share of the bucket array holds at least its slot");

    /// The bytes a key costs the table beside its entry: its share of the
    /// bucket array and, for a key kept behind a pointer, the node's block (at
    /// its class) and what the key owns. Part of the bytes an entry is scored
    /// at, the same when it is admitted (`key` is the caller's, standing for
    /// the copy the node will hold), swept, and kept as a ghost.
    [[nodiscard]] static size_t slotBytes(const K& key) {
        if constexpr (kIndirectKeys) {
            return kTableShare + relais::detail::blockBytes(sizeof(PoolBlock<Node>))
                + relais::detail::heapBytes(key);
        } else {
            return kTableShare;
        }
    }

    // =========================================================================
    // The blocks parlay's pools hold, for the L1 ledger
    // =========================================================================

    /// The node of a key kept behind a pointer (parlay's IndirectEntries).
    using Node = std::pair<K, TaggedEntry>;

    /// An entry's block in the entry pool: the entry's own count.
    static size_t weighEntry(const CacheEntry& entry, const void*, size_t) {
        return entry.bytes();
    }

    /// A node's block, read on it, and what its key owns.
    static size_t weighNode(const Node& node, const void* block, size_t block_size) {
        return relais::detail::blockBytes(block_size, block) + relais::detail::heapBytes(node);
    }

    /// A link's block, read on it. The slot it holds owns nothing: a key in
    /// the bucket is trivially copyable, and a node is counted in its pool.
    static size_t weighLink(const Link&, const void* block, size_t block_size) {
        return relais::detail::blockBytes(block_size, block);
    }
};

}  // namespace jcailloux::relais::cache::detail

#endif  // JCX_RELAIS_CACHE_ENTRY_LAYOUT_H
