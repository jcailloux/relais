#ifndef JCX_RELAIS_CACHE_L1_LEDGER_H
#define JCX_RELAIS_CACHE_L1_LEDGER_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <utils/epoch.h>
#include <utils/threads/thread_specific.h>

#include "jcailloux/relais/BlockBytes.h"
#include "jcailloux/relais/HeapBytes.h"

namespace jcailloux::relais::cache::detail {

// =============================================================================
// L1Ledger — the bytes the L1 holds
// =============================================================================
//
// Every block the L1 holds is counted, at the room it takes at the allocator
// (BlockBytes.h), from its allocation to its release, where it happens:
//   - an entry, by the entry pool: from its construction to its destruction,
//     at its own count (its pool block and what its value owns);
//   - a key parlay keeps behind a pointer, by parlay's node pool: the node's
//     block and what the key owns;
//   - each overflow link of the table, by parlay's link pool;
//   - each version of the table, by itself: its buckets, its block statuses
//     and the version object. parlay keeps every version until the map is
//     cleared, which a ChunkMap never is;
//   - the retire lists of those pools, by their pool: arrays that keep their
//     capacity;
//   - what the L1 keeps beside its entries, which no entry owns: the values a
//     tier served without storing them (its transient pool), the fetches in
//     flight, the lists of ghosts a sweep inserts once its chunk is done, and
//     what invalidates the copies an uncertain write may have changed (the
//     list tracker's lanes, the entity tier's row notes and the key sets
//     they share). Their containers count their own blocks (LedgerAllocator);
//     what an element owns besides is counted as it comes and goes. Evicting
//     an entry frees none of it: no score weighs it.
// A retired block not yet freed (an epoch still open) stays counted: it still
// holds its memory. Only tiers that count bytes (GDSF) report to it.
//
// Not counted: what is set up once and lives as long as the process (the
// registry of repositories, the histogram, the column facts, each thread's
// buffers and counters), and what a request holds while it runs (coroutine
// frames, a database result, an exception in flight).
//
// One counter per thread, on its own cache line, written only by that thread:
// an update is a relaxed load and store, never a read-modify-write, so it
// costs about a nanosecond and moves no line between cores. A block freed on
// another thread than the one that allocated it makes counters negative: only
// the sum means something. A thread that ends hands its counter to the next
// thread that takes its id, so nothing is lost. The sum walks one line per
// thread, off the request path.
class L1Ledger {
public:
    /// Adds `bytes` (negative on a release) to the calling thread's counter.
    static void add(int64_t bytes) noexcept {
        std::atomic_ref<int64_t> counter{counters().get()};
        counter.store(counter.load(std::memory_order_relaxed) + bytes,
                      std::memory_order_relaxed);
    }

    /// The bytes the L1 holds: the sum of every thread's counter. Exact once
    /// the threads that allocate or free have stopped; a moving value
    /// otherwise.
    [[nodiscard]] static int64_t sum() noexcept {
        int64_t total = 0;
        counters().for_each([&total](int64_t& c) {
            total += std::atomic_ref<int64_t>{c}.load(std::memory_order_relaxed);
        });
        return total;
    }

    /// The room a block of n requested bytes takes, read on it.
    static size_t room(size_t n, const void* block) noexcept {
        return relais::detail::blockBytes(n, block);
    }

private:
    // Never freed: a block released during static destruction still reports.
    static parlay::ThreadSpecific<int64_t>& counters() {
        static auto* c = new parlay::ThreadSpecific<int64_t>();
        return *c;
    }
};

/// The L1 ledger, as parlay's pools and table report to it.
inline constexpr epoch::memory_ledger kL1Ledger{&L1Ledger::add, &L1Ledger::room};

// =============================================================================
// LedgerAllocator — a container's blocks, counted as it allocates them
// =============================================================================
//
// For the containers the L1 keeps beside its entries: each block they
// allocate (a vector's array, a node, a bucket array, a shared object with
// its counts) is counted at the room it takes, read on it, from its
// allocation to its release. What an element owns besides (a string's
// characters, an entity copy) is the container owner's to count.

template<typename T>
struct LedgerAllocator {
    using value_type = T;
    /// heapBytes refuses a container that counts its own blocks: they would be
    /// counted twice.
    static constexpr bool kCountsItsBlocks = true;

    LedgerAllocator() noexcept = default;
    template<typename U>
    LedgerAllocator(const LedgerAllocator<U>&) noexcept {}

    [[nodiscard]] T* allocate(size_t n) {
        T* p = std::allocator<T>{}.allocate(n);
        L1Ledger::add(static_cast<int64_t>(L1Ledger::room(n * sizeof(T), p)));
        return p;
    }

    void deallocate(T* p, size_t n) noexcept {
        L1Ledger::add(-static_cast<int64_t>(L1Ledger::room(n * sizeof(T), p)));
        std::allocator<T>{}.deallocate(p, n);
    }

    template<typename U>
    friend bool operator==(const LedgerAllocator&, const LedgerAllocator<U>&) noexcept { return true; }
};

/// LedgerAllocator for a structure that counts bytes, std::allocator otherwise.
template<bool CountsBytes, typename T>
using LedgerAllocatorIf = std::conditional_t<CountsBytes, LedgerAllocator<T>, std::allocator<T>>;

/// What a key owns, as a signed count for the ledger.
template<typename Key>
[[nodiscard]] int64_t keyHeap(const Key& key) {
    return static_cast<int64_t>(relais::detail::heapBytes(key));
}

// =============================================================================
// LedgerSet — a set of keys, counted as it changes
// =============================================================================
//
// Its nodes and its bucket array count themselves (LedgerAllocator); what a
// key owns is counted when the key comes in and when it goes. A set that
// does not count bytes is a plain std::unordered_set.

template<typename Key, typename Hash, bool CountsBytes>
class LedgerSet {
    using Set = std::unordered_set<Key, Hash, std::equal_to<Key>, LedgerAllocatorIf<CountsBytes, Key>>;
    static constexpr bool kCountsKeys =
        CountsBytes && !relais::detail::heap_bytes::owns_nothing<Key>;

    Set set_;

    void releaseKeys() noexcept {
        if constexpr (kCountsKeys) {
            for (const auto& key : set_) L1Ledger::add(-keyHeap(key));
        }
    }

public:
    LedgerSet() = default;

    template<typename It>
    LedgerSet(It first, It last) : set_(first, last) {
        if constexpr (kCountsKeys) {
            for (const auto& key : set_) L1Ledger::add(keyHeap(key));
        }
    }

    ~LedgerSet() { releaseKeys(); }

    // Moved by swapping with an empty set: the keys and their count go along.
    LedgerSet(LedgerSet&& other) noexcept { set_.swap(other.set_); }
    LedgerSet& operator=(LedgerSet&& other) noexcept {
        if (this != &other) {
            clear();
            set_.swap(other.set_);
        }
        return *this;
    }
    LedgerSet(const LedgerSet&) = delete;
    LedgerSet& operator=(const LedgerSet&) = delete;

    /// True if `key` was not in the set.
    bool insert(const Key& key) {
        auto [it, inserted] = set_.insert(key);
        if constexpr (kCountsKeys) {
            if (inserted) L1Ledger::add(keyHeap(*it));
        }
        return inserted;
    }

    void clear() noexcept {
        releaseKeys();
        set_.clear();
    }

    [[nodiscard]] bool contains(const Key& key) const { return set_.contains(key); }
    [[nodiscard]] size_t size() const noexcept { return set_.size(); }

    /// Its blocks and its keys count themselves, as they come and go: a holder
    /// weighed by heapBytes adds nothing for them. A set that does not count
    /// has no weight to give.
    [[nodiscard]] size_t memoryUsage() const noexcept
        requires CountsBytes
    {
        return sizeof(*this);
    }
};

// =============================================================================
// LedgerList — items collected for later, counted as the list grows
// =============================================================================
//
// Its array counts its own block (LedgerAllocator); what an item owns
// (heapBytes) is counted when the item comes in and when the list goes. Its
// items do not change. A list that does not count bytes is a plain
// std::vector.

template<typename T, bool CountsBytes>
class LedgerList {
    static constexpr bool kCountsItems =
        CountsBytes && !relais::detail::heap_bytes::owns_nothing<T>;

    std::vector<T, LedgerAllocatorIf<CountsBytes, T>> items_;

public:
    LedgerList() = default;
    ~LedgerList() {
        if constexpr (kCountsItems) {
            for (const auto& item : items_)
                L1Ledger::add(-static_cast<int64_t>(relais::detail::heapBytes(item)));
        }
    }
    LedgerList(const LedgerList&) = delete;
    LedgerList& operator=(const LedgerList&) = delete;

    void push_back(T item) {
        items_.push_back(std::move(item));
        if constexpr (kCountsItems)
            L1Ledger::add(static_cast<int64_t>(relais::detail::heapBytes(items_.back())));
    }

    [[nodiscard]] auto begin() const noexcept { return items_.begin(); }
    [[nodiscard]] auto end() const noexcept { return items_.end(); }
    [[nodiscard]] size_t size() const noexcept { return items_.size(); }
    [[nodiscard]] bool empty() const noexcept { return items_.empty(); }
};

}  // namespace jcailloux::relais::cache::detail

#endif  // JCX_RELAIS_CACHE_L1_LEDGER_H
