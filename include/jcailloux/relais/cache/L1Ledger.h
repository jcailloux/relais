#ifndef JCX_RELAIS_CACHE_L1_LEDGER_H
#define JCX_RELAIS_CACHE_L1_LEDGER_H

#include <atomic>
#include <cstddef>
#include <cstdint>

#include <utils/epoch.h>
#include <utils/threads/thread_specific.h>

#include "jcailloux/relais/BlockBytes.h"

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
//     capacity.
// A retired block not yet freed (an epoch still open) stays counted: it still
// holds its memory. Only tiers that count bytes (GDSF) report to it.
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

}  // namespace jcailloux::relais::cache::detail

#endif  // JCX_RELAIS_CACHE_L1_LEDGER_H
