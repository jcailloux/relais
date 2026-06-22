#ifndef JCX_RELAIS_RECHECKGUARD_H
#define JCX_RELAIS_RECHECKGUARD_H

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "jcailloux/relais/cache/ChunkMap.h"   // cache::detail::AutoHash
#include "jcailloux/relais/config/FixedString.h"

namespace jcailloux::relais {

/// Read-fill recheck guard — a sharded generation counter shared by the L1
/// (LocalRepo) and L2 (RedisRepo) layers of a single repository.
///
/// The bug it closes: `find` on a miss fetches from a lower tier and stores
/// the result unconditionally. A reader whose fetch straddles a concurrent
/// delete re-writes the deleted row back into the tier *after* eviction
/// cleared it → a persistent phantom (nothing re-evicts; survives to TTL).
/// Intra-op tier ordering can't fix it — the reader's write-back is a third,
/// inter-thread write.
///
/// Mechanism (hit cost = 0): a filling reader snapshots its key's slot at
/// fetch-start, then recompares right before it stores. If a mutation bumped
/// the slot in between, the value straddled a write → return it to the caller
/// but do NOT cache it (the next read re-fetches). Hits never touch the
/// counter, so already-cached entries are immune.
///
/// Sharded, not per-key: the recheck fires during a MISS (entry absent), so
/// the slot must exist when the key is not cached — it cannot live in the
/// entry metadata, and an exact per-key counter would be unbounded. The fixed
/// array bounds memory by accepting collisions: a hammered key bumps another
/// key's slot → that other key's fill is skipped (one extra miss) — pessimistic,
/// NEVER stale.
///
/// No wraparound, any width: the comparison window is the FETCH duration
/// (µs–ms), not the entry lifetime. A false "unchanged" would need 2⁶⁴ bumps
/// during one fetch → impossible. `uint64_t` for comfort; no backstop needed.
///
/// Shared across the mixin chain: LocalRepo and RedisRepo for the same repo
/// instantiate RecheckGuard<Name, Key, SlotsLog2> identically → one static
/// array per repo, visible to both tiers. (In an L2-only config LocalRepo is
/// absent, so RedisRepo both bumps and rechecks; in L1+L2 both layers bump the
/// same array — a redundant bump is harmless, the counter is monotonic.)
template<config::FixedString Name, typename Key, std::size_t SlotsLog2>
struct RecheckGuard {
    static constexpr std::size_t kSlots = std::size_t{1} << SlotsLog2;
    static constexpr std::size_t kMask = kSlots - 1;
    using Hash = cache::detail::AutoHash<Key>;

    static inline std::array<std::atomic<uint64_t>, kSlots> slots_{};

    static std::size_t slotOf(const Key& id) {
        return Hash{}(id) & kMask;
    }

    /// Snapshot the slot for a key at fetch-start. Relaxed: just the baseline
    /// for the later changed() comparison; hits never read this counter.
    static uint64_t snapshot(const Key& id) {
        return slots_[slotOf(id)].load(std::memory_order_relaxed);
    }

    /// True if the slot moved since `snap` — a mutation landed during the fetch.
    ///
    /// Acquire (paired with bump's release): if this load does NOT observe a
    /// concurrent bump, then that bump — and everything sequenced after it on
    /// the writer (its cache evict / Redis UNLINK) — had not yet become
    /// globally visible. Combined with the seq_cst total order of the ChunkMap
    /// and Redis ops, that forces the reader's store to be ordered BEFORE the
    /// writer's evict (the evict then removes it) — so a missed bump can never
    /// leave a surviving phantom. This makes correctness hold in the C++
    /// abstract machine and on weak HW (ARM/POWER), not just on x86-TSO where
    /// the seq_cst map ops would incidentally cover it. Free on x86 (plain mov).
    static bool changed(const Key& id, uint64_t snap) {
        return slots_[slotOf(id)].load(std::memory_order_acquire) != snap;
    }

    /// Bump on every confirmed mutation (create/update/patch/erase/invalidate).
    /// Synchronous, write-only, monotonic. Release: see changed().
    static void bump(const Key& id) {
        slots_[slotOf(id)].fetch_add(1, std::memory_order_release);
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_RECHECKGUARD_H
