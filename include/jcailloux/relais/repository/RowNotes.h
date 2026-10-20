#ifndef JCX_RELAIS_REPOSITORY_ROWNOTES_H
#define JCX_RELAIS_REPOSITORY_ROWNOTES_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "jcailloux/relais/cache/ChunkMap.h"   // cache::detail::AutoHash
#include "jcailloux/relais/cache/PendingLane.h"
#include "jcailloux/relais/repository/RowMatch.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais::detail {

// =============================================================================
// RowNotes - rows an uncertain write may have changed, for the entity tier
// =============================================================================
//
// A write that may have changed rows nobody can name leaves a note: the
// predicate those rows satisfied. A note touches a cached copy if its
// predicate may hold for it; a touched copy is evicted when read.
//
// A note is Pending until its write is settled: the write may still commit,
// so it touches every copy, whenever stored. Once settled it touches only the
// copies stored before the seal: a copy read from the database afterwards is
// exempt (its key is recorded when it is stored). A pass over the chunks then
// evicts the touched copies left; each chunk it covers clears its bit in the
// note (see PendingLane), and the note goes with its last bit. A Pending note
// is never drained. A note whose write is found aborted is dropped at once.
//
// No per-copy state: while no note exists, a hit pays one atomic load.
//
// Chunks: a copy's chunk is the top ChunkCountLog2 bits of its bucket index,
// as ChunkMap::cleanup_chunk splits the buckets in 1 << ChunkCountLog2. The
// two agree while the table has at least that many buckets: it starts with
// 64 or more and only grows, and ChunkCountLog2 is at most 6.
//

template<typename E, typename Key, uint8_t ChunkCountLog2>
class RowNotes {
public:
    static constexpr size_t kChunks = size_t{1} << ChunkCountLog2;

    /// Empty: every row.
    using Predicate = RowPredicate<E>;

private:
    struct Note {
        uint64_t id = 0;
        Predicate pred;
        bool settled = false;
        // Written under the exclusive lock only.
        mutable std::unordered_set<Key, cache::detail::AutoHash<Key>> exempt;

        [[nodiscard]] bool mayMatch(const E& value) const {
            return !pred || pred(value) == RowMatch::Maybe;
        }
        [[nodiscard]] bool touches(const Key& key, const E& value) const {
            return mayMatch(value) && !exempt.contains(key);
        }
    };

    using Lane = cache::PendingLane<Note, kChunks, true>;

public:
    using BitmapType = typename Lane::BitmapType;

private:
    // Beyond every cutoff: a Pending note keeps all its bits.
    static constexpr uint32_t kPendingGeneration = std::numeric_limits<uint32_t>::max();

    Lane lane_;
    mutable std::shared_mutex mutex_;
    uint32_t generation_ = 0;   // of the last note settled
    uint64_t next_id_ = 0;
    bool draining_ = false;
#ifdef RELAIS_BUILDING_TESTS
    bool hold_drain_ = false;
    mutable std::atomic<uint64_t> hit_checks_{0};
#endif

    static BitmapType chunkBit(size_t chunk) {
        return static_cast<BitmapType>(BitmapType{1} << chunk);
    }

    void pushSettled(Note note) {
        note.settled = true;
        note.exempt.clear();
        lane_.push(std::move(note), ++generation_);
    }

public:
    /// One acquire load: false while no note exists.
    [[nodiscard]] bool active() const noexcept { return !lane_.skippable(); }

    // ---- writes -------------------------------------------------------------
    //
    // `bumpAll` runs under the lock, so that a fill in flight fails its
    // recheck and a copy is exempted only if no seal happened since its read
    // started (see exemptStored).

    /// Add a Pending note; returns its id, for settle().
    template<typename BumpAll>
    uint64_t addPending(Predicate pred, BumpAll&& bumpAll) {
        std::unique_lock lock(mutex_);
        bumpAll();
        const uint64_t id = ++next_id_;
        lane_.push(Note{.id = id, .pred = std::move(pred)}, kPendingGeneration);
        return id;
    }

    /// Add a note already settled (its write can no longer commit).
    template<typename BumpAll>
    void addSettled(Predicate pred, BumpAll&& bumpAll) {
        std::unique_lock lock(mutex_);
        bumpAll();
        pushSettled(Note{.id = ++next_id_, .pred = std::move(pred)});
    }

    /// Settle note `id`: from now on it touches only the copies stored before.
    template<typename BumpAll>
    void settle(uint64_t id, BumpAll&& bumpAll) {
        std::unique_lock lock(mutex_);
        bumpAll();
        if (auto note = lane_.take([id](const Note& n) { return n.id == id; }))
            pushSettled(std::move(*note));
    }

    /// Drop note `id`: its write was aborted, the rows it named are as they
    /// were, and every copy stored meanwhile was read from them.
    void discard(uint64_t id) {
        std::unique_lock lock(mutex_);
        lane_.take([id](const Note& n) { return n.id == id; });
    }

    /// Record a copy read from the database and stored as exempt from the
    /// settled notes that touch it. `fresh()`, under the lock, says that no
    /// seal happened since its read started: a copy read before a seal may
    /// predate the write's commit. A failure leaves the copy touched (it is
    /// read again).
    template<typename Fresh>
    void exemptStored(const Key& key, const E& value, Fresh&& fresh) noexcept {
        try {
            {
                std::shared_lock lock(mutex_);
                bool any = false;
                lane_.forEach([&](const Note& n, uint32_t) {
                    any = any || (n.settled && n.touches(key, value));
                });
                if (!any) return;
            }
            std::unique_lock lock(mutex_);
            if (!fresh()) return;
            lane_.forEach([&](const Note& n, uint32_t) {
                if (n.settled && n.mayMatch(value)) n.exempt.insert(key);
            });
        } catch (...) {
        }
    }

    // ---- reads --------------------------------------------------------------

    /// Hit path, once active(): true if a note touches the copy in the chunk
    /// `key_hash` names, as long as that chunk's bit is set.
    [[nodiscard]] bool touches(const Key& key, const E& value, size_t key_hash) const {
#ifdef RELAIS_BUILDING_TESTS
        hit_checks_.fetch_add(1, std::memory_order_relaxed);
#endif
        const BitmapType bit = chunkBit((key_hash >> (48 - ChunkCountLog2)) & (kChunks - 1));
        std::shared_lock lock(mutex_);
        bool touched = false;
        lane_.forEachWithBitmap([&](const Note& n, uint32_t, BitmapType pending) {
            touched = touched || ((pending & bit) && n.touches(key, value));
        });
        return touched;
    }

    // ---- drain pass ---------------------------------------------------------
    //
    // One pass at a time: claimDrain() starts it, continueDraining() ends it.
    // Per chunk: cutoff(), evict what touchesSettled() selects, drainChunk().

    /// True if the caller must run the pass (none runs, a note is settled).
    [[nodiscard]] bool claimDrain() {
        std::unique_lock lock(mutex_);
#ifdef RELAIS_BUILDING_TESTS
        if (hold_drain_) return false;
#endif
        if (draining_) return false;
        bool settled = false;
        lane_.forEach([&](const Note& n, uint32_t) { settled = settled || n.settled; });
        draining_ = settled;
        return settled;
    }

    /// The notes a chunk's step covers: those settled before it starts.
    [[nodiscard]] uint32_t cutoff() const {
        std::shared_lock lock(mutex_);
        return generation_;
    }

    [[nodiscard]] bool touchesSettled(const Key& key, const E& value,
                                      uint32_t cutoff_gen, size_t chunk) const {
        const BitmapType bit = chunkBit(chunk);
        std::shared_lock lock(mutex_);
        bool touched = false;
        lane_.forEachWithBitmap([&](const Note& n, uint32_t gen, BitmapType pending) {
            touched = touched
                || (n.settled && gen <= cutoff_gen && (pending & bit) && n.touches(key, value));
        });
        return touched;
    }

    /// One exclusive section: settle() reorders the lane.
    void drainChunk(uint32_t cutoff_gen, size_t chunk) {
        std::vector<size_t> drained;
        std::unique_lock lock(mutex_);
        lane_.collectDrained(cutoff_gen, chunkBit(chunk), drained);
        lane_.eraseDrained(drained);
    }

    /// End of a round over every chunk: true to run another (a settled note
    /// is left), false and the pass is over.
    [[nodiscard]] bool continueDraining() {
        std::unique_lock lock(mutex_);
        bool settled = false;
        lane_.forEach([&](const Note& n, uint32_t) { settled = settled || n.settled; });
        draining_ = settled;
        return settled;
    }

    /// The pass failed: the next seal starts another.
    void abortDraining() noexcept {
        std::unique_lock lock(mutex_);
        draining_ = false;
    }

    [[nodiscard]] size_t size() const {
        std::shared_lock lock(mutex_);
        return lane_.size();
    }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_REPOSITORY_ROWNOTES_H
