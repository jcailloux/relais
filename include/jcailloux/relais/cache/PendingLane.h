#ifndef JCX_RELAIS_CACHE_PENDINGLANE_H
#define JCX_RELAIS_CACHE_PENDINGLANE_H

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <vector>

namespace jcailloux::relais::cache {

namespace detail {
    template<size_t N>
    using SmallestUintFor = std::conditional_t<(N <= 8), uint8_t,
                            std::conditional_t<(N <= 16), uint16_t,
                            std::conditional_t<(N <= 32), uint32_t, uint64_t>>>;

    struct NoCount {};
}

// =============================================================================
// PendingLane - Entries that every chunk of a cache must see once
// =============================================================================
//
// Each entry carries a generation and a bitmap of the chunks that have not
// seen it yet. A sweep of a chunk clears that chunk's bit in every entry it
// covers (generation <= cutoff); an entry whose bitmap becomes 0 has been
// seen by every chunk and is erased. Readers check an entry against a cached
// value only while the value's chunk bit is still set.
//
// The lane holds no lock: its owner guards it with one shared_mutex, shared
// for reads and for clearing bits (atomic_ref), exclusive for pushes and
// erasures. Counted lanes publish their size in an atomic, so that readers
// can skip them without the lock while they are empty.
//

template<typename Payload, size_t TotalSegments, bool Counted = false>
class PendingLane {
public:
    static_assert(TotalSegments >= 2 && TotalSegments <= 64,
                  "TotalSegments must be between 2 and 64");

    using BitmapType = detail::SmallestUintFor<TotalSegments>;

    static constexpr BitmapType initial_bitmap_ =
        TotalSegments >= sizeof(BitmapType) * 8
            ? static_cast<BitmapType>(~BitmapType{0})
            : static_cast<BitmapType>((BitmapType{1} << TotalSegments) - 1);

    struct Entry {
        Payload payload;
        uint32_t generation;
        alignas(std::atomic_ref<BitmapType>::required_alignment)
        mutable BitmapType pending_segments;
    };

private:
    std::vector<Entry> entries_;
    [[no_unique_address]]
    std::conditional_t<Counted, std::atomic<uint32_t>, detail::NoCount> count_{};

    void publishCount() {
        if constexpr (Counted)
            count_.store(static_cast<uint32_t>(entries_.size()), std::memory_order_release);
    }

public:
    // ---- exclusive lock ----------------------------------------------------

    void reserve(size_t n) { entries_.reserve(n); }

    void push(Payload payload, uint32_t gen) {
        entries_.push_back(Entry{
            .payload = std::move(payload),
            .generation = gen,
            .pending_segments = initial_bitmap_
        });
        publishCount();
    }

    /// Erase the entries at `drained` (ascending indices from collectDrained).
    void eraseDrained(const std::vector<size_t>& drained) {
        for (auto it = drained.rbegin(); it != drained.rend(); ++it) {
            size_t idx = *it;
            if (idx < entries_.size()) {
                if (idx != entries_.size() - 1) {
                    std::swap(entries_[idx], entries_.back());
                }
                entries_.pop_back();
            }
        }
        publishCount();
    }

    /// Erase every entry with generation <= cutoff_gen.
    void drain(uint32_t cutoff_gen) {
        std::erase_if(entries_, [cutoff_gen](const Entry& e) {
            return e.generation <= cutoff_gen;
        });
        publishCount();
    }

    void clear() {
        entries_.clear();
        publishCount();
    }

    /// Remove the first entry whose payload `match` selects, and return it.
    template<typename Match>
    std::optional<Payload> take(Match&& match) {
        for (size_t i = 0; i < entries_.size(); ++i) {
            if (!match(entries_[i].payload)) continue;
            std::optional<Payload> out{std::move(entries_[i].payload)};
            if (i != entries_.size() - 1) std::swap(entries_[i], entries_.back());
            entries_.pop_back();
            publishCount();
            return out;
        }
        return std::nullopt;
    }

    // ---- shared lock -------------------------------------------------------

    /// Clear `chunk_bit` in every entry with generation <= cutoff_gen, and
    /// append to `drained` the indices of those left with no pending chunk.
    void collectDrained(uint32_t cutoff_gen, BitmapType chunk_bit,
                        std::vector<size_t>& drained) const {
        for (size_t i = 0; i < entries_.size(); ++i) {
            if (entries_[i].generation > cutoff_gen) continue;
            std::atomic_ref<BitmapType> bitmap(entries_[i].pending_segments);
            BitmapType remaining = bitmap.fetch_and(
                static_cast<BitmapType>(~chunk_bit), std::memory_order_relaxed)
                & static_cast<BitmapType>(~chunk_bit);
            if (remaining == 0) drained.push_back(i);
        }
    }

    /// Callback signature: void(const Payload&, uint32_t generation, BitmapType).
    template<typename Callback>
    void forEachWithBitmap(Callback&& callback) const {
        for (const auto& e : entries_) {
            std::atomic_ref<BitmapType> bitmap(e.pending_segments);
            callback(e.payload, e.generation, bitmap.load(std::memory_order_relaxed));
        }
    }

    /// Callback signature: void(const Payload&, uint32_t generation).
    template<typename Callback>
    void forEach(Callback&& callback) const {
        for (const auto& e : entries_) {
            callback(e.payload, e.generation);
        }
    }

    [[nodiscard]] size_t size() const { return entries_.size(); }
    [[nodiscard]] bool empty() const { return entries_.empty(); }

    // ---- no lock -----------------------------------------------------------

    /// True only if the lane is counted and was empty at its last change.
    [[nodiscard]] bool skippable() const {
        if constexpr (Counted) return count_.load(std::memory_order_acquire) == 0;
        else return false;
    }

    [[nodiscard]] static constexpr BitmapType initialBitmap() { return initial_bitmap_; }
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_PENDINGLANE_H
