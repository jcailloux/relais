#ifndef JCX_RELAIS_MULTI_VIEW_H
#define JCX_RELAIS_MULTI_VIEW_H

#include <cassert>
#include <vector>

#include <utils/epoch.h>

namespace jcailloux::relais::cache {

// =============================================================================
// MultiView<E> — epoch-guarded aggregate of N positional reads
//
// Returned by findMany(): one slot per requested id, in request order.
//   items_[i] == nullptr            → id absent (not found in any tier)
//   items_[i] points into an L1 slot → zero-copy hit, kept alive by guard_
//   items_[i] points into owned_     → fallback (config without L1, or a miss
//                                      the L1 admission policy refused)
//
// A SINGLE EpochGuard pins the global epoch for ALL L1-slot pointers (an epoch
// ticket is epoch-global, not per-entry). On the all-hit hot path no I/O runs,
// so the guard is only held for the caller's usage window, like CacheView.
//
// INVARIANT: owned_ is reserved up-front (reserveOwned) before any pointer is
// taken into it — push_back must never reallocate, or items_ pointers dangle.
//
// Move-only (like CacheView). The pointers are valid only while this view (and
// thus guard_) is alive; do not extract and store raw pointers.
// =============================================================================

template<typename E>
class MultiView {
    epoch::EpochGuard guard_;
    std::vector<E> owned_;
    std::vector<const E*> items_;

public:
    MultiView() = default;

    /// Build with `n` positional slots, all initially absent (nullptr).
    explicit MultiView(size_t n) : items_(n, nullptr) {}

    MultiView(MultiView&&) noexcept = default;
    MultiView& operator=(MultiView&&) noexcept = default;
    MultiView(const MultiView&) = delete;
    MultiView& operator=(const MultiView&) = delete;

    [[nodiscard]] size_t size() const noexcept { return items_.size(); }
    [[nodiscard]] bool empty() const noexcept { return items_.empty(); }

    /// Entity at position `i`, or nullptr if absent. Bool-convertible.
    [[nodiscard]] const E* operator[](size_t i) const noexcept { return items_[i]; }

    /// True if position `i` resolved to a present entity.
    [[nodiscard]] bool has(size_t i) const noexcept { return items_[i] != nullptr; }

    // Iteration over `const E*` slots (may contain nullptr).
    [[nodiscard]] auto begin() const noexcept { return items_.begin(); }
    [[nodiscard]] auto end() const noexcept { return items_.end(); }

    // -------------------------------------------------------------------------
    // Builder API (driven by LocalRepo::findMany)
    // -------------------------------------------------------------------------

    /// Install the batch epoch guard covering every L1-slot pointer.
    void setGuard(epoch::EpochGuard g) noexcept { guard_ = std::move(g); }

    /// Freeze owned_ capacity before adopting any value (anti-reallocation).
    void reserveOwned(size_t m) { owned_.reserve(m); }

    /// Point position `i` at an external entity (e.g. an L1 slot under guard_).
    void pointAt(size_t i, const E* p) noexcept { items_[i] = p; }

    /// Move `value` into owned_ and point position `i` at it.
    /// Precondition: reserveOwned() reserved enough capacity (no realloc).
    void adopt(size_t i, E value) {
        assert(owned_.size() < owned_.capacity()
               && "MultiView::adopt would reallocate owned_ and dangle pointers");
        owned_.push_back(std::move(value));
        items_[i] = &owned_.back();
    }

    /// Transfer guard ownership out. Leaves the view's slots intact but
    /// unguarded — caller takes responsibility for L1-slot pointer lifetime.
    [[nodiscard]] epoch::EpochGuard take_guard() noexcept {
        return std::move(guard_);
    }
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_MULTI_VIEW_H
