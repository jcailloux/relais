#ifndef JCX_RELAIS_CACHE_VIEW_H
#define JCX_RELAIS_CACHE_VIEW_H

#include <utils/epoch.h>

namespace jcailloux::relais::cache {

// =============================================================================
// CacheView<T> — epoch-guarded read-only view of a cached value
//
// Holds a raw pointer to the value + an EpochGuard ticket that prevents
// epoch-based reclamation while the view is alive.
//
// sizeof(CacheView) = 12 bytes (ptr 8 + ticket int 4).
// Thread-agnostic: tickets migrate freely across threads (safe across co_await).
//
// Pointer valid ONLY while guard exists. Do not store the raw pointer;
// store the CacheView itself (moveable, non-copyable).
// =============================================================================

template<typename T>
class CacheView {
    const T* ptr_ = nullptr;
    epoch::EpochGuard guard_;

public:
    CacheView() = default;

    CacheView(const T* p, epoch::EpochGuard g)
        : ptr_(p), guard_(std::move(g)) {}

    CacheView(CacheView&&) noexcept = default;
    CacheView& operator=(CacheView&&) noexcept = default;
    CacheView(const CacheView&) = delete;
    CacheView& operator=(const CacheView&) = delete;

    explicit operator bool() const { return ptr_ != nullptr; }
    const T& operator*() const { return *ptr_; }
    const T* operator->() const { return ptr_; }
    const T* get() const { return ptr_; }

    friend bool operator==(const CacheView& v, std::nullptr_t) { return v.ptr_ == nullptr; }
    friend bool operator!=(const CacheView& v, std::nullptr_t) { return v.ptr_ != nullptr; }

    /// Transfer guard ownership. Leaves this view empty (ptr_ = nullptr, guard moved out).
    epoch::EpochGuard take_guard() {
        auto g = std::move(guard_);
        ptr_ = nullptr;
        return g;
    }
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_VIEW_H
