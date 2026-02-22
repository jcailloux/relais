#ifndef JCX_RELAIS_WRAPPER_BUFFER_VIEW_H
#define JCX_RELAIS_WRAPPER_BUFFER_VIEW_H

#include <cstdint>
#include <string>
#include <vector>

#include <utils/epoch.h>

namespace jcailloux::relais::wrapper {

// =============================================================================
// BufferView<T> — epoch-guarded read-only view of a serialization buffer
//
// Holds a raw pointer to the buffer + an EpochGuard ticket that prevents
// epoch-based reclamation while the view is alive.
//
// sizeof(BufferView) = 12 bytes (ptr 8 + ticket int 4).
// Thread-agnostic: tickets migrate freely across threads (safe across co_await).
// =============================================================================

template<typename T>
class BufferView {
    const T* ptr_ = nullptr;
    epoch::EpochGuard guard_;

public:
    BufferView() = default;

    BufferView(const T* p, epoch::EpochGuard g)
        : ptr_(p), guard_(std::move(g)) {}

    BufferView(BufferView&&) noexcept = default;
    BufferView& operator=(BufferView&&) noexcept = default;
    BufferView(const BufferView&) = delete;
    BufferView& operator=(const BufferView&) = delete;

    explicit operator bool() const { return ptr_ != nullptr; }
    const T& operator*() const { return *ptr_; }
    const T* operator->() const { return ptr_; }
    const T* get() const { return ptr_; }

    /// Transfer guard ownership (e.g., BufferView<ListWrapper> → JsonView/BinaryView).
    /// Leaves this view empty (ptr_ = nullptr, guard moved out).
    epoch::EpochGuard take_guard() {
        auto g = std::move(guard_);
        ptr_ = nullptr;
        return g;
    }

    friend bool operator==(const BufferView& v, std::nullptr_t) { return v.ptr_ == nullptr; }
    friend bool operator!=(const BufferView& v, std::nullptr_t) { return v.ptr_ != nullptr; }
};

using JsonView = BufferView<std::string>;
using BinaryView = BufferView<std::vector<uint8_t>>;

}  // namespace jcailloux::relais::wrapper

#endif  // JCX_RELAIS_WRAPPER_BUFFER_VIEW_H
