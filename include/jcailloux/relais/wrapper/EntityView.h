#ifndef JCX_RELAIS_WRAPPER_ENTITY_VIEW_H
#define JCX_RELAIS_WRAPPER_ENTITY_VIEW_H

#include <utils/epoch.h>

namespace jcailloux::relais::wrapper {

// =============================================================================
// EntityView<Entity> — epoch-guarded read-only view of a cached entity
//
// Holds a raw pointer to the entity + an EpochGuard ticket that prevents
// epoch-based reclamation while the view is alive.
//
// sizeof(EntityView) = 12 bytes (ptr 8 + ticket int 4).
// Thread-agnostic: tickets migrate freely across threads (safe across co_await).
// =============================================================================

template<typename Entity>
class EntityView {
    const Entity* ptr_ = nullptr;
    epoch::EpochGuard guard_;

public:
    EntityView() = default;

    EntityView(const Entity* p, epoch::EpochGuard g)
        : ptr_(p), guard_(std::move(g)) {}

    EntityView(EntityView&&) noexcept = default;
    EntityView& operator=(EntityView&&) noexcept = default;
    EntityView(const EntityView&) = delete;
    EntityView& operator=(const EntityView&) = delete;

    explicit operator bool() const { return ptr_ != nullptr; }
    const Entity& operator*() const { return *ptr_; }
    const Entity* operator->() const { return ptr_; }
    const Entity* get() const { return ptr_; }

    friend bool operator==(const EntityView& v, std::nullptr_t) { return v.ptr_ == nullptr; }
    friend bool operator!=(const EntityView& v, std::nullptr_t) { return v.ptr_ != nullptr; }

    /// Transfer guard ownership (e.g., EntityView → JsonView/BinaryView).
    /// Leaves this view empty (ptr_ = nullptr, guard moved out).
    epoch::EpochGuard take_guard() {
        auto g = std::move(guard_);
        ptr_ = nullptr;
        return g;
    }
};

}  // namespace jcailloux::relais::wrapper

#endif  // JCX_RELAIS_WRAPPER_ENTITY_VIEW_H
