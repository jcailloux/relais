#ifndef JCX_RELAIS_INVALIDATION_MIXIN_H
#define JCX_RELAIS_INVALIDATION_MIXIN_H

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/cache/InvalidateOn.h"

namespace jcailloux::relais {

namespace detail {
/// Detect whether Base is (or derives from) ListMixin.
template<typename T>
concept HasListMixin = requires { typename T::ListDescriptorType; };
}  // namespace detail

/**
 * Optional mixin layer for cross-repository cache invalidation.
 *
 * Activated when the Repo has variadic Invalidations... (non-empty).
 * Sits at the top of the mixin chain and intercepts create/update/remove
 * to propagate invalidations to dependent caches.
 *
 * Chain: InvalidationMixin -> [ListMixin] -> CachedRepo -> [RedisRepo] -> BaseRepo
 *
 * Method hiding: InvalidationMixin::update() hides Base::update().
 * The explicit Base::update() call delegates down the chain correctly.
 */
template<typename Base, typename... Invalidations>
class InvalidationMixin : public Base {
    using Entity = typename Base::EntityType;
    using Key = typename Base::KeyType;
    using InvList = cache::InvalidateOn<Invalidations...>;

public:
    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::WrapperPtrType;
    using Base::name;
    using Base::find;

    // Expose Invalidates type for external detection
    using Invalidates = InvList;

    /// Create entity and propagate cross-invalidation to dependent caches.
    static io::Task<WrapperPtrType> create(WrapperPtrType wrapper)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        auto result = co_await Base::create(std::move(wrapper));
        if (result) {
            co_await cache::propagateCreate<Entity, InvList>(result);
        }
        co_return result;
    }

    /// Update entity and propagate cross-invalidation with old/new data.
    /// When Base is ListMixin, reuses the pre-fetched old entity via WithContext
    /// to avoid a redundant L1 lookup.
    static io::Task<bool> update(const Key& id, WrapperPtrType wrapper)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        auto old = co_await Base::find(id);
        auto new_entity = wrapper;

        bool ok;
        if constexpr (detail::HasListMixin<Base>) {
            ok = co_await Base::updateWithContext(id, std::move(wrapper), old);
        } else {
            ok = co_await Base::update(id, std::move(wrapper));
        }

        if (ok) {
            co_await cache::propagateUpdate<Entity, InvList>(
                std::move(old), std::move(new_entity));
        }
        co_return ok;
    }

    /// Remove entity and propagate cross-invalidation with deleted data.
    /// When Base is ListMixin, reuses the pre-fetched entity via WithContext.
    static io::Task<std::optional<size_t>> remove(const Key& id)
        requires (!Base::config.read_only)
    {
        auto entity = co_await Base::find(id);

        std::optional<size_t> result;
        if constexpr (detail::HasListMixin<Base>) {
            result = co_await Base::removeWithContext(id, entity);
        } else {
            result = co_await Base::remove(id);
        }

        if (result.has_value() && entity) {
            co_await cache::propagateDelete<Entity, InvList>(std::move(entity));
        }
        co_return result;
    }

    /// Partial update with cross-invalidation.
    /// When Base is ListMixin, reuses the pre-fetched old entity via WithContext.
    template<typename... Updates>
    static io::Task<WrapperPtrType> updateBy(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        auto old = co_await Base::find(id);

        WrapperPtrType result;
        if constexpr (detail::HasListMixin<Base>) {
            result = co_await Base::updateByWithContext(
                id, old, std::forward<Updates>(updates)...);
        } else {
            result = co_await Base::updateBy(id, std::forward<Updates>(updates)...);
        }

        if (result) {
            co_await cache::propagateUpdate<Entity, InvList>(
                std::move(old), result);
        }
        co_return result;
    }

    /// Invalidate all caches (L1 + L2) and propagate cross-invalidation.
    static io::Task<void> invalidate(const Key& id) {
        auto entity = co_await Base::find(id);
        if (entity) {
            co_await cache::propagateDelete<Entity, InvList>(std::move(entity));
        }
        co_await Base::invalidate(id);
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INVALIDATION_MIXIN_H
