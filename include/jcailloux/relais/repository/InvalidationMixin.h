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
 * Sits at the top of the mixin chain and intercepts insert/update/erase
 * to propagate invalidations to dependent caches.
 *
 * Chain: InvalidationMixin -> [ListMixin] -> CachedRepo -> [RedisRepo] -> BaseRepo
 *
 * Method hiding: InvalidationMixin::update() hides Base::update().
 * The explicit Base::update() call delegates down the chain correctly.
 *
 * EntityView from find() is converted to shared_ptr for cross-invalidation
 * propagation (write paths only — read paths return views directly).
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
    using typename Base::FindResultType;
    using Base::name;
    using Base::find;

    // Expose Invalidates type for external detection
    using Invalidates = InvList;

    /// Insert entity and propagate cross-invalidation to dependent caches.
    static io::Task<wrapper::EntityView<Entity>> insert(const Entity& entity)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        auto result = co_await Base::insert(entity);
        if (result) {
            co_await cache::propagateCreate<Entity, InvList>(*result);
        }
        co_return result;
    }

    /// Update entity and propagate cross-invalidation with old/new data.
    /// When Base is ListMixin, reuses the pre-fetched old entity via WithContext
    /// to avoid a redundant L1 lookup.
    static io::Task<bool> update(const Key& id, const Entity& entity)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        std::optional<Entity> old;
        {
            auto view = co_await Base::find(id);
            if (view) old.emplace(*view);
        }

        bool ok;
        if constexpr (detail::HasListMixin<Base>) {
            ok = co_await Base::updateWithContext(id, entity, old ? &*old : nullptr);
        } else {
            ok = co_await Base::update(id, entity);
        }

        if (ok) {
            co_await cache::propagateUpdate<Entity, InvList>(
                old ? &*old : nullptr, entity);
        }
        co_return ok;
    }

    /// Erase entity and propagate cross-invalidation with deleted data.
    /// When Base is ListMixin, reuses the pre-fetched entity via WithContext.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Base::config.read_only)
    {
        std::optional<Entity> old;
        {
            auto view = co_await Base::find(id);
            if (view) old.emplace(*view);
        }

        std::optional<size_t> result;
        if constexpr (detail::HasListMixin<Base>) {
            result = co_await Base::eraseWithContext(id, old ? &*old : nullptr);
        } else {
            result = co_await Base::erase(id);
        }

        if (result.has_value() && old) {
            co_await cache::propagateDelete<Entity, InvList>(*old);
        }
        co_return result;
    }

    /// Partial update with cross-invalidation.
    /// When Base is ListMixin, reuses the pre-fetched old entity via WithContext.
    template<typename... Updates>
    static io::Task<wrapper::EntityView<Entity>> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        std::optional<Entity> old;
        {
            auto view = co_await Base::find(id);
            if (view) old.emplace(*view);
        }

        wrapper::EntityView<Entity> result;
        if constexpr (detail::HasListMixin<Base>) {
            result = co_await Base::patchWithContext(
                id, old ? &*old : nullptr, std::forward<Updates>(updates)...);
        } else {
            result = co_await Base::patch(id, std::forward<Updates>(updates)...);
        }

        if (result) {
            co_await cache::propagateUpdate<Entity, InvList>(
                old ? &*old : nullptr, *result);
        }
        co_return result;
    }

    /// Invalidate all caches (L1 + L2) and propagate cross-invalidation.
    static io::Task<void> invalidate(const Key& id) {
        std::optional<Entity> old;
        {
            auto view = co_await Base::find(id);
            if (view) old.emplace(*view);
        }
        if (old) {
            co_await cache::propagateDelete<Entity, InvList>(*old);
        }
        co_await Base::invalidate(id);
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INVALIDATION_MIXIN_H
