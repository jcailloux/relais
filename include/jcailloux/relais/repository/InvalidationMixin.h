#ifndef JCX_RELAIS_INVALIDATION_MIXIN_H
#define JCX_RELAIS_INVALIDATION_MIXIN_H

#include <span>
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/WhenAll.h"
#include "jcailloux/relais/repository/InvalidateOn.h"

namespace relais_test { struct TestInternals; }

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
 * Chain: InvalidationMixin -> [ListMixin] -> LocalRepo -> [RedisRepo] -> PgRepo
 *
 * Method hiding: InvalidationMixin::update() hides Base::update().
 * The explicit Base::update() call delegates down the chain correctly.
 *
 */
template<typename Base, typename... Invalidations>
class InvalidationMixin : public Base {
    using Entity = typename Base::EntityType;
    using Key = typename Base::KeyType;
    using InvList = InvalidateOn<Invalidations...>;

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
    static io::Task<cache::CacheView<Entity>> insert(const Entity& entity)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        auto result = co_await Base::insert(entity);
        if (result) {
            co_await propagateCreate<Entity, InvList>(*result);
        }
        co_return result;
    }

    /// Update entity and propagate cross-invalidation with old/new data.
    /// When Base is ListMixin, reuses the pre-fetched old entity via WithContext
    /// to avoid a redundant L1 lookup.
    static io::Task<bool> update(const Key& id, const Entity& entity)
        requires MutableEntity<Entity> && HasFullUpdate<Entity> && (!Base::config.read_only)
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
            co_await propagateUpdate<Entity, InvList>(
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
            co_await propagateDelete<Entity, InvList>(*old);
        }
        co_return result;
    }

    /// Partial update with cross-invalidation.
    /// When Base is ListMixin, reuses the pre-fetched old entity via WithContext.
    template<typename... Updates>
    static io::Task<cache::CacheView<Entity>> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        std::optional<Entity> old;
        {
            auto view = co_await Base::find(id);
            if (view) old.emplace(*view);
        }

        cache::CacheView<Entity> result;
        if constexpr (detail::HasListMixin<Base>) {
            result = co_await Base::patchWithContext(
                id, old ? &*old : nullptr, std::forward<Updates>(updates)...);
        } else {
            result = co_await Base::patch(id, std::forward<Updates>(updates)...);
        }

        if (result) {
            co_await propagateUpdate<Entity, InvList>(
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
            co_await propagateDelete<Entity, InvList>(*old);
        }
        co_await Base::invalidate(id);
    }

protected:
    /// Batch invalidation, critical pass — no cross-target here (it is deferred):
    /// delegate the entity tier + L1 list tracker straight down the chain. Awaited
    /// by the facade (latency-critical: the entity UNLINK must precede the return,
    /// see PgRepo for the split rationale).
    template<bool WithLists = true>
    static io::Task<void> invalidateManyCritical(std::span<const Entity> entities) {
        co_await Base::template invalidateManyCritical<WithLists>(entities);
    }

    /// Batch invalidation, deferred pass — deduplicated cross-target invalidation
    /// (union of distinct target keys invalidated once, materialized by value
    /// before any invalidation — the InvalidationData raw pointers never outlive
    /// this frame) co-pipelined with the own L2 list EVALs (disjoint caches, no
    /// inter-dependency) so their commands share one flush. Fired fire-and-forget
    /// by the facade (invalidate-stale tolerated).
    template<bool WithLists = true>
    static io::Task<void> invalidateManyDeferred(std::span<const Entity> entities) {
        co_await io::whenAll(
            propagateDeleteMany<Entity, InvList>(entities),
            Base::template invalidateManyDeferred<WithLists>(entities));
    }

    friend struct ::relais_test::TestInternals;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INVALIDATION_MIXIN_H
