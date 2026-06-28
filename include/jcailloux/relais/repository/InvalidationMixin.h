#ifndef JCX_RELAIS_INVALIDATION_MIXIN_H
#define JCX_RELAIS_INVALIDATION_MIXIN_H

#include <span>
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/WhenAll.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/repository/InvalidateOn.h"
#include "jcailloux/relais/Log.h"

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
        // co_await is illegal in a catch handler: capture the uncertain timeout,
        // run the precautionary cross-invalidation after the try, then rethrow.
        cache::CacheView<Entity> result;
        std::exception_ptr timeout;
        try {
            result = co_await Base::insert(entity);
        } catch (const io::PgQueryTimeout&) {
            timeout = std::current_exception();
        }
        if (timeout) {
            // Uncertain: the row may have been inserted (RETURNING id lost).
            // Cross-invalidate from the INPUT entity — the dependent keys live in
            // the entity's own fields, not the generated id. Best-effort.
            try {
                co_await propagateCreate<Entity, InvList>(entity);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (insert timeout) - " << e.what();
            }
            std::rethrow_exception(timeout);
        }
        if (result) {
            // The write committed; cross-invalidation is best-effort. A dependency
            // may resolve its target keys through a DB read (InvalidateVia) that can
            // now surface an L3 error — never let that turn a committed write into an
            // apparent failure (the caller would retry → double-write). Log and
            // return success; an un-invalidated cross-target is TTL-bounded.
            try {
                co_await propagateCreate<Entity, InvList>(*result);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (insert committed) - " << e.what();
            }
        }
        co_return result;
    }

    /// Update entity and propagate cross-invalidation with old/new data.
    /// When Base is ListMixin, reuses the pre-fetched old entity via WithContext
    /// to avoid a redundant L1 lookup.
    static io::Task<std::optional<size_t>> update(const Key& id, const Entity& entity)
        requires MutableEntity<Entity> && HasFullUpdate<Entity> && (!Base::config.read_only)
    {
        std::optional<Entity> old = co_await findOldBestEffort(id);

        std::optional<size_t> affected;
        std::exception_ptr timeout;
        try {
            if constexpr (detail::HasListMixin<Base>) {
                affected = co_await Base::updateWithContext(id, entity, old ? &*old : nullptr);
            } else {
                affected = co_await Base::update(id, entity);
            }
        } catch (const io::PgQueryTimeout&) {
            timeout = std::current_exception();
        }
        if (timeout) {
            // Uncertain: cross-invalidate by precaution, keyed on old (lost ⇒
            // null) and the input new. Best-effort. The list/entity tiers already
            // evicted themselves as the exception unwound through the inner layers.
            try {
                co_await propagateUpdate<Entity, InvList>(
                    old ? &*old : nullptr, entity);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (update timeout) - " << e.what();
            }
            std::rethrow_exception(timeout);
        }
        if (affected.value_or(0) > 0) {
            // Best-effort: a committed write never fails on cross-invalidation.
            try {
                co_await propagateUpdate<Entity, InvList>(
                    old ? &*old : nullptr, entity);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (update committed) - " << e.what();
            }
        }
        co_return affected;
    }

    /// Erase entity and propagate cross-invalidation with deleted data.
    /// When Base is ListMixin, reuses the pre-fetched entity via WithContext.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Base::config.read_only)
    {
        std::optional<Entity> old = co_await findOldBestEffort(id);

        std::optional<size_t> result;
        std::exception_ptr timeout;
        try {
            if constexpr (detail::HasListMixin<Base>) {
                result = co_await Base::eraseWithContext(id, old ? &*old : nullptr);
            } else {
                result = co_await Base::erase(id);
            }
        } catch (const io::PgQueryTimeout&) {
            timeout = std::current_exception();
        }
        if (timeout) {
            // Uncertain: cross-invalidate by precaution from old (if known).
            // Best-effort; without old the cross tier is bounded by l*_ttl.
            if (old) {
                try {
                    co_await propagateDelete<Entity, InvList>(*old);
                } catch (const std::exception& e) {
                    RELAIS_LOG_ERROR << name()
                        << ": cross-invalidation failed (erase timeout) - " << e.what();
                }
            }
            std::rethrow_exception(timeout);
        }
        if (result.has_value() && old) {
            // Best-effort: a committed delete never fails on cross-invalidation.
            try {
                co_await propagateDelete<Entity, InvList>(*old);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (erase committed) - " << e.what();
            }
        }
        co_return result;
    }

    /// Partial update with cross-invalidation.
    /// When Base is ListMixin, reuses the pre-fetched old entity via WithContext.
    template<typename... Updates>
    static io::Task<cache::CacheView<Entity>> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        std::optional<Entity> old = co_await findOldBestEffort(id);

        cache::CacheView<Entity> result;
        std::exception_ptr timeout;
        try {
            if constexpr (detail::HasListMixin<Base>) {
                result = co_await Base::patchWithContext(
                    id, old ? &*old : nullptr, std::forward<Updates>(updates)...);
            } else {
                result = co_await Base::patch(id, std::forward<Updates>(updates)...);
            }
        } catch (const io::PgQueryTimeout&) {
            timeout = std::current_exception();
        }
        if (timeout) {
            // Uncertain: the patched new value is unknown (no RETURNING). Cross-
            // invalidate keyed on old only (old as both old and new ⇒ old's target
            // keys). A patch that moved a cross key leaves that target bounded by
            // l*_ttl (documented residual). Best-effort.
            if (old) {
                try {
                    co_await propagateUpdate<Entity, InvList>(&*old, *old);
                } catch (const std::exception& e) {
                    RELAIS_LOG_ERROR << name()
                        << ": cross-invalidation failed (patch timeout) - " << e.what();
                }
            }
            std::rethrow_exception(timeout);
        }
        if (result) {
            // Best-effort: a committed patch never fails on cross-invalidation.
            try {
                co_await propagateUpdate<Entity, InvList>(
                    old ? &*old : nullptr, *result);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (patch committed) - " << e.what();
            }
        }
        co_return result;
    }

    /// Invalidate all caches (L1 + L2) and propagate cross-invalidation.
    static io::Task<void> invalidate(const Key& id) {
        std::optional<Entity> old = co_await findOldBestEffort(id);
        if (old) {
            // Best-effort: a cross-invalidation failure must not skip the primary
            // entity-tier eviction below.
            try {
                co_await propagateDelete<Entity, InvList>(*old);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (invalidate) - " << e.what();
            }
        }
        co_await Base::invalidate(id);
    }

protected:
    /// Best-effort pre-read for cross-invalidation context. The read path no
    /// longer swallows L3 errors, so this pre-read can throw; a failure is
    /// treated as "old unknown" and the caller proceeds with the write +
    /// precautionary invalidation rather than aborting on a preliminary read.
    static io::Task<std::optional<Entity>> findOldBestEffort(const Key& id) {
        try {
            auto view = co_await Base::find(id);
            if (view) co_return Entity(*view);
        } catch (const io::PgError&) {
            // old unknown — proceed without it.
        }
        co_return std::nullopt;
    }

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
