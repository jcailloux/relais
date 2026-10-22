#ifndef JCX_RELAIS_INVALIDATION_MIXIN_H
#define JCX_RELAIS_INVALIDATION_MIXIN_H

#include <span>
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/repository/InvalidateOn.h"
#include "jcailloux/relais/repository/Precaution.h"
#include "jcailloux/relais/Log.h"

namespace relais_test { struct TestInternals; }

namespace jcailloux::relais {

namespace detail {
/// Detect whether Base is (or derives from) ListMixin.
template<typename T>
concept HasListMixin = requires { typename T::ListDescriptorType; };

/**
 * Optional mixin layer for cross-repository cache invalidation.
 *
 * Activated when the Repo has variadic Invalidations... (non-empty).
 * Sits at the top of the mixin chain and intercepts insert/update/erase
 * to propagate invalidations to dependent caches.
 *
 * Chain: InvalidationMixin -> [ListMixin] -> [LocalRepo] -> PgRepo
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
        std::shared_ptr<io::detail::SettleTicket> ticket;
        try {
            result = co_await Base::insert(entity);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            timeout = std::current_exception();
        }
        if (timeout) {
            // Uncertain: the row may have been inserted (RETURNING lost). No
            // row before it.
            std::optional<Key> id;
            if constexpr (kInsertKeyKnown<Entity>) id = Key(entity.key());
            co_await crossUncertain(std::move(ticket), std::move(id), RowBefore::absent(),
                                    entity, "insert");
            std::rethrow_exception(timeout);
        }
        if (result) {
            // The write committed; cross-invalidation is best-effort. A dependency
            // may resolve its target keys through a DB read (InvalidateVia) that can
            // now surface a DB error — never let that turn a committed write into an
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

    /// Insert-or-update and propagate cross-invalidation. A single best-effort
    /// pre-image is the create/update discriminant and is threaded to the list tier
    /// via upsertWithContext, so the row is read once for both tiers. Cross-targets
    /// are invalidated old→new on the update branch and from the committed row on
    /// the create branch — the same propagate primitives as insert/update.
    static io::Task<cache::CacheView<Entity>> upsert(const Entity& entity)
        requires UpsertableEntity<Entity, Key> && HasUpsertSql<Entity> && (!Base::config.read_only)
    {
        // A failed read leaves the row before unknown, not absent.
        RowBefore before;
        try {
            auto view = co_await Base::find(entity.key());
            if (view) before = RowBefore::of(Entity(*view));
            else before = RowBefore::absent();
        } catch (const io::PgError&) {
            if constexpr (detail::HasListMixin<Base>) {
                Base::keepPageCopy(entity.key(), &before.row);
                if (before.row) before.known = true;
            }
        }
        const std::optional<Entity>& old = before.row;

        cache::CacheView<Entity> result;
        std::exception_ptr timeout;
        std::shared_ptr<io::detail::SettleTicket> ticket;
        try {
            if constexpr (detail::HasListMixin<Base>) {
                result = co_await Base::upsertWithContext(entity, old ? &*old : nullptr);
            } else {
                result = co_await Base::upsert(entity);
            }
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            timeout = std::current_exception();
        }
        if (timeout) {
            // Uncertain: either branch may commit; the key is known in both.
            co_await crossUncertain(std::move(ticket), Key(entity.key()), std::move(before),
                                    entity, "upsert");
            std::rethrow_exception(timeout);
        }
        if (result) {
            // Best-effort: a committed write never fails on cross-invalidation.
            try {
                if (old) co_await propagateUpdate<Entity, InvList>(&*old, *result);
                else co_await propagateCreate<Entity, InvList>(*result);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (upsert committed) - " << e.what();
            }
        }
        co_return result;
    }

    /// Update entity and propagate cross-invalidation from the row before and
    /// after, which the UPDATE returns (through ListMixin when present, which
    /// invalidates the lists from the same rows). Uncertain: see
    /// crossUncertain, the input standing for the row until settled.
    static io::Task<std::optional<size_t>> update(const Key& id, const Entity& entity)
        requires MutableEntity<Entity> && HasFullUpdate<Entity> && (!Base::config.read_only)
    {
        std::optional<Entity> uncertain_before;
        typename Base::UpdateOutcome outcome;
        std::exception_ptr timeout;
        std::shared_ptr<io::detail::SettleTicket> ticket;
        try {
            outcome = co_await Base::template updateOutcome<true>(id, entity, &uncertain_before);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            timeout = std::current_exception();
        }
        if (timeout) {
            co_await crossUncertain(std::move(ticket), id, RowBefore::kept(std::move(uncertain_before)),
                                    entity, "update");
            std::rethrow_exception(timeout);
        }
        if (outcome.affected.value_or(0) > 0) {
            // Best-effort: a committed write never fails on cross-invalidation.
            try {
                co_await propagateUpdate<Entity, InvList>(
                    outcome.before ? &*outcome.before : nullptr,
                    outcome.after ? *outcome.after : entity);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (update committed) - " << e.what();
            }
        }
        co_return outcome.affected;
    }

    /// Erase entity and propagate cross-invalidation from the deleted row, which
    /// the DELETE returns (through ListMixin when present, which invalidates the
    /// lists from the same row). Uncertain: see crossUncertain.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Base::config.read_only)
    {
        std::optional<Entity> uncertain_before;
        typename Base::EraseOutcome outcome;
        std::exception_ptr timeout;
        std::shared_ptr<io::detail::SettleTicket> ticket;
        try {
            outcome = co_await Base::template eraseOutcome<true>(id, nullptr, &uncertain_before);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            timeout = std::current_exception();
        }
        if (timeout) {
            co_await crossUncertain(std::move(ticket), id, RowBefore::kept(std::move(uncertain_before)),
                                    std::nullopt, "erase");
            std::rethrow_exception(timeout);
        }
        if (outcome.before) {
            // Best-effort: a committed delete never fails on cross-invalidation.
            try {
                co_await propagateDelete<Entity, InvList>(*outcome.before);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": cross-invalidation failed (erase committed) - " << e.what();
            }
        }
        co_return outcome.affected;
    }

    /// Partial update with cross-invalidation (only if committed), keyed on the
    /// row before and after, both returned by the write itself. Uncertain: see
    /// crossUncertain (no estimate: the input is not a row).
    template<bool WithBefore, typename... Updates>
    static io::Task<typename Base::PatchedRow> patchRow(
        const Key& id, std::optional<Entity>* uncertain_before, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        static_assert(WithBefore, "this cascade reads the row before the write");
        std::optional<Entity> own_before;
        if (!uncertain_before) uncertain_before = &own_before;

        typename Base::PatchedRow row;
        std::exception_ptr timeout;
        std::shared_ptr<io::detail::SettleTicket> ticket;
        try {
            row = co_await Base::template patchRow<true>(
                id, uncertain_before, std::forward<Updates>(updates)...);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            timeout = std::current_exception();
        }
        if (timeout) {
            co_await crossUncertain(std::move(ticket), id, RowBefore::kept(std::move(*uncertain_before)),
                                    std::nullopt, "patch");
            std::rethrow_exception(timeout);
        }
        co_await propagatePatched(row, "patch");
        co_return row;
    }

    /// Guarded partial update with the same cross-invalidation as patchRow. A
    /// refused guard changed nothing: no target to invalidate.
    template<bool WithBefore, typename... Gs, typename... Updates>
    static io::Task<std::optional<typename Base::PatchedRow>> patchIfRow(
        const Key& id, std::optional<Entity>* uncertain_before,
        const entity::Guards<Gs...>& guard, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        static_assert(WithBefore, "this cascade reads the row before the write");
        std::optional<Entity> own_before;
        if (!uncertain_before) uncertain_before = &own_before;

        std::optional<typename Base::PatchedRow> row;
        std::exception_ptr timeout;
        std::shared_ptr<io::detail::SettleTicket> ticket;
        try {
            row = co_await Base::template patchIfRow<true>(
                id, uncertain_before, guard, std::forward<Updates>(updates)...);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            timeout = std::current_exception();
        }
        if (timeout) {
            co_await crossUncertain(std::move(ticket), id, RowBefore::kept(std::move(*uncertain_before)),
                                    std::nullopt, "patchIf");
            std::rethrow_exception(timeout);
        }
        if (row) co_await propagatePatched(*row, "patchIf");
        co_return row;
    }

    /// Invalidate the entity cache and propagate cross-invalidation.
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

    /// Every row (see PgRepo::invalidateEveryRow), and, as invalidate does,
    /// every target of them. A cycle of repos back to this one stops here.
    static void invalidateEveryRow(const std::shared_ptr<io::detail::SettleTicket>& ticket) {
        Base::invalidateEveryRow(ticket);
        static thread_local bool cascading = false;
        if (cascading) return;
        struct Reset { ~Reset() { cascading = false; } } reset;
        cascading = true;
        InvList::invalidateEvery(ticket);
    }

    /// Rows an uncertain predicate write may have changed (see
    /// PgRepo::rowsMayHaveChanged): the tiers below, then their targets.
    /// Nobody can name the rows, so a target is found only where the rows are
    /// located by what they hold (a list repo over them); every other target
    /// may be any.
    template<typename... Updates>
    static void rowsMayHaveChanged(const std::shared_ptr<io::detail::SettleTicket>& ticket,
                                   const detail::RowPredicate<Entity>& rows,
                                   const Updates&... updates) {
        Base::rowsMayHaveChanged(ticket, rows, updates...);
        everyTargetOf("predicate write", [&] {
            return InvList::template invalidateUnknownRows<Entity>(ticket, rows, updates...);
        });
    }

protected:
    /// Rows an uncertain write may have erased (see
    /// PgRepo::rowsMayHaveBeenErased): the tiers below, then the targets of
    /// each row from the version a tier kept, now and again once settled; for
    /// a row none kept, every target that cannot locate it by its key. An
    /// erased row has no new target.
    static io::Task<void> rowsMayHaveBeenErased(std::shared_ptr<io::detail::SettleTicket> ticket,
                                                std::span<const Key> keys,
                                                std::vector<std::optional<Entity>>* before) {
        std::vector<std::optional<Entity>> own(keys.size());
        if (!before) before = &own;
        co_await Base::rowsMayHaveBeenErased(ticket, keys, before);

        std::vector<Entity> kept;
        std::vector<Key> unknown;
        for (size_t i = 0; i < keys.size(); ++i) {
            if ((*before)[i]) kept.push_back(std::move(*(*before)[i]));
            else unknown.push_back(keys[i]);
        }
        if (!unknown.empty()) {
            auto rows = std::make_shared<const detail::KeySet<Key>>(std::span<const Key>(unknown));
            everyTargetOf("eraseMany", [&] {
                return InvList::template invalidateUnknownRows<Entity>(ticket,
                    [rows](const Entity& e) {
                        return rows->contains(Key(e.key())) ? detail::RowMatch::Maybe
                                                            : detail::RowMatch::No;
                    });
            });
        }
        if (!kept.empty()) {
            co_await detail::precaution(std::move(ticket),
                [kept = std::move(kept)]() -> io::Task<void> {
                    try {
                        co_await propagateDeleteMany<Entity, InvList>(std::span<const Entity>(kept));
                    } catch (const std::exception& e) {
                        logFailed("eraseMany", e);
                    }
                });
        }
    }

    /// Cross-invalidation of a committed patch (nothing if nothing was written).
    /// Best-effort: a committed patch never fails on cross-invalidation.
    static io::Task<void> propagatePatched(const typename Base::PatchedRow& row,
                                           const char* op) {
        if (!row.after) co_return;
        try {
            co_await propagateUpdate<Entity, InvList>(&*row.before, *row.after);
        } catch (const std::exception& e) {
            RELAIS_LOG_ERROR << name() << ": cross-invalidation failed (" << op
                << " committed) - " << e.what();
        }
    }

    /// The row before an uncertain write: a version of it, known absent, or
    /// unknown (no cache tier held it and no page did).
    struct RowBefore {
        std::optional<Entity> row;
        bool known = false;

        static RowBefore of(Entity e) { return {std::move(e), true}; }
        static RowBefore absent() { return {std::nullopt, true}; }
        /// The copy a cache tier kept before its precautionary evict.
        static RowBefore kept(std::optional<Entity> copy) {
            const bool known = copy.has_value();
            return {std::move(copy), known};
        }
    };

    /// Cross-targets of an uncertain write of the row `id` (none: a key the
    /// database assigns), which may commit until `ticket` settles:
    /// - the targets of the row before: from its version, now and again once
    ///   settled; unknown, every target that cannot locate it otherwise;
    /// - until settled, those of `estimate` (the input, when a whole row);
    /// - once settled, those of the row read again, as committed (trigger,
    ///   generated column). Without a key, every target.
    /// Best-effort: the uncertain exception must still surface.
    static io::Task<void> crossUncertain(std::shared_ptr<io::detail::SettleTicket> ticket,
                                         std::optional<Key> id, RowBefore before,
                                         std::optional<Entity> estimate, const char* op) {
        if (before.row) {
            co_await detail::precaution(ticket,
                [b = std::move(*before.row), op]() -> io::Task<void> {
                    try {
                        co_await propagateDelete<Entity, InvList>(b);
                    } catch (const std::exception& e) {
                        logFailed(op, e);
                    }
                });
        } else if (!before.known && id) {
            everyTargetOf(op, [&] {
                return InvList::template invalidateUnknownRow<Entity>(ticket, *id);
            });
        }
        if (estimate) {
            try {
                co_await propagateCreate<Entity, InvList>(*estimate);
            } catch (const std::exception& e) {
                logFailed(op, e);
            }
        }
        if (id) {
            co_await detail::onSettled(std::move(ticket),
                [id = std::move(*id), op] { return crossOnSettled(id, op); });
        } else {
            co_await detail::onSettled(std::move(ticket), [op]() -> io::Task<void> {
                everyTargetOf(op, [] { return InvList::invalidateEvery(nullptr); });
                co_return;
            });
        }
    }

    /// The targets of the row as committed, once an uncertain write of it is
    /// settled; if it cannot be read, every target.
    static io::Task<void> crossOnSettled(Key id, const char* op) {
        std::optional<Entity> row;
        try {
            row = co_await Base::rereadRaw(id);
        } catch (const std::exception& e) {
            RELAIS_LOG_WARN << name() << ": row unreadable after an uncertain " << op
                            << " - " << e.what();
            everyTargetOf(op, [] { return InvList::invalidateEvery(nullptr); });
            co_return;
        }
        if (!row) co_return;
        try {
            co_await propagateCreate<Entity, InvList>(*row);
        } catch (const std::exception& e) {
            logFailed(op, e);
        }
    }

    /// Runs `fallback` (true when a dependency invalidated every target) and
    /// logs it. Best-effort, like the rest of the uncertain path.
    template<typename Fallback>
    static void everyTargetOf(const char* op, Fallback&& fallback) {
        try {
            if (fallback())
                RELAIS_LOG_WARN << name() << ": uncertain " << op
                                << " of rows unknown, a cross-invalidation invalidated every target";
        } catch (const std::exception& e) {
            logFailed(op, e);
        }
    }

    static void logFailed(const char* op, const std::exception& e) {
        RELAIS_LOG_ERROR << name() << ": cross-invalidation failed (" << op << " timeout) - "
                         << e.what();
    }

    /// Best-effort pre-read for cross-invalidation context. The read path no
    /// longer swallows DB errors, so this pre-read can throw; a failure is
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
    /// delegate the entity tier + L1 list tracker straight down the chain.
    /// Awaited by the facade (see PgRepo for the split rationale).
    template<bool WithLists = true>
    static io::Task<void> invalidateManyCritical(std::span<const Entity> entities) {
        co_await Base::template invalidateManyCritical<WithLists>(entities);
    }

    /// Batch invalidation, deferred pass — deduplicated cross-target invalidation
    /// (union of distinct target keys invalidated once, materialized by value
    /// before any invalidation — the InvalidationData raw pointers never outlive
    /// this frame). The same for erase and invalidate: the targets depend on the
    /// row, not on whether it left the table. Fired fire-and-forget by the facade
    /// (invalidate-stale tolerated).
    static io::Task<void> invalidateManyDeferred(std::span<const Entity> entities) {
        co_await propagateDeleteMany<Entity, InvList>(entities);
    }

    /// Rows changed in place: no cross-target in the critical pass, as above.
    static io::Task<void> invalidateManyUpdatedCritical(std::span<const Change<Entity>> changes) {
        co_await Base::invalidateManyUpdatedCritical(changes);
    }

    /// Deduplicated cross-target invalidation of every row before and after the
    /// write.
    static io::Task<void> invalidateManyUpdatedDeferred(std::span<const Change<Entity>> changes) {
        co_await propagateUpdateMany<Entity, InvList>(changes);
    }

    friend struct ::relais_test::TestInternals;
};

}  // namespace detail

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INVALIDATION_MIXIN_H
