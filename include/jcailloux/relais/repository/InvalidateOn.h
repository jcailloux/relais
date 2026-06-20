#ifndef JCX_RELAIS_REPOSITORY_INVALIDATEON_H
#define JCX_RELAIS_REPOSITORY_INVALIDATEON_H

#include <algorithm>
#include <optional>
#include <span>
#include <utility>
#include <vector>
#include "jcailloux/relais/io/Task.h"

namespace jcailloux::relais {

namespace detail {

/// Sort + unique a key list by value. The result equals set(input): a distinct
/// key is NEVER dropped (the dangerous failure mode — a stale survivor), and
/// duplicates collapse idempotently. Pure (no I/O): the unit-testable core of
/// batch cross-invalidation, where N source events fold to M ≤ N distinct
/// target keys invalidated once each.
template<typename K>
[[nodiscard]] std::vector<K> dedupSorted(std::vector<K> keys) {
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    return keys;
}

/// Awaited value type of an awaitable expression (Task<T>/Immediate<T> → T).
/// Lets a batch resolver deduce its result container without a real co_await.
template<typename T> struct awaited { using type = T; };
template<typename T> struct awaited<io::Task<T>> { using type = T; };
template<typename T> struct awaited<io::Immediate<T>> { using type = T; };
template<typename T> using awaited_t = typename awaited<std::decay_t<T>>::type;

}  // namespace detail

// =============================================================================
// Cross-invalidation — choose based on use case:
//
// 1. Invalidate<Target, &Source::fk>
//    Direct entity->entity. Source change -> invalidate target by FK.
//
// 2. InvalidateList<ListRepo>
//    Direct entity->list. Source change -> notify list cache.
//
// 3. InvalidateVia<Target, &Source::fk, &Resolver::resolve>
//    Indirect entity->entity via async resolver.
//
// 4. InvalidateListVia<ListRepo, &Source::fk, &Resolver::resolve>
//    Indirect entity->list with selective page invalidation.
// =============================================================================

// =============================================================================
// InvalidationData - Carries old/new entity values for cross-invalidation
// =============================================================================

/// Invalidation data for cross-repository notifications.
/// Raw pointers to data owned by the caller's coroutine frame (const E&
/// parameter or local optional<E>). Safe because the fold expression
/// `(co_await Dependencies::invalidateWithData(data), ...)` is sequential.
template<typename E>
struct InvalidationData {
    const E* old_entity = nullptr;  // null for insert
    const E* new_entity = nullptr;  // null for delete

    static InvalidationData forCreate(const E& e) {
        return {nullptr, &e};
    }

    static InvalidationData forUpdate(const E* old_e, const E& new_e) {
        return {old_e, &new_e};
    }

    static InvalidationData forDelete(const E& e) {
        return {&e, nullptr};
    }

    bool isCreate() const { return !old_entity && new_entity; }
    bool isUpdate() const { return old_entity && new_entity; }
    bool isDelete() const { return old_entity && !new_entity; }
};

// =============================================================================
// Invalidate - Single cache dependency (Table -> Table)
// =============================================================================

template<typename Cache, auto KeyExtractor>
struct Invalidate {
    using CacheType = Cache;

    template<typename E>
    static io::Task<void> invalidate(const E& entity) {
        if constexpr (requires { KeyExtractor(entity); }) {
            co_await Cache::invalidate(KeyExtractor(entity));
        } else if constexpr (requires { (entity.*KeyExtractor); }) {
            co_await Cache::invalidate(entity.*KeyExtractor);
        }
    }

    template<typename E>
    static io::Task<void> invalidateWithData(const InvalidationData<E>& data) {
        using KeyT = decltype(extractKey(std::declval<E>()));
        std::optional<KeyT> old_key;
        std::optional<KeyT> new_key;

        if (data.old_entity) {
            old_key = extractKey(*data.old_entity);
        }
        if (data.new_entity) {
            new_key = extractKey(*data.new_entity);
        }

        if (old_key) {
            co_await Cache::invalidate(*old_key);
        }

        if (new_key && (!old_key || *new_key != *old_key)) {
            co_await Cache::invalidate(*new_key);
        }
    }

    /// Pure: deduplicated target keys for a batch of deleted source entities.
    /// No I/O — the unit-testable core (N entities → M ≤ N distinct keys).
    template<typename E>
    static auto targetKeysForDelete(std::span<const E> entities) {
        using KeyT = decltype(extractKey(std::declval<E>()));
        std::vector<KeyT> keys;
        keys.reserve(entities.size());
        for (const auto& e : entities) keys.push_back(extractKey(e));
        return detail::dedupSorted(std::move(keys));
    }

    /// Batch delete cross-invalidation: one Cache::invalidate per *distinct*
    /// target key. A set of N source entities sharing M targets costs M
    /// invalidations, not N — the dedup win the per-entity fold can't get.
    template<typename E>
    static io::Task<void> invalidateManyForDelete(std::span<const E> entities) {
        for (const auto& k : targetKeysForDelete<E>(entities))
            co_await Cache::invalidate(k);
    }

private:
    template<typename E>
    static auto extractKey(const E& entity) {
        if constexpr (requires { KeyExtractor(entity); }) {
            return KeyExtractor(entity);
        } else if constexpr (requires { (entity.*KeyExtractor); }) {
            return entity.*KeyExtractor;
        }
    }
};

// =============================================================================
// InvalidateList - For list cache invalidation with entity context
// =============================================================================

template<typename ListCache>
struct InvalidateList {
    using CacheType = ListCache;

    template<typename E>
    static io::Task<void> invalidate(const E& entity) {
        if constexpr (requires { ListCache::onEntityModified(entity); }) {
            co_await ListCache::onEntityModified(entity);
        } else if constexpr (requires { ListCache::onEntityCreated(entity); }) {
            co_await ListCache::onEntityCreated(entity);
        } else if constexpr (requires { ListCache::notifyCreated(entity); }) {
            ListCache::notifyCreated(entity);
            co_return;
        }
    }

    template<typename E>
    static io::Task<void> invalidateWithData(const InvalidationData<E>& data) {
        if constexpr (requires { ListCache::onEntityModified(data); }) {
            co_await ListCache::onEntityModified(data);
        }
        else if constexpr (requires { ListCache::onEntityUpdated(
            std::declval<const E&>(),
            std::declval<const E&>()); }) {

            if (data.isCreate() && data.new_entity) {
                if constexpr (requires { ListCache::onEntityCreated(std::declval<const E&>()); }) {
                    co_await ListCache::onEntityCreated(*data.new_entity);
                }
            } else if (data.isDelete() && data.old_entity) {
                if constexpr (requires { ListCache::onEntityDeleted(std::declval<const E&>()); }) {
                    co_await ListCache::onEntityDeleted(*data.old_entity);
                }
            } else if (data.isUpdate()) {
                co_await ListCache::onEntityUpdated(*data.old_entity, *data.new_entity);
            }
        }
        else if constexpr (requires { ListCache::notifyUpdated(
            std::declval<const E&>(),
            std::declval<const E&>()); }) {

            if (data.isCreate() && data.new_entity) {
                ListCache::notifyCreated(*data.new_entity);
            } else if (data.isDelete() && data.old_entity) {
                ListCache::notifyDeleted(*data.old_entity);
            } else if (data.isUpdate()) {
                ListCache::notifyUpdated(*data.old_entity, *data.new_entity);
            }
            co_return;
        }
        else if constexpr (requires { ListCache::onEntityModified(
            std::declval<const E&>()); }) {

            if (data.new_entity) {
                co_await ListCache::onEntityModified(*data.new_entity);
            }
            else if (data.old_entity) {
                co_await ListCache::onEntityModified(*data.old_entity);
            }
        }
    }

    /// Batch delete: per-entity loop. A list cache has no scalar key to
    /// deduplicate (the predicate/blob match is the unit of work); the foreign
    /// list cache batches its own tracker internally. Correct, not yet
    /// single-bump-collapsed.
    template<typename E>
    static io::Task<void> invalidateManyForDelete(std::span<const E> entities) {
        for (const auto& e : entities) {
            auto data = InvalidationData<E>::forDelete(e);
            co_await invalidateWithData(data);
        }
    }
};

// =============================================================================
// InvalidateVia - Indirect cache invalidation through an async resolver
// =============================================================================

template<typename TargetCache, auto SourceKeyExtractor, auto Resolver>
struct InvalidateVia {
    using CacheType = TargetCache;

    template<typename E>
    static io::Task<void> invalidate(const E& entity) {
        auto target_keys = co_await Resolver(extractKey(entity));
        for (const auto& tk : target_keys)
            co_await TargetCache::invalidate(tk);
    }

    template<typename E>
    static io::Task<void> invalidateWithData(const InvalidationData<E>& data) {
        using KeyT = decltype(extractKey(std::declval<E>()));
        std::optional<KeyT> old_key, new_key;

        if (data.old_entity)
            old_key = extractKey(*data.old_entity);
        if (data.new_entity)
            new_key = extractKey(*data.new_entity);

        if (old_key) {
            auto targets = co_await Resolver(*old_key);
            for (const auto& tk : targets)
                co_await TargetCache::invalidate(tk);
        }

        if (new_key && (!old_key || *new_key != *old_key)) {
            auto targets = co_await Resolver(*new_key);
            for (const auto& tk : targets)
                co_await TargetCache::invalidate(tk);
        }
    }

    /// Batch delete: dedup source keys, resolve, dedup target keys, invalidate
    /// each target once. The resolver runs once per *distinct source* by
    /// default; if it exposes a span overload (`Resolver(span<const KeyT>)`)
    /// the N source lookups collapse into a single round-trip (opt-in batch).
    template<typename E>
    static io::Task<void> invalidateManyForDelete(std::span<const E> entities) {
        using KeyT = decltype(extractKey(std::declval<E>()));
        std::vector<KeyT> sources;
        sources.reserve(entities.size());
        for (const auto& e : entities) sources.push_back(extractKey(e));
        sources = detail::dedupSorted(std::move(sources));

        if constexpr (requires { Resolver(std::span<const KeyT>(sources)); }) {
            // Opt-in batch resolver: one call collapses N source lookups.
            auto targets = co_await Resolver(std::span<const KeyT>(sources));
            for (const auto& tk : detail::dedupSorted(std::move(targets)))
                co_await TargetCache::invalidate(tk);
        } else {
            using TargetVec = detail::awaited_t<decltype(Resolver(std::declval<KeyT>()))>;
            TargetVec targets;
            for (const auto& s : sources) {
                auto resolved = co_await Resolver(s);
                for (auto& t : resolved) targets.push_back(std::move(t));
            }
            for (const auto& tk : detail::dedupSorted(std::move(targets)))
                co_await TargetCache::invalidate(tk);
        }
    }

private:
    template<typename E>
    static auto extractKey(const E& entity) {
        if constexpr (requires { SourceKeyExtractor(entity); })
            return SourceKeyExtractor(entity);
        else if constexpr (requires { (entity.*SourceKeyExtractor); })
            return entity.*SourceKeyExtractor;
    }
};

// =============================================================================
// InvalidateListVia - Selective list cache invalidation through enriched resolver
// =============================================================================

namespace detail {
    template<typename T> struct is_optional : std::false_type {};
    template<typename T> struct is_optional<std::optional<T>> : std::true_type {};
}

/// Typed invalidation target for list cache cross-invalidation.
template<typename GroupKey>
struct ListInvalidationTarget {
    GroupKey filters;
    std::optional<int64_t> sort_value;
};

template<typename ListRepo, auto SourceKeyExtractor, auto Resolver>
struct InvalidateListVia {
    using GroupKey = typename ListRepo::GroupKey;
    using Target = ListInvalidationTarget<GroupKey>;

    template<typename E>
    static io::Task<void> invalidate(const E& entity) {
        co_await resolveAndInvalidate(extractKey(entity));
    }

    template<typename E>
    static io::Task<void> invalidateWithData(const InvalidationData<E>& data) {
        using KeyT = decltype(extractKey(std::declval<E>()));
        std::optional<KeyT> old_key, new_key;

        if (data.old_entity)
            old_key = extractKey(*data.old_entity);
        if (data.new_entity)
            new_key = extractKey(*data.new_entity);

        if (old_key)
            co_await resolveAndInvalidate(*old_key);

        if (new_key && (!old_key || *new_key != *old_key))
            co_await resolveAndInvalidate(*new_key);
    }

    /// Batch delete: dedup source keys (collapses resolver calls for entities
    /// sharing a source), then resolve+invalidate. The resolver runs once per
    /// distinct source by default; a `Resolver(span<const KeyT>)` overload
    /// collapses them into a single round-trip (opt-in batch).
    template<typename E>
    static io::Task<void> invalidateManyForDelete(std::span<const E> entities) {
        using KeyT = decltype(extractKey(std::declval<E>()));
        std::vector<KeyT> sources;
        sources.reserve(entities.size());
        for (const auto& e : entities) sources.push_back(extractKey(e));
        sources = detail::dedupSorted(std::move(sources));

        if constexpr (requires { Resolver(std::span<const KeyT>(sources)); }) {
            co_await invalidateResolved(co_await Resolver(std::span<const KeyT>(sources)));
        } else {
            for (const auto& s : sources)
                co_await resolveAndInvalidate(s);
        }
    }

private:
    template<typename Resolved>
    static io::Task<void> invalidateResolved(Resolved resolved) {
        using ResolvedType = std::decay_t<Resolved>;

        if constexpr (detail::is_optional<ResolvedType>::value) {
            if (!resolved) {
                co_await ListRepo::invalidateAllListGroups();
                co_return;
            }
            for (const auto& target : *resolved)
                co_await ListRepo::invalidateByTarget(target.filters, target.sort_value);
        } else {
            for (const auto& target : resolved)
                co_await ListRepo::invalidateByTarget(target.filters, target.sort_value);
        }
    }

    template<typename KeyT>
    static io::Task<void> resolveAndInvalidate(const KeyT& key) {
        co_await invalidateResolved(co_await Resolver(key));
    }

    template<typename E>
    static auto extractKey(const E& entity) {
        if constexpr (requires { SourceKeyExtractor(entity); })
            return SourceKeyExtractor(entity);
        else if constexpr (requires { (entity.*SourceKeyExtractor); })
            return entity.*SourceKeyExtractor;
    }
};

// =============================================================================
// InvalidateOn - Aggregates multiple Invalidate<> dependencies
// =============================================================================

template<typename... Dependencies>
struct InvalidateOn {
    template<typename E>
    static io::Task<void> propagate(const E& entity) {
        (co_await Dependencies::template invalidate<E>(entity), ...);
    }

    template<typename E>
    static io::Task<void> propagateWithData(const InvalidationData<E>& data) {
        (co_await Dependencies::template invalidateWithData<E>(data), ...);
    }

    /// Batch delete propagation: each dependency folds its N source events into
    /// a deduplicated invalidation (cf. per-variant invalidateManyForDelete).
    template<typename E>
    static io::Task<void> propagateDeleteMany(std::span<const E> entities) {
        (co_await Dependencies::template invalidateManyForDelete<E>(entities), ...);
    }
};

template<>
struct InvalidateOn<> {
    template<typename E>
    static io::Task<void> propagate(const E&) {
        co_return;
    }

    template<typename E>
    static io::Task<void> propagateWithData(const InvalidationData<E>&) {
        co_return;
    }

    template<typename E>
    static io::Task<void> propagateDeleteMany(std::span<const E>) {
        co_return;
    }
};

// =============================================================================
// propagateInvalidations - Helper functions for use in repositories
// =============================================================================

template<typename E, typename InvalidatesType>
io::Task<void> propagateInvalidationsWithData(const InvalidationData<E>& data) {
    co_await InvalidatesType::template propagateWithData<E>(data);
}

template<typename E, typename InvalidatesType>
io::Task<void> propagateCreate(const E& entity) {
    auto data = InvalidationData<E>::forCreate(entity);
    co_await propagateInvalidationsWithData<E, InvalidatesType>(data);
}

template<typename E, typename InvalidatesType>
io::Task<void> propagateUpdate(const E* old_entity, const E& new_entity) {
    auto data = InvalidationData<E>::forUpdate(old_entity, new_entity);
    co_await propagateInvalidationsWithData<E, InvalidatesType>(data);
}

template<typename E, typename InvalidatesType>
io::Task<void> propagateDelete(const E& entity) {
    auto data = InvalidationData<E>::forDelete(entity);
    co_await propagateInvalidationsWithData<E, InvalidatesType>(data);
}

template<typename E, typename InvalidatesType>
io::Task<void> propagateDeleteMany(std::span<const E> entities) {
    co_await InvalidatesType::template propagateDeleteMany<E>(entities);
}

// =============================================================================
// Concept to detect if a type has Invalidates defined
// =============================================================================

template<typename T>
concept HasInvalidates = requires {
    typename T::Invalidates;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_REPOSITORY_INVALIDATEON_H
