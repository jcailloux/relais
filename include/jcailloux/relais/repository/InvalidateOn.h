#ifndef JCX_RELAIS_REPOSITORY_INVALIDATEON_H
#define JCX_RELAIS_REPOSITORY_INVALIDATEON_H

#include <optional>
#include <utility>
#include "jcailloux/relais/io/Task.h"

namespace jcailloux::relais {

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

private:
    template<typename KeyT>
    static io::Task<void> resolveAndInvalidate(const KeyT& key) {
        auto resolved = co_await Resolver(key);
        using ResolvedType = std::decay_t<decltype(resolved)>;

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

// =============================================================================
// Concept to detect if a type has Invalidates defined
// =============================================================================

template<typename T>
concept HasInvalidates = requires {
    typename T::Invalidates;
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_REPOSITORY_INVALIDATEON_H
