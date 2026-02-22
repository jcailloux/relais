#ifndef JCX_RELAIS_INVALIDATEON_H
#define JCX_RELAIS_INVALIDATEON_H

#include <optional>
#include <utility>
#include "jcailloux/relais/io/Task.h"

namespace jcailloux::relais::cache {

// =============================================================================
// InvalidationData - Carries old/new entity values for cross-invalidation
// =============================================================================

/// Invalidation data for cross-repository notifications.
/// Raw pointers to data owned by the caller's coroutine frame (const Entity&
/// parameter or local optional<Entity>). Safe because the fold expression
/// `(co_await Dependencies::invalidateWithData(data), ...)` is sequential.
template<typename Entity>
struct InvalidationData {
    const Entity* old_entity = nullptr;  // null for insert
    const Entity* new_entity = nullptr;  // null for delete

    static InvalidationData forCreate(const Entity& e) {
        return {nullptr, &e};
    }

    static InvalidationData forUpdate(const Entity* old_e, const Entity& new_e) {
        return {old_e, &new_e};
    }

    static InvalidationData forDelete(const Entity& e) {
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

    template<typename Entity>
    static io::Task<void> invalidate(const Entity& entity) {
        if constexpr (requires { KeyExtractor(entity); }) {
            co_await Cache::invalidate(KeyExtractor(entity));
        } else if constexpr (requires { (entity.*KeyExtractor); }) {
            co_await Cache::invalidate(entity.*KeyExtractor);
        }
    }

    template<typename Entity>
    static io::Task<void> invalidateWithData(const InvalidationData<Entity>& data) {
        using KeyT = decltype(extractKey(std::declval<Entity>()));
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
    template<typename Entity>
    static auto extractKey(const Entity& entity) {
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

    template<typename Entity>
    static io::Task<void> invalidate(const Entity& entity) {
        if constexpr (requires { ListCache::onEntityModified(entity); }) {
            co_await ListCache::onEntityModified(entity);
        } else if constexpr (requires { ListCache::onEntityCreated(entity); }) {
            co_await ListCache::onEntityCreated(entity);
        } else if constexpr (requires { ListCache::notifyCreated(entity); }) {
            ListCache::notifyCreated(entity);
            co_return;
        }
    }

    template<typename Entity>
    static io::Task<void> invalidateWithData(const InvalidationData<Entity>& data) {
        if constexpr (requires { ListCache::onEntityModified(data); }) {
            co_await ListCache::onEntityModified(data);
        }
        else if constexpr (requires { ListCache::onEntityUpdated(
            std::declval<const Entity&>(),
            std::declval<const Entity&>()); }) {

            if (data.isCreate() && data.new_entity) {
                if constexpr (requires { ListCache::onEntityCreated(std::declval<const Entity&>()); }) {
                    co_await ListCache::onEntityCreated(*data.new_entity);
                }
            } else if (data.isDelete() && data.old_entity) {
                if constexpr (requires { ListCache::onEntityDeleted(std::declval<const Entity&>()); }) {
                    co_await ListCache::onEntityDeleted(*data.old_entity);
                }
            } else if (data.isUpdate()) {
                co_await ListCache::onEntityUpdated(*data.old_entity, *data.new_entity);
            }
        }
        else if constexpr (requires { ListCache::notifyUpdated(
            std::declval<const Entity&>(),
            std::declval<const Entity&>()); }) {

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
            std::declval<const Entity&>()); }) {

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

    template<typename Entity>
    static io::Task<void> invalidate(const Entity& entity) {
        auto target_keys = co_await Resolver(extractKey(entity));
        for (const auto& tk : target_keys)
            co_await TargetCache::invalidate(tk);
    }

    template<typename Entity>
    static io::Task<void> invalidateWithData(const InvalidationData<Entity>& data) {
        using KeyT = decltype(extractKey(std::declval<Entity>()));
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
    template<typename Entity>
    static auto extractKey(const Entity& entity) {
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

    template<typename Entity>
    static io::Task<void> invalidate(const Entity& entity) {
        co_await resolveAndInvalidate(extractKey(entity));
    }

    template<typename Entity>
    static io::Task<void> invalidateWithData(const InvalidationData<Entity>& data) {
        using KeyT = decltype(extractKey(std::declval<Entity>()));
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

    template<typename Entity>
    static auto extractKey(const Entity& entity) {
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
    template<typename Entity>
    static io::Task<void> propagate(const Entity& entity) {
        (co_await Dependencies::template invalidate(entity), ...);
    }

    template<typename Entity>
    static io::Task<void> propagateWithData(const InvalidationData<Entity>& data) {
        (co_await Dependencies::template invalidateWithData(data), ...);
    }
};

template<>
struct InvalidateOn<> {
    template<typename Entity>
    static io::Task<void> propagate(const Entity&) {
        co_return;
    }

    template<typename Entity>
    static io::Task<void> propagateWithData(const InvalidationData<Entity>&) {
        co_return;
    }
};

// =============================================================================
// propagateInvalidations - Helper functions for use in repositories
// =============================================================================

template<typename Entity, typename InvalidatesType>
io::Task<void> propagateInvalidationsWithData(const InvalidationData<Entity>& data) {
    co_await InvalidatesType::template propagateWithData(data);
}

template<typename Entity, typename InvalidatesType>
io::Task<void> propagateCreate(const Entity& entity) {
    auto data = InvalidationData<Entity>::forCreate(entity);
    co_await propagateInvalidationsWithData<Entity, InvalidatesType>(data);
}

template<typename Entity, typename InvalidatesType>
io::Task<void> propagateUpdate(const Entity* old_entity, const Entity& new_entity) {
    auto data = InvalidationData<Entity>::forUpdate(old_entity, new_entity);
    co_await propagateInvalidationsWithData<Entity, InvalidatesType>(data);
}

template<typename Entity, typename InvalidatesType>
io::Task<void> propagateDelete(const Entity& entity) {
    auto data = InvalidationData<Entity>::forDelete(entity);
    co_await propagateInvalidationsWithData<Entity, InvalidatesType>(data);
}

// =============================================================================
// Concept to detect if a type has Invalidates defined
// =============================================================================

template<typename T>
concept HasInvalidates = requires {
    typename T::Invalidates;
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_INVALIDATEON_H
