#ifndef JCX_DROGON_INVALIDATEON_H
#define JCX_DROGON_INVALIDATEON_H

#include <memory>
#include <optional>
#include <utility>
#include <drogon/utils/coroutine.h>

namespace jcailloux::drogon::cache {

// =============================================================================
// InvalidationData - Carries old/new entity values for cross-invalidation
// =============================================================================
//
// When a table repository performs create/update/delete, it may need to notify
// other repositories (table or list) about the change.
//
// Simple invalidation (not expiration) notifies with ONE value only.
// Table -> Table: notify with the field associated with invalidation
// Table -> List: notify with complete entities (all fields can be used)
//

/// Wrapper pointer type
template<typename Entity>
using WrapperPtr = std::shared_ptr<const Entity>;

/// Invalidation data for cross-repository notifications
template<typename Entity>
struct InvalidationData {
    std::optional<WrapperPtr<Entity>> old_entity;  // Before mutation (null for create)
    std::optional<WrapperPtr<Entity>> new_entity;  // After mutation (null for delete)

    /// Create invalidation data for a CREATE operation
    static InvalidationData forCreate(WrapperPtr<Entity> entity) {
        InvalidationData data;
        data.new_entity = std::move(entity);
        return data;
    }

    /// Create invalidation data for an UPDATE operation
    static InvalidationData forUpdate(WrapperPtr<Entity> old_e, WrapperPtr<Entity> new_e) {
        InvalidationData data;
        data.old_entity = std::move(old_e);
        data.new_entity = std::move(new_e);
        return data;
    }

    /// Create invalidation data for a DELETE operation
    static InvalidationData forDelete(WrapperPtr<Entity> entity) {
        InvalidationData data;
        data.old_entity = std::move(entity);
        return data;
    }

    /// Check if this is a create operation
    bool isCreate() const { return !old_entity.has_value() && new_entity.has_value(); }

    /// Check if this is an update operation
    bool isUpdate() const { return old_entity.has_value() && new_entity.has_value(); }

    /// Check if this is a delete operation
    bool isDelete() const { return old_entity.has_value() && !new_entity.has_value(); }
};

// =============================================================================
// InvalidateOn - Declarative cache dependency specification
// =============================================================================
//
// Usage in a repository:
//
// Method 1: Using Invalidate<Cache, KeyMapper> helpers
//   struct PurchaseInvalidations {
//       static int64_t statsKey(const Purchase& p) { return p.user_id; }
//       static int64_t walletKey(const Purchase& p) { return p.user_id; }
//   };
//   using Invalidates = InvalidateOn<
//       Invalidate<StatsCache, &PurchaseInvalidations::statsKey>,
//       Invalidate<WalletCache, &PurchaseInvalidations::walletKey>
//   >;
//
// Method 2: Direct member pointer (when key matches a field)
//   using Invalidates = InvalidateOn<
//       Invalidate<StatsCache, &Purchase::user_id>,
//       Invalidate<WalletCache, &Purchase::user_id>
//   >;
//
// Method 3: Lambda-based (C++20 stateless lambdas as NTTP)
//   inline constexpr auto extractUserId = [](const auto& p) { return p.user_id; };
//   using Invalidates = InvalidateOn<
//       Invalidate<StatsCache, extractUserId>,
//       Invalidate<WalletCache, extractUserId>
//   >;
//

// =============================================================================
// Invalidate - Single cache dependency (Table -> Table)
// =============================================================================
//
// For table-to-table invalidation, only the field associated with invalidation
// is transmitted. The target cache receives a simple key for invalidation.
//

template<typename Cache, auto KeyExtractor>
struct Invalidate {
    using CacheType = Cache;

    /// Simple invalidation with a single entity (backward compatible)
    template<typename Entity>
    static ::drogon::Task<void> invalidate(const Entity& entity) {
        if constexpr (requires { KeyExtractor(entity); }) {
            // Free function or lambda
            co_await Cache::invalidate(KeyExtractor(entity));
        } else if constexpr (requires { (entity.*KeyExtractor); }) {
            // Member pointer
            co_await Cache::invalidate(entity.*KeyExtractor);
        }
    }

    /// Enhanced invalidation with old/new entity data
    template<typename Entity>
    static ::drogon::Task<void> invalidateWithData(const InvalidationData<Entity>& data) {
        // For table-to-table, we invalidate based on the extracted key
        // from both old and new entities (if they differ)

        std::optional<decltype(extractKey(std::declval<Entity>()))> old_key;
        std::optional<decltype(extractKey(std::declval<Entity>()))> new_key;

        if (data.old_entity && *data.old_entity) {
            old_key = extractKey(**data.old_entity);
        }
        if (data.new_entity && *data.new_entity) {
            new_key = extractKey(**data.new_entity);
        }

        // Invalidate old key (for updates where key changed, or deletes)
        if (old_key) {
            co_await Cache::invalidate(*old_key);
        }

        // Invalidate new key only if it differs from old (for updates/creates)
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
//
// Unlike Invalidate<>, this passes the full entity (as wrapper pointer or JSON)
// to the target cache's onEntityModified() method, enabling filter/sort range checks.
//
// For Table -> List invalidation, complete entities are transmitted so all fields
// can be used for segment invalidation decisions.
//
// Usage:
//   using Invalidates = InvalidateOn<
//       InvalidateList<UserListCache>  // No key extractor needed
//   >;
//
// The target cache must implement one of:
//   static Task<void> onEntityModified(shared_ptr<const Entity> entity);
//   static Task<void> onEntityModified(InvalidationData<Entity> data);
//

template<typename ListCache>
struct InvalidateList {
    using CacheType = ListCache;

    /// Simple invalidation with a single entity (backward compatible)
    template<typename Entity>
    static ::drogon::Task<void> invalidate(const Entity& entity) {
        auto entity_ptr = std::make_shared<const Entity>(entity);
        if constexpr (requires { ListCache::onEntityModified(entity_ptr); }) {
            co_await ListCache::onEntityModified(std::move(entity_ptr));
        } else if constexpr (requires { ListCache::onEntityCreated(entity_ptr); }) {
            co_await ListCache::onEntityCreated(std::move(entity_ptr));
        }
    }

    /// Enhanced invalidation with old/new entity data and optional JSON
    template<typename Entity>
    static ::drogon::Task<void> invalidateWithData(const InvalidationData<Entity>& data) {
        // Prefer the full data interface if available
        if constexpr (requires { ListCache::onEntityModified(data); }) {
            co_await ListCache::onEntityModified(data);
        }
        // Fall back to old/new pointer interface
        else if constexpr (requires { ListCache::onEntityUpdated(
            std::declval<WrapperPtr<Entity>>(),
            std::declval<WrapperPtr<Entity>>()); }) {

            WrapperPtr<Entity> old_ptr = data.old_entity.value_or(nullptr);
            WrapperPtr<Entity> new_ptr = data.new_entity.value_or(nullptr);

            if (data.isCreate() && new_ptr) {
                if constexpr (requires { ListCache::onEntityCreated(new_ptr); }) {
                    co_await ListCache::onEntityCreated(std::move(new_ptr));
                } else {
                    co_await ListCache::onEntityUpdated(nullptr, std::move(new_ptr));
                }
            } else if (data.isDelete() && old_ptr) {
                if constexpr (requires { ListCache::onEntityDeleted(old_ptr); }) {
                    co_await ListCache::onEntityDeleted(std::move(old_ptr));
                } else {
                    co_await ListCache::onEntityUpdated(std::move(old_ptr), nullptr);
                }
            } else if (data.isUpdate()) {
                co_await ListCache::onEntityUpdated(std::move(old_ptr), std::move(new_ptr));
            }
        }
        // Fall back to simple interface with new entity only
        else if constexpr (requires { ListCache::onEntityModified(
            std::declval<WrapperPtr<Entity>>()); }) {

            // For create/update, notify with new entity
            if (data.new_entity && *data.new_entity) {
                co_await ListCache::onEntityModified(*data.new_entity);
            }
            // For delete, notify with old entity
            else if (data.old_entity && *data.old_entity) {
                co_await ListCache::onEntityModified(*data.old_entity);
            }
        }
    }
};

// =============================================================================
// InvalidateVia - Indirect cache invalidation through an async resolver
// =============================================================================
//
// For indirect relationships where the source entity doesn't contain the
// target cache's key directly. A resolver function bridges the gap by
// querying intermediate data (e.g., a junction table).
//
// Template parameters:
//   TargetCache        - The cache to invalidate (Repository or VirtualCache)
//   SourceKeyExtractor - Extracts the lookup key from the source entity
//   Resolver           - Async function: source_key -> Task<iterable<target_key>>
//
// Usage:
//   struct CtoAResolver {
//       static ::drogon::Task<std::vector<int64_t>> resolve(int64_t c_id) {
//           // Query junction table B to find a_ids for the given c_id
//       }
//   };
//   using Invalidates = InvalidateOn<
//       InvalidateVia<RepoCible, &CEntity::c_id, &CtoAResolver::resolve>
//   >;
//

template<typename TargetCache, auto SourceKeyExtractor, auto Resolver>
struct InvalidateVia {
    using CacheType = TargetCache;

    /// Simple invalidation with a single entity
    template<typename Entity>
    static ::drogon::Task<void> invalidate(const Entity& entity) {
        auto target_keys = co_await Resolver(extractKey(entity));
        for (const auto& tk : target_keys)
            co_await TargetCache::invalidate(tk);
    }

    /// Enhanced invalidation with old/new entity data
    template<typename Entity>
    static ::drogon::Task<void> invalidateWithData(const InvalidationData<Entity>& data) {
        using KeyT = decltype(extractKey(std::declval<Entity>()));
        std::optional<KeyT> old_key, new_key;

        if (data.old_entity && *data.old_entity)
            old_key = extractKey(**data.old_entity);
        if (data.new_entity && *data.new_entity)
            new_key = extractKey(**data.new_entity);

        // Resolve and invalidate for old key
        if (old_key) {
            auto targets = co_await Resolver(*old_key);
            for (const auto& tk : targets)
                co_await TargetCache::invalidate(tk);
        }

        // Resolve and invalidate for new key only if different
        if (new_key && (!old_key || *new_key != *old_key)) {
            auto targets = co_await Resolver(*new_key);
            for (const auto& tk : targets)
                co_await TargetCache::invalidate(tk);
        }
    }

private:
    /// Extract key from entity (same pattern as Invalidate<>)
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
//
// For indirect relationships where a source entity change should selectively
// invalidate paginated list cache pages. The resolver queries the database
// to find affected entities with their filter values and sort values.
//
// Unlike InvalidateVia (which calls TargetCache::invalidate with a simple key),
// this dispatches based on the resolver's return to one of three granularities:
//   - Per-page:    target has sort_value → selective Lua invalidation
//   - Per-group:   target without sort_value → full group invalidation
//   - Full pattern: resolver returns nullopt → invalidate all list groups
//
// Template parameters:
//   ListRepo           - The list repository. Must provide:
//                          - GroupKey type alias (struct with filter fields)
//                          - invalidateByTarget(GroupKey, optional<int64_t>) method
//                          - invalidateAllListGroups() method (from CRTP base)
//   SourceKeyExtractor - Extracts the lookup key from the source entity
//   Resolver           - Async function: source_key -> Task<vector<Target>>
//                         or Task<optional<vector<Target>>> (nullopt = full pattern)
//
// Usage:
//   struct MyResolver {
//       using GroupKey = ArticleListRepo::GroupKey;
//       using Target = ListInvalidationTarget<GroupKey>;
//       static Task<vector<Target>> resolve(int64_t user_id) {
//           Target t;
//           t.filters.category = "tech";
//           t.sort_value = view_count;   // per-page (omit for per-group)
//       }
//   };
//   using Invalidates = InvalidateOn<
//       InvalidateListVia<ArticleListRepo, &Purchase::user_id, &MyResolver::resolve>
//   >;
//

namespace detail {
    template<typename T> struct is_optional : std::false_type {};
    template<typename T> struct is_optional<std::optional<T>> : std::true_type {};
}

/// Typed invalidation target for list cache cross-invalidation.
/// GroupKey is defined by the target ListRepo (e.g., struct { string category; }).
/// The sort_value controls granularity: present = per-page, absent = per-group.
template<typename GroupKey>
struct ListInvalidationTarget {
    GroupKey filters;                      // Typed filter values identifying the group
    std::optional<int64_t> sort_value;    // Present = per-page, absent = per-group
};

template<typename ListRepo, auto SourceKeyExtractor, auto Resolver>
struct InvalidateListVia {
    using GroupKey = typename ListRepo::GroupKey;
    using Target = ListInvalidationTarget<GroupKey>;

    /// Simple invalidation with a single entity
    template<typename Entity>
    static ::drogon::Task<void> invalidate(const Entity& entity) {
        co_await resolveAndInvalidate(extractKey(entity));
    }

    /// Enhanced invalidation with old/new entity data
    template<typename Entity>
    static ::drogon::Task<void> invalidateWithData(const InvalidationData<Entity>& data) {
        using KeyT = decltype(extractKey(std::declval<Entity>()));
        std::optional<KeyT> old_key, new_key;

        if (data.old_entity && *data.old_entity)
            old_key = extractKey(**data.old_entity);
        if (data.new_entity && *data.new_entity)
            new_key = extractKey(**data.new_entity);

        if (old_key)
            co_await resolveAndInvalidate(*old_key);

        if (new_key && (!old_key || *new_key != *old_key))
            co_await resolveAndInvalidate(*new_key);
    }

private:
    template<typename KeyT>
    static ::drogon::Task<void> resolveAndInvalidate(const KeyT& key) {
        auto resolved = co_await Resolver(key);
        using ResolvedType = std::decay_t<decltype(resolved)>;

        if constexpr (detail::is_optional<ResolvedType>::value) {
            if (!resolved) {
                // nullopt → full pattern invalidation (all list groups)
                co_await ListRepo::invalidateAllListGroups();
                co_return;
            }
            for (const auto& target : *resolved)
                co_await ListRepo::invalidateByTarget(target.filters, target.sort_value);
        } else {
            // Direct vector return (backward compatible path)
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
    /// Simple propagation (backward compatible)
    template<typename Entity>
    static ::drogon::Task<void> propagate(const Entity& entity) {
        (co_await Dependencies::template invalidate(entity), ...);
    }

    /// Enhanced propagation with old/new entity data
    template<typename Entity>
    static ::drogon::Task<void> propagateWithData(const InvalidationData<Entity>& data) {
        (co_await Dependencies::template invalidateWithData(data), ...);
    }
};

// Empty specialization
template<>
struct InvalidateOn<> {
    template<typename Entity>
    static ::drogon::Task<void> propagate(const Entity&) {
        co_return;
    }

    template<typename Entity>
    static ::drogon::Task<void> propagateWithData(const InvalidationData<Entity>&) {
        co_return;
    }
};

// =============================================================================
// propagateInvalidations - Helper functions for use in repositories
// =============================================================================

/// Propagation with InvalidationData
template<typename Entity, typename InvalidatesType>
::drogon::Task<void> propagateInvalidationsWithData(const InvalidationData<Entity>& data) {
    co_await InvalidatesType::template propagateWithData(data);
}

/// Helper for create operations
template<typename Entity, typename InvalidatesType>
::drogon::Task<void> propagateCreate(WrapperPtr<Entity> entity) {
    auto data = InvalidationData<Entity>::forCreate(std::move(entity));
    co_await propagateInvalidationsWithData<Entity, InvalidatesType>(data);
}

/// Helper for update operations
template<typename Entity, typename InvalidatesType>
::drogon::Task<void> propagateUpdate(WrapperPtr<Entity> old_entity, WrapperPtr<Entity> new_entity) {
    auto data = InvalidationData<Entity>::forUpdate(std::move(old_entity), std::move(new_entity));
    co_await propagateInvalidationsWithData<Entity, InvalidatesType>(data);
}

/// Helper for delete operations
template<typename Entity, typename InvalidatesType>
::drogon::Task<void> propagateDelete(WrapperPtr<Entity> entity) {
    auto data = InvalidationData<Entity>::forDelete(std::move(entity));
    co_await propagateInvalidationsWithData<Entity, InvalidatesType>(data);
}

// =============================================================================
// Concept to detect if a type has Invalidates defined
// =============================================================================

template<typename T>
concept HasInvalidates = requires {
    typename T::Invalidates;
};

}  // namespace jcailloux::drogon::cache

#endif  // JCX_DROGON_INVALIDATEON_H
