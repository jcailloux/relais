#ifndef CODIBOT_ListCacheRepo_H
#define CODIBOT_ListCacheRepo_H

#include <memory>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/Log.h"
#include "ListCache.h"
#include "ListCacheTraits.h"

namespace jcailloux::relais::cache::list {

// =============================================================================
// ListCacheRepo - Mixin that adds ListCache support to repositories
// =============================================================================
//
// Usage:
//   class MyRepo
//       : public config::repo::Repo<...>
//       , public cache::list::ListCacheRepo<MyRepo, MyEntity, ...>
//   {
//       using ListMixin = cache::list::ListCacheRepo<...>;
//   public:
//       static io::Task<ListResult> findItems(ListQuery query) {
//           co_return co_await cachedListQuery(std::move(query), [&]() {
//               return queryFromDb(query);
//           });
//       }
//   };
//

template<typename Derived, typename Entity, typename Key = int64_t,
         typename Traits = ListCacheTraits<Entity>>
class ListCacheRepo {
public:
    using ListCacheType = ListCache<Entity, Key, Traits>;
    using ListQuery = cache::list::ListQuery<typename Traits::Filters, typename Traits::SortField>;
    using ListResult = CachedListResult<Entity>;
    using ListResultPtr = std::shared_ptr<const ListResult>;
    using EntityPtr = std::shared_ptr<const Entity>;

    /// Prime the ListCache with dummy operations to force internal allocations.
    /// Called by CachedRepo::warmup() automatically.
    static void warmupListCache() {
        RELAIS_LOG_DEBUG << Derived::name() << ": warmupListCache() called";

        auto& lc = listCache();

        // Prime the query cache with a dummy entry
        ListQuery dummyQuery{};
        auto dummyResult = std::make_shared<const ListResult>();
        lc.put(dummyQuery, dummyResult);
        lc.invalidate(dummyQuery);

        // Prime the modification tracker
        lc.onEntityCreated(nullptr);
        lc.fullCleanup();

        RELAIS_LOG_DEBUG << Derived::name() << ": warmupListCache() complete";
    }

protected:
    /// Get the singleton ListCache instance for this entity type
    static ListCacheType& listCache() {
        static ListCacheType instance(getListCacheConfig());
        return instance;
    }

    /// Override in derived class for custom config
    static ListCacheConfig getListCacheConfig() {
        return ListCacheConfig{};  // defaults
    }

    // =========================================================================
    // Query with L1 list cache
    // =========================================================================

    /// Execute a list query with caching
    /// Returns shared_ptr to cached result (nullptr if not found and DB query fails)
    template<typename QueryFn>
    static io::Task<ListResultPtr> cachedListQuery(ListQuery query, QueryFn&& dbQuery) {
#ifndef NDEBUG
        using Clock = std::chrono::steady_clock;
        auto t0 = Clock::now();
#endif

        // Try L1 cache first
        if (auto cached = listCache().get(query)) {
            co_return cached;
        }

#ifndef NDEBUG
        auto t1 = Clock::now();
#endif

        // Execute DB query
        auto items = co_await dbQuery();

#ifndef NDEBUG
        auto t2 = Clock::now();
#endif

        // Build result
        auto result = std::make_shared<ListResult>();
        result->items.reserve(items.size());
        for (auto& item : items) {
            result->items.push_back(std::make_shared<const Entity>(std::move(item)));
        }

        // Compute next cursor if we got a full page
        auto sort = query.sort.value_or(Traits::defaultSort());
        if (result->items.size() >= query.limit && !result->items.empty()) {
            result->next_cursor = Traits::extractCursor(*result->items.back(), sort);
        }

        result->cached_at = std::chrono::steady_clock::now();

#ifndef NDEBUG
        auto t3 = Clock::now();
#endif

        // Store in cache
        listCache().put(query, result);

#ifndef NDEBUG
        auto t4 = Clock::now();
        auto us = [](auto d) { return std::chrono::duration_cast<std::chrono::microseconds>(d).count(); };
        RELAIS_LOG_DEBUG << Derived::name() << " ListCache timing: "
                  << "cache_get=" << us(t1 - t0) << "µs, "
                  << "db_query=" << us(t2 - t1) << "µs, "
                  << "build_result=" << us(t3 - t2) << "µs, "
                  << "cache_put=" << us(t4 - t3) << "µs, "
                  << "total=" << us(t4 - t0) << "µs";
#endif

        co_return result;
    }

    // =========================================================================
    // Modification notifications
    // =========================================================================

    /// Notify cache of entity creation
    static void notifyCreated(EntityPtr entity) {
        listCache().onEntityCreated(std::move(entity));
    }

    /// Notify cache of entity update
    static void notifyUpdated(EntityPtr old_entity, EntityPtr new_entity) {
        listCache().onEntityUpdated(std::move(old_entity), std::move(new_entity));
    }

    /// Notify cache of entity deletion
    static void notifyDeleted(EntityPtr entity) {
        listCache().onEntityDeleted(std::move(entity));
    }

    // =========================================================================
    // Cache management
    // =========================================================================

    /// Invalidate a specific query
    static void invalidateQuery(const ListQuery& query) {
        listCache().invalidate(query);
    }

    /// Try to trigger cleanup (non-blocking)
    static bool triggerListCacheCleanup() {
        return listCache().triggerCleanup();
    }

    /// Full cleanup (blocking)
    static size_t fullListCacheCleanup() {
        return listCache().fullCleanup();
    }

    /// Get cache size
    static size_t listCacheSize() {
        return listCache().size();
    }
};

// =============================================================================
// Helper macros for common patterns (optional)
// =============================================================================

/// Convenience macro for notifying creation in a repository create() method
#define LISTCACHE_NOTIFY_CREATED(entity_ptr) \
    ListMixin::notifyCreated((entity_ptr))

/// Convenience macro for notifying update in a repository update() method
#define LISTCACHE_NOTIFY_UPDATED(old_ptr, new_ptr) \
    ListMixin::notifyUpdated((old_ptr), (new_ptr))

/// Convenience macro for notifying deletion in a repository remove() method
#define LISTCACHE_NOTIFY_DELETED(entity_ptr) \
    ListMixin::notifyDeleted((entity_ptr))

}  // namespace jcailloux::relais::cache::list

#endif  // CODIBOT_ListCacheRepo_H
