#ifndef JCX_DROGON_LIST_MIXIN_H
#define JCX_DROGON_LIST_MIXIN_H

#include <drogon/HttpAppFramework.h>
#include <drogon/orm/CoroMapper.h>
#include <drogon/orm/Criteria.h>
#include <drogon/utils/coroutine.h>
#include <trantor/utils/Logger.h>

#include "jcailloux/drogon/list/ListCache.h"
#include "jcailloux/drogon/list/ListQuery.h"
#include "jcailloux/drogon/list/decl/ListDescriptor.h"
#include "jcailloux/drogon/list/decl/ListDescriptorQuery.h"
#include "jcailloux/drogon/list/decl/GeneratedFilters.h"
#include "jcailloux/drogon/list/decl/GeneratedTraits.h"
#include "jcailloux/drogon/list/decl/GeneratedCriteria.h"
#include "jcailloux/drogon/wrapper/EntityConcepts.h"
#include "jcailloux/drogon/wrapper/ListWrapper.h"

#ifdef SMARTREPO_BUILDING_TESTS
namespace smartrepo_test { struct TestInternals; }
#endif

namespace jcailloux::drogon::smartrepo {

/**
 * Optional mixin layer for declarative list caching.
 *
 * Activated when Entity has a ListDescriptor (detected via HasListDescriptor concept).
 * Sits in the mixin chain between the cache layer and InvalidationMixin.
 *
 * Chain: [InvalidationMixin] -> ListMixin -> CachedRepository -> [RedisRepository] -> BaseRepository
 *
 * Provides:
 * - query()           : paginated list queries with L1 caching + lazy invalidation
 * - CRUD interception : automatically notifies list cache on create/update/remove
 * - warmup()          : primes both entity and list L1 caches
 *
 * Uses the existing ListCache (shardmap-based) for storage and ModificationTracker
 * for lazy invalidation.
 * into a single shardmap-based mode.
 */
template<typename Base>
class ListMixin : public Base {
    using Entity = typename Base::EntityType;
    using Key = typename Base::KeyType;
    using Model = typename Base::ModelType;

    // =========================================================================
    // Augmented Descriptor — adds Entity/Model aliases to embedded ListDescriptor
    // =========================================================================

    struct Descriptor : Entity::MappingType::ListDescriptor {
        using Entity = ListMixin::Entity;
        using Model = ListMixin::Model;
    };

    // =========================================================================
    // Type aliases from list infrastructure
    // =========================================================================

    using DescriptorFilters = cache::list::decl::Filters<Descriptor>;
    using DescriptorSortSpec = cache::list::decl::SortSpec<Descriptor>;

    // =========================================================================
    // Traits adapter — bridges Descriptor helpers to ListCache interface
    // =========================================================================

    struct Traits {
        using Filters = DescriptorFilters;
        using SortField = size_t;
        using FilterTags = Filters;

        static bool matchesFilters(const Entity& e, const Filters& f) {
            return cache::list::decl::matchesFilters<Descriptor>(e, f);
        }

        static int compare(const Entity& a, const Entity& b,
                          SortField field_index, cache::list::SortDirection dir) {
            DescriptorSortSpec sort{field_index,
                dir == cache::list::SortDirection::Asc
                    ? cache::list::decl::SortDirection::Asc
                    : cache::list::decl::SortDirection::Desc};
            return cache::list::decl::compare<Descriptor>(a, b, sort);
        }

        static cache::list::Cursor extractCursor(const Entity& e,
                                                  const cache::list::SortSpec<size_t>& sort) {
            DescriptorSortSpec descriptor_sort{sort.field,
                sort.direction == cache::list::SortDirection::Asc
                    ? cache::list::decl::SortDirection::Asc
                    : cache::list::decl::SortDirection::Desc};
            auto cursor = cache::list::decl::extractCursor<Descriptor>(e, descriptor_sort);

            cache::list::Cursor result;
            result.data.reserve(cursor.data.size());
            for (uint8_t b : cursor.data) {
                result.data.push_back(static_cast<std::byte>(b));
            }
            return result;
        }

        static bool isBeforeOrAtCursor(const Entity& e,
                                       const cache::list::Cursor& cursor,
                                       const cache::list::SortSpec<size_t>& sort) {
            cache::list::decl::Cursor descriptor_cursor;
            descriptor_cursor.data.reserve(cursor.data.size());
            for (std::byte b : cursor.data) {
                descriptor_cursor.data.push_back(static_cast<uint8_t>(b));
            }

            DescriptorSortSpec descriptor_sort{sort.field,
                sort.direction == cache::list::SortDirection::Asc
                    ? cache::list::decl::SortDirection::Asc
                    : cache::list::decl::SortDirection::Desc};
            return cache::list::decl::isBeforeOrAtCursor<Descriptor>(
                e, descriptor_cursor, descriptor_sort);
        }

        static FilterTags extractTags(const Entity& e) {
            return cache::list::decl::extractTags<Descriptor>(e);
        }

        static int64_t extractSortValue(const Entity& e, size_t field_index) {
            return cache::list::decl::extractSortValue<Descriptor>(e, field_index);
        }

        static constexpr cache::list::SortSpec<size_t> defaultSort() {
            auto descriptor_sort = cache::list::decl::defaultSort<Descriptor>();
            return {descriptor_sort.field_index,
                descriptor_sort.direction == cache::list::decl::SortDirection::Asc
                    ? cache::list::SortDirection::Asc
                    : cache::list::SortDirection::Desc};
        }

        static std::optional<size_t> parseSortField(std::string_view field) {
            return cache::list::decl::parseSortField<Descriptor>(field);
        }

        static std::string_view sortFieldName(size_t field_index) {
            return cache::list::decl::sortFieldName<Descriptor>(field_index);
        }

        static constexpr std::array<uint16_t, 4> limitSteps = {10, 25, 50, 100};
        static constexpr uint16_t maxLimit = 100;

        static uint16_t normalizeLimit(uint16_t requested) {
            return cache::list::decl::normalizeLimit<Descriptor>(requested);
        }
    };

    // =========================================================================
    // Cache infrastructure
    // =========================================================================

    using ListWrapperType = wrapper::ListWrapper<Entity>;
    using ListCacheType = cache::list::ListCache<Entity, Base::config.l1_shard_count_log2, int64_t, Traits>;

    static cache::list::ListCacheConfig listCacheConfig() {
        return {
            .cleanup_every_n_gets = Base::config.l1_cleanup_every_n_gets,
            .default_ttl = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::nanoseconds(Base::config.l1_ttl)),
            .accept_expired_on_get = Base::config.l1_accept_expired_on_get,
            .refresh_on_get = Base::config.l1_refresh_on_get,
        };
    }

    static ListCacheType& listCache() {
        static ListCacheType instance(listCacheConfig());
        return instance;
    }

    // =========================================================================
    // Query types
    // =========================================================================

    using CacheQuery = cache::list::ListQuery<DescriptorFilters, size_t>;

    static CacheQuery toCacheQuery(const auto& q) {
        CacheQuery cq;
        cq.filters = q.filters;
        cq.limit = q.limit;
        cq.cursor = q.cursor;
        cq.query_hash = q.query_hash;
        if (q.sort) {
            cq.sort = *q.sort;  // Same SortSpec<size_t> type
        }
        return cq;
    }

    /// Convert decl::defaultSort → cache::list::SortSpec<size_t>
    static cache::list::SortSpec<size_t> defaultSortAsListSpec() {
        auto ds = cache::list::decl::defaultSort<Descriptor>();
        return {ds.field_index,
            ds.direction == cache::list::decl::SortDirection::Asc
                ? cache::list::SortDirection::Asc
                : cache::list::SortDirection::Desc};
    }

public:
    using typename Base::EntityType;
    using typename Base::ModelType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::WrapperPtrType;
    using Base::name;
    using Base::findById;

    /// Augmented descriptor — pass to parseListQueryStrict<ListDescriptorType>(req)
    using ListDescriptorType = Descriptor;

    /// List query type — compatible with parseListQueryStrict return type
    using ListQuery = cache::list::decl::ListDescriptorQuery<Descriptor>;

    /// List result type — returned by query()
    using ListResult = std::shared_ptr<const ListWrapperType>;

    /// Traits type — exposed for controllers (sort parsing, limit normalization, etc.)
    using ListTraits = Traits;

    // =========================================================================
    // Query interface
    // =========================================================================

    /// Execute a paginated list query with L1 caching and lazy invalidation.
    static ::drogon::Task<ListResult> query(const ListQuery& q) {
        co_return co_await cachedListQuery(q);
    }

    /// Get L1 list cache size.
    [[nodiscard]] static size_t listCacheSize() noexcept {
        return listCache().size();
    }

    // =========================================================================
    // CRUD interception — notifies list cache on entity changes
    // =========================================================================

    /// Create entity and notify list cache.
    static ::drogon::Task<WrapperPtrType> create(WrapperPtrType wrapper)
        requires MutableEntity<Entity, Model> && (!Base::config.read_only)
    {
        auto result = co_await Base::create(std::move(wrapper));
        if (result) {
            listCache().onEntityCreated(result);
        }
        co_return result;
    }

    /// Update entity and notify list cache with old/new data.
    static ::drogon::Task<bool> update(const Key& id, WrapperPtrType wrapper)
        requires MutableEntity<Entity, Model> && (!Base::config.read_only)
    {
        auto old = co_await Base::findById(id);
        co_return co_await updateWithContext(id, std::move(wrapper), std::move(old));
    }

    /// Remove entity and notify list cache.
    static ::drogon::Task<std::optional<size_t>> remove(const Key& id)
        requires (!Base::config.read_only)
    {
        auto entity = co_await Base::findById(id);
        co_return co_await removeWithContext(id, std::move(entity));
    }

    /// Partial update and notify list cache.
    template<typename... Updates>
    static ::drogon::Task<WrapperPtrType> updateBy(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        auto old = co_await Base::findById(id);
        co_return co_await updateByWithContext(id, std::move(old),
            std::forward<Updates>(updates)...);
    }

    // =========================================================================
    // Warmup — primes both entity and list L1 caches
    // =========================================================================

    static void warmup() {
        Base::warmup();
        LOG_DEBUG << name() << ": warming up list cache...";
        (void)listCache();
        LOG_DEBUG << name() << ": list cache primed";
    }

    // =========================================================================
    // Cache management — unified entity + list cleanup
    // =========================================================================

    /// Trigger cleanup on both entity and list L1 caches (non-blocking).
    static bool triggerCleanup() {
        bool entity_cleaned = Base::triggerCleanup();
        bool list_cleaned = listCache().triggerCleanup();
        return entity_cleaned || list_cleaned;
    }

    /// Full cleanup on both entity and list L1 caches (blocking).
    static size_t fullCleanup() {
        size_t entity_removed = Base::fullCleanup();
        size_t list_removed = listCache().fullCleanup();
        return entity_removed + list_removed;
    }

    /// Invalidate entity cache (list cache invalidation is lazy via ModificationTracker).
    static ::drogon::Task<void> invalidate(const Key& id) {
        co_await Base::invalidate(id);
    }

    // =========================================================================
    // Cross-invalidation entry points (used by InvalidateList)
    // =========================================================================

    static void notifyCreated(WrapperPtrType entity) {
        if (entity) listCache().onEntityCreated(std::move(entity));
    }

    static void notifyUpdated(WrapperPtrType old_entity, WrapperPtrType new_entity) {
        listCache().onEntityUpdated(std::move(old_entity), std::move(new_entity));
    }

    static void notifyDeleted(WrapperPtrType entity) {
        if (entity) listCache().onEntityDeleted(std::move(entity));
    }

#ifdef SMARTREPO_BUILDING_TESTS
    friend struct ::smartrepo_test::TestInternals;
#endif

protected:
    // =========================================================================
    // WithContext variants — accept pre-fetched old entity from upper mixin
    // =========================================================================

    static ::drogon::Task<bool> updateWithContext(
        const Key& id, WrapperPtrType wrapper, WrapperPtrType old_entity)
        requires MutableEntity<Entity, Model> && (!Base::config.read_only)
    {
        auto new_entity = wrapper;
        bool ok = co_await Base::update(id, std::move(wrapper));
        if (ok) {
            listCache().onEntityUpdated(std::move(old_entity), std::move(new_entity));
        }
        co_return ok;
    }

    static ::drogon::Task<std::optional<size_t>> removeWithContext(
        const Key& id, WrapperPtrType old_entity)
        requires (!Base::config.read_only)
    {
        auto result = co_await Base::remove(id);
        if (result.has_value() && old_entity) {
            listCache().onEntityDeleted(std::move(old_entity));
        }
        co_return result;
    }

    template<typename... Updates>
    static ::drogon::Task<WrapperPtrType> updateByWithContext(
        const Key& id, WrapperPtrType old_entity, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        auto result = co_await Base::updateBy(id, std::forward<Updates>(updates)...);
        if (result) {
            listCache().onEntityUpdated(std::move(old_entity), result);
        }
        co_return result;
    }

    // =========================================================================
    // Cached list query implementation
    // =========================================================================

    static ::drogon::Task<ListResult> cachedListQuery(const ListQuery& query) {
        auto& cache = listCache();
        auto cache_query = toCacheQuery(query);

        // Try cache first — zero copy: return shared_ptr directly
        if (auto cached = cache.get(cache_query)) {
            co_return cached;
        }

        // Cache miss — query database
        auto models = co_await queryFromDb(query);
        auto sort = query.sort.value_or(defaultSortAsListSpec());

        // Build ListWrapper directly from Models (no per-item shared_ptr allocation)
        auto wrapper = std::make_shared<ListWrapperType>(
            ListWrapperType::fromModels(models));

        // Set cursor for pagination (base64 string in ListWrapper)
        if (wrapper->items.size() >= query.limit && !wrapper->items.empty()) {
            auto cursor = Traits::extractCursor(
                wrapper->items.back(),
                cache_query.sort.value_or(Traits::defaultSort()));
            wrapper->next_cursor = cursor.encode();
        }

        // Extract sort bounds from Models (avoids Entity conversion overhead)
        cache::list::SortBounds bounds;
        if (!models.empty()) {
            bounds.first_value = cache::list::decl::extractSortValueFromModel<Descriptor>(
                models.front(), sort.field);
            bounds.last_value = cache::list::decl::extractSortValueFromModel<Descriptor>(
                models.back(), sort.field);
            bounds.is_valid = true;
        }

        // Store in cache and return shared_ptr (zero copy on subsequent gets)
        cache.put(cache_query, wrapper, bounds);
        co_return wrapper;
    }

    // =========================================================================
    // Database query
    // =========================================================================

    static ::drogon::Task<std::vector<Model>> queryFromDb(const ListQuery& query) {
        using namespace ::drogon::orm;

        try {
            auto db = ::drogon::app().getDbClient();
            CoroMapper<Model> mapper(db);

            // Build criteria from filters
            auto [criteria, hasCriteria] = cache::list::decl::buildCriteria<Descriptor>(
                query.filters);

            // Parse sort
            auto sort = query.sort.value_or(defaultSortAsListSpec());
            const auto& order_col = cache::list::decl::sortColumnName<Descriptor>(
                sort.field);
            SortOrder order_dir = (sort.direction == cache::list::SortDirection::Asc)
                                ? SortOrder::ASC : SortOrder::DESC;

            // Execute query
            std::vector<Model> models;
            if (hasCriteria) {
                models = co_await mapper.orderBy(order_col, order_dir)
                                        .limit(query.limit)
                                        .findBy(criteria);
            } else {
                models = co_await mapper.orderBy(order_col, order_dir)
                                        .limit(query.limit)
                                        .findAll();
            }

            co_return models;

        } catch (const ::drogon::orm::DrogonDbException& e) {
            LOG_ERROR << name() << ": queryFromDb error - " << e.base().what();
            co_return {};
        }
    }
};

}  // namespace jcailloux::drogon::smartrepo

#endif  // JCX_DROGON_LIST_MIXIN_H
