#ifndef JCX_RELAIS_LIST_MIXIN_H
#define JCX_RELAIS_LIST_MIXIN_H

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"

#include "jcailloux/relais/DbProvider.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/list/ListCache.h"
#include "jcailloux/relais/list/ListQuery.h"
#include "jcailloux/relais/list/decl/ListDescriptor.h"
#include "jcailloux/relais/list/decl/ListDescriptorQuery.h"
#include "jcailloux/relais/list/decl/GeneratedFilters.h"
#include "jcailloux/relais/list/decl/GeneratedTraits.h"
#include "jcailloux/relais/list/decl/GeneratedCriteria.h"
#include "jcailloux/relais/wrapper/EntityConcepts.h"
#include "jcailloux/relais/wrapper/ListWrapper.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais {

/**
 * Optional mixin layer for declarative list caching.
 *
 * Activated when Entity has a ListDescriptor (detected via HasListDescriptor concept).
 * Sits in the mixin chain between the cache layer and InvalidationMixin.
 *
 * Chain: [InvalidationMixin] -> ListMixin -> CachedRepo -> [RedisRepo] -> BaseRepo
 *
 * Provides:
 * - query()           : paginated list queries with L1 caching + lazy invalidation
 * - CRUD interception : automatically notifies list cache on insert/update/erase
 * - warmup()          : primes both entity and list L1 caches
 *
 * Uses the existing ListCache (shardmap-based) for storage and ModificationTracker
 * for lazy invalidation.
 */
template<typename Base>
class ListMixin : public Base {
    using Entity = typename Base::EntityType;
    using Key = typename Base::KeyType;
    using Mapping = typename Entity::MappingType;

    // =========================================================================
    // Augmented Descriptor — adds Entity alias to embedded ListDescriptor
    // =========================================================================

    struct Descriptor : Entity::MappingType::ListDescriptor {
        using Entity = ListMixin::Entity;
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
            cq.sort = *q.sort;
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
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::WrapperPtrType;
    using Base::name;
    using Base::find;

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
    static io::Task<ListResult> query(const ListQuery& q) {
        co_return co_await cachedListQuery(q);
    }

    /// Get L1 list cache size.
    [[nodiscard]] static size_t listCacheSize() noexcept {
        return listCache().size();
    }

    // =========================================================================
    // CRUD interception — notifies list cache on entity changes
    // =========================================================================

    /// insert entity and notify list cache.
    static io::Task<WrapperPtrType> insert(WrapperPtrType wrapper)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        auto result = co_await Base::insert(std::move(wrapper));
        if (result) {
            listCache().onEntityCreated(result);
        }
        co_return result;
    }

    /// Update entity and notify list cache with old/new data.
    static io::Task<bool> update(const Key& id, WrapperPtrType wrapper)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        auto old = co_await Base::find(id);
        co_return co_await updateWithContext(id, std::move(wrapper), std::move(old));
    }

    /// Erase entity and notify list cache.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Base::config.read_only)
    {
        auto entity = co_await Base::find(id);
        co_return co_await eraseWithContext(id, std::move(entity));
    }

    /// Partial update and notify list cache.
    template<typename... Updates>
    static io::Task<WrapperPtrType> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        auto old = co_await Base::find(id);
        co_return co_await patchWithContext(id, std::move(old),
            std::forward<Updates>(updates)...);
    }

    // =========================================================================
    // Warmup — primes both entity and list L1 caches
    // =========================================================================

    static void warmup() {
        Base::warmup();
        RELAIS_LOG_DEBUG << name() << ": warming up list cache...";
        (void)listCache();
        RELAIS_LOG_DEBUG << name() << ": list cache primed";
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
        size_t entity_erased = Base::fullCleanup();
        size_t list_erased = listCache().fullCleanup();
        return entity_erased + list_erased;
    }

    /// Invalidate entity cache (list cache invalidation is lazy via ModificationTracker).
    static io::Task<void> invalidate(const Key& id) {
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

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif

protected:
    // =========================================================================
    // WithContext variants — accept pre-fetched old entity from upper mixin
    // =========================================================================

    static io::Task<bool> updateWithContext(
        const Key& id, WrapperPtrType wrapper, WrapperPtrType old_entity)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        auto new_entity = wrapper;
        bool ok = co_await Base::update(id, std::move(wrapper));
        if (ok) {
            listCache().onEntityUpdated(std::move(old_entity), std::move(new_entity));
        }
        co_return ok;
    }

    static io::Task<std::optional<size_t>> eraseWithContext(
        const Key& id, WrapperPtrType old_entity)
        requires (!Base::config.read_only)
    {
        auto result = co_await Base::erase(id);
        if (result.has_value() && old_entity) {
            listCache().onEntityDeleted(std::move(old_entity));
        }
        co_return result;
    }

    template<typename... Updates>
    static io::Task<WrapperPtrType> patchWithContext(
        const Key& id, WrapperPtrType old_entity, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        auto result = co_await Base::patch(id, std::forward<Updates>(updates)...);
        if (result) {
            listCache().onEntityUpdated(std::move(old_entity), result);
        }
        co_return result;
    }

    // =========================================================================
    // Cached list query implementation
    // =========================================================================

    static io::Task<ListResult> cachedListQuery(const ListQuery& query) {
        auto& cache = listCache();
        auto cache_query = toCacheQuery(query);

        // Try cache first — zero copy: return shared_ptr directly
        if (auto cached = cache.get(cache_query)) {
            co_return cached;
        }

        // Cache miss — query database
        auto entities = co_await queryFromDb(query);
        auto sort = query.sort.value_or(defaultSortAsListSpec());

        // Build ListWrapper directly from entities
        auto wrapper = std::make_shared<ListWrapperType>();
        wrapper->items = std::move(entities);

        // Set cursor for pagination (base64 string in ListWrapper)
        if (wrapper->items.size() >= query.limit && !wrapper->items.empty()) {
            auto cursor = Traits::extractCursor(
                wrapper->items.back(),
                cache_query.sort.value_or(Traits::defaultSort()));
            wrapper->next_cursor = cursor.encode();
        }

        // Extract sort bounds from entities
        cache::list::SortBounds bounds;
        if (!wrapper->items.empty()) {
            bounds.first_value = cache::list::decl::extractSortValue<Descriptor>(
                wrapper->items.front(), sort.field);
            bounds.last_value = cache::list::decl::extractSortValue<Descriptor>(
                wrapper->items.back(), sort.field);
            bounds.is_valid = true;
        }

        // Store in cache and return shared_ptr (zero copy on subsequent gets)
        cache.put(cache_query, wrapper, bounds);
        co_return wrapper;
    }

    // =========================================================================
    // Database query — direct SQL via PgClient
    // =========================================================================

    static io::Task<std::vector<Entity>> queryFromDb(const ListQuery& query) {
        try {
            // Build WHERE clause from filters
            auto where = cache::list::decl::buildWhereClause<Descriptor>(query.filters);

            // Parse sort
            auto sort = query.sort.value_or(defaultSortAsListSpec());
            auto sort_col = cache::list::decl::sortColumnName<Descriptor>(sort.field);
            const char* sort_dir = (sort.direction == cache::list::SortDirection::Asc)
                                  ? "ASC" : "DESC";

            // Build SQL: SELECT * FROM "table" [WHERE ...] ORDER BY "col" DIR LIMIT N
            std::string sql;
            sql.reserve(256);
            sql += "SELECT * FROM ";
            sql += Mapping::table_name;
            if (!where.sql.empty()) {
                sql += " WHERE ";
                sql += where.sql;
            }
            sql += " ORDER BY \"";
            sql += sort_col;
            sql += "\" ";
            sql += sort_dir;
            sql += " LIMIT ";
            sql += std::to_string(query.limit);

            // Execute
            auto result = co_await DbProvider::queryParams(sql.c_str(), where.params);

            // Build entity vector from rows
            std::vector<Entity> entities;
            entities.reserve(static_cast<size_t>(result.rows()));
            for (int i = 0; i < result.rows(); ++i) {
                if (auto e = Entity::fromRow(result[i])) {
                    entities.push_back(std::move(*e));
                }
            }

            co_return entities;

        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": queryFromDb error - " << e.what();
            co_return {};
        }
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_LIST_MIXIN_H
