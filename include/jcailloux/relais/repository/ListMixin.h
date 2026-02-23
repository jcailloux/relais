#ifndef JCX_RELAIS_LIST_MIXIN_H
#define JCX_RELAIS_LIST_MIXIN_H

#include <mutex>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"

#include "jcailloux/relais/DbProvider.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/cache/RedisCache.h"
#include "jcailloux/relais/list/ListCache.h"
#include "jcailloux/relais/list/ListQuery.h"
#include "jcailloux/relais/list/decl/ListDescriptor.h"
#include "jcailloux/relais/list/decl/ListDescriptorQuery.h"
#include "jcailloux/relais/list/decl/GeneratedFilters.h"
#include "jcailloux/relais/list/decl/GeneratedTraits.h"
#include "jcailloux/relais/list/decl/GeneratedCriteria.h"
#include "jcailloux/relais/list/decl/HttpQueryParser.h"
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
 * - query()           : paginated list queries with L1/L2 caching
 * - CRUD interception : automatically invalidates list caches on insert/update/erase
 * - warmup()          : primes both entity and list L1 caches
 *
 * L1 uses ListCache (ChunkMap-based) with ModificationTracker for lazy invalidation.
 * L2 uses Redis with binary (BEVE) storage and active invalidation via Lua scripts.
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
    // Cache level detection (compile-time)
    // =========================================================================

    static constexpr bool kHasL1 =
        Base::config.cache_level == config::CacheLevel::L1
     || Base::config.cache_level == config::CacheLevel::L1_L2;

    static constexpr bool kHasL2 =
        Base::config.cache_level == config::CacheLevel::L2
     || Base::config.cache_level == config::CacheLevel::L1_L2;

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

    static constexpr bool HasGDSF = (cache::GDSFPolicy::kMaxMemory > 0);

    using ListWrapperType = wrapper::ListWrapper<Entity>;
    using ListCacheType = cache::list::ListCache<Entity, Base::config.l1_chunk_count_log2, Key, Traits, HasGDSF>;

    static cache::list::ListCacheConfig listCacheConfig() {
        return {
            .default_ttl = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::nanoseconds(Base::config.l1_ttl)),
        };
    }

    static ListCacheType& listCache() {
        static ListCacheType instance(listCacheConfig());
        if constexpr (HasGDSF) {
            static std::once_flag gdsf_flag;
            std::call_once(gdsf_flag, []() {
                static const std::string list_name =
                    std::string(Base::name()) + ":list";
                cache::GDSFPolicy::instance().enroll({
                    .sweep_fn = +[]() -> bool { return listCache().sweep(); },
                    .size_fn = +[]() -> size_t { return listCache().size(); },
                    .name = list_name.c_str()
                });
            });
        }
        return instance;
    }

    // L2 TTL helper
    static constexpr auto l2Ttl() { return std::chrono::nanoseconds(Base::config.l2_ttl); }

    // Epoch pools for list wrappers and serialization buffers (non-L1 paths)
    static epoch::memory_pool<ListWrapperType>& listPool() {
        static epoch::memory_pool<ListWrapperType> p;
        return p;
    }

    static wrapper::BufferView<ListWrapperType> makeListView(ListWrapperType&& w) {
        auto guard = epoch::EpochGuard::acquire();
        auto* ptr = listPool().New(std::move(w));
        listPool().Retire(ptr);
        return wrapper::BufferView<ListWrapperType>(ptr, std::move(guard));
    }

    static epoch::memory_pool<std::string>& jsonPool() {
        static epoch::memory_pool<std::string> p;
        return p;
    }

    static epoch::memory_pool<std::vector<uint8_t>>& binaryPool() {
        static epoch::memory_pool<std::vector<uint8_t>> p;
        return p;
    }

    // Redis key helpers for declarative list caching
    static std::string redisPageKey(const std::string& cache_key) {
        std::string key(Base::name());
        key += ":dlist:p:";
        key.append(cache_key);
        return key;
    }

    static std::string redisGroupKey(const std::string& group_key) {
        std::string key(Base::name());
        key += ":dlist:g:";
        key.append(group_key);
        return key;
    }

    static std::string redisMasterSetKey() {
        return std::string(Base::name()) + ":dlist_groups";
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
        cq.cache_key = q.cache_key;
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
    using typename Base::FindResultType;
    using Base::name;
    using Base::find;

    /// Augmented descriptor — pass to parseListQueryStrict<ListDescriptorType>(req)
    using ListDescriptorType = Descriptor;

    /// List query type — compatible with parseListQueryStrict return type
    using ListQuery = cache::list::decl::ListDescriptorQuery<Descriptor>;

    /// List result type — returned by query() (epoch-guarded, zero-copy)
    using ListResult = wrapper::BufferView<ListWrapperType>;

    /// Traits type — exposed for controllers (sort parsing, limit normalization, etc.)
    using ListTraits = Traits;

    // =========================================================================
    // Query interface
    // =========================================================================

    /// Execute a paginated list query with L1/L2 caching.
    /// L1 hit returns fromValue (zero-alloc, synchronous). Miss goes through coroutine.
    static io::Task<ListResult> query(const ListQuery& q) {
        if constexpr (kHasL1) {
            if (auto cached = listCache().getByKey(q.cache_key))
                return io::Task<ListResult>::fromValue(std::move(cached));
        }
        return cachedListQuery(q);
    }

    /// Execute a paginated list query and return raw JSON string.
    /// L1 hit: serializes from cached entity (lazy, epoch-guarded).
    /// L2 hit (BEVE): transcodes via glz::beve_to_json (skips 19-byte ListBoundsHeader).
    /// L2/DB miss: delegates to entity path (cachedListQuery).
    static io::Task<wrapper::JsonView> queryJson(const ListQuery& q) {
        // L1 check: serialize from cached entities
        if constexpr (kHasL1) {
            if (auto cached = listCache().getByKey(q.cache_key)) {
                auto json_sp = cached->json();  // lazy serialization, returns shared_ptr
                auto guard = cached.take_guard();
                co_return wrapper::JsonView(json_sp.get(), std::move(guard));
            }
        }

        // L2 check: BEVE → JSON transcode (skip ListBoundsHeader)
        if constexpr (kHasL2) {
            auto pageKey = redisPageKey(q.cache_key);

            std::optional<std::vector<uint8_t>> beve;
            if constexpr (Base::config.l2_refresh_on_get) {
                beve = co_await cache::RedisCache::getRawBinaryEx(pageKey, l2Ttl());
            } else {
                beve = co_await cache::RedisCache::getRawBinary(pageKey);
            }

            if (beve) {
                // Skip ListBoundsHeader (19 bytes, magic 0x53 0x52)
                size_t off = (beve->size() > 19
                    && (*beve)[0] == 0x53 && (*beve)[1] == 0x52) ? 19 : 0;
                std::string json;
                if (!glz::beve_to_json(
                        std::span(beve->data() + off, beve->size() - off), json)) {
                    auto guard = epoch::EpochGuard::acquire();
                    auto* ptr = jsonPool().New(std::move(json));
                    jsonPool().Retire(ptr);
                    co_return wrapper::JsonView(ptr, std::move(guard));
                }
            }
        }

        // Miss: entity path (needed for cursor/bounds/L1 population)
        auto wrapper = co_await cachedListQuery(q);
        if (!wrapper) co_return wrapper::JsonView{};
        auto json_sp = wrapper->json();
        auto guard = wrapper.take_guard();
        co_return wrapper::JsonView(json_sp.get(), std::move(guard));
    }

    /// Execute a paginated list query and return raw binary (BEVE).
    /// L1 hit: serializes from cached entity (lazy, epoch-guarded).
    /// L2 hit: returns raw binary (skips ListBoundsHeader).
    /// L2/DB miss: delegates to entity path (cachedListQuery).
    static io::Task<wrapper::BinaryView> queryBinary(const ListQuery& q)
        requires HasBinarySerialization<Entity>
    {
        // L1 check: serialize from cached entities
        if constexpr (kHasL1) {
            if (auto cached = listCache().getByKey(q.cache_key)) {
                auto bin_sp = cached->binary();  // lazy serialization, returns shared_ptr
                auto guard = cached.take_guard();
                co_return wrapper::BinaryView(bin_sp.get(), std::move(guard));
            }
        }

        // L2 check: raw binary from Redis (skip ListBoundsHeader)
        if constexpr (kHasL2) {
            auto pageKey = redisPageKey(q.cache_key);

            std::optional<std::vector<uint8_t>> beve;
            if constexpr (Base::config.l2_refresh_on_get) {
                beve = co_await cache::RedisCache::getRawBinaryEx(pageKey, l2Ttl());
            } else {
                beve = co_await cache::RedisCache::getRawBinary(pageKey);
            }

            if (beve) {
                // Skip ListBoundsHeader (19 bytes, magic 0x53 0x52)
                size_t off = (beve->size() > 19
                    && (*beve)[0] == 0x53 && (*beve)[1] == 0x52) ? 19 : 0;
                auto guard = epoch::EpochGuard::acquire();
                auto* ptr = binaryPool().New(
                    beve->begin() + static_cast<ptrdiff_t>(off), beve->end());
                binaryPool().Retire(ptr);
                co_return wrapper::BinaryView(ptr, std::move(guard));
            }
        }

        // Miss: entity path (needed for cursor/bounds/L1 population)
        auto wrapper = co_await cachedListQuery(q);
        if (!wrapper) co_return wrapper::BinaryView{};
        auto bin_sp = wrapper->binary();
        auto guard = wrapper.take_guard();
        co_return wrapper::BinaryView(bin_sp.get(), std::move(guard));
    }

    /// Get L1 list cache size.
    [[nodiscard]] static size_t listSize() noexcept {
        if constexpr (kHasL1) {
            return listCache().size();
        } else {
            return 0;
        }
    }

    // =========================================================================
    // CRUD interception — invalidates list caches on entity changes
    // =========================================================================

    /// Insert entity and invalidate list caches.
    static io::Task<wrapper::EntityView<Entity>> insert(const Entity& entity)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        auto result = co_await Base::insert(entity);
        if (result) {
            if constexpr (kHasL1) { listCache().onEntityCreated(*result); }
            if constexpr (kHasL2) { co_await invalidateL2Created(*result); }
        }
        co_return result;
    }

    /// Update entity and invalidate list caches.
    static io::Task<bool> update(const Key& id, const Entity& entity)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        std::optional<Entity> old;
        {
            auto view = co_await Base::find(id);
            if (view) old.emplace(*view);
        }
        co_return co_await updateWithContext(id, entity, old ? &*old : nullptr);
    }

    /// Erase entity and invalidate list caches.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Base::config.read_only)
    {
        std::optional<Entity> old;
        {
            auto view = co_await Base::find(id);
            if (view) old.emplace(*view);
        }
        co_return co_await eraseWithContext(id, old ? &*old : nullptr);
    }

    /// Partial update and invalidate list caches.
    template<typename... Updates>
    static io::Task<wrapper::EntityView<Entity>> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        std::optional<Entity> old;
        {
            auto view = co_await Base::find(id);
            if (view) old.emplace(*view);
        }
        co_return co_await patchWithContext(id, old ? &*old : nullptr,
            std::forward<Updates>(updates)...);
    }

    // =========================================================================
    // Warmup — primes entity and list L1 caches
    // =========================================================================

    static void warmup() {
        Base::warmup();
        if constexpr (kHasL1) {
            RELAIS_LOG_DEBUG << name() << ": warming up list cache...";
            (void)listCache();
            RELAIS_LOG_DEBUG << name() << ": list cache primed";
        }
    }

    // =========================================================================
    // Cache management — unified entity + list cleanup
    // =========================================================================

    /// Try to sweep one chunk on both entity and list caches.
    static bool trySweep() {
        bool entity_cleaned = Base::trySweep();
        if constexpr (kHasL1) {
            return entity_cleaned | listCache().trySweep();
        }
        return entity_cleaned;
    }

    /// Sweep one chunk on both entity and list caches.
    static bool sweep() {
        bool entity_cleaned = Base::sweep();
        if constexpr (kHasL1) {
            return entity_cleaned | listCache().sweep();
        }
        return entity_cleaned;
    }

    /// Sweep all chunks on both entity and list caches.
    static size_t purge() {
        size_t entity_erased = Base::purge();
        if constexpr (kHasL1) {
            return entity_erased + listCache().purge();
        }
        return entity_erased;
    }

    /// Try to sweep one entity cache chunk.
    static bool trySweepEntities() { return Base::trySweep(); }

    /// Sweep one entity cache chunk.
    static bool sweepEntities() { return Base::sweep(); }

    /// Sweep all entity cache chunks.
    static size_t purgeEntities() { return Base::purge(); }

    /// Try to sweep one list cache chunk.
    static bool trySweepLists() {
        if constexpr (kHasL1) { return listCache().trySweep(); }
        return false;
    }

    /// Sweep one list cache chunk.
    static bool sweepLists() {
        if constexpr (kHasL1) { return listCache().sweep(); }
        return false;
    }

    /// Sweep all list cache chunks.
    static size_t purgeLists() {
        if constexpr (kHasL1) { return listCache().purge(); }
        return 0;
    }

    /// Invalidate entity cache. L1 list cache uses lazy invalidation via ModificationTracker.
    static io::Task<void> invalidate(const Key& id) {
        co_await Base::invalidate(id);
    }

    /// Invalidate all L2 declarative list cache groups for this repository.
    static io::Task<size_t> invalidateAllListGroups() {
        if constexpr (kHasL2) {
            co_return co_await invalidateRedisListGroups();
        } else {
            co_return 0;
        }
    }

    // =========================================================================
    // Cross-invalidation entry points
    // =========================================================================
    //
    // Synchronous API: L1 invalidation inline + L2 fire-and-forget (DetachedTask).
    // Called by InvalidateList<> cross-invalidation and external sync callers.
    // CRUD methods use co_await for L2 instead (no fire-and-forget).

    static void notifyCreated(const Entity& entity) {
        if constexpr (kHasL1) { listCache().onEntityCreated(entity); }
        if constexpr (kHasL2) { fireL2Created(Entity(entity)); }
    }

    static void notifyUpdated(const Entity& old_entity, const Entity& new_entity) {
        if constexpr (kHasL1) { listCache().onEntityUpdated(old_entity, new_entity); }
        if constexpr (kHasL2) { fireL2Updated(Entity(old_entity), Entity(new_entity)); }
    }

    static void notifyDeleted(const Entity& entity) {
        if constexpr (kHasL1) { listCache().onEntityDeleted(entity); }
        if constexpr (kHasL2) { fireL2Deleted(Entity(entity)); }
    }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif

protected:
    // =========================================================================
    // WithContext variants — accept pre-fetched old entity from upper mixin
    // =========================================================================

    static io::Task<bool> updateWithContext(
        const Key& id, const Entity& entity, const Entity* old_entity)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        bool ok = co_await Base::update(id, entity);
        if (ok) {
            if constexpr (kHasL1) {
                if (old_entity) {
                    listCache().onEntityUpdated(*old_entity, entity);
                } else {
                    listCache().onEntityCreated(entity);
                }
            }
            if constexpr (kHasL2) {
                co_await invalidateL2Updated(old_entity ? *old_entity : entity, entity);
            }
        }
        co_return ok;
    }

    static io::Task<std::optional<size_t>> eraseWithContext(
        const Key& id, const Entity* old_entity)
        requires (!Base::config.read_only)
    {
        auto result = co_await Base::erase(id);
        if (result.has_value() && old_entity) {
            if constexpr (kHasL1) { listCache().onEntityDeleted(*old_entity); }
            if constexpr (kHasL2) { co_await invalidateL2Deleted(*old_entity); }
        }
        co_return result;
    }

    template<typename... Updates>
    static io::Task<wrapper::EntityView<Entity>> patchWithContext(
        const Key& id, const Entity* old_entity, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        auto result = co_await Base::patch(id, std::forward<Updates>(updates)...);
        if (result) {
            if constexpr (kHasL1) {
                if (old_entity) {
                    listCache().onEntityUpdated(*old_entity, *result);
                } else {
                    listCache().onEntityCreated(*result);
                }
            }
            if constexpr (kHasL2) {
                co_await invalidateL2Updated(
                    old_entity ? *old_entity : *result, *result);
            }
        }
        co_return result;
    }

    // =========================================================================
    // Redis L2 selective invalidation (all-in-one Lua, 1 RTT)
    // =========================================================================

    /// Build comma-separated sort values for all sort fields.
    static std::string buildSortValues(const Entity& entity) {
        std::string result;
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            size_t count = 0;
            ((result += (count++ > 0 ? "," : ""),
              result += std::to_string(Traits::extractSortValue(entity, Is))), ...);
        }(std::make_index_sequence<cache::list::decl::sort_count<Descriptor>>{});
        return result;
    }

    /// Selective L2 invalidation for entity creation (or deletion).
    static io::Task<size_t> invalidateL2Created(const Entity& entity) {
        auto masterKey = redisMasterSetKey();
        auto prefixLen = std::string(Base::name()).size() + 9; // ":dlist:g:"
        auto schema = cache::list::decl::filterSchema<Descriptor>();
        auto blob = cache::list::decl::encodeEntityFilterBlob<Descriptor>(entity);
        auto sortVals = buildSortValues(entity);

        co_return co_await cache::RedisCache::invalidateListGroupsSelective(
            masterKey, prefixLen, schema, blob, sortVals);
    }

    /// Selective L2 invalidation for entity update (old + new entities).
    static io::Task<size_t> invalidateL2Updated(const Entity& old_e, const Entity& new_e) {
        auto masterKey = redisMasterSetKey();
        auto prefixLen = std::string(Base::name()).size() + 9;
        auto schema = cache::list::decl::filterSchema<Descriptor>();
        auto newBlob = cache::list::decl::encodeEntityFilterBlob<Descriptor>(new_e);
        auto newSortVals = buildSortValues(new_e);
        auto oldBlob = cache::list::decl::encodeEntityFilterBlob<Descriptor>(old_e);
        auto oldSortVals = buildSortValues(old_e);

        co_return co_await cache::RedisCache::invalidateListGroupsSelectiveUpdate(
            masterKey, prefixLen, schema, newBlob, newSortVals, oldBlob, oldSortVals);
    }

    /// Selective L2 invalidation for entity deletion (same logic as creation).
    static io::Task<size_t> invalidateL2Deleted(const Entity& entity) {
        return invalidateL2Created(entity);
    }

    /// Fire-and-forget L2 invalidation for entity creation.
    static io::DetachedTask fireL2Created(Entity entity) {
        try { co_await invalidateL2Created(entity); } catch (...) {}
    }

    /// Fire-and-forget L2 invalidation for entity update.
    static io::DetachedTask fireL2Updated(Entity old_e, Entity new_e) {
        try { co_await invalidateL2Updated(old_e, new_e); } catch (...) {}
    }

    /// Fire-and-forget L2 invalidation for entity deletion.
    static io::DetachedTask fireL2Deleted(Entity entity) {
        try { co_await invalidateL2Deleted(entity); } catch (...) {}
    }

    /// Coarse L2 invalidation — fallback that invalidates all groups.
    static io::Task<size_t> invalidateRedisListGroups() {
        if constexpr (!kHasL2) {
            co_return 0;
        } else {
            try {
                if (!DbProvider::hasRedis()) co_return 0;

                auto masterKey = redisMasterSetKey();

                auto result = co_await DbProvider::redis("HKEYS", masterKey);
                if (result.isNil() || !result.isArray()) co_return 0;

                auto groups = result.asStringArray();
                size_t count = 0;

                for (const auto& group : groups) {
                    count += co_await cache::RedisCache::invalidateListGroup(group);
                }

                co_await DbProvider::redis("UNLINK", masterKey);

                co_return count;
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << Base::name()
                    << ": invalidateRedisListGroups error - " << e.what();
                co_return 0;
            }
        }
    }

    // =========================================================================
    // Cached list query implementation
    // =========================================================================

    static io::Task<ListResult> cachedListQuery(const ListQuery& query) {
        using Clock = std::chrono::steady_clock;

        // 1. L1 check — epoch-guarded view, zero-copy
        if constexpr (kHasL1) {
            if (auto cached = listCache().getByKey(query.cache_key))
                co_return std::move(cached);
        }

        auto start = Clock::now();

        // 2. L2 check — binary (BEVE) with auto header skip
        if constexpr (kHasL2) {
            auto pageKey = redisPageKey(query.cache_key);

            std::optional<ListWrapperType> cached;
            if constexpr (Base::config.l2_refresh_on_get) {
                cached = co_await cache::RedisCache::getListBinaryEx<ListWrapperType>(
                    pageKey, l2Ttl());
            } else {
                cached = co_await cache::RedisCache::getListBinary<ListWrapperType>(pageKey);
            }

            if (cached) {
                if constexpr (kHasL1) {
                    // Move into L1 cache, return epoch-guarded view
                    auto elapsed_us = static_cast<float>(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            Clock::now() - start).count());

                    auto sort = query.sort.value_or(defaultSortAsListSpec());
                    cache::list::SortBounds bounds;
                    if (!cached->items.empty()) {
                        bounds.first_value = cache::list::decl::extractSortValue<Descriptor>(
                            cached->items.front(), sort.field);
                        bounds.last_value = cache::list::decl::extractSortValue<Descriptor>(
                            cached->items.back(), sort.field);
                        bounds.is_valid = true;
                    }
                    co_return listCache().put(toCacheQuery(query), std::move(*cached),
                                              bounds, elapsed_us);
                } else {
                    co_return makeListView(std::move(*cached));
                }
            }
        }

        // 3. Cache miss — query database
        auto entities = co_await queryFromDb(query);
        auto sort = query.sort.value_or(defaultSortAsListSpec());

        auto elapsed_us = static_cast<float>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                Clock::now() - start).count());

        // Build ListWrapper directly from entities
        ListWrapperType wrapper;
        wrapper.items = std::move(entities);

        // Set cursor for pagination (base64 string in ListWrapper)
        if (wrapper.items.size() >= query.limit && !wrapper.items.empty()) {
            auto cache_sort = query.sort.value_or(Traits::defaultSort());
            auto cursor = Traits::extractCursor(wrapper.items.back(), cache_sort);
            wrapper.next_cursor = cursor.encode();
        }

        // Extract sort bounds from entities
        cache::list::SortBounds bounds;
        if (!wrapper.items.empty()) {
            bounds.first_value = cache::list::decl::extractSortValue<Descriptor>(
                wrapper.items.front(), sort.field);
            bounds.last_value = cache::list::decl::extractSortValue<Descriptor>(
                wrapper.items.back(), sort.field);
            bounds.is_valid = true;
        }

        // 4. Store in L2 with ListBoundsHeader (BEFORE moving into L1)
        if constexpr (kHasL2) {
            cache::list::ListBoundsHeader header;
            header.bounds = bounds;
            header.sort_direction = (sort.direction == cache::list::SortDirection::Desc)
                ? cache::list::SortDirection::Desc : cache::list::SortDirection::Asc;
            header.is_first_page = query.cursor.data.empty() && query.offset == 0;
            header.is_incomplete = wrapper.items.size() < static_cast<size_t>(query.limit);
            header.pagination_mode = query.cursor.data.empty()
                ? cache::list::PaginationMode::Offset
                : cache::list::PaginationMode::Cursor;

            auto pageKey = redisPageKey(query.cache_key);
            auto groupKey = redisGroupKey(query.group_key);

            // Store page binary with header prepended (reads wrapper, does not consume)
            co_await cache::RedisCache::setListBinary(pageKey, wrapper, l2Ttl(), header);
            // Track page in group SET
            co_await cache::RedisCache::trackListKey(groupKey, pageKey, l2Ttl());
            // Track group in master HASH (stores sort field index per group)
            co_await DbProvider::redis("HSET", redisMasterSetKey(), groupKey, std::to_string(sort.field));
        }

        // 5. Store in L1 cache or epoch pool
        if constexpr (kHasL1) {
            co_return listCache().put(toCacheQuery(query), std::move(wrapper),
                                      bounds, elapsed_us);
        } else {
            co_return makeListView(std::move(wrapper));
        }
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
            const bool is_desc = (sort.direction == cache::list::SortDirection::Desc);

            // Cursor keyset condition (page 2+ with cursor)
            if (!query.cursor.data.empty() && query.cursor.data.size() >= sizeof(int64_t) * 2) {
                int64_t cursor_sort_value = 0;
                int64_t cursor_id = 0;
                std::memcpy(&cursor_sort_value, query.cursor.data.data(),
                            sizeof(cursor_sort_value));
                std::memcpy(&cursor_id,
                            query.cursor.data.data() + sizeof(cursor_sort_value),
                            sizeof(cursor_id));

                if (!where.sql.empty()) where.sql += " AND ";
                where.sql += "(COALESCE(\"";
                where.sql += sort_col;
                where.sql += "\", 0), \"";
                where.sql += Mapping::primary_key_column;
                where.sql += "\") ";
                where.sql += is_desc ? "< " : "> ";
                where.sql += "($";
                where.sql += std::to_string(where.next_param++);
                where.sql += ", $";
                where.sql += std::to_string(where.next_param++);
                where.sql += ")";

                where.params.params.push_back(io::PgParam::bigint(cursor_sort_value));
                where.params.params.push_back(io::PgParam::bigint(cursor_id));
            }

            // Build SQL
            std::string sql;
            sql.reserve(256);
            sql += "SELECT * FROM ";
            sql += Mapping::table_name;
            if (!where.sql.empty()) {
                sql += " WHERE ";
                sql += where.sql;
            }
            sql += " ORDER BY COALESCE(\"";
            sql += sort_col;
            sql += "\", 0) ";
            sql += is_desc ? "DESC" : "ASC";
            sql += ", \"";
            sql += Mapping::primary_key_column;
            sql += "\" ";
            sql += is_desc ? "DESC" : "ASC";
            sql += " LIMIT ";
            sql += std::to_string(query.limit);

            // Offset pagination (mutually exclusive with cursor)
            if (query.offset > 0 && query.cursor.data.empty()) {
                sql += " OFFSET ";
                sql += std::to_string(query.offset);
            }

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
