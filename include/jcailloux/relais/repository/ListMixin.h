#ifndef JCX_RELAIS_LIST_MIXIN_H
#define JCX_RELAIS_LIST_MIXIN_H

#include <mutex>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"

#include "jcailloux/relais/PgProvider.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/cache/RedisCache.h"
#include "jcailloux/relais/list/ListCache.h"
#include "jcailloux/relais/list/ListQuery.h"
#include "jcailloux/relais/list/spec/ListDescriptor.h"
#include "jcailloux/relais/list/spec/ListDescriptorQuery.h"
#include "jcailloux/relais/list/spec/GeneratedFilters.h"
#include "jcailloux/relais/list/spec/GeneratedTraits.h"
#include "jcailloux/relais/list/spec/GeneratedCriteria.h"
#include "jcailloux/relais/list/spec/HttpQueryParser.h"
#include "jcailloux/relais/entity/EntityConcepts.h"
#include "jcailloux/relais/list/ListWrapper.h"
#include "jcailloux/relais/cache/Metrics.h"

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
 * Chain: [InvalidationMixin] -> ListMixin -> LocalRepo -> [RedisRepo] -> PgRepo
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

    using DescriptorFilters = list::spec::Filters<Descriptor>;
    using DescriptorSortSpec = list::spec::SortSpec<Descriptor>;

    // =========================================================================
    // Traits adapter — bridges Descriptor helpers to ListCache interface
    // =========================================================================

    struct Traits {
        using Filters = DescriptorFilters;
        using SortField = size_t;
        using FilterTags = list::spec::FilterTags<Descriptor>;

        static bool matchesFilters(const Entity& e, const Filters& f) {
            return list::spec::matchesFilters<Descriptor>(e, f);
        }

        static int compare(const Entity& a, const Entity& b,
                          SortField field_index, list::SortDirection dir) {
            return list::spec::compare<Descriptor>(a, b, {field_index, dir});
        }

        static list::Cursor extractCursor(const Entity& e,
                                                  const list::SortSpec<size_t>& sort) {
            return list::spec::extractCursor<Descriptor>(
                e, {sort.field, sort.direction});
        }

        static bool isBeforeOrAtCursor(const Entity& e,
                                       const list::Cursor& cursor,
                                       const list::SortSpec<size_t>& sort) {
            return list::spec::isBeforeOrAtCursor<Descriptor>(
                e, cursor, {sort.field, sort.direction});
        }

        static FilterTags extractTags(const Entity& e) {
            return list::spec::extractTags<Descriptor>(e);
        }

        static int64_t extractSortValue(const Entity& e, size_t field_index) {
            return list::spec::extractSortValue<Descriptor>(e, field_index);
        }

        static constexpr list::SortSpec<size_t> defaultSort() {
            auto ds = list::spec::defaultSort<Descriptor>();
            return {ds.field_index, ds.direction};
        }

        static std::optional<size_t> parseSortField(std::string_view field) {
            return list::spec::parseSortField<Descriptor>(field);
        }

        static std::string_view sortFieldName(size_t field_index) {
            return list::spec::sortFieldName<Descriptor>(field_index);
        }

        // Bucketed page sizes for cache key normalization. Sourced from the
        // descriptor's allowedLimits grid (the generator's single point of
        // truth, exposed under the `limitSteps` name at this Traits layer for
        // external consumers — see per-model-limits plan #5) when the model
        // declared a `limits=` annotation, else the shared kDefaultLimits
        // fallback. Arbitrary length — no fixed-size-4 assumption.
        static constexpr auto limitSteps = [] {
            if constexpr (list::spec::HasAllowedLimits<Descriptor>) {
                return Descriptor::allowedLimits;
            } else {
                return list::spec::kDefaultLimits;
            }
        }();
        static constexpr uint16_t maxLimit = limitSteps.back();

        static uint16_t normalizeLimit(uint16_t requested) {
            return list::spec::normalizeLimit<Descriptor>(requested);
        }
    };

    // =========================================================================
    // Cache infrastructure
    // =========================================================================

    static constexpr bool HasGDSF = cache::GDSFPolicy::enabled;

    using ListWrapperType = list::ListWrapper<Entity>;
    using ListCacheType = list::ListCache<Entity, Base::config.l1_chunk_count_log2, Key, Traits, HasGDSF>;

    static list::ListCacheConfig listCacheConfig() {
        return {
            .default_ttl_sec = static_cast<uint32_t>(
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::nanoseconds(Base::config.l1_ttl)).count()),
        };
    }

    static ListCacheType& listCache() {
        static ListCacheType instance(listCacheConfig());
        if constexpr (HasGDSF) {
            static std::once_flag gdsf_flag;
            std::call_once(gdsf_flag, []() {
                static const std::string list_name =
                    std::string(Base::name()) + ":list";
                instance.tier().enroll({
                    .sweep_fn = +[](long chunk_id) -> bool {
                        return listCache().sweep(chunk_id);
                    },
                    .size_fn = +[]() -> size_t {
                        return listCache().size();
                    },
                    .name = list_name.c_str()
                });
            });
        }
        return instance;
    }

    // L2 TTL helper
    static constexpr auto l2Ttl() { return std::chrono::nanoseconds(Base::config.l2_ttl); }

    // Epoch pool for list wrappers (non-L1 paths only)
    static epoch::memory_pool<ListWrapperType>& listPool() {
        static epoch::memory_pool<ListWrapperType> p;
        return p;
    }

    static cache::CacheView<ListWrapperType> makeListView(ListWrapperType&& w) {
        auto guard = epoch::EpochGuard::acquire();
        auto* ptr = listPool().New(std::move(w));
        listPool().Retire(ptr);
        return cache::CacheView<ListWrapperType>(ptr, std::move(guard));
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

    using CacheQuery = list::ListQuery<DescriptorFilters, size_t>;

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

    /// Convert spec::defaultSort → list::SortSpec<size_t>
    static list::SortSpec<size_t> defaultSortAsListSpec() {
        auto ds = list::spec::defaultSort<Descriptor>();
        return {ds.field_index, ds.direction};
    }

public:
#if RELAIS_ENABLE_METRICS
    static inline cache::L1Counters list_l1_counters_{};
    static inline cache::L2Counters list_l2_counters_{};
#endif

    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::FindResultType;
    using Base::name;
    using Base::find;

    /// Augmented descriptor — pass to parseListQueryStrict<ListDescriptorType>(req)
    using ListDescriptorType = Descriptor;

    /// List query type — compatible with parseListQueryStrict return type
    using ListQuery = list::spec::ListDescriptorQuery<Descriptor>;

    /// List result type — returned by query() (epoch-guarded, zero-copy)
    using ListResult = cache::CacheView<ListWrapperType>;

    /// Traits type — exposed for controllers (sort parsing, limit normalization, etc.)
    using ListTraits = Traits;

    // =========================================================================
    // Query interface
    // =========================================================================

    /// Execute a paginated list query with L1/L2 caching.
    /// L1 hit: zero overhead (Immediate holds ListResult directly, no Task).
    static io::Immediate<ListResult> query(const ListQuery& q) {
        if constexpr (kHasL1) {
            if (auto cached = listCache().getByKey(q.cache_key)) {
                RELAIS_METRICS_INC(list_l1_counters_.hits);
                return std::move(cached);
            }
            RELAIS_METRICS_INC(list_l1_counters_.misses);
        }
        return cachedListQuery(q);
    }

    /// Execute a paginated list query and return raw JSON string.
    /// L1 hit: serialize on demand from cached entities (Immediate, no Task).
    /// L2 hit (BEVE): transcodes via glz::beve_to_json (skips ListBoundsHeader).
    /// L2/DB miss: delegates to entity path (cachedListQuery).
    static io::Immediate<std::string> queryJson(const ListQuery& q) {
        // L1 check: serialize from cached entities
        if constexpr (kHasL1) {
            if (auto cached = listCache().getByKey(q.cache_key)) {
                RELAIS_METRICS_INC(list_l1_counters_.hits);
                return cached->json();
            }
            RELAIS_METRICS_INC(list_l1_counters_.misses);
        }
        return queryJsonSlow(q);
    }

    /// Execute a paginated list query and return raw binary (BEVE).
    /// L1 hit: serialize on demand from cached entities (Immediate, no Task).
    /// L2 hit: returns raw binary (skips ListBoundsHeader).
    /// L2/DB miss: delegates to entity path (cachedListQuery).
    static io::Immediate<std::vector<uint8_t>> queryBinary(const ListQuery& q)
        requires HasBinarySerialization<Entity>
    {
        // L1 check: serialize from cached entities
        if constexpr (kHasL1) {
            if (auto cached = listCache().getByKey(q.cache_key)) {
                RELAIS_METRICS_INC(list_l1_counters_.hits);
                return cached->binary();
            }
            RELAIS_METRICS_INC(list_l1_counters_.misses);
        }
        return queryBinarySlow(q);
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
    static io::Task<cache::CacheView<Entity>> insert(const Entity& entity)
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
        requires MutableEntity<Entity> && HasFullUpdate<Entity> && (!Base::config.read_only)
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
    static io::Task<cache::CacheView<Entity>> patch(const Key& id, Updates&&... updates)
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

    /// Sweep a specific chunk on both entity and list caches.
    /// Called by GDSFPolicy::sweep via sweep_fn — chunk_id is globally coordinated.
    static bool sweep(long chunk_id) {
        bool entity_cleaned = Base::sweep(chunk_id);
        if constexpr (kHasL1) {
            return entity_cleaned | listCache().sweep(chunk_id);
        }
        return entity_cleaned;
    }

    /// Purge all chunks on both entity and list caches.
    static size_t purge() {
        size_t entity_erased = Base::purge();
        if constexpr (kHasL1) {
            return entity_erased + listCache().purge();
        }
        return entity_erased;
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
        requires MutableEntity<Entity> && HasFullUpdate<Entity> && (!Base::config.read_only)
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
    static io::Task<cache::CacheView<Entity>> patchWithContext(
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
        }(std::make_index_sequence<list::spec::sort_count<Descriptor>>{});
        return result;
    }

    /// Selective L2 invalidation for entity creation (or deletion).
    static io::Task<size_t> invalidateL2Created(const Entity& entity) {
        auto masterKey = redisMasterSetKey();
        auto prefixLen = std::string(Base::name()).size() + 9; // ":dlist:g:"
        auto schema = list::spec::filterSchema<Descriptor>();
        auto blob = list::spec::encodeEntityFilterBlob<Descriptor>(entity);
        auto sortVals = buildSortValues(entity);

        co_return co_await cache::RedisCache::invalidateListGroupsSelective(
            masterKey, prefixLen, schema, blob, sortVals);
    }

    /// Selective L2 invalidation for entity update (old + new entities).
    static io::Task<size_t> invalidateL2Updated(const Entity& old_e, const Entity& new_e) {
        auto masterKey = redisMasterSetKey();
        auto prefixLen = std::string(Base::name()).size() + 9;
        auto schema = list::spec::filterSchema<Descriptor>();
        auto newBlob = list::spec::encodeEntityFilterBlob<Descriptor>(new_e);
        auto newSortVals = buildSortValues(new_e);
        auto oldBlob = list::spec::encodeEntityFilterBlob<Descriptor>(old_e);
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
                if (!PgProvider::hasRedis()) co_return 0;

                auto masterKey = redisMasterSetKey();

                auto result = co_await PgProvider::redis("HKEYS", masterKey);
                if (result.isNil() || !result.isArray()) co_return 0;

                auto groups = result.asStringArray();
                size_t count = 0;

                for (const auto& group : groups) {
                    count += co_await cache::RedisCache::invalidateListGroup(group);
                }

                co_await PgProvider::redis("UNLINK", masterKey);

                co_return count;
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << Base::name()
                    << ": invalidateRedisListGroups error - " << e.what();
                co_return 0;
            }
        }
    }

    // =========================================================================
    // Slow paths for queryJson / queryBinary (L1 miss → L2/DB)
    // =========================================================================

    /// Slow path for queryJson(): L1 miss → L2 transcode or DB fetch.
    static io::Task<std::string> queryJsonSlow(const ListQuery& q) {
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
                // Skip ListBoundsHeader if present (magic 0x53 0x52)
                size_t off = (beve->size() > list::kListBoundsHeaderSize
                    && (*beve)[0] == list::kListBoundsHeaderMagic[0]
                    && (*beve)[1] == list::kListBoundsHeaderMagic[1])
                    ? list::kListBoundsHeaderSize : 0;
                std::string json;
                if (!glz::beve_to_json(
                        std::span(beve->data() + off, beve->size() - off), json)) {
                    co_return json;
                }
            }
        }

        // Miss: entity path (needed for cursor/bounds/L1 population)
        auto wrapper = co_await cachedListQuery(q);
        if (!wrapper) co_return std::string{};
        co_return wrapper->json();
    }

    /// Slow path for queryBinary(): L1 miss → L2 or DB fetch.
    static io::Task<std::vector<uint8_t>> queryBinarySlow(const ListQuery& q)
        requires HasBinarySerialization<Entity>
    {
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
                // Skip ListBoundsHeader if present (magic 0x53 0x52)
                size_t off = (beve->size() > list::kListBoundsHeaderSize
                    && (*beve)[0] == list::kListBoundsHeaderMagic[0]
                    && (*beve)[1] == list::kListBoundsHeaderMagic[1])
                    ? list::kListBoundsHeaderSize : 0;
                co_return std::vector<uint8_t>(
                    beve->begin() + static_cast<ptrdiff_t>(off), beve->end());
            }
        }

        // Miss: entity path (needed for cursor/bounds/L1 population)
        auto wrapper = co_await cachedListQuery(q);
        if (!wrapper) co_return std::vector<uint8_t>{};
        co_return wrapper->binary();
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
                RELAIS_METRICS_INC(list_l2_counters_.hits);
                if constexpr (kHasL1) {
                    // Move into L1 cache, return epoch-guarded view
                    auto elapsed_us = static_cast<float>(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            Clock::now() - start).count());

                    auto sort = query.sort.value_or(defaultSortAsListSpec());
                    list::SortBounds bounds;
                    if (!cached->items.empty()) {
                        bounds.first_value = list::spec::extractSortValue<Descriptor>(
                            cached->items.front(), sort.field);
                        bounds.last_value = list::spec::extractSortValue<Descriptor>(
                            cached->items.back(), sort.field);
                        bounds.is_valid = true;
                    }
                    co_return listCache().put(toCacheQuery(query), std::move(*cached),
                                              bounds, elapsed_us);
                } else {
                    co_return makeListView(std::move(*cached));
                }
            }
            RELAIS_METRICS_INC(list_l2_counters_.misses);
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
        list::SortBounds bounds;
        if (!wrapper.items.empty()) {
            bounds.first_value = list::spec::extractSortValue<Descriptor>(
                wrapper.items.front(), sort.field);
            bounds.last_value = list::spec::extractSortValue<Descriptor>(
                wrapper.items.back(), sort.field);
            bounds.is_valid = true;
        }

        // 4. Store in L2 with ListBoundsHeader (BEFORE moving into L1)
        if constexpr (kHasL2) {
            list::ListBoundsHeader header;
            header.bounds = bounds;
            header.sort_direction = (sort.direction == list::SortDirection::Desc)
                ? list::SortDirection::Desc : list::SortDirection::Asc;
            header.is_first_page = query.cursor.data.empty() && query.offset == 0;
            header.is_incomplete = wrapper.items.size() < static_cast<size_t>(query.limit);
            header.pagination_mode = query.cursor.data.empty()
                ? list::PaginationMode::Offset
                : list::PaginationMode::Cursor;

            auto pageKey = redisPageKey(query.cache_key);
            auto groupKey = redisGroupKey(query.group_key);

            // Store page binary with header prepended (reads wrapper, does not consume)
            co_await cache::RedisCache::setListBinary(pageKey, wrapper, l2Ttl(), header);
            // Track page in group SET
            co_await cache::RedisCache::trackListKey(groupKey, pageKey, l2Ttl());
            // Track group in master HASH (stores sort field index per group)
            co_await PgProvider::redis("HSET", redisMasterSetKey(), groupKey, std::to_string(sort.field));
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
            auto where = list::spec::buildWhereClause<Descriptor>(query.filters);

            // Parse sort
            auto sort = query.sort.value_or(defaultSortAsListSpec());
            auto sort_col = list::spec::sortColumnName<Descriptor>(sort.field);
            const bool is_desc = (sort.direction == list::SortDirection::Desc);

            // Primary-key tiebreaker columns (one for a scalar key, N for a
            // composite key). Appends `, "col"[ DIR]` for each, matching the
            // key-component order encoded in the cursor.
            constexpr size_t kKeyN = list::spec::detail::keyComponentCount<Entity>;
            auto appendKeyColumns = [](std::string& s, const char* dir) {
                auto one = [&](const char* col) {
                    s += ", \"";
                    s += col;
                    s += "\"";
                    if (dir) { s += " "; s += dir; }
                };
                if constexpr (requires { Mapping::primary_key_columns; }) {
                    for (const char* col : Mapping::primary_key_columns) one(col);
                } else {
                    one(Mapping::primary_key_column);
                }
            };

            // Cursor keyset condition (page 2+ with cursor). The tiebreaker is
            // the full primary key — a PostgreSQL row-value comparison.
            if (!query.cursor.data.empty()
                && query.cursor.data.size() >= sizeof(int64_t) * (1 + kKeyN)) {
                int64_t cursor_sort_value = 0;
                std::memcpy(&cursor_sort_value, query.cursor.data.data(),
                            sizeof(cursor_sort_value));
                int64_t cursor_keys[kKeyN];
                for (size_t i = 0; i < kKeyN; ++i) {
                    std::memcpy(&cursor_keys[i],
                                query.cursor.data.data() + sizeof(int64_t) * (1 + i),
                                sizeof(int64_t));
                }

                if (!where.sql.empty()) where.sql += " AND ";
                where.sql += "(COALESCE(\"";
                where.sql += sort_col;
                where.sql += "\", 0)";
                appendKeyColumns(where.sql, nullptr);
                where.sql += ") ";
                where.sql += is_desc ? "< " : "> ";
                where.sql += "($";
                where.sql += std::to_string(where.next_param++);
                for (size_t i = 0; i < kKeyN; ++i) {
                    where.sql += ", $";
                    where.sql += std::to_string(where.next_param++);
                }
                where.sql += ")";

                where.params.params.push_back(io::PgParam::bigint(cursor_sort_value));
                for (size_t i = 0; i < kKeyN; ++i) {
                    where.params.params.push_back(io::PgParam::bigint(cursor_keys[i]));
                }
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
            appendKeyColumns(sql, is_desc ? "DESC" : "ASC");
            sql += " LIMIT ";
            sql += std::to_string(query.limit);

            // Offset pagination (mutually exclusive with cursor)
            if (query.offset > 0 && query.cursor.data.empty()) {
                sql += " OFFSET ";
                sql += std::to_string(query.offset);
            }

            // Execute
            auto result = co_await PgProvider::queryParams(sql.c_str(), where.params);

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
