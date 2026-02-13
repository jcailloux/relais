#ifndef JCX_DROGON_REDISREPOSITORY_H
#define JCX_DROGON_REDISREPOSITORY_H

#include "jcailloux/relais/repository/BaseRepository.h"
#include "jcailloux/relais/cache/RedisCache.h"
#include "jcailloux/relais/config/repository_config.h"

namespace jcailloux::relais {

/**
 * Repository with L2 Redis caching on top of L3 database.
 *
 * Automatically selects binary or JSON serialization based on Entity capabilities:
 * - Binary (BEVE/FlatBuffer entities): stored and served as binary
 * - JSON (JSON entities): stored and served as JSON
 *
 * The entity's native serialization format is always used -- no cross-format conversion.
 *
 * Cross-invalidation is not handled here; it belongs in InvalidationMixin.
 */
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires CacheableEntity<Entity, typename Entity::Model>
class RedisRepository : public BaseRepository<Entity, Name, Cfg, Key> {
    using Base = BaseRepository<Entity, Name, Cfg, Key>;
    using Model = typename Base::ModelType;

    public:
        using typename Base::EntityType;
        using typename Base::ModelType;
        using typename Base::KeyType;
        using typename Base::WrapperType;
        using typename Base::WrapperPtrType;
        using Base::name;

        static constexpr auto l2Ttl() { return std::chrono::nanoseconds(Cfg.l2_ttl); }

        /// Find by ID with L2 (Redis) -> L3 (DB) fallback.
        /// Returns shared_ptr to immutable entity (nullptr if not found).
        static ::drogon::Task<WrapperPtrType> findById(const Key& id) {
            auto redisKey = makeRedisKey(id);

            std::optional<Entity> cached = co_await getFromCache(redisKey);
            if (cached) {
                co_return std::make_shared<const Entity>(std::move(*cached));
            }

            auto ptr = co_await Base::findById(id);
            if (ptr) {
                co_await setInCache(redisKey, *ptr);
            }
            co_return ptr;
        }

        /// Find by ID and return raw JSON string.
        /// Returns shared_ptr to JSON string (nullptr if not found).
        static ::drogon::Task<std::shared_ptr<const std::string>> findByIdAsJson(const Key& id) {
            auto redisKey = makeRedisKey(id);

            std::optional<std::string> cached;
            if constexpr (Cfg.l2_refresh_on_get) {
                cached = co_await cache::RedisCache::getRawEx(redisKey, l2Ttl());
            } else {
                cached = co_await cache::RedisCache::getRaw(redisKey);
            }

            if (cached) {
                co_return std::make_shared<const std::string>(std::move(*cached));
            }

            if (auto ptr = co_await Base::findById(id)) {
                auto json = ptr->toJson();
                if (json) {
                    co_await cache::RedisCache::setRaw(redisKey, *json, l2Ttl());
                }
                co_return json;
            }
            co_return nullptr;
        }

        /// Create entity in database with L2 cache population.
        /// Returns shared_ptr to immutable entity (nullptr on error).
        /// Compile-time error if read_only is true.
        static ::drogon::Task<WrapperPtrType> create(WrapperPtrType wrapper)
            requires CreatableEntity<Entity, Model, Key> && (!Cfg.read_only)
        {
            auto inserted = co_await Base::create(wrapper);
            if (inserted) {
                // Populate L2 cache with the new entity
                co_await setInCache(makeRedisKey(inserted->getPrimaryKey()), *inserted);
            }
            co_return inserted;
        }

        /// Update entity in database with L2 cache handling.
        /// Returns true on success, false on error.
        /// Compile-time error if read_only is true.
        static ::drogon::Task<bool> update(const Key& id, WrapperPtrType wrapper)
            requires MutableEntity<Entity, Model> && (!Cfg.read_only)
        {
            using enum config::UpdateStrategy;

            bool success = co_await Base::update(id, wrapper);
            if (success) {
                // Update L2 cache based on configured strategy
                if constexpr (Cfg.update_strategy == InvalidateAndLazyReload) {
                    co_await invalidateRedis(id);
                } else {
                    co_await setInCache(makeRedisKey(id), *wrapper);
                }
            }
            co_return success;
        }

        /// Partial update: invalidates Redis then delegates to Base::updateBy.
        /// Returns the re-fetched entity (nullptr on error or not found).
        template<typename... Updates>
        static ::drogon::Task<WrapperPtrType> updateBy(const Key& id, Updates&&... updates)
            requires HasFieldUpdate<Entity> && (!Cfg.read_only)
        {
            co_await invalidateRedis(id);
            co_return co_await Base::updateBy(id, std::forward<Updates>(updates)...);
        }

        /// Remove entity by ID.
        /// Returns: rows deleted (0 if not found), or nullopt on DB error.
        /// Invalidates Redis cache unless DB error occurred.
        /// Compile-time error if read_only is true.
        static ::drogon::Task<std::optional<size_t>> remove(const Key& id)
            requires (!Cfg.read_only)
        {
            co_return co_await removeImpl(id, nullptr);
        }

    protected:
        /// Internal remove with optional entity hint for PartialKey optimization.
        /// For PartialKey repos, tries L2 cache if no L1 hint was provided.
        static ::drogon::Task<std::optional<size_t>> removeImpl(
            const Key& id, typename Base::WrapperPtrType cachedHint = nullptr)
            requires (!Cfg.read_only)
        {
            // For PartialKey: try L2 if no L1 hint (~0.1-1ms)
            if constexpr (!std::is_same_v<Key, typename Model::PrimaryKeyType>) {
                if (!cachedHint) {
                    auto redisKey = makeRedisKey(id);
                    auto cached = co_await getFromCache(redisKey);
                    if (cached) {
                        cachedHint = std::make_shared<const Entity>(std::move(*cached));
                    }
                }
            }

            auto result = co_await Base::removeImpl(id, std::move(cachedHint));
            if (result.has_value()) {
                co_await invalidateRedis(id);
            }
            co_return result;
        }

    public:

        /// Invalidate Redis cache for a key and return void.
        /// Used as cross-invalidation target interface.
        static ::drogon::Task<void> invalidate(const Key& id) {
            co_await invalidateRedis(id);
        }

        /// Invalidate Redis cache for a key.
        /// @param id The entity key to invalidate
        /// @return true if cache was invalidated successfully
        static ::drogon::Task<bool> invalidateRedis(const Key& id) {
            co_return co_await cache::RedisCache::invalidate(makeRedisKey(id));
        }

        static std::string makeRedisKey(const Key& id) {
            if constexpr (std::is_integral_v<Key>) {
                return std::string(name()) + ":" + std::to_string(id);
            } else {
                return std::string(name()) + ":" + std::string(id);
            }
        }

        /// Build a group key from key parts.
        /// Public wrapper for use by InvalidateListVia resolvers.
        template<typename... GroupArgs>
        static std::string makeGroupKey(GroupArgs&&... groupParts) {
            return makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
        }

        /// Selectively invalidate list pages for a pre-built group key.
        /// Used by InvalidateListVia for cross-invalidation through enriched resolvers.
        static ::drogon::Task<size_t> invalidateListGroupByKey(
            const std::string& groupKey, int64_t entity_sort_val)
        {
            co_return co_await cache::RedisCache::invalidateListGroupSelective(
                groupKey, entity_sort_val);
        }

        /// Invalidate all list cache groups for this repository.
        /// Uses SCAN with pattern "name:list:*" to find and delete all list keys.
        /// Used by InvalidateListVia when resolver returns nullopt (full pattern).
        static ::drogon::Task<size_t> invalidateAllListGroups()
        {
            std::string pattern = std::string(name()) + ":list:*";
            co_return co_await cache::RedisCache::invalidatePatternSafe(pattern);
        }

    protected:
        // =====================================================================
        // Serialization-aware cache helpers
        // =====================================================================

        /// Get entity from cache using its native serialization format.
        static ::drogon::Task<std::optional<Entity>> getFromCache(const std::string& key) {
            if constexpr (HasBinarySerialization<Entity>) {
                std::optional<std::vector<uint8_t>> data;
                if constexpr (Cfg.l2_refresh_on_get) {
                    data = co_await cache::RedisCache::getRawBinaryEx(key, l2Ttl());
                } else {
                    data = co_await cache::RedisCache::getRawBinary(key);
                }
                if (data) {
                    co_return Entity::fromBinary(std::span<const uint8_t>(*data));
                }
                co_return std::nullopt;
            } else {
                // JSON mode (default)
                if constexpr (Cfg.l2_refresh_on_get) {
                    co_return co_await cache::RedisCache::getEx<Entity>(key, l2Ttl());
                } else {
                    co_return co_await cache::RedisCache::get<Entity>(key);
                }
            }
        }

        /// Set entity in cache using its native serialization format.
        static ::drogon::Task<bool> setInCache(const std::string& key, const Entity& entity) {
            if constexpr (HasBinarySerialization<Entity>) {
                co_return co_await cache::RedisCache::setRawBinary(key, *entity.toBinary(), l2Ttl());
            } else {
                // JSON mode (default)
                co_return co_await cache::RedisCache::set(key, entity, l2Ttl());
            }
        }

        template<typename E = Entity>
        static ::drogon::Task<std::optional<std::vector<E>>> getListFromRedis(const std::string& key)
            requires HasJsonSerialization<E>
        {
            co_return co_await cache::RedisCache::getList<E>(key);
        }

        template<typename E = Entity, typename Rep, typename Period>
        static ::drogon::Task<bool> setListInRedis(const std::string& key,
                                                  const std::vector<E>& entities,
                                                  std::chrono::duration<Rep, Period> ttl)
            requires HasJsonSerialization<E>
        {
            co_return co_await cache::RedisCache::setList(key, entities, ttl);
        }

        template<typename... Args>
        static std::string makeListCacheKey(Args&&... args) {
            std::string key = std::string(name()) + ":list";
            ((key += ":" + Base::toString(std::forward<Args>(args))), ...);
            return key;
        }

        /// Execute a list query with Redis caching.
        template<typename QueryFn, typename... KeyArgs>
        static ::drogon::Task<std::vector<Entity>> cachedList(QueryFn&& query, KeyArgs&&... keyParts) {
            auto cacheKey = makeListCacheKey(std::forward<KeyArgs>(keyParts)...);

            std::optional<std::vector<Entity>> cached;
            if constexpr (Cfg.l2_refresh_on_get) {
                cached = co_await cache::RedisCache::getListEx<Entity>(cacheKey, l2Ttl());
            } else {
                cached = co_await getListFromRedis(cacheKey);
            }

            if (cached) {
                co_return std::move(*cached);
            }

            auto results = co_await query();
            co_await setListInRedis(cacheKey, results, l2Ttl());

            co_return results;
        }

        // =====================================================================
        // Tracked list caching - O(M) invalidation instead of O(N) KEYS scan
        // =====================================================================

        /// Build a group key for list tracking (without pagination params).
        template<typename... GroupArgs>
        static std::string makeListGroupKey(GroupArgs&&... groupParts) {
            std::string key = std::string(name()) + ":list";
            ((key += ":" + Base::toString(std::forward<GroupArgs>(groupParts))), ...);
            return key;
        }

        /// Execute a list query with Redis caching and group tracking.
        /// Unlike cachedList(), this tracks cache keys for efficient O(M) invalidation.
        /// @param query The database query function
        /// @param limit Pagination limit
        /// @param offset Pagination offset
        /// @param groupParts Key parts that identify the group (e.g., "user", user_id)
        template<typename QueryFn, typename... GroupArgs>
        static ::drogon::Task<std::vector<Entity>> cachedListTracked(
            QueryFn&& query,
            int limit,
            int offset,
            GroupArgs&&... groupParts)
        {
            co_return co_await cachedListTrackedWithHeader(
                std::forward<QueryFn>(query), limit, offset, nullptr,
                std::forward<GroupArgs>(groupParts)...);
        }

        /// Execute a list query with Redis caching, group tracking, and sort bounds header.
        /// The headerBuilder lambda constructs a ListBoundsHeader from the query results,
        /// enabling fine-grained Lua-based invalidation instead of full group invalidation.
        ///
        /// @param query The database query function
        /// @param limit Pagination limit
        /// @param offset Pagination offset
        /// @param headerBuilder Lambda: (const vector<Entity>&, int limit, int offset)
        ///                      -> optional<ListBoundsHeader>. Pass nullptr for no header.
        /// @param groupParts Key parts that identify the group
        template<typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
        static ::drogon::Task<std::vector<Entity>> cachedListTrackedWithHeader(
            QueryFn&& query,
            int limit,
            int offset,
            HeaderBuilder&& headerBuilder,
            GroupArgs&&... groupParts)
        {
            // Build group key (for tracking) and full cache key (with pagination)
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            std::string cacheKey = groupKey + ":limit:" + std::to_string(limit)
                                            + ":offset:" + std::to_string(offset);

            // Try to get from cache
            std::optional<std::vector<Entity>> cached;
            if constexpr (Cfg.l2_refresh_on_get) {
                cached = co_await cache::RedisCache::getListEx<Entity>(cacheKey, l2Ttl());
            } else {
                cached = co_await getListFromRedis(cacheKey);
            }

            if (cached) {
                co_return std::move(*cached);
            }

            // Cache miss: execute query
            auto results = co_await query();

            // Build header if headerBuilder is provided (not nullptr)
            std::optional<cache::list::ListBoundsHeader> header;
            if constexpr (!std::is_null_pointer_v<std::decay_t<HeaderBuilder>>) {
                header = headerBuilder(results, limit, offset);
            }

            // Store in cache (with optional header) and track the key
            co_await cache::RedisCache::setList(cacheKey, results, l2Ttl(), header);
            co_await cache::RedisCache::trackListKey(groupKey, cacheKey, l2Ttl());

            co_return results;
        }

        /// Invalidate all cached list pages for a group (full invalidation).
        /// Use for manual/bulk invalidation. For entity CRUD, prefer selective overloads.
        /// O(M) where M is the number of cached pages (typically small).
        template<typename... GroupArgs>
        static ::drogon::Task<size_t> invalidateListGroup(GroupArgs&&... groupParts) {
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            co_return co_await cache::RedisCache::invalidateListGroup(groupKey);
        }

        /// Selectively invalidate list pages for a group based on a sort value.
        /// Only pages whose sort range is affected by the create/delete are invalidated.
        /// @param entity_sort_val Sort value of the created/deleted entity
        /// @param groupParts Key parts that identify the group
        template<typename... GroupArgs>
        static ::drogon::Task<size_t> invalidateListGroupSelective(
            int64_t entity_sort_val,
            GroupArgs&&... groupParts)
        {
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            co_return co_await cache::RedisCache::invalidateListGroupSelective(
                groupKey, entity_sort_val);
        }

        /// Selectively invalidate list pages for a group based on old and new sort values.
        /// Uses interval overlap for offset mode, localized check for cursor mode.
        /// @param old_sort_val Sort value before the update
        /// @param new_sort_val Sort value after the update
        /// @param groupParts Key parts that identify the group
        template<typename... GroupArgs>
        static ::drogon::Task<size_t> invalidateListGroupSelectiveUpdate(
            int64_t old_sort_val,
            int64_t new_sort_val,
            GroupArgs&&... groupParts)
        {
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            co_return co_await cache::RedisCache::invalidateListGroupSelectiveUpdate(
                groupKey, old_sort_val, new_sort_val);
        }

        // =====================================================================
        // Binary List Caching - cachedListAs<ListEntity>()
        // =====================================================================

        /// Execute a list query and cache the result as a binary list entity.
        ///
        /// @tparam ListEntity The list wrapper type (e.g., ListWrapper<User>).
        ///         Must support fromBinary, toBinary, and fromModels.
        ///
        /// @param query Database query function returning vector<ListEntity::Model>
        /// @param keyParts Cache key components
        template<typename ListEntity, typename QueryFn, typename... KeyArgs>
        static ::drogon::Task<ListEntity> cachedListAs(
            QueryFn&& query,
            KeyArgs&&... keyParts)
        {
            auto cacheKey = makeListCacheKey(std::forward<KeyArgs>(keyParts)...);

            // Try L2 cache (binary)
            std::optional<ListEntity> cached;
            if constexpr (Cfg.l2_refresh_on_get) {
                cached = co_await cache::RedisCache::getListBinaryEx<ListEntity>(cacheKey, l2Ttl());
            } else {
                cached = co_await cache::RedisCache::getListBinary<ListEntity>(cacheKey);
            }

            if (cached) {
                co_return std::move(*cached);
            }

            // Cache miss: query DB and build list entity
            auto models = co_await query();
            auto listEntity = ListEntity::fromModels(models);

            // Store in L2 (binary)
            co_await cache::RedisCache::setListBinary(cacheKey, listEntity, l2Ttl());

            co_return listEntity;
        }

        /// Execute a list query with group tracking, returning a binary list entity.
        /// Combines cachedListAs with group tracking for efficient O(M) invalidation.
        template<typename ListEntity, typename QueryFn, typename... GroupArgs>
        static ::drogon::Task<ListEntity> cachedListAsTracked(
            QueryFn&& query,
            int limit,
            int offset,
            GroupArgs&&... groupParts)
        {
            co_return co_await cachedListAsTrackedWithHeader<ListEntity>(
                std::forward<QueryFn>(query), limit, offset, nullptr,
                std::forward<GroupArgs>(groupParts)...);
        }

        /// Execute a list query with group tracking + sort bounds header, returning a binary list entity.
        /// The headerBuilder enables fine-grained Lua-based invalidation.
        ///
        /// @param headerBuilder Lambda: (const ListEntity&, int limit, int offset)
        ///                      -> optional<ListBoundsHeader>. Pass nullptr for no header.
        template<typename ListEntity, typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
        static ::drogon::Task<ListEntity> cachedListAsTrackedWithHeader(
            QueryFn&& query,
            int limit,
            int offset,
            HeaderBuilder&& headerBuilder,
            GroupArgs&&... groupParts)
        {
            // Build group key (for tracking) and full cache key (with pagination)
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            std::string cacheKey = groupKey + ":limit:" + std::to_string(limit)
                                            + ":offset:" + std::to_string(offset);

            // Try L2 cache (binary)
            std::optional<ListEntity> cached;
            if constexpr (Cfg.l2_refresh_on_get) {
                cached = co_await cache::RedisCache::getListBinaryEx<ListEntity>(cacheKey, l2Ttl());
            } else {
                cached = co_await cache::RedisCache::getListBinary<ListEntity>(cacheKey);
            }

            if (cached) {
                co_return std::move(*cached);
            }

            // Cache miss: query DB and build list entity
            auto models = co_await query();
            auto listEntity = ListEntity::fromModels(models);

            // Build header if headerBuilder is provided
            std::optional<cache::list::ListBoundsHeader> header;
            if constexpr (!std::is_null_pointer_v<std::decay_t<HeaderBuilder>>) {
                header = headerBuilder(listEntity, limit, offset);
            }

            // Store in L2 (binary, with optional header) and track the key
            co_await cache::RedisCache::setListBinary(cacheKey, listEntity, l2Ttl(), header);
            co_await cache::RedisCache::trackListKey(groupKey, cacheKey, l2Ttl());

            co_return listEntity;
        }
};

}  // namespace jcailloux::relais

#endif //JCX_DROGON_REDISREPOSITORY_H
