#ifndef JCX_RELAIS_REDISREPO_H
#define JCX_RELAIS_REDISREPO_H

#include "jcailloux/relais/repository/BaseRepo.h"
#include "jcailloux/relais/cache/RedisCache.h"
#include "jcailloux/relais/config/repo_config.h"

namespace jcailloux::relais {

/**
 * Repo with L2 Redis caching on top of L3 database.
 *
 * Serialization format is controlled by CacheConfig::l2_format:
 * - Binary (default): uses BEVE for entities that support HasBinarySerialization
 * - Json: always uses JSON (useful for interop with non-C++ consumers)
 *
 * When l2_format is Binary but the entity lacks HasBinarySerialization,
 * JSON is used as an automatic fallback.
 *
 * Cross-invalidation is not handled here; it belongs in InvalidationMixin.
 */
template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires CacheableEntity<Entity>
class RedisRepo : public BaseRepo<Entity, Name, Cfg, Key> {
    using Base = BaseRepo<Entity, Name, Cfg, Key>;
    using Mapping = typename Entity::MappingType;

    static constexpr bool useL2Binary =
        (Cfg.l2_format == config::L2Format::Binary) && HasBinarySerialization<Entity>;

    public:
        using typename Base::EntityType;
        using typename Base::KeyType;
        using typename Base::WrapperType;
        using typename Base::WrapperPtrType;
        using Base::name;

        static constexpr auto l2Ttl() { return std::chrono::nanoseconds(Cfg.l2_ttl); }

        /// Find by ID with L2 (Redis) -> L3 (DB) fallback.
        /// Returns shared_ptr to immutable entity (nullptr if not found).
        static io::Task<WrapperPtrType> find(const Key& id) {
            auto redisKey = makeRedisKey(id);

            std::optional<Entity> cached = co_await getFromCache(redisKey);
            if (cached) {
                co_return std::make_shared<const Entity>(std::move(*cached));
            }

            auto ptr = co_await Base::find(id);
            if (ptr) {
                co_await setInCache(redisKey, *ptr);
            }
            co_return ptr;
        }

        /// Find by ID and return raw JSON string.
        /// Returns shared_ptr to JSON string (nullptr if not found).
        /// When L2 stores BEVE, transcodes directly via glz::beve_to_json (no entity construction).
        static io::Task<std::shared_ptr<const std::string>> findJson(const Key& id) {
            if constexpr (useL2Binary) {
                auto redisKey = makeRedisKey(id);

                std::optional<std::vector<uint8_t>> beve;
                if constexpr (Cfg.l2_refresh_on_get) {
                    beve = co_await cache::RedisCache::getRawBinaryEx(redisKey, l2Ttl());
                } else {
                    beve = co_await cache::RedisCache::getRawBinary(redisKey);
                }

                if (beve) {
                    auto json = std::make_shared<std::string>();
                    if (!glz::beve_to_json(*beve, *json)) {
                        co_return json;
                    }
                }

                // Cache miss: fetch entity, populate Redis, serialize to JSON
                auto ptr = co_await find(id);
                co_return ptr ? ptr->json() : nullptr;
            } else {
                auto ptr = co_await find(id);
                co_return ptr ? ptr->json() : nullptr;
            }
        }

        /// Find by ID and return raw binary.
        /// Returns shared_ptr to binary data (nullptr if not found).
        /// When l2_format is Binary, reads raw bytes directly from Redis (zero-copy).
        /// When l2_format is Json, fetches the entity and serializes to binary on the fly.
        static io::Task<std::shared_ptr<const std::vector<uint8_t>>> findBinary(const Key& id)
            requires HasBinarySerialization<Entity>
        {
            if constexpr (useL2Binary) {
                auto redisKey = makeRedisKey(id);

                std::optional<std::vector<uint8_t>> cached;
                if constexpr (Cfg.l2_refresh_on_get) {
                    cached = co_await cache::RedisCache::getRawBinaryEx(redisKey, l2Ttl());
                } else {
                    cached = co_await cache::RedisCache::getRawBinary(redisKey);
                }

                if (cached) {
                    co_return std::make_shared<const std::vector<uint8_t>>(std::move(*cached));
                }

                if (auto ptr = co_await Base::find(id)) {
                    auto bin = ptr->binary();
                    if (bin) {
                        co_await cache::RedisCache::setRawBinary(redisKey, *bin, l2Ttl());
                    }
                    co_return bin;
                }
                co_return nullptr;
            } else {
                auto ptr = co_await find(id);
                co_return ptr ? ptr->binary() : nullptr;
            }
        }

        /// insert entity in database with L2 cache population.
        /// Returns shared_ptr to immutable entity (nullptr on error).
        static io::Task<WrapperPtrType> insert(WrapperPtrType wrapper)
            requires CreatableEntity<Entity, Key> && (!Cfg.read_only)
        {
            auto inserted = co_await Base::insert(wrapper);
            if (inserted) {
                co_await setInCache(makeRedisKey(inserted->key()), *inserted);
            }
            co_return inserted;
        }

        /// Update entity in database with L2 cache handling.
        /// Returns true on success, false on error.
        static io::Task<bool> update(const Key& id, WrapperPtrType wrapper)
            requires MutableEntity<Entity> && (!Cfg.read_only)
        {
            using enum config::UpdateStrategy;

            bool success = co_await Base::update(id, wrapper);
            if (success) {
                if constexpr (Cfg.update_strategy == InvalidateAndLazyReload) {
                    co_await evictRedis(id);
                } else {
                    co_await setInCache(makeRedisKey(id), *wrapper);
                }
            }
            co_return success;
        }

        /// Partial update: invalidates Redis then delegates to Base::patch.
        /// Returns the re-fetched entity (nullptr on error or not found).
        template<typename... Updates>
        static io::Task<WrapperPtrType> patch(const Key& id, Updates&&... updates)
            requires HasFieldUpdate<Entity> && (!Cfg.read_only)
        {
            co_await evictRedis(id);
            co_return co_await Base::patch(id, std::forward<Updates>(updates)...);
        }

        /// Erase entity by ID.
        /// Returns: rows deleted (0 if not found), or nullopt on DB error.
        /// Invalidates Redis cache unless DB error occurred.
        static io::Task<std::optional<size_t>> erase(const Key& id)
            requires (!Cfg.read_only)
        {
            co_return co_await eraseImpl(id, nullptr);
        }

    protected:
        /// Internal erase with optional entity hint.
        /// For CompositeKey entities: if L1 didn't provide a hint,
        /// try L2 (Redis) as a near-free fallback (~0.1-1ms).
        static io::Task<std::optional<size_t>> eraseImpl(
            const Key& id, typename Base::WrapperPtrType cachedHint = nullptr)
            requires (!Cfg.read_only)
        {
            // L2 hint fallback for partition pruning
            if constexpr (HasPartitionHint<Entity>) {
                if (!cachedHint) {
                    auto cached = co_await getFromCache(makeRedisKey(id));
                    if (cached) {
                        cachedHint = std::make_shared<const Entity>(std::move(*cached));
                    }
                }
            }

            auto result = co_await Base::eraseImpl(id, std::move(cachedHint));
            if (result.has_value()) {
                co_await evictRedis(id);
            }
            co_return result;
        }

    public:

        /// Invalidate Redis cache for a key and return void.
        /// Used as cross-invalidation target interface.
        static io::Task<void> invalidate(const Key& id) {
            co_await evictRedis(id);
        }

        /// Invalidate Redis cache for a key.
        static io::Task<bool> evictRedis(const Key& id) {
            co_return co_await cache::RedisCache::invalidate(makeRedisKey(id));
        }

        static std::string makeRedisKey(const Key& id) {
            if constexpr (config::is_tuple_v<Key>) {
                std::string key = std::string(name());
                std::apply([&](const auto&... parts) {
                    ((key += ":" + keyPartToString(parts)), ...);
                }, id);
                return key;
            } else if constexpr (std::is_integral_v<Key>) {
                return std::string(name()) + ":" + std::to_string(id);
            } else {
                return std::string(name()) + ":" + std::string(id);
            }
        }

    private:
        template<typename T>
        static std::string keyPartToString(const T& v) {
            if constexpr (std::is_integral_v<T>) {
                return std::to_string(v);
            } else {
                return std::string(v);
            }
        }

    public:

        /// Build a group key from key parts.
        template<typename... GroupArgs>
        static std::string makeGroupKey(GroupArgs&&... groupParts) {
            return makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
        }

        /// Selectively invalidate list pages for a pre-built group key.
        static io::Task<size_t> invalidateListGroupByKey(
            const std::string& groupKey, int64_t entity_sort_val)
        {
            co_return co_await cache::RedisCache::invalidateListGroupSelective(
                groupKey, entity_sort_val);
        }

        /// Invalidate all list cache groups for this repository.
        static io::Task<size_t> invalidateAllListGroups()
        {
            std::string pattern = std::string(name()) + ":list:*";
            co_return co_await cache::RedisCache::invalidatePatternSafe(pattern);
        }

    protected:
        // =====================================================================
        // Serialization-aware cache helpers
        // =====================================================================

        /// Get entity from cache using the configured serialization format.
        static io::Task<std::optional<Entity>> getFromCache(const std::string& key) {
            if constexpr (useL2Binary) {
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

        /// Set entity in cache using the configured serialization format.
        static io::Task<bool> setInCache(const std::string& key, const Entity& entity) {
            if constexpr (useL2Binary) {
                co_return co_await cache::RedisCache::setRawBinary(key, *entity.binary(), l2Ttl());
            } else {
                co_return co_await cache::RedisCache::set(key, entity, l2Ttl());
            }
        }

        template<typename E = Entity>
        static io::Task<std::optional<std::vector<E>>> getListFromRedis(const std::string& key) {
            if constexpr (useL2Binary) {
                co_return co_await cache::RedisCache::getListBeve<E>(key);
            } else {
                co_return co_await cache::RedisCache::getList<E>(key);
            }
        }

        template<typename E = Entity>
        static io::Task<std::optional<std::vector<E>>> getListFromRedisEx(const std::string& key) {
            if constexpr (useL2Binary) {
                co_return co_await cache::RedisCache::getListBeveEx<E>(key, l2Ttl());
            } else {
                co_return co_await cache::RedisCache::getListEx<E>(key, l2Ttl());
            }
        }

        template<typename E = Entity, typename Rep, typename Period>
        static io::Task<bool> setListInRedis(const std::string& key,
                                                  const std::vector<E>& entities,
                                                  std::chrono::duration<Rep, Period> ttl,
                                                  std::optional<cache::list::ListBoundsHeader> header = std::nullopt)
        {
            if constexpr (useL2Binary) {
                co_return co_await cache::RedisCache::setListBeve(key, entities, ttl, header);
            } else {
                co_return co_await cache::RedisCache::setList(key, entities, ttl, header);
            }
        }

        template<typename... Args>
        static std::string makeListCacheKey(Args&&... args) {
            std::string key = std::string(name()) + ":list";
            ((key += ":" + Base::toString(std::forward<Args>(args))), ...);
            return key;
        }

        /// Execute a list query with Redis caching.
        template<typename QueryFn, typename... KeyArgs>
        static io::Task<std::vector<Entity>> cachedList(QueryFn&& query, KeyArgs&&... keyParts) {
            auto cacheKey = makeListCacheKey(std::forward<KeyArgs>(keyParts)...);

            std::optional<std::vector<Entity>> cached;
            if constexpr (Cfg.l2_refresh_on_get) {
                cached = co_await getListFromRedisEx(cacheKey);
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
        template<typename QueryFn, typename... GroupArgs>
        static io::Task<std::vector<Entity>> cachedListTracked(
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
        template<typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
        static io::Task<std::vector<Entity>> cachedListTrackedWithHeader(
            QueryFn&& query,
            int limit,
            int offset,
            HeaderBuilder&& headerBuilder,
            GroupArgs&&... groupParts)
        {
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            std::string cacheKey = groupKey + ":limit:" + std::to_string(limit)
                                            + ":offset:" + std::to_string(offset);

            std::optional<std::vector<Entity>> cached;
            if constexpr (Cfg.l2_refresh_on_get) {
                cached = co_await getListFromRedisEx(cacheKey);
            } else {
                cached = co_await getListFromRedis(cacheKey);
            }

            if (cached) {
                co_return std::move(*cached);
            }

            auto results = co_await query();

            std::optional<cache::list::ListBoundsHeader> header;
            if constexpr (!std::is_null_pointer_v<std::decay_t<HeaderBuilder>>) {
                header = headerBuilder(results, limit, offset);
            }

            co_await setListInRedis(cacheKey, results, l2Ttl(), header);
            co_await cache::RedisCache::trackListKey(groupKey, cacheKey, l2Ttl());

            co_return results;
        }

        /// Invalidate all cached list pages for a group.
        template<typename... GroupArgs>
        static io::Task<size_t> invalidateListGroup(GroupArgs&&... groupParts) {
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            co_return co_await cache::RedisCache::invalidateListGroup(groupKey);
        }

        /// Selectively invalidate list pages for a group based on a sort value.
        template<typename... GroupArgs>
        static io::Task<size_t> invalidateListGroupSelective(
            int64_t entity_sort_val,
            GroupArgs&&... groupParts)
        {
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            co_return co_await cache::RedisCache::invalidateListGroupSelective(
                groupKey, entity_sort_val);
        }

        /// Selectively invalidate list pages for a group based on old and new sort values.
        template<typename... GroupArgs>
        static io::Task<size_t> invalidateListGroupSelectiveUpdate(
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
        template<typename ListEntity, typename QueryFn, typename... KeyArgs>
        static io::Task<ListEntity> cachedListAs(
            QueryFn&& query,
            KeyArgs&&... keyParts)
        {
            auto cacheKey = makeListCacheKey(std::forward<KeyArgs>(keyParts)...);

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
            auto listEntity = co_await query();

            // Store in L2 (binary)
            co_await cache::RedisCache::setListBinary(cacheKey, listEntity, l2Ttl());

            co_return listEntity;
        }

        /// Execute a list query with group tracking, returning a binary list entity.
        template<typename ListEntity, typename QueryFn, typename... GroupArgs>
        static io::Task<ListEntity> cachedListAsTracked(
            QueryFn&& query,
            int limit,
            int offset,
            GroupArgs&&... groupParts)
        {
            co_return co_await cachedListAsTrackedWithHeader<ListEntity>(
                std::forward<QueryFn>(query), limit, offset, nullptr,
                std::forward<GroupArgs>(groupParts)...);
        }

        /// Execute a list query with group tracking + sort bounds header.
        template<typename ListEntity, typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
        static io::Task<ListEntity> cachedListAsTrackedWithHeader(
            QueryFn&& query,
            int limit,
            int offset,
            HeaderBuilder&& headerBuilder,
            GroupArgs&&... groupParts)
        {
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            std::string cacheKey = groupKey + ":limit:" + std::to_string(limit)
                                            + ":offset:" + std::to_string(offset);

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
            auto listEntity = co_await query();

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

#endif //JCX_RELAIS_REDISREPO_H
