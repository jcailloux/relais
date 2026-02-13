#ifndef JCX_DROGON_REDISCACHE_H
#define JCX_DROGON_REDISCACHE_H

#include <string>
#include <optional>
#include <vector>
#include <chrono>
#include <cstring>
#include <span>
#include <drogon/HttpAppFramework.h>
#include <drogon/nosql/RedisClient.h>
#include <drogon/utils/coroutine.h>
#include <trantor/utils/Logger.h>
#include <glaze/glaze.hpp>
#include <jcailloux/drogon/list/ListCache.h>

namespace jcailloux::drogon::cache {
    /**
     * Async Redis cache wrapper for L2 caching.
     * Entity must implement toJson() and fromJson().
     */
    class RedisCache {
        public:

            template<typename Entity>
            static ::drogon::Task<std::optional<Entity>> get(const std::string& key) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await redis->execCommandCoro("GET %s", key.c_str());
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    co_return Entity::fromJson(result.asString());
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache GET error: " << e.what();
                    co_return std::nullopt;
                }
            }

            template<typename Entity, typename Rep, typename Period>
            static ::drogon::Task<std::optional<Entity>> getEx(const std::string& key,
                                                              std::chrono::duration<Rep, Period> ttl) {
                if (auto json = co_await getRawEx(key, ttl)) {
                    co_return Entity::fromJson(*json);
                }
                co_return std::nullopt;
            }

            template<typename Entity, typename Rep, typename Period>
            static ::drogon::Task<bool> set(const std::string& key, const Entity& entity, std::chrono::duration<Rep, Period> ttl) {
                auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto json = entity.toJson();
                    co_await redis->execCommandCoro("SETEX %s %lld %s",
                                                    key.c_str(),
                                                    static_cast<long long>(ttl_seconds),
                                                    json->c_str());
                    co_return true;
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache SET error: " << e.what();
                    co_return false;
                }
            }

            template<typename Entity>
            static ::drogon::Task<std::optional<std::vector<Entity>>> getList(const std::string& key) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await redis->execCommandCoro("GET %s", key.c_str());
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    auto raw = result.asString();
                    co_return parseListWithHeader<Entity>(raw);
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache GET list error: " << e.what();
                    co_return std::nullopt;
                }
            }

            template<typename Entity, typename Rep, typename Period>
            static ::drogon::Task<std::optional<std::vector<Entity>>> getListEx(const std::string& key,
                                                                               std::chrono::duration<Rep, Period> ttl) {
                if (auto raw = co_await getRawEx(key, ttl)) {
                    co_return parseListWithHeader<Entity>(*raw);
                }
                co_return std::nullopt;
            }

            template<typename Entity, typename Rep, typename Period>
            static ::drogon::Task<bool> setList(const std::string& key,
                                               const std::vector<Entity>& entities,
                                               std::chrono::duration<Rep, Period> ttl,
                                               std::optional<list::ListBoundsHeader> header = std::nullopt) {
                auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto json = serializeList(entities);

                    if (header) {
                        // Prepend 19-byte binary header before JSON payload
                        std::string prefixed(list::kListBoundsHeaderSize + json.size(), '\0');
                        header->writeTo(reinterpret_cast<uint8_t*>(prefixed.data()));
                        std::memcpy(prefixed.data() + list::kListBoundsHeaderSize,
                                    json.data(), json.size());
                        co_await redis->execCommandCoro("SETEX %s %lld %b",
                                                        key.c_str(),
                                                        static_cast<long long>(ttl_seconds),
                                                        prefixed.data(),
                                                        prefixed.size());
                    } else {
                        co_await redis->execCommandCoro("SETEX %s %lld %s",
                                                        key.c_str(),
                                                        static_cast<long long>(ttl_seconds),
                                                        json.c_str());
                    }
                    co_return true;
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache SET list error: " << e.what();
                    co_return false;
                }
            }

            /// Get raw JSON string without deserialization.
            static ::drogon::Task<std::optional<std::string>> getRaw(const std::string& key) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return std::nullopt;
                }

                try {
                    const auto result = co_await redis->execCommandCoro("GET %s", key.c_str());
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    co_return result.asString();
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache getRaw error: " << e.what();
                    co_return std::nullopt;
                }
            }

            template<typename Rep, typename Period>
            static ::drogon::Task<std::optional<std::string>> getRawEx(const std::string& key,
                                                                      std::chrono::duration<Rep, Period> ttl) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return std::nullopt;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    const auto result = co_await redis->execCommandCoro("GETEX %s EX %lld",
                                                                         key.c_str(),
                                                                         static_cast<long long>(ttl_seconds));
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    co_return result.asString();
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache getRawEx error: " << e.what();
                    co_return std::nullopt;
                }
            }

            /// Store raw JSON string without serialization.
            template<typename Rep, typename Period>
            static ::drogon::Task<bool> setRaw(const std::string& key,
                                              const std::string_view json,
                                              std::chrono::duration<Rep, Period> ttl) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    co_await redis->execCommandCoro("SETEX %s %lld %b",
                                                    key.c_str(),
                                                    static_cast<long long>(ttl_seconds),
                                                    json.data(),
                                                    json.size());
                    co_return true;
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache setRaw error: " << e.what();
                    co_return false;
                }
            }

            static ::drogon::Task<std::optional<std::string>> getListRaw(const std::string& key) {
                co_return co_await getRaw(key);
            }

            template<typename Rep, typename Period>
            static ::drogon::Task<bool> setListRaw(const std::string& key,
                                                  std::string_view json,
                                                  std::chrono::duration<Rep, Period> ttl) {
                co_return co_await setRaw(key, json, ttl);
            }

            /// Get raw binary data (for BEVE or other binary formats).
            static ::drogon::Task<std::optional<std::vector<uint8_t>>> getRawBinary(const std::string& key) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return std::nullopt;
                }

                try {
                    const auto result = co_await redis->execCommandCoro("GET %s", key.c_str());
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    const auto& str = result.asString();
                    co_return std::vector<uint8_t>(str.begin(), str.end());
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache getRawBinary error: " << e.what();
                    co_return std::nullopt;
                }
            }

            /// Get raw binary data with TTL refresh (GETEX).
            template<typename Rep, typename Period>
            static ::drogon::Task<std::optional<std::vector<uint8_t>>> getRawBinaryEx(
                const std::string& key,
                std::chrono::duration<Rep, Period> ttl) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return std::nullopt;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    const auto result = co_await redis->execCommandCoro("GETEX %s EX %lld",
                                                                         key.c_str(),
                                                                         static_cast<long long>(ttl_seconds));
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    const auto& str = result.asString();
                    co_return std::vector<uint8_t>(str.begin(), str.end());
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache getRawBinaryEx error: " << e.what();
                    co_return std::nullopt;
                }
            }

            /// Store raw binary data.
            template<typename Rep, typename Period>
            static ::drogon::Task<bool> setRawBinary(const std::string& key,
                                                    const std::vector<uint8_t>& data,
                                                    std::chrono::duration<Rep, Period> ttl) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    co_await redis->execCommandCoro("SETEX %s %lld %b",
                                                    key.c_str(),
                                                    static_cast<long long>(ttl_seconds),
                                                    data.data(),
                                                    data.size());
                    co_return true;
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache setRawBinary error: " << e.what();
                    co_return false;
                }
            }

            // =================================================================
            // FlatBuffer List Entity Binary Methods
            // =================================================================
            // These methods work with list wrapper entities that support:
            // - static std::optional<ListEntity> fromBinary(std::span<const uint8_t>)
            // - std::vector<uint8_t> toBinary() const

            /// Get a FlatBuffer list entity from binary cache.
            /// Automatically skips the ListBoundsHeader if present (magic bytes 0x53 0x52).
            /// @tparam ListEntity Must have static fromBinary(span<const uint8_t>) method
            template<typename ListEntity>
            static ::drogon::Task<std::optional<ListEntity>> getListBinary(const std::string& key)
                requires requires(const std::vector<uint8_t>& data) {
                    { ListEntity::fromBinary(std::span<const uint8_t>(data)) } -> std::convertible_to<std::optional<ListEntity>>;
                }
            {
                auto data = co_await getRawBinary(key);
                if (data) {
                    std::span<const uint8_t> span(*data);
                    // Skip header if present
                    if (span.size() >= list::kListBoundsHeaderSize
                        && span[0] == list::kListBoundsHeaderMagic[0]
                        && span[1] == list::kListBoundsHeaderMagic[1]) {
                        span = span.subspan(list::kListBoundsHeaderSize);
                    }
                    co_return ListEntity::fromBinary(span);
                }
                co_return std::nullopt;
            }

            /// Get a FlatBuffer list entity with TTL refresh.
            /// Automatically skips the ListBoundsHeader if present.
            /// @tparam ListEntity Must have static fromBinary(span<const uint8_t>) method
            template<typename ListEntity, typename Rep, typename Period>
            static ::drogon::Task<std::optional<ListEntity>> getListBinaryEx(
                const std::string& key,
                std::chrono::duration<Rep, Period> ttl)
                requires requires(const std::vector<uint8_t>& data) {
                    { ListEntity::fromBinary(std::span<const uint8_t>(data)) } -> std::convertible_to<std::optional<ListEntity>>;
                }
            {
                auto data = co_await getRawBinaryEx(key, ttl);
                if (data) {
                    std::span<const uint8_t> span(*data);
                    if (span.size() >= list::kListBoundsHeaderSize
                        && span[0] == list::kListBoundsHeaderMagic[0]
                        && span[1] == list::kListBoundsHeaderMagic[1]) {
                        span = span.subspan(list::kListBoundsHeaderSize);
                    }
                    co_return ListEntity::fromBinary(span);
                }
                co_return std::nullopt;
            }

            /// Store a list entity as binary.
            /// Optionally prepends a ListBoundsHeader for fine-grained invalidation.
            /// @tparam ListEntity Must have toBinary() returning shared_ptr<const vector<uint8_t>>
            template<typename ListEntity, typename Rep, typename Period>
            static ::drogon::Task<bool> setListBinary(
                const std::string& key,
                const ListEntity& listEntity,
                std::chrono::duration<Rep, Period> ttl,
                std::optional<list::ListBoundsHeader> header = std::nullopt)
                requires requires(const ListEntity& l) {
                    { l.toBinary() } -> std::convertible_to<std::shared_ptr<const std::vector<uint8_t>>>;
                }
            {
                auto binary = listEntity.toBinary();
                if (header) {
                    std::vector<uint8_t> prefixed(list::kListBoundsHeaderSize + binary->size());
                    header->writeTo(prefixed.data());
                    std::memcpy(prefixed.data() + list::kListBoundsHeaderSize,
                                binary->data(), binary->size());
                    co_return co_await setRawBinary(key, prefixed, ttl);
                }
                co_return co_await setRawBinary(key, *binary, ttl);
            }

            /// Refresh TTL without modifying the value.
            template<typename Rep, typename Period>
            static ::drogon::Task<bool> expire(const std::string& key,
                                              std::chrono::duration<Rep, Period> ttl) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto result = co_await redis->execCommandCoro("EXPIRE %s %lld",
                                                                   key.c_str(),
                                                                   static_cast<long long>(ttl_seconds));
                    co_return result.asInteger() == 1;
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache EXPIRE error: " << e.what();
                    co_return false;
                }
            }

            static ::drogon::Task<bool> invalidate(const std::string& key) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return false;
                }

                try {
                    co_await redis->execCommandCoro("DEL %s", key.c_str());
                    co_return true;
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache DEL error: " << e.what();
                    co_return false;
                }
            }

            /// Invalidate keys matching a pattern using SCAN (non-blocking).
            /// Safer than invalidatePattern() for production use.
            /// @param pattern Redis glob pattern (e.g., "MyCache:list:user:123:*")
            /// @param batch_size Number of keys to scan per iteration (default 100)
            static ::drogon::Task<size_t> invalidatePatternSafe(const std::string& pattern,
                                                               size_t batch_size = 100) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return 0;
                }

                try {
                    size_t count = 0;
                    std::string cursor = "0";

                    do {
                        // Collect keys from SCAN result before issuing DEL commands,
                        // because co_await DEL may invalidate references to the SCAN result.
                        std::vector<std::string> batch_keys;

                        auto result = co_await redis->execCommandCoro(
                            "SCAN %s MATCH %s COUNT %lld",
                            cursor.c_str(), pattern.c_str(), static_cast<long long>(batch_size));

                        if (result.isNil() || result.type() != ::drogon::nosql::RedisResultType::kArray) {
                            break;
                        }

                        const auto& arr = result.asArray();
                        if (arr.size() < 2) break;

                        cursor = arr[0].asString();
                        const auto& keys = arr[1].asArray();

                        for (const auto& keyResult : keys) {
                            if (keyResult.type() == ::drogon::nosql::RedisResultType::kNil ||
                                keyResult.type() == ::drogon::nosql::RedisResultType::kArray) {
                                continue;
                            }
                            try {
                                auto key = keyResult.asString();
                                if (!key.empty()) {
                                    batch_keys.push_back(std::move(key));
                                }
                            } catch (...) {}
                        }

                        for (const auto& key : batch_keys) {
                            try {
                                co_await redis->execCommandCoro("DEL %s", key.c_str());
                                ++count;
                            } catch (...) {}
                        }
                    } while (cursor != "0");

                    co_return count;
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache invalidatePatternSafe error: " << e.what();
                    co_return 0;
                }
            }

            // =================================================================
            // List Group Tracking - O(M) invalidation instead of O(N) KEYS scan
            // =================================================================

            /// Track a list cache key in its group's tracking set.
            /// Call this when caching a list to enable efficient group invalidation.
            /// @param groupKey The logical group (e.g., "MyRepo:list:user:123")
            /// @param listKey The actual cache key (e.g., "MyRepo:list:user:123:limit:20:offset:0")
            template<typename Rep, typename Period>
            static ::drogon::Task<bool> trackListKey(const std::string& groupKey,
                                                    const std::string& listKey,
                                                    std::chrono::duration<Rep, Period> ttl) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return false;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();

                    // Add to tracking set (binary-safe value)
                    co_await redis->execCommandCoro("SADD %s %b",
                        trackingKey.c_str(), listKey.c_str(), listKey.size());

                    // Set TTL on tracking set only if none exists (NX = don't renew)
                    co_await redis->execCommandCoro("EXPIRE %s %lld NX",
                        trackingKey.c_str(), static_cast<long long>(ttl_seconds));

                    co_return true;
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache trackListKey error: " << e.what();
                    co_return false;
                }
            }

            /// Invalidate all list cache keys in a group.
            /// O(M) where M is the number of cached pages (typically small).
            /// @param groupKey The logical group (e.g., "MyRepo:list:user:123")
            /// @return Number of keys invalidated
            static ::drogon::Task<size_t> invalidateListGroup(const std::string& groupKey) {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return 0;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";

                    // Atomic Lua script: get all tracked keys, delete them, delete the set.
                    // This avoids element type parsing issues with SMEMBERS in some drogon/Redis versions.
                    static constexpr std::string_view lua = R"(
                        local keys = redis.call('SMEMBERS', KEYS[1])
                        local count = 0
                        for _, key in ipairs(keys) do
                            redis.call('DEL', key)
                            count = count + 1
                        end
                        redis.call('DEL', KEYS[1])
                        return count
                    )";

                    auto result = co_await redis->execCommandCoro(
                        "EVAL %s 1 %s",
                        std::string(lua).c_str(),
                        trackingKey.c_str());

                    co_return result.isNil() ? 0 : static_cast<size_t>(result.asInteger());
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache invalidateListGroup error: " << e.what();
                    co_return 0;
                }
            }

            // =================================================================
            // Selective List Group Invalidation (Lua-based, 1 round-trip)
            // =================================================================

            /// Selectively invalidate list pages in a group based on a single sort value.
            /// Used for create/delete operations.
            ///
            /// For each page in the tracking set, the Lua script reads the 19-byte header
            /// via GETRANGE and applies the appropriate invalidation logic:
            /// - Offset mode (cascade): invalidates pages whose range includes entity_val and all after
            /// - Cursor mode (localized): only invalidates pages whose range contains entity_val
            /// - No header (backward compat): always invalidates (conservative)
            ///
            /// @param groupKey The logical group (e.g., "MyRepo:list:category:tech")
            /// @param entity_sort_val The sort value of the created/deleted entity
            /// @return Number of pages invalidated
            static ::drogon::Task<size_t> invalidateListGroupSelective(
                const std::string& groupKey,
                int64_t entity_sort_val)
            {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return 0;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";

                    // Lua script: iterate pages, read header, decide whether to delete.
                    // KEYS[1] = trackingKey
                    // ARGV[1] = entity_sort_val (as string, converted via tonumber)
                    // ARGV[2] = header_size (19)
                    static constexpr std::string_view lua = R"(
local keys = redis.call('SMEMBERS', KEYS[1])
local entity_val = tonumber(ARGV[1])
local hdr_size = tonumber(ARGV[2])
local count = 0

local function read_int64(s, off)
    local b1,b2,b3,b4,b5,b6,b7,b8 = string.byte(s, off+1, off+8)
    local val = b1 + b2*256 + b3*65536 + b4*16777216
              + b5*4294967296 + b6*1099511627776
              + b7*281474976710656 + b8*72057594037927936
    if val >= 2^63 then val = val - 2^64 end
    return val
end

for _, page_key in ipairs(keys) do
    local hdr = redis.call('GETRANGE', page_key, 0, hdr_size - 1)
    local should_del = true

    if #hdr >= hdr_size and string.byte(hdr, 1) == 0x53 and string.byte(hdr, 2) == 0x52 then
        local first = read_int64(hdr, 2)
        local last  = read_int64(hdr, 10)
        local flags = string.byte(hdr, 19)
        local is_desc       = (flags % 2) == 1
        local is_first_page = (math.floor(flags / 2) % 2) == 1
        local is_incomplete = (math.floor(flags / 4) % 2) == 1
        local is_offset     = (math.floor(flags / 8) % 2) == 0

        if is_offset then
            if is_incomplete then
                should_del = true
            elseif is_desc then
                should_del = (entity_val >= last)
            else
                should_del = (entity_val <= last)
            end
        else
            if is_first_page and is_incomplete then
                should_del = true
            elseif is_desc then
                if is_first_page then
                    should_del = (entity_val >= last)
                elseif is_incomplete then
                    should_del = (entity_val <= first)
                else
                    should_del = (entity_val <= first and entity_val >= last)
                end
            else
                if is_first_page then
                    should_del = (entity_val <= last)
                elseif is_incomplete then
                    should_del = (entity_val >= first)
                else
                    should_del = (entity_val >= first and entity_val <= last)
                end
            end
        end
    end

    if should_del then
        redis.call('DEL', page_key)
        redis.call('SREM', KEYS[1], page_key)
        count = count + 1
    end
end

if count == #keys then redis.call('DEL', KEYS[1]) end
return count
)";

                    auto result = co_await redis->execCommandCoro(
                        "EVAL %s 1 %s %lld %d",
                        std::string(lua).c_str(),
                        trackingKey.c_str(),
                        static_cast<long long>(entity_sort_val),
                        static_cast<int>(list::kListBoundsHeaderSize));

                    co_return result.isNil() ? 0 : static_cast<size_t>(result.asInteger());
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache invalidateListGroupSelective error: " << e.what();
                    co_return 0;
                }
            }

            /// Selectively invalidate list pages in a group based on old and new sort values.
            /// Used for update operations where the entity's sort value changed.
            ///
            /// - Offset mode: uses interval overlap [page_min, page_max] ∩ [min(old,new), max(old,new)]
            /// - Cursor mode: checks if old OR new value falls in the page range
            /// - No header: always invalidates (conservative)
            ///
            /// @param groupKey The logical group
            /// @param old_sort_val Sort value before the update
            /// @param new_sort_val Sort value after the update
            /// @return Number of pages invalidated
            static ::drogon::Task<size_t> invalidateListGroupSelectiveUpdate(
                const std::string& groupKey,
                int64_t old_sort_val,
                int64_t new_sort_val)
            {
                const auto redis = ::drogon::app().getRedisClient();
                if (!redis) {
                    co_return 0;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";

                    // KEYS[1] = trackingKey
                    // ARGV[1] = old_sort_val, ARGV[2] = new_sort_val, ARGV[3] = header_size
                    static constexpr std::string_view lua = R"(
local keys = redis.call('SMEMBERS', KEYS[1])
local old_val = tonumber(ARGV[1])
local new_val = tonumber(ARGV[2])
local hdr_size = tonumber(ARGV[3])
local range_min = math.min(old_val, new_val)
local range_max = math.max(old_val, new_val)
local count = 0

local function read_int64(s, off)
    local b1,b2,b3,b4,b5,b6,b7,b8 = string.byte(s, off+1, off+8)
    local val = b1 + b2*256 + b3*65536 + b4*16777216
              + b5*4294967296 + b6*1099511627776
              + b7*281474976710656 + b8*72057594037927936
    if val >= 2^63 then val = val - 2^64 end
    return val
end

for _, page_key in ipairs(keys) do
    local hdr = redis.call('GETRANGE', page_key, 0, hdr_size - 1)
    local should_del = true

    if #hdr >= hdr_size and string.byte(hdr, 1) == 0x53 and string.byte(hdr, 2) == 0x52 then
        local first = read_int64(hdr, 2)
        local last  = read_int64(hdr, 10)
        local flags = string.byte(hdr, 19)
        local is_desc       = (flags % 2) == 1
        local is_first_page = (math.floor(flags / 2) % 2) == 1
        local is_incomplete = (math.floor(flags / 4) % 2) == 1
        local is_offset     = (math.floor(flags / 8) % 2) == 0

        if is_offset then
            local page_min = is_desc and last or first
            local page_max = is_desc and first or last
            if is_incomplete then
                should_del = (page_min <= range_max)
            else
                should_del = (page_min <= range_max) and (range_min <= page_max)
            end
        else
            local function in_range(val)
                if is_first_page and is_incomplete then return true end
                if is_desc then
                    if is_first_page then return val >= last end
                    if is_incomplete then return val <= first end
                    return val <= first and val >= last
                else
                    if is_first_page then return val <= last end
                    if is_incomplete then return val >= first end
                    return val >= first and val <= last
                end
            end
            should_del = in_range(old_val) or in_range(new_val)
        end
    end

    if should_del then
        redis.call('DEL', page_key)
        redis.call('SREM', KEYS[1], page_key)
        count = count + 1
    end
end

if count == #keys then redis.call('DEL', KEYS[1]) end
return count
)";

                    auto result = co_await redis->execCommandCoro(
                        "EVAL %s 1 %s %lld %lld %d",
                        std::string(lua).c_str(),
                        trackingKey.c_str(),
                        static_cast<long long>(old_sort_val),
                        static_cast<long long>(new_sort_val),
                        static_cast<int>(list::kListBoundsHeaderSize));

                    co_return result.isNil() ? 0 : static_cast<size_t>(result.asInteger());
                } catch (const std::exception& e) {
                    LOG_WARN << "RedisCache invalidateListGroupSelectiveUpdate error: " << e.what();
                    co_return 0;
                }
            }

        private:
            template<typename Entity>
            static std::string serializeList(const std::vector<Entity>& entities) {
                std::string buffer;
                if (glz::write_json(entities, buffer)) {
                    return "[]";
                }
                return buffer;
            }

            template<typename Entity>
            static std::optional<std::vector<Entity>> parseList(const std::string_view json) {
                std::vector<Entity> result;
                if (auto ec = glz::read_json(result, json)) {
                    LOG_WARN << "RedisCache parseList error: " << glz::format_error(ec, json);
                    return std::nullopt;
                }
                return result;
            }

            /// Parse a list value that may be prefixed with a ListBoundsHeader.
            /// Detects magic bytes and skips the header if present.
            template<typename Entity>
            static std::optional<std::vector<Entity>> parseListWithHeader(const std::string_view raw) {
                std::string_view data = raw;
                if (data.size() >= list::kListBoundsHeaderSize
                    && static_cast<uint8_t>(data[0]) == list::kListBoundsHeaderMagic[0]
                    && static_cast<uint8_t>(data[1]) == list::kListBoundsHeaderMagic[1]) {
                    data.remove_prefix(list::kListBoundsHeaderSize);
                }
                return parseList<Entity>(data);
            }
    };

} // namespace jcailloux::drogon::cache

#endif //JCX_DROGON_REDISCACHE_H
