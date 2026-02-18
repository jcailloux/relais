#ifndef JCX_RELAIS_REDISCACHE_H
#define JCX_RELAIS_REDISCACHE_H

#include <string>
#include <optional>
#include <vector>
#include <chrono>
#include <cstring>
#include <span>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/redis/RedisResult.h"
#include "jcailloux/relais/DbProvider.h"
#include "jcailloux/relais/Log.h"

#include <glaze/glaze.hpp>
#include <jcailloux/relais/list/ListCache.h>

namespace jcailloux::relais::cache {

    /**
     * Async Redis cache wrapper for L2 caching.
     * Entity must implement json() and fromJson().
     *
     * All Redis operations go through DbProvider::redis() which wraps
     * io::RedisClient via type-erased std::function.
     */
    class RedisCache {
        public:

            template<typename Entity>
            static io::Task<std::optional<Entity>> get(const std::string& key) {
                if (!DbProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await DbProvider::redis("GET", key);
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    co_return Entity::fromJson(result.asStringView());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache GET error: " << e.what();
                    co_return std::nullopt;
                }
            }

            template<typename Entity, typename Rep, typename Period>
            static io::Task<std::optional<Entity>> getEx(const std::string& key,
                                                              std::chrono::duration<Rep, Period> ttl) {
                if (auto json = co_await getRawEx(key, ttl)) {
                    co_return Entity::fromJson(*json);
                }
                co_return std::nullopt;
            }

            template<typename Entity, typename Rep, typename Period>
            static io::Task<bool> set(const std::string& key, const Entity& entity, std::chrono::duration<Rep, Period> ttl) {
                if (!DbProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto json = entity.json();
                    co_await DbProvider::redis("SETEX", key,
                        ttl_seconds, *json);
                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache SET error: " << e.what();
                    co_return false;
                }
            }

            template<typename Entity>
            static io::Task<std::optional<std::vector<Entity>>> getList(const std::string& key) {
                if (!DbProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await DbProvider::redis("GET", key);
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    auto raw = result.asStringView();
                    co_return parseListWithHeader<Entity>(raw);
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache GET list error: " << e.what();
                    co_return std::nullopt;
                }
            }

            template<typename Entity, typename Rep, typename Period>
            static io::Task<std::optional<std::vector<Entity>>> getListEx(const std::string& key,
                                                                               std::chrono::duration<Rep, Period> ttl) {
                if (auto raw = co_await getRawEx(key, ttl)) {
                    co_return parseListWithHeader<Entity>(*raw);
                }
                co_return std::nullopt;
            }

            template<typename Entity, typename Rep, typename Period>
            static io::Task<bool> setList(const std::string& key,
                                               const std::vector<Entity>& entities,
                                               std::chrono::duration<Rep, Period> ttl,
                                               std::optional<list::ListBoundsHeader> header = std::nullopt) {
                if (!DbProvider::hasRedis()) {
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
                        co_await DbProvider::redis("SETEX", key,
                            ttl_seconds,
                            std::string_view(prefixed.data(), prefixed.size()));
                    } else {
                        co_await DbProvider::redis("SETEX", key,
                            ttl_seconds, json);
                    }
                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache SET list error: " << e.what();
                    co_return false;
                }
            }

            // =================================================================
            // List BEVE Methods (vector<Entity> via glz::write_beve/read_beve)
            // =================================================================

            template<typename Entity>
            static io::Task<std::optional<std::vector<Entity>>> getListBeve(const std::string& key) {
                auto data = co_await getRawBinary(key);
                if (!data) co_return std::nullopt;
                co_return parseListBeveWithHeader<Entity>(*data);
            }

            template<typename Entity, typename Rep, typename Period>
            static io::Task<std::optional<std::vector<Entity>>> getListBeveEx(
                const std::string& key,
                std::chrono::duration<Rep, Period> ttl)
            {
                auto data = co_await getRawBinaryEx(key, ttl);
                if (!data) co_return std::nullopt;
                co_return parseListBeveWithHeader<Entity>(*data);
            }

            template<typename Entity, typename Rep, typename Period>
            static io::Task<bool> setListBeve(const std::string& key,
                                                   const std::vector<Entity>& entities,
                                                   std::chrono::duration<Rep, Period> ttl,
                                                   std::optional<list::ListBoundsHeader> header = std::nullopt) {
                auto beve = serializeListBeve(entities);
                if (beve.empty() && !entities.empty()) co_return false;

                if (header) {
                    std::vector<uint8_t> prefixed(list::kListBoundsHeaderSize + beve.size());
                    header->writeTo(prefixed.data());
                    std::memcpy(prefixed.data() + list::kListBoundsHeaderSize,
                                beve.data(), beve.size());
                    co_return co_await setRawBinary(key, prefixed, ttl);
                } else {
                    co_return co_await setRawBinary(key, beve, ttl);
                }
            }

            /// Get raw JSON string without deserialization.
            static io::Task<std::optional<std::string>> getRaw(const std::string& key) {
                if (!DbProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await DbProvider::redis("GET", key);
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    co_return result.asString();
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache getRaw error: " << e.what();
                    co_return std::nullopt;
                }
            }

            template<typename Rep, typename Period>
            static io::Task<std::optional<std::string>> getRawEx(const std::string& key,
                                                                      std::chrono::duration<Rep, Period> ttl) {
                if (!DbProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto result = co_await DbProvider::redis("GETEX", key,
                        "EX", ttl_seconds);
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    co_return result.asString();
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache getRawEx error: " << e.what();
                    co_return std::nullopt;
                }
            }

            /// Store raw JSON string without serialization.
            template<typename Rep, typename Period>
            static io::Task<bool> setRaw(const std::string& key,
                                              const std::string_view json,
                                              std::chrono::duration<Rep, Period> ttl) {
                if (!DbProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    co_await DbProvider::redis("SETEX", key,
                        ttl_seconds,
                        std::string_view(json.data(), json.size()));
                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache setRaw error: " << e.what();
                    co_return false;
                }
            }

            static io::Task<std::optional<std::string>> getListRaw(const std::string& key) {
                co_return co_await getRaw(key);
            }

            template<typename Rep, typename Period>
            static io::Task<bool> setListRaw(const std::string& key,
                                                  std::string_view json,
                                                  std::chrono::duration<Rep, Period> ttl) {
                co_return co_await setRaw(key, json, ttl);
            }

            /// Get raw binary data (for BEVE or other binary formats).
            static io::Task<std::optional<std::vector<uint8_t>>> getRawBinary(const std::string& key) {
                if (!DbProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await DbProvider::redis("GET", key);
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    auto sv = result.asStringView();
                    co_return std::vector<uint8_t>(
                        reinterpret_cast<const uint8_t*>(sv.data()),
                        reinterpret_cast<const uint8_t*>(sv.data()) + sv.size());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache getRawBinary error: " << e.what();
                    co_return std::nullopt;
                }
            }

            /// Get raw binary data with TTL refresh (GETEX).
            template<typename Rep, typename Period>
            static io::Task<std::optional<std::vector<uint8_t>>> getRawBinaryEx(
                const std::string& key,
                std::chrono::duration<Rep, Period> ttl) {
                if (!DbProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto result = co_await DbProvider::redis("GETEX", key,
                        "EX", ttl_seconds);
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    auto sv = result.asStringView();
                    co_return std::vector<uint8_t>(
                        reinterpret_cast<const uint8_t*>(sv.data()),
                        reinterpret_cast<const uint8_t*>(sv.data()) + sv.size());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache getRawBinaryEx error: " << e.what();
                    co_return std::nullopt;
                }
            }

            /// Store raw binary data.
            template<typename Rep, typename Period>
            static io::Task<bool> setRawBinary(const std::string& key,
                                                    const std::vector<uint8_t>& data,
                                                    std::chrono::duration<Rep, Period> ttl) {
                if (!DbProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    co_await DbProvider::redis("SETEX", key,
                        ttl_seconds,
                        std::string_view(reinterpret_cast<const char*>(data.data()), data.size()));
                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache setRawBinary error: " << e.what();
                    co_return false;
                }
            }

            // =================================================================
            // List Entity Binary Methods
            // =================================================================

            /// Get a list entity from binary cache.
            /// Automatically skips the ListBoundsHeader if present (magic bytes 0x53 0x52).
            template<typename ListEntity>
            static io::Task<std::optional<ListEntity>> getListBinary(const std::string& key)
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

            /// Get a list entity with TTL refresh.
            template<typename ListEntity, typename Rep, typename Period>
            static io::Task<std::optional<ListEntity>> getListBinaryEx(
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
            template<typename ListEntity, typename Rep, typename Period>
            static io::Task<bool> setListBinary(
                const std::string& key,
                const ListEntity& listEntity,
                std::chrono::duration<Rep, Period> ttl,
                std::optional<list::ListBoundsHeader> header = std::nullopt)
                requires requires(const ListEntity& l) {
                    { l.binary() } -> std::convertible_to<std::shared_ptr<const std::vector<uint8_t>>>;
                }
            {
                auto binary = listEntity.binary();
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
            static io::Task<bool> expire(const std::string& key,
                                              std::chrono::duration<Rep, Period> ttl) {
                if (!DbProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto result = co_await DbProvider::redis("EXPIRE", key,
                        ttl_seconds);
                    co_return result.asInteger() == 1;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache EXPIRE error: " << e.what();
                    co_return false;
                }
            }

            static io::Task<bool> invalidate(const std::string& key) {
                if (!DbProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    co_await DbProvider::redis("UNLINK", key);
                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache UNLINK error: " << e.what();
                    co_return false;
                }
            }

            /// Invalidate keys matching a pattern using SCAN (non-blocking).
            /// Safer than KEYS for production use.
            static io::Task<size_t> invalidatePatternSafe(const std::string& pattern,
                                                               size_t batch_size = 100) {
                if (!DbProvider::hasRedis()) {
                    co_return 0;
                }

                try {
                    size_t count = 0;
                    std::string cursor = "0";

                    do {
                        std::vector<std::string> batch_keys;

                        auto result = co_await DbProvider::redis(
                            "SCAN", cursor, "MATCH", pattern, "COUNT", batch_size);

                        if (result.isNil() || !result.isArray()) {
                            break;
                        }

                        if (result.arraySize() < 2) break;

                        cursor = result.at(0).asString();
                        auto keysResult = result.at(1);

                        for (size_t i = 0; i < keysResult.arraySize(); ++i) {
                            auto elem = keysResult.at(i);
                            if (elem.isNil() || elem.isArray()) {
                                continue;
                            }
                            auto keyStr = elem.asString();
                            if (!keyStr.empty()) {
                                batch_keys.push_back(std::move(keyStr));
                            }
                        }

                        for (const auto& k : batch_keys) {
                            try {
                                co_await DbProvider::redis("UNLINK", k);
                                ++count;
                            } catch (...) {}
                        }
                    } while (cursor != "0");

                    co_return count;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache invalidatePatternSafe error: " << e.what();
                    co_return 0;
                }
            }

            // =================================================================
            // List Group Tracking - O(M) invalidation instead of O(N) KEYS scan
            // =================================================================

            /// Track a list cache key in its group's tracking set.
            template<typename Rep, typename Period>
            static io::Task<bool> trackListKey(const std::string& groupKey,
                                                    const std::string& listKey,
                                                    std::chrono::duration<Rep, Period> ttl) {
                if (!DbProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();

                    // Add to tracking set (binary-safe via argvlen)
                    co_await DbProvider::redis("SADD", trackingKey, listKey);

                    // Set TTL on tracking set only if none exists (NX = don't renew)
                    co_await DbProvider::redis("EXPIRE", trackingKey,
                        ttl_seconds, "NX");

                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache trackListKey error: " << e.what();
                    co_return false;
                }
            }

            /// Invalidate all list cache keys in a group.
            /// O(M) where M is the number of cached pages (typically small).
            static io::Task<size_t> invalidateListGroup(const std::string& groupKey) {
                if (!DbProvider::hasRedis()) {
                    co_return 0;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";

                    // Atomic Lua script: get all tracked keys, delete them, delete the set.
                    static constexpr std::string_view lua = R"(
                        local keys = redis.call('SMEMBERS', KEYS[1])
                        local count = 0
                        for _, key in ipairs(keys) do
                            redis.call('UNLINK', key)
                            count = count + 1
                        end
                        redis.call('UNLINK', KEYS[1])
                        return count
                    )";

                    auto result = co_await DbProvider::redis(
                        "EVAL", lua, "1", trackingKey);

                    co_return result.isNil() ? 0 : static_cast<size_t>(result.asInteger());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache invalidateListGroup error: " << e.what();
                    co_return 0;
                }
            }

            // =================================================================
            // Selective List Group Invalidation (Lua-based, 1 round-trip)
            // =================================================================

            /// Selectively invalidate list pages in a group based on a single sort value.
            static io::Task<size_t> invalidateListGroupSelective(
                const std::string& groupKey,
                int64_t entity_sort_val)
            {
                if (!DbProvider::hasRedis()) {
                    co_return 0;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";

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

if count == #keys then redis.call('UNLINK', KEYS[1]) end
return count
)";

                    auto result = co_await DbProvider::redis(
                        "EVAL", lua, "1", trackingKey,
                        entity_sort_val,
                        static_cast<int>(list::kListBoundsHeaderSize));

                    co_return result.isNil() ? 0 : static_cast<size_t>(result.asInteger());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache invalidateListGroupSelective error: " << e.what();
                    co_return 0;
                }
            }

            /// Selectively invalidate list pages in a group based on old and new sort values.
            /// Used for update operations where the entity's sort value changed.
            static io::Task<size_t> invalidateListGroupSelectiveUpdate(
                const std::string& groupKey,
                int64_t old_sort_val,
                int64_t new_sort_val)
            {
                if (!DbProvider::hasRedis()) {
                    co_return 0;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";

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

if count == #keys then redis.call('UNLINK', KEYS[1]) end
return count
)";

                    auto result = co_await DbProvider::redis(
                        "EVAL", lua, "1", trackingKey,
                        old_sort_val, new_sort_val,
                        static_cast<int>(list::kListBoundsHeaderSize));

                    co_return result.isNil() ? 0 : static_cast<size_t>(result.asInteger());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache invalidateListGroupSelectiveUpdate error: " << e.what();
                    co_return 0;
                }
            }

            // =================================================================
            // All-in-one List Group Invalidation (single Lua EVAL, 1 RTT)
            // =================================================================

            /// Invalidate all matching list groups for a single entity (create/delete).
            /// Lua does: HGETALL master → filter match → SortBounds → DEL pages.
            static io::Task<size_t> invalidateListGroupsSelective(
                const std::string& masterKey,
                size_t prefixLen,
                const std::string& filterSchema,
                const std::string& entityBlob,
                const std::string& sortValues)
            {
                if (!DbProvider::hasRedis()) co_return 0;

                try {
                    static constexpr std::string_view lua = R"(
local master = KEYS[1]
local prefix_len = tonumber(ARGV[1])
local hdr_size = tonumber(ARGV[2])
local schema = ARGV[3]
local eblob = ARGV[4]
local sort_csv = ARGV[5]

local sort_vals = {}
for v in string.gmatch(sort_csv, '([^,]+)') do
    sort_vals[#sort_vals + 1] = tonumber(v)
end
local n_filters = #schema / 2
local total = 0

local function u32(s, p)
    local b1,b2,b3,b4 = string.byte(s, p, p+3)
    if not b4 then return 0 end
    return b1 + b2*256 + b3*65536 + b4*16777216
end

local function i64(s, p)
    local b1,b2,b3,b4,b5,b6,b7,b8 = string.byte(s, p, p+7)
    if not b8 then return 0 end
    local val = b1 + b2*256 + b3*65536 + b4*16777216
              + b5*4294967296 + b6*1099511627776
              + b7*281474976710656 + b8*72057594037927936
    if val >= 2^63 then val = val - 2^64 end
    return val
end

local function skip(s, pos, ft)
    if ft == 115 then return pos + 4 + u32(s, pos)
    elseif ft == 56 then return pos + 8
    elseif ft == 52 then return pos + 4
    else return pos + 1 end
end

local function cmp(bin, gp, blob, ep, ft, fo)
    if ft == 115 then
        local gl = u32(bin, gp); local el = u32(blob, ep)
        if fo == 61 then
            return gl == el and (gl == 0 or string.sub(bin, gp+4, gp+3+gl) == string.sub(blob, ep+4, ep+3+el))
        elseif fo == 33 then
            return gl ~= el or (gl > 0 and string.sub(bin, gp+4, gp+3+gl) ~= string.sub(blob, ep+4, ep+3+el))
        end
        return true
    elseif ft == 56 then
        if fo == 61 then return string.sub(bin, gp, gp+7) == string.sub(blob, ep, ep+7)
        elseif fo == 33 then return string.sub(bin, gp, gp+7) ~= string.sub(blob, ep, ep+7)
        else
            local gv = i64(bin, gp); local ev = i64(blob, ep)
            if fo == 62 then return ev > gv
            elseif fo == 71 then return ev >= gv
            elseif fo == 60 then return ev < gv
            elseif fo == 76 then return ev <= gv end
        end
    elseif ft == 52 then
        if fo == 61 then return string.sub(bin, gp, gp+3) == string.sub(blob, ep, ep+3)
        elseif fo == 33 then return string.sub(bin, gp, gp+3) ~= string.sub(blob, ep, ep+3)
        else
            local function r32(s, p)
                local a,b,c,d = string.byte(s, p, p+3)
                if not d then return 0 end
                local v = a + b*256 + c*65536 + d*16777216
                if v >= 2^31 then v = v - 2^32 end; return v
            end
            local gv = r32(bin, gp); local ev = r32(blob, ep)
            if fo == 62 then return ev > gv
            elseif fo == 71 then return ev >= gv
            elseif fo == 60 then return ev < gv
            elseif fo == 76 then return ev <= gv end
        end
    else
        local gv = string.byte(bin, gp); local ev = string.byte(blob, ep)
        if fo == 61 then return gv == ev
        elseif fo == 33 then return gv ~= ev end
        return true
    end
    return true
end

local function fmatch(bin, blob)
    local gp = 1; local ep = 1
    for f = 0, n_filters - 1 do
        local ft = string.byte(schema, f*2+1)
        local fo = string.byte(schema, f*2+2)
        if gp > #bin or ep > #blob then break end
        local gx = string.byte(bin, gp); local ex = string.byte(blob, ep)
        gp = gp + 1; ep = ep + 1
        if gx == 0 then
            if ex == 1 then ep = skip(blob, ep, ft) end
        elseif ex == 0 then
            gp = skip(bin, gp, ft)
            if fo == 61 then return false end
        else
            if not cmp(bin, gp, blob, ep, ft, fo) then return false end
            gp = skip(bin, gp, ft); ep = skip(blob, ep, ft)
        end
    end
    return true
end

local function chk(pk, ev)
    local hdr = redis.call('GETRANGE', pk, 0, hdr_size - 1)
    if #hdr < hdr_size or string.byte(hdr, 1) ~= 0x53 or string.byte(hdr, 2) ~= 0x52 then
        return true
    end
    local first = i64(hdr, 3); local last = i64(hdr, 11)
    local fl = string.byte(hdr, 19)
    local desc = (fl % 2) == 1
    local fp = (math.floor(fl / 2) % 2) == 1
    local inc = (math.floor(fl / 4) % 2) == 1
    local off = (math.floor(fl / 8) % 2) == 0
    if off then
        if inc then return true end
        if desc then return ev >= last else return ev <= last end
    else
        if fp or inc then return true end
        if desc then
            return ev <= first and ev >= last
        else
            return ev >= first and ev <= last
        end
    end
end

local groups = redis.call('HGETALL', master)
if not groups or #groups == 0 then return 0 end

for gi = 1, #groups, 2 do
    local gk = groups[gi]
    local si = tonumber(groups[gi + 1])
    local bin = string.sub(gk, prefix_len + 1)
    if fmatch(bin, eblob) then
        local ev = sort_vals[si + 1] or 0
        local tk = gk .. ':_keys'
        local pages = redis.call('SMEMBERS', tk)
        if pages and #pages > 0 then
            local c = 0
            for _, pk in ipairs(pages) do
                if chk(pk, ev) then
                    redis.call('DEL', pk)
                    redis.call('SREM', tk, pk)
                    c = c + 1
                end
            end
            if c == #pages then redis.call('UNLINK', tk) end
            total = total + c
        end
    end
end
return total
)";

                    auto result = co_await DbProvider::redis(
                        "EVAL", lua, "1", masterKey,
                        std::to_string(prefixLen),
                        static_cast<int>(list::kListBoundsHeaderSize),
                        filterSchema,
                        entityBlob,
                        sortValues);

                    co_return result.isNil() ? 0 : static_cast<size_t>(result.asInteger());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache invalidateListGroupsSelective error: " << e.what();
                    co_return 0;
                }
            }

            /// Invalidate all matching list groups for an update (two entities).
            /// Lua does: HGETALL master → filter match old+new → SortBounds → DEL pages.
            static io::Task<size_t> invalidateListGroupsSelectiveUpdate(
                const std::string& masterKey,
                size_t prefixLen,
                const std::string& filterSchema,
                const std::string& newEntityBlob,
                const std::string& newSortValues,
                const std::string& oldEntityBlob,
                const std::string& oldSortValues)
            {
                if (!DbProvider::hasRedis()) co_return 0;

                try {
                    static constexpr std::string_view lua = R"(
local master = KEYS[1]
local prefix_len = tonumber(ARGV[1])
local hdr_size = tonumber(ARGV[2])
local schema = ARGV[3]
local new_blob = ARGV[4]
local new_csv = ARGV[5]
local old_blob = ARGV[6]
local old_csv = ARGV[7]

local new_sv = {}
for v in string.gmatch(new_csv, '([^,]+)') do new_sv[#new_sv+1] = tonumber(v) end
local old_sv = {}
for v in string.gmatch(old_csv, '([^,]+)') do old_sv[#old_sv+1] = tonumber(v) end
local n_filters = #schema / 2
local total = 0

local function u32(s, p)
    local b1,b2,b3,b4 = string.byte(s, p, p+3)
    if not b4 then return 0 end
    return b1 + b2*256 + b3*65536 + b4*16777216
end

local function i64(s, p)
    local b1,b2,b3,b4,b5,b6,b7,b8 = string.byte(s, p, p+7)
    if not b8 then return 0 end
    local val = b1 + b2*256 + b3*65536 + b4*16777216
              + b5*4294967296 + b6*1099511627776
              + b7*281474976710656 + b8*72057594037927936
    if val >= 2^63 then val = val - 2^64 end
    return val
end

local function skip(s, pos, ft)
    if ft == 115 then return pos + 4 + u32(s, pos)
    elseif ft == 56 then return pos + 8
    elseif ft == 52 then return pos + 4
    else return pos + 1 end
end

local function cmp(bin, gp, blob, ep, ft, fo)
    if ft == 115 then
        local gl = u32(bin, gp); local el = u32(blob, ep)
        if fo == 61 then
            return gl == el and (gl == 0 or string.sub(bin, gp+4, gp+3+gl) == string.sub(blob, ep+4, ep+3+el))
        elseif fo == 33 then
            return gl ~= el or (gl > 0 and string.sub(bin, gp+4, gp+3+gl) ~= string.sub(blob, ep+4, ep+3+el))
        end
        return true
    elseif ft == 56 then
        if fo == 61 then return string.sub(bin, gp, gp+7) == string.sub(blob, ep, ep+7)
        elseif fo == 33 then return string.sub(bin, gp, gp+7) ~= string.sub(blob, ep, ep+7)
        else
            local gv = i64(bin, gp); local ev = i64(blob, ep)
            if fo == 62 then return ev > gv
            elseif fo == 71 then return ev >= gv
            elseif fo == 60 then return ev < gv
            elseif fo == 76 then return ev <= gv end
        end
    elseif ft == 52 then
        if fo == 61 then return string.sub(bin, gp, gp+3) == string.sub(blob, ep, ep+3)
        elseif fo == 33 then return string.sub(bin, gp, gp+3) ~= string.sub(blob, ep, ep+3)
        else
            local function r32(s, p)
                local a,b,c,d = string.byte(s, p, p+3)
                if not d then return 0 end
                local v = a + b*256 + c*65536 + d*16777216
                if v >= 2^31 then v = v - 2^32 end; return v
            end
            local gv = r32(bin, gp); local ev = r32(blob, ep)
            if fo == 62 then return ev > gv
            elseif fo == 71 then return ev >= gv
            elseif fo == 60 then return ev < gv
            elseif fo == 76 then return ev <= gv end
        end
    else
        local gv = string.byte(bin, gp); local ev = string.byte(blob, ep)
        if fo == 61 then return gv == ev
        elseif fo == 33 then return gv ~= ev end
        return true
    end
    return true
end

local function fmatch(bin, blob)
    local gp = 1; local ep = 1
    for f = 0, n_filters - 1 do
        local ft = string.byte(schema, f*2+1)
        local fo = string.byte(schema, f*2+2)
        if gp > #bin or ep > #blob then break end
        local gx = string.byte(bin, gp); local ex = string.byte(blob, ep)
        gp = gp + 1; ep = ep + 1
        if gx == 0 then
            if ex == 1 then ep = skip(blob, ep, ft) end
        elseif ex == 0 then
            gp = skip(bin, gp, ft)
            if fo == 61 then return false end
        else
            if not cmp(bin, gp, blob, ep, ft, fo) then return false end
            gp = skip(bin, gp, ft); ep = skip(blob, ep, ft)
        end
    end
    return true
end

local function chk_single(pk, ev)
    local hdr = redis.call('GETRANGE', pk, 0, hdr_size - 1)
    if #hdr < hdr_size or string.byte(hdr, 1) ~= 0x53 or string.byte(hdr, 2) ~= 0x52 then
        return true
    end
    local first = i64(hdr, 3); local last = i64(hdr, 11)
    local fl = string.byte(hdr, 19)
    local desc = (fl % 2) == 1
    local fp = (math.floor(fl / 2) % 2) == 1
    local inc = (math.floor(fl / 4) % 2) == 1
    local off = (math.floor(fl / 8) % 2) == 0
    if off then
        if inc then return true end
        if desc then return ev >= last else return ev <= last end
    else
        if fp or inc then return true end
        if desc then
            return ev <= first and ev >= last
        else
            return ev >= first and ev <= last
        end
    end
end

local function chk_range(pk, v1, v2)
    local hdr = redis.call('GETRANGE', pk, 0, hdr_size - 1)
    if #hdr < hdr_size or string.byte(hdr, 1) ~= 0x53 or string.byte(hdr, 2) ~= 0x52 then
        return true
    end
    local first = i64(hdr, 3); local last = i64(hdr, 11)
    local fl = string.byte(hdr, 19)
    local desc = (fl % 2) == 1
    local fp = (math.floor(fl / 2) % 2) == 1
    local inc = (math.floor(fl / 4) % 2) == 1
    local off = (math.floor(fl / 8) % 2) == 0
    local rmin = math.min(v1, v2)
    local rmax = math.max(v1, v2)
    if off then
        local pmin = desc and last or first
        local pmax = desc and first or last
        if inc then return pmin <= rmax end
        return (pmin <= rmax) and (rmin <= pmax)
    else
        local function inr(val)
            if fp or inc then return true end
            if desc then return val <= first and val >= last
            else return val >= first and val <= last end
        end
        return inr(v1) or inr(v2)
    end
end

local groups = redis.call('HGETALL', master)
if not groups or #groups == 0 then return 0 end

for gi = 1, #groups, 2 do
    local gk = groups[gi]
    local si = tonumber(groups[gi + 1])
    local bin = string.sub(gk, prefix_len + 1)
    local nm = fmatch(bin, new_blob)
    local om = fmatch(bin, old_blob)
    if nm or om then
        local nv = new_sv[si + 1] or 0
        local ov = old_sv[si + 1] or 0
        local tk = gk .. ':_keys'
        local pages = redis.call('SMEMBERS', tk)
        if pages and #pages > 0 then
            local c = 0
            for _, pk in ipairs(pages) do
                local del = false
                if nm and om then
                    if nv == ov then del = chk_single(pk, nv)
                    else del = chk_range(pk, ov, nv) end
                elseif nm then del = chk_single(pk, nv)
                else del = chk_single(pk, ov) end
                if del then
                    redis.call('DEL', pk)
                    redis.call('SREM', tk, pk)
                    c = c + 1
                end
            end
            if c == #pages then redis.call('UNLINK', tk) end
            total = total + c
        end
    end
end
return total
)";

                    auto result = co_await DbProvider::redis(
                        "EVAL", lua, "1", masterKey,
                        std::to_string(prefixLen),
                        static_cast<int>(list::kListBoundsHeaderSize),
                        filterSchema,
                        newEntityBlob, newSortValues,
                        oldEntityBlob, oldSortValues);

                    co_return result.isNil() ? 0 : static_cast<size_t>(result.asInteger());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache invalidateListGroupsSelectiveUpdate error: " << e.what();
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
                    RELAIS_LOG_WARN << "RedisCache parseList error: " << glz::format_error(ec, json);
                    return std::nullopt;
                }
                return result;
            }

            /// Parse a list value that may be prefixed with a ListBoundsHeader.
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

            // ----- BEVE list helpers -----

            template<typename Entity>
            static std::vector<uint8_t> serializeListBeve(const std::vector<Entity>& entities) {
                std::vector<uint8_t> buffer;
                if (glz::write_beve(entities, buffer)) {
                    buffer.clear();
                }
                return buffer;
            }

            template<typename Entity>
            static std::optional<std::vector<Entity>> parseListBeve(std::span<const uint8_t> data) {
                if (data.empty()) return std::nullopt;
                std::vector<Entity> result;
                if (glz::read_beve(result, std::string_view{
                        reinterpret_cast<const char*>(data.data()), data.size()})) {
                    return std::nullopt;
                }
                return result;
            }

            template<typename Entity>
            static std::optional<std::vector<Entity>> parseListBeveWithHeader(
                    const std::vector<uint8_t>& raw) {
                std::span<const uint8_t> data(raw);
                if (data.size() >= list::kListBoundsHeaderSize
                    && data[0] == list::kListBoundsHeaderMagic[0]
                    && data[1] == list::kListBoundsHeaderMagic[1]) {
                    data = data.subspan(list::kListBoundsHeaderSize);
                }
                return parseListBeve<Entity>(data);
            }
    };

} // namespace jcailloux::relais::cache

#endif //JCX_RELAIS_REDISCACHE_H
