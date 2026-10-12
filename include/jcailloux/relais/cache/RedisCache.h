#ifndef JCX_RELAIS_REDISCACHE_H
#define JCX_RELAIS_REDISCACHE_H

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <optional>
#include <vector>
#include <chrono>
#include <cstring>
#include <span>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/WhenAll.h"
#include "jcailloux/relais/io/redis/RedisError.h"
#include "jcailloux/relais/io/redis/RedisResult.h"
#include "jcailloux/relais/io/redis/RedisScript.h"
#include "jcailloux/relais/PgProvider.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/detail/Chunk.h"
#include "jcailloux/relais/cache/RedisScripts.h"

#include <glaze/glaze.hpp>
#include <jcailloux/relais/list/ListCache.h>
#include "jcailloux/relais/entity/SerializationTraits.h"

namespace relais_test { struct TestInternals; }

namespace jcailloux::relais::cache {

    /**
     * Async Redis cache wrapper for L2 caching.
     * E must implement json() and fromJson().
     *
     * All Redis operations go through PgProvider::redis() which wraps
     * io::RedisClient via type-erased std::function.
     */
    class RedisCache {
        public:

            template<typename E>
            static io::Task<std::optional<E>> get(const std::string& key) {
                if (!PgProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await PgProvider::redis("GET", key);
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    co_return E::fromJson(result.asStringView());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache GET error: " << e.what();
                    co_return std::nullopt;
                }
            }

            template<typename E, typename Rep, typename Period>
            static io::Task<std::optional<E>> getEx(const std::string& key,
                                                              std::chrono::duration<Rep, Period> ttl) {
                if (auto json = co_await getRawEx(key, ttl)) {
                    co_return E::fromJson(*json);
                }
                co_return std::nullopt;
            }

            template<typename E, typename Rep, typename Period>
            static io::Task<bool> set(const std::string& key, const E& entity, std::chrono::duration<Rep, Period> ttl) {
                if (!PgProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    co_await PgProvider::redis("SETEX", key,
                        ttl_seconds, entity.json());
                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache SET error: " << e.what();
                    co_return false;
                }
            }

            // =================================================================
            // Multi-key reads (MGET) — one round-trip for N keys
            // =================================================================

            /// MGET over N keys, format-agnostic. Returns one slot per key in
            /// request order: nullopt for a nil (absent) reply, otherwise the
            /// raw bulk payload (binary-safe — holds BEVE bytes or JSON text).
            /// Deserialization is the caller's responsibility (RedisRepo picks
            /// the format). Empty keys → empty result, no I/O.
            static io::Task<std::vector<std::optional<std::string>>> mgetRaw(
                std::span<const std::string> keys)
            {
                std::vector<std::optional<std::string>> out(keys.size());
                if (keys.empty() || !PgProvider::hasRedis()) {
                    co_return out;
                }

                std::vector<std::string> args;
                args.reserve(keys.size() + 1);
                args.emplace_back("MGET");
                for (const auto& k : keys) {
                    args.push_back(k);
                }

                try {
                    auto result = co_await PgProvider::redisDynamic(std::move(args));
                    if (result.isArray()) {
                        const size_t n = result.arraySize();
                        for (size_t i = 0; i < keys.size() && i < n; ++i) {
                            auto elem = result.at(i);
                            if (elem.isString()) {
                                out[i] = elem.asString();
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache MGET error: " << e.what();
                }
                co_return out;
            }

            /// MGET with per-key TTL refresh — mirror of mgetRaw that also bumps
            /// each present key's expiry to `ttl` (GETEX EX), the multi-key
            /// counterpart of getRawEx. Redis has no multi-key GETEX, so the N
            /// GETEX are fanned out as independent tasks and *gathered*: each
            /// task advances to its submit (entering the same batch) before any
            /// flush → one round-trip. Awaiting them one-by-one would serialize
            /// into N round-trips ("first goes direct"). Slot semantics identical
            /// to mgetRaw (nullopt = nil/absent, else raw bulk payload, binary-
            /// safe). Empty keys → empty result, no I/O. `keys` must outlive the
            /// call (span; the per-key tasks bind it by reference until gathered).
            template<typename Rep, typename Period>
            static io::Task<std::vector<std::optional<std::string>>> mgetRawEx(
                std::span<const std::string> keys,
                std::chrono::duration<Rep, Period> ttl)
            {
                std::vector<std::optional<std::string>> out(keys.size());
                if (keys.empty() || !PgProvider::hasRedis()) {
                    co_return out;
                }

                std::vector<io::Task<std::optional<std::string>>> tasks;
                tasks.reserve(keys.size());
                for (const auto& k : keys) {
                    tasks.push_back(getRawEx(k, ttl));
                }
                co_return co_await io::whenAll(std::move(tasks));
            }

            /// Typed MGET deserializing each present slot as JSON (mirror of
            /// get<E>). For BEVE consumers, use mgetRaw + E::fromBinary.
            template<typename E>
            static io::Task<std::vector<std::optional<E>>> mget(
                std::span<const std::string> keys)
            {
                auto raws = co_await mgetRaw(keys);
                std::vector<std::optional<E>> out;
                out.reserve(raws.size());
                for (const auto& r : raws) {
                    if (r) {
                        out.push_back(E::fromJson(*r));
                    } else {
                        out.emplace_back(std::nullopt);
                    }
                }
                co_return out;
            }

            template<typename E>
            static io::Task<std::optional<std::vector<E>>> getList(const std::string& key) {
                if (!PgProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await PgProvider::redis("GET", key);
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    auto raw = result.asStringView();
                    co_return parseListWithHeader<E>(raw);
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache GET list error: " << e.what();
                    co_return std::nullopt;
                }
            }

            template<typename E, typename Rep, typename Period>
            static io::Task<std::optional<std::vector<E>>> getListEx(const std::string& key,
                                                                               std::chrono::duration<Rep, Period> ttl) {
                if (auto raw = co_await getRawEx(key, ttl)) {
                    co_return parseListWithHeader<E>(*raw);
                }
                co_return std::nullopt;
            }

            /// A list page as JSON, in the layout getList reads back.
            template<typename E>
            static std::string encodeList(const std::vector<E>& entities,
                                          const std::optional<list::ListBoundsHeader>& header) {
                return withListHeader(serializeList(entities), header);
            }

            // =================================================================
            // List BEVE Methods (vector<E> via glz::write_beve/read_beve)
            // =================================================================

            template<typename E>
            static io::Task<std::optional<std::vector<E>>> getListBeve(const std::string& key) {
                auto data = co_await getRawBinary(key);
                if (!data) co_return std::nullopt;
                co_return parseListBeveWithHeader<E>(*data);
            }

            template<typename E, typename Rep, typename Period>
            static io::Task<std::optional<std::vector<E>>> getListBeveEx(
                const std::string& key,
                std::chrono::duration<Rep, Period> ttl)
            {
                auto data = co_await getRawBinaryEx(key, ttl);
                if (!data) co_return std::nullopt;
                co_return parseListBeveWithHeader<E>(*data);
            }

            /// A list page as BEVE, in the layout getListBeve reads back; nullopt
            /// when the list does not serialize.
            template<typename E>
            static std::optional<std::string> encodeListBeve(
                const std::vector<E>& entities,
                const std::optional<list::ListBoundsHeader>& header)
            {
                auto beve = serializeListBeve(entities);
                if (beve.empty() && !entities.empty()) return std::nullopt;
                return withListHeader(std::move(beve), header);
            }

            /// Get raw JSON string without deserialization.
            static io::Task<std::optional<std::string>> getRaw(const std::string& key) {
                if (!PgProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await PgProvider::redis("GET", key);
                    if (result.isNil()) {
                        co_return std::nullopt;
                    }
                    co_return result.asString();
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache getRaw error: " << e.what();
                    co_return std::nullopt;
                }
            }

            // INVARIANT: `key` (a reference) must be materialized (copied into
            // the command args by PgProvider::redis) BEFORE the first suspension.
            // mgetRawEx gathers N of these as lazy tasks over a caller-owned span;
            // the args snapshot happens synchronously in whenAll's start loop, so
            // the span need only outlive the call. Inserting a co_await before the
            // PgProvider::redis below would let `key` dangle under that gather.
            template<typename Rep, typename Period>
            static io::Task<std::optional<std::string>> getRawEx(const std::string& key,
                                                                      std::chrono::duration<Rep, Period> ttl) {
                if (!PgProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto result = co_await PgProvider::redis("GETEX", key,
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
                if (!PgProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    co_await PgProvider::redis("SETEX", key,
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
                if (!PgProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto result = co_await PgProvider::redis("GET", key);
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
                if (!PgProvider::hasRedis()) {
                    co_return std::nullopt;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto result = co_await PgProvider::redis("GETEX", key,
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
                if (!PgProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    co_await PgProvider::redis("SETEX", key,
                        ttl_seconds,
                        std::string_view(reinterpret_cast<const char*>(data.data()), data.size()));
                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache setRawBinary error: " << e.what();
                    co_return false;
                }
            }

            // =================================================================
            // List E Binary Methods
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

            /// A list entity as binary, in the layout getListBinary reads back.
            template<typename ListEntity>
            static std::string encodeListBinary(
                const ListEntity& listEntity,
                const std::optional<list::ListBoundsHeader>& header)
                requires HasBinarySerialization<ListEntity>
            {
                const auto binary = listEntity.binary();
                return withListHeader(
                    std::string(reinterpret_cast<const char*>(binary.data()), binary.size()),
                    header);
            }

            /// A list page in L2: the bounds header when given, then the list.
            static std::string withListHeader(std::string body,
                                              const std::optional<list::ListBoundsHeader>& header) {
                if (!header) return body;
                std::string prefixed(list::kListBoundsHeaderSize + body.size(), '\0');
                header->writeTo(reinterpret_cast<uint8_t*>(prefixed.data()));
                std::memcpy(prefixed.data() + list::kListBoundsHeaderSize,
                            body.data(), body.size());
                return prefixed;
            }

            /// Refresh TTL without modifying the value.
            template<typename Rep, typename Period>
            static io::Task<bool> expire(const std::string& key,
                                              std::chrono::duration<Rep, Period> ttl) {
                if (!PgProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    auto ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
                    auto result = co_await PgProvider::redis("EXPIRE", key,
                        ttl_seconds);
                    co_return result.asInteger() == 1;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache EXPIRE error: " << e.what();
                    co_return false;
                }
            }

            static io::Task<bool> invalidate(const std::string& key) {
                if (!PgProvider::hasRedis()) {
                    co_return false;
                }

                try {
                    co_await PgProvider::redis("UNLINK", key);
                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache UNLINK error: " << e.what();
                    co_return false;
                }
            }

            /// Redis command-size ceiling for variadic key invalidation. One
            /// `UNLINK k1 … kN` carries at most this many keys; larger sets are
            /// split into ⌈N/K⌉ commands. 1k keeps each argv well under Redis'
            /// limits and the proto buffer bounded on huge purges.
            static constexpr size_t kInvalidateChunk = 1000;

            /// Variadic batch eviction: `UNLINK k1 … kN` sub-chunked at
            /// kInvalidateChunk. A single primitive — NOT a loop over
            /// invalidate(key) — so a set of N keys costs ⌈N/K⌉ round-trips
            /// instead of N. Awaited per chunk (a purge is off the hot path) so
            /// the observable post-state is fully drained on return. UNLINK is
            /// idempotent on absent keys, so partial overlap is harmless.
            /// Returns true if every chunk succeeded; false on the first error
            /// (best-effort — already-sent chunks stay evicted).
            static io::Task<bool> invalidateMany(std::span<const std::string> keys) {
                if (keys.empty() || !PgProvider::hasRedis()) {
                    co_return keys.empty();
                }

                for (auto chunk : relais::detail::chunkSpan(keys, kInvalidateChunk)) {
                    std::vector<std::string> args;
                    args.reserve(chunk.size() + 1);
                    args.emplace_back("UNLINK");
                    for (const auto& k : chunk) {
                        args.push_back(k);
                    }
                    try {
                        co_await PgProvider::redisDynamic(std::move(args));
                    } catch (const std::exception& e) {
                        RELAIS_LOG_WARN << "RedisCache UNLINK (batch) error: " << e.what();
                        co_return false;
                    }
                }
                co_return true;
            }

            // =================================================================
            // Lua scripts: the only way a script reaches Redis
            // =================================================================
            //
            // EVALSHA sends the 40-byte digest instead of the body (up to 8.6 KB).
            // When the server has not cached the script (restart, failover,
            // SCRIPT FLUSH, eviction), it replies NOSCRIPT without running
            // anything, and the same call is resent once as EVAL, which runs the
            // script and caches it again. Any other error propagates unchanged.
            // The caller resumes only after the fallback, so "await X before Y"
            // orderings hold.

            /// Run `s` with its keys then its arguments, each converted to text
            /// before this returns: the caller's arguments are not read after.
            template<io::RedisArg... Args>
            static io::Task<io::RedisResult> evalScript(const io::RedisScript& s, int numkeys,
                                                        Args&&... args) {
                std::vector<std::string> keysAndArgs;
                keysAndArgs.reserve(sizeof...(args));
                (keysAndArgs.push_back(io::detail::redisArgText(std::forward<Args>(args))), ...);
                return evalScriptArgs(s, numkeys, std::move(keysAndArgs));
            }

            /// Run `s` with an argument list sized at run time (keys first).
            /// The strings are owned by this frame: the EVAL fallback resends
            /// them after a suspension, so they must never be views on the
            /// caller's data.
            static io::Task<io::RedisResult> evalScriptArgs(const io::RedisScript& s, int numkeys,
                                                            std::vector<std::string> keysAndArgs) {
                const std::string numkeysText = std::to_string(numkeys);
                const std::size_t argc = keysAndArgs.size() + 3;
                std::vector<const char*> argv(argc);
                std::vector<size_t> argvlen(argc);
                argv[0] = "EVALSHA";  // relais:allow-raw-eval
                argvlen[0] = 7;
                argv[1] = s.sha1().data();
                argvlen[1] = s.sha1().size();
                argv[2] = numkeysText.data();
                argvlen[2] = numkeysText.size();
                for (std::size_t i = 0; i < keysAndArgs.size(); ++i) {
                    argv[i + 3] = keysAndArgs[i].data();
                    argvlen[i + 3] = keysAndArgs[i].size();
                }

                try {
                    co_return co_await PgProvider::redisArgv(
                        static_cast<int>(argc), argv.data(), argvlen.data());
                } catch (const io::RedisNoScriptError&) {
                    // NOSCRIPT: nothing ran. co_await is not allowed in a
                    // handler, so the fallback follows the block.
                }

                noteScriptReload(s);
                argv[0] = "EVAL";  // relais:allow-raw-eval
                argvlen[0] = 4;
                argv[1] = s.body.data();
                argvlen[1] = s.body.size();
                co_return co_await PgProvider::redisArgv(
                    static_cast<int>(argc), argv.data(), argvlen.data());
            }

            // =================================================================
            // Cross-instance L2 generation (topology-gated)
            // =================================================================
            //
            // The read-fill recheck's shared authority when one Redis is hit by
            // multiple processes: a sharded generation hash `{repo}:l2gen` whose
            // field = the recheck slot. Invalidation HINCRBYs the slot; a filling
            // reader snapshots the slot at fetch-start (getGen/getGenMany) and the
            // fill is a conditional Lua SET — atomic check-and-set — so a value
            // straddling any instance's invalidation is never written. The check
            // and the SET share one EVAL, so no compensating UNLINK is needed
            // (unlike the process-local async-SET path). L2 hit is unaffected
            // (plain GET); the gen is touched only on the miss-fill cold path.
            //
            // Redis Cluster note: the gen hash lives on one node, and the fill
            // EVAL spans the gen key + entity key(s) → same-slot requirement.
            // relais targets a single logical Redis (one client); a Cluster
            // deployment must hash-tag the entity keys onto the gen node or shard
            // the gen hash. Single-node / single-shard Redis is unaffected.

            /// Build the generation-hash key for a repo from its name prefix.
            static std::string genKeyFor(std::string_view namePrefix) {
                return std::string(namePrefix) + ":l2gen";
            }

            /// Snapshot one generation slot. Absent field → 0 (the baseline
            /// before any bump; HINCRBY makes a touched slot ≥ 1).
            static io::Task<int64_t> getGen(const std::string& genKey, std::size_t slot) {
                if (!PgProvider::hasRedis()) co_return 0;
                try {
                    auto r = co_await PgProvider::redis("HGET", genKey, slot);
                    if (r.isNil()) co_return 0;
                    auto s = r.asString();
                    co_return s.empty() ? int64_t{0} : static_cast<int64_t>(std::stoll(s));
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache getGen error: " << e.what();
                    co_return 0;
                }
            }

            /// Snapshot many generation slots in one HMGET (request order; nil → 0).
            static io::Task<std::vector<int64_t>> getGenMany(
                const std::string& genKey, std::span<const std::size_t> slots)
            {
                std::vector<int64_t> out(slots.size(), 0);
                if (slots.empty() || !PgProvider::hasRedis()) co_return out;

                std::vector<std::string> args;
                args.reserve(slots.size() + 2);
                args.emplace_back("HMGET");
                args.push_back(genKey);
                for (auto s : slots) args.push_back(std::to_string(s));

                try {
                    auto r = co_await PgProvider::redisDynamic(std::move(args));
                    if (r.isArray()) {
                        const size_t n = r.arraySize();
                        for (size_t i = 0; i < slots.size() && i < n; ++i) {
                            auto e = r.at(i);
                            if (e.isString()) {
                                auto s = e.asString();
                                if (!s.empty()) out[i] = static_cast<int64_t>(std::stoll(s));
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache getGenMany error: " << e.what();
                }
                co_return out;
            }

            /// Bump one generation slot (HINCRBY +1). Must be awaited BEFORE the
            /// entity UNLINK on an erase, so a straddling fill sees the moved gen.
            static io::Task<void> bumpGen(const std::string& genKey, std::size_t slot) {
                if (!PgProvider::hasRedis()) co_return;
                try {
                    co_await PgProvider::redis("HINCRBY", genKey, slot, 1);
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache bumpGen error: " << e.what();
                }
            }

            /// Bump many generation slots in one EVAL (one HINCRBY per slot).
            static io::Task<void> bumpGenMany(
                const std::string& genKey, std::span<const std::size_t> slots)
            {
                if (slots.empty() || !PgProvider::hasRedis()) co_return;
                std::vector<std::string> args;
                args.reserve(slots.size() + 1);
                args.push_back(genKey);
                for (auto s : slots) args.push_back(std::to_string(s));

                try {
                    co_await evalScriptArgs(scripts::kBumpGenMany, 1, std::move(args));
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache bumpGenMany error: " << e.what();
                }
            }

            /// One filled entry for a conditional batch SET (setManyIfGen).
            struct GenFill {
                std::string key;       // entity Redis key
                std::size_t slot;      // its generation slot
                int64_t snap;          // gen snapshot taken at fetch-start
                std::string payload;   // serialized entity (binary-safe)
            };

            /// Conditional fill: `SET key payload EX ttl` iff the slot's gen still
            /// equals `snap`. Atomic check-and-set in one EVAL — a mutation that
            /// bumped the gen since the snapshot rejects the fill (returns false),
            /// with no compensating UNLINK needed. payload is binary-safe.
            static io::Task<bool> setIfGen(const std::string& genKey,
                                           const std::string& key,
                                           std::size_t slot, int64_t snap,
                                           std::string_view payload, int64_t ttlSeconds)
            {
                if (!PgProvider::hasRedis()) co_return false;
                try {
                    auto r = co_await evalScript(
                        scripts::kSetIfGen, 2, genKey, key,
                        slot, snap, payload, ttlSeconds);
                    co_return !r.isNil() && r.asInteger() == 1;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache setIfGen error: " << e.what();
                    co_return false;
                }
            }

            /// Batched conditional fill: one EVAL for N entries, each SET only if
            /// its slot's gen still matches its snapshot. Mirror of setIfGen for
            /// the findManyRaw warm-back. Returns the number of entries written.
            static io::Task<size_t> setManyIfGen(const std::string& genKey,
                                                 std::span<const GenFill> entries,
                                                 int64_t ttlSeconds)
            {
                if (entries.empty() || !PgProvider::hasRedis()) co_return 0;
                std::vector<std::string> args;
                args.reserve(entries.size() * 4 + 2);
                args.push_back(genKey);                          // KEYS[1]
                for (const auto& e : entries) args.push_back(e.key);  // KEYS[2..]
                args.push_back(std::to_string(ttlSeconds));      // ARGV[1]
                for (const auto& e : entries) {                  // ARGV triples
                    args.push_back(std::to_string(e.slot));
                    args.push_back(std::to_string(e.snap));
                    args.push_back(e.payload);
                }

                try {
                    auto r = co_await evalScriptArgs(scripts::kSetManyIfGen,
                        static_cast<int>(entries.size() + 1), std::move(args));
                    co_return r.isNil() ? 0 : static_cast<size_t>(r.asInteger());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache setManyIfGen error: " << e.what();
                    co_return 0;
                }
            }

            /// Invalidate keys matching a pattern using SCAN (non-blocking).
            /// Safer than KEYS for production use.
            static io::Task<size_t> invalidatePatternSafe(const std::string& pattern,
                                                               size_t batch_size = 100) {
                if (!PgProvider::hasRedis()) {
                    co_return 0;
                }

                try {
                    size_t count = 0;
                    std::string cursor = "0";

                    do {
                        std::vector<std::string> batch_keys;

                        auto result = co_await PgProvider::redis(
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
                                co_await PgProvider::redis("UNLINK", k);
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

            /// Write a list page and track it in its group ("<group>:_keys"), in
            /// one script: the page is never present untracked. With a master
            /// hash, also records the group's sort index there. The tracking
            /// set's TTL is set only if it has none.
            static io::Task<bool> storeListPage(const std::string& pageKey,
                                                std::string payload,
                                                int64_t ttlSeconds,
                                                const std::string& groupKey,
                                                const std::string& masterKey = {},
                                                std::size_t sortIndex = 0)
            {
                if (!PgProvider::hasRedis()) co_return false;
                try {
                    if (masterKey.empty()) {
                        co_await evalScript(scripts::kListStore, 2,
                            pageKey, groupKey + ":_keys", std::move(payload), ttlSeconds);
                    } else {
                        co_await evalScript(scripts::kListStore, 3,
                            pageKey, groupKey + ":_keys", masterKey,
                            std::move(payload), ttlSeconds, groupKey, sortIndex);
                    }
                    co_return true;
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache storeListPage error: " << e.what();
                    co_return false;
                }
            }

            /// storeListPage off the caller's path: started now, never awaited,
            /// owns its arguments. A Redis failure is logged, never thrown.
            static io::DetachedTask storeListPageDetached(std::string pageKey,
                                                          std::string payload,
                                                          int64_t ttlSeconds,
                                                          std::string groupKey,
                                                          std::string masterKey = {},
                                                          std::size_t sortIndex = 0)
            {
                co_await storeListPage(pageKey, std::move(payload), ttlSeconds,
                                       groupKey, masterKey, sortIndex);
            }

            /// `SET key payload EX ttl` off the caller's path, for a list page no
            /// group tracks. Owns its arguments; a Redis failure is logged.
            static io::DetachedTask setDetached(std::string key, std::string payload,
                                                int64_t ttlSeconds)
            {
                if (!PgProvider::hasRedis()) co_return;
                try {
                    co_await PgProvider::redis("SET", key, payload, "EX", ttlSeconds);
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache SET list error: " << e.what();
                }
            }

            /// Invalidate all list cache keys in a group.
            /// O(M) where M is the number of cached pages (typically small).
            static io::Task<size_t> invalidateListGroup(const std::string& groupKey) {
                if (!PgProvider::hasRedis()) {
                    co_return 0;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";

                    auto result = co_await evalScript(
                        scripts::kInvalidateListGroup, 1, trackingKey);

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
                if (!PgProvider::hasRedis()) {
                    co_return 0;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";

                    auto result = co_await evalScript(
                        scripts::kInvalidateListGroupSelective, 1, trackingKey,
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
                if (!PgProvider::hasRedis()) {
                    co_return 0;
                }

                try {
                    const std::string trackingKey = groupKey + ":_keys";

                    auto result = co_await evalScript(
                        scripts::kInvalidateListGroupSelectiveUpdate, 1, trackingKey,
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
                if (!PgProvider::hasRedis()) co_return 0;

                try {
                    auto result = co_await evalScript(
                        scripts::kInvalidateListGroupsSelective, 1, masterKey,
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

            /// Predicate-driven list invalidation (eraseWhere fast-path): ONE EVAL
            /// for the whole deleted set, O(groups) not O(N). Lua does HGETALL master
            /// → pmatch(group filters, predicate) prunes filter-incompatible groups
            /// → chk_range(page, [lo,hi]) DELs pages whose sort range overlaps the
            /// predicate's range on that group's sort dimension. Never-miss: an
            /// absent predicate constraint is a wildcard; only EQ-differ / IN-disjoint
            /// prune, only a provably non-overlapping page is skipped.
            static io::Task<size_t> invalidateListGroupsByPredicate(
                const std::string& masterKey,
                size_t prefixLen,
                const std::string& filterSchema,
                const std::string& predicateBlob,
                const std::string& loValues,
                const std::string& hiValues)
            {
                if (!PgProvider::hasRedis()) co_return 0;

                try {
                    auto result = co_await evalScript(
                        scripts::kInvalidateListGroupsByPredicate, 1, masterKey,
                        std::to_string(prefixLen),
                        static_cast<int>(list::kListBoundsHeaderSize),
                        filterSchema,
                        predicateBlob,
                        loValues,
                        hiValues);

                    co_return result.isNil() ? 0 : static_cast<size_t>(result.asInteger());
                } catch (const std::exception& e) {
                    RELAIS_LOG_WARN << "RedisCache invalidateListGroupsByPredicate error: " << e.what();
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
                if (!PgProvider::hasRedis()) co_return 0;

                try {
                    auto result = co_await evalScript(
                        scripts::kInvalidateListGroupsSelectiveUpdate, 1, masterKey,
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
            friend struct ::relais_test::TestInternals;

            // EVAL fallbacks since start. Relaxed: touched only on a fallback,
            // read only by tests.
            static inline std::atomic<uint64_t> script_reloads_{0};
            // One flag per registered script: its first fallback in the process
            // is logged as a warning (restart, failover and flush are events worth
            // seeing), later ones at debug, so a client evicting scripts in a
            // loop cannot flood the logs.
            static inline std::array<std::atomic<bool>, scripts::kAllScripts.size()>
                script_reloaded_{};

            static void noteScriptReload(const io::RedisScript& s) {
                script_reloads_.fetch_add(1, std::memory_order_relaxed);
                bool first = true;
                for (std::size_t i = 0; i < scripts::kAllScripts.size(); ++i) {
                    if (scripts::kAllScripts[i] == &s) {
                        first = !script_reloaded_[i].exchange(true, std::memory_order_relaxed);
                        break;
                    }
                }
                if (first)
                    RELAIS_LOG_WARN << "RedisCache: script " << s.sha1()
                                    << " not cached by Redis, reloaded with EVAL";
                else
                    RELAIS_LOG_DEBUG << "RedisCache: script " << s.sha1()
                                     << " not cached by Redis, reloaded with EVAL";
            }

            template<typename E>
            static std::string serializeList(const std::vector<E>& entities) {
                std::string buffer;
                if (glz::write_json(entities, buffer)) {
                    return "[]";
                }
                return buffer;
            }

            template<typename E>
            static std::optional<std::vector<E>> parseList(const std::string_view json) {
                std::vector<E> result;
                if (auto ec = glz::read_json(result, json)) {
                    RELAIS_LOG_WARN << "RedisCache parseList error: " << glz::format_error(ec, json);
                    return std::nullopt;
                }
                return result;
            }

            /// Parse a list value that may be prefixed with a ListBoundsHeader.
            template<typename E>
            static std::optional<std::vector<E>> parseListWithHeader(const std::string_view raw) {
                std::string_view data = raw;
                if (data.size() >= list::kListBoundsHeaderSize
                    && static_cast<uint8_t>(data[0]) == list::kListBoundsHeaderMagic[0]
                    && static_cast<uint8_t>(data[1]) == list::kListBoundsHeaderMagic[1]) {
                    data.remove_prefix(list::kListBoundsHeaderSize);
                }
                return parseList<E>(data);
            }

            // ----- BEVE list helpers -----

            template<typename E>
            static std::string serializeListBeve(const std::vector<E>& entities) {
                std::string buffer;
                if (glz::write_beve(entities, buffer)) {
                    buffer.clear();
                }
                return buffer;
            }

            template<typename E>
            static std::optional<std::vector<E>> parseListBeve(std::span<const uint8_t> data) {
                if (data.empty()) return std::nullopt;
                std::vector<E> result;
                if (glz::read_beve(result, std::string_view{
                        reinterpret_cast<const char*>(data.data()), data.size()})) {
                    return std::nullopt;
                }
                return result;
            }

            template<typename E>
            static std::optional<std::vector<E>> parseListBeveWithHeader(
                    const std::vector<uint8_t>& raw) {
                std::span<const uint8_t> data(raw);
                if (data.size() >= list::kListBoundsHeaderSize
                    && data[0] == list::kListBoundsHeaderMagic[0]
                    && data[1] == list::kListBoundsHeaderMagic[1]) {
                    data = data.subspan(list::kListBoundsHeaderSize);
                }
                return parseListBeve<E>(data);
            }
    };

} // namespace jcailloux::relais::cache

#endif //JCX_RELAIS_REDISCACHE_H
