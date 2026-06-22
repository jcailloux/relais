#ifndef JCX_RELAIS_REDISREPO_H
#define JCX_RELAIS_REDISREPO_H

#include "jcailloux/relais/repository/PgRepo.h"
#include "jcailloux/relais/repository/RecheckGuard.h"
#include "jcailloux/relais/cache/RedisCache.h"
#include "jcailloux/relais/cache/Metrics.h"
#include "jcailloux/relais/config/CacheConfig.h"

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
 * find() returns epoch-guarded CacheView. findJson()/findBinary() return by value.
 * Views are thread-agnostic and safe to hold across co_await.
 *
 * Cross-invalidation is not handled here; it belongs in InvalidationMixin.
 */
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires CacheableEntity<E>
class RedisRepo : public PgRepo<E, Name, Cfg, Key> {
    using Base = PgRepo<E, Name, Cfg, Key>;
    using Mapping = typename E::MappingType;

    /// Read-fill recheck guard — shared with LocalRepo for the same repo (same
    /// Name/Key/SlotsLog2 → one static slot array seen by both tiers). In an
    /// L2-only config LocalRepo is absent, so this layer both bumps (on its
    /// mutation paths) and rechecks; in L1+L2 LocalRepo's bump is on the same
    /// array, so the L2 recheck here observes it.
    using Recheck = RecheckGuard<Name, Key, Cfg.recheck_slots_log2>;

    static constexpr bool useL2Binary =
        (Cfg.l2_format == config::L2Format::Binary) && HasBinarySerialization<E>;

    public:
#if RELAIS_ENABLE_METRICS
        static inline cache::L2Counters l2_counters_{};
#endif

        using typename Base::EntityType;
        using typename Base::KeyType;
        using typename Base::WrapperType;
        using typename Base::FindResultType;
        using Base::name;

        // GCC miscompiles std::chrono::nanoseconds(Cfg.l2_ttl.ns) under
        // -fsanitize=thread (the class-type NTTP subobject read folds to 0 at
        // the construction site, though a plain field read is fine). Materialize
        // the count into a static constexpr int64 first, then build durations
        // from that scalar — both immune.
        static constexpr int64_t kL2TtlNs = Cfg.l2_ttl.ns;
        static constexpr auto l2Ttl() { return std::chrono::nanoseconds(kL2TtlNs); }

        /// Find by ID with L2 (Redis) -> L3 (DB) fallback.
        /// Returns epoch-guarded CacheView (empty if not found).
        static io::Task<cache::CacheView<E>> find(const Key& id) {
            auto entity = co_await findRaw(id);
            if (!entity) co_return {};
            co_return Base::makeView(std::move(*entity));
        }

        /// Find by ID and return JSON string.
        /// L2 hit (BEVE): transcodes via glz::beve_to_json (no entity construction).
        /// L2 hit (JSON): returns raw string directly.
        /// L2 miss: delegates to find() then serializes.
        static io::Task<std::string> findJson(const Key& id) {
            auto redisKey = makeRedisKey(id);

            if constexpr (useL2Binary) {
                std::optional<std::vector<uint8_t>> beve;
                if constexpr (Cfg.l2_refresh_on_get) {
                    beve = co_await cache::RedisCache::getRawBinaryEx(redisKey, l2Ttl());
                } else {
                    beve = co_await cache::RedisCache::getRawBinary(redisKey);
                }
                if (beve) {
                    std::string json;
                    if (!glz::beve_to_json(*beve, json)) {
                        co_return json;
                    }
                }
            } else {
                std::optional<std::string> cached;
                if constexpr (Cfg.l2_refresh_on_get) {
                    cached = co_await cache::RedisCache::getRawEx(redisKey, l2Ttl());
                } else {
                    cached = co_await cache::RedisCache::getRaw(redisKey);
                }
                if (cached) co_return std::move(*cached);
            }

            // L2 miss
            auto view = co_await find(id);
            if (!view) co_return {};
            co_return view->json();
        }

        /// Find by ID and return binary (BEVE) vector.
        /// L2 hit (Binary): returns raw bytes directly from Redis.
        /// L2 miss: delegates to find() then serializes.
        static io::Task<std::vector<uint8_t>> findBinary(const Key& id)
            requires HasBinarySerialization<E>
        {
            auto redisKey = makeRedisKey(id);

            if constexpr (useL2Binary) {
                std::optional<std::vector<uint8_t>> cached;
                if constexpr (Cfg.l2_refresh_on_get) {
                    cached = co_await cache::RedisCache::getRawBinaryEx(redisKey, l2Ttl());
                } else {
                    cached = co_await cache::RedisCache::getRawBinary(redisKey);
                }
                if (cached) co_return std::move(*cached);
            } else {
                std::optional<std::string> cached;
                if constexpr (Cfg.l2_refresh_on_get) {
                    cached = co_await cache::RedisCache::getRawEx(redisKey, l2Ttl());
                } else {
                    cached = co_await cache::RedisCache::getRaw(redisKey);
                }
                if (cached) {
                    auto entity_opt = E::fromJson(*cached);
                    if (entity_opt) co_return entity_opt->binary();
                }
            }

            // L2 miss
            auto view = co_await find(id);
            if (!view) co_return {};
            co_return view->binary();
        }

        /// Insert entity in database with L2 cache population.
        /// Returns epoch-guarded CacheView (empty on error).
        static io::Task<cache::CacheView<E>> insert(const E& entity)
            requires CreatableEntity<E, Key> && (!Cfg.read_only)
        {
            auto result = co_await insertRaw(entity);
            if (!result) co_return {};
            co_return Base::makeView(std::move(*result));
        }

        /// Update entity in database with L2 cache handling.
        /// Returns true on success, false on error.
        static io::Task<bool> update(const Key& id, const E& entity)
            requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
        {
            auto outcome = co_await updateOutcome(id, entity);
            co_return outcome.success;
        }

        /// Partial update: invalidates Redis then delegates to Base::patchRaw.
        /// Returns the re-fetched entity as epoch-guarded view.
        template<typename... Updates>
        static io::Task<cache::CacheView<E>> patch(const Key& id, Updates&&... updates)
            requires HasFieldUpdate<E> && (!Cfg.read_only)
        {
            auto entity = co_await patchRaw(id, std::forward<Updates>(updates)...);
            if (!entity) co_return {};
            co_return Base::makeView(std::move(*entity));
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
        using WriteOutcome = typename Base::WriteOutcome;
        using EraseOutcome = typename Base::EraseOutcome;

        /// Update returning full outcome. Skips L2 ops when coalesced.
        static io::Task<WriteOutcome> updateOutcome(const Key& id, const E& entity)
            requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
        {
            using enum config::UpdateStrategy;

            auto outcome = co_await Base::updateOutcome(id, entity);
            if (outcome.success && !outcome.coalesced) {
                co_await bumpGen(id);
                if constexpr (Cfg.update_strategy == InvalidateAndLazyReload) {
                    // Safe strategy: the bump above then an evict — the next read
                    // re-fills (read-fill recheck / Redis-side gen), so L2 always
                    // reconverges on the DB across instances.
                    co_await evictRedis(id);
                } else {
                    // Optimistic write-through: authoritative SET of the value we
                    // just committed. NOTE — under CONCURRENT updaters this is
                    // last-write-to-Redis-wins (two writers can land their SETs in
                    // an order opposite to their DB commits, leaving L2 holding the
                    // non-final value until l2_ttl). That window is inherent to
                    // optimistic write-through and orthogonal to the read-fill
                    // straddle the gen closes; the safe InvalidateAndLazyReload
                    // strategy has no such window. Single-writer / sequential
                    // updates are exact.
                    co_await setInCache(makeRedisKey(id), entity);
                }
            }
            co_return outcome;
        }

        /// Internal erase with optional entity hint.
        /// For CompositeKey entities: if L1 didn't provide a hint,
        /// try L2 (Redis) as a near-free fallback (~0.1-1ms).
        static io::Task<std::optional<size_t>> eraseImpl(
            const Key& id, const E* hint = nullptr)
            requires (!Cfg.read_only)
        {
            auto outcome = co_await eraseOutcome(id, hint);
            co_return outcome.affected;
        }

        /// Erase returning full outcome. Skips L2 ops when coalesced.
        static io::Task<EraseOutcome> eraseOutcome(
            const Key& id, const E* hint = nullptr)
            requires (!Cfg.read_only)
        {
            // L2 hint fallback for partition pruning
            std::optional<E> local_hint;
            if constexpr (HasPartitionHint<E>) {
                if (!hint) {
                    local_hint = co_await getFromCache(makeRedisKey(id));
                    if (local_hint) hint = &*local_hint;
                }
            }

            auto outcome = co_await Base::eraseOutcome(id, hint);
            if (outcome.affected.has_value() && !outcome.coalesced) {
                co_await bumpGen(id);
                co_await evictRedis(id);
            }
            co_return outcome;
        }

    public:

        /// Invalidate Redis cache for a key and return void.
        /// Used as cross-invalidation target interface.
        static io::Task<void> invalidate(const Key& id) {
            co_await bumpGen(id);
            co_await evictRedis(id);
        }

        /// Invalidate Redis cache for a key.
        static io::Task<bool> evictRedis(const Key& id) {
            co_return co_await cache::RedisCache::invalidate(makeRedisKey(id));
        }

        static std::string makeRedisKey(const Key& id) {
            if constexpr (is_tuple_v<Key>) {
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
        // Batch invalidation common path — L2 entity tier
        // =====================================================================

        /// Evict the whole affected set from L2 in one variadic UNLINK batch
        /// (sub-chunked at K_redis by RedisCache::invalidateMany), then delegate
        /// down the chain. ⌈N/K_redis⌉ round-trips instead of N evictRedis.
        template<bool WithLists = true>
        static io::Task<void> invalidateManyImpl(std::span<const E> entities) {
            std::vector<std::string> keys;
            keys.reserve(entities.size());
            for (const auto& e : entities) {
                keys.push_back(makeRedisKey(e.key()));
            }
            // Bump the whole affected set's generations BEFORE the UNLINK batch,
            // same ordering invariant as the mono path (a straddling fill must
            // see the moved gen). Multi-instance: one EVAL of HINCRBYs; single-
            // instance L2-only: the process-local array.
            if constexpr (Cfg.l2_shared_across_instances) {
                std::vector<std::size_t> slots;
                slots.reserve(entities.size());
                for (const auto& e : entities) slots.push_back(Recheck::slotOf(e.key()));
                co_await cache::RedisCache::bumpGenMany(
                    genHashKey(), std::span<const std::size_t>(slots));
            } else if constexpr (Cfg.cache_level == config::CacheLevel::L2) {
                for (const auto& e : entities) Recheck::bump(e.key());
            }
            co_await cache::RedisCache::invalidateMany(
                std::span<const std::string>(keys));
            co_await Base::template invalidateManyImpl<WithLists>(entities);
        }

        // =====================================================================
        // Raw methods returning entity by value (for LocalRepo move path)
        // =====================================================================

        /// Redis-side generation hash key for this repo (sharded; field = slot).
        static std::string genHashKey() { return cache::RedisCache::genKeyFor(name()); }

        /// L2 TTL in whole seconds (for the Redis-side conditional fills).
        static int64_t l2TtlSeconds() {
            return std::chrono::duration_cast<std::chrono::seconds>(l2Ttl()).count();
        }

        /// Generation bump on a confirmed mutation. Topology-gated:
        ///  - multi-instance (l2_shared_across_instances): the shared authority
        ///    is Redis — HINCRBY the sharded gen hash so every instance's fillers
        ///    observe this invalidation. Awaited; it MUST land before the entity
        ///    UNLINK that follows (a fill straddling the UNLINK must see the
        ///    moved gen and reject), mirroring 12a's bump-before-evict invariant.
        ///  - single-instance (default): the process-local recheck counter, and
        ///    only when L2 is the TOP layer (L2-only). In L1+L2 LocalRepo above
        ///    already bumps the same array, so the L2 recheck observes it.
        static io::Task<void> bumpGen(const Key& id) {
            if constexpr (Cfg.l2_shared_across_instances) {
                co_await cache::RedisCache::bumpGen(genHashKey(), Recheck::slotOf(id));
            } else if constexpr (Cfg.cache_level == config::CacheLevel::L2) {
                Recheck::bump(id);
            }
            co_return;
        }

        /// Find with L2 -> L3 fallback, returning entity by value.
        ///
        /// Read-fill recheck, topology-gated:
        ///  - single-instance (default): process-local recheck. Snapshot the
        ///    slot at fetch-start; if a mutation lands before the L2 store, skip
        ///    it. The store is itself async (a SET round-trip), so a mutation can
        ///    slip in during it — recheck once more afterwards and compensate
        ///    with a raw UNLINK. Airtight within ONE process; across MULTIPLE
        ///    processes the static counter can't see another instance's
        ///    invalidation, so L2 is bounded by l2_ttl there.
        ///  - multi-instance (l2_shared_across_instances): the recheck authority
        ///    is Redis. Snapshot the slot's gen (HGET, before the L3 fetch) and
        ///    fill via an atomic conditional SET (setIfGen) — a fill straddling
        ///    ANY instance's invalidation sees the moved gen and is rejected.
        ///    Guaranteed zero-stale regardless of instance count; no
        ///    compensating UNLINK (check + SET share one EVAL).
        static io::Task<std::optional<E>> findRaw(const Key& id) {
            auto redisKey = makeRedisKey(id);
            auto cached = co_await getFromCache(redisKey);
            if (cached) {
                RELAIS_METRICS_INC(l2_counters_.hits);
                co_return cached;
            }

            RELAIS_METRICS_INC(l2_counters_.misses);

            if constexpr (Cfg.l2_shared_across_instances) {
                // Multi-instance: the recheck authority is Redis. Snapshot the
                // slot's gen at fetch-start (HGET, before the L3 fetch), then
                // fill via an atomic conditional SET — if any instance bumped
                // the gen meanwhile, the fill is rejected. No compensating UNLINK
                // is needed: the check and the SET share one EVAL.
                std::size_t slot = Recheck::slotOf(id);
                int64_t snap = co_await cache::RedisCache::getGen(genHashKey(), slot);
                auto entity = co_await Base::findRaw(id);
                if (entity) {
                    std::string payload = l2Payload(*entity);
                    co_await cache::RedisCache::setIfGen(
                        genHashKey(), redisKey, slot, snap, payload, l2TtlSeconds());
                }
                co_return entity;
            } else {
                uint64_t snap = Recheck::snapshot(id);
                auto entity = co_await Base::findRaw(id);
                if (entity && !Recheck::changed(id, snap)) {
                    co_await setInCache(redisKey, *entity);
                    if (Recheck::changed(id, snap)) {
                        // Mutation slipped in during the async SET → undo. Raw
                        // invalidate (not evictRedis) to avoid a spurious bump.
                        co_await cache::RedisCache::invalidate(redisKey);
                    }
                }
                co_return entity;
            }
        }

        /// Batched find with L2 (Redis) MGET -> L3 (DB) fallback. One MGET over
        /// all ids; present slots deserialized in the configured format; the L2
        /// misses are delegated to Base::findManyRaw (one ANY) and realigned on
        /// request order. The L3-fetched misses are warmed back into L2 by a
        /// detached pipeline (fire-and-forget — the return does NOT await the
        /// SETs, one RTT off the miss path). With l2_refresh_on_get, the L2 hits
        /// get their TTL refreshed in the same MGET round-trip (mgetRawEx); the
        /// detached fill resets TTL on the warmed misses.
        /// Precondition: ids deduplicated (dedup lives at the public entry,
        /// LocalRepo::findMany); no re-dedup here.
        static io::Task<std::vector<std::optional<E>>> findManyRaw(std::span<const Key> ids) {
            std::vector<std::optional<E>> out(ids.size());
            if (ids.empty()) co_return out;

            std::vector<std::string> redisKeys;
            redisKeys.reserve(ids.size());
            for (const auto& id : ids) redisKeys.push_back(makeRedisKey(id));

            auto cached = co_await mgetFromCache(redisKeys);

            // Partition: L2 hits land in out directly; misses keep their origin
            // position so the L3 result realigns on request order.
            std::vector<Key> missIds;
            std::vector<size_t> missPos;
            for (size_t i = 0; i < ids.size(); ++i) {
                if (cached[i]) {
                    RELAIS_METRICS_INC(l2_counters_.hits);
                    out[i] = std::move(cached[i]);
                } else {
                    RELAIS_METRICS_INC(l2_counters_.misses);
                    missIds.push_back(ids[i]);
                    missPos.push_back(i);
                }
            }

            if (missIds.empty()) co_return out;

            if constexpr (Cfg.l2_shared_across_instances) {
                // Multi-instance: snapshot every miss slot's gen in one HMGET
                // (before the L3 fetch), then a single conditional EVAL fills
                // only the entries whose gen is unchanged — atomic per entry,
                // no compensating UNLINK. Detached (off the return path).
                std::vector<std::size_t> slots;
                slots.reserve(missIds.size());
                for (const auto& mid : missIds) slots.push_back(Recheck::slotOf(mid));
                auto snaps = co_await cache::RedisCache::getGenMany(
                    genHashKey(), std::span<const std::size_t>(slots));

                auto fetched = co_await Base::findManyRaw(missIds);

                std::vector<cache::RedisCache::GenFill> toFill;
                toFill.reserve(missPos.size());
                for (size_t j = 0; j < missPos.size(); ++j) {
                    if (fetched[j]) {
                        toFill.push_back(cache::RedisCache::GenFill{
                            std::move(redisKeys[missPos[j]]), slots[j],
                            snaps[j], l2Payload(*fetched[j])});
                        out[missPos[j]] = std::move(fetched[j]);
                    }
                }
                if (!toFill.empty()) fillL2GenDetached(std::move(toFill));
                co_return out;
            } else {
                // Snapshot the recheck slots at fetch-start, one per L2-miss key.
                std::vector<uint64_t> snaps;
                snaps.reserve(missIds.size());
                for (const auto& mid : missIds) snaps.push_back(Recheck::snapshot(mid));

                auto fetched = co_await Base::findManyRaw(missIds);

                // Merge the L3 hits back into request order and collect them (one
                // owned copy each) for the detached L2 warming. Read-fill recheck:
                // a key already mutated by the time we get here is dropped from the
                // fill set (early gate); the rest carry their id+snapshot so the
                // detached task can re-check around the async SET and compensate.
                std::vector<L2FillEntry> toFill;
                toFill.reserve(missPos.size());
                for (size_t j = 0; j < missPos.size(); ++j) {
                    if (fetched[j]) {
                        if (!Recheck::changed(missIds[j], snaps[j])) {
                            toFill.push_back(L2FillEntry{
                                std::move(redisKeys[missPos[j]]), missIds[j],
                                snaps[j], *fetched[j]});
                        }
                        out[missPos[j]] = std::move(fetched[j]);
                    }
                }

                if (!toFill.empty()) {
                    fillL2Detached(std::move(toFill));
                }
                co_return out;
            }
        }

        /// Insert with L2 cache population, returning entity by value.
        static io::Task<std::optional<E>> insertRaw(const E& entity)
            requires CreatableEntity<E, Key> && (!Cfg.read_only)
        {
            auto result = co_await Base::insertRaw(entity);
            if (result) {
                co_await bumpGen(result->key());
                co_await setInCache(makeRedisKey(result->key()), *result);
            }
            co_return result;
        }

        /// Partial update: invalidates Redis, returning entity by value.
        template<typename... Updates>
        static io::Task<std::optional<E>> patchRaw(const Key& id, Updates&&... updates)
            requires HasFieldUpdate<E> && (!Cfg.read_only)
        {
            co_await bumpGen(id);
            co_await evictRedis(id);
            co_return co_await Base::patchRaw(id, std::forward<Updates>(updates)...);
        }

        // =====================================================================
        // Serialization-aware cache helpers
        // =====================================================================

        /// Get entity from cache using the configured serialization format.
        static io::Task<std::optional<E>> getFromCache(const std::string& key) {
            if constexpr (useL2Binary) {
                std::optional<std::vector<uint8_t>> data;
                if constexpr (Cfg.l2_refresh_on_get) {
                    data = co_await cache::RedisCache::getRawBinaryEx(key, l2Ttl());
                } else {
                    data = co_await cache::RedisCache::getRawBinary(key);
                }
                if (data) {
                    co_return E::fromBinary(std::span<const uint8_t>(*data));
                }
                co_return std::nullopt;
            } else {
                // JSON mode (default)
                if constexpr (Cfg.l2_refresh_on_get) {
                    co_return co_await cache::RedisCache::getEx<E>(key, l2Ttl());
                } else {
                    co_return co_await cache::RedisCache::get<E>(key);
                }
            }
        }

        /// Set entity in cache using the configured serialization format.
        static io::Task<bool> setInCache(const std::string& key, const E& entity) {
            if constexpr (useL2Binary) {
                co_return co_await cache::RedisCache::setRawBinary(key, entity.binary(), l2Ttl());
            } else {
                co_return co_await cache::RedisCache::set(key, entity, l2Ttl());
            }
        }

        /// Serialize an entity into the L2 wire payload (binary-safe). Mirrors
        /// setInCache's format choice; used by the Redis-side conditional fills
        /// (setIfGen / setManyIfGen) which write the bytes inside a Lua SET.
        static std::string l2Payload(const E& entity) {
            if constexpr (useL2Binary) {
                auto b = entity.binary();
                return std::string(reinterpret_cast<const char*>(b.data()), b.size());
            } else {
                return entity.json();
            }
        }

        /// Batched get: one MGET, each present slot deserialized in the
        /// configured L2 format (BEVE or JSON). Mirror of getFromCache for the
        /// multi-key case. With l2_refresh_on_get, refreshes every hit's TTL via
        /// mgetRawEx (gathered GETEX, one round-trip) — same if constexpr gate as
        /// getFromCache.
        static io::Task<std::vector<std::optional<E>>> mgetFromCache(
            std::span<const std::string> keys)
        {
            std::vector<std::optional<std::string>> raws;
            if constexpr (Cfg.l2_refresh_on_get) {
                raws = co_await cache::RedisCache::mgetRawEx(keys, l2Ttl());
            } else {
                raws = co_await cache::RedisCache::mgetRaw(keys);
            }

            std::vector<std::optional<E>> out;
            out.reserve(raws.size());
            for (const auto& r : raws) {
                if (r) {
                    if constexpr (useL2Binary) {
                        out.push_back(E::fromBinary(std::span<const uint8_t>(
                            reinterpret_cast<const uint8_t*>(r->data()), r->size())));
                    } else {
                        out.push_back(E::fromJson(*r));
                    }
                } else {
                    out.emplace_back(std::nullopt);
                }
            }
            co_return out;
        }

        /// One detached L2 warm-back: the Redis key, the entity key + the
        /// recheck snapshot taken at fetch-start (for the compensating recheck),
        /// and an owned entity copy.
        struct L2FillEntry {
            std::string key;
            Key id;
            uint64_t snap;
            E entity;
        };

        /// Fire-and-forget L2 warming for a batch of fetched entities. Mirrors
        /// setInCache but off the return path (DetachedTask), so the caller
        /// never blocks on the SET RTT. Owns its arguments by value.
        ///
        /// Read-fill recheck (per entry): skip the SET if the key already
        /// mutated since fetch-start; otherwise SET, then re-check and UNLINK
        /// (compensate) if a mutation slipped in during the async SET.
        static io::DetachedTask fillL2Detached(std::vector<L2FillEntry> entries) {
            try {
                for (const auto& e : entries) {
                    if (Recheck::changed(e.id, e.snap)) continue;
                    co_await setInCache(e.key, e.entity);
                    if (Recheck::changed(e.id, e.snap)) {
                        co_await cache::RedisCache::invalidate(e.key);
                    }
                }
            } catch (...) {}
        }

        /// Fire-and-forget multi-instance L2 warming: one conditional EVAL that
        /// SETs each entry only if its slot's gen still matches the fetch-start
        /// snapshot. Atomic per entry — no compensating UNLINK. Off the return
        /// path (DetachedTask), owns its arguments by value.
        static io::DetachedTask fillL2GenDetached(
            std::vector<cache::RedisCache::GenFill> entries)
        {
            try {
                co_await cache::RedisCache::setManyIfGen(
                    genHashKey(),
                    std::span<const cache::RedisCache::GenFill>(entries),
                    l2TtlSeconds());
            } catch (...) {}
        }

        static io::Task<std::optional<std::vector<E>>> getListFromRedis(const std::string& key) {
            if constexpr (useL2Binary) {
                co_return co_await cache::RedisCache::getListBeve<E>(key);
            } else {
                co_return co_await cache::RedisCache::getList<E>(key);
            }
        }

        static io::Task<std::optional<std::vector<E>>> getListFromRedisEx(const std::string& key) {
            if constexpr (useL2Binary) {
                co_return co_await cache::RedisCache::getListBeveEx<E>(key, l2Ttl());
            } else {
                co_return co_await cache::RedisCache::getListEx<E>(key, l2Ttl());
            }
        }

        template<typename Rep, typename Period>
        static io::Task<bool> setListInRedis(const std::string& key,
                                                  const std::vector<E>& entities,
                                                  std::chrono::duration<Rep, Period> ttl,
                                                  std::optional<list::ListBoundsHeader> header = std::nullopt)
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
        static io::Task<std::vector<E>> cachedList(QueryFn&& query, KeyArgs&&... keyParts) {
            auto cacheKey = makeListCacheKey(std::forward<KeyArgs>(keyParts)...);

            std::optional<std::vector<E>> cached;
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
        static io::Task<std::vector<E>> cachedListTracked(
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
        static io::Task<std::vector<E>> cachedListTrackedWithHeader(
            QueryFn&& query,
            int limit,
            int offset,
            HeaderBuilder&& headerBuilder,
            GroupArgs&&... groupParts)
        {
            std::string groupKey = makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
            std::string cacheKey = groupKey + ":limit:" + std::to_string(limit)
                                            + ":offset:" + std::to_string(offset);

            std::optional<std::vector<E>> cached;
            if constexpr (Cfg.l2_refresh_on_get) {
                cached = co_await getListFromRedisEx(cacheKey);
            } else {
                cached = co_await getListFromRedis(cacheKey);
            }

            if (cached) {
                co_return std::move(*cached);
            }

            auto results = co_await query();

            std::optional<list::ListBoundsHeader> header;
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
            std::optional<list::ListBoundsHeader> header;
            if constexpr (!std::is_null_pointer_v<std::decay_t<HeaderBuilder>>) {
                header = headerBuilder(listEntity, limit, offset);
            }

            // Store in L2 (binary, with optional header) and track the key
            co_await cache::RedisCache::setListBinary(cacheKey, listEntity, l2Ttl(), header);
            co_await cache::RedisCache::trackListKey(groupKey, cacheKey, l2Ttl());

            co_return listEntity;
        }

#ifdef RELAIS_BUILDING_TESTS
    public:
        friend struct ::relais_test::TestInternals;
#endif
};

}  // namespace jcailloux::relais

#endif //JCX_RELAIS_REDISREPO_H
