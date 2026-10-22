#ifndef JCX_RELAIS_LIST_MIXIN_H
#define JCX_RELAIS_LIST_MIXIN_H

#include <cassert>
#include <mutex>
#include <utility>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"

#include "jcailloux/relais/PgProvider.h"
#include "jcailloux/relais/repository/PgRepo.h"
#include "jcailloux/relais/repository/KeyDedup.h"
#include "jcailloux/relais/repository/Precaution.h"
#include "jcailloux/relais/repository/RowMatch.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/list/ListCache.h"
#include "jcailloux/relais/list/ListQuery.h"
#include "jcailloux/relais/list/spec/ListDescriptor.h"
#include "jcailloux/relais/list/spec/ListDescriptorQuery.h"
#include "jcailloux/relais/list/spec/GeneratedFilters.h"
#include "jcailloux/relais/list/spec/GeneratedTraits.h"
#include "jcailloux/relais/list/spec/GeneratedCriteria.h"
#include "jcailloux/relais/list/spec/HttpQueryParser.h"
#include "jcailloux/relais/list/spec/ListQueryBuilder.h"
#include "jcailloux/relais/entity/EntityConcepts.h"
#include "jcailloux/relais/list/ListWrapper.h"
#include "jcailloux/relais/cache/Metrics.h"

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais {

namespace detail {

/// Per-thread buffer the L1 lookup of a list query encodes its page key into,
/// shared by every repository. It is valid only from the encoding to the
/// lookup, with no suspension in between: a key that must outlive the lookup
/// (the store after a miss) is encoded into its own string.
inline std::string& listKeyScratch() noexcept {
    thread_local std::string key;
    key.clear();
    return key;
}

/// Release the buffer after a lookup once an oversized key (a large IN set)
/// has grown it, so that a thread does not retain that memory.
inline void trimListKeyScratch(std::string& key) noexcept {
    constexpr size_t kMaxRetained = 4096;
    if (key.capacity() > kMaxRetained) std::string().swap(key);
}

}  // namespace detail

/**
 * Optional mixin layer for declarative list caching.
 *
 * Activated when Entity has a ListDescriptor (detected via HasListDescriptor concept).
 * Sits in the mixin chain between the cache layer and InvalidationMixin.
 *
 * Chain: [InvalidationMixin] -> ListMixin -> [LocalRepo] -> PgRepo
 *
 * Provides:
 * - query()           : paginated list queries with L1 caching
 * - CRUD interception : automatically invalidates list caches on insert/update/erase
 * - warmup()          : primes both entity and list L1 caches
 *
 * L1 uses ListCache (ChunkMap-based) with ModificationTracker for lazy invalidation.
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
        Base::config.cache_level == config::CacheLevel::L1;

    static constexpr bool kInsertKeyKnown = relais::kInsertKeyKnown<Entity>;

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

        // Ranges of rows (predicate writes) — see GeneratedTraits. Enable
        // ListCache's range consumption.
        using RowsRange = list::spec::RowsRange<Descriptor>;

        static std::optional<list::spec::SortRange> rowsSortRange(
            const RowsRange& rows, const Filters& group, size_t field_index) {
            return list::spec::rowsSortRange<Descriptor>(rows, group, field_index);
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
        // external consumers) when the model
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

    /// Enrolled in the maintenance sweep, which drains the modification
    /// tracker, with or without GDSF.
    static ListCacheType& listCache() {
        struct Holder {
            std::string name = std::string(Base::name()) + ":list";
            ListCacheType instance{listCacheConfig()};
            Holder() {
                instance.tier().enroll({
                    .sweep_fn = +[](long chunk_id) -> bool {
                        return listCache().sweep(chunk_id);
                    },
                    .size_fn = +[]() -> size_t {
                        return listCache().size();
                    },
                    .name = name.c_str()
                });
            }
        };
        static Holder h;
        return h.instance;
    }

    // Epoch pool for list wrappers (non-L1 paths only)
    static epoch::memory_pool<ListWrapperType>& listPool() {
        // Force epoch_s + ThreadIdPool construction before the pool, so they
        // outlive ~memory_pool() at static teardown (see PgRepo::pool()).
        static const int deps [[maybe_unused]] =
            (epoch::internal::get_epoch(), parlay::num_thread_ids(), 0);
        static epoch::memory_pool<ListWrapperType> p;
        return p;
    }

    static cache::CacheView<ListWrapperType> makeListView(ListWrapperType&& w) {
        auto guard = epoch::EpochGuard::acquire();
        auto* ptr = listPool().New(std::move(w));
        listPool().Retire(ptr);
        return cache::CacheView<ListWrapperType>(ptr, std::move(guard));
    }

    // =========================================================================
    // Query types
    // =========================================================================

    using CacheQuery = list::ListQuery<DescriptorFilters, size_t>;

    static CacheQuery toCacheQuery(const auto& q, std::string page_key) {
        CacheQuery cq;
        cq.filters = q.filters();
        cq.limit = q.limit();
        cq.cursor = q.cursor().raw();
        cq.offset = q.offset();
        cq.cache_key = std::move(page_key);
        if (q.sort()) {
            cq.sort = *q.sort();
        }
        return cq;
    }

    /// L1 lookup of a query's page, keyed through the per-thread buffer. Not a
    /// coroutine: nothing can suspend between the encoding and the lookup, so
    /// no other query can overwrite the buffer while it is in use. The view
    /// returned points to the cache entry, never to the key.
    static cache::CacheView<ListWrapperType> findInL1(
            const list::spec::ListQuery<Descriptor>& q) {
        auto& key = detail::listKeyScratch();
        list::spec::appendPageKey<Descriptor>(key, q);
        auto cached = listCache().getByKey(key);
        detail::trimListKeyScratch(key);
        return cached;
    }

    /// Convert spec::defaultSort → list::SortSpec<size_t>
    static list::SortSpec<size_t> defaultSortAsListSpec() {
        auto ds = list::spec::defaultSort<Descriptor>();
        return {ds.field_index, ds.direction};
    }

public:
#if RELAIS_ENABLE_METRICS
    static inline cache::L1Counters list_l1_counters_{};
#endif

    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::FindResultType;
    using Base::name;
    using Base::find;

    /// Augmented descriptor — pass to parseListQueryStrict<ListDescriptorType>(req)
    using ListDescriptorType = Descriptor;

    /// Sealed list query type — the sole argument query() accepts. Produced by
    /// seal()/builder; compatible with parseListQueryStrict's return type.
    using ListQuery = list::spec::ListQuery<Descriptor>;

    /// Mutable params bundle — fill then seal() into a ListQuery.
    using ListQueryParams = list::spec::ListQueryParams<Descriptor>;

    /// Fluent builder type — accumulates params, build() seals into a ListQuery.
    using QueryBuilder = list::spec::ListQueryBuilder<Descriptor>;

    /// Descriptor-tagged keyset cursor — decode a server-minted token with
    /// Cursor::decode(token), pass it to queryBuilder().after(). A cursor from
    /// another repo's descriptor is rejected at compile time; a token of the
    /// wrong width, at decode (nullopt).
    using Cursor = list::spec::TypedCursor<Descriptor>;

    /// Open a fluent builder: the primary, name-checked construction path for a
    /// ListQuery. Sets filters/sort by name (compile-time verified), exact limit
    /// (trusted path — no grid), and the keyset cursor; .build() seals.
    ///   auto q = Repo::queryBuilder().filter<"gallery_id">(gid)
    ///               .sortDesc<"created_at">().limit(24).after(cursor).build();
    ///   co_await Repo::query(q);
    [[nodiscard]] static QueryBuilder queryBuilder() noexcept { return QueryBuilder{}; }

    /// List result type — returned by query() (epoch-guarded, zero-copy)
    using ListResult = cache::CacheView<ListWrapperType>;

    /// Traits type — exposed for controllers (sort parsing, limit normalization, etc.)
    using ListTraits = Traits;

    // =========================================================================
    // Query interface
    // =========================================================================

    /// Execute a paginated list query with L1 caching.
    /// L1 hit: zero overhead (Immediate holds ListResult directly, no Task).
    static io::Immediate<ListResult> query(const ListQuery& q) {
        if constexpr (kHasL1) {
            if (auto cached = findInL1(q)) {
                RELAIS_METRICS_INC(list_l1_counters_.hits);
                return std::move(cached);
            }
            RELAIS_METRICS_INC(list_l1_counters_.misses);
        }
        return cachedListQuery(q);
    }

    /// Execute a paginated list query and return raw JSON string.
    /// L1 hit: serialize on demand from cached entities (Immediate, no Task).
    /// Miss: delegates to the entity path (cachedListQuery).
    static io::Immediate<std::string> queryJson(const ListQuery& q) {
        // L1 check: serialize from cached entities
        if constexpr (kHasL1) {
            if (auto cached = findInL1(q)) {
                RELAIS_METRICS_INC(list_l1_counters_.hits);
                return cached->json();
            }
            RELAIS_METRICS_INC(list_l1_counters_.misses);
        }
        return queryJsonSlow(q);
    }

    /// Execute a paginated list query and return raw binary (BEVE).
    /// L1 hit: serialize on demand from cached entities (Immediate, no Task).
    /// Miss: delegates to the entity path (cachedListQuery).
    static io::Immediate<std::vector<uint8_t>> queryBinary(const ListQuery& q)
        requires HasBinarySerialization<Entity>
    {
        // L1 check: serialize from cached entities
        if constexpr (kHasL1) {
            if (auto cached = findInL1(q)) {
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

    /// Insert and invalidate list caches. Uncertain (the INSERT may commit
    /// later): the input stands for the row until the write is settled. Then a
    /// caller-assigned key realigns the pages on the row read again; a key the
    /// database assigns cannot be read back, and every page is invalidated.
    static io::Task<cache::CacheView<Entity>> insert(const Entity& entity)
        requires MutableEntity<Entity> && (!Base::config.read_only)
    {
        cache::CacheView<Entity> result;
        std::exception_ptr uncertain;
        std::shared_ptr<io::SettleTicket> ticket;
        try {
            result = co_await Base::insert(entity);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            uncertain = std::current_exception();
        }
        if (uncertain) {
            if constexpr (kHasL1) {
                if constexpr (kInsertKeyKnown) {
                    co_await realignUncertain(std::move(ticket), Key(entity.key()), entity);
                } else {
                    listCache().onEntityCreated(entity);
                    co_await detail::onSettled(std::move(ticket),
                        []() -> io::Task<void> { co_await invalidateAllListGroups(); });
                }
            }
            std::rethrow_exception(uncertain);
        }
        if (result) {
            if constexpr (kHasL1) { listCache().onEntityCreated(*result); }
        }
        co_return result;
    }

    /// Insert-or-update and invalidate list caches. The create/update discriminant
    /// is the pre-image: an existing row migrates its list pages (old position →
    /// new), an absent one appends (create). The pre-read is best-effort and paid
    /// only because this entity has lists; it is threaded into upsertWithContext so
    /// a stacked InvalidationMixin reuses it instead of reading the row twice.
    static io::Task<cache::CacheView<Entity>> upsert(const Entity& entity)
        requires UpsertableEntity<Entity, Key> && HasUpsertSql<Entity> && (!Base::config.read_only)
    {
        std::optional<Entity> old = co_await findOldBestEffort(entity.key());
        co_return co_await upsertWithContext(entity, old ? &*old : nullptr);
    }

    /// Update entity and invalidate list caches from the row before and after,
    /// which the UPDATE returns: no read before the write.
    static io::Task<std::optional<size_t>> update(const Key& id, const Entity& entity)
        requires MutableEntity<Entity> && HasFullUpdate<Entity> && (!Base::config.read_only)
    {
        auto outcome = co_await updateOutcome<true>(id, entity);
        co_return outcome.affected;
    }

    /// Erase entity and invalidate list caches from the deleted row, which the
    /// DELETE returns: no read before the write.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Base::config.read_only)
    {
        auto outcome = co_await eraseOutcome<true>(id);
        co_return outcome.affected;
    }

    /// Partial update, then list invalidation (only if committed) from the row
    /// before and after, both returned by the write itself: the row leaves the
    /// pages of its old position and enters those of its new one. Uncertain:
    /// see realignUncertain (no estimate: the input is not a row).
    template<bool WithBefore, typename... Updates>
    static io::Task<typename Base::PatchedRow> patchRow(
        const Key& id, std::optional<Entity>* uncertain_before, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        static_assert(WithBefore, "this cascade reads the row before the write");
        typename Base::PatchedRow row;
        std::exception_ptr uncertain;
        std::shared_ptr<io::SettleTicket> ticket;
        try {
            row = co_await Base::template patchRow<true>(
                id, uncertain_before, std::forward<Updates>(updates)...);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            uncertain = std::current_exception();
        }
        if (uncertain) {
            if constexpr (kHasL1) {
                keepPageCopy(id, uncertain_before);
                co_await realignUncertain(std::move(ticket), id, std::nullopt);
            }
            std::rethrow_exception(uncertain);
        }
        co_await invalidateListsPatched(row);
        co_return row;
    }

    /// Guarded partial update, then the same list invalidation as patchRow. A
    /// refused guard changed nothing: the pages stay valid.
    template<bool WithBefore, typename... Gs, typename... Updates>
    static io::Task<std::optional<typename Base::PatchedRow>> patchIfRow(
        const Key& id, std::optional<Entity>* uncertain_before,
        const entity::Guards<Gs...>& guard, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Base::config.read_only)
    {
        static_assert(WithBefore, "this cascade reads the row before the write");
        std::optional<typename Base::PatchedRow> row;
        std::exception_ptr uncertain;
        std::shared_ptr<io::SettleTicket> ticket;
        try {
            row = co_await Base::template patchIfRow<true>(
                id, uncertain_before, guard, std::forward<Updates>(updates)...);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            uncertain = std::current_exception();
        }
        if (uncertain) {
            if constexpr (kHasL1) {
                keepPageCopy(id, uncertain_before);
                co_await realignUncertain(std::move(ticket), id, std::nullopt);
            }
            std::rethrow_exception(uncertain);
        }
        if (row) co_await invalidateListsPatched(*row);
        co_return row;
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

    /// Full-pattern target of InvalidateListVia (the resolver returned
    /// nullopt). Records ONE RangeModification with an empty predicate on the
    /// L1 tracker: every filter is a wildcard and the sort range is full, so
    /// every cached page is invalidated lazily, in O(1). Returns 0: the pages
    /// are dropped on their next read or sweep, not counted here.
    static io::Task<size_t> invalidateAllListGroups() {
        if constexpr (kHasL1) {
            listCache().onRowsRanged({});
        }
        co_return 0;
    }

    // =========================================================================
    // Cross-invalidation entry points
    // =========================================================================
    //
    // Synchronous API: L1 invalidation inline. Called by InvalidateList<>
    // cross-invalidation and external sync callers.

    static void notifyCreated(const Entity& entity) {
        if constexpr (kHasL1) { listCache().onEntityCreated(entity); }
    }

    static void notifyUpdated(const Entity& old_entity, const Entity& new_entity) {
        if constexpr (kHasL1) { listCache().onEntityUpdated(old_entity, new_entity); }
    }

    static void notifyDeleted(const Entity& entity) {
        if constexpr (kHasL1) { listCache().onEntityDeleted(entity); }
    }

    // Uncertain writes of rows this repo pages, by a repo that reaches its
    // pages through InvalidateList: the write may commit until settled. Each
    // runs now and again once settled.

    /// The row `key` left every page holding it.
    static void notifyRowLeft(const std::shared_ptr<io::SettleTicket>& ticket, const Key& key) {
        if constexpr (kHasL1) detail::precautionSync(ticket, [key] { listCache().onRowLeft(key); });
    }

    /// Rows `rows` may hold for left every page holding one of them (every
    /// page, if empty) and, for the values `updates` set, may enter the
    /// pages of the groups that filter on, or sort by, a column they set.
    template<typename... Updates>
    static void notifyRowsChanged(const std::shared_ptr<io::SettleTicket>& ticket,
                                  const detail::RowPredicate<Entity>& rows,
                                  const Updates&... updates) {
        if constexpr (kHasL1) {
            if (rows) {
                detail::precautionSync(ticket, [rows] {
                    listCache().onRowsLeft([rows](const Entity& e) {
                        return rows(e) == detail::RowMatch::Maybe;
                    });
                });
            } else {
                detail::precautionSync(ticket, [] { listCache().onRowsRanged({}); });
            }
            if constexpr (sizeof...(Updates) > 0) {
                auto moved = rowsMoved(updates...);
                if (moved.moved_filters != 0 || moved.moved_sorts != 0)
                    detail::precautionSync(ticket, [moved] { listCache().onRowsRanged(moved); });
            }
        }
    }

    /// Any row may have changed: every page.
    static void notifyEveryRow(const std::shared_ptr<io::SettleTicket>& ticket) {
        notifyRowsChanged(ticket, {});
    }

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
#endif

protected:
    // =========================================================================
    // WithContext variants — accept pre-fetched old entity from upper mixin
    // =========================================================================

    /// Batch invalidation common path — list tier. When WithLists (eraseMany),
    /// invalidates the list pages touched by every affected entity (L1 tracker)
    /// — the entity left the table, like mono erase. When !WithLists
    /// (invalidateMany), the entity still exists so its lists are left intact,
    /// matching mono invalidate (ListMixin::invalidate is a no-op on lists).
    template<bool WithLists = true>
    static io::Task<void> invalidateManyCritical(std::span<const Entity> entities) {
        // The synchronous tracker bump is what guards a concurrent L1 list
        // read-fill against re-caching a page the deleted rows belonged to (the
        // tracker is consulted on every list GET).
        if constexpr (WithLists && kHasL1) {
            for (const auto& e : entities) listCache().onEntityDeleted(e);
        }
        co_await Base::template invalidateManyCritical<WithLists>(entities);
    }

    /// Rows changed in place (patchWhere): each row leaves the pages of its
    /// version before the write and joins those of the committed one.
    static io::Task<void> invalidateManyUpdatedCritical(std::span<const Change<Entity>> changes) {
        if constexpr (kHasL1) {
            for (const auto& c : changes) listCache().onEntityUpdated(c.before, c.after);
        }
        co_await Base::invalidateManyUpdatedCritical(changes);
    }

    /// Predicate list invalidation (eraseWhere fast-path). Replaces the per-entity
    /// list work with ONE RangeModification (L1) for the whole deleted set — O(1)
    /// instead of O(N). The caller (Repo::eraseWhere) runs the entity tier
    /// separately with WithLists=false. The predicate arrives as Filters<Desc> where Desc is
    /// the entity's standalone FilterSet; its value tuple is identical to this
    /// repo's ListDescriptor filter tuple (both inherit the same FilterSet), so it
    /// copies straight across.
    template<typename Desc>
    static io::Task<void> invalidateWhereListsCritical(
        const list::spec::Filters<Desc>& predicate) {
        // ONE RangeModification on the L1 tracker (RAM, synchronous) — the
        // guard for L1 list reads, like the per-entity bump.
        if constexpr (kHasL1) {
            typename Traits::RowsRange rows;
            rows.predicate.values = predicate.values;
            listCache().onRowsRanged(std::move(rows));
        }
        co_await Base::template invalidateWhereListsCritical<Desc>(predicate);
    }

    /// Best-effort pre-read of the current entity, used by the top mixin to feed
    /// precise list/cross invalidation. The read path no longer swallows L3
    /// errors, so this pre-read can throw; a failure is treated as "old unknown"
    /// and the caller proceeds with the write + precautionary invalidation rather
    /// than aborting on a preliminary read failure (preserves write liveness —
    /// the precautionary eviction covers the lost `old`).
    static io::Task<std::optional<Entity>> findOldBestEffort(const Key& id) {
        try {
            auto view = co_await Base::find(id);
            if (view) co_return Entity(*view);
        } catch (const io::PgError&) {
            // old unknown — fall through and proceed without it.
        }
        co_return std::nullopt;
    }

    /// Update, then list invalidation from the row before and after (the row
    /// leaves the pages of its old position and enters those of its new one).
    /// Uncertain: see realignUncertain, the input standing for the row until
    /// the write is settled.
    template<bool WithBefore>
    static io::Task<typename Base::UpdateOutcome> updateOutcome(
        const Key& id, const Entity& entity,
        std::optional<Entity>* uncertain_before = nullptr)
        requires MutableEntity<Entity> && HasFullUpdate<Entity> && (!Base::config.read_only)
    {
        static_assert(WithBefore, "this cascade reads the row before the write");
        typename Base::UpdateOutcome outcome;
        std::exception_ptr uncertain;
        std::shared_ptr<io::SettleTicket> ticket;
        try {
            outcome = co_await Base::template updateOutcome<true>(id, entity, uncertain_before);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            uncertain = std::current_exception();
        }
        if (uncertain) {
            if constexpr (kHasL1) {
                keepPageCopy(id, uncertain_before);
                co_await realignUncertain(std::move(ticket), id, entity);
            }
            std::rethrow_exception(uncertain);
        }
        if (outcome.affected.value_or(0) > 0) {
            // The committed row, not the input, is the new position: the
            // database may have changed the row as it wrote it (trigger,
            // generated column, rounding). The input stands in only if the
            // returned rows did not decode.
            const Entity& after = outcome.after ? *outcome.after : entity;
            if constexpr (kHasL1) {
                if (outcome.before) listCache().onEntityUpdated(*outcome.before, after);
                else listCache().onEntityCreated(after);
            }
        }
        co_return outcome;
    }

    /// Erase, then list invalidation from the deleted row (the pages it lived
    /// in). Uncertain: see realignUncertain (no estimate: the row may be gone).
    template<bool WithBefore>
    static io::Task<typename Base::EraseOutcome> eraseOutcome(
        const Key& id, const Entity* hint = nullptr,
        std::optional<Entity>* uncertain_before = nullptr)
        requires (!Base::config.read_only)
    {
        static_assert(WithBefore, "this cascade reads the row before the write");
        typename Base::EraseOutcome outcome;
        std::exception_ptr uncertain;
        std::shared_ptr<io::SettleTicket> ticket;
        try {
            outcome = co_await Base::template eraseOutcome<true>(id, hint, uncertain_before);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            uncertain = std::current_exception();
        }
        if (uncertain) {
            if constexpr (kHasL1) {
                keepPageCopy(id, uncertain_before);
                co_await realignUncertain(std::move(ticket), id, std::nullopt);
            }
            std::rethrow_exception(uncertain);
        }
        if (outcome.before) {
            if constexpr (kHasL1) { listCache().onEntityDeleted(*outcome.before); }
        }
        co_return outcome;
    }

    /// List invalidation of a committed patch (nothing if nothing was written).
    static io::Task<void> invalidateListsPatched(const typename Base::PatchedRow& row) {
        if (!row.after) co_return;
        const Entity& after = *row.after;
        if constexpr (kHasL1) { listCache().onEntityUpdated(*row.before, after); }
        co_return;
    }

    /// For the cascade above (`slot` set), when no tier kept the row before an
    /// uncertain write of it: the row as a cached page holds it (see
    /// ListCache::findRow). Before the row leaves its pages, which would pass
    /// over every page holding it. Best-effort: left empty on failure.
    static void keepPageCopy(const Key& id, std::optional<Entity>* slot) noexcept {
        if constexpr (kHasL1) {
            if (!slot || *slot) return;
            try {
                *slot = listCache().findRow(id);
            } catch (...) {
            }
        }
    }

    /// The pages of an uncertain write of the row `id` (it may commit after
    /// this, until the write is settled). The row leaves every page holding
    /// it, whatever version they hold: now, and again once settled. Until
    /// then, `estimate` (the input, when it is a whole row) stands for where it
    /// may enter. Once settled, the row read again enters the pages of its
    /// position as committed (realignOnSettled).
    static io::Task<void> realignUncertain(std::shared_ptr<io::SettleTicket> ticket, Key id,
                                           std::optional<Entity> estimate) {
        detail::precautionSync(ticket, [id] { listCache().onRowLeft(id); });
        if (estimate) listCache().onEntityCreated(*estimate);
        co_await detail::onSettled(std::move(ticket), [id] { return realignOnSettled(id); });
    }

    /// Rows an uncertain write may have erased (see
    /// PgRepo::rowsMayHaveBeenErased): the tiers below, then, for the keys
    /// they kept no copy of, the copies the pages hold (see keepPageCopy);
    /// then the rows leave every page holding one of them, in one entry.
    static io::Task<void> rowsMayHaveBeenErased(std::shared_ptr<io::SettleTicket> ticket,
                                                std::span<const Key> keys,
                                                std::vector<std::optional<Entity>>* before) {
        co_await Base::rowsMayHaveBeenErased(ticket, keys, before);
        if constexpr (kHasL1) {
            for (size_t i = 0; i < keys.size(); ++i)
                keepPageCopy(keys[i], before ? &(*before)[i] : nullptr);
            auto rows = std::make_shared<const detail::KeySet<Key>>(keys);
            detail::precautionSync(ticket, [rows] {
                listCache().onRowsLeft([rows](const Entity& e) { return rows->contains(Key(e.key())); });
            });
        }
    }

    /// Rows an uncertain predicate write may have changed (see
    /// PgRepo::rowsMayHaveChanged): their pages (see notifyRowsChanged; the
    /// pages read before the write is settled are checked like the others),
    /// then the tiers below.
    template<typename... Updates>
    static void rowsMayHaveChanged(const std::shared_ptr<io::SettleTicket>& ticket,
                                   const detail::RowPredicate<Entity>& rows,
                                   const Updates&... updates) {
        notifyRowsChanged(ticket, rows, updates...);
        Base::rowsMayHaveChanged(ticket, rows, updates...);
    }

    /// Where rows `updates` were applied to may now be (see RowsRange).
    template<typename... Updates>
    static typename Traits::RowsRange rowsMoved(const Updates&... updates) {
        typename Traits::RowsRange rows;
        rows.moved = true;
        (moveRows(rows, updates), ...);
        return rows;
    }

    template<typename T, typename V>
    static std::optional<T> setAs(const V& value) {
        if constexpr (std::is_same_v<T, V>) return value;
        else if constexpr ((std::is_arithmetic_v<T> || std::is_enum_v<T>)
                           && std::is_convertible_v<V, T>)
            return static_cast<T>(value);
        else if constexpr (std::is_same_v<T, std::string>
                           && std::is_constructible_v<std::string, const V&>)
            return std::string(value);
        else return std::nullopt;
    }

    template<typename U>
    static void moveRows(typename Traits::RowsRange& rows, const U& update) {
        constexpr auto F = entity::update_field_v<U>;
        constexpr std::string_view column = detail::match::bareColumn(
            Entity::TraitsType::template FieldInfo<F>::column_name);
        constexpr bool sets_null = std::is_same_v<U, entity::FieldSetNull<F>>;

        [&]<size_t... Is>(std::index_sequence<Is...>) {
            ([&] {
                using FT = list::spec::filter_at<Descriptor, Is>;
                if constexpr (FT::column() == column) {
                    rows.moved_filters |= uint64_t{1} << Is;
                    auto& value = std::get<Is>(rows.predicate.values);
                    if constexpr (FT::is_null_op) {
                        if constexpr (sets_null) value = true;
                        else if constexpr (entity::assigns_value_v<U>) value = false;
                    } else if constexpr (entity::assigns_value_v<U>) {
                        using T = typename FT::element_type;
                        if (auto v = setAs<T>(update.value)) {
                            if constexpr (FT::op == list::spec::Op::IN)
                                value = std::vector<T>{std::move(*v)};
                            else if constexpr (FT::op == list::spec::Op::EQ)
                                value = std::move(*v);
                        }
                    }
                }
            }(), ...);
        }(std::make_index_sequence<list::spec::filter_count<Descriptor>>{});

        [&]<size_t... Ss>(std::index_sequence<Ss...>) {
            ([&] {
                using ST = list::spec::sort_at<Descriptor, Ss>;
                if constexpr (ST::column() == column) {
                    rows.moved_sorts |= uint64_t{1} << Ss;
                    if constexpr (entity::assigns_value_v<U>) {
                        using W = list::spec::detail::unwrap_optional_t<typename ST::value_type>;
                        if (auto v = setAs<W>(update.value)) {
                            const int64_t x = list::spec::detail::toInt64ForCursor(*v);
                            rows.sorts[Ss] = {x, x};
                        }
                    }
                }
            }(), ...);
        }(std::make_index_sequence<list::spec::sort_count<Descriptor>>{});
    }

    /// The row as committed, once an uncertain write of it is settled: it
    /// enters the pages of its position (it may differ from the caller's
    /// version: trigger, generated column, rounding). If it cannot be read,
    /// nothing tells that position: every page is invalidated.
    static io::Task<void> realignOnSettled(Key id) {
        std::optional<Entity> row;
        try {
            row = co_await Base::rereadRaw(id);
        } catch (const std::exception& e) {
            RELAIS_LOG_WARN << name() << ": row unreadable after an uncertain write, "
                               "every list page invalidated - " << e.what();
            listCache().onRowsRanged({});
            co_return;
        }
        if (row) listCache().onEntityCreated(*row);
    }

    static io::Task<cache::CacheView<Entity>> upsertWithContext(
        const Entity& entity, const Entity* old_entity)
        requires UpsertableEntity<Entity, Key> && HasUpsertSql<Entity> && (!Base::config.read_only)
    {
        cache::CacheView<Entity> result;
        std::exception_ptr uncertain;
        std::shared_ptr<io::SettleTicket> ticket;
        try {
            result = co_await Base::upsert(entity);
        } catch (const io::PgUncertainError& e) {
            ticket = e.settleTicket();
            uncertain = std::current_exception();
        }
        if (uncertain) {
            // Either branch may commit: the key is known in both.
            if constexpr (kHasL1)
                co_await realignUncertain(std::move(ticket), Key(entity.key()), entity);
            std::rethrow_exception(uncertain);
        }
        if (result) {
            // Committed: an existing row migrates (old → the RETURNING row), an
            // absent one appends. The RETURNING row carries DB-computed columns, so
            // it — not the input — is the authoritative new list position.
            if constexpr (kHasL1) {
                if (old_entity) listCache().onEntityUpdated(*old_entity, *result);
                else listCache().onEntityCreated(*result);
            }
        }
        co_return result;
    }

    // =========================================================================
    // Slow paths for queryJson / queryBinary (L1 miss → DB)
    // =========================================================================

    /// Slow path for queryJson(): L1 miss → DB fetch.
    static io::Task<std::string> queryJsonSlow(const ListQuery& q) {
        // Miss: entity path (needed for cursor/bounds/L1 population)
        auto wrapper = co_await cachedListQuery(q);
        if (!wrapper) co_return std::string{};
        co_return wrapper->json();
    }

    /// Slow path for queryBinary(): L1 miss → DB fetch.
    static io::Task<std::vector<uint8_t>> queryBinarySlow(const ListQuery& q)
        requires HasBinarySerialization<Entity>
    {
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

        // Owned page key, encoded before the first suspension: L1 check and store.
        [[maybe_unused]] std::string page_key;

        // 1. L1 check — epoch-guarded view, zero-copy
        if constexpr (kHasL1) {
            list::spec::appendPageKey<Descriptor>(page_key, query);
            if (auto cached = listCache().getByKey(page_key))
                co_return std::move(cached);
        }

        auto start = Clock::now();

        // Generation before any fetch: every modification notified from here on
        // is checked against the page when it is stored.
        [[maybe_unused]] uint32_t fetch_gen = 0;
        if constexpr (kHasL1) fetch_gen = listCache().generation();

        // 2. Cache miss — query database
        auto entities = co_await queryFromDb(query);
        auto sort = query.sort().value_or(defaultSortAsListSpec());

        auto elapsed_us = static_cast<float>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                Clock::now() - start).count());

        // Build ListWrapper directly from entities
        ListWrapperType wrapper;
        wrapper.items = std::move(entities);

        // Set cursor for pagination (base64 string in ListWrapper)
        if (wrapper.items.size() >= query.limit() && !wrapper.items.empty()) {
            auto cache_sort = query.sort().value_or(Traits::defaultSort());
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

        // 3. Store in L1 cache or epoch pool
        if constexpr (kHasL1) {
            co_return listCache().put(toCacheQuery(query, std::move(page_key)),
                                      std::move(wrapper), fetch_gen, bounds, elapsed_us);
        } else {
            co_return makeListView(std::move(wrapper));
        }
    }

    // =========================================================================
    // Database query — direct SQL via PgClient
    // =========================================================================

    /// The page's statement and its parameters. The text depends only on the
    /// query's shape (active filters, sort, cursor, presence of an offset):
    /// limit and offset are parameters, so a shape is one prepared statement per
    /// connection, and the interned pointer lets identical reads in flight
    /// coalesce.
    static std::pair<const char*, io::PgParams> querySql(const ListQuery& query) {
        // Build WHERE clause from filters
        auto where = list::spec::buildWhereClause<Descriptor>(query.filters());

        // Parse sort
        auto sort = query.sort().value_or(defaultSortAsListSpec());
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
        // the full primary key — a PostgreSQL row-value comparison. The width
        // is checked by Cursor::decode, the only source of a non-empty cursor.
        const auto& cursor_data = query.cursor().raw().data;
        assert(cursor_data.empty()
            || cursor_data.size() == sizeof(int64_t) * (1 + kKeyN));
        if (!cursor_data.empty()) {
            int64_t cursor_sort_value = 0;
            std::memcpy(&cursor_sort_value, cursor_data.data(),
                        sizeof(cursor_sort_value));
            int64_t cursor_keys[kKeyN];
            for (size_t i = 0; i < kKeyN; ++i) {
                std::memcpy(&cursor_keys[i],
                            cursor_data.data() + sizeof(int64_t) * (1 + i),
                            sizeof(int64_t));
            }

            if (!where.sql.empty()) where.sql += " AND ";
            where.sql += "(COALESCE(";
            list::spec::appendSortExpression<Descriptor>(where.sql, sort.field);
            where.sql += ", 0)";
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

            where.params.params.push_back(io::PgParam::number(cursor_sort_value));
            for (size_t i = 0; i < kKeyN; ++i) {
                where.params.params.push_back(io::PgParam::number(cursor_keys[i]));
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
        sql += " ORDER BY COALESCE(";
        list::spec::appendSortExpression<Descriptor>(sql, sort.field);
        sql += ", 0) ";
        sql += is_desc ? "DESC" : "ASC";
        appendKeyColumns(sql, is_desc ? "DESC" : "ASC");
        sql += " LIMIT $";
        sql += std::to_string(where.next_param++);
        where.params.params.push_back(io::PgParam::number(query.limit()));

        // Offset pagination (mutually exclusive with cursor). No offset is
        // its own shape rather than OFFSET $n bound to 0.
        if (query.offset() > 0 && query.cursor().empty()) {
            sql += " OFFSET $";
            sql += std::to_string(where.next_param++);
            where.params.params.push_back(io::PgParam::number(query.offset()));
        }

        return {detail::internSql(std::move(sql)), std::move(where.params)};
    }

    static io::Task<std::vector<Entity>> queryFromDb(const ListQuery& query) {
        try {
            auto [sql, params] = querySql(query);
            auto result = co_await PgProvider::queryParams(sql, params);

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
            // Propagate: caching {} here would negative-cache a false "no results"
            // until TTL (permanent under l1_ttl=0). Mirrors the entity read path.
            RELAIS_LOG_ERROR << name() << ": queryFromDb error - " << e.what();
            throw;
        }
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_LIST_MIXIN_H
