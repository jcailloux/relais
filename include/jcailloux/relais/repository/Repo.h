#ifndef JCX_RELAIS_REPO_H
#define JCX_RELAIS_REPO_H

#include <concepts>
#include <optional>
#include <span>
#include <type_traits>
#include <vector>
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/WhenAll.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/repository/LocalRepo.h"
#include "jcailloux/relais/repository/InvalidationMixin.h"
#include "jcailloux/relais/repository/ListMixin.h"
#include "jcailloux/relais/config/FixedString.h"
#include "jcailloux/relais/entity/EntityConcepts.h"
#include "jcailloux/relais/list/spec/GeneratedFilters.h"
#include "jcailloux/relais/cache/Metrics.h"
#include "jcailloux/relais/cache/GDSFPolicy.h"

namespace jcailloux::relais {

// =============================================================================
// CacheLayerSelector + MixinStack — assemble the mixin chain from template params
// =============================================================================
//
// Chain (bottom to top):
//   PgRepo
//     ↑ (if L2 or L1_L2)
//   RedisRepo
//     ↑ (if L1 or L1_L2)
//   LocalRepo
//     ↑ (if E has ListDescriptor)
//   ListMixin
//     ↑ (if Invalidations... non-empty)
//   InvalidationMixin
//
// The final Repo class sits on top and adds convenience methods.
//

namespace detail {

/// Select the cache layer based on CacheConfig::cache_level
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
struct CacheLayerSelector {
    using type = std::conditional_t<
        Cfg.cache_level == config::CacheLevel::L1 || Cfg.cache_level == config::CacheLevel::L1_L2,
        LocalRepo<E, Name, Cfg, Key>,
        std::conditional_t<
            Cfg.cache_level == config::CacheLevel::L2,
            RedisRepo<E, Name, Cfg, Key>,
            PgRepo<E, Name, Cfg, Key>
        >
    >;
};

/// Stack optional mixins on top of the cache layer
template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key,
         typename... Invalidations>
struct MixinStack {
    using CacheLayer = typename CacheLayerSelector<E, Name, Cfg, Key>::type;

    using WithList = std::conditional_t<
        HasListDescriptor<E>,
        ListMixin<CacheLayer>,
        CacheLayer
    >;

    using type = std::conditional_t<
        (sizeof...(Invalidations) > 0),
        InvalidationMixin<WithList, Invalidations...>,
        WithList
    >;
};

}  // namespace detail

// =============================================================================
// Repo — final class with convenience methods
// =============================================================================
//
// Usage:
//   using MyRepo = Repo<MyEntity, "MyEntity">;                      // L1 (default)
//   using MyRepo = Repo<MyEntity, "MyEntity", config::Both>;        // L1+L2
//   using MyRepo = Repo<MyEntity, "MyEntity", config::Local,
//       Invalidate<OtherRepo, &MyStruct::other_id>>;                // with cross-inv
//

template<typename E, config::FixedString Name, config::CacheConfig Cfg = config::Local,
         typename... Invalidations>
class Repo
    : public detail::MixinStack<
          E, Name, Cfg,
          decltype(std::declval<const E>().key()),
          Invalidations...
      >::type
{
    using Key = decltype(std::declval<const E>().key());
    using Base = typename detail::MixinStack<E, Name, Cfg, Key, Invalidations...>::type;

    // Augment the entity's standalone FilterSet with the Entity alias so it
    // satisfies ValidFilterSet (the filter core needs Descriptor::Entity).
    // A nested template so it is only instantiated for HasFilterSet entities —
    // a bare nested struct would hard-error on entities without a FilterSet.
    template<typename EE>
    struct FilterDescriptorFor : EE::MappingType::FilterSet {
        using Entity = EE;
    };

    // Default predicate type for the where-variants' template parameter. The
    // user-facing FilterSet<E> alias is a hard-requires type — used as a default
    // template argument it would be instantiated (and ill-formed) at *class*
    // instantiation for entities without a FilterSet, before the requires-clause
    // could exclude the method. This resolves to the aggregate when present and a
    // harmless dummy otherwise, keeping the declaration well-formed; the
    // requires-clause then removes the method for non-FilterSet entities.
    template<typename EE, bool = HasFilterSet<EE>>
    struct WherePredicate { using type = int; };
    template<typename EE>
    struct WherePredicate<EE, true> {
        using type = typename EE::MappingType::FilterSet::Values;
    };

    // Compile-time validation
    static_assert(ReadableEntity<E>,
        "E must satisfy ReadableEntity (provide fromRow)");

    static_assert(
        Cfg.cache_level == config::CacheLevel::None ||
        CacheableEntity<E>,
        "Cached entities must satisfy CacheableEntity (provide JSON or binary serialization)");

    static_assert(
        Cfg.cache_level != config::CacheLevel::L1 &&
        Cfg.cache_level != config::CacheLevel::L1_L2 ||
        (Cfg.l1_chunk_count_log2 >= 1),
        "L1 cache requires l1_chunk_count_log2 >= 1");

    static_assert(
        Cfg.cache_level != config::CacheLevel::L2 &&
        Cfg.cache_level != config::CacheLevel::L1_L2 ||
        Cfg.l2_ttl.ns > 0,
        "L2 cache requires l2_ttl > 0");

public:
    using typename Base::EntityType;
    using typename Base::KeyType;
    using typename Base::WrapperType;
    using typename Base::FindResultType;

    // Re-export all Base methods via using declarations
    using Base::name;
    using Base::find;
    using Base::config;

#if RELAIS_ENABLE_METRICS
    // =======================================================================
    // Metrics — aggregated from all active cache layers
    // =======================================================================

    [[nodiscard]] static cache::MetricsSnapshot metrics() {
        cache::MetricsSnapshot snap{};

        // L1 entity counters
        if constexpr (Cfg.cache_level == config::CacheLevel::L1
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using CachedLayer = LocalRepo<E, Name, Cfg, Key>;
            snap.l1_hits   = CachedLayer::l1_counters_.hits.load();
            snap.l1_misses = CachedLayer::l1_counters_.misses.load();
        }

        // L2 entity counters
        if constexpr (Cfg.cache_level == config::CacheLevel::L2
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using RedisLayer = RedisRepo<E, Name, Cfg, Key>;
            snap.l2_hits   = RedisLayer::l2_counters_.hits.load();
            snap.l2_misses = RedisLayer::l2_counters_.misses.load();
        }

        // List counters
        if constexpr (HasListDescriptor<E>) {
            using CacheLayer = typename detail::CacheLayerSelector<E, Name, Cfg, Key>::type;
            using ListLayer = ListMixin<CacheLayer>;
            snap.list_l1_hits   = ListLayer::list_l1_counters_.hits.load();
            snap.list_l1_misses = ListLayer::list_l1_counters_.misses.load();
            snap.list_l2_hits   = ListLayer::list_l2_counters_.hits.load();
            snap.list_l2_misses = ListLayer::list_l2_counters_.misses.load();
        }

        // Sweep counters (global, shared across all repos)
        auto& sc = cache::GDSFPolicy::instance().sweepCounters();
        snap.sweep_count    = sc.count.load(std::memory_order_relaxed);
        snap.sweep_total_ns = sc.total_ns.load(std::memory_order_relaxed);
        snap.sweep_last_ns  = sc.last_ns.load(std::memory_order_relaxed);
        snap.sweep_max_ns   = sc.max_ns.load(std::memory_order_relaxed);

        return snap;
    }

    static void resetMetrics() {
        if constexpr (Cfg.cache_level == config::CacheLevel::L1
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using CachedLayer = LocalRepo<E, Name, Cfg, Key>;
            CachedLayer::l1_counters_.hits.reset();
            CachedLayer::l1_counters_.misses.reset();
        }

        if constexpr (Cfg.cache_level == config::CacheLevel::L2
                    || Cfg.cache_level == config::CacheLevel::L1_L2) {
            using RedisLayer = RedisRepo<E, Name, Cfg, Key>;
            RedisLayer::l2_counters_.hits.reset();
            RedisLayer::l2_counters_.misses.reset();
        }

        if constexpr (HasListDescriptor<E>) {
            using CacheLayer = typename detail::CacheLayerSelector<E, Name, Cfg, Key>::type;
            using ListLayer = ListMixin<CacheLayer>;
            ListLayer::list_l1_counters_.hits.reset();
            ListLayer::list_l1_counters_.misses.reset();
            ListLayer::list_l2_counters_.hits.reset();
            ListLayer::list_l2_counters_.misses.reset();
        }

        // Sweep counters (global)
        cache::GDSFPolicy::instance().sweepCounters().reset();
    }
#endif

    // =======================================================================
    // Convenience methods (need correct dispatch through method hiding)
    // =======================================================================

    /// Update entity from JSON string.
    /// Parses JSON to create entity, then updates via the full mixin chain.
    /// Returns: rows affected (0 if not found), or nullopt on DB error or parse failure.
    static io::Task<std::optional<size_t>> updateJson(const Key& id, std::string_view json)
        requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
    {
        auto entity_opt = E::fromJson(json);
        if (!entity_opt) {
            RELAIS_LOG_ERROR << name() << ": updateJson failed to parse JSON";
            co_return std::nullopt;
        }
        co_return co_await Base::update(id, *entity_opt);
    }

    /// Update entity from binary data.
    /// Creates entity from binary, then updates via the full mixin chain.
    /// Returns: rows affected (0 if not found), or nullopt on DB error or parse failure.
    static io::Task<std::optional<size_t>> updateBinary(const Key& id, std::span<const uint8_t> buffer)
        requires MutableEntity<E> && HasFullUpdate<E> && HasBinarySerialization<E> && (!Cfg.read_only)
    {
        auto entity_opt = E::fromBinary(buffer);
        if (!entity_opt) {
            RELAIS_LOG_ERROR << name() << ": updateBinary failed to parse binary data";
            co_return std::nullopt;
        }
        co_return co_await Base::update(id, *entity_opt);
    }

    // =======================================================================
    // Batch erase / invalidate (enumerated keys)
    // =======================================================================
    //
    // Entry is span<const Key> — the "set of keys" convention shared with
    // findMany (zero-copy, caller-owned lifetime). Both dedup the input by
    // equality first (composite/partition Keys model == but not <), then drive
    // the shared invalidateManyImpl cascade resolved at the top of the chain.

    /// Delete the enumerated rows from L3, then evict the deleted set across the
    /// cache hierarchy (L1 + L2 + own lists + deduplicated cross-inval). Returns
    /// the number of rows actually deleted (duplicates and absent ids contribute
    /// nothing) — parité mono erase: nullopt signals a DB error, 0 a non-matching
    /// (but valid) set. eraseManyRaw's RETURNING is the source of the affected
    /// entity set, so no extra read is needed.
    static io::Task<std::optional<size_t>> eraseMany(std::span<const Key> ids)
        requires (!Cfg.read_only)
    {
        if (ids.empty()) co_return std::optional<size_t>{0};
        auto keys = detail::dedupStable<Key>(ids);
        // co_await is illegal in a catch handler: capture the uncertain timeout,
        // evict the input key set after the try, then rethrow.
        std::optional<std::vector<E>> deleted;
        std::exception_ptr timeout;
        try {
            deleted = co_await Base::eraseManyRaw(std::span<const Key>(keys));
        } catch (const io::PgQueryTimeout&) {
            timeout = std::current_exception();
        }
        if (timeout) {
            // Uncertain: the batch DELETE may have committed (lost ACK). Evict the
            // known input key set from the entity tier (L1+L2) by precaution — a
            // possibly-deleted row must not stay cached. Per-key invalidate also
            // runs cross-invalidation when the entity is still cached (the common
            // case right after an erase). Fanned out with whenAll so the worst
            // case — keys absent from cache while the DB is frozen, each falling
            // through to a bounded L3 find — costs ONE query_timeout in parallel,
            // not N sequentially (`keys` outlives the gather). Lists are NOT
            // recoverable here: without the row bodies (no RETURNING) their
            // filter/sort values are unknown, so that tier stays bounded by l*_ttl.
            std::vector<io::Task<void>> evictions;
            evictions.reserve(keys.size());
            for (const auto& k : keys) evictions.push_back(Base::invalidate(k));
            co_await io::whenAll(std::move(evictions));
            std::rethrow_exception(timeout);
        }
        if (!deleted) co_return std::nullopt;  // DB error
        // Await the latency-critical cleanup (L1 evict + L2 entity UNLINK + gen
        // bump + L1 list bump), then fire the order-free remainder (cross-target
        // + L2 list EVALs) detached — the caller returns after DELETE + 1 entity
        // RTT, not after the full cascade.
        co_await Base::template invalidateManyCritical<true>(
            std::span<const E>(*deleted));
        auto n = deleted->size();
        fireInvalidateManyDeferred<true>(std::move(*deleted));
        co_return n;
    }

    /// Evict the enumerated entities from the cache hierarchy without deleting
    /// them from L3 — a later find repopulates from the DB. Materializes each
    /// entity through the cache-first read path (exactly mono invalidate's
    /// `find` + propagate), then runs the cascade with WithLists=false: lists are
    /// left intact because the rows still exist (parité mono invalidate). Best
    /// effort, void return: a missing entity simply contributes no eviction.
    static io::Task<void> invalidateMany(std::span<const Key> ids) {
        if (ids.empty()) co_return;
        auto keys = detail::dedupStable<Key>(ids);
        std::vector<E> entities;
        entities.reserve(keys.size());
        for (const auto& k : keys) {
            auto view = co_await Base::find(k);
            if (view) entities.push_back(*view);
        }
        co_await Base::template invalidateManyCritical<false>(
            std::span<const E>(entities));
        fireInvalidateManyDeferred<false>(std::move(entities));
    }

    // =======================================================================
    // Predicate erase / invalidate (where — declared FilterSet required)
    // =======================================================================
    //
    // The predicate is the generated named aggregate FilterSet<E>, built with
    // designated initializers: repo.eraseWhere({.author_id = 42}). It bridges to
    // the positional Filters<Descriptor> the L3 raw methods consume via
    // toFilterTuple() — index-aligned because both follow the same param-sorted
    // filter order. Pred is a defaulted template parameter so the signature stays
    // well-formed (SFINAE) for entities without a FilterSet, and so a braced
    // initializer (a non-deduced context) falls back to FilterSet<E>.

    /// Delete every row matching the predicate from L3, then evict the deleted
    /// set across the cache hierarchy (WithLists=true — the rows leave the table,
    /// so their lists change, exactly like eraseMany). Returns the number of rows
    /// deleted across all K_pg chunks; nullopt signals a DB error.
    template<typename Pred = typename WherePredicate<E>::type>
    static io::Task<std::optional<size_t>> eraseWhere(Pred pred)
        requires HasFilterSet<E>
              && std::same_as<Pred, typename WherePredicate<E>::type>
              && (!Cfg.read_only)
    {
        using FD = FilterDescriptorFor<E>;
        list::spec::Filters<FD> filters;
        filters.values = pred.toFilterTuple();
        std::optional<std::vector<E>> deleted;
        std::exception_ptr timeout;
        try {
            deleted = co_await Base::template eraseWhereRaw<FD>(filters);
        } catch (const io::PgQueryTimeout&) {
            timeout = std::current_exception();
        }
        if (timeout) {
            // Uncertain: a chunk DELETE may have committed. The matched primary
            // keys are unknowable after a lost ACK (a post-hoc SELECT no longer
            // finds a committed delete), so the entity tier is left bounded by
            // l1/l2_ttl (logged). The LIST tier is still recoverable — the
            // predicate drives it directly, no resolved id set needed.
            RELAIS_LOG_ERROR << name()
                << ": eraseWhere timeout — entity tier left to l*_ttl "
                   "(matched keys unknowable), invalidating lists by predicate";
            try {
                co_await Base::template invalidateWhereListsCritical<FD>(filters);
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << name()
                    << ": predicate list invalidate failed (eraseWhere timeout) - "
                    << e.what();
            }
            std::rethrow_exception(timeout);
        }
        if (!deleted) co_return std::nullopt;  // DB error
        // Two disjoint tiers, gathered so their L2 commands share one flush:
        //  - entity tier (L1 evict + L2 UNLINK + cross-inval) per deleted row,
        //    WithLists=false so the OWN-list tier is left to the predicate path;
        //  - own-list tier: ONE RangeModification (L1) + ONE predicate EVAL (L2)
        //    for the whole deleted set — O(1)/O(groups), filter-aware, never-miss.
        // The rows left the table, so their lists change (eraseMany analogue), but
        // the predicate drives that, not the resolved id set. Await the critical
        // tiers (entity L1/L2 evict + ONE L1 RangeModification), then fire the
        // deferred remainder (cross-target + the single predicate L2 list EVAL)
        // detached. `*deleted`/`filters` outlive the await; each critical child
        // evicts L1 before any submit (anti-stale).
        co_await io::whenAll(
            Base::template invalidateManyCritical<false>(std::span<const E>(*deleted)),
            Base::template invalidateWhereListsCritical<FD>(filters));
        auto n = deleted->size();
        fireEraseWhereDeferred<FD>(std::move(*deleted), filters);
        co_return n;
    }

    /// Evict every row matching the predicate from the cache hierarchy without
    /// deleting it from L3 — a later find/list repopulates from the DB. Resolves
    /// the affected set via selectWhereRaw (SELECT WHERE pred), then runs the
    /// cascade with WithLists=false: the rows still exist, so lists stay valid
    /// (parité mono invalidate; oracle invalidateWhere(P) ≡ invalidateMany(ids)).
    template<typename Pred = typename WherePredicate<E>::type>
    static io::Task<void> invalidateWhere(Pred pred)
        requires HasFilterSet<E>
              && std::same_as<Pred, typename WherePredicate<E>::type>
    {
        using FD = FilterDescriptorFor<E>;
        list::spec::Filters<FD> filters;
        filters.values = pred.toFilterTuple();
        auto entities = co_await Base::template selectWhereRaw<FD>(filters);
        if (!entities) co_return;  // DB error → nothing evicted
        co_await Base::template invalidateManyCritical<false>(
            std::span<const E>(*entities));
        fireInvalidateManyDeferred<false>(std::move(*entities));
    }

private:
    // ----------------------------------------------------------------------
    // Fire-and-forget deferred cleanup (commit 13). The detached coroutine owns
    // the affected set / predicate by value, so the lazy deferred tasks'
    // references stay live for the whole detached lifetime. Exceptions are
    // swallowed (best-effort; staleness is l2_ttl-bounded). Defined here (the
    // most-derived class) so `Base::invalidateManyDeferred` resolves to the chain
    // top — InvalidationMixin (cross-target) when present, else the own tiers.
    // ----------------------------------------------------------------------

    template<bool WithLists>
    static io::DetachedTask fireInvalidateManyDeferred(std::vector<E> entities) {
        try {
            co_await Base::template invalidateManyDeferred<WithLists>(
                std::span<const E>(entities));
        } catch (...) {}
    }

    template<typename Desc>
    static io::DetachedTask fireEraseWhereDeferred(
        std::vector<E> deleted, list::spec::Filters<Desc> predicate) {
        try {
            co_await io::whenAll(
                Base::template invalidateManyDeferred<false>(
                    std::span<const E>(deleted)),
                Base::template invalidateWhereListsDeferred<Desc>(predicate));
        } catch (...) {}
    }
};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_REPO_H
