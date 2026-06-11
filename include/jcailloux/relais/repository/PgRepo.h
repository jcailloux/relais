#ifndef JCX_RELAIS_PGREPO_H
#define JCX_RELAIS_PGREPO_H

#include <atomic>
#include <optional>
#include <string>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/PgProvider.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/config/CacheConfig.h"
#include "jcailloux/relais/config/FixedString.h"
#include "jcailloux/relais/TypeTraits.h"
#include "jcailloux/relais/entity/EntityConcepts.h"
#include "jcailloux/relais/cache/CacheView.h"
#include "jcailloux/relais/entity/FieldUpdate.h"

namespace jcailloux::relais {

// =========================================================================
// Concepts
// =========================================================================

/// E supports partial field updates (has TraitsType with Field enum)
template<typename E>
concept HasFieldUpdate = requires {
    typename E::TraitsType;
    typename E::TraitsType::Field;
};

/// E supports a full-row UPDATE: it has at least one updatable column, so the
/// generator emitted toUpdateParams (+ SQL::update). False for pure all-PK
/// junctions, where update() would reference a suppressed SQL::update. Gates
/// every update path so the method is cleanly absent rather than failing to
/// instantiate at the call site.
template<typename E>
concept HasFullUpdate = requires(const E& e) {
    E::toUpdateParams(e);
};

// =========================================================================
// SQL helper for UPDATE ... RETURNING
// =========================================================================

namespace detail {

/// Build: UPDATE "table" SET "col1"=$1, "col2"=$2 WHERE "pk"=$N RETURNING cols
inline std::string buildUpdateReturning(
    std::string_view table_name,
    std::string_view pk_column,
    std::initializer_list<std::string_view> columns,
    std::string_view returning_columns)
{
    std::string sql;
    sql.reserve(128);
    sql += "UPDATE ";
    sql += table_name;
    sql += " SET ";
    size_t param = 1;
    bool first = true;
    for (auto col : columns) {
        if (!first) sql += ',';
        first = false;
        sql += col;
        sql += "=$";
        sql += std::to_string(param++);
    }
    sql += " WHERE \"";
    sql += pk_column;
    sql += "\"=$";
    sql += std::to_string(param);
    sql += " RETURNING ";
    sql += returning_columns;
    return sql;
}

/// Build UPDATE ... RETURNING for composite primary keys.
template<size_t N>
inline std::string buildUpdateReturning(
    std::string_view table_name,
    const std::array<const char*, N>& pk_columns,
    std::initializer_list<std::string_view> columns,
    std::string_view returning_columns)
{
    std::string sql;
    sql.reserve(128);
    sql += "UPDATE ";
    sql += table_name;
    sql += " SET ";
    size_t param = 1;
    bool first = true;
    for (auto col : columns) {
        if (!first) sql += ',';
        first = false;
        sql += col;
        sql += "=$";
        sql += std::to_string(param++);
    }
    sql += " WHERE ";
    for (size_t i = 0; i < N; ++i) {
        if (i > 0) sql += " AND ";
        sql += "\"";
        sql += pk_columns[i];
        sql += "\"=$";
        sql += std::to_string(param++);
    }
    sql += " RETURNING ";
    sql += returning_columns;
    return sql;
}

}  // namespace detail

// =========================================================================
// PgRepo - CRUD operations with L3 (database) access only
// =========================================================================
//
// find() returns epoch-guarded CacheView. findJson()/findBinary() return
// serialized data by value (std::string / std::vector<uint8_t>).

template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires ReadableEntity<E>
class PgRepo {
    using Mapping = typename E::MappingType;

public:
    using EntityType = E;
    using KeyType = Key;
    using WrapperType = E;
    using FindResultType = cache::CacheView<E>;

    static constexpr auto config = Cfg;
    static constexpr const char* name() { return Name; }

    // =====================================================================
    // Find by ID (L3: database only)
    // =====================================================================

    /// Find by ID. Returns epoch-guarded CacheView (empty if not found).
    static io::Task<cache::CacheView<E>> find(const Key& id) {
        auto entity = co_await findRaw(id);
        if (!entity) co_return {};
        co_return makeView(std::move(*entity));
    }

    // =====================================================================
    // Find by ID — JSON serialization (L3: database only)
    // =====================================================================

    /// Find by ID and return JSON string (empty if not found).
    static io::Task<std::string> findJson(const Key& id) {
        try {
            auto params = io::PgParams::fromKey(id);
            auto result = co_await PgProvider::queryParams(
                Mapping::SQL::select_by_pk, params);
            if (result.empty()) co_return {};
            auto entity = E::fromRow(result[0]);
            if (!entity) co_return {};
            co_return entity->json();
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": findJson DB error - " << e.what();
            co_return {};
        }
    }

    // =====================================================================
    // Find by ID — binary serialization (L3: database only)
    // =====================================================================

    /// Find by ID and return binary (BEVE) vector (empty if not found).
    static io::Task<std::vector<uint8_t>> findBinary(const Key& id)
        requires HasBinarySerialization<E>
    {
        try {
            auto params = io::PgParams::fromKey(id);
            auto result = co_await PgProvider::queryParams(
                Mapping::SQL::select_by_pk, params);
            if (result.empty()) co_return {};
            auto entity = E::fromRow(result[0]);
            if (!entity) co_return {};
            co_return entity->binary();
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": findBinary DB error - " << e.what();
            co_return {};
        }
    }

    // =====================================================================
    // insert
    // =====================================================================

    /// Insert entity in database. Returns epoch-guarded CacheView (empty on error).
    static io::Task<cache::CacheView<E>> insert(const E& entity)
        requires MutableEntity<E> && (!Cfg.read_only)
    {
        auto result = co_await insertRaw(entity);
        if (!result) co_return {};
        co_return makeView(std::move(*result));
    }

    // =====================================================================
    // Update
    // =====================================================================

    /// Full update of entity in database. Returns true on success.
    static io::Task<bool> update(const Key& id, const E& entity)
        requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
    {
        auto outcome = co_await updateOutcome(id, entity);
        co_return outcome.success;
    }

    // =====================================================================
    // Erase
    // =====================================================================

    /// Erase entity by ID.
    /// Returns: rows deleted (0 if not found), or nullopt on DB error.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Cfg.read_only)
    {
        co_return co_await eraseImpl(id, nullptr);
    }

protected:
    /// Internal erase with optional entity hint (for partition pruning).
    static io::Task<std::optional<size_t>> eraseImpl(
        const Key& id, const E* hint = nullptr)
        requires (!Cfg.read_only)
    {
        auto outcome = co_await eraseOutcome(id, hint);
        co_return outcome.affected;
    }

public:

    // =====================================================================
    // Partial update (patch)
    // =====================================================================

    /// Partial update. Returns epoch-guarded CacheView (empty on error).
    template<typename... Updates>
    static io::Task<cache::CacheView<E>> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        auto entity = co_await patchRaw(id, std::forward<Updates>(updates)...);
        if (!entity) co_return {};
        co_return makeView(std::move(*entity));
    }

    // =====================================================================
    // Invalidation pass-through (public interface)
    // =====================================================================

    static io::Task<void> invalidate([[maybe_unused]] const Key& id) {
        co_return;
    }

    template<typename... GroupArgs>
    static std::string makeGroupKey(GroupArgs&&... groupParts) {
        return makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
    }

    static io::Task<size_t> invalidateListGroupByKey(
        [[maybe_unused]] const std::string& groupKey,
        [[maybe_unused]] int64_t entity_sort_val)
    {
        co_return 0;
    }

    static io::Task<size_t> invalidateAllListGroups()
    {
        co_return 0;
    }

protected:

    // =====================================================================
    // Epoch memory pool for temporary entity allocations
    // =====================================================================

    static epoch::memory_pool<E>& pool() {
        static epoch::memory_pool<E> p;
        return p;
    }

    /// Allocate entity in pool, retire immediately, return epoch-guarded view.
    static cache::CacheView<E> makeView(E entity) {
        auto guard = epoch::EpochGuard::acquire();
        auto* ptr = pool().New(std::move(entity));
        pool().Retire(ptr);
        return cache::CacheView<E>(ptr, std::move(guard));
    }

    // =====================================================================
    // Raw methods returning entity by value (for LocalRepo move path)
    // =====================================================================

    /// Find by ID, returning entity by value (no pool/view allocation).
    /// Routes through submitEntityRead for ANY-array batching.
    static io::Task<std::optional<E>> findRaw(const Key& id) {
        try {
            auto params = io::PgParams::fromKey(id);
            auto result = co_await PgProvider::entityQueryParams(
                Mapping::SQL::select_by_pk_batch,
                Mapping::SQL::select_by_pk, params);
            if (result.empty()) co_return std::nullopt;
            co_return E::fromRow(result[0]);
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": DB error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Insert entity in database, returning entity by value.
    static io::Task<std::optional<E>> insertRaw(const E& entity)
        requires MutableEntity<E> && (!Cfg.read_only)
    {
        try {
            auto params = E::toInsertParams(entity);
            auto result = co_await PgProvider::queryParams(
                Mapping::SQL::insert, params);
            if (result.empty()) co_return std::nullopt;
            co_return E::fromRow(result[0]);
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": insert error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Partial update, returning entity by value.
    template<typename... Updates>
    static io::Task<std::optional<E>> patchRaw(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        static_assert(sizeof...(Updates) > 0, "patch requires at least one field update");
        try {
            static const auto sql = []{
                if constexpr (is_tuple_v<Key>) {
                    return detail::buildUpdateReturning(
                        Mapping::table_name,
                        Mapping::primary_key_columns,
                        {entity::fieldColumnName<typename E::TraitsType>(Updates{})...},
                        Mapping::SQL::returning_columns);
                } else {
                    return detail::buildUpdateReturning(
                        Mapping::table_name,
                        Mapping::primary_key_column,
                        {entity::fieldColumnName<typename E::TraitsType>(Updates{})...},
                        Mapping::SQL::returning_columns);
                }
            }();

            io::PgParams params;
            auto fieldParams = io::PgParams::make(
                entity::fieldValue<typename E::TraitsType>(
                    std::forward<Updates>(updates))...);
            auto keyParams = io::PgParams::fromKey(id);
            params.params.reserve(fieldParams.params.size() + keyParams.params.size());
            for (auto& p : fieldParams.params)
                params.params.push_back(std::move(p));
            for (auto& p : keyParams.params)
                params.params.push_back(std::move(p));

            auto result = co_await PgProvider::queryParams(sql.c_str(), params);
            if (result.empty()) co_return std::nullopt;
            co_return E::fromRow(result[0]);
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": patch error - " << e.what();
            co_return std::nullopt;
        }
    }

    // =====================================================================
    // Write outcome types (for write coalescing propagation)
    // =====================================================================
    //
    // When the BatchScheduler coalesces identical writes (same SQL + same
    // params), followers receive the leader's result with coalesced=true.
    // Upper layers (RedisRepo, LocalRepo) use this to skip redundant
    // cache operations (L1 evict, L2 Redis SET/DEL).

    struct WriteOutcome {
        bool success = false;
        bool coalesced = false;
    };

    struct EraseOutcome {
        std::optional<size_t> affected;
        bool coalesced = false;
    };

    /// Update returning full outcome (success + coalesced flag).
    static io::Task<WriteOutcome> updateOutcome(const Key& id, const E& entity)
        requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
    {
        try {
            auto keyParams = io::PgParams::fromKey(id);
            io::PgParams fieldParams = E::toUpdateParams(entity);
            io::PgParams params;
            params.params.reserve(keyParams.params.size() + fieldParams.params.size());
            for (auto& p : keyParams.params)
                params.params.push_back(std::move(p));
            for (auto& p : fieldParams.params)
                params.params.push_back(std::move(p));

            auto [affected, coalesced] = co_await PgProvider::execute(
                Mapping::SQL::update, params);
            co_return WriteOutcome{affected > 0, coalesced};

        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": update error - " << e.what();
            co_return WriteOutcome{};
        }
    }

    /// Erase returning full outcome (affected + coalesced flag).
    static io::Task<EraseOutcome> eraseOutcome(
        const Key& id, const E* hint = nullptr)
        requires (!Cfg.read_only)
    {
        try {
            int affected;
            bool coalesced;
            if constexpr (HasPartitionHint<E>) {
                if (hint) {
                    auto params = Mapping::makePartitionHintParams(*hint);
                    std::tie(affected, coalesced) = co_await PgProvider::execute(
                        Mapping::SQL::delete_with_partition, params);
                } else {
                    auto params = io::PgParams::fromKey(id);
                    std::tie(affected, coalesced) = co_await PgProvider::execute(
                        Mapping::SQL::delete_by_pk, params);
                }
            } else {
                auto params = io::PgParams::fromKey(id);
                std::tie(affected, coalesced) = co_await PgProvider::execute(
                    Mapping::SQL::delete_by_pk, params);
            }
            co_return EraseOutcome{static_cast<size_t>(affected), coalesced};
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": erase error - " << e.what();
            co_return EraseOutcome{};
        }
    }

    // =====================================================================
    // List query pass-through methods (no caching at L3 level)
    // =====================================================================

    template<typename... Args>
    static std::string makeListCacheKey(Args&&... args) {
        std::string key = std::string(name()) + ":list";
        ((key += ":" + toString(std::forward<Args>(args))), ...);
        return key;
    }

    template<typename... GroupArgs>
    static std::string makeListGroupKey(GroupArgs&&... groupParts) {
        std::string key = std::string(name()) + ":list";
        ((key += ":" + toString(std::forward<GroupArgs>(groupParts))), ...);
        return key;
    }

    template<typename QueryFn, typename... KeyArgs>
    static io::Task<std::vector<E>> cachedList(
        QueryFn&& query,
        [[maybe_unused]] KeyArgs&&... keyParts)
    {
        co_return co_await query();
    }

    template<typename QueryFn, typename... GroupArgs>
    static io::Task<std::vector<E>> cachedListTracked(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    template<typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
    static io::Task<std::vector<E>> cachedListTrackedWithHeader(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] HeaderBuilder&& headerBuilder,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    template<typename... GroupArgs>
    static io::Task<size_t> invalidateListGroup(
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    template<typename... GroupArgs>
    static io::Task<size_t> invalidateListGroupSelective(
        [[maybe_unused]] int64_t entity_sort_val,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    template<typename... GroupArgs>
    static io::Task<size_t> invalidateListGroupSelectiveUpdate(
        [[maybe_unused]] int64_t old_sort_val,
        [[maybe_unused]] int64_t new_sort_val,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    template<typename ListEntity, typename QueryFn, typename... KeyArgs>
    static io::Task<ListEntity> cachedListAs(
        QueryFn&& query,
        [[maybe_unused]] KeyArgs&&... keyParts)
    {
        co_return co_await query();
    }

    template<typename ListEntity, typename QueryFn, typename... GroupArgs>
    static io::Task<ListEntity> cachedListAsTracked(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    template<typename ListEntity, typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
    static io::Task<ListEntity> cachedListAsTrackedWithHeader(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] HeaderBuilder&& headerBuilder,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    template<typename T>
    static std::string toString(const T& value) {
        if constexpr (is_tuple_v<T>) {
            std::string result;
            std::apply([&](const auto&... parts) {
                bool first = true;
                ((result += (first ? "" : ":"),
                  result += toString(parts),
                  first = false), ...);
            }, value);
            return result;
        } else if constexpr (std::is_integral_v<T>) {
            return std::to_string(value);
        } else {
            return std::string(value);
        }
    }
};

}  // namespace jcailloux::relais

#endif //JCX_RELAIS_PGREPO_H
