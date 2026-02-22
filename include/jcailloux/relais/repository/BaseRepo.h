#ifndef JCX_RELAIS_BASEREPO_H
#define JCX_RELAIS_BASEREPO_H

#include <atomic>
#include <optional>
#include <string>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/DbProvider.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/config/repo_config.h"
#include "jcailloux/relais/config/FixedString.h"
#include "jcailloux/relais/config/TypeTraits.h"
#include "jcailloux/relais/wrapper/EntityConcepts.h"
#include "jcailloux/relais/wrapper/EntityView.h"
#include "jcailloux/relais/wrapper/BufferView.h"
#include "jcailloux/relais/wrapper/FieldUpdate.h"

namespace jcailloux::relais {

// =========================================================================
// Concepts
// =========================================================================

/// Entity supports partial field updates (has TraitsType with Field enum)
template<typename E>
concept HasFieldUpdate = requires {
    typename E::TraitsType;
    typename E::TraitsType::Field;
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
// BaseRepo - CRUD operations with L3 (database) access only
// =========================================================================
//
// All methods return epoch-guarded views (EntityView / JsonView / BinaryView).
// Entities are allocated in a static memory_pool and immediately retired.
// The EpochGuard prevents reclamation until the view is destroyed.

template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires ReadableEntity<Entity>
class BaseRepo {
    using Mapping = typename Entity::MappingType;

public:
    using EntityType = Entity;
    using KeyType = Key;
    using WrapperType = Entity;
    using FindResultType = wrapper::EntityView<Entity>;

    static constexpr auto config = Cfg;
    static constexpr const char* name() { return Name; }

    // =====================================================================
    // Find by ID (L3: database only)
    // =====================================================================

    /// Find by ID. Returns epoch-guarded EntityView (empty if not found).
    static io::Task<wrapper::EntityView<Entity>> find(const Key& id) {
        auto entity = co_await findRaw(id);
        if (!entity) co_return {};
        co_return makeView(std::move(*entity));
    }

    // =====================================================================
    // Find by ID — JSON serialization (L3: database only)
    // =====================================================================

    /// Find by ID and return JSON buffer view (empty if not found).
    static io::Task<wrapper::JsonView> findJson(const Key& id) {
        try {
            auto params = io::PgParams::fromKey(id);
            auto result = co_await DbProvider::queryParams(
                Mapping::SQL::select_by_pk, params);
            if (result.empty()) co_return {};
            auto entity = Entity::fromRow(result[0]);
            if (!entity) co_return {};
            auto guard = epoch::EpochGuard::acquire();
            auto* ptr = pool().New(std::move(*entity));
            pool().Retire(ptr);
            co_return wrapper::JsonView(ptr->json(), std::move(guard));
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": findJson DB error - " << e.what();
            co_return {};
        }
    }

    // =====================================================================
    // Find by ID — binary serialization (L3: database only)
    // =====================================================================

    /// Find by ID and return binary (BEVE) buffer view (empty if not found).
    static io::Task<wrapper::BinaryView> findBinary(const Key& id)
        requires HasBinarySerialization<Entity>
    {
        try {
            auto params = io::PgParams::fromKey(id);
            auto result = co_await DbProvider::queryParams(
                Mapping::SQL::select_by_pk, params);
            if (result.empty()) co_return {};
            auto entity = Entity::fromRow(result[0]);
            if (!entity) co_return {};
            auto guard = epoch::EpochGuard::acquire();
            auto* ptr = pool().New(std::move(*entity));
            pool().Retire(ptr);
            co_return wrapper::BinaryView(ptr->binary(), std::move(guard));
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": findBinary DB error - " << e.what();
            co_return {};
        }
    }

    // =====================================================================
    // insert
    // =====================================================================

    /// Insert entity in database. Returns epoch-guarded EntityView (empty on error).
    static io::Task<wrapper::EntityView<Entity>> insert(const Entity& entity)
        requires MutableEntity<Entity> && (!Cfg.read_only)
    {
        auto result = co_await insertRaw(entity);
        if (!result) co_return {};
        co_return makeView(std::move(*result));
    }

    // =====================================================================
    // Update
    // =====================================================================

    /// Full update of entity in database. Returns true on success.
    static io::Task<bool> update(const Key& id, const Entity& entity)
        requires MutableEntity<Entity> && (!Cfg.read_only)
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
        const Key& id, const Entity* hint = nullptr)
        requires (!Cfg.read_only)
    {
        auto outcome = co_await eraseOutcome(id, hint);
        co_return outcome.affected;
    }

public:

    // =====================================================================
    // Partial update (patch)
    // =====================================================================

    /// Partial update. Returns epoch-guarded EntityView (empty on error).
    template<typename... Updates>
    static io::Task<wrapper::EntityView<Entity>> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Cfg.read_only)
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

    static epoch::memory_pool<Entity>& pool() {
        static epoch::memory_pool<Entity> p;
        return p;
    }

    /// Allocate entity in pool, retire immediately, return epoch-guarded view.
    static wrapper::EntityView<Entity> makeView(Entity entity) {
        auto guard = epoch::EpochGuard::acquire();
        auto* ptr = pool().New(std::move(entity));
        pool().Retire(ptr);
        return wrapper::EntityView<Entity>(ptr, std::move(guard));
    }

    // =====================================================================
    // Raw methods returning entity by value (for CachedRepo move path)
    // =====================================================================

    /// Find by ID, returning entity by value (no pool/view allocation).
    static io::Task<std::optional<Entity>> findRaw(const Key& id) {
        try {
            auto params = io::PgParams::fromKey(id);
            auto result = co_await DbProvider::queryParams(
                Mapping::SQL::select_by_pk, params);
            if (result.empty()) co_return std::nullopt;
            co_return Entity::fromRow(result[0]);
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": DB error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Insert entity in database, returning entity by value.
    static io::Task<std::optional<Entity>> insertRaw(const Entity& entity)
        requires MutableEntity<Entity> && (!Cfg.read_only)
    {
        try {
            auto params = Entity::toInsertParams(entity);
            auto result = co_await DbProvider::queryParams(
                Mapping::SQL::insert, params);
            if (result.empty()) co_return std::nullopt;
            co_return Entity::fromRow(result[0]);
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": insert error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Partial update, returning entity by value.
    template<typename... Updates>
    static io::Task<std::optional<Entity>> patchRaw(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Cfg.read_only)
    {
        static_assert(sizeof...(Updates) > 0, "patch requires at least one field update");
        try {
            static const auto sql = []{
                if constexpr (config::is_tuple_v<Key>) {
                    return detail::buildUpdateReturning(
                        Mapping::table_name,
                        Mapping::primary_key_columns,
                        {wrapper::fieldColumnName<typename Entity::TraitsType>(Updates{})...},
                        Mapping::SQL::returning_columns);
                } else {
                    return detail::buildUpdateReturning(
                        Mapping::table_name,
                        Mapping::primary_key_column,
                        {wrapper::fieldColumnName<typename Entity::TraitsType>(Updates{})...},
                        Mapping::SQL::returning_columns);
                }
            }();

            io::PgParams params;
            auto fieldParams = io::PgParams::make(
                wrapper::fieldValue<typename Entity::TraitsType>(
                    std::forward<Updates>(updates))...);
            auto keyParams = io::PgParams::fromKey(id);
            params.params.reserve(fieldParams.params.size() + keyParams.params.size());
            for (auto& p : fieldParams.params)
                params.params.push_back(std::move(p));
            for (auto& p : keyParams.params)
                params.params.push_back(std::move(p));

            auto result = co_await DbProvider::queryParams(sql.c_str(), params);
            if (result.empty()) co_return std::nullopt;
            co_return Entity::fromRow(result[0]);
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
    // Upper layers (RedisRepo, CachedRepo) use this to skip redundant
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
    static io::Task<WriteOutcome> updateOutcome(const Key& id, const Entity& entity)
        requires MutableEntity<Entity> && (!Cfg.read_only)
    {
        try {
            auto keyParams = io::PgParams::fromKey(id);
            io::PgParams fieldParams;
            if constexpr (config::is_tuple_v<Key>) {
                fieldParams = Entity::toUpdateParams(entity);
            } else {
                fieldParams = Entity::toInsertParams(entity);
            }
            io::PgParams params;
            params.params.reserve(keyParams.params.size() + fieldParams.params.size());
            for (auto& p : keyParams.params)
                params.params.push_back(std::move(p));
            for (auto& p : fieldParams.params)
                params.params.push_back(std::move(p));

            auto [affected, coalesced] = co_await DbProvider::execute(
                Mapping::SQL::update, params);
            co_return WriteOutcome{affected > 0, coalesced};

        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": update error - " << e.what();
            co_return WriteOutcome{};
        }
    }

    /// Erase returning full outcome (affected + coalesced flag).
    static io::Task<EraseOutcome> eraseOutcome(
        const Key& id, const Entity* hint = nullptr)
        requires (!Cfg.read_only)
    {
        try {
            int affected;
            bool coalesced;
            if constexpr (HasPartitionHint<Entity>) {
                if (hint) {
                    auto params = Mapping::makePartitionHintParams(*hint);
                    std::tie(affected, coalesced) = co_await DbProvider::execute(
                        Mapping::SQL::delete_with_partition, params);
                } else {
                    auto params = io::PgParams::fromKey(id);
                    std::tie(affected, coalesced) = co_await DbProvider::execute(
                        Mapping::SQL::delete_by_pk, params);
                }
            } else {
                auto params = io::PgParams::fromKey(id);
                std::tie(affected, coalesced) = co_await DbProvider::execute(
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
    static io::Task<std::vector<Entity>> cachedList(
        QueryFn&& query,
        [[maybe_unused]] KeyArgs&&... keyParts)
    {
        co_return co_await query();
    }

    template<typename QueryFn, typename... GroupArgs>
    static io::Task<std::vector<Entity>> cachedListTracked(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    template<typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
    static io::Task<std::vector<Entity>> cachedListTrackedWithHeader(
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
        if constexpr (config::is_tuple_v<T>) {
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

#endif //JCX_RELAIS_BASEREPO_H
