#ifndef JCX_RELAIS_BASEREPO_H
#define JCX_RELAIS_BASEREPO_H

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
#include "jcailloux/relais/wrapper/EntityConcepts.h"
#include "jcailloux/relais/wrapper/FieldUpdate.h"

namespace jcailloux::relais {

// =========================================================================
// Wrapper pointer type - immutable shared pointer to entity
// =========================================================================

template<typename Entity>
using WrapperPtr = std::shared_ptr<const Entity>;

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
/// table_name is used as-is; pk_column is wrapped in double quotes;
/// columns are expected pre-quoted (e.g. "\"name\"");
/// returning_columns is the explicit column list for RETURNING clause.
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

}  // namespace detail

// =========================================================================
// BaseRepo - CRUD operations with L3 (database) access only
// =========================================================================
//
// Hierarchy: BaseRepo -> RedisRepo -> CachedRepo
//
// No CRTP. Config is a CacheConfig NTTP. Cross-invalidation is handled
// by InvalidationMixin at a higher layer.
//
// All DB access goes through DbProvider (type-erased PgClient).
// SQL queries come from Entity::MappingType::SQL.
//

template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires ReadableEntity<Entity>
class BaseRepo {
    using Mapping = typename Entity::MappingType;

public:
    using EntityType = Entity;
    using KeyType = Key;
    using WrapperType = Entity;
    using WrapperPtrType = WrapperPtr<Entity>;

    static constexpr auto config = Cfg;
    static constexpr const char* name() { return Name; }

    // =====================================================================
    // Find by ID (L3: database only)
    // =====================================================================

    /// Find by ID with L3 (database) access only.
    /// Returns shared_ptr to immutable entity (nullptr if not found).
    static io::Task<WrapperPtrType> find(const Key& id) {
        try {
            auto result = co_await DbProvider::queryArgs(
                Mapping::SQL::select_by_pk, id);
            if (result.empty()) co_return nullptr;
            auto entity = Entity::fromRow(result[0]);
            co_return entity ? std::make_shared<const Entity>(std::move(*entity)) : nullptr;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": DB error - " << e.what();
            co_return nullptr;
        }
    }

    // =====================================================================
    // insert
    // =====================================================================

    /// insert entity in database.
    /// Returns shared_ptr to immutable entity (nullptr on error).
    /// Uses INSERT ... RETURNING to get the full entity back (with DB-managed fields).
    static io::Task<WrapperPtrType> insert(WrapperPtrType wrapper)
        requires MutableEntity<Entity> && (!Cfg.read_only)
    {
        if (!wrapper) co_return nullptr;

        try {
            auto params = Entity::toInsertParams(*wrapper);
            auto result = co_await DbProvider::queryParams(
                Mapping::SQL::insert, params);
            if (result.empty()) co_return nullptr;
            auto entity = Entity::fromRow(result[0]);
            co_return entity ? std::make_shared<const Entity>(std::move(*entity)) : nullptr;

        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": insert error - " << e.what();
            co_return nullptr;
        }
    }

    // =====================================================================
    // Update
    // =====================================================================

    /// Full update of entity in database.
    /// Builds params as: PK ($1), then insert fields ($2...$N).
    /// Returns true on success, false on error.
    static io::Task<bool> update(const Key& id, WrapperPtrType wrapper)
        requires MutableEntity<Entity> && (!Cfg.read_only)
    {
        if (!wrapper) co_return false;

        try {
            // toInsertParams returns fields without PK (db_managed).
            // SQL::update expects PK as $1, then fields as $2...$N.
            auto insertParams = Entity::toInsertParams(*wrapper);
            io::PgParams params;
            params.params.reserve(insertParams.params.size() + 1);
            // $1 = primary key
            params.params.push_back(
                io::PgParams::make(id).params[0]);
            // $2...$N = fields (same order as toInsertParams)
            for (auto& p : insertParams.params)
                params.params.push_back(std::move(p));

            auto affected = co_await DbProvider::execute(
                Mapping::SQL::update, params);
            co_return affected > 0;

        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": update error - " << e.what();
            co_return false;
        }
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
    /// Internal erase with optional entity hint (used by cache layers for
    /// partition pruning optimization on composite-key entities).
    /// When a hint is available AND the entity has a composite key, uses the
    /// full PK SQL for single-partition deletion. Otherwise falls back to
    /// partial key SQL (scans all partitions — acceptable).
    static io::Task<std::optional<size_t>> eraseImpl(
        const Key& id, WrapperPtrType cachedHint = nullptr)
        requires (!Cfg.read_only)
    {
        try {
            int affected;
            if constexpr (HasPartitionKey<Entity>) {
                if (cachedHint) {
                    // Full composite key: prunes to 1 partition
                    auto params = Mapping::makeFullKeyParams(*cachedHint);
                    affected = co_await DbProvider::execute(
                        Mapping::SQL::delete_by_full_pk, params);
                } else {
                    // Partial key: scans all partitions
                    affected = co_await DbProvider::executeArgs(
                        Mapping::SQL::delete_by_pk, id);
                }
            } else {
                affected = co_await DbProvider::executeArgs(
                    Mapping::SQL::delete_by_pk, id);
            }
            co_return static_cast<size_t>(affected);
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": erase error - " << e.what();
            co_return std::nullopt;
        }
    }

public:

    // =====================================================================
    // Partial update (patch)
    // =====================================================================

    /// Partial update: modifies only the specified fields in the database.
    /// Builds UPDATE ... SET col=$1, ... WHERE pk=$N RETURNING cols (single query).
    /// Returns the re-fetched entity from DB (nullptr on error or not found).
    template<typename... Updates>
    static io::Task<WrapperPtrType> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Cfg.read_only)
    {
        static_assert(sizeof...(Updates) > 0, "patch requires at least one field update");

        try {
            // Build SQL at first call (thread-safe static init).
            // Field values are $1..$N, PK is $N+1.
            static const auto sql = detail::buildUpdateReturning(
                Mapping::table_name,
                Mapping::primary_key_column,
                {wrapper::fieldColumnName<typename Entity::TraitsType>(updates)...},
                Mapping::SQL::returning_columns);

            // Build params: field values first, then PK at the end
            auto params = io::PgParams::make(
                wrapper::fieldValue<typename Entity::TraitsType>(
                    std::forward<Updates>(updates))...,
                id);

            auto result = co_await DbProvider::queryParams(sql.c_str(), params);
            if (result.empty()) co_return nullptr;

            auto entity = Entity::fromRow(result[0]);
            co_return entity ? std::make_shared<const Entity>(std::move(*entity)) : nullptr;

        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": patch error - " << e.what();
            co_return nullptr;
        }
    }

    // =====================================================================
    // Invalidation pass-through (public interface)
    // =====================================================================

    /// Invalidate cache for a key. No-op at BaseRepo level.
    static io::Task<void> invalidate([[maybe_unused]] const Key& id) {
        co_return;
    }

    /// Build a group key from key parts.
    template<typename... GroupArgs>
    static std::string makeGroupKey(GroupArgs&&... groupParts) {
        return makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
    }

    /// Selectively invalidate list pages for a pre-built group key.
    /// No-op at BaseRepo level.
    static io::Task<size_t> invalidateListGroupByKey(
        [[maybe_unused]] const std::string& groupKey,
        [[maybe_unused]] int64_t entity_sort_val)
    {
        co_return 0;
    }

    /// Invalidate all list cache groups. No-op at BaseRepo level.
    static io::Task<size_t> invalidateAllListGroups()
    {
        co_return 0;
    }

protected:
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

    /// Execute a list query directly (no caching).
    template<typename QueryFn, typename... KeyArgs>
    static io::Task<std::vector<Entity>> cachedList(
        QueryFn&& query,
        [[maybe_unused]] KeyArgs&&... keyParts)
    {
        co_return co_await query();
    }

    /// Execute a tracked list query directly (no caching, no tracking).
    template<typename QueryFn, typename... GroupArgs>
    static io::Task<std::vector<Entity>> cachedListTracked(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    /// Execute a tracked list query with header directly (no caching, no header).
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

    /// Invalidate all cached list pages for a group. No-op at base level.
    template<typename... GroupArgs>
    static io::Task<size_t> invalidateListGroup(
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    /// Selectively invalidate list pages based on a sort value. No-op at base level.
    template<typename... GroupArgs>
    static io::Task<size_t> invalidateListGroupSelective(
        [[maybe_unused]] int64_t entity_sort_val,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    /// Selectively invalidate list pages based on old and new sort values. No-op.
    template<typename... GroupArgs>
    static io::Task<size_t> invalidateListGroupSelectiveUpdate(
        [[maybe_unused]] int64_t old_sort_val,
        [[maybe_unused]] int64_t new_sort_val,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    /// Execute a list query and return as a custom list entity (no caching).
    template<typename ListEntity, typename QueryFn, typename... KeyArgs>
    static io::Task<ListEntity> cachedListAs(
        QueryFn&& query,
        [[maybe_unused]] KeyArgs&&... keyParts)
    {
        co_return co_await query();
    }

    /// Execute a tracked list query and return as a custom list entity (no caching).
    template<typename ListEntity, typename QueryFn, typename... GroupArgs>
    static io::Task<ListEntity> cachedListAsTracked(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    /// Execute a tracked list query with header as custom list entity (no caching).
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
        if constexpr (std::is_integral_v<T>) {
            return std::to_string(value);
        } else {
            return std::string(value);
        }
    }
};

}  // namespace jcailloux::relais

#endif //JCX_RELAIS_BASEREPO_H
