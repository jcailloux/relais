#ifndef JCX_DROGON_BASEREPOSITORY_H
#define JCX_DROGON_BASEREPOSITORY_H

#include <optional>
#include <span>
#include <vector>
#include <chrono>
#include <drogon/utils/coroutine.h>
#include <drogon/HttpAppFramework.h>
#include <drogon/orm/CoroMapper.h>
#include <trantor/utils/Logger.h>
#include "jcailloux/relais/config/repository_config.h"
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

/// Entity supports partial field updates (has TraitsType with Field enum and setPrimaryKeyOnModel)
template<typename E>
concept HasFieldUpdate = requires(typename E::TraitsType::Model& m, int64_t k) {
    typename E::TraitsType;
    typename E::TraitsType::Field;
    E::TraitsType::setPrimaryKeyOnModel(m, k);
};

// =========================================================================
// SQL helper for UPDATE ... RETURNING *
// =========================================================================

namespace detail {

/// Build: UPDATE "table" SET "col1"=$1, "col2"=$2 WHERE "pk"=$N RETURNING *
/// table_name and columns are expected pre-quoted; pk_column is unquoted.
inline std::string buildUpdateReturning(
    std::string_view table_name,
    std::string_view pk_column,
    std::initializer_list<std::string_view> columns)
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
    sql += " RETURNING *";
    return sql;
}

}  // namespace detail

// =========================================================================
// BaseRepository - CRUD operations with L3 (database) access only
// =========================================================================
//
// Hierarchy: BaseRepository -> RedisRepository -> CachedRepository
//
// No CRTP. Config is a CacheConfig NTTP. Cross-invalidation is handled
// by InvalidationMixin at a higher layer.
//

template<typename Entity, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires ReadableEntity<Entity, typename Entity::Model>
class BaseRepository {
    using Model = typename Entity::Model;

public:
    using EntityType = Entity;
    using ModelType = Model;
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
    static ::drogon::Task<WrapperPtrType> findById(const Key& id) {
        try {
            auto db = ::drogon::app().getDbClient();
            ::drogon::orm::CoroMapper<Model> mapper(db);

            if constexpr (std::is_same_v<Key, typename Model::PrimaryKeyType>) {
                // Standard path - Key matches Model's PK type
                auto row = co_await mapper.findByPrimaryKey(id);
                auto entity = Entity::fromModel(row);
                co_return entity ? std::make_shared<const Entity>(std::move(*entity)) : nullptr;
            } else {
                // Partial key path - for partitioned tables where Key is a subset of PK
                static_assert(HasPartialKey<Entity, Model, Key>,
                    "Entity must provide makeKeyCriteria when Key differs from "
                    "Model::PrimaryKeyType. Add '@relais primary_key db_managed' "
                    "to the PK field.");

                // Use LIMIT 2 to detect non-unique results
                auto rows = co_await mapper.limit(2).findBy(
                    Entity::template makeKeyCriteria<Model>(id)
                );

                if (rows.size() > 1) {
                    LOG_ERROR << name() << ": Non-unique partial key! "
                              << "Expected 1 row but query returned " << rows.size()
                              << ". This indicates a data integrity issue.";
                }

                if (rows.empty()) co_return nullptr;
                auto entity = Entity::fromModel(rows[0]);
                co_return entity ? std::make_shared<const Entity>(std::move(*entity)) : nullptr;
            }

        } catch (const ::drogon::orm::UnexpectedRows&) {
            co_return nullptr;
        } catch (const ::drogon::orm::DrogonDbException& e) {
            LOG_ERROR << name() << ": DB error - " << e.base().what();
            co_return nullptr;
        }
    }

    // =====================================================================
    // Create
    // =====================================================================

    /// Create entity in database.
    /// Returns shared_ptr to immutable entity (nullptr on error).
    /// Compile-time error if Cfg.read_only is true.
    static ::drogon::Task<WrapperPtrType> create(WrapperPtrType wrapper)
        requires MutableEntity<Entity, Model> && (!Cfg.read_only)
    {
        if (!wrapper) co_return nullptr;

        try {
            auto db = ::drogon::app().getDbClient();
            ::drogon::orm::CoroMapper<Model> mapper(db);

            Model model = Entity::toModel(*wrapper);
            Model inserted = co_await mapper.insert(model);

            auto entity = Entity::fromModel(inserted);
            co_return entity ? std::make_shared<const Entity>(std::move(*entity)) : nullptr;

        } catch (const ::drogon::orm::DrogonDbException& e) {
            LOG_ERROR << name() << ": create error - " << e.base().what();
            co_return nullptr;
        }
    }

    // =====================================================================
    // Update
    // =====================================================================

    /// Update entity in database.
    /// Returns true on success, false on error.
    /// Compile-time error if Cfg.read_only is true.
    static ::drogon::Task<bool> update(const Key& id, WrapperPtrType wrapper)
        requires MutableEntity<Entity, Model> && (!Cfg.read_only)
    {
        if (!wrapper) co_return false;

        try {
            auto db = ::drogon::app().getDbClient();
            ::drogon::orm::CoroMapper<Model> mapper(db);

            Model model = Entity::toModel(*wrapper);
            // Ensure PK is set on the model -- toModel() skips DbManaged
            // fields (like auto-increment PK), but update() needs the PK
            // for the WHERE clause.
            if constexpr (requires { Entity::TraitsType::setPrimaryKeyOnModel(model, id); }) {
                Entity::TraitsType::setPrimaryKeyOnModel(model, id);
            }
            co_await mapper.update(model);

            co_return true;

        } catch (const ::drogon::orm::DrogonDbException& e) {
            LOG_ERROR << name() << ": update error - " << e.base().what();
            co_return false;
        }
    }

    // =====================================================================
    // Remove
    // =====================================================================

    /// Remove entity by ID.
    /// Returns: rows deleted (0 if not found), or nullopt on DB error (FK constraint, etc.)
    /// Compile-time error if Cfg.read_only is true.
    static ::drogon::Task<std::optional<size_t>> remove(const Key& id)
        requires (!Cfg.read_only)
    {
        co_return co_await removeImpl(id, nullptr);
    }

protected:
    /// Internal remove with optional entity hint for PartialKey optimization.
    /// When hint is provided (e.g. from L1/L2 cache), uses full composite PK
    /// for partition pruning. Otherwise falls back to criteria-based delete.
    static ::drogon::Task<std::optional<size_t>> removeImpl(
        const Key& id, WrapperPtrType cachedHint = nullptr)
        requires (!Cfg.read_only)
    {
        try {
            auto db = ::drogon::app().getDbClient();
            ::drogon::orm::CoroMapper<Model> mapper(db);

            std::optional<size_t> result;
            if constexpr (std::is_same_v<Key, typename Model::PrimaryKeyType>) {
                // Standard path - Key matches Model's PK type
                result = co_await mapper.deleteByPrimaryKey(id);
            } else {
                // Partial key path - for partitioned tables
                static_assert(HasPartialKey<Entity, Model, Key>,
                    "Entity must provide makeKeyCriteria when Key differs from "
                    "Model::PrimaryKeyType. Add '@relais primary_key db_managed' "
                    "to the PK field.");

                if (cachedHint) {
                    // Entity available (from L1/L2 cache) -- full PK for partition pruning.
                    // toModel() may skip db_managed fields (like auto-increment id),
                    // so we also set the partial key to get a complete PK.
                    auto model = Entity::toModel(*cachedHint);
                    Entity::TraitsType::setPrimaryKeyOnModel(model, id);
                    result = co_await mapper.deleteByPrimaryKey(
                        model.getPrimaryKey());
                } else {
                    // No entity -- criteria-based (1 query, scans all partitions)
                    result = co_await mapper.deleteBy(
                        Entity::template makeKeyCriteria<Model>(id));
                }
            }

            co_return result;

        } catch (const ::drogon::orm::DrogonDbException& e) {
            LOG_ERROR << name() << ": remove error - " << e.base().what();
            co_return std::nullopt;
        }
    }

public:

    // =====================================================================
    // Partial update (updateBy)
    // =====================================================================

    /// Partial update: modifies only the specified fields in the database.
    /// Uses Drogon's dirty-flag tracking to generate SET only for given fields.
    /// Returns the re-fetched entity from DB (nullptr on error or not found).
    /// Compile-time error if Cfg.read_only is true or Entity lacks TraitsType.
    template<typename... Updates>
    static ::drogon::Task<WrapperPtrType> updateBy(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<Entity> && (!Cfg.read_only)
    {
        static_assert(sizeof...(Updates) > 0, "updateBy requires at least one field update");

        try {
            auto db = ::drogon::app().getDbClient();

            if constexpr (std::is_same_v<Key, typename Model::PrimaryKeyType>) {
                // Standard path: UPDATE ... RETURNING * (single DB query)
                static const auto sql = detail::buildUpdateReturning(
                    Model::tableName,
                    Model::primaryKeyName,
                    {wrapper::fieldColumnName<typename Entity::TraitsType>(updates)...});

                auto result = co_await db->execSqlCoro(
                    sql,
                    wrapper::fieldValue<typename Entity::TraitsType>(
                        std::forward<Updates>(updates))...,
                    id);

                if (result.empty()) co_return nullptr;
                Model model(result[0], static_cast<ssize_t>(-1));
                auto entity = Entity::fromModel(model);
                co_return entity ? std::make_shared<const Entity>(std::move(*entity)) : nullptr;

            } else {
                // PartialKey: criteria-based update + re-fetch (2 queries)
                ::drogon::orm::CoroMapper<Model> mapper(db);
                auto criteria = Entity::template makeKeyCriteria<Model>(id);
                std::vector<std::string> columns = {
                    wrapper::fieldColumnName<typename Entity::TraitsType>(updates)...
                };
                co_await mapper.updateBy(columns, criteria,
                    wrapper::fieldValue<typename Entity::TraitsType>(updates)...);
                co_return co_await findById(id);
            }

        } catch (const ::drogon::orm::DrogonDbException& e) {
            LOG_ERROR << name() << ": updateBy error - " << e.base().what();
            co_return nullptr;
        }
    }

    // =====================================================================
    // List invalidation pass-through (public interface)
    // =====================================================================

    /// Invalidate cache for a key. No-op at BaseRepository level (no cache to invalidate).
    /// Exists so that any repository can be a cross-invalidation target.
    static ::drogon::Task<void> invalidate([[maybe_unused]] const Key& id) {
        co_return;
    }

    /// Build a group key from key parts.
    /// Public wrapper for use by InvalidateListVia resolvers.
    template<typename... GroupArgs>
    static std::string makeGroupKey(GroupArgs&&... groupParts) {
        return makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
    }

    /// Selectively invalidate list pages for a pre-built group key.
    /// No-op at BaseRepository level (no cache to invalidate).
    static ::drogon::Task<size_t> invalidateListGroupByKey(
        [[maybe_unused]] const std::string& groupKey,
        [[maybe_unused]] int64_t entity_sort_val)
    {
        co_return 0;
    }

    /// Invalidate all list cache groups for this repository.
    /// No-op at BaseRepository level (no cache to invalidate).
    /// Used by InvalidateListVia when resolver returns nullopt (full pattern).
    static ::drogon::Task<size_t> invalidateAllListGroups()
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

    /// Build a group key for list tracking (without pagination params).
    template<typename... GroupArgs>
    static std::string makeListGroupKey(GroupArgs&&... groupParts) {
        std::string key = std::string(name()) + ":list";
        ((key += ":" + toString(std::forward<GroupArgs>(groupParts))), ...);
        return key;
    }

    /// Execute a list query directly (no caching).
    template<typename QueryFn, typename... KeyArgs>
    static ::drogon::Task<std::vector<Entity>> cachedList(
        QueryFn&& query,
        [[maybe_unused]] KeyArgs&&... keyParts)
    {
        co_return co_await query();
    }

    /// Execute a tracked list query directly (no caching, no tracking).
    template<typename QueryFn, typename... GroupArgs>
    static ::drogon::Task<std::vector<Entity>> cachedListTracked(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    /// Execute a tracked list query with header directly (no caching, no header).
    template<typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
    static ::drogon::Task<std::vector<Entity>> cachedListTrackedWithHeader(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] HeaderBuilder&& headerBuilder,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    /// Invalidate all cached list pages for a group.
    /// No-op at BaseRepository level (no cache to invalidate).
    template<typename... GroupArgs>
    static ::drogon::Task<size_t> invalidateListGroup(
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    /// Selectively invalidate list pages based on a sort value.
    /// No-op at BaseRepository level.
    template<typename... GroupArgs>
    static ::drogon::Task<size_t> invalidateListGroupSelective(
        [[maybe_unused]] int64_t entity_sort_val,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    /// Selectively invalidate list pages based on old and new sort values.
    /// No-op at BaseRepository level.
    template<typename... GroupArgs>
    static ::drogon::Task<size_t> invalidateListGroupSelectiveUpdate(
        [[maybe_unused]] int64_t old_sort_val,
        [[maybe_unused]] int64_t new_sort_val,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    /// Execute a list query and return as a custom list entity (no caching).
    template<typename ListEntity, typename QueryFn, typename... KeyArgs>
    static ::drogon::Task<ListEntity> cachedListAs(
        QueryFn&& query,
        [[maybe_unused]] KeyArgs&&... keyParts)
    {
        auto models = co_await query();
        co_return ListEntity::fromModels(models);
    }

    /// Execute a tracked list query and return as a custom list entity (no caching).
    template<typename ListEntity, typename QueryFn, typename... GroupArgs>
    static ::drogon::Task<ListEntity> cachedListAsTracked(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        auto models = co_await query();
        co_return ListEntity::fromModels(models);
    }

    /// Execute a tracked list query with header and return as a custom list entity (no caching).
    template<typename ListEntity, typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
    static ::drogon::Task<ListEntity> cachedListAsTrackedWithHeader(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] HeaderBuilder&& headerBuilder,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        auto models = co_await query();
        co_return ListEntity::fromModels(models);
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

#endif //JCX_DROGON_BASEREPOSITORY_H
