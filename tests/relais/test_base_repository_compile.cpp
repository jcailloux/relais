/**
 * test_base_repository_compile.cpp
 *
 * Compile-time and structural tests for BaseRepository.
 * Verifies that:
 *   - BaseRepository instantiates with all entity types
 *   - Concepts (ReadableEntity, MutableEntity, HasFieldUpdate) are satisfied
 *   - SQL strings are correct
 *   - FieldUpdate utilities (set, fieldColumnName, fieldValue) work
 *   - buildUpdateReturning produces correct SQL
 *   - Type aliases, config, and list key building are correct
 *
 * No actual DB connection is needed — all tests are structural.
 */

#include <catch2/catch_test_macros.hpp>

#include "jcailloux/relais/repository/BaseRepository.h"
#include "fixtures/generated/TestItemWrapper.h"
#include "fixtures/generated/TestUserWrapper.h"
#include "fixtures/generated/TestOrderWrapper.h"
#include "fixtures/generated/TestEventWrapper.h"

using namespace jcailloux::relais;

// =========================================================================
// Instantiate BaseRepository with each entity type to verify compilation.
// These are direct BaseRepository instantiations (not Repository<>) to avoid
// depending on the full mixin chain (RedisRepo, CachedRepo, etc.) which
// is not yet refactored.
// =========================================================================

using ItemRepo = BaseRepository<
    entity::generated::TestItemWrapper, "test:item", config::Uncached, int64_t>;
using UserRepo = BaseRepository<
    entity::generated::TestUserWrapper, "test:user", config::Uncached, int64_t>;
using OrderRepo = BaseRepository<
    entity::generated::TestOrderWrapper, "test:order", config::Uncached, int64_t>;
using EventRepo = BaseRepository<
    entity::generated::TestEventWrapper, "test:event", config::Uncached, int64_t>;

// Read-only repo
using ReadOnlyItemRepo = BaseRepository<
    entity::generated::TestItemWrapper, "test:item:ro",
    config::Uncached.with_read_only(), int64_t>;

// =========================================================================
// Type trait tests
// =========================================================================

TEST_CASE("BaseRepository type traits", "[base_repo]") {
    SECTION("EntityType is correct") {
        STATIC_REQUIRE(std::is_same_v<
            ItemRepo::EntityType, entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(std::is_same_v<
            UserRepo::EntityType, entity::generated::TestUserWrapper>);
        STATIC_REQUIRE(std::is_same_v<
            OrderRepo::EntityType, entity::generated::TestOrderWrapper>);
        STATIC_REQUIRE(std::is_same_v<
            EventRepo::EntityType, entity::generated::TestEventWrapper>);
    }

    SECTION("Key type") {
        STATIC_REQUIRE(std::is_same_v<ItemRepo::KeyType, int64_t>);
        STATIC_REQUIRE(std::is_same_v<OrderRepo::KeyType, int64_t>);
        STATIC_REQUIRE(std::is_same_v<EventRepo::KeyType, int64_t>);
    }

    SECTION("WrapperPtr type") {
        STATIC_REQUIRE(std::is_same_v<
            ItemRepo::WrapperPtrType,
            std::shared_ptr<const entity::generated::TestItemWrapper>>);
    }

    SECTION("name() returns correct name") {
        REQUIRE(std::string(ItemRepo::name()) == "test:item");
        REQUIRE(std::string(UserRepo::name()) == "test:user");
        REQUIRE(std::string(OrderRepo::name()) == "test:order");
        REQUIRE(std::string(EventRepo::name()) == "test:event");
    }

    SECTION("config is correct") {
        STATIC_REQUIRE(ItemRepo::config.cache_level == config::CacheLevel::None);
        STATIC_REQUIRE(!ItemRepo::config.read_only);
        STATIC_REQUIRE(ReadOnlyItemRepo::config.read_only);
    }
}

// =========================================================================
// Concept tests
// =========================================================================

TEST_CASE("BaseRepository concepts", "[base_repo]") {
    SECTION("ReadableEntity") {
        STATIC_REQUIRE(ReadableEntity<entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(ReadableEntity<entity::generated::TestUserWrapper>);
        STATIC_REQUIRE(ReadableEntity<entity::generated::TestOrderWrapper>);
        STATIC_REQUIRE(ReadableEntity<entity::generated::TestEventWrapper>);
    }

    SECTION("MutableEntity") {
        STATIC_REQUIRE(MutableEntity<entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(MutableEntity<entity::generated::TestUserWrapper>);
        STATIC_REQUIRE(MutableEntity<entity::generated::TestOrderWrapper>);
        STATIC_REQUIRE(MutableEntity<entity::generated::TestEventWrapper>);
    }

    SECTION("Serializable") {
        STATIC_REQUIRE(Serializable<entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(Serializable<entity::generated::TestOrderWrapper>);
    }

    SECTION("Keyed") {
        STATIC_REQUIRE(Keyed<entity::generated::TestItemWrapper, int64_t>);
        STATIC_REQUIRE(Keyed<entity::generated::TestOrderWrapper, int64_t>);
    }

    SECTION("HasFieldUpdate") {
        STATIC_REQUIRE(HasFieldUpdate<entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(HasFieldUpdate<entity::generated::TestOrderWrapper>);
        STATIC_REQUIRE(HasFieldUpdate<entity::generated::TestEventWrapper>);
    }

    SECTION("HasPartitionKey") {
        STATIC_REQUIRE(HasPartitionKey<entity::generated::TestEventWrapper>);
        STATIC_REQUIRE_FALSE(HasPartitionKey<entity::generated::TestItemWrapper>);
        STATIC_REQUIRE_FALSE(HasPartitionKey<entity::generated::TestOrderWrapper>);
        STATIC_REQUIRE_FALSE(HasPartitionKey<entity::generated::TestUserWrapper>);
    }
}

// =========================================================================
// SQL string tests
// =========================================================================

TEST_CASE("SQL strings are correct", "[base_repo][sql]") {
    using ItemMapping = entity::generated::TestItemMapping;

    SECTION("select_by_pk") {
        std::string sql = ItemMapping::SQL::select_by_pk;
        REQUIRE(sql.starts_with("SELECT"));
        REQUIRE(sql.find("FROM relais_test_items") != std::string::npos);
        REQUIRE(sql.find("WHERE id = $1") != std::string::npos);
    }

    SECTION("insert") {
        std::string sql = ItemMapping::SQL::insert;
        REQUIRE(sql.starts_with("INSERT"));
        REQUIRE(sql.find("INTO relais_test_items") != std::string::npos);
        REQUIRE(sql.find("RETURNING") != std::string::npos);
    }

    SECTION("update") {
        std::string sql = ItemMapping::SQL::update;
        REQUIRE(sql.starts_with("UPDATE"));
        REQUIRE(sql.find("relais_test_items") != std::string::npos);
        REQUIRE(sql.find("WHERE id = $1") != std::string::npos);
    }

    SECTION("delete_by_pk") {
        std::string sql = ItemMapping::SQL::delete_by_pk;
        REQUIRE(sql.starts_with("DELETE"));
        REQUIRE(sql.find("FROM relais_test_items") != std::string::npos);
        REQUIRE(sql.find("WHERE id = $1") != std::string::npos);
    }

    SECTION("table_name") {
        REQUIRE(std::string(ItemMapping::table_name) == "relais_test_items");
    }

    SECTION("primary_key_column") {
        REQUIRE(std::string(ItemMapping::primary_key_column) == "id");
    }
}

TEST_CASE("SQL strings for complex entity", "[base_repo][sql]") {
    using OrderMapping = entity::generated::TestOrderMapping;

    SECTION("select_by_pk includes all columns") {
        std::string sql = OrderMapping::SQL::select_by_pk;
        REQUIRE(sql.find("user_id") != std::string::npos);
        REQUIRE(sql.find("amount") != std::string::npos);
        REQUIRE(sql.find("discount") != std::string::npos);
        REQUIRE(sql.find("is_express") != std::string::npos);
        REQUIRE(sql.find("priority") != std::string::npos);
        REQUIRE(sql.find("status") != std::string::npos);
        REQUIRE(sql.find("metadata") != std::string::npos);
        REQUIRE(sql.find("address") != std::string::npos);
        REQUIRE(sql.find("tags") != std::string::npos);
    }

    SECTION("insert skips id (db_managed)") {
        std::string sql = OrderMapping::SQL::insert;
        // The VALUES clause starts with $1 (user_id), not with id
        REQUIRE(sql.find("VALUES ($1,") != std::string::npos);
    }
}

TEST_CASE("SQL strings for partition key entity", "[base_repo][sql][partial_key]") {
    using EventMapping = entity::generated::TestEventMapping;

    SECTION("delete_by_pk uses partial key only") {
        std::string sql = EventMapping::SQL::delete_by_pk;
        REQUIRE(sql.starts_with("DELETE"));
        REQUIRE(sql.find("WHERE id = $1") != std::string::npos);
        // Must NOT include region in partial key delete
        REQUIRE(sql.find("region") == std::string::npos);
    }

    SECTION("delete_by_full_pk includes partition key") {
        std::string sql = EventMapping::SQL::delete_by_full_pk;
        REQUIRE(sql.starts_with("DELETE"));
        REQUIRE(sql.find("WHERE id = $1 AND region = $2") != std::string::npos);
    }

    SECTION("makeFullKeyParams produces correct params") {
        entity::generated::TestEventWrapper event;
        event.id = 42;
        event.region = "eu";
        auto params = EventMapping::makeFullKeyParams(event);
        // PgParams::make(42, "eu") produces 2 parameters
        REQUIRE(params.params.size() == 2);
    }

    SECTION("non-partitioned entity has no delete_by_full_pk") {
        // Structural check: TestItemMapping::SQL does not have delete_by_full_pk
        STATIC_REQUIRE_FALSE(requires {
            entity::generated::TestItemMapping::SQL::delete_by_full_pk;
        });
    }
}

// =========================================================================
// FieldUpdate tests
// =========================================================================

TEST_CASE("FieldUpdate utilities", "[base_repo][field_update]") {
    using Traits = entity::generated::TestItemMapping::TraitsType;
    using Field = Traits::Field;

    SECTION("set() creates FieldUpdate with correct value") {
        auto update = wrapper::set<Field::name>(std::string("test_name"));
        REQUIRE(update.value == "test_name");
    }

    SECTION("set() with integer value") {
        auto update = wrapper::set<Field::value>(42);
        REQUIRE(update.value == 42);
    }

    SECTION("fieldColumnName returns quoted column name") {
        auto update = wrapper::set<Field::name>(std::string("test"));
        auto col = wrapper::fieldColumnName<Traits>(update);
        REQUIRE(col == "\"name\"");
    }

    SECTION("fieldColumnName for value field") {
        auto update = wrapper::set<Field::value>(0);
        auto col = wrapper::fieldColumnName<Traits>(update);
        REQUIRE(col == "\"value\"");
    }

    SECTION("fieldValue returns typed value") {
        auto update = wrapper::set<Field::value>(42);
        auto val = wrapper::fieldValue<Traits>(update);
        STATIC_REQUIRE(std::is_same_v<decltype(val), int32_t>);
        REQUIRE(val == 42);
    }

    SECTION("fieldValue for string field") {
        auto update = wrapper::set<Field::name>(std::string("hello"));
        auto val = wrapper::fieldValue<Traits>(update);
        REQUIRE(val == "hello");
    }

    SECTION("fieldValue for timestamp field returns string") {
        auto update = wrapper::set<Field::created_at>(
            std::string("2024-01-01 00:00:00"));
        auto val = wrapper::fieldValue<Traits>(update);
        STATIC_REQUIRE(std::is_same_v<decltype(val), std::string>);
        REQUIRE(val == "2024-01-01 00:00:00");
    }

    SECTION("fieldValue for boolean field") {
        auto update = wrapper::set<Field::is_active>(true);
        auto val = wrapper::fieldValue<Traits>(update);
        REQUIRE(val == true);
    }
}

TEST_CASE("FieldUpdate with nullable fields", "[base_repo][field_update]") {
    using Traits = entity::generated::TestOrderMapping::TraitsType;
    using Field = Traits::Field;

    SECTION("setNull for nullable field compiles and returns nullptr") {
        auto update = wrapper::setNull<Field::discount>();
        auto val = wrapper::fieldValue<Traits>(update);
        REQUIRE(val == nullptr);
    }

    SECTION("fieldColumnName for nullable field") {
        auto update = wrapper::setNull<Field::discount>();
        auto col = wrapper::fieldColumnName<Traits>(update);
        REQUIRE(col == "\"discount\"");
    }
}

// =========================================================================
// buildUpdateReturning tests
// =========================================================================

TEST_CASE("buildUpdateReturning", "[base_repo][sql]") {
    SECTION("single column") {
        auto sql = detail::buildUpdateReturning(
            "my_table", "id", {"\"name\""});
        REQUIRE(sql == "UPDATE my_table SET \"name\"=$1 WHERE \"id\"=$2 RETURNING *");
    }

    SECTION("multiple columns") {
        auto sql = detail::buildUpdateReturning(
            "my_table", "id", {"\"name\"", "\"value\"", "\"active\""});
        REQUIRE(sql.find("UPDATE my_table SET") == 0);
        REQUIRE(sql.find("\"name\"=$1") != std::string::npos);
        REQUIRE(sql.find("\"value\"=$2") != std::string::npos);
        REQUIRE(sql.find("\"active\"=$3") != std::string::npos);
        REQUIRE(sql.find("WHERE \"id\"=$4") != std::string::npos);
        REQUIRE(sql.find("RETURNING *") != std::string::npos);
    }

    SECTION("with real mapping table name and pk column") {
        using M = entity::generated::TestItemMapping;
        auto sql = detail::buildUpdateReturning(
            M::table_name, M::primary_key_column, {"\"name\"", "\"value\""});
        REQUIRE(sql.find("UPDATE relais_test_items SET") == 0);
        REQUIRE(sql.find("WHERE \"id\"=$3") != std::string::npos);
    }
}

// =========================================================================
// PgParams construction tests
// =========================================================================

TEST_CASE("PgParams construction for CRUD operations", "[base_repo][params]") {
    SECTION("make with mixed types") {
        auto params = jcailloux::relais::io::PgParams::make(
            int64_t(42), std::string("hello"), true, int32_t(100));
        REQUIRE(params.count() == 4);
    }

    SECTION("make with nullable") {
        auto params = jcailloux::relais::io::PgParams::make(
            int64_t(1), nullptr, std::string("test"));
        REQUIRE(params.count() == 3);
        REQUIRE(params.params[1].isNull());
    }

    SECTION("make with optional") {
        std::optional<int32_t> some_val = 42;
        std::optional<int32_t> no_val = std::nullopt;
        auto params = jcailloux::relais::io::PgParams::make(some_val, no_val);
        REQUIRE(params.count() == 2);
        REQUIRE(!params.params[0].isNull());
        REQUIRE(params.params[1].isNull());
    }

    SECTION("toInsertParams produces correct count") {
        entity::generated::TestItemWrapper item;
        item.name = "test";
        item.value = 42;
        item.description = "desc";
        item.is_active = true;
        item.created_at = "2024-01-01";
        auto params = entity::generated::TestItemWrapper::toInsertParams(item);
        // 5 fields: name, value, description, is_active, created_at (no id)
        REQUIRE(params.count() == 5);
    }

    SECTION("update params construction: PK + insert params") {
        entity::generated::TestItemWrapper item;
        item.id = 1;
        item.name = "test";
        item.value = 42;
        item.description = "desc";
        item.is_active = true;
        item.created_at = "2024-01-01";

        auto insertParams = entity::generated::TestItemWrapper::toInsertParams(item);
        jcailloux::relais::io::PgParams updateParams;
        updateParams.params.reserve(insertParams.params.size() + 1);
        // $1 = PK
        updateParams.params.push_back(
            jcailloux::relais::io::PgParams::make(item.id).params[0]);
        // $2...$N = fields
        for (auto& p : insertParams.params)
            updateParams.params.push_back(std::move(p));

        // 6 params: id + 5 fields
        REQUIRE(updateParams.count() == 6);
        REQUIRE(!updateParams.params[0].isNull());
    }
}

// =========================================================================
// Mapping metadata tests
// =========================================================================

TEST_CASE("Mapping metadata", "[base_repo]") {
    SECTION("All mappings have table_name") {
        REQUIRE(std::string(entity::generated::TestItemMapping::table_name) ==
                "relais_test_items");
        REQUIRE(std::string(entity::generated::TestUserMapping::table_name) ==
                "relais_test_users");
        REQUIRE(std::string(entity::generated::TestOrderMapping::table_name) ==
                "relais_test_orders");
        REQUIRE(std::string(entity::generated::TestEventMapping::table_name) ==
                "relais_test_events");
    }

    SECTION("All mappings have primary_key_column") {
        REQUIRE(std::string(entity::generated::TestItemMapping::primary_key_column) == "id");
        REQUIRE(std::string(entity::generated::TestUserMapping::primary_key_column) == "id");
        REQUIRE(std::string(entity::generated::TestOrderMapping::primary_key_column) == "id");
        REQUIRE(std::string(entity::generated::TestEventMapping::primary_key_column) == "id");
    }

    SECTION("read_only flag") {
        STATIC_REQUIRE(!entity::generated::TestItemMapping::read_only);
        STATIC_REQUIRE(!entity::generated::TestOrderMapping::read_only);
    }
}

// =========================================================================
// List key building tests
// =========================================================================

TEST_CASE("List cache key building", "[base_repo]") {
    SECTION("makeGroupKey with string parts") {
        auto key = ItemRepo::makeGroupKey("category", "tech");
        REQUIRE(key == "test:item:list:category:tech");
    }

    SECTION("makeGroupKey with integer parts") {
        auto key = ItemRepo::makeGroupKey(int64_t(42));
        REQUIRE(key == "test:item:list:42");
    }

    SECTION("makeGroupKey with mixed parts") {
        auto key = UserRepo::makeGroupKey("guild", int64_t(123));
        REQUIRE(key == "test:user:list:guild:123");
    }
}
