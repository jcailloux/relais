/**
 * test_base_repository_compile.cpp
 *
 * Compile-time and structural tests for BaseRepo.
 * Verifies that:
 *   - BaseRepo instantiates with all entity types
 *   - Concepts (ReadableEntity, MutableEntity, HasFieldUpdate) are satisfied
 *   - SQL strings are correct
 *   - FieldUpdate utilities (set, fieldColumnName, fieldValue) work
 *   - buildUpdateReturning produces correct SQL
 *   - Type aliases, config, and list key building are correct
 *
 * No actual DB connection is needed — all tests are structural.
 */

#include <catch2/catch_test_macros.hpp>

#include "jcailloux/relais/repository/BaseRepo.h"
#include "fixtures/generated/TestItemWrapper.h"
#include "fixtures/generated/TestUserWrapper.h"
#include "fixtures/generated/TestOrderWrapper.h"
#include "fixtures/generated/TestEventWrapper.h"
#include "fixtures/generated/TestProductWrapper.h"

using namespace jcailloux::relais;

// =========================================================================
// Instantiate BaseRepo with each entity type to verify compilation.
// These are direct BaseRepo instantiations (not Repo<>) to avoid
// depending on the full mixin chain (RedisRepo, CachedRepo, etc.) which
// is not yet refactored.
// =========================================================================

using ItemRepo = BaseRepo<
    entity::generated::TestItemWrapper, "test:item", config::Uncached, int64_t>;
using UserRepo = BaseRepo<
    entity::generated::TestUserWrapper, "test:user", config::Uncached, int64_t>;
using OrderRepo = BaseRepo<
    entity::generated::TestOrderWrapper, "test:order", config::Uncached, int64_t>;
using EventRepo = BaseRepo<
    entity::generated::TestEventWrapper, "test:event", config::Uncached, int64_t>;
using ProductRepo = BaseRepo<
    entity::generated::TestProductWrapper, "test:product", config::Uncached, int64_t>;

// Read-only repo
using ReadOnlyItemRepo = BaseRepo<
    entity::generated::TestItemWrapper, "test:item:ro",
    config::Uncached.with_read_only(), int64_t>;

// =========================================================================
// Type trait tests
// =========================================================================

TEST_CASE("BaseRepo type traits", "[base_repo]") {
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

TEST_CASE("BaseRepo concepts", "[base_repo]") {
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

TEST_CASE("SQL strings for partition key entity", "[base_repo][sql][partition_key]") {
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

    SECTION("non-partitioned entity has delete_by_pk") {
        STATIC_REQUIRE(requires {
            entity::generated::TestItemMapping::SQL::delete_by_pk;
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

    SECTION("fieldValue for string field returns string") {
        auto update = wrapper::set<Field::description>(
            std::string("some description"));
        auto val = wrapper::fieldValue<Traits>(update);
        STATIC_REQUIRE(std::is_same_v<decltype(val), std::string>);
        REQUIRE(val == "some description");
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
            "my_table", "id", {"\"name\""}, "id, name");
        REQUIRE(sql == "UPDATE my_table SET \"name\"=$1 WHERE \"id\"=$2 RETURNING id, name");
    }

    SECTION("multiple columns") {
        auto sql = detail::buildUpdateReturning(
            "my_table", "id", {"\"name\"", "\"value\"", "\"active\""},
            "id, name, value, active");
        REQUIRE(sql.find("UPDATE my_table SET") == 0);
        REQUIRE(sql.find("\"name\"=$1") != std::string::npos);
        REQUIRE(sql.find("\"value\"=$2") != std::string::npos);
        REQUIRE(sql.find("\"active\"=$3") != std::string::npos);
        REQUIRE(sql.find("WHERE \"id\"=$4") != std::string::npos);
        REQUIRE(sql.find("RETURNING id, name, value, active") != std::string::npos);
    }

    SECTION("with real mapping returning_columns") {
        using M = entity::generated::TestItemMapping;
        auto sql = detail::buildUpdateReturning(
            M::table_name, M::primary_key_column,
            {"\"name\"", "\"value\""}, M::SQL::returning_columns);
        REQUIRE(sql.find("UPDATE relais_test_items SET") == 0);
        REQUIRE(sql.find("WHERE \"id\"=$3") != std::string::npos);
        REQUIRE(sql.find("RETURNING id, name, value, description, is_active, created_at") != std::string::npos);
    }

    SECTION("never produces RETURNING *") {
        using M = entity::generated::TestItemMapping;
        auto sql = detail::buildUpdateReturning(
            M::table_name, M::primary_key_column,
            {"\"name\""}, M::SQL::returning_columns);
        REQUIRE(sql.find("RETURNING *") == std::string::npos);
    }
}

TEST_CASE("returning_columns matches select_by_pk column order", "[base_repo][sql]") {
    // Ensures that RETURNING and SELECT use the same column list,
    // so fromRow mapping by index is always consistent.

    SECTION("TestItem") {
        using M = entity::generated::TestItemMapping;
        std::string select = M::SQL::select_by_pk;
        std::string expected_prefix = "SELECT " + std::string(M::SQL::returning_columns) + " FROM";
        REQUIRE(select.find(expected_prefix) == 0);
    }

    SECTION("TestOrder") {
        using M = entity::generated::TestOrderMapping;
        std::string select = M::SQL::select_by_pk;
        std::string expected_prefix = "SELECT " + std::string(M::SQL::returning_columns) + " FROM";
        REQUIRE(select.find(expected_prefix) == 0);
    }

    SECTION("TestEvent") {
        using M = entity::generated::TestEventMapping;
        std::string select = M::SQL::select_by_pk;
        std::string expected_prefix = "SELECT " + std::string(M::SQL::returning_columns) + " FROM";
        REQUIRE(select.find(expected_prefix) == 0);
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

    SECTION("toInsertParams excludes db_managed fields") {
        entity::generated::TestItemWrapper item;
        item.id = 999;
        item.name = "test";
        item.value = 42;
        item.description = "desc";
        item.is_active = true;
        item.created_at = "2024-01-01 00:00:00";
        auto params = entity::generated::TestItemWrapper::toInsertParams(item);
        // db_managed fields (id, created_at) are set on the struct but must
        // NOT appear in insert params — the DB manages them.
        // Only user-supplied fields: name, value, description, is_active
        REQUIRE(params.count() == 4);
    }

    SECTION("update params construction: PK + insert params") {
        entity::generated::TestItemWrapper item;
        item.id = 1;
        item.name = "test";
        item.value = 42;
        item.description = "desc";
        item.is_active = true;

        auto insertParams = entity::generated::TestItemWrapper::toInsertParams(item);
        jcailloux::relais::io::PgParams updateParams;
        updateParams.params.reserve(insertParams.params.size() + 1);
        // $1 = PK
        updateParams.params.push_back(
            jcailloux::relais::io::PgParams::make(item.id).params[0]);
        // $2...$N = fields
        for (auto& p : insertParams.params)
            updateParams.params.push_back(std::move(p));

        // 5 params: id + 4 fields
        REQUIRE(updateParams.count() == 5);
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

// =========================================================================
// column= mapping tests (C++ field names ≠ DB column names)
// =========================================================================

TEST_CASE("SQL strings with column= mapping", "[base_repo][sql][column_mapping]") {
    using M = entity::generated::TestProductMapping;

    SECTION("select_by_pk uses DB column names for mapped fields") {
        std::string sql = M::SQL::select_by_pk;
        // column= mapped fields use DB names
        REQUIRE(sql.find("product_name") != std::string::npos);
        REQUIRE(sql.find("stock_level") != std::string::npos);
        REQUIRE(sql.find("discount_pct") != std::string::npos);
        REQUIRE(sql.find("is_available") != std::string::npos);
        REQUIRE(sql.find("created_at") != std::string::npos);
        // Non-mapped field keeps its C++ name (which IS the DB name)
        REQUIRE(sql.find("description") != std::string::npos);
        // Must NOT contain C++ field names that differ from DB names
        REQUIRE(sql.find("productName") == std::string::npos);
        REQUIRE(sql.find("stockLevel") == std::string::npos);
        REQUIRE(sql.find("discountPct") == std::string::npos);
        REQUIRE(sql.find("createdAt") == std::string::npos);
    }

    SECTION("insert uses DB column names for mapped fields and auto for others") {
        std::string sql = M::SQL::insert;
        REQUIRE(sql.find("product_name") != std::string::npos);
        REQUIRE(sql.find("stock_level") != std::string::npos);
        REQUIRE(sql.find("description") != std::string::npos);
        REQUIRE(sql.find("productName") == std::string::npos);
    }

    SECTION("update uses DB column names for mapped fields and auto for others") {
        std::string sql = M::SQL::update;
        REQUIRE(sql.find("product_name") != std::string::npos);
        REQUIRE(sql.find("stock_level") != std::string::npos);
        REQUIRE(sql.find("description") != std::string::npos);
        REQUIRE(sql.find("productName") == std::string::npos);
    }

    SECTION("returning_columns mixes mapped and auto column names") {
        std::string ret = M::SQL::returning_columns;
        REQUIRE(ret.find("product_name") != std::string::npos);
        REQUIRE(ret.find("stock_level") != std::string::npos);
        REQUIRE(ret.find("is_available") != std::string::npos);
        REQUIRE(ret.find("created_at") != std::string::npos);
        REQUIRE(ret.find("description") != std::string::npos);
        REQUIRE(ret.find("productName") == std::string::npos);
    }

    SECTION("returning_columns matches select_by_pk column order") {
        std::string select = M::SQL::select_by_pk;
        std::string expected_prefix = "SELECT " + std::string(M::SQL::returning_columns) + " FROM";
        REQUIRE(select.find(expected_prefix) == 0);
    }
}

TEST_CASE("column= mapping preserves C++ identifiers", "[base_repo][column_mapping]") {
    using M = entity::generated::TestProductMapping;

    SECTION("Col enum uses C++ field names") {
        REQUIRE(M::Col::productName == 1);
        REQUIRE(M::Col::stockLevel == 2);
        REQUIRE(M::Col::discountPct == 3);
        REQUIRE(M::Col::available == 4);
        REQUIRE(M::Col::description == 5);
        REQUIRE(M::Col::createdAt == 6);
    }

    SECTION("Field enum uses C++ field names") {
        using Field = M::TraitsType::Field;
        // These compile = C++ names are used in the enum
        [[maybe_unused]] auto f1 = Field::productName;
        [[maybe_unused]] auto f2 = Field::stockLevel;
        [[maybe_unused]] auto f3 = Field::discountPct;
        [[maybe_unused]] auto f4 = Field::available;
        [[maybe_unused]] auto f5 = Field::description;
    }

    SECTION("primary_key_column is DB name") {
        REQUIRE(std::string(M::primary_key_column) == "id");
    }

    SECTION("table_name is correct") {
        REQUIRE(std::string(M::table_name) == "relais_test_products");
    }
}

TEST_CASE("FieldInfo column_name uses DB name with column= mapping", "[base_repo][field_update][column_mapping]") {
    using Traits = entity::generated::TestProductMapping::TraitsType;
    using Field = Traits::Field;

    SECTION("fieldColumnName returns DB column name") {
        auto update = wrapper::set<Field::productName>(std::string("test"));
        auto col = wrapper::fieldColumnName<Traits>(update);
        REQUIRE(col == "\"product_name\"");
    }

    SECTION("fieldColumnName for integer field") {
        auto update = wrapper::set<Field::stockLevel>(42);
        auto col = wrapper::fieldColumnName<Traits>(update);
        REQUIRE(col == "\"stock_level\"");
    }

    SECTION("fieldColumnName for boolean field") {
        auto update = wrapper::set<Field::available>(true);
        auto col = wrapper::fieldColumnName<Traits>(update);
        REQUIRE(col == "\"is_available\"");
    }

    SECTION("fieldColumnName for nullable field") {
        auto update = wrapper::setNull<Field::discountPct>();
        auto col = wrapper::fieldColumnName<Traits>(update);
        REQUIRE(col == "\"discount_pct\"");
    }

    SECTION("fieldColumnName for non-mapped field uses C++ name as DB name") {
        auto update = wrapper::set<Field::description>(std::string("test"));
        auto col = wrapper::fieldColumnName<Traits>(update);
        REQUIRE(col == "\"description\"");
    }
}

TEST_CASE("buildUpdateReturning with column= mapping", "[base_repo][sql][column_mapping]") {
    using M = entity::generated::TestProductMapping;
    using Traits = M::TraitsType;
    using Field = Traits::Field;

    SECTION("produces SQL with DB column names") {
        auto update1 = wrapper::set<Field::productName>(std::string("x"));
        auto update2 = wrapper::set<Field::stockLevel>(10);

        auto sql = detail::buildUpdateReturning(
            M::table_name, M::primary_key_column,
            {wrapper::fieldColumnName<Traits>(update1),
             wrapper::fieldColumnName<Traits>(update2)},
            M::SQL::returning_columns);

        // SET clause uses DB names
        REQUIRE(sql.find("\"product_name\"=$1") != std::string::npos);
        REQUIRE(sql.find("\"stock_level\"=$2") != std::string::npos);
        // RETURNING uses DB names
        REQUIRE(sql.find("RETURNING id, product_name, stock_level") != std::string::npos);
        // No C++ names in SQL
        REQUIRE(sql.find("productName") == std::string::npos);
        REQUIRE(sql.find("stockLevel") == std::string::npos);
    }
}
