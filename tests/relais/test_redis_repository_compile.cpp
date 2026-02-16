/**
 * test_redis_repository_compile.cpp
 *
 * Compile-time and structural tests for RedisRepository and RedisCache
 * after the Drogon -> direct I/O refactoring. Verifies that:
 *   - RedisRepository instantiates with all entity types (no Drogon needed)
 *   - Type aliases, config, and l2Ttl are correct
 *   - RedisCache namespace is jcailloux::relais::cache
 *   - InvalidateOn types compile in the new namespace
 *   - makeRedisKey produces correct keys
 *   - InvalidationData helpers work
 *
 * No actual Redis connection is needed — all tests are structural.
 */

#include <catch2/catch_test_macros.hpp>

#include "jcailloux/relais/repository/RedisRepository.h"
#include "jcailloux/relais/cache/InvalidateOn.h"
#include "fixtures/generated/TestItemWrapper.h"
#include "fixtures/generated/TestUserWrapper.h"
#include "fixtures/generated/TestOrderWrapper.h"

using namespace jcailloux::relais;

// =========================================================================
// Instantiate RedisRepository with each entity type to verify compilation.
// L2 TTL = 5min in nanoseconds.
// =========================================================================

static constexpr auto kRedisConfig = config::CacheConfig{
    .cache_level = config::CacheLevel::L2,
    .l2_ttl = std::chrono::minutes(5),
};

using ItemRedisRepo = RedisRepository<
    entity::generated::TestItemWrapper, "test:item:redis", kRedisConfig, int64_t>;
using UserRedisRepo = RedisRepository<
    entity::generated::TestUserWrapper, "test:user:redis", kRedisConfig, int64_t>;
using OrderRedisRepo = RedisRepository<
    entity::generated::TestOrderWrapper, "test:order:redis", kRedisConfig, int64_t>;

// With l2_refresh_on_get
static constexpr auto kRedisRefreshConfig = config::CacheConfig{
    .cache_level = config::CacheLevel::L2,
    .l2_ttl = std::chrono::minutes(10),
    .l2_refresh_on_get = true,
};

using ItemRedisRefreshRepo = RedisRepository<
    entity::generated::TestItemWrapper, "test:item:redis:refresh", kRedisRefreshConfig, int64_t>;

// Read-only variant
static constexpr auto kReadOnlyRedisConfig = kRedisConfig.with_read_only();

using ReadOnlyItemRedisRepo = RedisRepository<
    entity::generated::TestItemWrapper, "test:item:redis:ro", kReadOnlyRedisConfig, int64_t>;

// =========================================================================
// Type trait tests
// =========================================================================

TEST_CASE("RedisRepository type traits", "[redis_repo]") {
    SECTION("EntityType is correct") {
        STATIC_REQUIRE(std::is_same_v<
            ItemRedisRepo::EntityType, entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(std::is_same_v<
            UserRedisRepo::EntityType, entity::generated::TestUserWrapper>);
        STATIC_REQUIRE(std::is_same_v<
            OrderRedisRepo::EntityType, entity::generated::TestOrderWrapper>);
    }

    SECTION("Key type") {
        STATIC_REQUIRE(std::is_same_v<ItemRedisRepo::KeyType, int64_t>);
        STATIC_REQUIRE(std::is_same_v<OrderRedisRepo::KeyType, int64_t>);
    }

    SECTION("WrapperPtrType is shared_ptr<const Entity>") {
        STATIC_REQUIRE(std::is_same_v<
            ItemRedisRepo::WrapperPtrType,
            std::shared_ptr<const entity::generated::TestItemWrapper>>);
    }

    SECTION("name() returns correct name") {
        REQUIRE(std::string(ItemRedisRepo::name()) == "test:item:redis");
        REQUIRE(std::string(UserRedisRepo::name()) == "test:user:redis");
        REQUIRE(std::string(OrderRedisRepo::name()) == "test:order:redis");
    }

    SECTION("config is correct") {
        STATIC_REQUIRE(ItemRedisRepo::config.cache_level == config::CacheLevel::L2);
        STATIC_REQUIRE(!ItemRedisRepo::config.read_only);
        STATIC_REQUIRE(ReadOnlyItemRedisRepo::config.read_only);
    }
}

// =========================================================================
// L2 TTL tests
// =========================================================================

TEST_CASE("RedisRepository l2Ttl", "[redis_repo]") {
    SECTION("l2Ttl returns configured duration") {
        auto ttl = ItemRedisRepo::l2Ttl();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
        REQUIRE(seconds == 300);
    }

    SECTION("l2Ttl with refresh config") {
        auto ttl = ItemRedisRefreshRepo::l2Ttl();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
        REQUIRE(seconds == 600);
    }
}

// =========================================================================
// Redis key generation tests
// =========================================================================

TEST_CASE("RedisRepository makeRedisKey", "[redis_repo]") {
    SECTION("integer key") {
        auto key = ItemRedisRepo::makeRedisKey(42);
        REQUIRE(key == "test:item:redis:42");
    }

    SECTION("large integer key") {
        auto key = ItemRedisRepo::makeRedisKey(int64_t(9999999999));
        REQUIRE(key == "test:item:redis:9999999999");
    }

    SECTION("different repos produce different keys") {
        auto item_key = ItemRedisRepo::makeRedisKey(1);
        auto user_key = UserRedisRepo::makeRedisKey(1);
        REQUIRE(item_key != user_key);
        REQUIRE(item_key == "test:item:redis:1");
        REQUIRE(user_key == "test:user:redis:1");
    }
}

// =========================================================================
// Concept verification tests
// =========================================================================

TEST_CASE("RedisRepository concepts", "[redis_repo]") {
    SECTION("CacheableEntity is satisfied") {
        STATIC_REQUIRE(CacheableEntity<entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(CacheableEntity<entity::generated::TestUserWrapper>);
        STATIC_REQUIRE(CacheableEntity<entity::generated::TestOrderWrapper>);
    }

    SECTION("HasJsonSerialization is satisfied") {
        STATIC_REQUIRE(HasJsonSerialization<entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(HasJsonSerialization<entity::generated::TestOrderWrapper>);
    }

    SECTION("HasBinarySerialization is satisfied") {
        STATIC_REQUIRE(HasBinarySerialization<entity::generated::TestItemWrapper>);
    }

    SECTION("CreatableEntity is satisfied") {
        STATIC_REQUIRE(CreatableEntity<entity::generated::TestItemWrapper, int64_t>);
        STATIC_REQUIRE(CreatableEntity<entity::generated::TestOrderWrapper, int64_t>);
    }
}

// =========================================================================
// Group key tests (same logic as BaseRepository but via RedisRepository)
// =========================================================================

TEST_CASE("RedisRepository group keys", "[redis_repo]") {
    SECTION("makeGroupKey with string parts") {
        auto key = ItemRedisRepo::makeGroupKey("category", "tech");
        REQUIRE(key == "test:item:redis:list:category:tech");
    }

    SECTION("makeGroupKey with integer parts") {
        auto key = ItemRedisRepo::makeGroupKey(int64_t(42));
        REQUIRE(key == "test:item:redis:list:42");
    }
}

// =========================================================================
// InvalidationData tests
// =========================================================================

TEST_CASE("InvalidationData helpers", "[redis_repo][invalidation]") {
    using Entity = entity::generated::TestItemWrapper;
    using Data = cache::InvalidationData<Entity>;

    SECTION("forCreate") {
        auto entity = std::make_shared<const Entity>();
        auto data = Data::forCreate(entity);
        REQUIRE(data.isCreate());
        REQUIRE(!data.isUpdate());
        REQUIRE(!data.isDelete());
        REQUIRE(!data.old_entity.has_value());
        REQUIRE(data.new_entity.has_value());
    }

    SECTION("forUpdate") {
        auto old_e = std::make_shared<const Entity>();
        auto new_e = std::make_shared<const Entity>();
        auto data = Data::forUpdate(old_e, new_e);
        REQUIRE(!data.isCreate());
        REQUIRE(data.isUpdate());
        REQUIRE(!data.isDelete());
        REQUIRE(data.old_entity.has_value());
        REQUIRE(data.new_entity.has_value());
    }

    SECTION("forDelete") {
        auto entity = std::make_shared<const Entity>();
        auto data = Data::forDelete(entity);
        REQUIRE(!data.isCreate());
        REQUIRE(!data.isUpdate());
        REQUIRE(data.isDelete());
        REQUIRE(data.old_entity.has_value());
        REQUIRE(!data.new_entity.has_value());
    }
}

// =========================================================================
// InvalidateOn empty specialization compile test
// =========================================================================

TEST_CASE("InvalidateOn<> empty specialization", "[redis_repo][invalidation]") {
    // Verify the empty InvalidateOn compiles and the types exist
    using EmptyInvalidation = cache::InvalidateOn<>;
    STATIC_REQUIRE(std::is_class_v<EmptyInvalidation>);
}

// =========================================================================
// RedisCache namespace verification
// =========================================================================

TEST_CASE("RedisCache is in correct namespace", "[redis_cache]") {
    // Verify RedisCache is accessible in jcailloux::relais::cache
    STATIC_REQUIRE(std::is_class_v<cache::RedisCache>);
}
