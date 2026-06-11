/**
 * test_repository_compile.cpp
 *
 * Compile-time tests for the full Repo mixin chain.
 * Verifies that Repo.h, LocalRepo.h, InvalidationMixin.h,
 * and ListMixin.h compile correctly.
 *
 * Exercises all mixin combinations:
 *   - Uncached (PgRepo only)
 *   - L1 (LocalRepo)
 *   - L2 (RedisRepo)
 *   - L1+L2 (LocalRepo + RedisRepo)
 *   - With ListDescriptor (ListMixin auto-detected)
 *   - With cross-invalidation (InvalidationMixin)
 *   - Read-only variants
 *
 * No database or Redis connection needed — all tests are structural.
 */

#include <catch2/catch_test_macros.hpp>
#include <type_traits>
#include <tuple>

#include "fixtures/TestRepositories.h"

using namespace relais_test;

// Detects whether a full UPDATE ... SET is generated for an entity. The
// generator suppresses toUpdateParams (and SQL::update) when there is no
// updatable column, so this is false for read-only views and all-PK junctions.
template<typename E>
concept HasToUpdateParams = requires(const E& e) {
    { E::toUpdateParams(e) }
        -> std::convertible_to<jcailloux::relais::io::PgParams>;
};

// Detects whether the full Repo chain exposes update(). Gated on HasFullUpdate
// across every mixin layer, so it disappears for entities with no updatable
// column rather than failing to instantiate at the call site.
template<typename Repo>
concept RepoHasUpdate = requires(const typename Repo::KeyType& k,
                                 const typename Repo::EntityType& e) {
    Repo::update(k, e);
};

// =============================================================================
// Verify Repo instantiation compiles for all cache levels
// =============================================================================

TEST_CASE("Repo instantiation - all cache levels", "[repository][compile]") {
    SECTION("Uncached - PgRepo only") {
        STATIC_REQUIRE(std::is_same_v<UncachedTestItemRepo::EntityType,
                                       entity::generated::TestItemEntity>);
        STATIC_REQUIRE(std::is_same_v<UncachedTestItemRepo::KeyType, int64_t>);
    }

    SECTION("L1 - LocalRepo") {
        STATIC_REQUIRE(std::is_same_v<L1TestItemRepo::EntityType,
                                       entity::generated::TestItemEntity>);
        STATIC_REQUIRE(std::is_same_v<L1TestItemRepo::KeyType, int64_t>);
    }

    SECTION("L2 - RedisRepo") {
        STATIC_REQUIRE(std::is_same_v<L2TestItemRepo::EntityType,
                                       entity::generated::TestItemEntity>);
    }

    SECTION("L1+L2 - full hierarchy") {
        STATIC_REQUIRE(std::is_same_v<FullCacheTestItemRepo::EntityType,
                                       entity::generated::TestItemEntity>);
    }
}

// =============================================================================
// Verify Repo name() works
// =============================================================================

TEST_CASE("Repo name()", "[repository][compile]") {
    REQUIRE(std::string(UncachedTestItemRepo::name()) == "test:uncached");
    REQUIRE(std::string(L1TestItemRepo::name()) == "test:l1");
    REQUIRE(std::string(L2TestItemRepo::name()) == "test:l2");
    REQUIRE(std::string(FullCacheTestItemRepo::name()) == "test:both");
}

// =============================================================================
// Verify config() accessor
// =============================================================================

TEST_CASE("Repo config", "[repository][compile]") {
    SECTION("Uncached") {
        constexpr auto cfg = UncachedTestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::None);
        STATIC_REQUIRE(!cfg.read_only);
    }

    SECTION("L1 local") {
        constexpr auto cfg = L1TestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L1);
    }

    SECTION("L2 Redis") {
        constexpr auto cfg = L2TestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L2);
    }

    SECTION("L1+L2 Both") {
        constexpr auto cfg = FullCacheTestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L1_L2);
    }
}

// =============================================================================
// Verify LocalRepo-specific features compile
// =============================================================================

TEST_CASE("LocalRepo features", "[repository][compile][cached]") {
    SECTION("l1Ttl") {
        auto ttl = L1TestItemRepo::l1Ttl();
        REQUIRE(ttl.count() > 0);
    }

    SECTION("size") {
        auto size = L1TestItemRepo::size();
        REQUIRE(size == 0);  // empty at start
    }

    SECTION("purge") {
        auto erased = L1TestItemRepo::purge();
        REQUIRE(erased == 0);  // empty cache
    }

    SECTION("warmup") {
        L1TestItemRepo::warmup();
    }
}

// =============================================================================
// Verify config presets compile
// =============================================================================

TEST_CASE("Config presets", "[repository][compile]") {
    SECTION("ShortTTL") {
        constexpr auto cfg = ShortTTLTestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L1);
    }

    SECTION("WriteThrough") {
        constexpr auto cfg = WriteThroughTestItemRepo::config;
        STATIC_REQUIRE(cfg.update_strategy ==
            jcailloux::relais::config::UpdateStrategy::PopulateImmediately);
    }

    SECTION("FewChunks") {
        constexpr auto cfg = FewChunksTestItemRepo::config;
        STATIC_REQUIRE(cfg.l1_chunk_count_log2 == 1);
    }
}

// =============================================================================
// Verify ListMixin auto-detection (Article has ListDescriptor)
// =============================================================================

TEST_CASE("ListMixin auto-detected from ListDescriptor", "[repository][compile][list]") {
    SECTION("Article repo with list") {
        STATIC_REQUIRE(std::is_same_v<TestArticleListRepo::EntityType,
                                       entity::generated::TestArticleEntity>);
        // ListDescriptorType should exist if ListMixin is active
        using Desc = TestArticleListRepo::ListDescriptorType;
        (void)sizeof(Desc);  // verify type exists
    }

    SECTION("purge — unified (entity + list)") {
        auto erased = TestArticleListRepo::purge();
        REQUIRE(erased == 0);
    }

    SECTION("listSize") {
        REQUIRE(TestArticleListRepo::listSize() == 0);
    }
}

// =============================================================================
// Verify InvalidationMixin (cross-invalidation) compiles
// =============================================================================

TEST_CASE("InvalidationMixin with cross-invalidation", "[repository][compile][invalidation]") {
    SECTION("Purchase repo with User invalidation") {
        STATIC_REQUIRE(std::is_same_v<L1TestPurchaseRepo::EntityType,
                                       entity::generated::TestPurchaseEntity>);
    }
}

// =============================================================================
// Verify read-only repositories compile (write methods should be absent)
// =============================================================================

TEST_CASE("Read-only repositories", "[repository][compile][readonly]") {
    SECTION("ReadOnly uncached") {
        constexpr auto cfg = ReadOnlyTestItemRepo::config;
        STATIC_REQUIRE(cfg.read_only);
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::None);
    }

    SECTION("ReadOnly L2") {
        constexpr auto cfg = ReadOnlyL2TestItemRepo::config;
        STATIC_REQUIRE(cfg.read_only);
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L2);
    }
}

// =============================================================================
// All-PK junction: every column is part of the composite key, so there is no
// updatable column. Regression for the empty TraitsType::Field path (Entity<>
// must still instantiate) and suppressed SQL::update/toUpdateParams.
// =============================================================================

TEST_CASE("All-PK junction entity", "[repository][compile][junction]") {
    using Entity = entity::generated::TestAllPkJunctionEntity;

    SECTION("Entity<> instantiates with an empty Field enum") {
        using Field = Entity::TraitsType::Field;   // would fail to compile if absent
        STATIC_REQUIRE(std::is_enum_v<Field>);
    }

    SECTION("composite key type") {
        STATIC_REQUIRE(std::is_same_v<UncachedTestAllPkJunctionRepo::KeyType,
                                       std::tuple<int64_t, int64_t>>);
    }

    SECTION("no toUpdateParams generated (no updatable column)") {
        STATIC_REQUIRE(!HasToUpdateParams<Entity>);
        // sanity: a normal entity DOES expose it
        STATIC_REQUIRE(HasToUpdateParams<entity::generated::TestItemEntity>);
    }

    SECTION("update() is cleanly absent from the whole chain") {
        STATIC_REQUIRE(!RepoHasUpdate<FullCacheTestAllPkJunctionRepo>);
        STATIC_REQUIRE(!RepoHasUpdate<L1TestAllPkJunctionRepo>);
        STATIC_REQUIRE(!RepoHasUpdate<UncachedTestAllPkJunctionRepo>);
        // sanity: a normal entity's repo keeps update()
        STATIC_REQUIRE(RepoHasUpdate<FullCacheTestItemRepo>);
    }

    SECTION("full mixin chain instantiates (L1+L2)") {
        STATIC_REQUIRE(std::is_same_v<FullCacheTestAllPkJunctionRepo::EntityType, Entity>);
        REQUIRE(FullCacheTestAllPkJunctionRepo::size() == 0);
        REQUIRE(std::string(UncachedTestAllPkJunctionRepo::name()) == "test:junction:uncached");
    }
}

// =============================================================================
// Read-only view (@relais read_only): no writes, empty Field enum.
// =============================================================================

TEST_CASE("Read-only view entity", "[repository][compile][readonly]") {
    using Entity = entity::generated::TestReadOnlyViewEntity;

    SECTION("mapping is read-only and Entity<> instantiates") {
        STATIC_REQUIRE(Entity::read_only);
        using Field = Entity::TraitsType::Field;
        STATIC_REQUIRE(std::is_enum_v<Field>);
    }

    SECTION("no toUpdateParams generated") {
        STATIC_REQUIRE(!HasToUpdateParams<Entity>);
    }

    SECTION("read-only repo chain instantiates") {
        STATIC_REQUIRE(UncachedTestReadOnlyViewRepo::config.read_only);
        STATIC_REQUIRE(L2TestReadOnlyViewRepo::config.read_only);
        STATIC_REQUIRE(std::is_same_v<UncachedTestReadOnlyViewRepo::KeyType, int64_t>);
        REQUIRE(std::string(UncachedTestReadOnlyViewRepo::name()) == "test:roview:uncached");
    }
}

// =============================================================================
// Verify User repository variants compile
// =============================================================================

TEST_CASE("User repository variants", "[repository][compile]") {
    REQUIRE(std::string(UncachedTestUserRepo::name()) == "test:user:uncached");
    REQUIRE(std::string(L1TestUserRepo::name()) == "test:user:l1");
    REQUIRE(std::string(L2TestUserRepo::name()) == "test:user:l2");
    REQUIRE(std::string(FullCacheTestUserRepo::name()) == "test:user:both");
}

// =============================================================================
// Verify Event (PartitionKey) repositories compile
// =============================================================================

TEST_CASE("PartitionKey event repositories", "[repository][compile][partition_key]") {
    SECTION("Uncached event") {
        STATIC_REQUIRE(std::is_same_v<UncachedTestEventRepo::KeyType, int64_t>);
    }

    SECTION("L1 event") {
        STATIC_REQUIRE(std::is_same_v<L1TestEventRepo::KeyType, int64_t>);
    }

    SECTION("L2 event") {
        STATIC_REQUIRE(std::is_same_v<L2TestEventRepo::KeyType, int64_t>);
    }

    SECTION("L1+L2 event") {
        STATIC_REQUIRE(std::is_same_v<L1L2TestEventRepo::KeyType, int64_t>);
    }
}
