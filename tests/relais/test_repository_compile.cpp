/**
 * test_repository_compile.cpp
 *
 * Compile-time tests for the full Repository mixin chain after Drogon removal.
 * Verifies that Repository.h, CachedRepository.h, InvalidationMixin.h,
 * ListMixin.h, and PartialKeyValidator.h compile without Drogon.
 *
 * Exercises all mixin combinations:
 *   - Uncached (BaseRepository only)
 *   - L1 (CachedRepository)
 *   - L2 (RedisRepository)
 *   - L1+L2 (CachedRepository + RedisRepository)
 *   - With ListDescriptor (ListMixin auto-detected)
 *   - With cross-invalidation (InvalidationMixin)
 *   - Read-only variants
 *
 * No database or Redis connection needed — all tests are structural.
 */

#include <catch2/catch_test_macros.hpp>
#include <type_traits>

#include "fixtures/TestRepositories.h"

using namespace relais_test;

// =============================================================================
// Verify Repository instantiation compiles for all cache levels
// =============================================================================

TEST_CASE("Repository instantiation - all cache levels", "[repository][compile]") {
    SECTION("Uncached - BaseRepository only") {
        STATIC_REQUIRE(std::is_same_v<UncachedTestItemRepository::EntityType,
                                       entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(std::is_same_v<UncachedTestItemRepository::KeyType, int64_t>);
    }

    SECTION("L1 - CachedRepository") {
        STATIC_REQUIRE(std::is_same_v<L1TestItemRepository::EntityType,
                                       entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(std::is_same_v<L1TestItemRepository::KeyType, int64_t>);
    }

    SECTION("L2 - RedisRepository") {
        STATIC_REQUIRE(std::is_same_v<L2TestItemRepository::EntityType,
                                       entity::generated::TestItemWrapper>);
    }

    SECTION("L1+L2 - full hierarchy") {
        STATIC_REQUIRE(std::is_same_v<FullCacheTestItemRepository::EntityType,
                                       entity::generated::TestItemWrapper>);
    }
}

// =============================================================================
// Verify Repository name() works
// =============================================================================

TEST_CASE("Repository name()", "[repository][compile]") {
    REQUIRE(std::string(UncachedTestItemRepository::name()) == "test:uncached");
    REQUIRE(std::string(L1TestItemRepository::name()) == "test:l1");
    REQUIRE(std::string(L2TestItemRepository::name()) == "test:l2");
    REQUIRE(std::string(FullCacheTestItemRepository::name()) == "test:both");
}

// =============================================================================
// Verify config() accessor
// =============================================================================

TEST_CASE("Repository config", "[repository][compile]") {
    SECTION("Uncached") {
        constexpr auto cfg = UncachedTestItemRepository::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::None);
        STATIC_REQUIRE(!cfg.read_only);
    }

    SECTION("L1 local") {
        constexpr auto cfg = L1TestItemRepository::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L1);
    }

    SECTION("L2 Redis") {
        constexpr auto cfg = L2TestItemRepository::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L2);
    }

    SECTION("L1+L2 Both") {
        constexpr auto cfg = FullCacheTestItemRepository::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L1_L2);
    }
}

// =============================================================================
// Verify CachedRepository-specific features compile
// =============================================================================

TEST_CASE("CachedRepository features", "[repository][compile][cached]") {
    SECTION("l1Ttl") {
        auto ttl = L1TestItemRepository::l1Ttl();
        REQUIRE(ttl.count() > 0);
    }

    SECTION("cacheSize") {
        auto size = L1TestItemRepository::cacheSize();
        REQUIRE(size == 0);  // empty at start
    }

    SECTION("triggerCleanup") {
        auto result = L1TestItemRepository::triggerCleanup();
        // Just verify it compiles and returns bool
        (void)result;
    }

    SECTION("warmup") {
        L1TestItemRepository::warmup();
    }
}

// =============================================================================
// Verify config presets compile
// =============================================================================

TEST_CASE("Config presets", "[repository][compile]") {
    SECTION("ShortTTL") {
        constexpr auto cfg = ShortTTLTestItemRepository::config;
        STATIC_REQUIRE(!cfg.l1_accept_expired_on_get);
        STATIC_REQUIRE(!cfg.l1_refresh_on_get);
    }

    SECTION("WriteThrough") {
        constexpr auto cfg = WriteThroughTestItemRepository::config;
        STATIC_REQUIRE(cfg.update_strategy ==
            jcailloux::relais::config::UpdateStrategy::PopulateImmediately);
    }

    SECTION("AcceptExpired") {
        constexpr auto cfg = AcceptExpiredTestItemRepository::config;
        STATIC_REQUIRE(cfg.l1_accept_expired_on_get);
    }

    SECTION("FewShards") {
        constexpr auto cfg = FewShardsTestItemRepository::config;
        STATIC_REQUIRE(cfg.l1_shard_count_log2 == 1);
    }
}

// =============================================================================
// Verify ListMixin auto-detection (Article has ListDescriptor)
// =============================================================================

TEST_CASE("ListMixin auto-detected from ListDescriptor", "[repository][compile][list]") {
    SECTION("Article repo with list") {
        STATIC_REQUIRE(std::is_same_v<TestArticleListRepository::EntityType,
                                       entity::generated::TestArticleWrapper>);
        // ListDescriptorType should exist if ListMixin is active
        using Desc = TestArticleListRepository::ListDescriptorType;
        (void)sizeof(Desc);  // verify type exists
    }
}

// =============================================================================
// Verify InvalidationMixin (cross-invalidation) compiles
// =============================================================================

TEST_CASE("InvalidationMixin with cross-invalidation", "[repository][compile][invalidation]") {
    SECTION("Purchase repo with User invalidation") {
        STATIC_REQUIRE(std::is_same_v<L1TestPurchaseRepository::EntityType,
                                       entity::generated::TestPurchaseWrapper>);
    }
}

// =============================================================================
// Verify read-only repositories compile (write methods should be absent)
// =============================================================================

TEST_CASE("Read-only repositories", "[repository][compile][readonly]") {
    SECTION("ReadOnly uncached") {
        constexpr auto cfg = ReadOnlyTestItemRepository::config;
        STATIC_REQUIRE(cfg.read_only);
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::None);
    }

    SECTION("ReadOnly L2") {
        constexpr auto cfg = ReadOnlyL2TestItemRepository::config;
        STATIC_REQUIRE(cfg.read_only);
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L2);
    }
}

// =============================================================================
// Verify User repository variants compile
// =============================================================================

TEST_CASE("User repository variants", "[repository][compile]") {
    REQUIRE(std::string(UncachedTestUserRepository::name()) == "test:user:uncached");
    REQUIRE(std::string(L1TestUserRepository::name()) == "test:user:l1");
    REQUIRE(std::string(L2TestUserRepository::name()) == "test:user:l2");
    REQUIRE(std::string(FullCacheTestUserRepository::name()) == "test:user:both");
}

// =============================================================================
// Verify Event (PartialKey) repositories compile
// =============================================================================

TEST_CASE("PartialKey event repositories", "[repository][compile][partial_key]") {
    SECTION("Uncached event") {
        STATIC_REQUIRE(std::is_same_v<UncachedTestEventRepository::KeyType, int64_t>);
    }

    SECTION("L1 event") {
        STATIC_REQUIRE(std::is_same_v<L1TestEventRepository::KeyType, int64_t>);
    }

    SECTION("L2 event") {
        STATIC_REQUIRE(std::is_same_v<L2TestEventRepository::KeyType, int64_t>);
    }

    SECTION("L1+L2 event") {
        STATIC_REQUIRE(std::is_same_v<L1L2TestEventRepository::KeyType, int64_t>);
    }
}
