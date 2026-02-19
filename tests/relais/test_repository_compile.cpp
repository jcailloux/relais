/**
 * test_repository_compile.cpp
 *
 * Compile-time tests for the full Repo mixin chain.
 * Verifies that Repo.h, CachedRepo.h, InvalidationMixin.h,
 * and ListMixin.h compile correctly.
 *
 * Exercises all mixin combinations:
 *   - Uncached (BaseRepo only)
 *   - L1 (CachedRepo)
 *   - L2 (RedisRepo)
 *   - L1+L2 (CachedRepo + RedisRepo)
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
// Verify Repo instantiation compiles for all cache levels
// =============================================================================

TEST_CASE("Repo instantiation - all cache levels", "[repository][compile]") {
    SECTION("Uncached - BaseRepo only") {
        STATIC_REQUIRE(std::is_same_v<UncachedTestItemRepo::EntityType,
                                       entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(std::is_same_v<UncachedTestItemRepo::KeyType, int64_t>);
    }

    SECTION("L1 - CachedRepo") {
        STATIC_REQUIRE(std::is_same_v<L1TestItemRepo::EntityType,
                                       entity::generated::TestItemWrapper>);
        STATIC_REQUIRE(std::is_same_v<L1TestItemRepo::KeyType, int64_t>);
    }

    SECTION("L2 - RedisRepo") {
        STATIC_REQUIRE(std::is_same_v<L2TestItemRepo::EntityType,
                                       entity::generated::TestItemWrapper>);
    }

    SECTION("L1+L2 - full hierarchy") {
        STATIC_REQUIRE(std::is_same_v<FullCacheTestItemRepo::EntityType,
                                       entity::generated::TestItemWrapper>);
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
// Verify CachedRepo-specific features compile
// =============================================================================

TEST_CASE("CachedRepo features", "[repository][compile][cached]") {
    SECTION("l1Ttl") {
        auto ttl = L1TestItemRepo::l1Ttl();
        REQUIRE(ttl.count() > 0);
    }

    SECTION("size") {
        auto size = L1TestItemRepo::size();
        REQUIRE(size == 0);  // empty at start
    }

    SECTION("trySweep") {
        auto result = L1TestItemRepo::trySweep();
        // Just verify it compiles and returns bool
        (void)result;
    }

    SECTION("sweep") {
        auto result = L1TestItemRepo::sweep();
        (void)result;
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

    SECTION("FewShards") {
        constexpr auto cfg = FewShardsTestItemRepo::config;
        STATIC_REQUIRE(cfg.l1_shard_count_log2 == 1);
    }
}

// =============================================================================
// Verify ListMixin auto-detection (Article has ListDescriptor)
// =============================================================================

TEST_CASE("ListMixin auto-detected from ListDescriptor", "[repository][compile][list]") {
    SECTION("Article repo with list") {
        STATIC_REQUIRE(std::is_same_v<TestArticleListRepo::EntityType,
                                       entity::generated::TestArticleWrapper>);
        // ListDescriptorType should exist if ListMixin is active
        using Desc = TestArticleListRepo::ListDescriptorType;
        (void)sizeof(Desc);  // verify type exists
    }

    SECTION("sweep — unified (entity + list)") {
        (void)TestArticleListRepo::trySweep();
        (void)TestArticleListRepo::sweep();
        auto erased = TestArticleListRepo::purge();
        REQUIRE(erased == 0);
    }

    SECTION("sweepEntities — entity cache only") {
        (void)TestArticleListRepo::trySweepEntities();
        (void)TestArticleListRepo::sweepEntities();
        auto erased = TestArticleListRepo::purgeEntities();
        REQUIRE(erased == 0);
    }

    SECTION("sweepLists — list cache only") {
        (void)TestArticleListRepo::trySweepLists();
        (void)TestArticleListRepo::sweepLists();
        auto erased = TestArticleListRepo::purgeLists();
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
                                       entity::generated::TestPurchaseWrapper>);
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
