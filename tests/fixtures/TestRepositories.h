/**
 * TestRepositories.h
 * Test repositories with different cache configurations.
 * Used to test all levels of the repository hierarchy.
 *
 * All repos are simple `using` aliases — no CRTP, no Config structs.
 * Cross-invalidation is expressed via variadic template parameters.
 * ListMixin is auto-detected when Entity has an embedded ListDescriptor.
 */

#pragma once

#include <jcailloux/relais/repository/Repository.h>
#include <jcailloux/relais/config/repository_config.h>
#include <jcailloux/relais/cache/InvalidateOn.h>
#include "generated/TestItemWrapper.h"
#include "generated/TestUserWrapper.h"
#include "generated/TestArticleWrapper.h"
#include "generated/TestPurchaseWrapper.h"
#include "generated/TestEventWrapper.h"

namespace relais_test {

// Convenience aliases
using jcailloux::relais::Repository;
namespace cache = jcailloux::relais::cache;
namespace cfg = jcailloux::relais::config;

// Type aliases — import generated wrapper names into relais_test namespace
using entity::generated::TestItemWrapper;
using entity::generated::TestUserWrapper;
using entity::generated::TestArticleWrapper;
using entity::generated::TestPurchaseWrapper;
using TestArticleList = entity::generated::TestArticleListWrapper;
using TestPurchaseList = entity::generated::TestPurchaseListWrapper;
using entity::generated::TestEventWrapper;

// Cross-invalidation key extractors
inline constexpr auto purchaseUserId = [](const auto& p) -> int64_t { return p.user_id; };

// =============================================================================
// Entity Construction Helpers
// =============================================================================

inline auto makeTestItem(
    const std::string& name,
    int32_t value = 0,
    const std::string& description = {},
    bool is_active = true,
    int64_t id = 0
) {
    TestItemWrapper entity;
    entity.id = id;
    entity.name = name;
    entity.value = value;
    entity.description = description;
    entity.is_active = is_active;
    return std::make_shared<const TestItemWrapper>(std::move(entity));
}

inline auto makeTestUser(
    const std::string& username,
    const std::string& email,
    int32_t balance = 0,
    int64_t id = 0
) {
    TestUserWrapper entity;
    entity.id = id;
    entity.username = username;
    entity.email = email;
    entity.balance = balance;
    return std::make_shared<const TestUserWrapper>(std::move(entity));
}

inline auto makeTestPurchase(
    int64_t user_id,
    const std::string& product_name,
    int32_t amount,
    const std::string& status = "pending",
    int64_t id = 0
) {
    TestPurchaseWrapper entity;
    entity.id = id;
    entity.user_id = user_id;
    entity.product_name = product_name;
    entity.amount = amount;
    entity.status = status;
    return std::make_shared<const TestPurchaseWrapper>(std::move(entity));
}

inline auto makeTestArticle(
    const std::string& category,
    int64_t author_id,
    const std::string& title,
    std::optional<int32_t> view_count = std::nullopt,
    bool is_published = false,
    int64_t id = 0
) {
    TestArticleWrapper entity;
    entity.id = id;
    entity.category = category;
    entity.author_id = author_id;
    entity.title = title;
    entity.view_count = view_count;
    entity.is_published = is_published;
    return std::make_shared<const TestArticleWrapper>(std::move(entity));
}

// =============================================================================
// CacheConfig presets for tests
// =============================================================================

namespace test_config {

using namespace jcailloux::relais::config;

/// Short TTL for expiration tests — L1 expires quickly, no expired acceptance
inline constexpr auto ShortTTL = Local
    .with_l1_ttl(std::chrono::milliseconds{100})
    .with_l1_accept_expired_on_get(false)
    .with_l1_refresh_on_get(false);

/// Write-through strategy — PopulateImmediately on update
inline constexpr auto WriteThrough = Local
    .with_update_strategy(UpdateStrategy::PopulateImmediately);

/// No refresh on get — TTL not extended on cache hit
inline constexpr auto NoRefresh = Local
    .with_l1_ttl(std::chrono::milliseconds{200})
    .with_l1_refresh_on_get(false)
    .with_l1_accept_expired_on_get(true);

/// Accept expired entries — returns expired entries until cleanup
inline constexpr auto AcceptExpired = Local
    .with_l1_ttl(std::chrono::milliseconds{100})
    .with_l1_accept_expired_on_get(true)
    .with_l1_refresh_on_get(false);

/// Few shards for predictable cleanup testing
inline constexpr auto FewShards = Local
    .with_l1_shard_count_log2(1);  // 2^1 = 2 shards

/// Read-only presets
inline constexpr auto ReadOnlyUncached = Uncached.with_read_only();
inline constexpr auto ReadOnlyL2 = Redis.with_read_only();

} // namespace test_config

// =============================================================================
// Test Repositories - TestItem (no ListDescriptor)
// =============================================================================

/// No caching — tests BaseRepository directly
using UncachedTestItemRepository = Repository<TestItemWrapper, "test:uncached", cfg::Uncached>;

/// L1 only — tests CachedRepository without Redis
using L1TestItemRepository = Repository<TestItemWrapper, "test:l1">;

/// L2 only — tests RedisRepository
using L2TestItemRepository = Repository<TestItemWrapper, "test:l2", cfg::Redis>;

/// Both L1+L2 — tests full hierarchy
using FullCacheTestItemRepository = Repository<TestItemWrapper, "test:both", cfg::Both>;

// Configuration test repositories
using ShortTTLTestItemRepository = Repository<TestItemWrapper, "test:short_ttl", test_config::ShortTTL>;
using WriteThroughTestItemRepository = Repository<TestItemWrapper, "test:write_through", test_config::WriteThrough>;
using NoRefreshTestItemRepository = Repository<TestItemWrapper, "test:no_refresh", test_config::NoRefresh>;
using AcceptExpiredTestItemRepository = Repository<TestItemWrapper, "test:accept_expired", test_config::AcceptExpired>;
using FewShardsTestItemRepository = Repository<TestItemWrapper, "test:few_shards", test_config::FewShards>;

// =============================================================================
// User Repositories (no ListDescriptor)
// =============================================================================

using UncachedTestUserRepository = Repository<TestUserWrapper, "test:user:uncached", cfg::Uncached>;
using L1TestUserRepository = Repository<TestUserWrapper, "test:user:l1">;
using L2TestUserRepository = Repository<TestUserWrapper, "test:user:l2", cfg::Redis>;
using FullCacheTestUserRepository = Repository<TestUserWrapper, "test:user:both", cfg::Both>;

// =============================================================================
// Purchase Repositories (has ListDescriptor → ListMixin auto-detected)
// =============================================================================

/// Purchase without cross-invalidation
using UncachedTestPurchaseRepository = Repository<TestPurchaseWrapper, "test:purchase:uncached", cfg::Uncached>;

/// Purchase L1 with cross-invalidation → User
using L1TestPurchaseRepository = Repository<TestPurchaseWrapper, "test:purchase:l1", cfg::Local,
    cache::Invalidate<L1TestUserRepository, purchaseUserId>>;

// =============================================================================
// Article Repositories (has ListDescriptor → ListMixin auto-detected)
// =============================================================================

using UncachedTestArticleRepository = Repository<TestArticleWrapper, "test:article:uncached", cfg::Uncached>;
using L1TestArticleRepository = Repository<TestArticleWrapper, "test:article:l1">;
using L2TestArticleRepository = Repository<TestArticleWrapper, "test:article:l2", cfg::Redis>;

// =============================================================================
// ListDescriptor Repositories — auto-detected from Entity's embedded descriptor
// =============================================================================

/// Article list — ListMixin auto-detected (TestArticleWrapper has ListDescriptor)
using TestArticleListRepository = Repository<TestArticleWrapper, "test:article:list:l1">;

/// Alias for the augmented descriptor — used by tests building ListDescriptorQuery
using TestArticleListDecl = TestArticleListRepository::ListDescriptorType;

/// Purchase list — same pattern
using TestPurchaseListRepository = Repository<TestPurchaseWrapper, "test:purchase:list:l1">;

// =============================================================================
// Read-only Repositories
// =============================================================================

/// Base-level read-only — no caching, no writes allowed
using ReadOnlyTestItemRepository = Repository<TestItemWrapper, "test:readonly:uncached", test_config::ReadOnlyUncached>;

/// L2 read-only — Redis caching, no writes allowed
using ReadOnlyL2TestItemRepository = Repository<TestItemWrapper, "test:readonly:l2", test_config::ReadOnlyL2>;

/// L2 read-only user — Redis caching, no writes.
/// RedisRepository provides invalidate() for cross-invalidation target use.
using ReadOnlyL2TestUserRepository = Repository<TestUserWrapper, "test:readonly:user:l2", test_config::ReadOnlyL2>;

// =============================================================================
// Event Construction Helper
// =============================================================================

// Cross-invalidation key extractor: Event -> User
inline constexpr auto eventUserId = [](const auto& e) -> int64_t { return e.user_id; };

inline auto makeTestEvent(
    const std::string& region,
    int64_t user_id,
    const std::string& title,
    int32_t priority = 0,
    int64_t id = 0
) {
    TestEventWrapper entity;
    entity.id = id;
    entity.region = region;
    entity.user_id = user_id;
    entity.title = title;
    entity.priority = priority;
    return std::make_shared<const TestEventWrapper>(std::move(entity));
}

// =============================================================================
// Event Repositories (PartialKey: Key auto-deduced as int64_t from Mapping)
// =============================================================================

using UncachedTestEventRepository = Repository<TestEventWrapper, "test:event:partial:uncached", cfg::Uncached>;
using L1TestEventRepository = Repository<TestEventWrapper, "test:event:partial:l1">;
using L2TestEventRepository = Repository<TestEventWrapper, "test:event:partial:l2", cfg::Redis>;
using L1L2TestEventRepository = Repository<TestEventWrapper, "test:event:partial:both", cfg::Both>;

} // namespace relais_test
