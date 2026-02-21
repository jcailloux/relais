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

#include <jcailloux/relais/repository/Repo.h>
#include <jcailloux/relais/config/repo_config.h>
#include <jcailloux/relais/cache/InvalidateOn.h>
#include "generated/TestItemWrapper.h"
#include "generated/TestUserWrapper.h"
#include "generated/TestArticleWrapper.h"
#include "generated/TestPurchaseWrapper.h"
#include "generated/TestEventWrapper.h"
#include "generated/TestProductWrapper.h"
#include "generated/TestMembershipWrapper.h"

namespace relais_test {

// Convenience aliases
using jcailloux::relais::Repo;
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
using entity::generated::TestProductWrapper;
using entity::generated::TestMembershipWrapper;

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

/// Short TTL for expiration tests — L1 expires quickly, GDSF evicts on cleanup
inline constexpr auto ShortTTL = Local
    .with_l1_ttl(std::chrono::milliseconds{100});

/// Write-through strategy — PopulateImmediately on update
inline constexpr auto WriteThrough = Local
    .with_update_strategy(UpdateStrategy::PopulateImmediately);

/// Few chunks for predictable cleanup testing
inline constexpr auto FewChunks = Local
    .with_l1_chunk_count_log2(1);  // 2^1 = 2 chunks

/// Read-only presets
inline constexpr auto ReadOnlyUncached = Uncached.with_read_only();
inline constexpr auto ReadOnlyL2 = Redis.with_read_only();

} // namespace test_config

// =============================================================================
// Test Repositories - TestItem (no ListDescriptor)
// =============================================================================

/// No caching — tests BaseRepo directly
using UncachedTestItemRepo = Repo<TestItemWrapper, "test:uncached", cfg::Uncached>;

/// L1 only — tests CachedRepo without Redis
using L1TestItemRepo = Repo<TestItemWrapper, "test:l1">;

/// L2 only — tests RedisRepo
using L2TestItemRepo = Repo<TestItemWrapper, "test:l2", cfg::Redis>;

/// Both L1+L2 — tests full hierarchy
using FullCacheTestItemRepo = Repo<TestItemWrapper, "test:both", cfg::Both>;

// Configuration test repositories
using ShortTTLTestItemRepo = Repo<TestItemWrapper, "test:short_ttl", test_config::ShortTTL>;
using WriteThroughTestItemRepo = Repo<TestItemWrapper, "test:write_through", test_config::WriteThrough>;
using FewChunksTestItemRepo = Repo<TestItemWrapper, "test:few_chunks", test_config::FewChunks>;

// =============================================================================
// User Repositories (no ListDescriptor)
// =============================================================================

using UncachedTestUserRepo = Repo<TestUserWrapper, "test:user:uncached", cfg::Uncached>;
using L1TestUserRepo = Repo<TestUserWrapper, "test:user:l1">;
using L2TestUserRepo = Repo<TestUserWrapper, "test:user:l2", cfg::Redis>;
using FullCacheTestUserRepo = Repo<TestUserWrapper, "test:user:both", cfg::Both>;

// =============================================================================
// Purchase Repositories (has ListDescriptor → ListMixin auto-detected)
// =============================================================================

/// Purchase without cross-invalidation
using UncachedTestPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:uncached", cfg::Uncached>;

/// Purchase L1 with cross-invalidation → User
using L1TestPurchaseRepo = Repo<TestPurchaseWrapper, "test:purchase:l1", cfg::Local,
    cache::Invalidate<L1TestUserRepo, purchaseUserId>>;

// =============================================================================
// Article Repositories (has ListDescriptor → ListMixin auto-detected)
// =============================================================================

using UncachedTestArticleRepo = Repo<TestArticleWrapper, "test:article:uncached", cfg::Uncached>;
using L1TestArticleRepo = Repo<TestArticleWrapper, "test:article:l1">;
using L2TestArticleRepo = Repo<TestArticleWrapper, "test:article:l2", cfg::Redis>;

// =============================================================================
// ListDescriptor Repositories — auto-detected from Entity's embedded descriptor
// =============================================================================

/// Article list — ListMixin auto-detected (TestArticleWrapper has ListDescriptor)
using TestArticleListRepo = Repo<TestArticleWrapper, "test:article:list:l1">;

/// Alias for the augmented descriptor — used by tests building ListDescriptorQuery
using TestArticleListDecl = TestArticleListRepo::ListDescriptorType;

/// Purchase list — same pattern
using TestPurchaseListRepo = Repo<TestPurchaseWrapper, "test:purchase:list:l1">;

// =============================================================================
// Read-only Repositories
// =============================================================================

/// Base-level read-only — no caching, no writes allowed
using ReadOnlyTestItemRepo = Repo<TestItemWrapper, "test:readonly:uncached", test_config::ReadOnlyUncached>;

/// L2 read-only — Redis caching, no writes allowed
using ReadOnlyL2TestItemRepo = Repo<TestItemWrapper, "test:readonly:l2", test_config::ReadOnlyL2>;

/// L2 read-only user — Redis caching, no writes.
/// RedisRepo provides invalidate() for cross-invalidation target use.
using ReadOnlyL2TestUserRepo = Repo<TestUserWrapper, "test:readonly:user:l2", test_config::ReadOnlyL2>;

// =============================================================================
// Product Repositories (column= mapping: C++ field names ≠ DB column names)
// =============================================================================

using UncachedTestProductRepo = Repo<TestProductWrapper, "test:product:uncached", cfg::Uncached>;

inline auto makeTestProduct(
    const std::string& productName,
    int32_t stockLevel = 0,
    std::optional<int32_t> discountPct = std::nullopt,
    bool available = true,
    const std::string& description = {},
    int64_t id = 0
) {
    TestProductWrapper entity;
    entity.id = id;
    entity.productName = productName;
    entity.stockLevel = stockLevel;
    entity.discountPct = discountPct;
    entity.available = available;
    entity.description = description;
    return std::make_shared<const TestProductWrapper>(std::move(entity));
}

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
// Event Repositories (PartitionKey: Key auto-deduced as int64_t from Mapping)
// =============================================================================

using UncachedTestEventRepo = Repo<TestEventWrapper, "test:event:partial:uncached", cfg::Uncached>;
using L1TestEventRepo = Repo<TestEventWrapper, "test:event:partial:l1">;
using L2TestEventRepo = Repo<TestEventWrapper, "test:event:partial:l2", cfg::Redis>;
using L1L2TestEventRepo = Repo<TestEventWrapper, "test:event:partial:both", cfg::Both>;

// =============================================================================
// Membership Repositories (composite key: user_id + group_id)
// =============================================================================

using UncachedTestMembershipRepo = Repo<TestMembershipWrapper, "test:member:uncached", cfg::Uncached>;
using L1TestMembershipRepo = Repo<TestMembershipWrapper, "test:member:l1">;
using L2TestMembershipRepo = Repo<TestMembershipWrapper, "test:member:l2", cfg::Redis>;
using FullCacheTestMembershipRepo = Repo<TestMembershipWrapper, "test:member:both", cfg::Both>;

inline auto makeTestMembership(
    int64_t user_id,
    int64_t group_id,
    const std::string& role = ""
) {
    TestMembershipWrapper entity;
    entity.user_id = user_id;
    entity.group_id = group_id;
    entity.role = role;
    return std::make_shared<const TestMembershipWrapper>(std::move(entity));
}

} // namespace relais_test
