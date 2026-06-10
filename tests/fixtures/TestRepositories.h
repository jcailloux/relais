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
#include <jcailloux/relais/config/CacheConfig.h>
#include <jcailloux/relais/repository/InvalidateOn.h>
#include "generated/TestItemEntity.h"
#include "generated/TestUserEntity.h"
#include "generated/TestArticleEntity.h"
#include "generated/TestPurchaseEntity.h"
#include "generated/TestEventEntity.h"
#include "generated/TestProductEntity.h"
#include "generated/TestMembershipEntity.h"
#include "generated/TestAssignedKeyEntity.h"

namespace relais_test {

// Convenience aliases
using jcailloux::relais::Repo;
using jcailloux::relais::Invalidate;
using jcailloux::relais::InvalidateOn;
using jcailloux::relais::InvalidateList;
using jcailloux::relais::InvalidateVia;
using jcailloux::relais::InvalidateListVia;
namespace cache = jcailloux::relais::cache;
namespace cfg = jcailloux::relais::config;

// Type aliases — import generated wrapper names into relais_test namespace
using entity::generated::TestItemEntity;
using entity::generated::TestUserEntity;
using entity::generated::TestArticleEntity;
using entity::generated::TestPurchaseEntity;
using TestArticleList = entity::generated::TestArticleListWrapper;
using TestPurchaseList = entity::generated::TestPurchaseListWrapper;
using entity::generated::TestEventEntity;
using entity::generated::TestProductEntity;
using entity::generated::TestMembershipEntity;
using entity::generated::TestAssignedKeyEntity;

// Cross-invalidation key extractors
inline constexpr auto purchaseUserId = [](const auto& p) -> int64_t { return p.user_id; };

// =============================================================================
// Entity Construction Helpers
// =============================================================================

inline TestItemEntity makeTestItem(
    const std::string& name,
    int32_t value = 0,
    const std::string& description = {},
    bool is_active = true,
    int64_t id = 0
) {
    TestItemEntity entity;
    entity.id = id;
    entity.name = name;
    entity.value = value;
    entity.description = description;
    entity.is_active = is_active;
    return entity;
}

inline TestUserEntity makeTestUser(
    const std::string& username,
    const std::string& email,
    int32_t balance = 0,
    int64_t id = 0
) {
    TestUserEntity entity;
    entity.id = id;
    entity.username = username;
    entity.email = email;
    entity.balance = balance;
    return entity;
}

inline TestPurchaseEntity makeTestPurchase(
    int64_t user_id,
    const std::string& product_name,
    int32_t amount,
    const std::string& status = "pending",
    int64_t id = 0
) {
    TestPurchaseEntity entity;
    entity.id = id;
    entity.user_id = user_id;
    entity.product_name = product_name;
    entity.amount = amount;
    entity.status = status;
    return entity;
}

inline TestArticleEntity makeTestArticle(
    const std::string& category,
    int64_t author_id,
    const std::string& title,
    std::optional<int32_t> view_count = std::nullopt,
    bool is_published = false,
    int64_t id = 0
) {
    TestArticleEntity entity;
    entity.id = id;
    entity.category = category;
    entity.author_id = author_id;
    entity.title = title;
    entity.view_count = view_count;
    entity.is_published = is_published;
    return entity;
}

// =============================================================================
// CacheConfig presets for tests
// =============================================================================

namespace test_config {

using namespace jcailloux::relais::config;

/// Short TTL for expiration tests — L1 expires quickly, GDSF evicts on cleanup
/// CachedClock uses uint32_t seconds → minimum useful TTL is 1 second.
inline constexpr auto ShortTTL = Local
    .with_l1_ttl(std::chrono::seconds{1});

/// Write-through strategy — PopulateImmediately on update
inline constexpr auto WriteThrough = Local
    .with_update_strategy(UpdateStrategy::PopulateImmediately);

/// Few chunks for predictable cleanup testing
inline constexpr auto FewChunks = Local
    .with_l1_chunk_count_log2(1);  // 2^1 = 2 chunks

/// Bare L1 for benchmarks — no TTL, no GDSF, zero metadata per entry
inline constexpr auto BareL1 = Local
    .with_l1_ttl(Duration{});

/// Read-only presets
inline constexpr auto ReadOnlyUncached = Uncached.with_read_only();
inline constexpr auto ReadOnlyL2 = Redis.with_read_only();

} // namespace test_config

// =============================================================================
// Test Repositories - TestItem (no ListDescriptor)
// =============================================================================

/// No caching — tests PgRepo directly
using UncachedTestItemRepo = Repo<TestItemEntity, "test:uncached", cfg::Uncached>;

/// L1 only — tests LocalRepo without Redis
using L1TestItemRepo = Repo<TestItemEntity, "test:l1">;

/// L2 only — tests RedisRepo
using L2TestItemRepo = Repo<TestItemEntity, "test:l2", cfg::Redis>;

/// Both L1+L2 — tests full hierarchy
using FullCacheTestItemRepo = Repo<TestItemEntity, "test:both", cfg::Both>;

// Configuration test repositories
using ShortTTLTestItemRepo = Repo<TestItemEntity, "test:short_ttl", test_config::ShortTTL>;
using WriteThroughTestItemRepo = Repo<TestItemEntity, "test:write_through", test_config::WriteThrough>;
using FewChunksTestItemRepo = Repo<TestItemEntity, "test:few_chunks", test_config::FewChunks>;

// =============================================================================
// User Repositories (no ListDescriptor)
// =============================================================================

using UncachedTestUserRepo = Repo<TestUserEntity, "test:user:uncached", cfg::Uncached>;
using L1TestUserRepo = Repo<TestUserEntity, "test:user:l1">;
using L2TestUserRepo = Repo<TestUserEntity, "test:user:l2", cfg::Redis>;
using FullCacheTestUserRepo = Repo<TestUserEntity, "test:user:both", cfg::Both>;

// =============================================================================
// Purchase Repositories (has ListDescriptor → ListMixin auto-detected)
// =============================================================================

/// Purchase without cross-invalidation
using UncachedTestPurchaseRepo = Repo<TestPurchaseEntity, "test:purchase:uncached", cfg::Uncached>;

/// Purchase L1 with cross-invalidation → User
using L1TestPurchaseRepo = Repo<TestPurchaseEntity, "test:purchase:l1", cfg::Local,
    Invalidate<L1TestUserRepo, purchaseUserId>>;

// =============================================================================
// Article Repositories (has ListDescriptor → ListMixin auto-detected)
// =============================================================================

using UncachedTestArticleRepo = Repo<TestArticleEntity, "test:article:uncached", cfg::Uncached>;
using L1TestArticleRepo = Repo<TestArticleEntity, "test:article:l1">;
using L2TestArticleRepo = Repo<TestArticleEntity, "test:article:l2", cfg::Redis>;

// =============================================================================
// ListDescriptor Repositories — auto-detected from Entity's embedded descriptor
// =============================================================================

/// Article list — ListMixin auto-detected (TestArticleEntity has ListDescriptor)
using TestArticleListRepo = Repo<TestArticleEntity, "test:article:list:l1">;

/// Alias for the augmented descriptor — used by tests building ListDescriptorQuery
using TestArticleListDecl = TestArticleListRepo::ListDescriptorType;

/// Purchase list — same pattern
using TestPurchaseListRepo = Repo<TestPurchaseEntity, "test:purchase:list:l1">;

// =============================================================================
// Read-only Repositories
// =============================================================================

/// Base-level read-only — no caching, no writes allowed
using ReadOnlyTestItemRepo = Repo<TestItemEntity, "test:readonly:uncached", test_config::ReadOnlyUncached>;

/// L2 read-only — Redis caching, no writes allowed
using ReadOnlyL2TestItemRepo = Repo<TestItemEntity, "test:readonly:l2", test_config::ReadOnlyL2>;

/// L2 read-only user — Redis caching, no writes.
/// RedisRepo provides invalidate() for cross-invalidation target use.
using ReadOnlyL2TestUserRepo = Repo<TestUserEntity, "test:readonly:user:l2", test_config::ReadOnlyL2>;

// =============================================================================
// Product Repositories (column= mapping: C++ field names ≠ DB column names)
// =============================================================================

using UncachedTestProductRepo = Repo<TestProductEntity, "test:product:uncached", cfg::Uncached>;

inline TestProductEntity makeTestProduct(
    const std::string& productName,
    int32_t stockLevel = 0,
    std::optional<int32_t> discountPct = std::nullopt,
    bool available = true,
    const std::string& description = {},
    int64_t id = 0
) {
    TestProductEntity entity;
    entity.id = id;
    entity.productName = productName;
    entity.stockLevel = stockLevel;
    entity.discountPct = discountPct;
    entity.available = available;
    entity.description = description;
    return entity;
}

// =============================================================================
// Event Construction Helper
// =============================================================================

// Cross-invalidation key extractor: Event -> User
inline constexpr auto eventUserId = [](const auto& e) -> int64_t { return e.user_id; };

inline TestEventEntity makeTestEvent(
    const std::string& region,
    int64_t user_id,
    const std::string& title,
    int32_t priority = 0,
    int64_t id = 0
) {
    TestEventEntity entity;
    entity.id = id;
    entity.region = region;
    entity.user_id = user_id;
    entity.title = title;
    entity.priority = priority;
    return entity;
}

// =============================================================================
// Event Repositories (PartitionKey: Key auto-deduced as int64_t from Mapping)
// =============================================================================

using UncachedTestEventRepo = Repo<TestEventEntity, "test:event:partial:uncached", cfg::Uncached>;
using L1TestEventRepo = Repo<TestEventEntity, "test:event:partial:l1">;
using L2TestEventRepo = Repo<TestEventEntity, "test:event:partial:l2", cfg::Redis>;
using L1L2TestEventRepo = Repo<TestEventEntity, "test:event:partial:both", cfg::Both>;

// =============================================================================
// Membership Repositories (composite key: user_id + group_id)
// =============================================================================

using UncachedTestMembershipRepo = Repo<TestMembershipEntity, "test:member:uncached", cfg::Uncached>;
using L1TestMembershipRepo = Repo<TestMembershipEntity, "test:member:l1">;
using L2TestMembershipRepo = Repo<TestMembershipEntity, "test:member:l2", cfg::Redis>;
using FullCacheTestMembershipRepo = Repo<TestMembershipEntity, "test:member:both", cfg::Both>;

inline TestMembershipEntity makeTestMembership(
    int64_t user_id,
    int64_t group_id,
    const std::string& role = ""
) {
    TestMembershipEntity entity;
    entity.user_id = user_id;
    entity.group_id = group_id;
    entity.role = role;
    return entity;
}

// =============================================================================
// AssignedKey Repositories (simple PK, assigned by caller — not db_managed)
// =============================================================================

using UncachedTestAssignedKeyRepo = Repo<TestAssignedKeyEntity, "test:akey:uncached", cfg::Uncached>;
using L1TestAssignedKeyRepo = Repo<TestAssignedKeyEntity, "test:akey:l1">;
using FullCacheTestAssignedKeyRepo = Repo<TestAssignedKeyEntity, "test:akey:both", cfg::Both>;

inline TestAssignedKeyEntity makeTestAssignedKey(
    int64_t key_id,
    int64_t payload = 0,
    const std::string& note = ""
) {
    TestAssignedKeyEntity entity;
    entity.key_id = key_id;
    entity.payload = payload;
    entity.note = note;
    return entity;
}

} // namespace relais_test
