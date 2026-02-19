/**
 * test_gdsf_disabled.cpp
 *
 * Tests that GDSF is fully disabled (zero overhead) when RELAIS_L1_MAX_MEMORY == 0.
 * This file is compiled WITHOUT RELAIS_L1_MAX_MEMORY to verify the default path.
 *
 * In the combined binary (test_relais_all), RELAIS_L1_MAX_MEMORY is set to 268435456,
 * so these tests are skipped via preprocessor guard.
 *
 * Covers:
 *   1. HasGDSF is false when kMaxMemory == 0
 *   2. Monostate metadata (0 bytes) when no TTL and no GDSF
 *   3. TTL-only metadata (8 bytes) when TTL but no GDSF
 *   4. No GDSFPolicy registration from disabled repos
 */

#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/cache/GDSFMetadata.h"

// These tests only compile when GDSF is disabled (kMaxMemory == 0).
// In the combined binary (test_relais_all), RELAIS_L1_MAX_MEMORY=268435456 -> skip.
#if RELAIS_L1_MAX_MEMORY == 0

#include <catch2/catch_test_macros.hpp>
#include <type_traits>

#include "fixtures/TestRepositories.h"

using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;

// Compile-time check: GDSF must be disabled in this TU
static_assert(GDSFPolicy::kMaxMemory == 0,
    "test_gdsf_disabled.cpp must be compiled WITHOUT RELAIS_L1_MAX_MEMORY");

namespace relais_test::gdsf_disabled {

using namespace jcailloux::relais::config;

// Default Local config (no TTL, no GDSF)
using NoGDSFRepo = relais_test::Repo<TestItemWrapper, "gdsf_dis:item", Local>;

// TTL-only (no GDSF, but TTL active)
inline constexpr auto WithTTL = Local
    .with_l1_ttl(std::chrono::seconds{60});

using TTLOnlyRepo = relais_test::Repo<TestItemWrapper, "gdsf_dis:ttl", WithTTL>;

} // namespace relais_test::gdsf_disabled

using namespace relais_test::gdsf_disabled;

// =============================================================================
// GDSF disabled - zero overhead when kMaxMemory == 0
// =============================================================================

TEST_CASE("GDSF disabled - zero overhead when kMaxMemory == 0",
          "[gdsf][disabled]")
{
    SECTION("HasGDSF is false when kMaxMemory == 0") {
        static_assert(GDSFPolicy::kMaxMemory == 0);
        SUCCEED();
    }

    SECTION("monostate metadata when no TTL and no GDSF") {
        using Metadata = jcailloux::relais::cache::CacheMetadata<false, false>;
        static_assert(std::is_empty_v<Metadata>,
            "CacheMetadata<false, false> should be empty (0 bytes via EBO)");
        SUCCEED();
    }

    SECTION("TTL-only metadata when TTL configured but no GDSF") {
        using Metadata = jcailloux::relais::cache::CacheMetadata<false, true>;
        static_assert(sizeof(Metadata) == sizeof(int64_t),
            "CacheMetadata<false, true> should be 8 bytes (TTL only)");

        // Verify TTL functionality
        Metadata meta{};
        meta.ttl_expiration_rep = 1000;

        auto expired_tp = std::chrono::steady_clock::time_point{
            std::chrono::steady_clock::duration{2000}};
        REQUIRE(meta.isExpired(expired_tp));

        auto not_expired_tp = std::chrono::steady_clock::time_point{
            std::chrono::steady_clock::duration{500}};
        REQUIRE_FALSE(meta.isExpired(not_expired_tp));
    }

    SECTION("no GDSFPolicy registration from disabled repos") {
        size_t before = GDSFPolicy::instance().nbRepos();

        // warmup() triggers cache() construction — should NOT register with GDSFPolicy
        NoGDSFRepo::warmup();
        TTLOnlyRepo::warmup();

        size_t after = GDSFPolicy::instance().nbRepos();

        // No new registrations (HasGDSF == false -> if constexpr skips enrollment)
        REQUIRE(after == before);
    }
}

#endif // RELAIS_L1_MAX_MEMORY == 0
