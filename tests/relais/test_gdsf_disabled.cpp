/**
 * test_gdsf_disabled.cpp
 *
 * Tests that GDSF is fully disabled (zero overhead) when RELAIS_GDSF_ENABLED == 0.
 * This file is compiled WITHOUT RELAIS_GDSF_ENABLED to verify the default path.
 *
 * In the combined binary (test_relais_all), RELAIS_GDSF_ENABLED=1,
 * so these tests are skipped via preprocessor guard.
 *
 * Covers:
 *   1. HasGDSF is false when GDSF disabled
 *   2. Monostate metadata (0 bytes) when no TTL and no GDSF
 *   3. TTL-only metadata (8 bytes) when TTL but no GDSF
 *   4. No GDSFPolicy registration from disabled repos
 */

#include "jcailloux/relais/cache/GDSFPolicy.h"
#include "jcailloux/relais/cache/GDSFMetadata.h"

// These tests only compile when GDSF is disabled.
// In the combined binary (test_relais_all), RELAIS_GDSF_ENABLED=1 -> skip.
#if !RELAIS_GDSF_ENABLED

#include <catch2/catch_test_macros.hpp>
#include <type_traits>

#include "fixtures/TestRepositories.h"

using GDSFPolicy = jcailloux::relais::cache::GDSFPolicy;

// Compile-time check: GDSF must be disabled in this TU
static_assert(!GDSFPolicy::enabled,
    "test_gdsf_disabled.cpp must be compiled without RELAIS_GDSF_ENABLED");

namespace relais_test::gdsf_disabled {

using namespace jcailloux::relais::config;

// Default Local config (has default TTL of 1h, no GDSF)
using DefaultTTLRepo = relais_test::Repo<TestItemWrapper, "gdsf_dis:item", Local>;

// TTL-only (no GDSF, but explicit TTL active)
inline constexpr auto WithTTL = Local
    .with_l1_ttl(std::chrono::seconds{60});

using TTLOnlyRepo = relais_test::Repo<TestItemWrapper, "gdsf_dis:ttl", WithTTL>;

// No TTL, no GDSF — truly no cleanup
inline constexpr auto NoCleanup = Local
    .with_l1_ttl(std::chrono::nanoseconds{0});

using NoCleanupRepo = relais_test::Repo<TestItemWrapper, "gdsf_dis:noclean", NoCleanup>;

} // namespace relais_test::gdsf_disabled

using namespace relais_test::gdsf_disabled;

// =============================================================================
// GDSF disabled - zero overhead when not enabled
// =============================================================================

TEST_CASE("GDSF disabled - zero overhead when not enabled",
          "[gdsf][disabled]")
{
    SECTION("HasGDSF is false when GDSF disabled") {
        static_assert(!GDSFPolicy::enabled);
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
        static_assert(sizeof(Metadata) == sizeof(uint32_t),
            "CacheMetadata<false, true> should be 4 bytes (TTL only)");

        // Verify TTL functionality
        Metadata meta{};
        meta.ttl_expiration_sec = 1000;

        REQUIRE(meta.isExpired(2000));          // now > expiration → expired
        REQUIRE_FALSE(meta.isExpired(500));     // now < expiration → not expired
    }

    SECTION("TTL repos register for global sweep, no-cleanup repos do not") {
        size_t before = GDSFPolicy::instance().nbRepos();

        // DefaultTTLRepo has default TTL (1h) -> HasCleanup == true -> registers
        DefaultTTLRepo::warmup();
        REQUIRE(GDSFPolicy::instance().nbRepos() == before + 1);

        // TTLOnlyRepo has explicit TTL (60s) -> HasCleanup == true -> registers
        TTLOnlyRepo::warmup();
        REQUIRE(GDSFPolicy::instance().nbRepos() == before + 2);

        // NoCleanupRepo has no TTL and no GDSF -> HasCleanup == false -> no registration
        NoCleanupRepo::warmup();
        REQUIRE(GDSFPolicy::instance().nbRepos() == before + 2);
    }
}

#endif // !RELAIS_GDSF_ENABLED
