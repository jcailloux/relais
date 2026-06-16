/**
 * test_list_limits.cpp
 * Runtime limit handling against per-descriptor allowedLimits grids.
 *
 * normalizeLimit rounds a requested page size up to the first allowed step (the
 * largest step when the request exceeds the grid). isLimitAllowed enforces
 * strict membership. Both read Descriptor::allowedLimits when present and fall
 * back to kDefaultLimits otherwise. The grid feeds the canonical cache key, so
 * these must be deterministic per descriptor — a descriptor with maxLimit 50
 * must never normalize to or accept 100.
 */

#include <array>
#include <cstdint>

#include <catch2/catch_test_macros.hpp>

#include "fixtures/generated/TestArticleEntity.h"
#include "fixtures/generated/TestCompositeKeyListEntity.h"
#include "fixtures/generated/TestLimitsEntity.h"
#include "fixtures/generated/TestLimitsMessyEntity.h"

#include "jcailloux/relais/list/spec/GeneratedTraits.h"

using namespace jcailloux::relais::list::spec;

namespace {

template<typename E>
using Desc = typename E::MappingType::ListDescriptor;

// Five-value grid {5,10,20,50,100}.
using L5 = Desc<entity::generated::TestLimitsEntity>;
// Deduped grid {10,25,50}, max 50.
using LM = Desc<entity::generated::TestLimitsMessyEntity>;
// {10,25,50}, max 50.
using LA = Desc<entity::generated::TestArticleEntity>;
// Single-value grid {50}.
using LC = Desc<entity::generated::TestCompositeKeyListEntity>;

// Descriptor without allowedLimits — exercises the kDefaultLimits fallback.
struct NoLimits {};

}  // namespace

TEST_CASE("normalizeLimit rounds up to the next allowed step", "[list][limits]") {
    // Below the first step rounds up to the first step.
    CHECK(normalizeLimit<L5>(1) == 5);
    CHECK(normalizeLimit<L5>(5) == 5);
    // Between steps rounds up to the next.
    CHECK(normalizeLimit<L5>(6) == 10);
    CHECK(normalizeLimit<L5>(11) == 20);
    CHECK(normalizeLimit<L5>(21) == 50);
    CHECK(normalizeLimit<L5>(50) == 50);
    CHECK(normalizeLimit<L5>(99) == 100);
    CHECK(normalizeLimit<L5>(100) == 100);
}

TEST_CASE("normalizeLimit clamps over-grid requests to the largest step", "[list][limits]") {
    CHECK(normalizeLimit<L5>(101) == 100);
    CHECK(normalizeLimit<L5>(65535) == 100);
    CHECK(normalizeLimit<L5>(0) == 5);  // 0 rounds up to the smallest step
}

TEST_CASE("normalizeLimit honors a deduped/sorted grid", "[list][limits]") {
    // limits=50,10,10,25 -> {10,25,50}
    CHECK(normalizeLimit<LM>(1) == 10);
    CHECK(normalizeLimit<LM>(11) == 25);
    CHECK(normalizeLimit<LM>(26) == 50);
    CHECK(normalizeLimit<LM>(50) == 50);
}

TEST_CASE("normalizeLimit respects a single-value grid", "[list][limits]") {
    CHECK(normalizeLimit<LC>(1) == 50);
    CHECK(normalizeLimit<LC>(50) == 50);
    CHECK(normalizeLimit<LC>(51) == 50);
}

// Regression: before per-descriptor grids, normalizeLimit ignored the
// descriptor and returned the raw request up to a fixed max of 100, so a
// maxLimit-50 model could leak a limit=100 page into its cache key.
TEST_CASE("a maxLimit-50 grid never normalizes to 100", "[list][limits]") {
    CHECK(normalizeLimit<LA>(100) == 50);
    CHECK(normalizeLimit<LA>(75) == 50);
    CHECK(normalizeLimit<LM>(100) == 50);
}

TEST_CASE("isLimitAllowed is strict membership against the grid", "[list][limits]") {
    CHECK(isLimitAllowed<L5>(5));
    CHECK(isLimitAllowed<L5>(20));
    CHECK(isLimitAllowed<L5>(100));
    CHECK_FALSE(isLimitAllowed<L5>(6));
    CHECK_FALSE(isLimitAllowed<L5>(25));   // 25 not in {5,10,20,50,100}

    // maxLimit-50 grid rejects 100.
    CHECK(isLimitAllowed<LA>(50));
    CHECK_FALSE(isLimitAllowed<LA>(100));
}

TEST_CASE("validateLimit mirrors isLimitAllowed", "[list][limits]") {
    CHECK_FALSE(validateLimit<L5>(20).has_value());
    auto err = validateLimit<LA>(100);
    REQUIRE(err.has_value());
    CHECK(err->type == QueryValidationError::Type::InvalidLimit);
    CHECK(err->limit == 100);
}

TEST_CASE("getAllowedLimitsString lists the grid in order", "[list][limits]") {
    CHECK(getAllowedLimitsString<L5>() == "5, 10, 20, 50, 100");
    CHECK(getAllowedLimitsString<LM>() == "10, 25, 50");
    CHECK(getAllowedLimitsString<LC>() == "50");
}

// Hand-written / annotation-less descriptors fall back to kDefaultLimits.
TEST_CASE("descriptors without allowedLimits use the default grid", "[list][limits]") {
    CHECK(normalizeLimit<NoLimits>(1) == 10);
    CHECK(normalizeLimit<NoLimits>(11) == 25);
    CHECK(normalizeLimit<NoLimits>(101) == 100);
    CHECK(isLimitAllowed<NoLimits>(25));
    CHECK_FALSE(isLimitAllowed<NoLimits>(20));
    CHECK(getAllowedLimitsString<NoLimits>() == "10, 25, 50, 100");
}
