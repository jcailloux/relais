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
#include <unordered_map>

#include <catch2/catch_test_macros.hpp>

#include "fixtures/generated/TestArticleEntity.h"
#include "fixtures/generated/TestCompositeKeyListEntity.h"
#include "fixtures/generated/TestLimitsEntity.h"
#include "fixtures/generated/TestLimitsMessyEntity.h"

#include "jcailloux/relais/list/spec/GeneratedTraits.h"
#include "jcailloux/relais/list/spec/HttpQueryParser.h"
#include "jcailloux/relais/list/ListCacheTraits.h"
#include "jcailloux/relais/config/CacheConfig.h"
#include <jcailloux/relais/repository/Repo.h>

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

// --- HasLimitConfig size-agnostic contract (Étape 3) ----------------------
// A Traits surface exposing a uint16_t grid of arbitrary length must satisfy
// HasLimitConfig — no fixed-size-4 assumption.

template<size_t N>
struct FakeTraits {
    static constexpr std::array<uint16_t, N> limitSteps =
        []{ std::array<uint16_t, N> a{}; for (size_t i = 0; i < N; ++i) a[i] = uint16_t(i + 1); return a; }();
    static constexpr uint16_t maxLimit = limitSteps.back();
    static uint16_t normalizeLimit(uint16_t r) { return r; }
};

// Missing limitSteps entirely — must NOT satisfy the concept.
struct NoGrid {
    static constexpr uint16_t maxLimit = 50;
    static uint16_t normalizeLimit(uint16_t r) { return r; }
};

namespace lst = jcailloux::relais::list;
static_assert(lst::HasLimitConfig<FakeTraits<1>, int>, "size-1 grid satisfies HasLimitConfig");
static_assert(lst::HasLimitConfig<FakeTraits<3>, int>, "size-3 grid satisfies HasLimitConfig");
static_assert(lst::HasLimitConfig<FakeTraits<4>, int>, "size-4 grid still satisfies HasLimitConfig");
static_assert(lst::HasLimitConfig<FakeTraits<5>, int>, "size-5 grid satisfies HasLimitConfig");
static_assert(!lst::HasLimitConfig<NoGrid, int>, "no grid -> not satisfied");

// --- ListMixin::ListTraits derives its grid from the descriptor (Étape 3) -
// limitSteps/maxLimit must mirror the model's allowedLimits, not a fixed
// {10,25,50,100}. Type-only instantiation — no DB connection.
namespace rel = jcailloux::relais;
using FiveRepo   = rel::Repo<entity::generated::TestLimitsEntity, "test:limits:traits", rel::config::Both>;
using SingleRepo = rel::Repo<entity::generated::TestCompositeKeyListEntity, "test:single:traits", rel::config::Both>;

static_assert(FiveRepo::ListTraits::limitSteps == std::array<uint16_t, 5>{5, 10, 20, 50, 100},
              "ListMixin Traits sources the 5-value grid from the descriptor");
static_assert(FiveRepo::ListTraits::maxLimit == 100, "maxLimit == grid back");
static_assert(SingleRepo::ListTraits::limitSteps == std::array<uint16_t, 1>{50},
              "single-value grid flows through to ListMixin Traits");
static_assert(SingleRepo::ListTraits::maxLimit == 50);

// --- defaultLimit<Descriptor> (Étape 4) -----------------------------------
// The default page size (no `limit` param) is allowedLimits.front() for a
// generated descriptor, falling back to the ListQuery struct default of 20 for
// a hand-written descriptor without a grid.
static_assert(defaultLimit<L5>() == 5, "default = grid front (5,10,20,50,100)");
static_assert(defaultLimit<LM>() == 10, "default = grid front (10,25,50)");
static_assert(defaultLimit<LC>() == 50, "default = grid front (single-value 50)");
static_assert(defaultLimit<NoLimits>() == 20, "no grid -> ListQuery struct default 20");

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

// All Étape 3 checks (HasLimitConfig size-agnosticism, ListMixin Traits grid
// derivation) are compile-time static_asserts above — reaching here means they
// held.
TEST_CASE("limit config is size-agnostic and descriptor-derived", "[list][limits]") {
    SUCCEED();
}

// Étape 4: a request omitting `limit` falls back to the descriptor's declared
// default page size, not the struct default of 20.
TEST_CASE("parse without a limit param uses the descriptor default", "[list][limits]") {
    // parseListQuery requires the augmented descriptor (Entity alias) exposed as
    // Repo::ListDescriptorType, not the raw embedded ListDescriptor.
    using FiveDesc   = FiveRepo::ListDescriptorType;    // grid {5,10,20,50,100}, default 5
    using SingleDesc = SingleRepo::ListDescriptorType;  // grid {50}, default 50
    const std::unordered_map<std::string, std::string> no_params;

    SECTION("tolerant parser") {
        CHECK(parseListQuery<FiveDesc>(no_params).limit == 5);
        CHECK(parseListQuery<SingleDesc>(no_params).limit == 50);
    }

    SECTION("strict parser") {
        auto q5 = parseListQueryStrict<FiveDesc>(no_params);
        REQUIRE(q5.has_value());
        CHECK(q5->limit == 5);

        auto qc = parseListQueryStrict<SingleDesc>(no_params);
        REQUIRE(qc.has_value());
        CHECK(qc->limit == 50);
    }

    SECTION("an explicit limit still overrides the default") {
        const std::unordered_map<std::string, std::string> with_limit{{"limit", "20"}};
        CHECK(parseListQuery<FiveDesc>(with_limit).limit == 20);  // 20 ∈ {5,10,20,50,100}
    }
}
