/**
 * test_gen_list_limits.cpp
 * Generator support for per-model page-size grids (limits= annotation).
 *
 * Proves the Python entity generator emits a `std::array allowedLimits` from
 * `@relais_list limits=...`, for an ARBITRARY number of values, and that it
 * sorts ascending + deduplicates the input. The derived scalars defaultLimit
 * and maxLimit must equal the front and back of the sorted array — the
 * precondition normalizeLimit's round-up relies on.
 *
 * Pure compile-time: the generated descriptors are inspected directly, no
 * DB / Redis.
 */

#include <array>
#include <cstdint>
#include <type_traits>

#include "fixtures/generated/TestArticleEntity.h"
#include "fixtures/generated/TestCompositeKeyListEntity.h"
#include "fixtures/generated/TestLimitsEntity.h"
#include "fixtures/generated/TestLimitsMessyEntity.h"

namespace {

template<typename E>
using Desc = typename E::MappingType::ListDescriptor;

// Five-value arbitrary grid emitted verbatim (N != 4).
using L5 = Desc<entity::generated::TestLimitsEntity>;
static_assert(std::is_same_v<std::remove_const_t<decltype(L5::allowedLimits)>,
                             std::array<uint16_t, 5>>,
              "five-value grid -> std::array<uint16_t, 5>");
static_assert(L5::allowedLimits == std::array<uint16_t, 5>{5, 10, 20, 50, 100});
static_assert(L5::defaultLimit == 5, "defaultLimit == front");
static_assert(L5::maxLimit == 100, "maxLimit == back");

// Unsorted + duplicate input is normalized: 50,10,10,25 -> {10,25,50}.
using LM = Desc<entity::generated::TestLimitsMessyEntity>;
static_assert(std::is_same_v<std::remove_const_t<decltype(LM::allowedLimits)>,
                             std::array<uint16_t, 3>>,
              "messy input deduped to 3 values");
static_assert(LM::allowedLimits == std::array<uint16_t, 3>{10, 25, 50},
              "generator sorts ascending and dedups");
static_assert(LM::defaultLimit == 10, "defaultLimit == sorted front");
static_assert(LM::maxLimit == 50, "maxLimit == sorted back");

// Existing fixtures keep their grids unchanged.
using LA = Desc<entity::generated::TestArticleEntity>;
static_assert(LA::allowedLimits == std::array<uint16_t, 3>{10, 25, 50});
static_assert(LA::defaultLimit == 10 && LA::maxLimit == 50);

// Single-value grid: front == back.
using LC = Desc<entity::generated::TestCompositeKeyListEntity>;
static_assert(LC::allowedLimits == std::array<uint16_t, 1>{50});
static_assert(LC::defaultLimit == 50 && LC::maxLimit == 50);

}  // namespace

// Catch2 main is linked; a no-op TU is enough — all checks are static_asserts.
#include <catch2/catch_test_macros.hpp>

TEST_CASE("generator emits sorted/deduped allowedLimits", "[list][limits][generator]") {
    // All assertions are compile-time; reaching here means they held.
    SUCCEED();
}
