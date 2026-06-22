/**
 * test_recheck_guard.cpp
 * Pure unit tests for RecheckGuard — no DB, no Redis, no event loop.
 *
 * RecheckGuard is the sharded generation counter behind the read-fill recheck.
 * A filling reader snapshots its key's slot at fetch-start, then asks changed()
 * right before it stores; a write bumps the slot. The contract under test:
 *   - snapshot()/changed() never mutate the counter (hit cost = 0);
 *   - bump() makes changed() true for that key (write detected);
 *   - a bump on a DIFFERENT key sharing the slot also trips changed()
 *     (collision = pessimistic miss, NEVER a missed write → never stale);
 *   - a bump on a key in another slot does NOT trip changed() (independence).
 */

#include <cstdint>
#include <optional>

#include <catch2/catch_test_macros.hpp>

#include "jcailloux/relais/repository/RecheckGuard.h"

using jcailloux::relais::RecheckGuard;

namespace {

// 4 slots — collisions are dense, so a colliding key is easy to find.
using Guard = RecheckGuard<"rg_unit", int64_t, 2>;

// Find a key != base that hashes to the same slot as base (a collision).
std::optional<int64_t> collidingKey(int64_t base) {
    const auto target = Guard::slotOf(base);
    for (int64_t k = base + 1; k < base + 100000; ++k) {
        if (Guard::slotOf(k) == target) return k;
    }
    return std::nullopt;
}

// Find a key whose slot differs from base's (independent slot).
std::optional<int64_t> disjointKey(int64_t base) {
    const auto target = Guard::slotOf(base);
    for (int64_t k = base + 1; k < base + 100000; ++k) {
        if (Guard::slotOf(k) != target) return k;
    }
    return std::nullopt;
}

}  // namespace

TEST_CASE("RecheckGuard - no write means no change", "[recheck]") {
    const int64_t key = 1001;
    auto snap = Guard::snapshot(key);
    // Reading the counter must not move it (hit cost = 0).
    REQUIRE_FALSE(Guard::changed(key, snap));
    REQUIRE_FALSE(Guard::changed(key, snap));
    REQUIRE(Guard::snapshot(key) == snap);
}

TEST_CASE("RecheckGuard - a bump trips changed for that key", "[recheck]") {
    const int64_t key = 2002;
    auto snap = Guard::snapshot(key);
    Guard::bump(key);
    REQUIRE(Guard::changed(key, snap));

    // Monotonic: further bumps keep it changed; a fresh snapshot re-baselines.
    Guard::bump(key);
    REQUIRE(Guard::changed(key, snap));
    auto snap2 = Guard::snapshot(key);
    REQUIRE_FALSE(Guard::changed(key, snap2));
}

TEST_CASE("RecheckGuard - collision is pessimistic, never stale", "[recheck]") {
    const int64_t key = 3003;
    auto other = collidingKey(key);
    REQUIRE(other.has_value());
    REQUIRE(*other != key);

    auto snap = Guard::snapshot(key);
    // A write to a DIFFERENT key sharing the slot trips our recheck. The fill
    // is skipped (one extra miss) — pessimistic, but never a missed write.
    Guard::bump(*other);
    REQUIRE(Guard::changed(key, snap));
}

TEST_CASE("RecheckGuard - disjoint slots are independent", "[recheck]") {
    const int64_t key = 4004;
    auto other = disjointKey(key);
    REQUIRE(other.has_value());

    auto snap = Guard::snapshot(key);
    Guard::bump(*other);  // unrelated slot — must not trip our recheck
    REQUIRE_FALSE(Guard::changed(key, snap));
}

TEST_CASE("RecheckGuard - slot count is the configured power of two", "[recheck]") {
    STATIC_REQUIRE(Guard::kSlots == 4);
    STATIC_REQUIRE(RecheckGuard<"rg_big", int64_t, 12>::kSlots == 4096);
}
