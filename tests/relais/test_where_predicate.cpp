/**
 * test_where_predicate.cpp
 * Étape 7 — predicate list fast-path for eraseWhere: ONE RangeModification (L1)
 * + ONE predicate EVAL (L2) for the whole deleted set, instead of N per-entity
 * blob-array invalidations.
 *
 * §1 (pure, no I/O) — the new primitives:
 *   - SortBounds::isRangeInRange — range overlap, the range generalization of the
 *     per-entity point check (lo==hi). ASC/DESC × first/incomplete/middle.
 *   - predicateGroupCompatible — never-miss filter pruning: EQ-differ / IN-disjoint
 *     prune, absent constraint is a wildcard.
 *   - predicateSortRange — aligned dimension narrows [lo,hi]; orthogonal stays full.
 *
 * §4 (L1 end-to-end) — duality vs blob-array:
 *   - eraseWhere drives the fast-path: one range mod, zero per-entity mods.
 *   - never-miss: the purged group's cached list reflects the deletion.
 *   - filter-aware pruning: the orthogonal group's cache survives (stays stale) —
 *     the distinctive win over a coarse range-only invalidation.
 */

#include <limits>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"
#include "fixtures/generated/TestArticleEntity.h"

#include <jcailloux/relais/list/ListCache.h>
#include <jcailloux/relais/list/spec/FilterDescriptor.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>
#include <jcailloux/relais/list/spec/GeneratedFilters.h>
#include <jcailloux/relais/list/spec/GeneratedTraits.h>

namespace decl = jcailloux::relais::list::spec;
using namespace relais_test;
using ArticleEntity = entity::generated::TestArticleEntity;
using jcailloux::relais::list::SortBounds;
using jcailloux::relais::list::SortDirection;

// ===========================================================================
// §1 — SortBounds::isRangeInRange (pure). A page covers [first,last] (ASC) or
// [last,first] (DESC); the open first/incomplete ends extend to ±∞. The point
// check is the degenerate lo==hi case.
// ===========================================================================

TEST_CASE("isRangeInRange overlaps a middle ASC page exactly", "[where][unit][range]") {
    // ASC complete middle page covering [10, 20].
    SortBounds b{.first_value = 10, .last_value = 20, .is_valid = true};
    const bool fp = false, inc = false, desc = false;

    CHECK(b.isRangeInRange(15, 18, fp, inc, desc));   // inside
    CHECK(b.isRangeInRange(0, 12, fp, inc, desc));     // straddles low edge
    CHECK(b.isRangeInRange(18, 99, fp, inc, desc));    // straddles high edge
    CHECK(b.isRangeInRange(10, 10, fp, inc, desc));    // point on first
    CHECK(b.isRangeInRange(20, 20, fp, inc, desc));    // point on last
    CHECK_FALSE(b.isRangeInRange(0, 9, fp, inc, desc));    // strictly below
    CHECK_FALSE(b.isRangeInRange(21, 99, fp, inc, desc));  // strictly above
}

TEST_CASE("isRangeInRange overlaps a middle DESC page exactly", "[where][unit][range]") {
    // DESC complete middle page: first=20 (largest), last=10 (smallest) → [10,20].
    SortBounds b{.first_value = 20, .last_value = 10, .is_valid = true};
    const bool fp = false, inc = false, desc = true;

    CHECK(b.isRangeInRange(12, 18, fp, inc, desc));
    CHECK(b.isRangeInRange(0, 10, fp, inc, desc));
    CHECK(b.isRangeInRange(20, 99, fp, inc, desc));
    CHECK_FALSE(b.isRangeInRange(0, 9, fp, inc, desc));
    CHECK_FALSE(b.isRangeInRange(21, 99, fp, inc, desc));
}

TEST_CASE("isRangeInRange open-ended pages extend to infinity", "[where][unit][range]") {
    SortBounds b{.first_value = 10, .last_value = 20, .is_valid = true};

    SECTION("ASC first page covers (-inf, last]") {
        CHECK(b.isRangeInRange(-100, 5, /*fp*/ true, /*inc*/ false, /*desc*/ false));
        CHECK_FALSE(b.isRangeInRange(25, 30, true, false, false));  // above last
    }
    SECTION("ASC incomplete (last) page covers [first, +inf)") {
        CHECK(b.isRangeInRange(50, 99, /*fp*/ false, /*inc*/ true, /*desc*/ false));
        CHECK_FALSE(b.isRangeInRange(0, 5, false, true, false));    // below first
    }
    SECTION("single incomplete page (first && incomplete) always overlaps") {
        CHECK(b.isRangeInRange(1000, 2000, true, true, false));
        CHECK(b.isRangeInRange(1000, 2000, true, true, true));
    }
    SECTION("invalid bounds conservatively overlap") {
        SortBounds none{.is_valid = false};
        CHECK(none.isRangeInRange(1, 2, false, false, false));
    }
}

TEST_CASE("isRangeInRange with the full int64 range always overlaps a valid page",
          "[where][unit][range]") {
    SortBounds b{.first_value = 10, .last_value = 20, .is_valid = true};
    const int64_t lo = std::numeric_limits<int64_t>::min();
    const int64_t hi = std::numeric_limits<int64_t>::max();
    CHECK(b.isRangeInRange(lo, hi, false, false, false));
    CHECK(b.isRangeInRange(lo, hi, false, false, true));
    CHECK(b.isRangeInRange(lo, hi, true, false, false));
}

// ===========================================================================
// §1 — predicateGroupCompatible (pure). Never-miss pruning: prune only when a
// shared filter is provably disjoint.
// ===========================================================================

namespace {

// Two EQ filters (author_id [0], category [1]) — the orthogonal-prune case.
struct DescEq {
    using Entity = ArticleEntity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"author_id", &TestArticle::author_id, "author_id", decl::Op::EQ>{},
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::EQ>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

// Single IN filter on category — the set-disjoint case.
struct DescIn {
    using Entity = ArticleEntity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"category", &TestArticle::category, "category", decl::Op::IN>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &TestArticle::id, "id", decl::SortDirection::Desc>{}
    };
};

// GE filter on view_count, sorted by view_count — the aligned dimension case.
struct DescAligned {
    using Entity = ArticleEntity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"views_min", &TestArticle::view_count, "view_count", decl::Op::GE>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"view_count", &TestArticle::view_count, "view_count", decl::SortDirection::Desc>{}
    };
};

}  // namespace

TEST_CASE("predicateGroupCompatible prunes EQ-incompatible groups, wildcards stay",
          "[where][unit][compat]") {
    decl::Filters<DescEq> group;
    group.get<0>() = int64_t{42};            // group filters author_id = 42

    SECTION("same EQ value → compatible") {
        decl::Filters<DescEq> pred;
        pred.get<0>() = int64_t{42};
        CHECK(decl::predicateGroupCompatible<DescEq>(pred, group));
    }
    SECTION("different EQ value → pruned") {
        decl::Filters<DescEq> pred;
        pred.get<0>() = int64_t{7};
        CHECK_FALSE(decl::predicateGroupCompatible<DescEq>(pred, group));
    }
    SECTION("absent predicate constraint is a wildcard → compatible") {
        decl::Filters<DescEq> pred;     // nothing set
        CHECK(decl::predicateGroupCompatible<DescEq>(pred, group));
    }
    SECTION("absent group constraint is a wildcard → compatible") {
        decl::Filters<DescEq> pred;
        pred.get<0>() = int64_t{7};
        decl::Filters<DescEq> open;     // group filters nothing
        CHECK(decl::predicateGroupCompatible<DescEq>(pred, open));
    }
    SECTION("a single incompatible filter prunes the group") {
        decl::Filters<DescEq> pred;
        pred.get<0>() = int64_t{42};                 // author matches
        pred.get<1>() = std::string("tech");
        decl::Filters<DescEq> g2;
        g2.get<0>() = int64_t{42};
        g2.get<1>() = std::string("news");           // category differs → prune
        CHECK_FALSE(decl::predicateGroupCompatible<DescEq>(pred, g2));
    }
}

TEST_CASE("predicateGroupCompatible prunes IN-disjoint groups only",
          "[where][unit][compat]") {
    SECTION("sets share an element → compatible") {
        decl::Filters<DescIn> pred, group;
        pred.get<0>() = std::vector<std::string>{"a", "b"};
        group.get<0>() = std::vector<std::string>{"b", "c"};
        CHECK(decl::predicateGroupCompatible<DescIn>(pred, group));
    }
    SECTION("disjoint sets → pruned") {
        decl::Filters<DescIn> pred, group;
        pred.get<0>() = std::vector<std::string>{"a"};
        group.get<0>() = std::vector<std::string>{"c"};
        CHECK_FALSE(decl::predicateGroupCompatible<DescIn>(pred, group));
    }
}

// ===========================================================================
// §1 — predicateSortRange (pure). Aligned dimension narrows; orthogonal stays full.
// ===========================================================================

TEST_CASE("predicateSortRange narrows the aligned sort dimension", "[where][unit][range]") {
    decl::Filters<DescAligned> pred;
    pred.get<0>() = int32_t{50};   // view_count >= 50 (GE)

    // Sort dim 0 == view_count == the filtered column → lower-bounded at 50.
    auto r = decl::predicateSortRange<DescAligned>(pred, 0);
    CHECK(r.lo == 50);
    CHECK(r.hi == std::numeric_limits<int64_t>::max());
}

TEST_CASE("predicateSortRange leaves an unconstrained dimension full", "[where][unit][range]") {
    // EQ on author_id, sorts on id — orthogonal: the predicate says nothing about
    // the sorted column, so every page is in range.
    decl::Filters<DescEq> pred;
    pred.get<0>() = int64_t{42};

    auto r = decl::predicateSortRange<DescEq>(pred, 0);  // sort dim 0 == id
    CHECK(r.lo == std::numeric_limits<int64_t>::min());
    CHECK(r.hi == std::numeric_limits<int64_t>::max());
}

// ===========================================================================
// §4 — L1 end-to-end: the fast-path shape and the duality vs blob-array.
// TestArticleListRepo is an L1 list repo; eraseWhere routes its own-list tier
// through the predicate fast-path.
// ===========================================================================

TEST_CASE("eraseWhere drives the list fast-path: one range mod, no entity mods",
          "[where][fastpath][integration][db][list]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    auto A = insertTestUser("wfp_a", "wfp_a@x", 0);
    auto B = insertTestUser("wfp_b", "wfp_b@x", 0);
    insertTestArticle("tech", A, "A1", 10);
    insertTestArticle("news", A, "A2", 20);
    insertTestArticle("tech", A, "A3", 30);
    insertTestArticle("tech", B, "B1", 40);
    insertTestArticle("tech", B, "B2", 50);

    // Warm both groups so there is real list state to invalidate.
    REQUIRE(sync(TestArticleListRepo::query(makeArticleQuery(std::nullopt, A)))->size() == 3);
    REQUIRE(sync(TestArticleListRepo::query(makeArticleQuery(std::nullopt, B)))->size() == 2);

    auto n = sync(TestArticleListRepo::eraseWhere({.author_id = A}));
    REQUIRE(n.has_value());
    REQUIRE(*n == 3);

    // The defining property: exactly ONE RangeModification (O(1)), and ZERO
    // per-entity modifications (the entity tier runs WithLists=false; the list
    // tier is the single predicate mod, not the N-entity blob-array path).
    CHECK(TestInternals::pendingRangeCount<TestArticleListRepo>() == 1);
    CHECK(TestInternals::pendingModificationCount<TestArticleListRepo>() == 0);
}

TEST_CASE("eraseWhere invalidates the purged group and prunes the orthogonal one",
          "[where][fastpath][integration][db][list]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<TestArticleListRepo>();

    auto A = insertTestUser("wfp2_a", "wfp2_a@x", 0);
    auto B = insertTestUser("wfp2_b", "wfp2_b@x", 0);
    insertTestArticle("tech", A, "A1", 10);
    insertTestArticle("tech", A, "A2", 20);
    insertTestArticle("tech", A, "A3", 30);
    insertTestArticle("tech", B, "B1", 40);
    insertTestArticle("tech", B, "B2", 50);

    auto qA = makeArticleQuery(std::nullopt, A);
    auto qB = makeArticleQuery(std::nullopt, B);
    REQUIRE(sync(TestArticleListRepo::query(qA))->size() == 3);  // warm A
    REQUIRE(sync(TestArticleListRepo::query(qB))->size() == 2);  // warm B

    // Direct L3 insert into B's group — the cache does not know about it. A read
    // that hits the (surviving) cache returns the stale 2; a re-fetch returns 3.
    insertTestArticle("tech", B, "B3", 60);

    sync(TestArticleListRepo::eraseWhere({.author_id = A}));

    // Never-miss: A's group is filter-compatible → invalidated → re-fetch → 0.
    CHECK(sync(TestArticleListRepo::query(qA))->size() == 0);

    // Filter-aware pruning: B's group is EQ-incompatible with author_id = A → its
    // cache survives → stale 2, NOT the fresh 3. This is the win over a coarse,
    // group-blind range invalidation (which would evict B too).
    CHECK(sync(TestArticleListRepo::query(qB))->size() == 2);
}
