/**
 * test_array_mapping.cpp
 *
 * Array-column mapping: PostgreSQL T[] -> std::vector<T>.
 *   - Unit: the text-format array parser (detail::parsePgArray), quoting-aware.
 *   - Integration: a read-only aggregated view (array_agg) read back as an entity
 *     carrying std::vector<int64_t> (int8[]) and std::vector<std::string> (text[]).
 */

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

#include <jcailloux/relais/io/pg/PgResult.h>
#include <jcailloux/relais/io/pg/PgParams.h>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"

using namespace relais_test;

namespace pg = jcailloux::relais::io;

// =============================================================================
// Unit: the array-literal parser, no database.
// =============================================================================

TEST_CASE("[array] parsePgArray on numeric int8[]", "[array][unit]")
{
    using pg::detail::parsePgArray;

    CHECK(parsePgArray<int64_t>("{}").empty());                 // empty array
    CHECK(parsePgArray<int64_t>("").empty());                   // NULL column → empty
    CHECK(parsePgArray<int64_t>("{42}") == std::vector<int64_t>{42});
    CHECK(parsePgArray<int64_t>("{1,2,3}") == std::vector<int64_t>{1, 2, 3});
    CHECK(parsePgArray<int64_t>("{-5,0,9223372036854775807}")
          == std::vector<int64_t>{-5, 0, 9223372036854775807LL});

    CHECK(parsePgArray<int32_t>("{10,20}") == std::vector<int32_t>{10, 20});
    CHECK(parsePgArray<double>("{1.5,2.25}") == std::vector<double>{1.5, 2.25});
}

TEST_CASE("[array] parsePgArray on text[] with quoting", "[array][unit]")
{
    using pg::detail::parsePgArray;

    // Unquoted simple elements.
    CHECK(parsePgArray<std::string>("{a,b,c}")
          == std::vector<std::string>{"a", "b", "c"});

    // Quoted elements: comma INSIDE an element must not split it.
    CHECK(parsePgArray<std::string>(R"({"a,b","c"})")
          == std::vector<std::string>{"a,b", "c"});

    // Backslash escaping of a literal double-quote and backslash.
    CHECK(parsePgArray<std::string>(R"({"with\"quote","back\\slash"})")
          == std::vector<std::string>{"with\"quote", "back\\slash"});

    // Braces inside a quoted element stay literal.
    CHECK(parsePgArray<std::string>(R"({"{not a brace}"})")
          == std::vector<std::string>{"{not a brace}"});
}

TEST_CASE("[array] parsePgArray rejects NULL elements", "[array][unit]")
{
    using pg::detail::parsePgArray;
    CHECK_THROWS_AS(parsePgArray<int64_t>("{1,NULL,3}"), pg::PgError);
}

TEST_CASE("[array] toParam serializes a vector into a PG array literal", "[array][unit]")
{
    auto lit = [](auto&& v) {
        auto p = pg::PgParams::make(std::forward<decltype(v)>(v));
        return std::string(p.params[0].data(), static_cast<size_t>(p.params[0].length()));
    };

    // Numeric: never quoted.
    CHECK(lit(std::vector<int64_t>{}) == "{}");
    CHECK(lit(std::vector<int64_t>{1, 2, 3}) == "{1,2,3}");
    CHECK(lit(std::vector<int32_t>{-5, 0, 7}) == "{-5,0,7}");

    // Text: quote/escape only elements that need it (delimiter, space, empty).
    CHECK(lit(std::vector<std::string>{"x", "plain"}) == "{x,plain}");
    CHECK(lit(std::vector<std::string>{"a,b", "c"}) == R"({"a,b",c})");
    CHECK(lit(std::vector<std::string>{"with\"q", "back\\s"}) == R"({"with\"q","back\\s"})");
    CHECK(lit(std::vector<std::string>{""}) == R"({""})");

    // Round-trip with the parser: serialize then re-parse yields the original.
    const std::vector<std::string> orig{"a,b", "x", "{brace}", ""};
    CHECK(pg::detail::parsePgArray<std::string>(lit(orig)) == orig);
}

// =============================================================================
// Integration: aggregated view (array_agg) → entity with std::vector fields.
// =============================================================================

namespace {

void seedSrc(int64_t owner, int64_t tag, const std::string& label) {
    execQueryArgs(
        "INSERT INTO relais_test_array_src (owner_id, tag_id, label) VALUES ($1, $2, $3)",
        owner, tag, label);
}

}  // namespace

TEST_CASE("[array] read-only view aggregates rows into vector fields",
          "[integration][db][array]")
{
    TransactionGuard tx;

    // Owner 1: three rows; one label carries a comma to exercise text[] quoting.
    seedSrc(1, 30, "plain");
    seedSrc(1, 10, "x");
    seedSrc(1, 20, "a,b");
    // Owner 2: a single row.
    seedSrc(2, 99, "solo");

    SECTION("uncached find parses int8[] and text[] in array_agg order") {
        auto v = sync(UncachedTestArrayViewRepo::find(int64_t{1}));
        REQUIRE(v != nullptr);
        CHECK(v->owner_id == 1);
        // ORDER BY tag_id inside array_agg → deterministic {10,20,30}.
        CHECK(v->tag_ids == std::vector<int64_t>{10, 20, 30});
        CHECK(v->labels == std::vector<std::string>{"x", "a,b", "plain"});
    }

    SECTION("single-row owner yields single-element vectors") {
        auto v = sync(UncachedTestArrayViewRepo::find(int64_t{2}));
        REQUIRE(v != nullptr);
        CHECK(v->tag_ids == std::vector<int64_t>{99});
        CHECK(v->labels == std::vector<std::string>{"solo"});
    }

    SECTION("absent owner → find miss (fail-closed, no {NULL})") {
        auto v = sync(UncachedTestArrayViewRepo::find(int64_t{999}));
        CHECK(v == nullptr);
    }

    SECTION("L1-cached find returns identical vectors on hit") {
        auto a = sync(L1TestArrayViewRepo::find(int64_t{1}));
        REQUIRE(a != nullptr);
        auto b = sync(L1TestArrayViewRepo::find(int64_t{1}));  // cache hit
        REQUIRE(b != nullptr);
        CHECK(b->tag_ids == std::vector<int64_t>{10, 20, 30});
        CHECK(b->labels == std::vector<std::string>{"x", "a,b", "plain"});
    }
}

TEST_CASE("[array] writable array columns round-trip insert/find/update",
          "[integration][db][array]")
{
    TransactionGuard tx;

    TestArrayRwEntity e;
    e.owner_id = 1;
    e.tag_ids = {10, 20, 30};
    e.labels = {"x", "a,b", "plain"};  // "a,b" forces text[] quoting on write

    SECTION("insert then find round-trips both arrays through the write path") {
        auto inserted = sync(UncachedTestArrayRwRepo::insert(e));
        REQUIRE(inserted != nullptr);
        CHECK(inserted->tag_ids == std::vector<int64_t>{10, 20, 30});
        CHECK(inserted->labels == std::vector<std::string>{"x", "a,b", "plain"});

        auto found = sync(UncachedTestArrayRwRepo::find(int64_t{1}));
        REQUIRE(found != nullptr);
        CHECK(found->tag_ids == std::vector<int64_t>{10, 20, 30});
        CHECK(found->labels == std::vector<std::string>{"x", "a,b", "plain"});
    }

    SECTION("update rewrites the arrays") {
        sync(UncachedTestArrayRwRepo::insert(e));

        TestArrayRwEntity upd;
        upd.owner_id = 1;
        upd.tag_ids = {99};
        upd.labels = {"solo"};
        REQUIRE(sync(UncachedTestArrayRwRepo::update(int64_t{1}, upd)));

        auto found = sync(UncachedTestArrayRwRepo::find(int64_t{1}));
        REQUIRE(found != nullptr);
        CHECK(found->tag_ids == std::vector<int64_t>{99});
        CHECK(found->labels == std::vector<std::string>{"solo"});
    }

    SECTION("empty array round-trips as {}") {
        TestArrayRwEntity empty;
        empty.owner_id = 2;
        empty.tag_ids = {};
        empty.labels = {};
        sync(UncachedTestArrayRwRepo::insert(empty));

        auto found = sync(UncachedTestArrayRwRepo::find(int64_t{2}));
        REQUIRE(found != nullptr);
        CHECK(found->tag_ids.empty());
        CHECK(found->labels.empty());
    }
}
