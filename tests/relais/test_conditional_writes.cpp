/**
 * test_conditional_writes.cpp
 *
 * Relative field updates: increment<F> / decrement<F> (col = col ± $n) and
 * nowPlus<F> (col = now() + offset). The arithmetic and the clock live in the
 * database, so a relative patch is never computed on a cached value, and two
 * identical relative patches are two changes: they are sent as Exclusive writes
 * and never coalesced.
 *
 *   A. generated SQL — SET fragments, parameter numbering, write-mode trait.
 *   B. relative patch on every cache preset — returned view, DB, and cache agree.
 *   C. concurrent identical increments — N patches add exactly N.
 *   D. nowPlus — stamps the database clock, NULL-able timestamp target.
 *   E. composite key.
 *   F. errors — overflow is a DB error (nullopt), the row is untouched.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "jcailloux/relais/io/WhenAll.h"

using namespace relais_test;
using namespace std::chrono_literals;

namespace jr = jcailloux::relais;
using jr::entity::set;
using jr::entity::increment;
using jr::entity::decrement;
using jr::entity::nowPlus;

using SlotF = TestSlotEntity::Field;
using TallyF = TestSlotTallyEntity::Field;

namespace {

int64_t insertSlot(int32_t counter = 0, int64_t version = 0) {
    auto r = execQueryArgs(
        "INSERT INTO relais_test_slots (counter, version) VALUES ($1, $2) RETURNING id",
        counter, version);
    return r[0].get<int64_t>(0);
}

int32_t dbCounter(int64_t id) {
    auto r = execQueryArgs("SELECT counter FROM relais_test_slots WHERE id = $1", id);
    return r[0].get<int32_t>(0);
}

int64_t dbVersion(int64_t id) {
    auto r = execQueryArgs("SELECT version FROM relais_test_slots WHERE id = $1", id);
    return r[0].get<int64_t>(0);
}

/// Seconds between expires_at and the database clock (NULL → nullopt).
std::optional<double> dbExpiresInSeconds(int64_t id) {
    auto r = execQueryArgs(
        "SELECT extract(epoch FROM expires_at - now())::float8 FROM relais_test_slots "
        "WHERE id = $1", id);
    if (r[0].isNull(0)) return std::nullopt;
    return r[0].get<double>(0);
}

}  // namespace

// #############################################################################
//
//  A. Generated SQL (no DB)
//
// #############################################################################

TEST_CASE("[relative] SET fragments and write-mode trait", "[relative][sql]")
{
    namespace d = jr::detail;
    using Op = jr::entity::SetOp;

    SECTION("each op emits its own right-hand side, values numbered in order") {
        auto sql = d::buildUpdateReturning(
            "t", "id",
            {d::SetColumn("\"a\""),
             d::SetColumn("\"b\"", Op::Add),
             d::SetColumn("\"c\"", Op::Subtract),
             d::SetColumn("\"d\"", Op::NowPlus)},
            "id");
        REQUIRE(sql ==
            "UPDATE t SET \"a\"=$1,\"b\"=\"b\"+$2,\"c\"=\"c\"-$3,"
            "\"d\"=now()+interval '1 microsecond'*$4 WHERE \"id\"=$5 RETURNING id");
    }

    SECTION("composite key: PK values follow the SET values") {
        using M = entity::generated::TestSlotTallyMapping;
        auto sql = d::buildUpdateReturning(
            M::table_name, M::primary_key_columns,
            {d::SetColumn("\"hits\"", Op::Add)}, M::SQL::returning_columns);
        REQUIRE(sql.find("SET \"hits\"=\"hits\"+$1 WHERE \"group_id\"=$2 AND \"bucket\"=$3")
                != std::string::npos);
    }

    SECTION("only relative updates are marked non-idempotent") {
        STATIC_REQUIRE_FALSE(jr::entity::is_relative_update_v<
            decltype(set<SlotF::counter>(1))>);
        STATIC_REQUIRE_FALSE(jr::entity::is_relative_update_v<
            decltype(jr::entity::setNull<SlotF::holder>())>);
        STATIC_REQUIRE(jr::entity::is_relative_update_v<
            decltype(increment<SlotF::counter>(1))>);
        STATIC_REQUIRE(jr::entity::is_relative_update_v<
            decltype(decrement<SlotF::counter>(1))>);
        STATIC_REQUIRE(jr::entity::is_relative_update_v<
            decltype(nowPlus<SlotF::expires_at>(5s))&>);
    }

    SECTION("nowPlus keeps microsecond resolution, truncating toward zero") {
        REQUIRE(nowPlus<SlotF::expires_at>(1500ms).microseconds == 1'500'000);
        REQUIRE(nowPlus<SlotF::expires_at>(std::chrono::nanoseconds(2'999)).microseconds == 2);
        REQUIRE(nowPlus<SlotF::expires_at>(-3s).microseconds == -3'000'000);
    }
}

// #############################################################################
//
//  B. Relative patch on every cache preset
//
// #############################################################################

TEMPLATE_TEST_CASE("[relative] increment and decrement through patch",
                   "[relative][integration]",
                   UncachedTestSlotRepo, L1TestSlotRepo, L2TestSlotRepo,
                   FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto id = insertSlot(5, 10);

    SECTION("the returned view carries the database result") {
        auto v = sync(Repo::patch(id, increment<SlotF::counter>(3)));
        REQUIRE(v);
        REQUIRE(v->counter == 8);
        REQUIRE(dbCounter(id) == 8);

        v = sync(Repo::patch(id, decrement<SlotF::counter>(10)));
        REQUIRE(v);
        REQUIRE(v->counter == -2);
        REQUIRE(dbCounter(id) == -2);
    }

    SECTION("applies to the committed row, not to a cached copy") {
        auto warm = sync(Repo::find(id));  // cache counter = 5 (cached presets)
        REQUIRE(warm);
        REQUIRE(warm->counter == 5);
        // Bypass relais: the cached copy is now stale.
        execQueryArgs("UPDATE relais_test_slots SET counter = 100 WHERE id = $1", id);

        auto v = sync(Repo::patch(id, increment<SlotF::counter>(1)));
        REQUIRE(v);
        REQUIRE(v->counter == 101);
        auto after = sync(Repo::find(id));
        REQUIRE(after);
        REQUIRE(after->counter == 101);
    }

    SECTION("mixes with absolute updates in one statement") {
        auto v = sync(Repo::patch(id,
            increment<SlotF::version>(1),
            set<SlotF::priority>(7),
            decrement<SlotF::counter>(2)));
        REQUIRE(v);
        REQUIRE(v->version == 11);
        REQUIRE(v->priority == 7);
        REQUIRE(v->counter == 3);
        REQUIRE(dbVersion(id) == 11);
    }

    SECTION("the same update object applied twice changes the row twice") {
        auto inc = increment<SlotF::counter>(2);
        REQUIRE(sync(Repo::patch(id, inc)));
        REQUIRE(sync(Repo::patch(id, inc)));
        REQUIRE(dbCounter(id) == 9);
    }

    SECTION("absent row: empty view, nothing written") {
        auto v = sync(Repo::patch(id + 1'000'000, increment<SlotF::counter>(1)));
        REQUIRE_FALSE(v);
        REQUIRE(dbCounter(id) == 5);
    }
}

// #############################################################################
//
//  C. Concurrent identical increments
//
//  Identical absolute patches may be coalesced (the follower observes the
//  leader's row). Identical increments must not: each one is a change of its own.
//
// #############################################################################

TEMPLATE_TEST_CASE("[relative] concurrent identical increments all apply",
                   "[relative][integration][concurrency]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto id = insertSlot(0);
    constexpr int N = 32;

    auto returned = sync([](int64_t id) -> io::Task<std::vector<int32_t>> {
        std::vector<int32_t> out(N, -1);
        std::vector<io::Task<void>> tasks;
        tasks.reserve(N);
        for (int i = 0; i < N; ++i) {
            tasks.push_back([](int64_t id, int32_t& slot) -> io::Task<void> {
                auto v = co_await Repo::patch(id, increment<SlotF::counter>(1));
                if (v) slot = v->counter;
            }(id, out[i]));
        }
        co_await io::whenAll(std::move(tasks));
        co_return out;
    }(id));

    REQUIRE(dbCounter(id) == N);
    // Every caller saw its own increment: the returned counters are 1..N.
    std::sort(returned.begin(), returned.end());
    for (int i = 0; i < N; ++i) REQUIRE(returned[i] == i + 1);
}

// #############################################################################
//
//  D. nowPlus — database clock
//
// #############################################################################

TEMPLATE_TEST_CASE("[relative] nowPlus stamps the database clock",
                   "[relative][integration]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto id = insertSlot();
    REQUIRE_FALSE(dbExpiresInSeconds(id));  // NULL before

    SECTION("a future deadline") {
        auto v = sync(Repo::patch(id, nowPlus<SlotF::expires_at>(10min)));
        REQUIRE(v);
        REQUIRE(v->expires_at.has_value());
        auto in = dbExpiresInSeconds(id);
        REQUIRE(in);
        // now() of the patch is at most a few seconds before now() of the check.
        REQUIRE(*in > 590.0);
        REQUIRE(*in <= 600.0);
    }

    SECTION("zero offset: the database time") {
        REQUIRE(sync(Repo::patch(id, nowPlus<SlotF::expires_at>(0s))));
        auto in = dbExpiresInSeconds(id);
        REQUIRE(in);
        REQUIRE(*in <= 0.0);
        REQUIRE(*in > -10.0);
    }

    SECTION("sub-second offsets are kept") {
        REQUIRE(sync(Repo::patch(id, nowPlus<SlotF::expires_at>(1500ms))));
        auto in = dbExpiresInSeconds(id);
        REQUIRE(in);
        // Whole-second truncation would leave at most 1.0 s.
        REQUIRE(*in <= 1.5);
        REQUIRE(*in > 1.1);
    }
}

// #############################################################################
//
//  E. Composite key
//
// #############################################################################

TEMPLATE_TEST_CASE("[relative] increment on a composite key",
                   "[relative][integration][composite]",
                   UncachedTestSlotTallyRepo, FullCacheTestSlotTallyRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    execQuery("INSERT INTO relais_test_slot_tallies (group_id, bucket, hits) VALUES "
              "(1, 1, 10), (1, 2, 20), (2, 1, 30)");

    auto v = sync(Repo::patch(std::tuple<int64_t, int64_t>{1, 2},
                              increment<TallyF::hits>(int64_t{5})));
    REQUIRE(v);
    REQUIRE(v->hits == 25);

    auto r = execQuery(
        "SELECT hits FROM relais_test_slot_tallies ORDER BY group_id, bucket");
    REQUIRE(r[0].get<int64_t>(0) == 10);
    REQUIRE(r[1].get<int64_t>(0) == 25);
    REQUIRE(r[2].get<int64_t>(0) == 30);
}

// #############################################################################
//
//  F. Errors
//
// #############################################################################

TEMPLATE_TEST_CASE("[relative] overflow is a DB error and leaves the row intact",
                   "[relative][integration][error]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto id = insertSlot(2'147'483'000);

    auto v = sync(Repo::patch(id, increment<SlotF::counter>(1'000)));
    REQUIRE_FALSE(v);
    REQUIRE(dbCounter(id) == 2'147'483'000);

    auto after = sync(Repo::find(id));
    REQUIRE(after);
    REQUIRE(after->counter == 2'147'483'000);
}
