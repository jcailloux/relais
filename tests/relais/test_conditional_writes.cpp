/**
 * test_conditional_writes.cpp
 *
 * Relative field updates: increment<F> / decrement<F> (col = col ± $n) and
 * nowPlus<F> (col = now() + offset). The arithmetic and the clock live in the
 * database, so a relative patch is never computed on a cached value, and two
 * identical relative patches are two changes: they are sent as Exclusive writes
 * and never coalesced.
 *
 * Typed guards and orderings: when(...) predicates in bounded disjunctive
 * normal form, dbNow, in/notIn, and orderBy(asc, desc, first) closed by the
 * primary key.
 *
 *   A. generated SQL — SET fragments, parameter numbering, write-mode trait.
 *   B. relative patch on every cache preset — returned view, DB, and cache agree.
 *   C. concurrent identical increments — N patches add exactly N.
 *   D. nowPlus — stamps the database clock, NULL-able timestamp target.
 *   E. composite key.
 *   F. errors — overflow is a DB error (nullopt), the row is untouched.
 *   G. guard and ordering SQL — each form, structure rules, binding order.
 *   H. guards and orderings evaluated by PostgreSQL — NULL, empty sets, enums.
 *   I. row column offset — one result row carrying the old and the new version,
 *      each decoded by the generated fromRow.
 *
 * Guarded patch: patchIf(id, when(...), updates...) writes one row only if the
 * guard holds in the database at write time. nullopt is a DB error; an empty
 * view is a refusal (guard false or row absent) and changes nothing downstream.
 *
 *   J. patchIf — SQL shape, every cache preset, identical guarded writes never
 *      coalesced, list pages and cross-invalidation touched on commit only.
 *
 * Predicate update: patchWhere<{.returns}>(when(...), updates...) changes every
 * row matching the predicate in one statement and returns the count, the
 * committed rows, or each row before and after. The changed rows are known
 * only after the write, so every tier is invalidated from them.
 *
 *   K. patchWhere — SQL shape, the three return forms on every cache preset,
 *      the predicate re-checked under contention, composite key, list pages of
 *      the old and the new group, cross-invalidation of both targets.
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
#include "fixtures/RelaisTestAccessors.h"
#include "jcailloux/relais/io/WhenAll.h"

using namespace relais_test;
using namespace std::chrono_literals;

namespace jr = jcailloux::relais;
namespace rspec = jcailloux::relais::list::spec;
using jr::entity::set;
using jr::entity::increment;
using jr::entity::decrement;
using jr::entity::nowPlus;
using jr::entity::when;
using jr::entity::anyOf;
using jr::entity::allOf;
using jr::entity::eq;
using jr::entity::ne;
using jr::entity::gt;
using jr::entity::ge;
using jr::entity::lt;
using jr::entity::le;
using jr::entity::in;
using jr::entity::notIn;
using jr::entity::isNull;
using jr::entity::isNotNull;
using jr::entity::dbNow;
using jr::entity::orderBy;
using jr::entity::asc;
using jr::entity::desc;
using jr::entity::first;
using jr::entity::Nulls;

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

// #############################################################################
//
//  G. Guard and ordering SQL (no DB)
//
// #############################################################################

namespace {

using SlotTraits = TestSlotEntity::TraitsType;
using SlotMapping = entity::generated::TestSlotMapping;

/// SQL of one guard (or a when(...) root), numbered from `param`.
template<typename G>
std::string guardSql(const G&, std::string_view qual = {}, size_t param = 1) {
    std::string sql;
    jr::detail::GuardSql<SlotTraits, G>::emit(sql, param, qual);
    return sql;
}

template<typename G>
std::vector<std::string> boundValues(const G& g) {
    io::PgParams p;
    jr::detail::GuardSql<SlotTraits, G>::bind(p, g);
    std::vector<std::string> out;
    for (const auto& v : p.params)
        out.emplace_back(v.isNull() ? "<null>" : std::string(v.data(), v.length()));
    return out;
}

template<typename O>
std::string orderSql(std::string_view qual = {}, size_t param = 1) {
    std::string sql;
    jr::detail::OrderBySql<SlotTraits, O>::emit(sql, param, qual, SlotMapping::primary_key_column);
    return sql;
}

}  // namespace

TEST_CASE("[guard] leaf forms", "[guard][sql]")
{
    SECTION("comparisons bind one value each") {
        REQUIRE(guardSql(eq<SlotF::counter>(3)) == "\"counter\"=$1");
        REQUIRE(guardSql(ne<SlotF::counter>(3)) == "\"counter\"!=$1");
        REQUIRE(guardSql(gt<SlotF::counter>(3)) == "\"counter\">$1");
        REQUIRE(guardSql(ge<SlotF::counter>(3)) == "\"counter\">=$1");
        REQUIRE(guardSql(lt<SlotF::counter>(3)) == "\"counter\"<$1");
        REQUIRE(guardSql(le<SlotF::counter>(3), {}, 7) == "\"counter\"<=$7");
        REQUIRE(boundValues(eq<SlotF::counter>(3)) == std::vector<std::string>{"3"});
    }

    SECTION("dbNow compares with the database clock and binds nothing") {
        auto g = le<SlotF::expires_at>(dbNow);
        STATIC_REQUIRE(std::is_same_v<decltype(g), jr::entity::FieldGuardNow<SlotF::expires_at, jr::entity::Cmp::Le>>);
        REQUIRE(guardSql(g) == "\"expires_at\"<=now()");
        REQUIRE(boundValues(g).empty());
        STATIC_REQUIRE(jr::detail::GuardSql<SlotTraits, decltype(g)>::params == 0);
    }

    SECTION("in / notIn bind the whole set as one array") {
        REQUIRE(guardSql(in<SlotF::priority>({1, 2, 3})) == "\"priority\"=ANY($1)");
        REQUIRE(guardSql(notIn<SlotF::priority>({1, 2, 3})) == "\"priority\"!=ALL($1)");
        REQUIRE(boundValues(in<SlotF::priority>({1, 2, 3})) == std::vector<std::string>{"{1,2,3}"});
        REQUIRE(boundValues(in<SlotF::priority>(std::vector<int32_t>{})) == std::vector<std::string>{"{}"});
    }

    SECTION("NULL tests bind nothing") {
        REQUIRE(guardSql(isNull<SlotF::holder>()) == "\"holder\" IS NULL");
        REQUIRE(guardSql(isNotNull<SlotF::holder>()) == "\"holder\" IS NOT NULL");
        REQUIRE(boundValues(isNull<SlotF::holder>()).empty());
    }

    SECTION("a mapped enum binds its database string") {
        REQUIRE(boundValues(eq<SlotF::state>(SlotState::Held)) == std::vector<std::string>{"held"});
        REQUIRE(boundValues(in<SlotF::state>({SlotState::Free, SlotState::Taken}))
                == std::vector<std::string>{"{free,taken}"});
    }

    SECTION("the qualifier prefixes the column") {
        REQUIRE(guardSql(eq<SlotF::counter>(3), "t.") == "t.\"counter\"=$1");
        REQUIRE(guardSql(isNull<SlotF::holder>(), "t.") == "t.\"holder\" IS NULL");
    }
}

TEST_CASE("[guard] bounded disjunctive normal form", "[guard][sql]")
{
    using namespace jr::entity;

    SECTION("when joins its children with AND, anyOf / allOf parenthesize") {
        auto pred = when(
            eq<SlotF::group_id>(int64_t{1}),
            anyOf(isNull<SlotF::holder>(),
                  allOf(eq<SlotF::state>(SlotState::Held), lt<SlotF::expires_at>(dbNow))),
            in<SlotF::state>({SlotState::Free, SlotState::Held}));
        REQUIRE(guardSql(pred, "t.") ==
            "t.\"group_id\"=$1 AND (t.\"holder\" IS NULL OR "
            "(t.\"state\"=$2 AND t.\"expires_at\"<now())) AND t.\"state\"=ANY($3)");
        STATIC_REQUIRE(jr::detail::GuardSql<SlotTraits, decltype(pred)>::params == 3);
        REQUIRE(boundValues(pred) == std::vector<std::string>{"1", "held", "{free,held}"});
    }

    SECTION("each node accepts only its allowed children") {
        using Leaf = decltype(eq<SlotF::counter>(1));
        using All = decltype(allOf(eq<SlotF::counter>(1)));
        using Any = decltype(anyOf(eq<SlotF::counter>(1)));

        STATIC_REQUIRE(valid_when_child_v<Leaf>);
        STATIC_REQUIRE(valid_when_child_v<Any>);
        STATIC_REQUIRE_FALSE(valid_when_child_v<All>);

        STATIC_REQUIRE(valid_any_of_child_v<Leaf>);
        STATIC_REQUIRE(valid_any_of_child_v<All>);
        STATIC_REQUIRE_FALSE(valid_any_of_child_v<Any>);

        STATIC_REQUIRE(valid_all_of_child_v<Leaf>);
        STATIC_REQUIRE_FALSE(valid_all_of_child_v<All>);
        STATIC_REQUIRE_FALSE(valid_all_of_child_v<Any>);

        STATIC_REQUIRE_FALSE(valid_when_child_v<int>);
    }
}

TEST_CASE("[guard] ordering", "[guard][sql][order]")
{
    SECTION("no ordering: the primary key alone") {
        REQUIRE(orderSql<void>("t.") == "ORDER BY t.\"id\"");
    }

    SECTION("keys in order, the primary key closes the ordering") {
        using O = decltype(orderBy(desc<SlotF::priority>(), asc<SlotF::counter>()));
        REQUIRE(orderSql<O>() == "ORDER BY \"priority\" DESC,\"counter\" ASC,\"id\"");
    }

    SECTION("explicit NULL placement") {
        using O = decltype(orderBy(asc<SlotF::expires_at, Nulls::First>(),
                                   desc<SlotF::holder, Nulls::Last>()));
        REQUIRE(orderSql<O>() ==
            "ORDER BY \"expires_at\" ASC NULLS FIRST,\"holder\" DESC NULLS LAST,\"id\"");
    }

    SECTION("first() emits a CASE, so a NULL guard sorts with the unsatisfied rows") {
        auto o = orderBy(first(anyOf(isNull<SlotF::holder>(), eq<SlotF::priority>(9))));
        REQUIRE(orderSql<decltype(o)>("", 4) ==
            "ORDER BY CASE WHEN (\"holder\" IS NULL OR \"priority\"=$4) THEN 0 ELSE 1 END,\"id\"");
        io::PgParams p;
        jr::detail::OrderBySql<SlotTraits, decltype(o)>::bind(p, o);
        REQUIRE(p.params.size() == 1);
    }

    SECTION("composite primary key: every key column, in order") {
        using M = entity::generated::TestSlotTallyMapping;
        std::string sql;
        size_t param = 1;
        jr::detail::OrderBySql<TestSlotTallyEntity::TraitsType, void>::emit(
            sql, param, "c.", M::primary_key_columns);
        REQUIRE(sql == "ORDER BY c.\"group_id\",c.\"bucket\"");
    }
}

TEST_CASE("[guard] parameter numbering across SET, predicate and ordering", "[guard][sql]")
{
    namespace d = jr::detail;
    using Op = jr::entity::SetOp;

    auto pred = when(eq<SlotF::group_id>(int64_t{1}), lt<SlotF::expires_at>(dbNow),
                     in<SlotF::state>({SlotState::Free}));
    auto order = orderBy(first(eq<SlotF::holder>(int64_t{5})),
                         asc<SlotF::priority, Nulls::First>());

    std::string sql;
    size_t param = d::appendSetClause(sql,
        {d::SetColumn("\"counter\"", Op::Add), d::SetColumn("\"holder\"")}, "t.");
    sql += " WHERE ";
    d::GuardSql<SlotTraits, decltype(pred)>::emit(sql, param, "t.");
    sql += ' ';
    d::OrderBySql<SlotTraits, decltype(order)>::emit(
        sql, param, "t.", SlotMapping::primary_key_column);

    // The right-hand side of a SET is qualified, the assigned column never is.
    REQUIRE(sql ==
        "\"counter\"=t.\"counter\"+$1,\"holder\"=$2"
        " WHERE t.\"group_id\"=$3 AND t.\"expires_at\"<now() AND t.\"state\"=ANY($4)"
        " ORDER BY CASE WHEN t.\"holder\"=$5 THEN 0 ELSE 1 END,"
        "t.\"priority\" ASC NULLS FIRST,t.\"id\"");
    REQUIRE(param == 6);
}

// #############################################################################
//
//  H. Guards and orderings evaluated by PostgreSQL
//
// #############################################################################

namespace {

/// Four slots covering NULL / past / future deadlines and each state.
///   a: free,  holder NULL, expires NULL,    priority 1
///   b: held,  holder 7,    expires past,    priority 5
///   c: held,  holder 8,    expires future,  priority 5
///   d: taken, holder 9,    expires NULL,    priority 3
struct SlotSet { int64_t a, b, c, d; };

SlotSet insertSlotSet() {
    auto one = [](const char* values) {
        std::string sql = "INSERT INTO relais_test_slots "
            "(state, holder, expires_at, priority) VALUES ";
        sql += values;
        sql += " RETURNING id";
        return execQuery(sql.c_str())[0].get<int64_t>(0);
    };
    SlotSet s;
    s.a = one("('free', NULL, NULL, 1)");
    s.b = one("('held', 7, now() - interval '1 minute', 5)");
    s.c = one("('held', 8, now() + interval '1 hour', 5)");
    s.d = one("('taken', 9, NULL, 3)");
    return s;
}

/// Ids selected by `pred`, in the given ordering (void = primary key only).
template<typename O = void, typename P>
std::vector<int64_t> selectIds(const P& pred, const O* order = nullptr) {
    namespace d = jr::detail;
    std::string sql = "SELECT id FROM relais_test_slots WHERE ";
    size_t param = 1;
    d::GuardSql<SlotTraits, P>::emit(sql, param, "");
    sql += ' ';
    d::OrderBySql<SlotTraits, O>::emit(sql, param, "", SlotMapping::primary_key_column);

    io::PgParams params;
    d::GuardSql<SlotTraits, P>::bind(params, pred);
    if constexpr (!std::is_void_v<O>) d::OrderBySql<SlotTraits, O>::bind(params, *order);

    auto r = sync(relais_test::detail::testPg()->queryParams(sql.c_str(), params));
    std::vector<int64_t> ids;
    for (int i = 0; i < r.rows(); ++i) ids.push_back(r[i].get<int64_t>(0));
    return ids;
}

using Ids = std::vector<int64_t>;

}  // namespace

TEST_CASE("[guard] predicates evaluated by the database", "[guard][integration]")
{
    TransactionGuard guard;
    auto s = insertSlotSet();

    SECTION("enum equality and set membership") {
        REQUIRE(selectIds(when(eq<SlotF::state>(SlotState::Held))) == Ids{s.b, s.c});
        REQUIRE(selectIds(when(in<SlotF::state>({SlotState::Free, SlotState::Taken})))
                == Ids{s.a, s.d});
        REQUIRE(selectIds(when(notIn<SlotF::state>({SlotState::Held}))) == Ids{s.a, s.d});
    }

    SECTION("empty sets: in matches nothing, notIn matches everything") {
        REQUIRE(selectIds(when(in<SlotF::state>(std::vector<SlotState>{}))).empty());
        REQUIRE(selectIds(when(notIn<SlotF::state>(std::vector<SlotState>{})))
                == Ids{s.a, s.b, s.c, s.d});
    }

    SECTION("NULL tests and SQL comparison semantics on a nullable column") {
        REQUIRE(selectIds(when(isNull<SlotF::holder>())) == Ids{s.a});
        REQUIRE(selectIds(when(isNotNull<SlotF::expires_at>())) == Ids{s.b, s.c});
        // NULL != 7 is not true: the free slot is not selected.
        REQUIRE(selectIds(when(ne<SlotF::holder>(int64_t{7}))) == Ids{s.c, s.d});
    }

    SECTION("dbNow: deadlines against the database clock") {
        REQUIRE(selectIds(when(lt<SlotF::expires_at>(dbNow))) == Ids{s.b});
        REQUIRE(selectIds(when(ge<SlotF::expires_at>(dbNow))) == Ids{s.c});
    }

    SECTION("free or expired: anyOf over allOf") {
        auto takeable = when(anyOf(
            eq<SlotF::state>(SlotState::Free),
            allOf(eq<SlotF::state>(SlotState::Held), lt<SlotF::expires_at>(dbNow))));
        REQUIRE(selectIds(takeable) == Ids{s.a, s.b});
    }
}

TEST_CASE("[guard] orderings evaluated by the database", "[guard][integration][order]")
{
    TransactionGuard guard;
    auto s = insertSlotSet();
    auto all = when(ge<SlotF::priority>(0));

    SECTION("ties are broken by the primary key") {
        auto o = orderBy(desc<SlotF::priority>());
        REQUIRE(selectIds(all, &o) == Ids{s.b, s.c, s.d, s.a});
    }

    SECTION("NULL placement: PostgreSQL default, then overridden") {
        auto dflt = orderBy(desc<SlotF::expires_at>());
        REQUIRE(selectIds(all, &dflt) == Ids{s.a, s.d, s.c, s.b});
        auto last = orderBy(desc<SlotF::expires_at, Nulls::Last>());
        REQUIRE(selectIds(all, &last) == Ids{s.c, s.b, s.a, s.d});
    }

    SECTION("first() puts matching rows ahead, NULL counts as not matching") {
        // holder > 7 is NULL for the free slot: it must not sort first.
        auto o = orderBy(first(gt<SlotF::holder>(int64_t{7})));
        REQUIRE(selectIds(all, &o) == Ids{s.c, s.d, s.a, s.b});
    }

    SECTION("first() then a key: expired first, then by priority") {
        auto o = orderBy(first(lt<SlotF::expires_at>(dbNow)), desc<SlotF::priority>());
        REQUIRE(selectIds(all, &o) == Ids{s.b, s.c, s.d, s.a});
    }
}

TEST_CASE("[guard] set<F>(enum) binds the database string", "[guard][integration]")
{
    TransactionGuard guard;
    auto id = insertSlot();
    auto v = sync(UncachedTestSlotRepo::patch(id, set<SlotF::state>(SlotState::Taken)));
    REQUIRE(v);
    REQUIRE(v->state == SlotState::Taken);
    auto r = execQueryArgs("SELECT state FROM relais_test_slots WHERE id = $1", id);
    REQUIRE(r[0].get<std::string>(0) == "taken");
}

// #############################################################################
//
//  I. Row column offset: before and after in one row
//
// #############################################################################

TEST_CASE("[row-offset] decode RETURNING o.*, t.* at both offsets", "[row-offset][integration]")
{
    TransactionGuard guard;
    auto id = insertSlot(4, 10);

    // Locked old version, then the update: one row holds both, old first.
    auto r = execQueryArgs(
        "WITH o AS (SELECT * FROM relais_test_slots WHERE id = $1 FOR UPDATE) "
        "UPDATE relais_test_slots AS t "
        "SET state = 'held', holder = 42, expires_at = now(), counter = t.counter + 1 "
        "FROM o WHERE t.id = o.id RETURNING o.*, t.*", id);
    REQUIRE(r.rows() == 1);
    const int width = r.cols() / 2;
    REQUIRE(width == 8);

    auto before = SlotMapping::fromRow<TestSlotEntity>(r[0]);
    auto after = SlotMapping::fromRow<TestSlotEntity>(r[0].shifted(width));
    REQUIRE(before);
    REQUIRE(after);

    CHECK(before->id == id);
    CHECK(before->state == SlotState::Free);
    CHECK_FALSE(before->holder);
    CHECK_FALSE(before->expires_at);
    CHECK(before->counter == 4);
    CHECK(before->version == 10);

    CHECK(after->id == id);
    CHECK(after->state == SlotState::Held);
    CHECK(after->holder == std::optional<int64_t>{42});
    CHECK(after->expires_at);
    CHECK(after->counter == 5);
    CHECK(after->version == 10);

    SECTION("offsets accumulate, and NULL tests follow the offset") {
        auto row = r[0].shifted(3);
        CHECK(row.isNull(0));                                  // o.holder
        CHECK(row.shifted(width).get<int64_t>(0) == 42);       // t.holder
        CHECK(row.shifted(width - 3).get<int64_t>(0) == id);   // t.id
        CHECK(r[0].rawValue(0) == row.shifted(-3).rawValue(0));
    }
}

// #############################################################################
//
//  J. Guarded patch (patchIf)
//
// #############################################################################

TEST_CASE("[patchIf] SQL: SET values, then the key, then the guard", "[patchIf][sql]")
{
    namespace d = jr::detail;

    SECTION("single key") {
        using G = decltype(when(eq<SlotF::version>(int64_t{0}),
            anyOf(isNull<SlotF::holder>(), lt<SlotF::expires_at>(dbNow))));
        auto sql = d::buildGuardedUpdateReturning<SlotTraits, G>(
            SlotMapping::table_name, SlotMapping::primary_key_column,
            {d::SetColumn(std::string_view("\"holder\"")),
             d::SetColumn(std::string_view("\"version\""), jr::entity::SetOp::Add)},
            "id");
        REQUIRE(sql ==
            "UPDATE relais_test_slots SET \"holder\"=$1,\"version\"=\"version\"+$2 "
            "WHERE \"id\"=$3 AND \"version\"=$4 AND (\"holder\" IS NULL OR \"expires_at\"<now()) "
            "RETURNING id");
    }

    SECTION("composite key") {
        using TallyTraits = TestSlotTallyEntity::TraitsType;
        using TallyMapping = entity::generated::TestSlotTallyMapping;
        using G = decltype(when(lt<TallyF::hits>(int64_t{0})));
        auto sql = d::buildGuardedUpdateReturning<TallyTraits, G>(
            TallyMapping::table_name, TallyMapping::primary_key_columns,
            {d::SetColumn(std::string_view("\"hits\""), jr::entity::SetOp::Add)},
            "hits");
        REQUIRE(sql ==
            "UPDATE relais_test_slot_tallies SET \"hits\"=\"hits\"+$1 "
            "WHERE \"group_id\"=$2 AND \"bucket\"=$3 AND \"hits\"<$4 RETURNING hits");
    }
}

TEMPLATE_TEST_CASE("[patchIf] success, refusal, absence and error",
                   "[patchIf][integration]",
                   UncachedTestSlotRepo, L1TestSlotRepo, L2TestSlotRepo,
                   FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto id = insertSlot(5, 10);

    SECTION("guard holds: the committed row is returned, cached and in the DB") {
        auto v = sync(Repo::patchIf(id, when(eq<SlotF::version>(int64_t{10})),
            set<SlotF::holder>(int64_t{42}), increment<SlotF::version>(1)));
        REQUIRE(v);
        REQUIRE(*v);
        CHECK((*v)->holder == std::optional<int64_t>{42});
        CHECK((*v)->version == 11);
        CHECK(dbVersion(id) == 11);

        auto after = sync(Repo::find(id));
        REQUIRE(after);
        CHECK(after->version == 11);
        CHECK(after->holder == std::optional<int64_t>{42});
    }

    SECTION("guard false: an empty view, nothing written") {
        REQUIRE(sync(Repo::find(id)));  // warm
        auto v = sync(Repo::patchIf(id, when(eq<SlotF::version>(int64_t{9})),
            set<SlotF::holder>(int64_t{42}), increment<SlotF::version>(1)));
        REQUIRE(v);
        CHECK_FALSE(*v);
        CHECK(dbVersion(id) == 10);

        auto after = sync(Repo::find(id));
        REQUIRE(after);
        CHECK(after->version == 10);
        CHECK_FALSE(after->holder);
    }

    SECTION("the guard reads the committed row, not a cached copy") {
        REQUIRE(sync(Repo::find(id))->version == 10);  // cache version = 10
        // Bypass relais: the cached copy is now stale.
        execQueryArgs("UPDATE relais_test_slots SET version = 11 WHERE id = $1", id);

        auto v = sync(Repo::patchIf(id, when(eq<SlotF::version>(int64_t{10})),
            increment<SlotF::version>(1)));
        REQUIRE(v);
        CHECK_FALSE(*v);
        CHECK(dbVersion(id) == 11);
    }

    SECTION("absent row: an empty view, like a refusal") {
        auto v = sync(Repo::patchIf(id + 1'000'000, when(eq<SlotF::version>(int64_t{10})),
            increment<SlotF::counter>(1)));
        REQUIRE(v);
        CHECK_FALSE(*v);
        CHECK(dbCounter(id) == 5);
    }

    SECTION("DB error: nullopt, distinct from a refusal") {
        execQueryArgs("UPDATE relais_test_slots SET counter = 2147483000 WHERE id = $1", id);
        auto v = sync(Repo::patchIf(id, when(eq<SlotF::version>(int64_t{10})),
            increment<SlotF::counter>(1'000)));
        CHECK_FALSE(v);
        CHECK(dbCounter(id) == 2'147'483'000);
    }
}

TEMPLATE_TEST_CASE("[patchIf] state transitions and expiring holds",
                   "[patchIf][integration]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto s = insertSlotSet();
    auto taken = [](int64_t id) -> std::optional<bool> {
        auto v = sync(Repo::patchIf(id,
            when(in<SlotF::state>({SlotState::Free, SlotState::Held})),
            set<SlotF::state>(SlotState::Taken)));
        if (!v) return std::nullopt;
        return static_cast<bool>(*v);
    };
    auto held = [](int64_t id) -> std::optional<bool> {
        auto v = sync(Repo::patchIf(id,
            when(anyOf(isNull<SlotF::holder>(), lt<SlotF::expires_at>(dbNow))),
            set<SlotF::holder>(int64_t{100}), nowPlus<SlotF::expires_at>(1min)));
        if (!v) return std::nullopt;
        return static_cast<bool>(*v);
    };

    SECTION("in: only a free or held slot can be taken") {
        CHECK(taken(s.a) == std::optional{true});
        CHECK(taken(s.b) == std::optional{true});
        CHECK(taken(s.d) == std::optional{false});   // already taken
        CHECK(taken(s.a) == std::optional{false});   // taken on the first call
        CHECK(sync(Repo::find(s.a))->state == SlotState::Taken);
    }

    SECTION("anyOf + dbNow: a hold goes to a free slot or an expired one") {
        CHECK(held(s.a) == std::optional{true});    // no holder
        CHECK(held(s.b) == std::optional{true});    // expired a minute ago
        CHECK(held(s.c) == std::optional{false});   // held for another hour
        CHECK(held(s.a) == std::optional{false});   // now held for a minute
        auto c = sync(Repo::find(s.c));
        REQUIRE(c);
        CHECK(c->holder == std::optional<int64_t>{8});
    }
}

TEMPLATE_TEST_CASE("[patchIf] identical compare-and-set writes: exactly one wins",
                   "[patchIf][integration][concurrency]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto id = insertSlot(0, 0);
    constexpr int N = 32;

    // Same SQL, same params: coalescing them would report N wins.
    auto outcomes = sync([](int64_t id) -> io::Task<std::vector<int>> {
        std::vector<int> out(N, -1);  // -1 error, 0 refused, 1 won
        std::vector<io::Task<void>> tasks;
        tasks.reserve(N);
        for (int i = 0; i < N; ++i) {
            tasks.push_back([](int64_t id, int& slot) -> io::Task<void> {
                auto v = co_await Repo::patchIf(id, when(eq<SlotF::version>(int64_t{0})),
                    increment<SlotF::version>(1));
                slot = !v ? -1 : (*v ? 1 : 0);
            }(id, out[i]));
        }
        co_await io::whenAll(std::move(tasks));
        co_return out;
    }(id));

    CHECK(std::count(outcomes.begin(), outcomes.end(), 1) == 1);
    CHECK(std::count(outcomes.begin(), outcomes.end(), 0) == N - 1);
    CHECK(dbVersion(id) == 1);
}

TEMPLATE_TEST_CASE("[patchIf] composite key", "[patchIf][integration]",
                   UncachedTestSlotTallyRepo, FullCacheTestSlotTallyRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    execQuery("INSERT INTO relais_test_slot_tallies (group_id, bucket, hits) VALUES "
              "(1, 1, 2), (1, 2, 2)");
    auto key = std::tuple{int64_t{1}, int64_t{2}};

    // A bounded counter: the increment applies while below the cap.
    auto bump = [&] {
        auto v = sync(Repo::patchIf(key, when(lt<TallyF::hits>(int64_t{3})),
            increment<TallyF::hits>(1)));
        REQUIRE(v);
        return static_cast<bool>(*v);
    };
    CHECK(bump());
    CHECK_FALSE(bump());
    auto r = execQuery("SELECT bucket, hits FROM relais_test_slot_tallies "
                       "WHERE group_id = 1 ORDER BY bucket");
    CHECK(r[0].get<int64_t>(1) == 2);  // the other bucket is untouched
    CHECK(r[1].get<int64_t>(1) == 3);
}

namespace {

template<typename List>
typename List::ListQuery groupPage(int64_t group) {
    using Desc = typename List::ListDescriptorType;
    rspec::ListQueryParams<Desc> q;
    q.limit = 50;
    q.filters.template get<"group_id">() = group;
    return rspec::seal<Desc>(std::move(q));
}

template<typename List>
size_t groupSize(int64_t group) {
    return sync(List::query(groupPage<List>(group)))->size();
}

int64_t insertGroupedSlot(int64_t group, int32_t priority = 0) {
    return execQueryArgs(
        "INSERT INTO relais_test_slots (group_id, priority) VALUES ($1, $2) RETURNING id",
        group, priority)[0].get<int64_t>(0);
}

}  // namespace

TEMPLATE_TEST_CASE("[patchIf] list pages move on commit, stay on refusal",
                   "[patchIf][integration][list]",
                   L1TestSlotListRepo, FullCacheTestSlotListRepo, InvalidatingTestSlotListRepo)
{
    using List = TestType;
    using ListF = TestSlotListEntity::Field;
    TransactionGuard guard;
    TestInternals::resetEntityCacheState<List>();
    TestInternals::resetListCacheState<List>();

    auto id = insertGroupedSlot(100);
    insertGroupedSlot(200);
    REQUIRE(groupSize<List>(100) == 1);   // cache page group=100
    REQUIRE(groupSize<List>(200) == 1);   // cache page group=200

    // Bypass relais: a row joins group 100 behind the cached page. Only an
    // invalidation of that page can reveal it.
    insertGroupedSlot(100);

    SECTION("refused: the pages are not invalidated") {
        auto v = sync(List::patchIf(id, when(eq<ListF::version>(int64_t{1})),
            set<ListF::group_id>(int64_t{200})));
        REQUIRE(v);
        REQUIRE_FALSE(*v);
        CHECK(groupSize<List>(100) == 1);   // still the cached page
        CHECK(groupSize<List>(200) == 1);
    }

    SECTION("committed: the row leaves the old page and joins the new one") {
        auto v = sync(List::patchIf(id, when(eq<ListF::version>(int64_t{0})),
            set<ListF::group_id>(int64_t{200})));
        REQUIRE(v);
        REQUIRE(*v);
        CHECK(groupSize<List>(100) == 1);   // re-fetched: the bypass row, not ours
        CHECK(groupSize<List>(200) == 2);
    }
}

TEMPLATE_TEST_CASE("[patchIf] cross-invalidation on commit only",
                   "[patchIf][integration][cross-invalidation]",
                   InvalidatingTestSlotRepo, InvalidatingTestSlotListRepo)
{
    using Src = TestType;
    using SrcF = typename Src::EntityType::Field;
    using Target = L1SlotInvTargetRepo;
    TransactionGuard guard;
    TestInternals::resetEntityCacheState<Src>();
    TestInternals::resetEntityCacheState<Target>();

    auto id = insertGroupedSlot(5000);
    TestInternals::putInCache<Target>(int64_t{5000}, makeTestItem("old", 0, "", true, 5000));
    TestInternals::putInCache<Target>(int64_t{6000}, makeTestItem("new", 0, "", true, 6000));

    auto move = [&](int64_t expected_version) {
        return sync(Src::patchIf(id, when(eq<SrcF::version>(expected_version)),
            set<SrcF::group_id>(int64_t{6000})));
    };

    SECTION("refused: both targets stay cached") {
        auto v = move(1);
        REQUIRE(v);
        REQUIRE_FALSE(*v);
        CHECK(TestInternals::getFromCache<Target>(int64_t{5000}));
        CHECK(TestInternals::getFromCache<Target>(int64_t{6000}));
    }

    SECTION("committed: the old and the new target drop") {
        auto v = move(0);
        REQUIRE(v);
        REQUIRE(*v);
        CHECK_FALSE(TestInternals::getFromCache<Target>(int64_t{5000}));
        CHECK_FALSE(TestInternals::getFromCache<Target>(int64_t{6000}));
    }
}

// #############################################################################
//
//  K. Predicate update (patchWhere)
//
// #############################################################################

TEST_CASE("[patchWhere] SQL: locking CTE, qualified SET, before and after returned",
          "[patchWhere][sql]")
{
    namespace d = jr::detail;

    SECTION("single key: SET values first, then the predicate") {
        using P = decltype(when(eq<SlotF::group_id>(int64_t{1}), lt<SlotF::expires_at>(dbNow)));
        auto sql = d::buildPatchWhereSql<SlotTraits, P>(
            SlotMapping::table_name, SlotMapping::primary_key_column,
            {d::SetColumn(std::string_view("\"holder\"")),
             d::SetColumn(std::string_view("\"counter\""), jr::entity::SetOp::Add)},
            "id, holder, counter");
        REQUIRE(sql ==
            "WITH o AS (SELECT id, holder, counter FROM relais_test_slots "
            "WHERE \"group_id\"=$3 AND \"expires_at\"<now() FOR UPDATE) "
            "UPDATE relais_test_slots AS t SET \"holder\"=$1,\"counter\"=t.\"counter\"+$2 "
            "FROM o WHERE t.\"id\"=o.\"id\" "
            "RETURNING o.id,o.holder,o.counter,t.id,t.holder,t.counter");
        STATIC_REQUIRE(d::countColumns("id, holder, counter") == 3);
    }

    SECTION("composite key: the join covers every key column") {
        using TallyTraits = TestSlotTallyEntity::TraitsType;
        using TallyMapping = entity::generated::TestSlotTallyMapping;
        using P = decltype(when(lt<TallyF::hits>(int64_t{3})));
        auto sql = d::buildPatchWhereSql<TallyTraits, P>(
            TallyMapping::table_name, TallyMapping::primary_key_columns,
            {d::SetColumn(std::string_view("\"hits\""), jr::entity::SetOp::Add)},
            TallyMapping::SQL::returning_columns);
        REQUIRE(sql ==
            "WITH o AS (SELECT group_id, bucket, hits FROM relais_test_slot_tallies "
            "WHERE \"hits\"<$2 FOR UPDATE) "
            "UPDATE relais_test_slot_tallies AS t SET \"hits\"=t.\"hits\"+$1 "
            "FROM o WHERE t.\"group_id\"=o.\"group_id\" AND t.\"bucket\"=o.\"bucket\" "
            "RETURNING o.group_id,o.bucket,o.hits,t.group_id,t.bucket,t.hits");
    }
}

namespace {

constexpr jr::WhereOptions kAfter{.returns = jr::Returns::After};
constexpr jr::WhereOptions kChanges{.returns = jr::Returns::Changes};

template<typename T, typename Proj>
void sortBy(std::vector<T>& v, Proj proj) {
    std::sort(v.begin(), v.end(), [&](const T& a, const T& b) { return proj(a) < proj(b); });
}

}  // namespace

TEMPLATE_TEST_CASE("[patchWhere] count, committed rows, before and after",
                   "[patchWhere][integration]",
                   UncachedTestSlotRepo, L1TestSlotRepo, L2TestSlotRepo,
                   FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto a = insertGroupedSlot(1);
    auto b = insertGroupedSlot(1);
    auto other = insertGroupedSlot(2);
    for (auto id : {a, b, other}) REQUIRE(sync(Repo::find(id)));  // warm

    auto inGroup1 = when(eq<SlotF::group_id>(int64_t{1}));

    SECTION("Count: the number of rows changed, and every tier sees the change") {
        auto n = sync(Repo::patchWhere(inGroup1, increment<SlotF::counter>(3)));
        REQUIRE(n == std::optional<size_t>{2});
        CHECK(dbCounter(a) == 3);
        CHECK(dbCounter(b) == 3);
        CHECK(dbCounter(other) == 0);
        CHECK(sync(Repo::find(a))->counter == 3);
        CHECK(sync(Repo::find(b))->counter == 3);
        CHECK(sync(Repo::find(other))->counter == 0);
    }

    SECTION("After: each committed row") {
        auto rows = sync(Repo::template patchWhere<kAfter>(inGroup1,
            set<SlotF::holder>(int64_t{42}), increment<SlotF::version>(1)));
        REQUIRE(rows);
        REQUIRE(rows->size() == 2);
        sortBy(*rows, [](const auto& e) { return e.id; });
        CHECK((*rows)[0].id == a);
        CHECK((*rows)[1].id == b);
        for (const auto& e : *rows) {
            CHECK(e.holder == std::optional<int64_t>{42});
            CHECK(e.version == 1);
        }
    }

    SECTION("Changes: each row as locked, then as committed") {
        execQueryArgs("UPDATE relais_test_slots SET holder = 7 WHERE id = $1", a);
        auto changes = sync(Repo::template patchWhere<kChanges>(inGroup1,
            set<SlotF::holder>(int64_t{42}), increment<SlotF::counter>(1)));
        REQUIRE(changes);
        REQUIRE(changes->size() == 2);
        sortBy(*changes, [](const auto& c) { return c.before.id; });
        CHECK((*changes)[0].before.holder == std::optional<int64_t>{7});
        CHECK_FALSE((*changes)[1].before.holder);
        for (const auto& c : *changes) {
            CHECK(c.after.id == c.before.id);
            CHECK(c.before.counter == 0);
            CHECK(c.after.counter == 1);
            CHECK(c.after.holder == std::optional<int64_t>{42});
        }
    }

    SECTION("no row matches: zero, an empty vector, nothing written") {
        auto none = when(eq<SlotF::group_id>(int64_t{3}));
        CHECK(sync(Repo::patchWhere(none, increment<SlotF::counter>(1)))
              == std::optional<size_t>{0});
        auto rows = sync(Repo::template patchWhere<kAfter>(none, increment<SlotF::counter>(1)));
        REQUIRE(rows);
        CHECK(rows->empty());
        CHECK(dbCounter(a) == 0);
    }

    SECTION("DB error: nullopt, and the statement changed no row") {
        execQueryArgs("UPDATE relais_test_slots SET counter = 2147483000 WHERE id = $1", b);
        auto n = sync(Repo::patchWhere(inGroup1, increment<SlotF::counter>(1'000)));
        CHECK_FALSE(n);
        CHECK(dbCounter(a) == 0);
        CHECK(dbCounter(b) == 2'147'483'000);
        CHECK(sync(Repo::find(a))->counter == 0);
    }
}

TEMPLATE_TEST_CASE("[patchWhere] expired holds released by the database clock",
                   "[patchWhere][integration]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto s = insertSlotSet();
    REQUIRE(sync(Repo::find(s.b))->state == SlotState::Held);  // warm

    auto released = sync(Repo::template patchWhere<kAfter>(
        when(eq<SlotF::state>(SlotState::Held), lt<SlotF::expires_at>(dbNow)),
        set<SlotF::state>(SlotState::Free)));
    REQUIRE(released);
    REQUIRE(released->size() == 1);
    CHECK(released->front().id == s.b);
    CHECK(sync(Repo::find(s.b))->state == SlotState::Free);
    CHECK(sync(Repo::find(s.c))->state == SlotState::Held);   // not expired yet
}

TEMPLATE_TEST_CASE("[patchWhere] concurrent identical writes change each row once",
                   "[patchWhere][integration][concurrency]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    std::vector<int64_t> ids;
    for (int i = 0; i < 3; ++i) ids.push_back(insertGroupedSlot(9));
    constexpr int N = 16;

    // Same SQL, same params: each statement waits on the rows another locked,
    // re-checks the predicate, and skips the rows already moved to version 1.
    auto counts = sync([]() -> io::Task<std::vector<long>> {
        std::vector<long> out(N, -1);
        std::vector<io::Task<void>> tasks;
        tasks.reserve(N);
        for (int i = 0; i < N; ++i) {
            tasks.push_back([](long& slot) -> io::Task<void> {
                auto n = co_await Repo::patchWhere(
                    when(eq<SlotF::group_id>(int64_t{9}), eq<SlotF::version>(int64_t{0})),
                    increment<SlotF::version>(1));
                slot = n ? static_cast<long>(*n) : -1;
            }(out[i]));
        }
        co_await io::whenAll(std::move(tasks));
        co_return out;
    }());

    CHECK(std::count(counts.begin(), counts.end(), -1) == 0);
    long total = 0;
    for (auto c : counts) total += c;
    CHECK(total == 3);
    for (auto id : ids) CHECK(dbVersion(id) == 1);
}

TEMPLATE_TEST_CASE("[patchWhere] composite key", "[patchWhere][integration]",
                   UncachedTestSlotTallyRepo, FullCacheTestSlotTallyRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    execQuery("INSERT INTO relais_test_slot_tallies (group_id, bucket, hits) VALUES "
              "(1, 1, 1), (1, 2, 3), (1, 3, 0), (2, 1, 0)");
    auto k2 = std::tuple{int64_t{1}, int64_t{3}};
    REQUIRE(sync(Repo::find(k2))->hits == 0);  // warm

    // A capped counter: every bucket below the cap moves up by one.
    auto changes = sync(Repo::template patchWhere<kChanges>(
        when(lt<TallyF::hits>(int64_t{3})), increment<TallyF::hits>(1)));
    REQUIRE(changes);
    REQUIRE(changes->size() == 3);
    sortBy(*changes, [](const auto& c) { return c.before.key(); });
    CHECK((*changes)[0].before.key() == std::tuple{int64_t{1}, int64_t{1}});
    CHECK((*changes)[0].after.hits == 2);
    CHECK((*changes)[1].before.key() == k2);
    CHECK((*changes)[1].before.hits == 0);
    CHECK((*changes)[1].after.hits == 1);
    CHECK((*changes)[2].after.key() == std::tuple{int64_t{2}, int64_t{1}});
    CHECK(sync(Repo::find(k2))->hits == 1);

    auto r = execQuery("SELECT hits FROM relais_test_slot_tallies ORDER BY group_id, bucket");
    CHECK(r[1].get<int64_t>(0) == 3);  // at the cap: not matched
}

TEMPLATE_TEST_CASE("[patchWhere] a row changing group moves between list pages",
                   "[patchWhere][integration][list]",
                   L1TestSlotListRepo, FullCacheTestSlotListRepo, InvalidatingTestSlotListRepo)
{
    using List = TestType;
    using ListF = TestSlotListEntity::Field;
    TransactionGuard guard;
    TestInternals::resetEntityCacheState<List>();
    TestInternals::resetListCacheState<List>();

    insertGroupedSlot(100, 7);             // the row the predicate selects
    insertGroupedSlot(200);
    REQUIRE(groupSize<List>(100) == 1);   // cache page group=100
    REQUIRE(groupSize<List>(200) == 1);   // cache page group=200

    // Bypass relais: a row joins group 100 behind the cached page. Only an
    // invalidation of that page can reveal it.
    insertGroupedSlot(100);

    SECTION("no row matches: the pages are not invalidated") {
        auto n = sync(List::patchWhere(
            when(eq<ListF::priority>(7), eq<ListF::version>(int64_t{1})),
            set<ListF::group_id>(int64_t{200})));
        REQUIRE(n == std::optional<size_t>{0});
        CHECK(groupSize<List>(100) == 1);   // still the cached page
        CHECK(groupSize<List>(200) == 1);
    }

    SECTION("changed: the old and the new group are both invalidated") {
        auto n = sync(List::patchWhere(when(eq<ListF::priority>(7)),
            set<ListF::group_id>(int64_t{200})));
        REQUIRE(n == std::optional<size_t>{1});
        CHECK(groupSize<List>(100) == 1);   // re-fetched: the bypass row, not ours
        CHECK(groupSize<List>(200) == 2);
    }
}

TEMPLATE_TEST_CASE("[patchWhere] cross-invalidation of the old and the new target",
                   "[patchWhere][integration][cross-invalidation]",
                   InvalidatingTestSlotRepo, InvalidatingTestSlotListRepo)
{
    using Src = TestType;
    using SrcF = typename Src::EntityType::Field;
    using Target = L1SlotInvTargetRepo;
    TransactionGuard guard;
    TestInternals::resetEntityCacheState<Src>();
    TestInternals::resetEntityCacheState<Target>();

    insertGroupedSlot(5000, 7);
    TestInternals::putInCache<Target>(int64_t{5000}, makeTestItem("old", 0, "", true, 5000));
    TestInternals::putInCache<Target>(int64_t{6000}, makeTestItem("new", 0, "", true, 6000));

    auto move = [&](int64_t expected_version) {
        return sync(Src::patchWhere(
            when(eq<SrcF::priority>(7), eq<SrcF::version>(expected_version)),
            set<SrcF::group_id>(int64_t{6000})));
    };

    SECTION("no row matches: both targets stay cached") {
        REQUIRE(move(1) == std::optional<size_t>{0});
        CHECK(TestInternals::getFromCache<Target>(int64_t{5000}));
        CHECK(TestInternals::getFromCache<Target>(int64_t{6000}));
    }

    SECTION("changed: the old and the new target drop") {
        REQUIRE(move(0) == std::optional<size_t>{1});
        CHECK_FALSE(TestInternals::getFromCache<Target>(int64_t{5000}));
        CHECK_FALSE(TestInternals::getFromCache<Target>(int64_t{6000}));
    }
}
