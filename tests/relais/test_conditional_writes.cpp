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
