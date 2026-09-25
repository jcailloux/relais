/**
 * test_enum_list.cpp
 *
 * List filters on an enum field (TestTicket::state, stored as text).
 *
 * Covers:
 *   1. Cache identity — distinct enum values give distinct group keys, and the
 *      entity blob carries the enum bytes the filter schema announces. Query
 *      side: the SQL parameters bind the codec's strings, and the HTTP parsers
 *      read them (the strict one rejects an unknown string).
 *   2. L2 selective invalidation — the Lua matcher tells enum values and enum
 *      sets apart and stays aligned on the filters that follow the enum.
 *   3. End to end — two queries on different enum values do not share an L1
 *      page, nor an L2 group; a write invalidates only the pages its enum value
 *      matches.
 *   4. Rows — equality and set filters on the enum select the right rows, and
 *      every row reader decodes the enum through the field codec.
 *   5. Order — range filters, sorts, ordering guards and claim orderings on
 *      the enum all follow the underlying value, never the database strings.
 */

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;

namespace decl = jcailloux::relais::list::spec;

namespace {

using Desc = L1TestTicketRepo::ListDescriptorType;
using Params = decl::ListQueryParams<Desc>;

template<typename D = Desc>
decl::ListQueryParams<D> ticketParams(std::optional<TicketState> state,
                                      std::optional<int32_t> weight = std::nullopt,
                                      std::optional<TicketState> state_min = std::nullopt) {
    decl::ListQueryParams<D> q;
    q.limit = 10;
    if (state) q.filters.template get<"state">() = *state;
    if (weight) q.filters.template get<"weight">() = *weight;
    if (state_min) q.filters.template get<"state_min">() = *state_min;
    return q;
}

std::string groupKeyOf(const Params& q) {
    return decl::groupKey<Desc>(q.filters, q.sort);
}

TestTicketEntity makeTicket(TicketState state, int32_t weight = 0, int64_t queue = 0) {
    TestTicketEntity t;
    t.queue_id = queue;
    t.state = state;
    t.weight = weight;
    return t;
}

}  // namespace

// #############################################################################
//
//  1. Cache identity
//
// #############################################################################

TEST_CASE("[EnumList] distinct enum filter values give distinct group keys",
          "[list][enum][unit]") {
    CHECK(groupKeyOf(ticketParams(TicketState::Open))
          != groupKeyOf(ticketParams(TicketState::Closed)));
    CHECK(groupKeyOf(ticketParams(std::nullopt, std::nullopt, TicketState::Blocked))
          != groupKeyOf(ticketParams(std::nullopt, std::nullopt, TicketState::Archived)));
    CHECK(decl::encodeFilterSet<Desc>(ticketParams(TicketState::Open).filters)
          != decl::encodeFilterSet<Desc>(ticketParams(TicketState::Closed).filters));
}

TEST_CASE("[EnumList] distinct enum filter values give distinct page keys",
          "[list][enum][unit]") {
    auto open = decl::seal<Desc>(ticketParams(TicketState::Open));
    auto closed = decl::seal<Desc>(ticketParams(TicketState::Closed));
    CHECK(open.cacheKey() != closed.cacheKey());
}

TEST_CASE("[EnumList] entity blob carries the enum bytes the schema announces",
          "[list][enum][unit]") {
    // queue int64 '8=' | state '4=' | state_gt '4>' | state_in '4@' |
    // state_lt '4<' | state_max '4L' | state_min '4G' | state_ne '4!' |
    // state_nin '4#' | weight int32 '4='
    REQUIRE(decl::filterSchema<Desc>() == "8=4=4>4@4<4L4G4!4#4=");

    auto blob = decl::encodeEntityFilterBlob<Desc>(makeTicket(TicketState::Open, 3, 7));
    CHECK(blob.size() == (1 + 8) + 9 * (1 + 4));
    CHECK(blob != decl::encodeEntityFilterBlob<Desc>(makeTicket(TicketState::Closed, 3, 7)));
}

TEST_CASE("[EnumList] equality and set filters bind the codec's strings",
          "[list][enum][unit]") {
    using jcailloux::relais::io::PgParam;
    Params q;

    SECTION("EQ and NE") {
        q.filters.get<"state">() = TicketState::Closed;
        q.filters.get<"state_ne">() = TicketState::Open;
        auto w = decl::buildWhereClause<Desc>(q.filters);
        CHECK(w.sql == "\"state\"=$1 AND \"state\"!=$2");
        REQUIRE(w.params.params.size() == 2);
        CHECK(w.params.params[0] == PgParam::text("closed"));
        CHECK(w.params.params[1] == PgParam::text("open"));
    }

    SECTION("IN and NIN: one array of strings each") {
        q.filters.get<"state_in">() = std::vector{TicketState::Closed, TicketState::Open};
        q.filters.get<"state_nin">() = std::vector{TicketState::Archived};
        auto w = decl::buildWhereClause<Desc>(q.filters);
        CHECK(w.sql == "\"state\" = ANY($1) AND \"state\" != ALL($2)");
        REQUIRE(w.params.params.size() == 2);
        CHECK(w.params.params[0] == PgParam::text("{closed,open}"));
        CHECK(w.params.params[1] == PgParam::text("{archived}"));
    }

    SECTION("an ordering operator compares the rank with the underlying value") {
        q.filters.get<"state_min">() = TicketState::Closed;
        auto w = decl::buildWhereClause<Desc>(q.filters);
        CHECK(w.sql == "CASE \"state\" WHEN 'open' THEN 5 WHEN 'blocked' THEN 20 "
                       "WHEN 'closed' THEN 10 WHEN 'archived' THEN 0 END>=$1");
        REQUIRE(w.params.params.size() == 1);
        CHECK(w.params.params[0] == PgParam::bigint(10));
    }
}

namespace {

enum class Quoted : int16_t { Neg = -300, Zero = 0, Big = 32000 };

struct QuotedCodec : jcailloux::relais::entity::MappedEnumCodec<QuotedCodec, Quoted> {
    static constexpr std::array<std::pair<enum_type, std::string_view>, 3>
        pairs{{{enum_type::Neg, "it's"}, {enum_type::Zero, ""}, {enum_type::Big, "''"}}};
};

}  // namespace

TEST_CASE("[EnumList] the rank expression is built at compile time from the codec",
          "[list][enum][unit]") {
    static_assert(::entity::generated::TestTicketMapping::StateCodec::rankCases()
        == " WHEN 'open' THEN 5 WHEN 'blocked' THEN 20 WHEN 'closed' THEN 10 "
           "WHEN 'archived' THEN 0 END");
    // Quotes doubled, negative and empty values kept.
    static_assert(QuotedCodec::rankCases()
        == " WHEN 'it''s' THEN -300 WHEN '' THEN 0 WHEN '''''' THEN 32000 END");

    std::string sql;
    QuotedCodec::appendRank(sql, "t.", "\"q\"");
    CHECK(sql == "CASE t.\"q\" WHEN 'it''s' THEN -300 WHEN '' THEN 0 "
                 "WHEN '''''' THEN 32000 END");
}

TEST_CASE("[EnumList] HTTP parsing reads the codec's strings",
          "[list][enum][unit][parse]") {
    using HttpParams = std::unordered_map<std::string, std::string>;
    using Error = decl::QueryValidationError::Type;

    SECTION("tolerant: a known string sets the filter, an unknown one leaves it inactive") {
        auto q = decl::parseListQuery<Desc>(HttpParams{{"state", "closed"}, {"state_ne", "bogus"}});
        CHECK(q.filters().get<"state">() == TicketState::Closed);
        CHECK_FALSE(q.filters().get<"state_ne">().has_value());
    }

    SECTION("tolerant: a set keeps its known strings, in underlying order") {
        auto q = decl::parseListQuery<Desc>(HttpParams{{"state_in", "closed,bogus,open,closed"}});
        REQUIRE(q.filters().get<"state_in">().has_value());
        CHECK(*q.filters().get<"state_in">() == std::vector{TicketState::Open, TicketState::Closed});
    }

    SECTION("strict: known strings are accepted") {
        auto r = decl::parseListQueryStrict<Desc>(
            HttpParams{{"state", "blocked"}, {"state_nin", "archived,open"}});
        REQUIRE(r.has_value());
        CHECK(r->filters().get<"state">() == TicketState::Blocked);
        CHECK(*r->filters().get<"state_nin">() == std::vector{TicketState::Archived, TicketState::Open});
    }

    SECTION("strict: an unknown string is an invalid value") {
        // The enumerator name is not the database string.
        auto r = decl::parseListQueryStrict<Desc>(HttpParams{{"state", "Closed"}});
        REQUIRE_FALSE(r.has_value());
        CHECK(r.error().type == Error::InvalidValue);
        CHECK(r.error().field == "state");
    }

    SECTION("strict: a set with an unknown string is an invalid value") {
        auto r = decl::parseListQueryStrict<Desc>(HttpParams{{"state_in", "closed,bogus"}});
        REQUIRE_FALSE(r.has_value());
        CHECK(r.error().type == Error::InvalidValue);
        CHECK(r.error().field == "state_in");
    }
}

// #############################################################################
//
//  2. L2 selective invalidation (Lua matcher)
//
//  Each group is registered with a 1-byte page "x" — shorter than the bounds
//  header, so the Lua range check short-circuits to "delete". Whether a page
//  survives therefore depends only on the filter match.
//
// #############################################################################

namespace {

namespace cache_ns = jcailloux::relais::cache;
using jcailloux::relais::PgProvider;

constexpr std::string_view kPrefix = "T:dlist:g:";  // 10 bytes
constexpr size_t kPrefixLen = 10;
const std::string kMaster = "test:enum:l2:master";

std::string registerGroup(const Params& q) {
    std::string groupKey = std::string(kPrefix) + groupKeyOf(q);
    std::string pageKey = groupKey + ":p";
    sync(PgProvider::redis("SET", pageKey, "x"));
    sync(PgProvider::redis("SADD", groupKey + ":_keys", pageKey));
    sync(PgProvider::redis("HSET", kMaster, groupKey, "0"));
    return pageKey;
}

bool alive(const std::string& pageKey) {
    return sync(PgProvider::redis("EXISTS", pageKey)).asInteger() == 1;
}

void fireCreate(const TestTicketEntity& e) {
    sync(cache_ns::RedisCache::invalidateListGroupsSelective(
        kMaster, kPrefixLen, decl::filterSchema<Desc>(),
        decl::encodeEntityFilterBlob<Desc>(e), "0"));
}

void fireUpdate(const TestTicketEntity& oldE, const TestTicketEntity& newE) {
    sync(cache_ns::RedisCache::invalidateListGroupsSelectiveUpdate(
        kMaster, kPrefixLen, decl::filterSchema<Desc>(),
        decl::encodeEntityFilterBlob<Desc>(newE), "0",
        decl::encodeEntityFilterBlob<Desc>(oldE), "0"));
}

}  // namespace

TEST_CASE("[EnumList][L2] create invalidates only the groups the enum value matches",
          "[integration][db][redis][list][enum][l2]") {
    TransactionGuard tx;

    auto gOpen     = registerGroup(ticketParams(TicketState::Open));
    auto gClosed   = registerGroup(ticketParams(TicketState::Closed));
    auto gClosedW3 = registerGroup(ticketParams(TicketState::Closed, 3));
    auto gClosedW9 = registerGroup(ticketParams(TicketState::Closed, 9));
    auto gMinLow   = registerGroup(ticketParams(std::nullopt, std::nullopt, TicketState::Open));
    auto gMinHigh  = registerGroup(ticketParams(std::nullopt, std::nullopt, TicketState::Blocked));

    fireCreate(makeTicket(TicketState::Closed, 3));

    CHECK(alive(gOpen));             // open ≠ closed
    CHECK_FALSE(alive(gClosed));
    CHECK_FALSE(alive(gClosedW3));
    CHECK(alive(gClosedW9));         // weight after the enum stays aligned: 3 ≠ 9
    CHECK_FALSE(alive(gMinLow));     // closed (10) ≥ open (5)
    CHECK(alive(gMinHigh));          // closed (10) < blocked (20)
}

TEST_CASE("[EnumList][L2] create invalidates only the enum set groups it matches",
          "[integration][db][redis][list][enum][l2]") {
    TransactionGuard tx;

    auto setParams = [](std::vector<TicketState> in, std::vector<TicketState> nin,
                        std::optional<int32_t> weight = std::nullopt) {
        Params q = ticketParams(std::nullopt, weight);
        if (!in.empty()) q.filters.get<"state_in">() = std::move(in);
        if (!nin.empty()) q.filters.get<"state_nin">() = std::move(nin);
        return q;
    };
    auto gInHit    = registerGroup(setParams({TicketState::Open, TicketState::Closed}, {}));
    auto gInMiss   = registerGroup(setParams({TicketState::Open, TicketState::Blocked}, {}));
    auto gInHitW9  = registerGroup(setParams({TicketState::Closed}, {}, 9));
    auto gNinHit   = registerGroup(setParams({}, {TicketState::Archived}));
    auto gNinMiss  = registerGroup(setParams({}, {TicketState::Closed, TicketState::Open}));
    auto gNe       = registerGroup([] { Params q = ticketParams(std::nullopt);
                                        q.filters.get<"state_ne">() = TicketState::Closed;
                                        return q; }());

    fireCreate(makeTicket(TicketState::Closed, 3));

    CHECK_FALSE(alive(gInHit));      // closed ∈ {open, closed}
    CHECK(alive(gInMiss));           // closed ∉ {open, blocked}
    CHECK(alive(gInHitW9));          // weight after the sets stays aligned: 3 ≠ 9
    CHECK_FALSE(alive(gNinHit));     // closed ∉ {archived}
    CHECK(alive(gNinMiss));          // closed ∈ {closed, open}
    CHECK(alive(gNe));               // closed ≠ closed is false
}

TEST_CASE("[EnumList][L2] update invalidates the groups of the old and new enum values",
          "[integration][db][redis][list][enum][l2]") {
    TransactionGuard tx;

    auto gOpen    = registerGroup(ticketParams(TicketState::Open));
    auto gBlocked = registerGroup(ticketParams(TicketState::Blocked));
    auto gClosed  = registerGroup(ticketParams(TicketState::Closed));

    fireUpdate(makeTicket(TicketState::Open), makeTicket(TicketState::Blocked));

    CHECK_FALSE(alive(gOpen));
    CHECK_FALSE(alive(gBlocked));
    CHECK(alive(gClosed));
}

// #############################################################################
//
//  3. End to end
//
// #############################################################################

TEST_CASE("[EnumList][L1] queries on different enum values do not share a page",
          "[integration][db][list][enum][l1]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<L1TestTicketRepo>();

    sync(L1TestTicketRepo::query(decl::seal<Desc>(ticketParams(TicketState::Open))));
    sync(L1TestTicketRepo::query(decl::seal<Desc>(ticketParams(TicketState::Closed))));

    CHECK(TestInternals::listCacheSize<L1TestTicketRepo>() == 2);
}

TEST_CASE("[EnumList][L2] queries on different enum values do not share a group",
          "[integration][db][redis][list][enum][l2]") {
    TransactionGuard tx;
    using L2Desc = L2TestTicketRepo::ListDescriptorType;

    sync(L2TestTicketRepo::query(decl::seal<L2Desc>(ticketParams<L2Desc>(TicketState::Open))));
    sync(L2TestTicketRepo::query(decl::seal<L2Desc>(ticketParams<L2Desc>(TicketState::Closed))));

    CHECK(sync(PgProvider::redis("HLEN", "test:ticket:l2:dlist_groups")).asInteger() == 2);
}

// #############################################################################
//
//  4. Rows selected by a mapped enum filter
//
//  One ticket per state, inserted in declaration order, so the primary key
//  order (open, blocked, closed, archived) differs from both the underlying
//  order (archived, open, closed, blocked) and the alphabetical order of the
//  database strings (archived, blocked, closed, open).
//
// #############################################################################

namespace {

namespace jr = jcailloux::relais;
using TicketF = TestTicketEntity::Field;
using Ids = std::vector<int64_t>;

struct Tickets { int64_t open, blocked, closed, archived; };

int64_t insertTicket(const char* state, int64_t queue = 1) {
    auto r = execQueryArgs(
        "INSERT INTO relais_test_tickets (queue_id, state, weight, title) "
        "VALUES ($1, $2, 0, '') RETURNING id", queue, state);
    return r[0].get<int64_t>(0);
}

Tickets seedTickets() {
    Tickets t{};
    t.open = insertTicket("open");
    t.blocked = insertTicket("blocked");
    t.closed = insertTicket("closed");
    t.archived = insertTicket("archived");
    return t;
}

template<typename E>
Ids idsOf(const std::vector<E>& rows) {
    Ids ids;
    for (const auto& e : rows) ids.push_back(e.id);
    return ids;
}

template<typename Repo>
Ids queryIds(decl::ListQueryParams<typename Repo::ListDescriptorType> q) {
    auto r = sync(Repo::query(decl::seal<typename Repo::ListDescriptorType>(std::move(q))));
    return idsOf(r->items);
}

}  // namespace

TEST_CASE("[EnumList][L1] an equality filter returns the rows of that enum value",
          "[integration][db][list][enum][l1]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<L1TestTicketRepo>();
    auto t = seedTickets();

    CHECK(queryIds<L1TestTicketRepo>(ticketParams(TicketState::Closed)) == Ids{t.closed});
    CHECK(queryIds<L1TestTicketRepo>(ticketParams(TicketState::Open)) == Ids{t.open});
}

TEST_CASE("[EnumList][L2] an equality filter returns the rows of that enum value",
          "[integration][db][redis][list][enum][l2]") {
    TransactionGuard tx;
    using L2Desc = L2TestTicketRepo::ListDescriptorType;
    auto t = seedTickets();

    CHECK(queryIds<L2TestTicketRepo>(ticketParams<L2Desc>(TicketState::Closed)) == Ids{t.closed});
    CHECK(queryIds<L2TestTicketRepo>(ticketParams<L2Desc>(TicketState::Open)) == Ids{t.open});
}

TEST_CASE("[EnumList][L1] not-equal and set filters return the rows of those enum values",
          "[integration][db][list][enum][l1]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<L1TestTicketRepo>();
    auto t = seedTickets();

    SECTION("NE") {
        auto q = ticketParams(std::nullopt);
        q.filters.get<"state_ne">() = TicketState::Closed;
        CHECK(queryIds<L1TestTicketRepo>(std::move(q)) == Ids{t.open, t.blocked, t.archived});
    }

    SECTION("IN") {
        auto q = ticketParams(std::nullopt);
        q.filters.get<"state_in">() = std::vector{TicketState::Archived, TicketState::Blocked};
        CHECK(queryIds<L1TestTicketRepo>(std::move(q)) == Ids{t.blocked, t.archived});
    }

    SECTION("NIN") {
        auto q = ticketParams(std::nullopt);
        q.filters.get<"state_nin">() = std::vector{TicketState::Archived, TicketState::Blocked};
        CHECK(queryIds<L1TestTicketRepo>(std::move(q)) == Ids{t.open, t.closed});
    }
}

TEST_CASE("[EnumList] rows decode the enum through the field codec",
          "[integration][db][enum][rowview]") {
    TransactionGuard tx;
    using Mapping = entity::generated::TestTicketMapping;
    auto t = seedTickets();

    const std::pair<int64_t, TicketState> expected[] = {
        {t.open, TicketState::Open}, {t.blocked, TicketState::Blocked},
        {t.closed, TicketState::Closed}, {t.archived, TicketState::Archived}};
    for (const auto& [id, state] : expected) {
        auto r = execQueryArgs(
            (std::string("SELECT ") + Mapping::SQL::returning_columns
             + " FROM relais_test_tickets WHERE id = $1").c_str(), id);
        REQUIRE(r.rows() == 1);

        auto e = Mapping::fromRow<TestTicketEntity>(r[0]);
        REQUIRE(e.has_value());
        CHECK(e->state == state);
        CHECK(*Mapping::rowToJson(r[0]) == e->json());
        CHECK(*Mapping::rowToBeve(r[0]) == e->binary());
    }
}

namespace {

/// Cache the pages of `closed` and of `{open, archived}`, then insert a closed
/// ticket through the repository: the first page is refreshed, the second is
/// kept. A ticket inserted behind the cache's back shows which pages stayed.
template<typename Repo>
void checkWriteInvalidatesMatchingPages() {
    using D = typename Repo::ListDescriptorType;
    auto t = seedTickets();

    auto closedQ = [] { return ticketParams<D>(TicketState::Closed); };
    auto setQ = [] {
        auto q = ticketParams<D>(std::nullopt);
        q.filters.template get<"state_in">() = std::vector{TicketState::Open, TicketState::Archived};
        return q;
    };
    REQUIRE(queryIds<Repo>(closedQ()) == Ids{t.closed});
    REQUIRE(queryIds<Repo>(setQ()) == Ids{t.open, t.archived});

    insertTicket("open");
    auto created = sync(Repo::insert(makeTicket(TicketState::Closed, 0, 1)));
    REQUIRE(created != nullptr);

    CHECK(queryIds<Repo>(closedQ()) == Ids{t.closed, created->id});
    CHECK(queryIds<Repo>(setQ()) == Ids{t.open, t.archived});   // kept: the hidden ticket stays unseen
}

}  // namespace

TEST_CASE("[EnumList][L1] a write invalidates only the pages its enum value matches",
          "[integration][db][list][enum][l1]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<L1TestTicketRepo>();
    checkWriteInvalidatesMatchingPages<L1TestTicketRepo>();
}

TEST_CASE("[EnumList][L2] a write invalidates only the pages its enum value matches",
          "[integration][db][redis][list][enum][l2]") {
    TransactionGuard tx;
    checkWriteInvalidatesMatchingPages<L2TestTicketRepo>();
}

// #############################################################################
//
//  5. Order of a mapped enum: the underlying value, on every surface
//
// #############################################################################

TEST_CASE("[EnumList][L1] a range filter compares underlying values",
          "[integration][db][list][enum][l1]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<L1TestTicketRepo>();
    auto t = seedTickets();

    // state ≥ closed (10): closed and blocked (20), in primary key order.
    CHECK(queryIds<L1TestTicketRepo>(ticketParams(std::nullopt, std::nullopt, TicketState::Closed))
          == Ids{t.blocked, t.closed});
    // state ≥ open (5): every state but archived (0).
    CHECK(queryIds<L1TestTicketRepo>(ticketParams(std::nullopt, std::nullopt, TicketState::Open))
          == Ids{t.open, t.blocked, t.closed});
}

TEST_CASE("[EnumList][L1] strict and upper range filters compare underlying values",
          "[integration][db][list][enum][l1]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<L1TestTicketRepo>();
    auto t = seedTickets();

    auto range = [](auto set) {
        auto q = ticketParams(std::nullopt);
        set(q.filters);
        return queryIds<L1TestTicketRepo>(std::move(q));
    };
    // state > open (5): closed (10) and blocked (20).
    CHECK(range([](auto& f) { f.template get<"state_gt">() = TicketState::Open; })
          == Ids{t.blocked, t.closed});
    // state < closed (10): open (5) and archived (0).
    CHECK(range([](auto& f) { f.template get<"state_lt">() = TicketState::Closed; })
          == Ids{t.open, t.archived});
    // state ≤ open (5): open and archived; blocked sorts before open as text.
    CHECK(range([](auto& f) { f.template get<"state_max">() = TicketState::Open; })
          == Ids{t.open, t.archived});
    // open (5) < state ≤ closed (10): closed only.
    CHECK(range([](auto& f) {
              f.template get<"state_gt">() = TicketState::Open;
              f.template get<"state_max">() = TicketState::Closed;
          }) == Ids{t.closed});
}

TEST_CASE("[EnumList][L1] a sort on a mapped enum follows underlying values",
          "[integration][db][list][enum][l1]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<L1TestTicketRepo>();
    auto t = seedTickets();

    SECTION("ascending") {
        auto q = ticketParams(std::nullopt);
        q.sort = jr::list::SortSpec<size_t>{1, jr::list::SortDirection::Asc};
        CHECK(queryIds<L1TestTicketRepo>(std::move(q))
              == Ids{t.archived, t.open, t.closed, t.blocked});
    }

    SECTION("descending") {
        auto q = ticketParams(std::nullopt);
        q.sort = jr::list::SortSpec<size_t>{1, jr::list::SortDirection::Desc};
        CHECK(queryIds<L1TestTicketRepo>(std::move(q))
              == Ids{t.blocked, t.closed, t.open, t.archived});
    }
}

namespace {

/// Every page of a sort on `state`, three rows at a time, each page after the
/// first reached through the previous page's cursor.
template<typename Repo>
Ids walkStatePages(jcailloux::relais::list::SortDirection dir) {
    using D = typename Repo::ListDescriptorType;
    Ids all;
    decl::TypedCursor<D> cursor;
    for (int page = 0; page < 10; ++page) {
        auto q = ticketParams<D>(std::nullopt);
        q.limit = 3;
        q.sort = jr::list::SortSpec<size_t>{1, dir};
        q.cursor = cursor;
        auto r = sync(Repo::query(decl::seal<D>(std::move(q))));
        for (const auto& e : r->items) all.push_back(e.id);
        if (r->cursor().empty()) break;
        cursor = decl::TypedCursor<D>::decode(r->cursor()).value();
    }
    return all;
}

/// Two rows per state but archived: ties are broken by primary key, and each
/// page boundary falls between or inside a run of equal states.
template<typename Repo>
void checkStatePagination() {
    auto t = seedTickets();
    auto open2 = insertTicket("open");
    auto closed2 = insertTicket("closed");
    auto blocked2 = insertTicket("blocked");

    CHECK(walkStatePages<Repo>(jr::list::SortDirection::Asc)
          == Ids{t.archived, t.open, open2, t.closed, closed2, t.blocked, blocked2});
    CHECK(walkStatePages<Repo>(jr::list::SortDirection::Desc)
          == Ids{blocked2, t.blocked, closed2, t.closed, open2, t.open, t.archived});
}

}  // namespace

TEST_CASE("[EnumList][L1] keyset pages on a mapped enum follow underlying values",
          "[integration][db][list][enum][l1]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<L1TestTicketRepo>();
    checkStatePagination<L1TestTicketRepo>();
}

TEST_CASE("[EnumList][L2] keyset pages on a mapped enum follow underlying values",
          "[integration][db][redis][list][enum][l2]") {
    TransactionGuard tx;
    checkStatePagination<L2TestTicketRepo>();
}

TEST_CASE("[EnumList] an ordering guard compares underlying values",
          "[integration][db][guard][enum]") {
    using jr::entity::when;
    using jr::entity::gt;
    using jr::entity::le;
    using jr::entity::set;
    TransactionGuard tx;
    auto t = seedTickets();

    SECTION("gt: blocked (20) > closed (10), open (5) is not") {
        auto blocked = sync(L1TestTicketRepo::patchIf(t.blocked,
            when(gt<TicketF::state>(TicketState::Closed)), set<TicketF::weight>(1)));
        REQUIRE(blocked);
        CHECK(*blocked);

        auto open = sync(L1TestTicketRepo::patchIf(t.open,
            when(gt<TicketF::state>(TicketState::Closed)), set<TicketF::weight>(1)));
        REQUIRE(open);
        CHECK_FALSE(*open);
    }

    SECTION("le: open (5) ≤ closed (10), blocked (20) is not") {
        auto open = sync(L1TestTicketRepo::patchIf(t.open,
            when(le<TicketF::state>(TicketState::Closed)), set<TicketF::weight>(1)));
        REQUIRE(open);
        CHECK(*open);

        auto blocked = sync(L1TestTicketRepo::patchIf(t.blocked,
            when(le<TicketF::state>(TicketState::Closed)), set<TicketF::weight>(1)));
        REQUIRE(blocked);
        CHECK_FALSE(*blocked);
    }
}

TEST_CASE("[EnumList] a claim ordered on a mapped enum follows underlying values",
          "[integration][db][claim][enum]") {
    using jr::entity::when;
    using jr::entity::eq;
    using jr::entity::set;
    using jr::entity::orderBy;
    using jr::entity::asc;
    using jr::entity::desc;
    TransactionGuard tx;
    auto t = seedTickets();

    SECTION("ascending") {
        auto rows = sync(L1TestTicketRepo::claim(when(eq<TicketF::queue_id>(int64_t{1})),
            orderBy(asc<TicketF::state>()), 4, set<TicketF::weight>(1)));
        REQUIRE(rows);
        CHECK(idsOf(*rows) == Ids{t.archived, t.open, t.closed, t.blocked});
    }

    SECTION("descending, first row only") {
        auto rows = sync(L1TestTicketRepo::claim(when(eq<TicketF::queue_id>(int64_t{1})),
            orderBy(desc<TicketF::state>()), 1, set<TicketF::weight>(1)));
        REQUIRE(rows);
        CHECK(idsOf(*rows) == Ids{t.blocked});
    }
}
