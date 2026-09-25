/**
 * test_enum_list.cpp
 *
 * List filters on an enum field (TestTicket::state, stored as text).
 *
 * Covers:
 *   1. Cache identity — distinct enum values give distinct group keys, and the
 *      entity blob carries the enum bytes the filter schema announces.
 *   2. L2 selective invalidation — the Lua matcher tells enum values apart and
 *      stays aligned on the filters that follow the enum.
 *   3. End to end — two queries on different enum values do not share an L1
 *      page, nor an L2 group.
 *   4. Rows — an equality filter on the enum selects the rows of that value.
 *   5. Order — range filters, sorts, ordering guards and claim orderings on
 *      the enum all follow the underlying value, never the database strings.
 */

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
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
    // queue int64 '8=' | state '4=' | state_min '4G' | weight int32 '4='
    REQUIRE(decl::filterSchema<Desc>() == "8=4=4G4=");

    auto blob = decl::encodeEntityFilterBlob<Desc>(makeTicket(TicketState::Open, 3, 7));
    CHECK(blob.size() == (1 + 8) + (1 + 4) + (1 + 4) + (1 + 4));
    CHECK(blob != decl::encodeEntityFilterBlob<Desc>(makeTicket(TicketState::Closed, 3, 7)));
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
          "[integration][db][list][enum][l1][!shouldfail]") {
    TransactionGuard tx;
    TestInternals::resetListCacheState<L1TestTicketRepo>();
    auto t = seedTickets();

    CHECK(queryIds<L1TestTicketRepo>(ticketParams(TicketState::Closed)) == Ids{t.closed});
    CHECK(queryIds<L1TestTicketRepo>(ticketParams(TicketState::Open)) == Ids{t.open});
}

TEST_CASE("[EnumList][L2] an equality filter returns the rows of that enum value",
          "[integration][db][redis][list][enum][l2][!shouldfail]") {
    TransactionGuard tx;
    using L2Desc = L2TestTicketRepo::ListDescriptorType;
    auto t = seedTickets();

    CHECK(queryIds<L2TestTicketRepo>(ticketParams<L2Desc>(TicketState::Closed)) == Ids{t.closed});
    CHECK(queryIds<L2TestTicketRepo>(ticketParams<L2Desc>(TicketState::Open)) == Ids{t.open});
}

// #############################################################################
//
//  5. Order of a mapped enum: the underlying value, on every surface
//
// #############################################################################

TEST_CASE("[EnumList][L1] a range filter compares underlying values",
          "[integration][db][list][enum][l1][!shouldfail]") {
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

TEST_CASE("[EnumList][L1] a sort on a mapped enum follows underlying values",
          "[integration][db][list][enum][l1][!shouldfail]") {
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

TEST_CASE("[EnumList] an ordering guard compares underlying values",
          "[integration][db][guard][enum][!shouldfail]") {
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
          "[integration][db][claim][enum][!shouldfail]") {
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
