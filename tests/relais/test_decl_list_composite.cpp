/**
 * test_decl_list_composite.cpp
 *
 * Keyset-cursor pagination over a COMPOSITE primary key.
 * The tiebreaker must span every key column — a single-column tiebreaker
 * would dup/skip rows whenever the sort column ties.
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;

namespace rlist = jcailloux::relais::list;
namespace rspec = jcailloux::relais::list::spec;

namespace {

using CRepo = L1TestCompositeKeyListRepo;
using CDesc = CRepo::ListDescriptorType;

CRepo::ListQuery makeQ(uint16_t limit, CRepo::Cursor cursor = {},
                       std::optional<int64_t> tenant = std::nullopt) {
    rspec::ListQueryParams<CDesc> q;
    q.limit = limit;
    q.cursor = std::move(cursor);
    if (tenant) q.filters.template get<"tenant_id">() = *tenant;
    return rspec::seal<CDesc>(std::move(q));
}

void seed(int64_t tenant_id, int64_t item_id) {
    TestCompositeKeyListEntity e;
    e.tenant_id = tenant_id;
    e.item_id = item_id;
    sync(CRepo::insert(e));
}

}  // namespace

TEST_CASE("[DeclList composite] keyset pagination over a composite key",
          "[integration][db][list][composite][query]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<CRepo>();

    // Rows that tie on the sort column (tenant_id), forcing the tiebreaker to
    // use item_id: a single-column cursor could not order these.
    seed(1, 10);
    seed(1, 20);
    seed(2, 10);
    seed(2, 20);
    seed(3, 10);

    const std::vector<std::pair<int64_t, int64_t>> expected = {
        {1, 10}, {1, 20}, {2, 10}, {2, 20}, {3, 10}};

    SECTION("single large page returns every row in keyset order") {
        auto r = sync(CRepo::query(makeQ(50)));
        REQUIRE(r->items.size() == expected.size());
        std::vector<std::pair<int64_t, int64_t>> got;
        for (const auto& e : r->items) got.emplace_back(e.tenant_id, e.item_id);
        REQUIRE(got == expected);
    }

    SECTION("paginate with limit=2: no dups, no gaps, correct order") {
        std::vector<std::pair<int64_t, int64_t>> got;
        CRepo::Cursor cursor;
        for (int page = 0; page < 8; ++page) {
            auto r = sync(CRepo::query(makeQ(2, cursor)));
            for (const auto& e : r->items) got.emplace_back(e.tenant_id, e.item_id);
            if (r->items.size() < 2) break;
            auto next = CRepo::Cursor::decode(r->cursor());
            if (!next || next->empty()) break;
            cursor = std::move(*next);
        }
        REQUIRE(got == expected);
    }

    SECTION("filter on first key column narrows the set") {
        auto r = sync(CRepo::query(makeQ(50, {}, int64_t{1})));
        REQUIRE(r->items.size() == 2);
        for (const auto& e : r->items) REQUIRE(e.tenant_id == 1);
    }
}

// =============================================================================
// Automatic same-repo list invalidation: a CRUD write through the repo must
// drop the cached pages it affects, matched by filter values (not the key).
// =============================================================================

TEST_CASE("[DeclList composite] automatic list invalidation on CRUD",
          "[integration][db][list][composite][invalidation]")
{
    TransactionGuard tx;
    TestInternals::resetListCacheState<CRepo>();

    seed(1, 10);
    seed(1, 20);
    seed(2, 10);

    auto tenantQuery = [](int64_t tenant) {
        return makeQ(50, {}, tenant);
    };

    SECTION("insert through the repo drops the matching cached page") {
        REQUIRE(sync(CRepo::query(tenantQuery(1)))->size() == 2);  // cache page tenant=1
        REQUIRE(sync(CRepo::query(tenantQuery(2)))->size() == 1);  // cache page tenant=2

        seed(1, 30);  // insert via repo → onEntityCreated → invalidate tenant=1

        // tenant=1 page is fresh (3 rows), tenant=2 page untouched (1 row).
        REQUIRE(sync(CRepo::query(tenantQuery(1)))->size() == 3);
        REQUIRE(sync(CRepo::query(tenantQuery(2)))->size() == 1);
    }

    SECTION("erase through the repo drops the matching cached page") {
        REQUIRE(sync(CRepo::query(tenantQuery(1)))->size() == 2);

        sync(CRepo::erase(std::make_tuple(int64_t{1}, int64_t{10})));  // erase via repo → onEntityDeleted

        auto r = sync(CRepo::query(tenantQuery(1)));
        REQUIRE(r->size() == 1);
        REQUIRE(r->items.front().item_id == 20);
    }
}