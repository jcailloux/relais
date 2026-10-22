/**
 * test_find_many.cpp
 * Multi-id batched read (findMany) across the mixin chain.
 *
 * Covers, tier by tier:
 *   - PgRepo::findManyRaw (WHERE pk = ANY($1));
 *   - LocalRepo::findMany (guarded MultiView<E>), composite and partition keys;
 *   - findMany without L1 (self-contained MultiView owning its entities).
 *
 * SECTION tags:
 *   [findmany]        — public/raw findMany surface
 *   [composite-key]   — multi-column matching
 *   [partition-key]   — cross-partition ANY
 */

#include <optional>
#include <span>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

using namespace relais_test;

// ---------------------------------------------------------------------------
// PgRepo::findManyRaw via WHERE pk = ANY($1)
// findManyRaw is internal (like findRaw); reached through TestInternals on
// Uncached* repos (no L1 — exercises the pure database ANY path).
// ---------------------------------------------------------------------------

namespace {

template<typename Repo, typename Key>
auto findManyRawSync(std::span<const Key> ids) {
    return sync(relais_test::TestInternals::findManyRaw<Repo>(ids));
}

// Public findMany entry — returns a guarded MultiView<E>.
template<typename Repo, typename Key>
auto findManySync(std::span<const Key> ids) {
    return sync(Repo::findMany(ids));
}


}  // namespace

TEST_CASE("findManyRaw simple key: subset / all / none / single", "[findmany][pg]") {
    TransactionGuard guard;

    auto u1 = insertTestUser("fm_u1", "fm_u1@x", 10);
    auto u2 = insertTestUser("fm_u2", "fm_u2@x", 20);
    auto u3 = insertTestUser("fm_u3", "fm_u3@x", 30);

    SECTION("subset found, request order preserved") {
        std::vector<int64_t> ids = {u3, u1};
        auto out = findManyRawSync<UncachedTestUserRepo, int64_t>(ids);
        REQUIRE(out.size() == 2);
        REQUIRE(out[0].has_value());
        REQUIRE(out[0]->username == "fm_u3");
        REQUIRE(out[1].has_value());
        REQUIRE(out[1]->username == "fm_u1");
    }

    SECTION("all found") {
        std::vector<int64_t> ids = {u1, u2, u3};
        auto out = findManyRawSync<UncachedTestUserRepo, int64_t>(ids);
        REQUIRE(out.size() == 3);
        for (const auto& o : out) REQUIRE(o.has_value());
        REQUIRE(out[0]->balance == 10);
        REQUIRE(out[1]->balance == 20);
        REQUIRE(out[2]->balance == 30);
    }

    SECTION("none found → all nullopt") {
        std::vector<int64_t> ids = {-1, -2};  // serial ids are > 0, never present
        auto out = findManyRawSync<UncachedTestUserRepo, int64_t>(ids);
        REQUIRE(out.size() == 2);
        REQUIRE(out[0] == std::nullopt);
        REQUIRE(out[1] == std::nullopt);
    }

    SECTION("single id (N=1)") {
        std::vector<int64_t> ids = {u2};
        auto out = findManyRawSync<UncachedTestUserRepo, int64_t>(ids);
        REQUIRE(out.size() == 1);
        REQUIRE(out[0].has_value());
        REQUIRE(out[0]->username == "fm_u2");
    }
}

TEST_CASE("findManyRaw simple key: shuffled order with interleaved holes", "[findmany][pg]") {
    TransactionGuard guard;

    auto u1 = insertTestUser("fm_a", "fm_a@x", 1);
    auto u2 = insertTestUser("fm_b", "fm_b@x", 2);
    auto u3 = insertTestUser("fm_c", "fm_c@x", 3);

    // ANY returns rows unordered & omits absents; output must realign on ids.
    std::vector<int64_t> ids = {u3, -1, u1, -2, u2};
    auto out = findManyRawSync<UncachedTestUserRepo, int64_t>(ids);

    REQUIRE(out.size() == 5);
    REQUIRE(out[0].has_value());  REQUIRE(out[0]->username == "fm_c");
    REQUIRE(out[1] == std::nullopt);            // hole at requested position
    REQUIRE(out[2].has_value());  REQUIRE(out[2]->username == "fm_a");
    REQUIRE(out[3] == std::nullopt);            // hole at requested position
    REQUIRE(out[4].has_value());  REQUIRE(out[4]->username == "fm_b");
}

TEST_CASE("findManyRaw empty ids → empty result, no query", "[findmany][pg]") {
    TransactionGuard guard;
    std::vector<int64_t> ids;
    auto out = findManyRawSync<UncachedTestUserRepo, int64_t>(ids);
    REQUIRE(out.empty());
}

TEST_CASE("findManyRaw composite key: two-column matching", "[findmany][composite-key]") {
    TransactionGuard guard;
    using MemKey = std::tuple<int64_t, int64_t>;

    // (1,2) and (2,1) are distinct composite keys — the matcher must not
    // confuse the columns.
    insertTestMembership(1, 2, "alpha");
    insertTestMembership(2, 1, "beta");
    insertTestMembership(1, 20, "gamma");

    SECTION("transposed keys (1,2)/(2,1) both present, not confused") {
        std::vector<MemKey> ids;
        ids.emplace_back(1, 2);
        ids.emplace_back(2, 1);
        auto out = findManyRawSync<UncachedTestMembershipRepo, MemKey>(ids);
        REQUIRE(out.size() == 2);
        REQUIRE(out[0].has_value());  REQUIRE(out[0]->role == "alpha");
        REQUIRE(out[1].has_value());  REQUIRE(out[1]->role == "beta");
    }

    SECTION("(1,2) present, transposed (2,1)… present, (20,1) absent → hole") {
        std::vector<MemKey> ids;
        ids.emplace_back(1, 20);   // present
        ids.emplace_back(20, 1);   // absent — transposition of a present key
        ids.emplace_back(1, 2);    // present
        auto out = findManyRawSync<UncachedTestMembershipRepo, MemKey>(ids);
        REQUIRE(out.size() == 3);
        REQUIRE(out[0].has_value());  REQUIRE(out[0]->role == "gamma");
        REQUIRE(out[1] == std::nullopt);   // (20,1) was never inserted
        REQUIRE(out[2].has_value());  REQUIRE(out[2]->role == "alpha");
    }

    SECTION("subset / none") {
        std::vector<MemKey> none;
        none.emplace_back(99, 99);
        auto out = findManyRawSync<UncachedTestMembershipRepo, MemKey>(none);
        REQUIRE(out.size() == 1);
        REQUIRE(out[0] == std::nullopt);
    }
}

TEST_CASE("findManyRaw partition key: cross-partition ANY", "[findmany][partition-key]") {
    TransactionGuard guard;

    auto uid = insertTestUser("fm_evt", "fm_evt@x", 0);
    auto e_eu = insertTestEvent("eu", uid, "euro", 1);
    auto e_us = insertTestEvent("us", uid, "yankee", 2);

    // ids live in different partitions (eu/us) — one ANY scans both.
    std::vector<int64_t> ids = {e_us, e_eu};
    auto out = findManyRawSync<UncachedTestEventRepo, int64_t>(ids);

    REQUIRE(out.size() == 2);
    REQUIRE(out[0].has_value());
    REQUIRE(out[0]->title == "yankee");
    REQUIRE(out[0]->region == "us");
    REQUIRE(out[1].has_value());
    REQUIRE(out[1]->title == "euro");
    REQUIRE(out[1]->region == "eu");
}

// ---------------------------------------------------------------------------
// LocalRepo::findMany : public guarded MultiView<E>
// L1-bearing repos (Local). Hot path = zero-copy under one batch guard;
// miss path = ANY query through the lower tier, force-insert L1.
// findMany is public (inherited up the chain) — called directly on the repo.
// ---------------------------------------------------------------------------

TEMPLATE_TEST_CASE("findMany simple key: cold miss then zero-copy hot path",
                   "[findmany][local]", L1TestUserRepo) {
    using Repo = TestType;
    TransactionGuard guard;

    auto u1 = insertTestUser("fm4_a", "fm4a@x", 1);
    auto u2 = insertTestUser("fm4_b", "fm4b@x", 2);
    auto u3 = insertTestUser("fm4_c", "fm4c@x", 3);

    SECTION("all cold → fetched from the database, request order with holes") {
        std::vector<int64_t> ids = {u3, -1, u1, -2, u2};
        auto v = findManySync<Repo, int64_t>(ids);
        REQUIRE(v.size() == 5);
        REQUIRE(v[0]);  REQUIRE(v[0]->username == "fm4_c");
        REQUIRE(v[1] == nullptr);
        REQUIRE(v[2]);  REQUIRE(v[2]->username == "fm4_a");
        REQUIRE(v[3] == nullptr);
        REQUIRE(v[4]);  REQUIRE(v[4]->username == "fm4_b");
    }

    SECTION("warm then all-L1-hit → synchronous, zero-copy") {
        std::vector<int64_t> ids = {u1, u2, u3};
        (void)findManySync<Repo, int64_t>(ids);   // prime L1

        // Second pass: every distinct id is in L1 → Immediate resolves without
        // a coroutine frame (await_ready), and slots are not copied.
        auto imm = Repo::findMany(std::span<const int64_t>(ids));
        REQUIRE(imm.await_ready());
        auto v = sync(std::move(imm));
        REQUIRE(v.size() == 3);
        for (size_t i = 0; i < 3; ++i) REQUIRE(v[i]);

        // Zero-copy: the view points at the exact L1 slot address.
        auto slot = TestInternals::getFromCache<Repo>(u1);
        REQUIRE(slot);
        REQUIRE(v[0] == slot.get());
    }
}

TEMPLATE_TEST_CASE("findMany oracle: findMany[i] equals find(ids[i])",
                   "[findmany]", UncachedTestUserRepo, L1TestUserRepo) {
    using Repo = TestType;
    TransactionGuard guard;

    auto u1 = insertTestUser("orc_a", "orca@x", 7);
    auto u2 = insertTestUser("orc_b", "orcb@x", 8);

    // Varied set: present, absent, and a duplicate of a present id.
    std::vector<int64_t> ids = {u2, -1, u1, u2};
    auto v = findManySync<Repo, int64_t>(ids);
    REQUIRE(v.size() == ids.size());

    for (size_t i = 0; i < ids.size(); ++i) {
        auto single = sync(Repo::find(ids[i]));
        if (single) {
            REQUIRE(v[i]);
            REQUIRE(v[i]->username == single->username);
            REQUIRE(v[i]->balance == single->balance);
        } else {
            REQUIRE(v[i] == nullptr);
        }
    }
}

TEMPLATE_TEST_CASE("findMany dedup: duplicate ids share one entry",
                   "[findmany]", UncachedTestUserRepo, L1TestUserRepo) {
    using Repo = TestType;
    TransactionGuard guard;

    auto u1 = insertTestUser("dup_a", "dupa@x", 1);
    auto u2 = insertTestUser("dup_b", "dupb@x", 2);

    // Duplicate present (u1), duplicate absent (-5), and a singleton (u2).
    std::vector<int64_t> ids = {u1, u1, -5, -5, u2};
    auto v = findManySync<Repo, int64_t>(ids);
    REQUIRE(v.size() == 5);
    REQUIRE(v[0]);
    REQUIRE(v[0] == v[1]);          // same pointer → one unique key downstream
    REQUIRE(v[2] == nullptr);
    REQUIRE(v[3] == nullptr);       // duplicate absent → nullptr everywhere
    REQUIRE(v[4]);  REQUIRE(v[4]->username == "dup_b");
}

TEMPLATE_TEST_CASE("findMany warming: miss populates L1 (same slot on re-probe)",
                   "[findmany][local]", L1TestUserRepo) {
    using Repo = TestType;
    TransactionGuard guard;

    auto u1 = insertTestUser("warm4", "warm4@x", 9);

    std::vector<int64_t> ids = {u1};
    auto v = findManySync<Repo, int64_t>(ids);   // miss → PG → store L1
    REQUIRE(v[0]);

    auto slot = TestInternals::getFromCache<Repo>(u1);
    REQUIRE(slot);
    REQUIRE(slot.get() == v[0]);     // the miss force-inserted this exact slot
}

TEST_CASE("findMany edge cases", "[findmany][local]") {
    TransactionGuard guard;

    SECTION("empty ids → empty view, synchronous, no frame") {
        std::vector<int64_t> ids;
        auto imm = L1TestUserRepo::findMany(std::span<const int64_t>(ids));
        REQUIRE(imm.await_ready());
        auto v = sync(std::move(imm));
        REQUIRE(v.empty());
        REQUIRE(v.size() == 0);
    }

    SECTION("N=1 present / absent") {
        auto u = insertTestUser("e1", "e1@x", 1);
        std::vector<int64_t> present = {u};
        auto vp = findManySync<L1TestUserRepo, int64_t>(present);
        REQUIRE(vp.size() == 1);  REQUIRE(vp[0]);  REQUIRE(vp[0]->username == "e1");

        std::vector<int64_t> absent = {-7};
        auto va = findManySync<L1TestUserRepo, int64_t>(absent);
        REQUIRE(va.size() == 1);  REQUIRE(va[0] == nullptr);
    }

    SECTION("all absent → all nullptr") {
        std::vector<int64_t> ids = {-1, -2, -3};
        auto v = findManySync<L1TestUserRepo, int64_t>(ids);
        REQUIRE(v.size() == 3);
        for (const auto* p : v) REQUIRE(p == nullptr);
    }
}

TEMPLATE_TEST_CASE("findMany composite key end-to-end",
                   "[findmany][local][composite-key]",
                   L1TestMembershipRepo) {
    using Repo = TestType;
    using MemKey = std::tuple<int64_t, int64_t>;
    TransactionGuard guard;

    insertTestMembership(1, 2, "ca");
    insertTestMembership(2, 1, "cb");
    insertTestMembership(1, 20, "cc");

    std::vector<MemKey> ids;
    ids.emplace_back(2, 1);    // present (transposed)
    ids.emplace_back(1, 2);    // present
    ids.emplace_back(20, 1);   // absent — must not be confused with (1,20)
    auto v = findManySync<Repo, MemKey>(ids);
    REQUIRE(v.size() == 3);
    REQUIRE(v[0]);  REQUIRE(v[0]->role == "cb");
    REQUIRE(v[1]);  REQUIRE(v[1]->role == "ca");
    REQUIRE(v[2] == nullptr);

    // Second pass over the present keys only = zero-copy hot path (composite).
    std::vector<MemKey> present;
    present.emplace_back(2, 1);
    present.emplace_back(1, 2);
    auto imm = Repo::findMany(std::span<const MemKey>(present));
    REQUIRE(imm.await_ready());
    auto v2 = sync(std::move(imm));
    REQUIRE(v2[0]);  REQUIRE(v2[0]->role == "cb");
    REQUIRE(v2[1]);  REQUIRE(v2[1]->role == "ca");
}

TEMPLATE_TEST_CASE("findMany partition key cross-partition end-to-end",
                   "[findmany][local][partition-key]",
                   L1TestEventRepo) {
    using Repo = TestType;
    TransactionGuard guard;

    auto uid = insertTestUser("fm4_evt", "fm4evt@x", 0);
    auto e_eu = insertTestEvent("eu", uid, "euro", 1);
    auto e_us = insertTestEvent("us", uid, "yankee", 2);

    std::vector<int64_t> ids = {e_us, e_eu, -1};
    auto v = findManySync<Repo, int64_t>(ids);
    REQUIRE(v.size() == 3);
    REQUIRE(v[0]);  REQUIRE(v[0]->title == "yankee");  REQUIRE(v[0]->region == "us");
    REQUIRE(v[1]);  REQUIRE(v[1]->title == "euro");    REQUIRE(v[1]->region == "eu");
    REQUIRE(v[2] == nullptr);
}

// findMany without L1 (Uncached): self-contained MultiView owning its entities,
// same signature as the L1 presets.
// ---------------------------------------------------------------------------

TEMPLATE_TEST_CASE("findMany without L1: edge cases", "[findmany][owned]",
                   UncachedTestUserRepo) {
    using Repo = TestType;
    TransactionGuard guard;

    SECTION("empty ids → empty view, synchronous, no frame") {
        std::vector<int64_t> ids;
        auto imm = Repo::findMany(std::span<const int64_t>(ids));
        REQUIRE(imm.await_ready());
        auto v = sync(std::move(imm));
        REQUIRE(v.empty());
    }

    SECTION("N=1 present / absent") {
        auto u = insertTestUser("own1", "own1@x", 1);
        std::vector<int64_t> present = {u};
        auto vp = findManySync<Repo, int64_t>(present);
        REQUIRE(vp.size() == 1);  REQUIRE(vp[0]);  REQUIRE(vp[0]->username == "own1");

        std::vector<int64_t> absent = {-7};
        auto va = findManySync<Repo, int64_t>(absent);
        REQUIRE(va.size() == 1);  REQUIRE(va[0] == nullptr);
    }

    SECTION("all absent → all nullptr") {
        std::vector<int64_t> ids = {-1, -2, -3};
        auto v = findManySync<Repo, int64_t>(ids);
        REQUIRE(v.size() == 3);
        for (const auto* p : v) REQUIRE(p == nullptr);
    }

    SECTION("request order with holes") {
        auto u1 = insertTestUser("own_a", "owna@x", 1);
        auto u2 = insertTestUser("own_b", "ownb@x", 2);
        std::vector<int64_t> ids = {u2, -1, u1};
        auto v = findManySync<Repo, int64_t>(ids);
        REQUIRE(v.size() == 3);
        REQUIRE(v[0]);  REQUIRE(v[0]->username == "own_b");
        REQUIRE(v[1] == nullptr);
        REQUIRE(v[2]);  REQUIRE(v[2]->username == "own_a");
    }
}

TEMPLATE_TEST_CASE("findMany without L1: composite key",
                   "[findmany][owned][composite-key]",
                   UncachedTestMembershipRepo) {
    using Repo = TestType;
    using MemKey = std::tuple<int64_t, int64_t>;
    TransactionGuard guard;

    insertTestMembership(1, 2, "ca");
    insertTestMembership(2, 1, "cb");

    std::vector<MemKey> ids;
    ids.emplace_back(2, 1);
    ids.emplace_back(20, 1);   // absent
    ids.emplace_back(1, 2);
    auto v = findManySync<Repo, MemKey>(ids);
    REQUIRE(v.size() == 3);
    REQUIRE(v[0]);  REQUIRE(v[0]->role == "cb");
    REQUIRE(v[1] == nullptr);
    REQUIRE(v[2]);  REQUIRE(v[2]->role == "ca");
}
