/**
 * test_upsert.cpp
 *
 * Native upsert = INSERT … ON CONFLICT (pk) DO UPDATE SET <non-pk>=EXCLUDED.<col>
 * RETURNING. One atomic statement resolves the create-vs-update race in the
 * database; the repository layers around it never invent a coherence model, they
 * compose the primitives already used by insert (store-through the committed row)
 * and update (evict-by-precaution on an uncertain outcome, pre-image discriminant
 * for list/cross invalidation).
 *
 * This suite walks the contract from the outside in:
 *   A. generation + applicability gate — what SQL is emitted, and for which shapes
 *      upsert is exposed at all (assigned PK + a non-PK column to SET).
 *   B. INSERT branch (key absent)     — the committed row is returned and cached.
 *   C. UPDATE branch (key present)    — the cache is superseded, not left stale.
 *   D. composite key                  — ON CONFLICT spans every PK column.
 *   E. automatic list invalidation    — the pre-image routes create vs. migrate.
 *   F. cross invalidation             — old and new targets both drop on migrate.
 *
 * Ground truth for the write is read back over the ordinary relais connection
 * (every case here is a happy-path commit). The one resilience axis that needs a
 * poisoned connection — an uncertain timeout after the write flushed — lives in
 * test_cache_coherence.cpp, which owns the fault-injection harness; the upsert
 * cases there prove the evict-by-precaution (update model, not insert).
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/TestQueryHelpers.h"
#include "fixtures/RelaisTestAccessors.h"
#include "jcailloux/relais/cache/RedisCache.h"

using namespace relais_test;

namespace jr = jcailloux::relais;
namespace rspec = jcailloux::relais::list::spec;

namespace {

// Read a row straight from the DB (ground truth for "did the write land").
struct AkeyRow { int64_t payload; std::string note; };
std::optional<AkeyRow> dbAkey(int64_t key_id) {
    auto r = execQueryArgs(
        "SELECT payload, note FROM relais_test_assigned_keys WHERE key_id = $1", key_id);
    if (r.empty()) return std::nullopt;
    return AkeyRow{r[0].get<int64_t>(0), r[0].get<std::string>(1)};
}

io::Task<std::optional<std::string>> redisGet(std::string key) {
    co_return co_await jr::cache::RedisCache::getRaw(key);
}

}  // namespace

// #############################################################################
//
//  A. Generation + applicability gate (no DB)
//
//  Upsert is only meaningful when the caller supplies the PK (so it can appear
//  in the INSERT) and there is at least one non-PK column to SET. The generator
//  emits SQL::upsert + supports_upsert exactly then; the concepts mirror that so
//  an ineligible entity simply does not expose the method (clean SFINAE, not a
//  hard error).
//
// #############################################################################

TEST_CASE("[upsert] generated SQL and applicability gate", "[upsert][compile][sql]")
{
    using Akey = entity::generated::TestAssignedKeyMapping;
    using Member = entity::generated::TestMembershipMapping;

    SECTION("scalar PK: ON CONFLICT (pk) DO UPDATE SET <non-pk>=EXCLUDED, RETURNING all") {
        std::string sql = Akey::SQL::upsert;
        CHECK(sql.find("INSERT INTO relais_test_assigned_keys (key_id, payload, note)")
              != std::string::npos);
        CHECK(sql.find("ON CONFLICT (key_id)") != std::string::npos);
        CHECK(sql.find("DO UPDATE SET payload=EXCLUDED.payload, note=EXCLUDED.note")
              != std::string::npos);
        CHECK(sql.find("RETURNING key_id, payload, note") != std::string::npos);
        // The conflict target is the PK, and EXCLUDED (the proposed row) feeds the
        // SET — no extra params beyond the INSERT's $1..$n.
        CHECK(sql.find("$4") == std::string::npos);
    }

    SECTION("composite PK: ON CONFLICT lists every key column") {
        std::string sql = Member::SQL::upsert;
        CHECK(sql.find("ON CONFLICT (user_id, group_id)") != std::string::npos);
        CHECK(sql.find("DO UPDATE SET role=EXCLUDED.role") != std::string::npos);
        // db_managed joined_at is not INSERTed but is returned for store-through.
        CHECK(sql.find("RETURNING user_id, group_id, role, joined_at") != std::string::npos);
    }

    SECTION("supports_upsert trait tracks eligibility") {
        STATIC_CHECK(Akey::supports_upsert);
        STATIC_CHECK(Member::supports_upsert);
        STATIC_CHECK(entity::generated::TestUpsertListMapping::supports_upsert);
        // All-PK junction: no non-PK column to SET → DO UPDATE would be empty.
        STATIC_CHECK_FALSE(entity::generated::TestCompositeKeyListMapping::supports_upsert);
    }

    SECTION("HasUpsertSql gates on the emitted statement") {
        STATIC_CHECK(jr::HasUpsertSql<TestAssignedKeyEntity>);
        STATIC_CHECK(jr::HasUpsertSql<TestMembershipEntity>);
        STATIC_CHECK(jr::HasUpsertSql<TestUpsertListEntity>);
        // Serial (db_managed) PK: the INSERT omits the id, so ON CONFLICT (id)
        // has nothing to bind → no upsert SQL, method not exposed.
        STATIC_CHECK_FALSE(jr::HasUpsertSql<TestItemEntity>);
        // All-PK junction: same gate as supports_upsert.
        STATIC_CHECK_FALSE(jr::HasUpsertSql<TestCompositeKeyListEntity>);
    }

    SECTION("UpsertableEntity requires a full-row update capability") {
        STATIC_CHECK(jr::UpsertableEntity<TestAssignedKeyEntity, int64_t>);
        // NB: UpsertableEntity alone does NOT exclude a serial PK (TestItem is
        // creatable + full-updatable); HasUpsertSql is the half of the method
        // gate that encodes "the PK is caller-assigned". Both must hold.
        STATIC_CHECK_FALSE(jr::HasUpsertSql<TestItemEntity>);
    }
}

// #############################################################################
//
//  B. INSERT branch — key absent
//
//  A fresh key falls through ON CONFLICT to a plain INSERT. The RETURNING row is
//  the source of truth (db-computed columns and all) and is store-through cached
//  exactly like insert().
//
// #############################################################################

TEST_CASE("[upsert] insert branch commits and caches the row",
          "[integration][db][upsert]")
{
    TransactionGuard tx;

    SECTION("Uncached: returns the committed row, DB holds it") {
        auto v = sync(UncachedTestAssignedKeyRepo::upsert(makeTestAssignedKey(1, 10, "a")));
        REQUIRE(v);
        CHECK(v->key_id == 1);
        CHECK(v->payload == 10);
        CHECK(v->note == "a");

        auto row = dbAkey(1);
        REQUIRE(row.has_value());
        CHECK(row->payload == 10);
        CHECK(row->note == "a");
    }

    SECTION("L1: the following find is served from cache") {
        TestInternals::resetEntityCacheState<L1TestAssignedKeyRepo>();

        sync(L1TestAssignedKeyRepo::upsert(makeTestAssignedKey(2, 20, "b")));
        // Store-through populated L1: the entry is present without a DB re-read.
        REQUIRE(TestInternals::getFromCache<L1TestAssignedKeyRepo>(int64_t{2}));

        auto v = sync(L1TestAssignedKeyRepo::find(2));
        REQUIRE(v);
        CHECK(v->payload == 20);
    }

    SECTION("Both: the committed row lands in L2") {
        TestInternals::resetEntityCacheState<FullCacheTestAssignedKeyRepo>();

        auto v = sync(FullCacheTestAssignedKeyRepo::upsert(makeTestAssignedKey(3, 30, "c")));
        REQUIRE(v);

        auto key = FullCacheTestAssignedKeyRepo::makeRedisKey(int64_t{3});
        REQUIRE(sync(redisGet(key)).has_value());   // store-through reached Redis
    }
}

// #############################################################################
//
//  C. UPDATE branch — key present
//
//  A conflicting key resolves to DO UPDATE. Unlike insert, a pre-existing cache
//  entry must be superseded, not appended: the RETURNING row is store-through
//  cached over the old one and the PK is left intact.
//
// #############################################################################

TEST_CASE("[upsert] update branch supersedes the cached value",
          "[integration][db][upsert]")
{
    TransactionGuard tx;

    SECTION("Uncached: DB carries the new value, PK unchanged") {
        sync(UncachedTestAssignedKeyRepo::upsert(makeTestAssignedKey(5, 1, "old")));
        auto v = sync(UncachedTestAssignedKeyRepo::upsert(makeTestAssignedKey(5, 2, "new")));
        REQUIRE(v);
        CHECK(v->key_id == 5);
        CHECK(v->payload == 2);
        CHECK(v->note == "new");

        auto row = dbAkey(5);
        REQUIRE(row.has_value());
        CHECK(row->payload == 2);
    }

    SECTION("L1: a warm entry is replaced, find serves the new value") {
        TestInternals::resetEntityCacheState<L1TestAssignedKeyRepo>();

        sync(L1TestAssignedKeyRepo::upsert(makeTestAssignedKey(6, 1, "old")));
        REQUIRE(sync(L1TestAssignedKeyRepo::find(6))->payload == 1);  // warm

        sync(L1TestAssignedKeyRepo::upsert(makeTestAssignedKey(6, 2, "new")));
        auto v = sync(L1TestAssignedKeyRepo::find(6));
        REQUIRE(v);
        CHECK(v->payload == 2);   // superseded, not the stale 1
        CHECK(v->note == "new");
    }

    SECTION("Both: L2 holds the new value (no stale straddle)") {
        TestInternals::resetEntityCacheState<FullCacheTestAssignedKeyRepo>();

        sync(FullCacheTestAssignedKeyRepo::upsert(makeTestAssignedKey(7, 1, "old")));
        sync(FullCacheTestAssignedKeyRepo::find(7));  // warm L1+L2
        sync(FullCacheTestAssignedKeyRepo::upsert(makeTestAssignedKey(7, 2, "new")));

        // Drop L1 so the next read must go through L2 — proves L2 was superseded.
        TestInternals::evict<FullCacheTestAssignedKeyRepo>(int64_t{7});
        auto v = sync(FullCacheTestAssignedKeyRepo::find(7));
        REQUIRE(v);
        CHECK(v->payload == 2);
    }

    SECTION("idempotent: a second identical upsert converges") {
        sync(UncachedTestAssignedKeyRepo::upsert(makeTestAssignedKey(8, 42, "x")));
        auto v = sync(UncachedTestAssignedKeyRepo::upsert(makeTestAssignedKey(8, 42, "x")));
        REQUIRE(v);
        CHECK(v->payload == 42);

        auto count = execQueryArgs(
            "SELECT count(*) FROM relais_test_assigned_keys WHERE key_id = $1", int64_t{8});
        CHECK(count[0].get<int64_t>(0) == 1);   // exactly one row, not two
    }
}

// #############################################################################
//
//  D. Composite key
//
//  ON CONFLICT must span the whole composite PK so the conflict targets the one
//  right row. The db_managed column (joined_at) is populated from RETURNING, not
//  from the input struct.
//
// #############################################################################

TEST_CASE("[upsert] composite key targets the exact row",
          "[integration][db][upsert][composite]")
{
    TransactionGuard tx;

    SECTION("insert then update the same (user_id, group_id)") {
        auto a = sync(FullCacheTestMembershipRepo::upsert(makeTestMembership(1, 2, "member")));
        REQUIRE(a);
        CHECK(a->role == "member");

        auto b = sync(FullCacheTestMembershipRepo::upsert(makeTestMembership(1, 2, "admin")));
        REQUIRE(b);
        CHECK(b->user_id == 1);
        CHECK(b->group_id == 2);
        CHECK(b->role == "admin");   // updated in place

        // A neighbouring composite key is untouched by the conflict resolution.
        sync(FullCacheTestMembershipRepo::upsert(makeTestMembership(1, 3, "guest")));
        auto other = sync(FullCacheTestMembershipRepo::find(std::make_tuple(int64_t{1}, int64_t{3})));
        REQUIRE(other);
        CHECK(other->role == "guest");

        auto still = sync(FullCacheTestMembershipRepo::find(std::make_tuple(int64_t{1}, int64_t{2})));
        REQUIRE(still);
        CHECK(still->role == "admin");
    }
}

// #############################################################################
//
//  E. Automatic list invalidation
//
//  The list tier is where the create/update discriminant earns its keep. A
//  single best-effort pre-image decides: an absent row appends (onEntityCreated),
//  a present one migrates (onEntityUpdated(old, new)). The pre-image is what lets
//  the OLD owner's page drop when the filter/sort field moves — a create-only
//  path would leave it stale.
//
// #############################################################################

namespace {

using UList = L1TestUpsertListRepo;
using UDesc = UList::ListDescriptorType;

UList::ListQuery ownerPage(int64_t owner) {
    rspec::ListQueryParams<UDesc> q;
    q.limit = 50;
    q.filters.template get<"owner_id">() = owner;
    return rspec::seal<UDesc>(std::move(q));
}

size_t ownerSize(int64_t owner) {
    return sync(UList::query(ownerPage(owner)))->size();
}

}  // namespace

TEST_CASE("[upsert] insert via upsert drops the matching list page",
          "[integration][db][upsert][list]")
{
    TransactionGuard tx;
    TestInternals::resetEntityCacheState<UList>();
    TestInternals::resetListCacheState<UList>();

    sync(UList::insert(makeTestUpsertList(1, 100, "a")));
    sync(UList::insert(makeTestUpsertList(10, 200, "z")));
    REQUIRE(ownerSize(100) == 1);   // cache page owner=100
    REQUIRE(ownerSize(200) == 1);   // cache page owner=200

    // Absent key → insert branch → onEntityCreated → invalidate owner=100 only.
    sync(UList::upsert(makeTestUpsertList(2, 100, "b")));

    CHECK(ownerSize(100) == 2);     // fresh: the new row appears
    CHECK(ownerSize(200) == 1);     // untouched neighbour
}

TEST_CASE("[upsert] update via upsert migrates the row across list pages",
          "[integration][db][upsert][list]")
{
    TransactionGuard tx;
    TestInternals::resetEntityCacheState<UList>();
    TestInternals::resetListCacheState<UList>();

    sync(UList::insert(makeTestUpsertList(1, 100, "a")));
    sync(UList::insert(makeTestUpsertList(10, 200, "z")));
    REQUIRE(ownerSize(100) == 1);
    REQUIRE(ownerSize(200) == 1);

    // Present key, owner_id (filter+sort) changes 100 → 200. The pre-image makes
    // this an onEntityUpdated(old owner=100, new owner=200): the row must LEAVE
    // the owner=100 page and JOIN the owner=200 page. A create-only path would
    // never invalidate owner=100 and it would answer a stale size of 1.
    sync(UList::upsert(makeTestUpsertList(1, 200, "a2")));

    CHECK(ownerSize(100) == 0);     // the row left — proves the pre-image discriminant
    CHECK(ownerSize(200) == 2);     // the row arrived
}

TEST_CASE("[upsert] update via upsert refreshes a page on a plain-column change",
          "[integration][db][upsert][list]")
{
    TransactionGuard tx;
    TestInternals::resetEntityCacheState<UList>();
    TestInternals::resetListCacheState<UList>();

    sync(UList::insert(makeTestUpsertList(1, 100, "a")));
    REQUIRE(sync(UList::query(ownerPage(100)))->items.front().label == "a");  // warm

    // owner_id unchanged, only label moves. Both pre- and post-image match the
    // owner=100 page, so it is invalidated and re-fetched fresh — never left
    // holding the stale "a".
    sync(UList::upsert(makeTestUpsertList(1, 100, "b")));

    auto r = sync(UList::query(ownerPage(100)));
    REQUIRE(r->size() == 1);
    CHECK(r->items.front().label == "b");
}

// #############################################################################
//
//  F. Cross invalidation
//
//  InvalidationMixin routes the same pre-image discriminant to propagateCreate /
//  propagateUpdate. On a migrate, BOTH the old and the new target key drop — the
//  extractor is evaluated on the pre-image and on the committed row. This entity
//  has no list, so the mixin takes the non-list branch (Base::upsert).
//
// #############################################################################

TEST_CASE("[upsert] insert via upsert cross-invalidates the target",
          "[integration][db][upsert][cross-invalidation]")
{
    TransactionGuard tx;
    using Src = InvalidatingTestAssignedKeyRepo;
    TestInternals::resetEntityCacheState<Src>();
    TestInternals::resetEntityCacheState<L1UpsertInvTargetRepo>();

    // payload = 7000 is a foreign key into the item repo; seed that target entry.
    TestInternals::putInCache<L1UpsertInvTargetRepo>(int64_t{7000},
        makeTestItem("t", 0, "", true, 7000));
    REQUIRE(TestInternals::getFromCache<L1UpsertInvTargetRepo>(int64_t{7000}));

    // Absent key → insert branch → propagateCreate → invalidate target 7000.
    sync(Src::upsert(makeTestAssignedKey(1, 7000, "a")));

    CHECK_FALSE(TestInternals::getFromCache<L1UpsertInvTargetRepo>(int64_t{7000}));
}

TEST_CASE("[upsert] update via upsert cross-invalidates old and new targets",
          "[integration][db][upsert][cross-invalidation]")
{
    TransactionGuard tx;
    using Src = InvalidatingTestAssignedKeyRepo;
    TestInternals::resetEntityCacheState<Src>();
    TestInternals::resetEntityCacheState<L1UpsertInvTargetRepo>();

    // Establish the source row with payload → target 5000 (pre-image key).
    sync(Src::insert(makeTestAssignedKey(1, 5000, "a")));

    // Seed both the old (5000) and new (6000) target entries AFTER the insert, so
    // the insert's own propagateCreate cannot pre-empt the assertion.
    TestInternals::putInCache<L1UpsertInvTargetRepo>(int64_t{5000},
        makeTestItem("old", 0, "", true, 5000));
    TestInternals::putInCache<L1UpsertInvTargetRepo>(int64_t{6000},
        makeTestItem("new", 0, "", true, 6000));
    REQUIRE(TestInternals::getFromCache<L1UpsertInvTargetRepo>(int64_t{5000}));
    REQUIRE(TestInternals::getFromCache<L1UpsertInvTargetRepo>(int64_t{6000}));

    // Present key, payload 5000 → 6000 → propagateUpdate(old, new): both targets
    // drop. Missing the old key would strand a stale foreign reference.
    sync(Src::upsert(makeTestAssignedKey(1, 6000, "b")));

    CHECK_FALSE(TestInternals::getFromCache<L1UpsertInvTargetRepo>(int64_t{5000}));
    CHECK_FALSE(TestInternals::getFromCache<L1UpsertInvTargetRepo>(int64_t{6000}));
}
