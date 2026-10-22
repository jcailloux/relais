/**
 * test_write_affinity.cpp
 *
 * Which row a write declares to the scheduler (io::batch::detail::WriteAffinity), and
 * what each repository write declares. No database: the write provider is
 * replaced by one that records the declaration and answers an empty result.
 */

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/batch/WriteAffinity.h>

#include "fixtures/TestRepositories.h"
#include "fixtures/generated/TestLateKeyEntity.h"

using namespace relais_test;

namespace jr = jcailloux::relais;
namespace batch = jr::io::batch;
using batch::detail::WriteAffinity;
using jr::io::PgParams;
using jr::entity::set;
using jr::entity::increment;
using jr::entity::when;
using jr::entity::eq;

using SlotF = TestSlotEntity::Field;
using ::entity::generated::TestLateKeyEntity;
using UncachedTestLateKeyRepo = Repo<TestLateKeyEntity, "test:latekey:uncached", cfg::Uncached>;

namespace {

/// Hash of the row whose key fills `params` from `first`, in `table`.
uint64_t hashAt(std::string_view table, const PgParams& params, size_t first, size_t count) {
    return batch::detail::rowHash(batch::detail::rowAt(batch::detail::tableTag(table), first, count), params);
}

uint64_t hashOf(std::string_view table, const auto&... key) {
    return hashAt(table, PgParams::make(key...), 0, sizeof...(key));
}

/// Records each write's declaration and answers it with an empty result, so a
/// repository call completes inline.
struct WriteRecorder {
    struct Write {
        WriteAffinity affinity;
        PgParams params;
        uint64_t hash() const { return batch::detail::rowHash(affinity, params); }
    };
    std::vector<Write> writes;

    WriteRecorder() {
        jr::PgProvider::pg_write_ = [this](const char*, const PgParams& params,
                                           batch::WriteMode, WriteAffinity affinity) {
            writes.push_back({affinity, params});
            return jr::io::Task<batch::PgWriteResult>::fromValue(batch::PgWriteResult{});
        };
    }
    ~WriteRecorder() { jr::PgProvider::reset(); }

    /// Run `task` to completion and return the one write it submitted (a
    /// provider call may submit before the task is awaited).
    template<typename Awaitable>
    Write only(Awaitable task) {
        bool done = false;
        [](Awaitable t, bool& d) -> jr::io::DetachedTask {
            co_await std::move(t);
            d = true;
        }(std::move(task), done);
        REQUIRE(done);
        REQUIRE(writes.size() == 1);
        auto w = std::move(writes.front());
        writes.clear();
        return w;
    }
};

}  // namespace

// =============================================================================
// The descriptor and its hash
// =============================================================================

TEST_CASE("WriteAffinity: a row is named by its table and key text", "[affinity]") {
    SECTION("the key type does not matter, only its text") {
        CHECK(hashOf("t", int32_t{42}) == hashOf("t", int64_t{42}));
        CHECK(hashOf("t", int16_t{-7}) == hashOf("t", int64_t{-7}));
        CHECK(hashOf("t", std::string("abc")) == hashOf("t", std::string_view("abc")));
    }

    SECTION("the key is read where it sits") {
        CHECK(hashAt("t", PgParams::make(std::string("x"), int64_t{5}, int64_t{6}), 1, 2)
              == hashOf("t", int64_t{5}, int64_t{6}));
        CHECK(hashAt("t", PgParams::make(int64_t{5}, std::string("x")), 0, 1)
              == hashOf("t", int64_t{5}));
    }

    SECTION("a composite key keeps its components apart and in order") {
        CHECK(hashOf("t", int64_t{1}, int64_t{23}) != hashOf("t", int64_t{12}, int64_t{3}));
        CHECK(hashOf("t", int64_t{1}, int64_t{2}) != hashOf("t", int64_t{2}, int64_t{1}));
        CHECK(hashOf("t", std::string("ab"), std::string(""))
              != hashOf("t", std::string("a"), std::string("b")));
    }

    SECTION("NULL is not the empty text") {
        CHECK(hashOf("t", nullptr) != hashOf("t", std::string("")));
        CHECK(hashOf("t", std::optional<int64_t>{5}) == hashOf("t", int64_t{5}));
    }

    SECTION("different tables, same key: different values") {
        CHECK(batch::detail::tableTag("relais_test_items") != batch::detail::tableTag("relais_test_users"));
        CHECK(hashOf("relais_test_items", int64_t{1}) != hashOf("relais_test_users", int64_t{1}));
    }

    SECTION("a row always hashes to non-zero") {
        for (int64_t k = -1000; k <= 1000; ++k)
            REQUIRE(hashOf("t", k) != 0);
    }

    SECTION("what is not a row hashes to zero: the write stays ordered") {
        const auto params = PgParams::make(int64_t{1});
        CHECK(batch::detail::rowHash(batch::detail::kOrdered, params) == 0);
        CHECK(batch::detail::rowHash(batch::detail::kFree, params) == 0);
        CHECK(hashAt("t", params, 1, 1) == 0);   // beyond the parameters
        CHECK(hashAt("t", params, 0, 2) == 0);
    }

    SECTION("positions that do not fit fall back to kOrdered") {
        CHECK(batch::detail::rowAt(1, 0, 0) == batch::detail::kOrdered);
        CHECK(batch::detail::rowAt(1, 256, 1) == batch::detail::kOrdered);
        CHECK(batch::detail::rowAt(1, 0, WriteAffinity::kFreeCount) == batch::detail::kOrdered);
        CHECK(batch::detail::rowAt(1, 255, 2).row());
    }

    SECTION("the default is kOrdered") {
        CHECK(WriteAffinity{} == batch::detail::kOrdered);
        CHECK(batch::detail::kOrdered.ordered());
        CHECK(batch::detail::kFree.free());
    }
}

TEST_CASE("WriteAffinity: an insert binds an assigned key first", "[affinity]") {
    using M = ::entity::generated::TestLateKeyMapping;
    STATIC_CHECK(M::insert_assigns_pk);
    CHECK(std::string_view(M::SQL::insert).starts_with(
        "INSERT INTO relais_test_late_keys (group_id, key_id, note, seq)"));

    TestLateKeyEntity e;
    e.note = "n";
    e.group_id = 3;
    e.seq = 9;
    e.key_id = 4;
    const auto params = TestLateKeyEntity::toInsertParams(e);
    REQUIRE(params.params.size() == 4);
    CHECK(std::string_view(params.params[0].data()) == "3");
    CHECK(std::string_view(params.params[1].data()) == "4");

    STATIC_CHECK_FALSE(::entity::generated::TestItemMapping::insert_assigns_pk);
    STATIC_CHECK_FALSE(::entity::generated::TestSlotMapping::insert_assigns_pk);
    STATIC_CHECK(::entity::generated::TestAssignedKeyMapping::insert_assigns_pk);
    // A db_managed column outside the key does not matter.
    STATIC_CHECK(::entity::generated::TestMembershipMapping::insert_assigns_pk);
    STATIC_CHECK(::entity::generated::TestAllPkJunctionMapping::insert_assigns_pk);
}

// =============================================================================
// What each repository write declares
// =============================================================================

TEST_CASE("WriteAffinity: each repository write names its row", "[affinity]") {
    WriteRecorder rec;

    SECTION("insert with a database-assigned key: free") {
        CHECK(rec.only(UncachedTestItemRepo::insert(makeTestItem("a"))).affinity
              == batch::detail::kFree);
    }

    SECTION("every write on one assigned-key row names it alike") {
        const auto mine = hashOf("relais_test_assigned_keys", int64_t{7});
        CHECK(rec.only(UncachedTestAssignedKeyRepo::insert(makeTestAssignedKey(7))).hash() == mine);
        CHECK(rec.only(UncachedTestAssignedKeyRepo::upsert(makeTestAssignedKey(7))).hash() == mine);
        CHECK(rec.only(UncachedTestAssignedKeyRepo::update(7, makeTestAssignedKey(7, 1))).hash()
              == mine);
        CHECK(rec.only(UncachedTestAssignedKeyRepo::erase(7)).hash() == mine);
    }

    SECTION("a composite key declared out of order: insert agrees with the rest") {
        TestLateKeyEntity e;
        e.note = "n";
        e.group_id = 3;
        e.key_id = 4;
        const auto key = std::make_tuple(int64_t{3}, int64_t{4});
        const auto mine = hashOf("relais_test_late_keys", int64_t{3}, int64_t{4});
        CHECK(rec.only(UncachedTestLateKeyRepo::insert(e)).hash() == mine);
        CHECK(rec.only(UncachedTestLateKeyRepo::upsert(e)).hash() == mine);
        CHECK(rec.only(UncachedTestLateKeyRepo::update(key, e)).hash() == mine);
        CHECK(rec.only(UncachedTestLateKeyRepo::erase(key)).hash() == mine);
    }

    SECTION("composite key: insert, update and erase agree") {
        const auto key = std::make_tuple(int64_t{3}, int64_t{4});
        const auto mine = hashOf("relais_test_memberships", int64_t{3}, int64_t{4});
        CHECK(rec.only(UncachedTestMembershipRepo::insert(makeTestMembership(3, 4))).hash()
              == mine);
        CHECK(rec.only(UncachedTestMembershipRepo::update(key, makeTestMembership(3, 4, "r")))
                  .hash() == mine);
        CHECK(rec.only(UncachedTestMembershipRepo::erase(key)).hash() == mine);
    }

    SECTION("update, erase, patch and patchIf of one row agree") {
        const auto mine = hashOf("relais_test_slots", int64_t{11});
        CHECK(rec.only(UncachedTestSlotRepo::erase(11)).hash() == mine);
        CHECK(rec.only(UncachedTestSlotRepo::patch(11, increment<SlotF::counter>(1))).hash()
              == mine);
        CHECK(rec.only(UncachedTestSlotRepo::patch(
                  11, set<SlotF::counter>(int32_t{3}), set<SlotF::version>(int64_t{2})))
                  .hash() == mine);
        CHECK(rec.only(UncachedTestSlotRepo::patchIf(
                  11, when(eq<SlotF::version>(int64_t{1})), set<SlotF::counter>(int32_t{2})))
                  .hash() == mine);
        CHECK(rec.only(UncachedTestItemRepo::update(5, makeTestItem("b", 1, {}, true, 5))).hash()
              == hashOf("relais_test_items", int64_t{5}));
    }

    SECTION("several rows, or rows known only once it runs: ordered") {
        const std::vector<int64_t> ids{1, 2, 3};
        CHECK(rec.only(UncachedTestItemRepo::eraseMany(ids)).affinity == batch::detail::kOrdered);
        CHECK(rec.only(UncachedTestSlotRepo::patchWhere(
                  when(eq<SlotF::group_id>(int64_t{1})), increment<SlotF::counter>(1)))
                  .affinity == batch::detail::kOrdered);
        CHECK(rec.only(UncachedTestSlotRepo::claim(
                  when(eq<SlotF::group_id>(int64_t{1})), 2, set<SlotF::counter>(int32_t{1})))
                  .affinity == batch::detail::kOrdered);
    }

    SECTION("raw queryWrite: ordered") {
        PgParams params;
        CHECK(rec.only(jr::PgProvider::queryWrite("UPDATE t SET x = 1", params)).affinity
              == batch::detail::kOrdered);
    }
}
