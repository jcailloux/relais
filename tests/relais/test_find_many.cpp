/**
 * test_find_many.cpp
 * Multi-id batched read (findMany) across the mixin chain.
 *
 * Étape 1 covers the L2 MGET primitive (RedisCache::mgetRaw / mget<E>):
 *   - request-order preservation, nil holes, empty input (no I/O)
 *   - binary-safety (BEVE bytes survive the raw round-trip)
 *   - typed JSON deserialization
 * Later étapes extend this file with [pg], [l2], composite-key and partition-key
 * coverage of findManyRaw / findMany.
 *
 * SECTION tags:
 *   [mget]            — RedisCache MGET primitive
 *   [findmany]        — public/raw findMany surface (later étapes)
 *   [composite-key]   — multi-column matching
 *   [partition-key]   — cross-partition ANY
 */

#include <optional>
#include <span>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

#include "jcailloux/relais/cache/RedisCache.h"
#include "jcailloux/relais/PgProvider.h"

using namespace relais_test;

namespace {

io::Task<void> setKey(std::string key, std::string value) {
    co_await jcailloux::relais::PgProvider::redis("SET", std::move(key), std::move(value));
}

io::Task<std::vector<std::optional<std::string>>> mgetRawKeys(std::vector<std::string> keys) {
    co_return co_await cache::RedisCache::mgetRaw(keys);
}

}  // namespace

TEST_CASE("mgetRaw handles nil and empty input", "[findmany][mget]") {
    TransactionGuard guard;

    SECTION("empty keys → empty result, no I/O") {
        auto out = sync(mgetRawKeys({}));
        REQUIRE(out.empty());
    }

    SECTION("all absent → one nullopt per key") {
        auto out = sync(mgetRawKeys({"mget:none:0", "mget:none:1"}));
        REQUIRE(out.size() == 2);
        REQUIRE(out[0] == std::nullopt);
        REQUIRE(out[1] == std::nullopt);
    }
}

TEST_CASE("mgetRaw round-trips N keys in request order with holes", "[findmany][mget]") {
    TransactionGuard guard;

    sync(setKey("mget:k0", "v0"));
    sync(setKey("mget:k1", "v1"));
    // mget:k2 deliberately absent
    sync(setKey("mget:k3", "v3"));
    sync(setKey("mget:k4", "v4"));

    auto out = sync(mgetRawKeys(
        {"mget:k0", "mget:k1", "mget:k2", "mget:k3", "mget:k4"}));

    REQUIRE(out.size() == 5);
    REQUIRE(out[0] == "v0");
    REQUIRE(out[1] == "v1");
    REQUIRE(out[2] == std::nullopt);   // hole at the exact requested position
    REQUIRE(out[3] == "v3");
    REQUIRE(out[4] == "v4");
}

TEST_CASE("mgetRaw is binary-safe (BEVE payload survives)", "[findmany][mget]") {
    TransactionGuard guard;

    auto user = makeTestUser("bob", "bob@example.com", 50, 2);
    auto bytes = user.binary();
    REQUIRE_FALSE(bytes.empty());

    sync(setKey("mget:beve:2",
                std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size())));

    auto out = sync(mgetRawKeys({"mget:beve:2"}));
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].has_value());

    auto decoded = TestUserEntity::fromBinary(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(out[0]->data()), out[0]->size()));
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->username == "bob");
    REQUIRE(decoded->balance == 50);
}

TEST_CASE("mget<E> deserializes present JSON slots", "[findmany][mget]") {
    TransactionGuard guard;

    auto alice = makeTestUser("alice", "alice@example.com", 100, 1);
    sync(setKey("mget:json:1", alice.json()));
    // mget:json:absent left unset

    auto out = sync([]() -> io::Task<std::vector<std::optional<TestUserEntity>>> {
        std::vector<std::string> keys = {"mget:json:1", "mget:json:absent"};
        co_return co_await cache::RedisCache::mget<TestUserEntity>(keys);
    }());

    REQUIRE(out.size() == 2);
    REQUIRE(out[0].has_value());
    REQUIRE(out[0]->username == "alice");
    REQUIRE(out[0]->balance == 100);
    REQUIRE(out[1] == std::nullopt);
}

// ---------------------------------------------------------------------------
// Étape 2 — PgRepo::findManyRaw via WHERE pk = ANY($1)
// findManyRaw is internal (like findRaw); reached through TestInternals on
// Uncached* repos (no L1/L2 — exercises the pure L3 ANY path).
// ---------------------------------------------------------------------------

namespace {

template<typename Repo, typename Key>
auto findManyRawSync(std::span<const Key> ids) {
    return sync(relais_test::TestInternals::findManyRaw<Repo>(ids));
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
