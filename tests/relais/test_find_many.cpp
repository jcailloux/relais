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
