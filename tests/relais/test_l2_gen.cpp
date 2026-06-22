/**
 * test_l2_gen.cpp
 *
 * Cross-instance L2 generation primitives (commit 12b). When one Redis is
 * shared by several processes the read-fill recheck's authority moves to a
 * sharded generation hash {repo}:l2gen. These tests pin the contract of the
 * RedisCache primitives behind that path — deterministic, single-threaded:
 *   - getGen / bumpGen: an absent slot reads 0; HINCRBY moves it monotonically;
 *   - setIfGen: writes IFF the slot's gen still equals the snapshot — the
 *     central straddle guarantee. A fill carrying a pre-bump snapshot is
 *     rejected, so a value straddling another instance's invalidation never
 *     lands, with no compensating UNLINK (check + SET share one EVAL);
 *   - setManyIfGen: same, per entry, in one batch (partial accept);
 *   - bumpGenMany: bumps every listed slot.
 * Plus a repo-level smoke that an l2_shared_across_instances repo round-trips
 * find → erase through the Redis-side gen path.
 */

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "jcailloux/relais/cache/RedisCache.h"

using namespace relais_test;

namespace {

const std::string kGen = "test:gen:unit:l2gen";

io::Task<std::optional<std::string>> getKey(std::string key) {
    co_return co_await cache::RedisCache::getRaw(key);
}

}  // namespace

TEST_CASE("[l2gen] absent slot reads 0; bumpGen is monotonic",
          "[integration][db][redis][l2gen]")
{
    TransactionGuard tx;
    REQUIRE(sync(cache::RedisCache::getGen(kGen, 7)) == 0);
    sync(cache::RedisCache::bumpGen(kGen, 7));
    REQUIRE(sync(cache::RedisCache::getGen(kGen, 7)) == 1);
    sync(cache::RedisCache::bumpGen(kGen, 7));
    REQUIRE(sync(cache::RedisCache::getGen(kGen, 7)) == 2);
    // A different slot is independent.
    REQUIRE(sync(cache::RedisCache::getGen(kGen, 8)) == 0);
}

TEST_CASE("[l2gen] setIfGen writes only when the gen still matches the snapshot",
          "[integration][db][redis][l2gen]")
{
    TransactionGuard tx;
    const std::string key = "test:gen:unit:e1";
    const std::size_t slot = 3;

    SECTION("matching snapshot -> fill lands") {
        int64_t snap = sync(cache::RedisCache::getGen(kGen, slot));
        bool ok = sync(cache::RedisCache::setIfGen(kGen, key, slot, snap, "payload-A", 60));
        REQUIRE(ok);
        REQUIRE(sync(getKey(key)) == "payload-A");
    }

    SECTION("straddle: a bump after the snapshot rejects the fill") {
        int64_t snap = sync(cache::RedisCache::getGen(kGen, slot));
        // Simulate another instance invalidating the key mid-fetch.
        sync(cache::RedisCache::bumpGen(kGen, slot));
        bool ok = sync(cache::RedisCache::setIfGen(kGen, key, slot, snap, "stale-A", 60));
        REQUIRE_FALSE(ok);
        REQUIRE_FALSE(sync(getKey(key)).has_value());  // nothing written
    }
}

TEST_CASE("[l2gen] setManyIfGen accepts only the unbumped entries",
          "[integration][db][redis][l2gen]")
{
    TransactionGuard tx;
    using GenFill = cache::RedisCache::GenFill;

    const std::string kA = "test:gen:unit:a";
    const std::string kB = "test:gen:unit:b";
    const std::string kC = "test:gen:unit:c";

    int64_t sA = sync(cache::RedisCache::getGen(kGen, 1));
    int64_t sB = sync(cache::RedisCache::getGen(kGen, 2));
    int64_t sC = sync(cache::RedisCache::getGen(kGen, 3));

    // B is invalidated after its snapshot → its fill must be rejected.
    sync(cache::RedisCache::bumpGen(kGen, 2));

    std::vector<GenFill> entries{
        {kA, 1, sA, "VA"},
        {kB, 2, sB, "VB"},
        {kC, 3, sC, "VC"},
    };
    size_t written = sync(cache::RedisCache::setManyIfGen(
        kGen, std::span<const GenFill>(entries), 60));

    REQUIRE(written == 2);
    REQUIRE(sync(getKey(kA)) == "VA");
    REQUIRE_FALSE(sync(getKey(kB)).has_value());
    REQUIRE(sync(getKey(kC)) == "VC");
}

TEST_CASE("[l2gen] bumpGenMany bumps every listed slot",
          "[integration][db][redis][l2gen]")
{
    TransactionGuard tx;
    std::vector<std::size_t> slots{4, 5, 6};
    sync(cache::RedisCache::bumpGenMany(kGen, std::span<const std::size_t>(slots)));
    REQUIRE(sync(cache::RedisCache::getGen(kGen, 4)) == 1);
    REQUIRE(sync(cache::RedisCache::getGen(kGen, 5)) == 1);
    REQUIRE(sync(cache::RedisCache::getGen(kGen, 6)) == 1);
    REQUIRE(sync(cache::RedisCache::getGen(kGen, 7)) == 0);  // untouched
}

TEST_CASE("[l2gen] shared-instance repo round-trips find/erase",
          "[integration][db][redis][l2gen]")
{
    TransactionGuard tx;
    auto id = insertTestItem("gen_smoke", 123);

    // find warms L2 via the conditional fill (no concurrent mutation → lands).
    auto v = sync(SharedL2TestItemRepo::find(id));
    REQUIRE(v != nullptr);

    auto key = SharedL2TestItemRepo::makeRedisKey(id);
    REQUIRE(sync(getKey(key)).has_value());   // present in L2

    // A second find is now an L2 hit (still present, value intact).
    REQUIRE(sync(SharedL2TestItemRepo::find(id)) != nullptr);

    // erase HINCRBYs the gen then UNLINKs the entity.
    REQUIRE(sync(SharedL2TestItemRepo::erase(id)) == 1);
    REQUIRE_FALSE(sync(getKey(key)).has_value());
    REQUIRE(sync(SharedL2TestItemRepo::find(id)) == nullptr);
}
