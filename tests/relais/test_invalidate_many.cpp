/**
 * test_invalidate_many.cpp
 * Variadic batch eviction at L2 (RedisCache::invalidateMany) + its chunk splitter.
 *
 * Étape 3 du plan erase/invalidate batch :
 *   - rdetail::chunkSpan(span, K) — pure splitter: partitions a key set into
 *     subspans of ≤ K, covering the input exactly (no loss, no overlap).
 *   - RedisCache::invalidateMany(span<string>) → UNLINK k1…kN sub-chunked at
 *     kInvalidateChunk (1k). One primitive, ⌈N/K⌉ round-trips, idempotent.
 *
 * SECTION tags:
 *   [splitter]   — pure chunk partition (no DB, no Redis)
 *   [redis]      — UNLINK batch over a live Redis
 *   [chunking]   — set > K_redis converges in ⌈N/K⌉ commands
 */

#include <span>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"

#include <jcailloux/relais/detail/Chunk.h>
#include <jcailloux/relais/cache/RedisCache.h>

using namespace relais_test;
namespace rdetail = jcailloux::relais::detail;
namespace cache = jcailloux::relais::cache;

// ---------------------------------------------------------------------------
// §1 — pure splitter: union of chunks == input, no loss, no overlap
// ---------------------------------------------------------------------------

namespace {

// Assert that chunkSpan(items, K) exactly tiles `items`: every chunk ≤ K, the
// concatenation reproduces the input in order, count == ⌈N/K⌉.
void assertExactTiling(size_t n, size_t k) {
    std::vector<int> items(n);
    for (size_t i = 0; i < n; ++i) items[i] = static_cast<int>(i);

    auto chunks = rdetail::chunkSpan(std::span<const int>(items), k);

    // Count is the ceiling division (0 chunks for an empty input).
    const size_t expected = (n == 0) ? 0 : (n + k - 1) / k;
    REQUIRE(chunks.size() == expected);

    size_t seen = 0;
    for (size_t c = 0; c < chunks.size(); ++c) {
        REQUIRE_FALSE(chunks[c].empty());
        REQUIRE(chunks[c].size() <= k);
        // Only the last chunk may be short.
        if (c + 1 < chunks.size()) REQUIRE(chunks[c].size() == k);
        for (int v : chunks[c]) {
            REQUIRE(v == static_cast<int>(seen));  // contiguous, in order
            ++seen;
        }
    }
    REQUIRE(seen == n);  // no loss, no overlap
}

}  // namespace

TEST_CASE("chunkSpan tiles a key set exactly at the boundary sizes",
          "[splitter]") {
    // K_redis (1k) and K_pg (10k): the splitter is K-parametric, shared L2/L3.
    for (size_t k : {size_t{1000}, size_t{10000}}) {
        for (size_t n : {size_t{0}, size_t{1}, k - 1, k, k + 1, 2 * k}) {
            assertExactTiling(n, k);
        }
    }
}

TEST_CASE("chunkSpan degenerate ceilings", "[splitter]") {
    std::vector<int> items = {1, 2, 3};

    SECTION("chunk == 0 → empty (no progress possible)") {
        REQUIRE(rdetail::chunkSpan(std::span<const int>(items), 0).empty());
    }
    SECTION("empty input → empty regardless of K") {
        std::vector<int> none;
        REQUIRE(rdetail::chunkSpan(std::span<const int>(none), 1000).empty());
    }
    SECTION("K ≥ N → single chunk") {
        auto chunks = rdetail::chunkSpan(std::span<const int>(items), 1000);
        REQUIRE(chunks.size() == 1);
        REQUIRE(chunks[0].size() == 3);
    }
}

// ---------------------------------------------------------------------------
// §5 — invalidateMany over a live Redis
// ---------------------------------------------------------------------------

namespace {

// Seed `n` distinct raw string keys under a prefix; returns the key list.
std::vector<std::string> seedKeys(const std::string& prefix, size_t n) {
    std::vector<std::string> keys;
    keys.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        std::string k = prefix + std::to_string(i);
        sync(jcailloux::relais::PgProvider::redis("SET", k, "v"));
        keys.push_back(std::move(k));
    }
    return keys;
}

int64_t redisExists(const std::string& key) {
    return sync(jcailloux::relais::PgProvider::redis("EXISTS", key)).asInteger();
}

}  // namespace

TEST_CASE("invalidateMany evicts the whole enumerated set", "[redis]") {
    TransactionGuard guard;

    auto keys = seedKeys("im:basic:", 5);
    for (const auto& k : keys) REQUIRE(redisExists(k) == 1);

    bool ok = sync(cache::RedisCache::invalidateMany(
        std::span<const std::string>(keys)));
    REQUIRE(ok);

    for (const auto& k : keys) REQUIRE(redisExists(k) == 0);
}

TEST_CASE("invalidateMany is idempotent on absent keys", "[redis]") {
    TransactionGuard guard;

    // Mix of present and never-set keys: UNLINK silently skips the absent ones.
    auto present = seedKeys("im:idem:", 3);
    std::vector<std::string> keys = present;
    keys.push_back("im:idem:absent:a");
    keys.push_back("im:idem:absent:b");

    bool ok = sync(cache::RedisCache::invalidateMany(
        std::span<const std::string>(keys)));
    REQUIRE(ok);
    for (const auto& k : present) REQUIRE(redisExists(k) == 0);

    // Re-running on an already-drained set still succeeds (no error).
    REQUIRE(sync(cache::RedisCache::invalidateMany(
        std::span<const std::string>(keys))));
}

TEST_CASE("invalidateMany empty set is a no-op success", "[redis]") {
    TransactionGuard guard;
    std::vector<std::string> none;
    REQUIRE(sync(cache::RedisCache::invalidateMany(
        std::span<const std::string>(none))));
}

TEST_CASE("invalidateMany converges past K_redis in ⌈N/K⌉ commands",
          "[redis][chunking]") {
    TransactionGuard guard;

    // kInvalidateChunk = 1000. Seed 2001 keys → 3 UNLINK commands (1000+1000+1).
    static_assert(cache::RedisCache::kInvalidateChunk == 1000);
    auto keys = seedKeys("im:bulk:", 2001);

    bool ok = sync(cache::RedisCache::invalidateMany(
        std::span<const std::string>(keys)));
    REQUIRE(ok);

    // Every key drained across the chunks; spot-check the boundaries.
    REQUIRE(redisExists(keys.front()) == 0);
    REQUIRE(redisExists(keys[999]) == 0);
    REQUIRE(redisExists(keys[1000]) == 0);
    REQUIRE(redisExists(keys[2000]) == 0);
}
