/**
 * test_chunk_map.cpp
 * Pure unit tests for ChunkMap mutations — no DB, no Redis, no event loop.
 *
 * The contract under test:
 *   - remove_if tests and removes in one step: a failing predicate leaves the
 *     entry in place (same entry, still counted), a ghost is never removed;
 *   - update_ghost on an absent key leaves no placeholder behind;
 *   - size() counts the real entries only, whatever sequence of ghost, real
 *     and placeholder states a key goes through, including under concurrency.
 *
 * Maps are function-local statics: ChunkMap never frees its table (it is meant
 * to be a static singleton), so a static keeps it reachable for leak checkers.
 */

#include <cstdint>
#include <random>
#include <thread>
#include <variant>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "jcailloux/relais/cache/ChunkMap.h"

// parlayhash's big_atomic copies a bucket state under a sequence lock and
// retries when the copy raced a writer; TSan cannot see the retry and flags the
// copy. The suppressions match parlayhash frames only: a race in relais code
// carries relais frames and is still reported.
#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#    define RELAIS_TSAN_BUILD 1
#  endif
#endif
#if defined(__SANITIZE_THREAD__)
#  define RELAIS_TSAN_BUILD 1
#endif

#ifdef RELAIS_TSAN_BUILD
extern "C" const char* __tsan_default_suppressions() {
    return
        "race:parlay::parlay_hash\n"
        "race:parlay::DirectEntries\n";
}
#endif

using jcailloux::relais::cache::ChunkMap;
using jcailloux::relais::cache::TaggedEntry;

namespace {

/// Values are the id of the thread (or test step) that stored them.
using Map = ChunkMap<int64_t, int, std::monostate, /*HasGhost=*/true>;

int valueOf(const Map::EntryHeader* h) {
    return static_cast<const Map::CacheEntry*>(h)->value;
}

auto ownedBy(int owner) {
    return [owner](const Map::EntryHeader* h) { return valueOf(h) == owner; };
}

TaggedEntry bumpGhost(TaggedEntry g) {
    return TaggedEntry::fromGhost(g.ghostCount() + 1, g.ghostBytes(), g.ghostFlags());
}

}  // namespace

TEST_CASE("ChunkMap - remove_if removes only when the predicate holds", "[chunkmap]") {
    static Map map;
    map.upsert(1, 10);
    auto* stored = map.find(1).asReal();
    REQUIRE(stored);

    CHECK_FALSE(map.remove_if(1, ownedBy(11)));
    auto kept = map.find(1);
    CHECK(kept.asReal() == stored);   // the entry stays
    CHECK(map.size() == 1);

    CHECK(map.remove_if(1, ownedBy(10)));
    CHECK_FALSE(map.find(1));
    CHECK(map.size() == 0);

    CHECK_FALSE(map.remove_if(1, ownedBy(10)));   // absent
}

TEST_CASE("ChunkMap - remove_if never removes a ghost", "[chunkmap]") {
    static Map map;
    REQUIRE(map.insert_ghost(1, 1, 64, 0));
    bool called = false;
    CHECK_FALSE(map.remove_if(1, [&](const Map::EntryHeader*) { return called = true; }));
    CHECK_FALSE(called);
    CHECK(map.find(1).isGhost());
    CHECK(map.size() == 0);
}

TEST_CASE("ChunkMap - update_ghost leaves no placeholder on an absent key", "[chunkmap]") {
    static Map map;
    map.update_ghost(1, bumpGhost);
    CHECK_FALSE(map.find(1));
    CHECK(map.totalEntries() == 0);

    REQUIRE(map.insert_ghost(2, 1, 64, 0));
    map.update_ghost(2, bumpGhost);
    CHECK(map.find(2).ghostCount() == 2);
}

TEST_CASE("ChunkMap - size counts real entries across ghost transitions", "[chunkmap]") {
    static Map map;
    REQUIRE(map.insert_ghost(1, 1, 64, 0));
    CHECK(map.size() == 0);
    map.upsert(1, 1);              // ghost → real
    CHECK(map.size() == 1);
    map.upsert(1, 2);              // real → real
    CHECK(map.size() == 1);
    CHECK(map.remove(1));
    CHECK(map.size() == 0);
}

// Threads race every mutation on a few keys. Once they are joined, the live
// count must match the real entries present, and no empty placeholder may
// remain. A removal that is not atomic, or a placeholder cleanup that drops a
// concurrent real entry, breaks one of the two.
TEST_CASE("ChunkMap - concurrent mutations keep the live count exact", "[chunkmap][concurrency]") {
    static Map map;
    constexpr int kThreads = 4;
    constexpr int kKeys = 16;
    constexpr int kOps = 20'000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([t] {
            std::mt19937 rng(static_cast<unsigned>(t) * 7919u + 1u);
            std::uniform_int_distribution<int> key(0, kKeys - 1);
            std::uniform_int_distribution<int> op(0, 4);
            for (int i = 0; i < kOps; ++i) {
                const int64_t k = key(rng);
                switch (op(rng)) {
                    case 0: map.upsert(k, t); break;
                    case 1: map.remove_if(k, ownedBy(t)); break;
                    case 2: map.remove(k); break;
                    case 3: map.update_ghost(k, bumpGhost); break;
                    case 4: map.insert_ghost(k, 1, 64, 0); break;
                }
            }
        });
    }
    for (auto& th : threads) th.join();

    long real = 0, present = 0;
    for (int64_t k = 0; k < kKeys; ++k) {
        auto r = map.find(k);
        if (r) ++present;
        if (r.asReal()) ++real;
    }
    CHECK(map.size() == real);
    CHECK(map.totalEntries() == present);
}
