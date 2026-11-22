/**
 * test_chunk_map.cpp
 * Pure unit tests for ChunkMap mutations — no DB, no event loop.
 *
 * The contract under test:
 *   - remove_if tests and removes in one step: a failing predicate leaves the
 *     entry in place (same entry, still counted), a ghost is never removed;
 *   - update_ghost on an absent key leaves no placeholder behind;
 *   - size() counts the real entries only, whatever sequence of ghost, real
 *     and placeholder states a key goes through, including under concurrency;
 *   - a scan visits the keys of the range it computed, each once, though the
 *     table grows between that computation and its reads: a chunk scan its
 *     chunk's keys (a prefix of their hash) and no other, a whole-table scan
 *     every key;
 *   - removals raced against inserts that grow the table keep the live count
 *     exact (run under the sanitizers of the build).
 *
 * Maps are function-local statics: ChunkMap never frees its table (it is meant
 * to be a static singleton), so a static keeps it reachable for leak checkers.
 */

#include <cstdint>
#include <map>
#include <random>
#include <thread>
#include <variant>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "jcailloux/relais/cache/ChunkMap.h"
#include "fixtures/RelaisTestAccessors.h"

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

using jcailloux::relais::cache::detail::ChunkMap;
using jcailloux::relais::cache::detail::TaggedEntry;

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

// =============================================================================
// Scans across a growth of the table
//
// A growth splits each bucket of a version into several buckets of the next:
// bucket i of the larger version holds other keys than bucket i of the
// smaller. The hook grows the table once the scan has computed its range and
// before it reads a bucket. Keys 0..kKeys-1 are stored; the growth's fillers
// are negative keys, removed before the scan reads.
// =============================================================================

namespace {

using TI = relais_test::TestInternals;

constexpr long kChunks = 8;
constexpr int kChunkBits = 3;
constexpr int64_t kKeys = 2000;

/// A key's chunk: the prefix of its hash, as the list cache and the row notes
/// compute it.
long chunkOf(int64_t k) {
    return static_cast<long>((Map::get_hash(Map::make_key(k)) >> (48 - kChunkBits)) & (kChunks - 1));
}

int64_t filler(long i) { return -1 - i; }

void fill(Map& map) {
    for (int64_t k = 0; k < kKeys; ++k) map.upsert(k, 0);
}

/// Grows the table at the first scan of `chunk` (-1: a whole-table scan);
/// `buckets` receives the count after it.
void growAtScanOf(Map& map, long chunk, bool& grown, long& buckets) {
    TI::onScanStart(map, [&map, chunk, &grown, &buckets](long scanned) {
        if (grown || scanned != chunk) return;
        grown = true;
        buckets = TI::growTable(map, filler, 0);
    });
}

struct Visits {
    long missed = 0;   // keys of the range not visited
    long foreign = 0;  // keys visited outside it (or fillers)
    long twice = 0;    // keys visited more than once
};

template<typename InRange>
Visits visits(const std::map<int64_t, int>& seen, InRange&& in_range) {
    Visits v;
    for (int64_t k = 0; k < kKeys; ++k)
        if (in_range(k) && !seen.contains(k)) ++v.missed;
    for (const auto& [k, n] : seen) {
        if (k < 0 || !in_range(k)) ++v.foreign;
        if (n > 1) ++v.twice;
    }
    return v;
}

}  // namespace

TEST_CASE("ChunkMap - a chunk scan visits its chunk's keys though the table grows meanwhile",
          "[chunkmap][growth]") {
    static Map map;
    fill(map);
    // Grown under chunk 0, the larger version's first buckets would hold a
    // quarter of its keys; under chunk 5, keys of another chunk.
    for (long chunk : {0L, 5L}) {
        const long before = map.num_buckets();
        REQUIRE(before >= kChunks);
        bool grown = false;
        long after = 0;
        growAtScanOf(map, chunk, grown, after);
        std::map<int64_t, int> seen;
        map.cleanup_chunk(chunk, kChunks, [&](const int64_t& k, TaggedEntry) {
            ++seen[k];
            return false;
        });
        TI::onScanStart(map, {});
        REQUIRE(grown);
        REQUIRE(after > before);

        auto v = visits(seen, [chunk](int64_t k) { return chunkOf(k) == chunk; });
        INFO("chunk " << chunk << ", " << before << " -> " << after << " buckets");
        CHECK(seen.size() > 0);
        CHECK(v.missed == 0);
        CHECK(v.foreign == 0);
        CHECK(v.twice == 0);
    }
}

TEST_CASE("ChunkMap - a whole-table scan visits every key though the table grows meanwhile",
          "[chunkmap][growth]") {
    static Map map;
    fill(map);
    const long before = map.num_buckets();
    bool grown = false;
    long after = 0;
    growAtScanOf(map, -1, grown, after);
    std::map<int64_t, int> seen;
    auto all = [](int64_t) { return true; };

    SECTION("for_each") {
        map.for_each([&](const int64_t& k, TaggedEntry) { ++seen[k]; });
        TI::onScanStart(map, {});
        REQUIRE(grown);
        REQUIRE(after > before);
        auto v = visits(seen, all);
        CHECK(v.missed == 0);
        CHECK(v.foreign == 0);
        CHECK(v.twice == 0);
    }

    SECTION("full_cleanup removes every key its predicate selects") {
        map.full_cleanup([&](const int64_t& k, TaggedEntry) {
            ++seen[k];
            return k % 2 == 0;
        });
        TI::onScanStart(map, {});
        REQUIRE(grown);
        REQUIRE(after > before);
        auto v = visits(seen, all);
        CHECK(v.missed == 0);
        CHECK(v.foreign == 0);
        CHECK(v.twice == 0);
        long even_left = 0, odd_gone = 0;
        for (int64_t k = 0; k < kKeys; ++k) {
            const bool present = static_cast<bool>(map.find(k));
            if (k % 2 == 0 && present) ++even_left;
            if (k % 2 != 0 && !present) ++odd_gone;
        }
        CHECK(even_left == 0);
        CHECK(odd_gone == 0);
    }
}

// Removals raced against inserts that keep growing the table, from a table of
// a few buckets. A removal joins the copy of the bucket it targets, walking
// the overflow links of the buckets it copies while writers rewrite them and
// retire their links: the walk must run within an epoch, or the links it
// reads may be freed under it. That window is too narrow to hit on demand;
// the case runs the path under the sanitizers of the build, and checks that
// once joined, the live count matches the entries present.
TEST_CASE("ChunkMap - removals raced against growth read no freed block",
          "[chunkmap][concurrency][growth]") {
    static Map map(1);
    constexpr int kWriters = 2;
    constexpr int kRemovers = 2;
    constexpr int64_t kPerWriter = 100'000;
    std::atomic<int> writing{kWriters};

    std::vector<std::thread> threads;
    for (int t = 0; t < kWriters; ++t) {
        threads.emplace_back([t, &writing] {
            const int64_t base = t * kPerWriter;
            for (int64_t i = 0; i < kPerWriter; ++i) {
                map.upsert(base + i, t);
                if (i % 4 == 3) map.remove(base + i / 2);
            }
            writing.fetch_sub(1, std::memory_order_release);
        });
    }
    for (int r = 0; r < kRemovers; ++r) {
        threads.emplace_back([r, &writing] {
            std::mt19937_64 rng(static_cast<uint64_t>(r) * 104729u + 7u);
            std::uniform_int_distribution<int64_t> key(0, kWriters * kPerWriter - 1);
            while (writing.load(std::memory_order_acquire) > 0) map.remove(key(rng));
        });
    }
    for (auto& th : threads) th.join();

    long present = 0;
    for (int64_t k = 0; k < kWriters * kPerWriter; ++k)
        if (map.find(k)) ++present;
    CHECK(map.size() == present);
    CHECK(map.num_buckets() > 4);
}
