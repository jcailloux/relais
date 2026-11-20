/**
 * test_ledger.cpp
 *
 * The L1 ledger counts every block the L1 holds, at the room it takes at the
 * allocator. Checked against the blocks themselves: the executable follows
 * each block its recording threads allocate, for the whole process, until any
 * thread frees it (ProcessBlocks.h, linked with --wrap=malloc,--wrap=calloc,
 * --wrap=free).
 * The tiers are driven directly, without a database, so that between two
 * readings the only blocks a recording thread allocates and keeps are the
 * L1's: the change of the ledger must equal the change of the room those
 * blocks take, exactly, at every phase. Every block of the L1 is recorded,
 * from the first tier built on, so that one freed during a phase is known
 * however old it is. Any block the ledger misses, or counts at the wrong time
 * (at retirement rather than at its release), shows up as a difference.
 *
 * Every block is weighed by the test from its usable size and the header it
 * knows for the allocator in use (TestAllocator.h). The values hold no
 * bucket array: the L1 weighs one at the class of its request, the one block
 * it cannot read (test_gdsf covers that rule).
 */

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <variant>
#include <vector>

#include <jcailloux/relais/cache/CacheMetadata.h>
#include <jcailloux/relais/cache/CacheTier.h>
#include <jcailloux/relais/cache/EntryLayout.h>
#include <jcailloux/relais/cache/L1Ledger.h>
#include <jcailloux/relais/list/ListCache.h>
#include <jcailloux/relais/list/ListWrapper.h>
#include <jcailloux/relais/repository/RowMatch.h>

#include "fixtures/ProcessBlocks.h"
#include "fixtures/RelaisTestAccessors.h"
#include "fixtures/TestAllocator.h"
#include "fixtures/generated/TestItemEntity.h"
#include "fixtures/generated/TestOrderEntity.h"

namespace {

using jcailloux::relais::cache::detail::CacheMetadata;
using jcailloux::relais::cache::detail::CacheTier;
using jcailloux::relais::cache::detail::EntryLayout;
using jcailloux::relais::cache::detail::L1Ledger;
using jcailloux::relais::cache::detail::PoolBlock;
using jcailloux::relais::cache::detail::TaggedEntry;
using jcailloux::relais::detail::blockBytes;
using jcailloux::relais::detail::heapBytes;
using relais_test::NotRecording;
using relais_test::ProcessBlocks;
using relais_test::Recording;
using relais_test::TestInternals;
using TestItemEntity = ::entity::generated::TestItemEntity;
using TestOrderEntity = ::entity::generated::TestOrderEntity;

// =============================================================================
// Values and tiers
// =============================================================================

/// A value under the rules of heapBytes the entities leave out.
struct Assorted {
    std::map<std::string, int64_t> by_name;
    std::set<int32_t> ids;
    std::unordered_map<std::string, int64_t> by_tag;  // its one bucket inside the object
    std::vector<bool> flags;
    std::array<std::string, 2> names;
    std::variant<int64_t, std::string> either;
    std::optional<std::vector<int64_t>> maybe;
};

/// A list page's filters: what its query copies into the page's metadata.
struct PageFilters {
    std::optional<int64_t> owner;
    std::optional<std::string> category;
    std::optional<std::vector<int64_t>> ids;
};

using PageQuery = jcailloux::relais::list::detail::ListQuery<PageFilters, size_t>;
using PageMetadata = jcailloux::relais::list::detail::ListCacheMetadataImpl<PageFilters, size_t>;
using Page = jcailloux::relais::list::ListWrapper<TestItemEntity>;
using PairKey = std::tuple<int64_t, int64_t>;

// One tier per kind of key and entry: the count stored or recomputed, keys in
// the bucket or behind a pointer, entities, list pages and their metadata.
using ItemTier = CacheTier<int64_t, TestItemEntity, CacheMetadata<true, false>>;
using OrderTier = CacheTier<int64_t, TestOrderEntity, CacheMetadata<true, true>>;
using PairTier = CacheTier<PairKey, TestItemEntity, CacheMetadata<true, true>>;
using PageTier = CacheTier<std::string, Page, PageMetadata>;
using AssortedTier = CacheTier<std::string, Assorted, CacheMetadata<true, false>>;

template<typename Tier>
Tier& tier() {
    static Tier* t = new Tier();
    return *t;
}

/// A string of n characters, distinct per seed.
std::string text(size_t n, int64_t seed) {
    std::string s = std::to_string(seed);
    s.resize(std::max(n, s.size()), static_cast<char>('a' + seed % 26));
    return s;
}

TestItemEntity makeItem(int64_t i) {
    TestItemEntity e;
    e.id = i;
    e.name = text(static_cast<size_t>(i % 40), i);       // in the object, or on the heap
    e.value = static_cast<int32_t>(i);
    e.description = text(static_cast<size_t>(i % 90), i + 1);
    e.created_at = "2026-11-20T10:00:00Z";
    return e;
}

TestOrderEntity makeOrder(int64_t i) {
    TestOrderEntity o;
    o.id = i;
    o.user_id = i % 7;
    o.amount = static_cast<int32_t>(i);
    if (i % 2) o.discount = 5;
    o.label = text(static_cast<size_t>(i % 30), i);
    o.metadata.str = R"({"k":")" + text(static_cast<size_t>(i % 50), i) + R"("})";
    o.address.street = text(24, i);
    o.address.city = text(static_cast<size_t>(i % 20), i);
    o.address.geo.metadata.source = text(18, i);
    o.history.resize(static_cast<size_t>(i % 3));
    for (auto& a : o.history) a.zip_code = text(17, i);
    o.quantities.reserve(static_cast<size_t>(i % 9));     // capacity beyond the size
    o.quantities.push_back(1);
    o.tags = {text(5, i), text(33, i)};
    return o;
}

Page makePage(int64_t i) {
    Page page;
    page.items.reserve(static_cast<size_t>(i % 6) + 1);
    for (int64_t k = 0; k < i % 6; ++k) page.items.push_back(makeItem(i * 10 + k));
    if (i % 3) page.next_cursor = text(static_cast<size_t>(10 + i % 40), i);
    return page;
}

PageMetadata makePageMetadata(const std::string& key, int64_t i) {
    PageQuery query;
    query.filters.owner = i;
    if (i % 2) query.filters.category = text(static_cast<size_t>(i % 35), i);
    if (i % 5 == 0) query.filters.ids = std::vector<int64_t>{1, 2, 3};
    query.limit = 20;
    if (i % 3) query.cursor.data.resize(static_cast<size_t>(8 + i % 20));
    query.cache_key = key;
    return PageMetadata(std::move(query), 1, 0, {}, 0);
}

Assorted makeAssorted(int64_t i) {
    Assorted a;
    for (int64_t k = 0; k < i % 4; ++k) a.by_name.emplace(text(static_cast<size_t>(k * 12), i + k), k);
    for (int32_t k = 0; k < static_cast<int32_t>(i % 5); ++k) a.ids.insert(k);
    a.by_tag.max_load_factor(1e9f);  // keeps its single bucket: no bucket array
    for (int64_t k = 0; k < i % 3; ++k) a.by_tag.emplace(text(static_cast<size_t>(k * 20), i), k);
    a.flags.assign(static_cast<size_t>(i % 200), true);
    a.names = {text(3, i), text(static_cast<size_t>(i % 60), i)};
    if (i % 2) a.either = text(static_cast<size_t>(i % 45), i);
    else a.either = i;
    if (i % 3 == 0) a.maybe = std::vector<int64_t>(static_cast<size_t>(i % 11), i);
    return a;
}

// =============================================================================
// Keys, built before any reading: a key the test holds is not the L1's
// =============================================================================

struct Keys {
    std::vector<int64_t> items, orders;
    std::vector<PairKey> pairs;
    std::vector<std::string> pages, assorted;
};

Keys makeKeys(int64_t first, size_t items, size_t orders, size_t pairs, size_t pages,
              size_t assorted) {
    Keys k;
    for (size_t i = 0; i < items; ++i) k.items.push_back(first + static_cast<int64_t>(i));
    for (size_t i = 0; i < orders; ++i) k.orders.push_back(first + static_cast<int64_t>(i));
    for (size_t i = 0; i < pairs; ++i) {
        k.pairs.emplace_back(first + static_cast<int64_t>(i), static_cast<int64_t>(i % 3));
    }
    for (size_t i = 0; i < pages; ++i) {
        // A list key: a binary-like buffer, in the object or on the heap.
        k.pages.push_back("page:" + text(static_cast<size_t>(i % 70), first + static_cast<int64_t>(i)));
    }
    for (size_t i = 0; i < assorted; ++i) {
        k.assorted.push_back(text(16 + i % 50, first + static_cast<int64_t>(i)));
    }
    return k;
}

int64_t seedOf(int64_t key) { return key; }
int64_t seedOf(const PairKey& key) { return std::get<0>(key) * 3 + std::get<1>(key); }
int64_t seedOf(const std::string& key) { return static_cast<int64_t>(key.size()) * 131 + key[key.size() / 2]; }

// =============================================================================
// Operations on every tier
// =============================================================================

void storeAll(const Keys& k, int64_t variant) {
    for (int64_t key : k.items) (void)tier<ItemTier>().store(key, makeItem(key + variant), {});
    for (int64_t key : k.orders) (void)tier<OrderTier>().store(key, makeOrder(key + variant), {});
    for (const auto& key : k.pairs) {
        (void)tier<PairTier>().store(key, makeItem(seedOf(key) + variant), {});
    }
    for (const auto& key : k.pages) {
        int64_t i = seedOf(key) + variant;
        (void)tier<PageTier>().store(key, makePage(i), makePageMetadata(key, i));
    }
    for (const auto& key : k.assorted) {
        (void)tier<AssortedTier>().store(key, makeAssorted(seedOf(key) + variant), {});
    }
}

/// Ghosts for keys that hold no entry: a key kept behind a pointer takes a
/// node, a ghost included.
void insertGhosts(const Keys& k) {
    for (int64_t key : k.items) (void)tier<ItemTier>().map().insert_ghost(key, 3, 200, 0);
    for (int64_t key : k.orders) (void)tier<OrderTier>().map().insert_ghost(key, 3, 200, 0);
    for (const auto& key : k.pairs) (void)tier<PairTier>().map().insert_ghost(key, 3, 200, 0);
    for (const auto& key : k.pages) (void)tier<PageTier>().map().insert_ghost(key, 3, 200, 0);
    for (const auto& key : k.assorted) (void)tier<AssortedTier>().map().insert_ghost(key, 3, 200, 0);
}

/// An insert of a key already there: the new entry is built, then destroyed
/// at once, never retired.
void insertExisting(const Keys& k) {
    for (int64_t key : k.items) (void)tier<ItemTier>().map().insert(key, makeItem(key), {});
    for (const auto& key : k.pages) {
        int64_t i = seedOf(key);
        (void)tier<PageTier>().map().insert(key, makePage(i), makePageMetadata(key, i));
    }
}

/// Evicts every other key, one way or another; ghosts and entries alike.
void evictSome(const Keys& k) {
    for (size_t i = 0; i < k.items.size(); i += 2) tier<ItemTier>().evict(k.items[i]);
    for (size_t i = 0; i < k.orders.size(); i += 2) tier<OrderTier>().evict(k.orders[i]);
    for (size_t i = 0; i < k.pairs.size(); i += 2) tier<PairTier>().evict(k.pairs[i]);
    for (size_t i = 0; i < k.pages.size(); i += 2) tier<PageTier>().removeGhost(k.pages[i]);
    for (size_t i = 1; i < k.pages.size(); i += 4) tier<PageTier>().evict(k.pages[i]);
    for (size_t i = 0; i < k.assorted.size(); i += 2) {
        auto hit = tier<AssortedTier>().find(k.assorted[i]);
        const Assorted* value = hit.value;
        hit = {};
        if (value != nullptr) tier<AssortedTier>().evictIfSame(k.assorted[i], value);
    }
}

/// Sweeps every chunk of every tier: entries evicted by score become ghosts,
/// ghosts decay, a few are dropped.
template<typename Tier>
void sweepTier(int64_t round) {
    constexpr long kChunks = 4;
    for (long c = 0; c < kChunks; ++c) {
        (void)tier<Tier>().sweepChunk(c, kChunks, [round](const auto& key, auto&, auto&, long) {
            return (seedOf(key) + round) % 3 == 0;
        });
    }
}

void sweepAll(int64_t round) {
    sweepTier<ItemTier>(round);
    sweepTier<OrderTier>(round);
    sweepTier<PairTier>(round);
    sweepTier<PageTier>(round);
    sweepTier<AssortedTier>(round);
}

/// Frees the retired entries the calling thread can: those no epoch still
/// protects, from its own retire lists.
void reclaimAll() {
    tier<ItemTier>().map().reclaim();
    tier<OrderTier>().map().reclaim();
    tier<PairTier>().map().reclaim();
    tier<PageTier>().map().reclaim();
    tier<AssortedTier>().map().reclaim();
}

/// Removes every key, entry or ghost, then frees every retired block.
void emptyAll() {
    auto all = [](const auto&, auto) { return true; };
    (void)tier<ItemTier>().map().full_cleanup(all);
    (void)tier<OrderTier>().map().full_cleanup(all);
    (void)tier<PairTier>().map().full_cleanup(all);
    (void)tier<PageTier>().map().full_cleanup(all);
    (void)tier<AssortedTier>().map().full_cleanup(all);
    TestInternals::freeRetired(tier<ItemTier>().map());
    TestInternals::freeRetired(tier<OrderTier>().map());
    TestInternals::freeRetired(tier<PairTier>().map());
    TestInternals::freeRetired(tier<PageTier>().map());
    TestInternals::freeRetired(tier<AssortedTier>().map());
}

/// Every path once, before the first reading: what the process sets up once
/// (the allocator's classes, parlay's lock table, each thread's ids and
/// counters) is not the L1's to count, and stays alive. Recorded all the same:
/// a block of the L1 allocated here (a link, a retire list's array) may be
/// freed later, which the ledger counts, and so must the record.
void warmUp() {
    static bool done = false;
    if (done) return;
    done = true;
    Keys k = makeKeys(1'000'000, 1500, 300, 600, 300, 300);
    Recording recording;
    storeAll(k, 0);
    storeAll(k, 1);
    insertExisting(k);
    sweepAll(0);
    evictSome(k);
    insertGhosts(makeKeys(2'000'000, 50, 50, 50, 50, 50));
    reclaimAll();
    emptyAll();
    (void)L1Ledger::sum();
}

// =============================================================================
// Readings
// =============================================================================

/// The ledger and the room of the recorded blocks, read together.
struct Reading {
    int64_t ledger = 0;
    int64_t held = 0;
};

Reading readNow() {
    NotRecording quiet;
    return {L1Ledger::sum(), ProcessBlocks::instance().room()};
}

/// What changed since `base`: the ledger, and the room of the blocks.
struct Change {
    int64_t ledger = 0;
    int64_t held = 0;
};

Change changeSince(const Reading& base) {
    Reading now = readNow();
    return {now.ledger - base.ledger, now.held - base.held};
}

}  // namespace

// =============================================================================
// The layout the score reads
// =============================================================================

TEST_CASE("Ledger - a key's slot, as the score weighs it", "[ledger][slot]")
{
    using Items = EntryLayout<int64_t, TestItemEntity, CacheMetadata<true, false>, true,
                              jcailloux::relais::cache::detail::AutoHash<int64_t>>;
    using Pages = EntryLayout<std::string, Page, PageMetadata, true,
                              jcailloux::relais::cache::detail::AutoHash<std::string>>;
    using Pairs = EntryLayout<PairKey, TestItemEntity, CacheMetadata<true, true>, true,
                              jcailloux::relais::cache::detail::AutoHash<PairKey>>;

    SECTION("[slot] parlay keeps a key in the bucket only when it is trivially copyable") {
        STATIC_REQUIRE_FALSE(Items::kIndirectKeys);
        STATIC_REQUIRE(Pages::kIndirectKeys);
        STATIC_REQUIRE(Pairs::kIndirectKeys);  // std::tuple is not trivially copyable
    }

    SECTION("[slot] a key's share of the bucket array, at parlay's expected load") {
        // A bucket is one 64-byte line: a version word, the list head, and
        // 48 bytes of slots. An int64_t key and its tagged pointer take 16
        // bytes: 3 inline, an expected 4 keys per bucket. A pointer to a node
        // takes 8: 6 inline, an expected 8 per bucket.
        STATIC_REQUIRE(sizeof(Items::Table::bucket) == 64);
        STATIC_REQUIRE(sizeof(Pages::Table::bucket) == 64);
        STATIC_REQUIRE(Items::Table::buffer_size == 3);
        STATIC_REQUIRE(Items::Table::log_bucket_size == 2);
        STATIC_REQUIRE(Pages::Table::buffer_size == 6);
        STATIC_REQUIRE(Pages::Table::log_bucket_size == 3);
        STATIC_REQUIRE(Items::kTableShare == 64 / 4);
        STATIC_REQUIRE(Pages::kTableShare == 64 / 8);
        STATIC_REQUIRE(Pairs::kTableShare == 64 / 8);
        CHECK(Items::slotBytes(42) == 16);
    }

    SECTION("[slot] a key behind a pointer adds its node and what it owns") {
        // The node: the key and the tagged pointer, in a block of parlay's
        // node pool, with its guard words when parlay checks its pools.
        STATIC_REQUIRE(sizeof(Pages::Node) == sizeof(std::string) + sizeof(TaggedEntry));
        STATIC_REQUIRE(sizeof(Pairs::Node) == 2 * sizeof(int64_t) + sizeof(TaggedEntry));
#ifdef EpochMemCheck
        STATIC_REQUIRE(sizeof(PoolBlock<Pages::Node>) == sizeof(Pages::Node) + 3 * sizeof(long));
#else
        STATIC_REQUIRE(sizeof(PoolBlock<Pages::Node>) == sizeof(Pages::Node));
#endif
        std::string short_key = "page:1";
        std::string long_key(60, 'k');
        CHECK(Pages::slotBytes(short_key) == 8 + blockBytes(sizeof(PoolBlock<Pages::Node>)));
        CHECK(Pages::slotBytes(long_key)
              == 8 + blockBytes(sizeof(PoolBlock<Pages::Node>)) + blockBytes(61, long_key.data()));
        CHECK(Pairs::slotBytes({1, 2}) == 8 + blockBytes(sizeof(PoolBlock<Pairs::Node>)));
    }

    SECTION("[slot] a page's entry counts what its metadata owns") {
        warmUp();
        std::string key = "page:" + std::string(50, 'q');
        (void)tier<PageTier>().store(key, makePage(7), makePageMetadata(key, 7));
        auto found = tier<PageTier>().map().find(key);
        auto* entry = found.asReal();
        REQUIRE(entry != nullptr);
        size_t query_heap = heapBytes(entry->metadata.query);
        CHECK(query_heap >= heapBytes(entry->metadata.query.cache_key));
        CHECK(heapBytes(entry->metadata.query.cache_key) > 0);  // a copy of the key, on the heap
        CHECK(entry->bytes() == blockBytes(PageTier::Map::kEntryOverhead + sizeof(Page), entry->block())
                                    + heapBytes(entry->value) + query_heap);
        found = {};
        emptyAll();
    }
}

// =============================================================================
// The balance: the ledger against the blocks
// =============================================================================

TEST_CASE("Ledger - counts exactly the room of the blocks the L1 holds", "[ledger][balance]")
{
    warmUp();
    Keys k = makeKeys(10'000'000, 6000, 1500, 2500, 1200, 1200);
    Keys ghosts = makeKeys(20'000'000, 800, 300, 500, 400, 400);
    Reading base = readNow();

    auto check = [&](const char* phase) {
        Change c = changeSince(base);
        INFO(phase);
        INFO("recorded blocks alive: " << ProcessBlocks::instance().count());
        CHECK(c.ledger == c.held);
        return c;
    };

    {
        Recording recording;
        storeAll(k, 0);  // the tables grow past their warm-up size
    }
    Change filled = check("fill");
    CHECK(filled.held > 0);

    {
        Recording recording;
        insertGhosts(ghosts);
    }
    check("ghosts");

    {
        Recording recording;
        storeAll(k, 7);  // every entry replaced: the old ones retired, not freed
        insertExisting(k);
    }
    check("replace");

    {
        Recording recording;
        sweepAll(1);
        evictSome(k);
        sweepAll(2);
    }
    check("evict and sweep");

    {
        Recording recording;
        reclaimAll();
    }
    check("reclaim");

    {
        Recording recording;
        emptyAll();
    }
    check("empty");  // left: each version of each table, the retire lists' arrays
}

TEST_CASE("Ledger - threads that allocate and free each other's blocks", "[ledger][threads]")
{
    warmUp();
    constexpr int kThreads = 4;
    constexpr int64_t kPerThread = 3000;
    std::vector<Keys> keys;
    for (int t = 0; t < kThreads; ++t) {
        keys.push_back(makeKeys(30'000'000 + t * kPerThread, kPerThread, 0, 0, kPerThread / 4, 0));
    }
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    Reading base = readNow();

    {
        Recording recording;  // the threads' own state, allocated here, is freed by them
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&keys, t] {
                {
                    // What the process sets up once per thread: its ids, its counter.
                    (void)parlay::my_thread_id();
                    L1Ledger::add(0);
                }
                Recording thread_recording;
                const Keys& mine = keys[t];
                const Keys& theirs = keys[(t + 1) % kThreads];
                for (int round = 0; round < 3; ++round) {
                    storeAll(mine, round);
                    // Replace and evict the next thread's keys: its entries
                    // are retired and freed here.
                    for (size_t i = round; i < theirs.items.size(); i += 3) {
                        (void)tier<ItemTier>().store(theirs.items[i], makeItem(round), {});
                        tier<ItemTier>().evict(theirs.items[(i + 1) % theirs.items.size()]);
                    }
                    for (size_t i = round; i < theirs.pages.size(); i += 2) {
                        tier<PageTier>().evict(theirs.pages[i]);
                    }
                    reclaimAll();
                }
            });
        }
        for (auto& t : threads) t.join();
    }
    Change during = changeSince(base);
    CHECK(during.ledger == during.held);

    {
        Recording recording;
        emptyAll();
    }
    Change after = changeSince(base);
    CHECK(after.ledger == after.held);
}
