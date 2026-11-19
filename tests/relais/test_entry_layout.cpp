/**
 * test_entry_layout.cpp
 *
 * What the L1 allocates for one entry, term by term, checked against the
 * malloc the entry pool really makes.
 *
 * Linked with -Wl,--wrap=malloc: every malloc called from this executable's
 * own code, the header-only epoch pool and parlay included, passes through
 * __wrap_malloc. The standard library's operator new calls malloc from inside
 * libstdc++, unwrapped: a value's heap is not seen here (test_gdsf covers it).
 *
 * Each expected overhead is written out from its terms, so a change in any of
 * them (metadata, stored count, padding, pool guards) fails here and has to be
 * explained. The entry's count weighs that block at the room the allocator
 * gives it, checked against the block itself.
 */

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <type_traits>
#include <vector>

#include <malloc.h>

#include <jcailloux/relais/cache/CacheMetadata.h>
#include <jcailloux/relais/cache/ChunkMap.h>

#include "fixtures/TestAllocator.h"
#include "fixtures/generated/TestItemEntity.h"

using jcailloux::relais::cache::detail::CacheMetadata;
using jcailloux::relais::cache::detail::ChunkMap;
using jcailloux::relais::detail::blockBytes;
using TestItemEntity = ::entity::generated::TestItemEntity;

namespace {

/// The mallocs made while recording: their sizes, and the blocks returned;
/// and the key the recorded insert stored.
struct Mallocs {
    std::vector<size_t> sizes;
    std::vector<void*> blocks;
    int64_t key = 0;
};

thread_local bool tl_recording = false;
thread_local Mallocs* tl_mallocs = nullptr;

/// 12 bytes aligned on 4: a value with no padding before it to absorb a count.
struct Triple { int32_t a = 0, b = 0, c = 0; };

/// The pool's guard words around each entry when parlay checks its pool
/// (EpochMemCheck, set in builds without NDEBUG): pad, head and tail.
constexpr size_t kPoolGuards =
#ifdef EpochMemCheck
    3 * sizeof(long);
#else
    0;
#endif

/// One map per type, never freed: its entries stay in their blocks.
template<typename Map>
Map& mapOf() {
    static Map map;
    return map;
}

/// The mallocs made while inserting one new key.
template<typename Map, typename V>
Mallocs mallocsOfOneInsert(const V& value) {
    static int64_t next_key = 0;
    mapOf<Map>().upsert(++next_key, value, {});  // the table and the pool are set up
    (void)Map::entryBytes(value);                // and the allocator's classes read
    Mallocs mallocs;
    mallocs.sizes.reserve(16);
    mallocs.blocks.reserve(16);
    mallocs.key = ++next_key;
    tl_mallocs = &mallocs;
    tl_recording = true;
    mapOf<Map>().upsert(mallocs.key, value, {});
    tl_recording = false;
    tl_mallocs = nullptr;
    return mallocs;
}

}  // namespace

extern "C" void* __real_malloc(size_t n);
extern "C" void* __wrap_malloc(size_t n) {
    void* p = __real_malloc(n);
    if (tl_recording) {
        tl_recording = false;  // push_back must not record itself
        tl_mallocs->sizes.push_back(n);
        tl_mallocs->blocks.push_back(p);
        tl_recording = true;
    }
    return p;
}

TEST_CASE("Entry layout - one malloc per entry, of the size counted",
          "[entry-layout]")
{
    SECTION("[entry-layout] GDSF, value aligned on 8: the count fills the header word") {
        using Map = ChunkMap<int64_t, TestItemEntity, CacheMetadata<true, false>, true, true>;
        // header: access count (4) + stored byte count (4); the value follows at 8
        STATIC_REQUIRE(Map::kStoresBytes);
        STATIC_REQUIRE(Map::kEntryOverhead == 4 + 4 + kPoolGuards);
        CHECK(mallocsOfOneInsert<Map>(TestItemEntity{}).sizes
              == std::vector<size_t>{Map::kEntryOverhead + sizeof(TestItemEntity)});
    }

    SECTION("[entry-layout] GDSF + TTL: the metadata fills the header, the count is not stored") {
        using Map = ChunkMap<int64_t, TestItemEntity, CacheMetadata<true, true>, true, true>;
        // header: access count (4) + TTL (4); a count would push the value to 16
        STATIC_REQUIRE_FALSE(Map::kStoresBytes);
        STATIC_REQUIRE(Map::kEntryOverhead == 4 + 4 + kPoolGuards);
        CHECK(mallocsOfOneInsert<Map>(TestItemEntity{}).sizes
              == std::vector<size_t>{Map::kEntryOverhead + sizeof(TestItemEntity)});
    }

    SECTION("[entry-layout] GDSF, value aligned on 4: no padding, the count is not stored") {
        using Map = ChunkMap<int64_t, Triple, CacheMetadata<true, false>, true, true>;
        // header: access count (4); the value follows at 4, nothing to fill
        STATIC_REQUIRE_FALSE(Map::kStoresBytes);
        STATIC_REQUIRE(Map::kEntryOverhead == 4 + kPoolGuards);
        CHECK(mallocsOfOneInsert<Map>(Triple{}).sizes
              == std::vector<size_t>{Map::kEntryOverhead + sizeof(Triple)});
    }

    SECTION("[entry-layout] no GDSF, no TTL: an empty header, nothing counted") {
        using Map = ChunkMap<int64_t, TestItemEntity, CacheMetadata<false, false>, false, false>;
        // the empty metadata takes no room: the value starts the entry
        STATIC_REQUIRE_FALSE(Map::kStoresBytes);
        STATIC_REQUIRE(Map::kEntryOverhead == 0 + kPoolGuards);
        CHECK(mallocsOfOneInsert<Map>(TestItemEntity{}).sizes
              == std::vector<size_t>{Map::kEntryOverhead + sizeof(TestItemEntity)});
    }
}

TEST_CASE("Entry layout - an entry weighs the room its block takes", "[entry-layout]")
{
    // The value owns no heap: the entry's count is its pool block alone, the
    // block the pool allocated, at its usable size and the header in front of
    // it. Before the entry exists, the block weighs the class of its size.
    auto check = [](auto map_tag, const auto& value) {
        using Map = typename decltype(map_tag)::type;
        using V = std::remove_cvref_t<decltype(value)>;
        auto mallocs = mallocsOfOneInsert<Map>(value);
        REQUIRE(mallocs.blocks.size() == 1);
        auto found = mapOf<Map>().find(mallocs.key);
        auto* entry = found.asReal();
        REQUIRE(entry != nullptr);

        CHECK(entry->block() == mallocs.blocks[0]);
        size_t held = malloc_usable_size(mallocs.blocks[0]) + relais_test::heapBlockHeader();
        CHECK(entry->heldBytes() == held);
        CHECK(entry->bytes() == held);
        CHECK(Map::entryBytes(value) == blockBytes(Map::kEntryOverhead + sizeof(V)));
    };

    SECTION("[entry-layout] GDSF, the count stored") {
        using Map = ChunkMap<int64_t, TestItemEntity, CacheMetadata<true, false>, true, true>;
        check(std::type_identity<Map>{}, TestItemEntity{});
    }

    SECTION("[entry-layout] GDSF + TTL, the count recomputed") {
        using Map = ChunkMap<int64_t, TestItemEntity, CacheMetadata<true, true>, true, true>;
        check(std::type_identity<Map>{}, TestItemEntity{});
    }

    SECTION("[entry-layout] a value aligned on 4") {
        using Map = ChunkMap<int64_t, Triple, CacheMetadata<true, false>, true, true>;
        check(std::type_identity<Map>{}, Triple{});
    }
}
