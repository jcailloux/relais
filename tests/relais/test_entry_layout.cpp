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
 * explained.
 */

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <vector>

#include <jcailloux/relais/cache/CacheMetadata.h>
#include <jcailloux/relais/cache/ChunkMap.h>

#include "fixtures/generated/TestItemEntity.h"

using jcailloux::relais::cache::detail::CacheMetadata;
using jcailloux::relais::cache::detail::ChunkMap;
using TestItemEntity = ::entity::generated::TestItemEntity;

namespace {

thread_local bool tl_recording = false;
thread_local std::vector<size_t>* tl_sizes = nullptr;

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

/// The sizes malloc was asked for while inserting one new key.
template<typename Map, typename V>
std::vector<size_t> mallocsOfOneInsert(const V& value) {
    static Map map;          // a map is never freed: one per type
    static int64_t next_key = 0;
    map.upsert(++next_key, value, {});  // the table and the pool are set up
    std::vector<size_t> sizes;
    sizes.reserve(16);
    tl_sizes = &sizes;
    tl_recording = true;
    map.upsert(++next_key, value, {});
    tl_recording = false;
    tl_sizes = nullptr;
    return sizes;
}

}  // namespace

extern "C" void* __real_malloc(size_t n);
extern "C" void* __wrap_malloc(size_t n) {
    if (tl_recording) {
        tl_recording = false;  // push_back must not record itself
        tl_sizes->push_back(n);
        tl_recording = true;
    }
    return __real_malloc(n);
}

TEST_CASE("Entry layout - one malloc per entry, of the size counted",
          "[entry-layout]")
{
    SECTION("[entry-layout] GDSF, value aligned on 8: the count fills the header word") {
        using Map = ChunkMap<int64_t, TestItemEntity, CacheMetadata<true, false>, true, true>;
        // header: access count (4) + stored byte count (4); the value follows at 8
        STATIC_REQUIRE(Map::kStoresBytes);
        STATIC_REQUIRE(Map::kEntryOverhead == 4 + 4 + kPoolGuards);
        CHECK(mallocsOfOneInsert<Map>(TestItemEntity{})
              == std::vector<size_t>{Map::kEntryOverhead + sizeof(TestItemEntity)});
    }

    SECTION("[entry-layout] GDSF + TTL: the metadata fills the header, the count is not stored") {
        using Map = ChunkMap<int64_t, TestItemEntity, CacheMetadata<true, true>, true, true>;
        // header: access count (4) + TTL (4); a count would push the value to 16
        STATIC_REQUIRE_FALSE(Map::kStoresBytes);
        STATIC_REQUIRE(Map::kEntryOverhead == 4 + 4 + kPoolGuards);
        CHECK(mallocsOfOneInsert<Map>(TestItemEntity{})
              == std::vector<size_t>{Map::kEntryOverhead + sizeof(TestItemEntity)});
    }

    SECTION("[entry-layout] GDSF, value aligned on 4: no padding, the count is not stored") {
        using Map = ChunkMap<int64_t, Triple, CacheMetadata<true, false>, true, true>;
        // header: access count (4); the value follows at 4, nothing to fill
        STATIC_REQUIRE_FALSE(Map::kStoresBytes);
        STATIC_REQUIRE(Map::kEntryOverhead == 4 + kPoolGuards);
        CHECK(mallocsOfOneInsert<Map>(Triple{})
              == std::vector<size_t>{Map::kEntryOverhead + sizeof(Triple)});
    }

    SECTION("[entry-layout] no GDSF, no TTL: an empty header, nothing counted") {
        using Map = ChunkMap<int64_t, TestItemEntity, CacheMetadata<false, false>, false, false>;
        // the empty metadata takes no room: the value starts the entry
        STATIC_REQUIRE_FALSE(Map::kStoresBytes);
        STATIC_REQUIRE(Map::kEntryOverhead == 0 + kPoolGuards);
        CHECK(mallocsOfOneInsert<Map>(TestItemEntity{})
              == std::vector<size_t>{Map::kEntryOverhead + sizeof(TestItemEntity)});
    }
}
