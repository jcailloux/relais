/**
 * test_list_filter_widths.cpp
 *
 * List filters on fixed-size values of every width and signedness, as the L2
 * Lua matchers read them.
 *
 * Covers:
 *   1. Schema — each filter announces the width and the signedness of its
 *      value, so the Lua parser reads exactly the bytes the encoder wrote.
 *   2. Alignment — a 2-byte value is compared on both of its bytes and keeps
 *      the filters that follow it aligned, in the create, update and
 *      predicate scripts.
 *   3. Order — range filters on signed 2-byte values (enum included) and on
 *      unsigned values follow the numeric order; a range filter the matcher
 *      cannot order never keeps a page it may hold.
 *
 * Each group is registered with a 1-byte page "x" — shorter than the bounds
 * header, so the Lua range check short-circuits to "delete". Whether a page
 * survives therefore depends only on the filter match.
 */

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>

#include "fixtures/test_helper.h"
#include <jcailloux/relais/cache/RedisCache.h>
#include <jcailloux/relais/list/spec/CanonicalEncoding.h>

using relais_test::sync;              // relais_test::sync, not POSIX ::sync from <unistd.h>
using relais_test::TransactionGuard;

namespace decl = jcailloux::relais::list::spec;

namespace {

enum class Level : int16_t { Low = -300, Mid = 5, High = 300 };
enum class Flag : uint8_t { On = 1, Off = 2 };

// Filtered only: never read from the database.
struct Wide {
    int64_t id = 0;
    int64_t owner = 0;
    int16_t temp = 0;
    Level level = Level::Mid;
    Flag flag = Flag::On;
    uint16_t port = 0;
    uint32_t count = 0;
    uint64_t big = 0;
    float ratio = 0.0f;
    double score = 0.0;

    static std::optional<Wide> fromRow(const jcailloux::relais::io::PgResult::Row&) {
        return std::nullopt;
    }
};

using IdSort = decl::Sort<"id", &Wide::id, "id", decl::SortDirection::Asc>;

// [temp int16 EQ][owner int64 EQ]
struct DescTemp {
    using Entity = Wide;
    static constexpr auto filters = std::tuple{
        decl::Filter<"temp", &Wide::temp, "temp", decl::Op::EQ>{},
        decl::Filter<"owner", &Wide::owner, "owner", decl::Op::EQ>{}
    };
    static constexpr auto sorts = std::tuple{IdSort{}};
};

// [flag enum:uint8 EQ][owner int64 EQ]
struct DescFlag {
    using Entity = Wide;
    static constexpr auto filters = std::tuple{
        decl::Filter<"flag", &Wide::flag, "flag", decl::Op::EQ>{},
        decl::Filter<"owner", &Wide::owner, "owner", decl::Op::EQ>{}
    };
    static constexpr auto sorts = std::tuple{IdSort{}};
};

// [flag enum:uint8 EQ][level enum:int16 GE][owner int64 EQ]
struct DescLevel {
    using Entity = Wide;
    static constexpr auto filters = std::tuple{
        decl::Filter<"flag", &Wide::flag, "flag", decl::Op::EQ>{},
        decl::Filter<"level_min", &Wide::level, "level", decl::Op::GE>{},
        decl::Filter<"owner", &Wide::owner, "owner", decl::Op::EQ>{}
    };
    static constexpr auto sorts = std::tuple{IdSort{}};
};

// [port uint16 GE][count uint32 GE][big uint64 GE]
struct DescUnsigned {
    using Entity = Wide;
    static constexpr auto filters = std::tuple{
        decl::Filter<"port_min", &Wide::port, "port", decl::Op::GE>{},
        decl::Filter<"count_min", &Wide::count, "count", decl::Op::GE>{},
        decl::Filter<"big_min", &Wide::big, "big", decl::Op::GE>{}
    };
    static constexpr auto sorts = std::tuple{IdSort{}};
};

// [ratio float GE][score double GE]
struct DescFloat {
    using Entity = Wide;
    static constexpr auto filters = std::tuple{
        decl::Filter<"ratio_min", &Wide::ratio, "ratio", decl::Op::GE>{},
        decl::Filter<"score_min", &Wide::score, "score", decl::Op::GE>{}
    };
    static constexpr auto sorts = std::tuple{IdSort{}};
};

namespace cache_ns = jcailloux::relais::cache;
using jcailloux::relais::PgProvider;

constexpr std::string_view kPrefix = "T:dlist:g:";  // 10 bytes
constexpr size_t kPrefixLen = 10;
const std::string kMaster = "test:widths:l2:master";

template<typename D>
std::string registerGroup(const decl::ListQueryParams<D>& q) {
    std::string groupKey = std::string(kPrefix) + decl::groupKey<D>(q.filters, q.sort);
    std::string pageKey = groupKey + ":p";
    sync(PgProvider::redis("SET", pageKey, "x"));
    sync(PgProvider::redis("SADD", groupKey + ":_keys", pageKey));
    sync(PgProvider::redis("HSET", kMaster, groupKey, "0"));
    return pageKey;
}

bool alive(const std::string& pageKey) {
    return sync(PgProvider::redis("EXISTS", pageKey)).asInteger() == 1;
}

template<typename D>
void fireCreate(const Wide& e) {
    sync(cache_ns::RedisCache::invalidateListGroupsSelective(
        kMaster, kPrefixLen, decl::filterSchema<D>(),
        decl::encodeEntityFilterBlob<D>(e), "0"));
}

template<typename D>
void fireUpdate(const Wide& oldE, const Wide& newE) {
    sync(cache_ns::RedisCache::invalidateListGroupsSelectiveUpdate(
        kMaster, kPrefixLen, decl::filterSchema<D>(),
        decl::encodeEntityFilterBlob<D>(newE), "0",
        decl::encodeEntityFilterBlob<D>(oldE), "0"));
}

template<typename D>
void firePredicate(const decl::ListQueryParams<D>& predicate) {
    sync(cache_ns::RedisCache::invalidateListGroupsByPredicate(
        kMaster, kPrefixLen, decl::filterSchema<D>(),
        decl::encodeFilterSet<D>(predicate.filters), "", ""));
}

decl::ListQueryParams<DescTemp> tempParams(int16_t temp, int64_t owner) {
    decl::ListQueryParams<DescTemp> q;
    q.filters.get<"temp">() = temp;
    q.filters.get<"owner">() = owner;
    return q;
}

decl::ListQueryParams<DescLevel> levelParams(Flag flag, Level levelMin, int64_t owner) {
    decl::ListQueryParams<DescLevel> q;
    q.filters.get<"flag">() = flag;
    q.filters.get<"level_min">() = levelMin;
    q.filters.get<"owner">() = owner;
    return q;
}

Wide wide(int16_t temp, int64_t owner) {
    Wide w;
    w.temp = temp;
    w.owner = owner;
    return w;
}

constexpr uint64_t kHighBit = uint64_t{1} << 63;

}  // namespace

// #############################################################################
//
//  1. Schema
//
// #############################################################################

TEST_CASE("[ListWidths] the filter schema gives each value its width and signedness",
          "[list][widths][unit]") {
    CHECK(decl::filterSchema<DescFlag>() == "1=8=");
    CHECK(decl::filterSchema<DescTemp>() == "2=8=");
    CHECK(decl::filterSchema<DescLevel>() == "1=2G8=");
    CHECK(decl::filterSchema<DescUnsigned>() == "wGuGUG");
    CHECK(decl::filterSchema<DescFloat>() == "fGdG");
}

TEST_CASE("[ListWidths] the entity blob carries every byte of each value",
          "[list][widths][unit]") {
    CHECK(decl::encodeEntityFilterBlob<DescTemp>(wide(7, 1)).size() == (1 + 2) + (1 + 8));
    CHECK(decl::encodeEntityFilterBlob<DescLevel>(Wide{}).size() == (1 + 1) + (1 + 2) + (1 + 8));
    CHECK(decl::encodeEntityFilterBlob<DescUnsigned>(Wide{}).size() == (1 + 2) + (1 + 4) + (1 + 8));
    CHECK(decl::encodeEntityFilterBlob<DescFloat>(Wide{}).size() == (1 + 4) + (1 + 8));
}

// #############################################################################
//
//  2. Alignment
//
// #############################################################################

TEST_CASE("[ListWidths][L2] create compares both bytes of a 2-byte value and stays aligned",
          "[integration][redis][list][widths][l2]") {
    TransactionGuard tx;

    auto gMatch     = registerGroup(tempParams(7, 1));
    auto gOwner     = registerGroup(tempParams(7, 2));
    auto gHighByte  = registerGroup(tempParams(7 + 256, 1));

    fireCreate<DescTemp>(wide(7, 1));

    CHECK_FALSE(alive(gMatch));
    CHECK(alive(gOwner));        // the owner after the 2-byte value is read aligned
    CHECK(alive(gHighByte));     // 263 and 7 share their low byte only
}

TEST_CASE("[ListWidths][L2] update compares both bytes of a 2-byte value and stays aligned",
          "[integration][redis][list][widths][l2]") {
    TransactionGuard tx;

    auto gOld       = registerGroup(tempParams(7, 1));
    auto gNew       = registerGroup(tempParams(9, 1));
    auto gOwner     = registerGroup(tempParams(7, 2));
    auto gHighByte  = registerGroup(tempParams(9 + 256, 1));

    fireUpdate<DescTemp>(wide(7, 1), wide(9, 1));

    CHECK_FALSE(alive(gOld));
    CHECK_FALSE(alive(gNew));
    CHECK(alive(gOwner));
    CHECK(alive(gHighByte));
}

TEST_CASE("[ListWidths][L2] a predicate compares both bytes of a 2-byte value and stays aligned",
          "[integration][redis][list][widths][l2]") {
    TransactionGuard tx;

    auto gMatch     = registerGroup(tempParams(7, 1));
    auto gOwner     = registerGroup(tempParams(7, 2));
    auto gHighByte  = registerGroup(tempParams(7 + 256, 1));

    firePredicate<DescTemp>(tempParams(7, 1));

    CHECK_FALSE(alive(gMatch));
    CHECK(alive(gOwner));
    CHECK(alive(gHighByte));
}

TEST_CASE("[ListWidths][L2] a 1-byte enum keeps the filters that follow it aligned",
          "[integration][redis][list][widths][l2]") {
    TransactionGuard tx;

    auto flagParams = [](Flag flag, int64_t owner) {
        decl::ListQueryParams<DescFlag> q;
        q.filters.get<"flag">() = flag;
        q.filters.get<"owner">() = owner;
        return q;
    };
    auto gMatch = registerGroup(flagParams(Flag::On, 1));
    auto gFlag  = registerGroup(flagParams(Flag::Off, 1));
    auto gOwner = registerGroup(flagParams(Flag::On, 2));

    Wide e;
    e.flag = Flag::On;
    e.owner = 1;
    fireCreate<DescFlag>(e);

    CHECK_FALSE(alive(gMatch));
    CHECK(alive(gFlag));
    CHECK(alive(gOwner));
}

// #############################################################################
//
//  3. Order
//
// #############################################################################

TEST_CASE("[ListWidths][L2] a range filter on a signed 2-byte enum follows its sign",
          "[integration][redis][list][widths][l2]") {
    TransactionGuard tx;

    auto gLow   = registerGroup(levelParams(Flag::On, Level::Low, 1));
    auto gMid   = registerGroup(levelParams(Flag::On, Level::Mid, 1));
    auto gHigh  = registerGroup(levelParams(Flag::On, Level::High, 1));
    auto gOwner = registerGroup(levelParams(Flag::On, Level::Low, 2));

    Wide e;
    e.flag = Flag::On;
    e.level = Level::Mid;
    e.owner = 1;
    fireCreate<DescLevel>(e);

    CHECK_FALSE(alive(gLow));    // 5 ≥ -300
    CHECK_FALSE(alive(gMid));    // 5 ≥ 5
    CHECK(alive(gHigh));         // 5 < 300
    CHECK(alive(gOwner));        // the owner after the 2-byte value is read aligned
}

TEST_CASE("[ListWidths][L2] range filters on unsigned values follow unsigned order",
          "[integration][redis][list][widths][l2]") {
    TransactionGuard tx;

    auto portParams = [](uint16_t v) {
        decl::ListQueryParams<DescUnsigned> q;
        q.filters.get<"port_min">() = v;
        return q;
    };
    auto countParams = [](uint32_t v) {
        decl::ListQueryParams<DescUnsigned> q;
        q.filters.get<"count_min">() = v;
        return q;
    };
    auto bigParams = [](uint64_t v) {
        decl::ListQueryParams<DescUnsigned> q;
        q.filters.get<"big_min">() = v;
        return q;
    };

    auto gPortLow   = registerGroup(portParams(100));
    auto gPortHigh  = registerGroup(portParams(50000));
    auto gCountLow  = registerGroup(countParams(100));
    auto gCountHigh = registerGroup(countParams(4'000'000'000u));
    auto gBigLow    = registerGroup(bigParams(100));
    auto gBigHigh   = registerGroup(bigParams(kHighBit + (uint64_t{1} << 20)));

    Wide e;
    e.port = 40000;
    e.count = 3'000'000'000u;
    e.big = kHighBit + 5;
    fireCreate<DescUnsigned>(e);

    // Each value has its top bit set: read as signed, it would sort below 100.
    CHECK_FALSE(alive(gPortLow));
    CHECK(alive(gPortHigh));
    CHECK_FALSE(alive(gCountLow));
    CHECK(alive(gCountHigh));
    CHECK_FALSE(alive(gBigLow));
    CHECK(alive(gBigHigh));
}

TEST_CASE("[ListWidths][L2] a range filter on a floating-point value never keeps a page it may hold",
          "[integration][redis][list][widths][l2]") {
    TransactionGuard tx;

    auto ratioParams = [](float v) {
        decl::ListQueryParams<DescFloat> q;
        q.filters.get<"ratio_min">() = v;
        return q;
    };
    auto scoreParams = [](double v) {
        decl::ListQueryParams<DescFloat> q;
        q.filters.get<"score_min">() = v;
        return q;
    };

    auto gRatio = registerGroup(ratioParams(-10.0f));
    auto gScore = registerGroup(scoreParams(-10.0));

    Wide e;
    e.ratio = -5.0f;
    e.score = -5.0;
    fireCreate<DescFloat>(e);

    // -5 ≥ -10. The IEEE bits of two negatives, read as integers, sort the
    // other way round.
    CHECK_FALSE(alive(gRatio));
    CHECK_FALSE(alive(gScore));
}
