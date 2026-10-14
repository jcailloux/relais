/**
 * test_list_filter_widths.cpp
 *
 * List filters on fixed-size values of every width and signedness, as the
 * list cache matches them against a written entity or a predicate.
 *
 * Covers:
 *   1. Multi-byte values — a 2-byte value is compared on its whole value (two
 *      values sharing their low byte differ) and the filters that follow a 1-
 *      or 2-byte value are still checked, on create, update and predicate.
 *   2. Order — range filters on signed values (2-byte enum, 8-byte integer
 *      near zero), on unsigned values with their top bit set and on
 *      floating-point values follow the numeric order.
 */

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>

#include "fixtures/test_helper.h"
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/ListDescriptorQuery.h>

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

// [owner int64 GE]
struct DescSigned {
    using Entity = Wide;
    static constexpr auto filters = std::tuple{
        decl::Filter<"owner_min", &Wide::owner, "owner", decl::Op::GE>{}
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

/// The entity belongs to the group: a write of it reaches the group's pages.
template<typename D>
bool matches(const decl::ListQueryParams<D>& group, const Wide& e) {
    return decl::matchesFilters<D>(e, group.filters);
}

/// An update reaches the group when the old or the new state belongs to it.
template<typename D>
bool reachedByUpdate(const decl::ListQueryParams<D>& group, const Wide& oldE, const Wide& newE) {
    return matches(group, oldE) || matches(group, newE);
}

/// A predicate write reaches the group when both can hold a common row.
template<typename D>
bool reachedByPredicate(const decl::ListQueryParams<D>& group,
                        const decl::ListQueryParams<D>& predicate) {
    return decl::predicateGroupCompatible<D>(predicate.filters, group.filters);
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
//  1. Multi-byte values
//
// #############################################################################

TEST_CASE("[ListWidths] create compares a 2-byte value whole and checks the filter after it",
          "[list][widths][unit]") {
    const auto e = wide(7, 1);

    CHECK(matches(tempParams(7, 1), e));
    CHECK_FALSE(matches(tempParams(7, 2), e));        // the owner after the 2-byte value
    CHECK_FALSE(matches(tempParams(7 + 256, 1), e));  // 263 and 7 share their low byte only
}

TEST_CASE("[ListWidths] update compares a 2-byte value whole and checks the filter after it",
          "[list][widths][unit]") {
    const auto oldE = wide(7, 1);
    const auto newE = wide(9, 1);

    CHECK(reachedByUpdate(tempParams(7, 1), oldE, newE));
    CHECK(reachedByUpdate(tempParams(9, 1), oldE, newE));
    CHECK_FALSE(reachedByUpdate(tempParams(7, 2), oldE, newE));
    CHECK_FALSE(reachedByUpdate(tempParams(9 + 256, 1), oldE, newE));
}

TEST_CASE("[ListWidths] a predicate compares a 2-byte value whole and checks the filter after it",
          "[list][widths][unit]") {
    const auto predicate = tempParams(7, 1);

    CHECK(reachedByPredicate(tempParams(7, 1), predicate));
    CHECK_FALSE(reachedByPredicate(tempParams(7, 2), predicate));
    CHECK_FALSE(reachedByPredicate(tempParams(7 + 256, 1), predicate));
}

TEST_CASE("[ListWidths] a 1-byte enum keeps the filters that follow it checked",
          "[list][widths][unit]") {
    auto flagParams = [](Flag flag, int64_t owner) {
        decl::ListQueryParams<DescFlag> q;
        q.filters.get<"flag">() = flag;
        q.filters.get<"owner">() = owner;
        return q;
    };

    Wide e;
    e.flag = Flag::On;
    e.owner = 1;

    CHECK(matches(flagParams(Flag::On, 1), e));
    CHECK_FALSE(matches(flagParams(Flag::Off, 1), e));
    CHECK_FALSE(matches(flagParams(Flag::On, 2), e));
}

// #############################################################################
//
//  2. Order
//
// #############################################################################

TEST_CASE("[ListWidths] a range filter on a signed 2-byte enum follows its sign",
          "[list][widths][unit]") {
    Wide e;
    e.flag = Flag::On;
    e.level = Level::Mid;
    e.owner = 1;

    CHECK(matches(levelParams(Flag::On, Level::Low, 1), e));         // 5 ≥ -300
    CHECK(matches(levelParams(Flag::On, Level::Mid, 1), e));         // 5 ≥ 5
    CHECK_FALSE(matches(levelParams(Flag::On, Level::High, 1), e));  // 5 < 300
    CHECK_FALSE(matches(levelParams(Flag::On, Level::Low, 2), e));   // the owner after the enum
}

TEST_CASE("[ListWidths] a range filter on a negative 8-byte value reads it exactly",
          "[list][widths][unit]") {
    auto ownerParams = [](int64_t v) {
        decl::ListQueryParams<DescSigned> q;
        q.filters.get<"owner_min">() = v;
        return q;
    };

    Wide e;
    e.owner = -2;

    CHECK(matches(ownerParams(-2), e));         // -2 ≥ -2
    CHECK_FALSE(matches(ownerParams(-1), e));   // -2 < -1
    CHECK_FALSE(matches(ownerParams(0), e));    // -2 < 0
}

TEST_CASE("[ListWidths] range filters on unsigned values follow unsigned order",
          "[list][widths][unit]") {
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

    Wide e;
    e.port = 40000;
    e.count = 3'000'000'000u;
    e.big = kHighBit + 5;

    // Each value has its top bit set: read as signed, it would sort below 100.
    CHECK(matches(portParams(100), e));
    CHECK_FALSE(matches(portParams(50000), e));
    CHECK(matches(countParams(100), e));
    CHECK_FALSE(matches(countParams(4'000'000'000u), e));
    CHECK(matches(bigParams(100), e));
    CHECK_FALSE(matches(bigParams(kHighBit + (uint64_t{1} << 20)), e));
}

TEST_CASE("[ListWidths] a range filter on a floating-point value follows numeric order",
          "[list][widths][unit]") {
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

    Wide e;
    e.ratio = -5.0f;
    e.score = -5.0;

    // The IEEE bits of two negatives, read as integers, sort the other way round.
    CHECK(matches(ratioParams(-10.0f), e));         // -5 ≥ -10
    CHECK(matches(scoreParams(-10.0), e));
    CHECK_FALSE(matches(ratioParams(-1.0f), e));    // -5 < -1
    CHECK_FALSE(matches(scoreParams(-1.0), e));
}
