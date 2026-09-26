/**
 * test_list_key_golden.cpp
 *
 * Byte-exact format of the list cache encodings.
 *
 * The page and group keys name the L1 entries and the L2 keys; the entity
 * blob and the predicate blob are read byte by byte by the Lua matchers. A
 * change in any of them silently orphans the L2 keys already written and
 * desynchronizes the matchers, so each encoding is pinned here to its exact
 * bytes (little-endian host) over a corpus that covers every value width and
 * signedness, optional members null and present, strings (empty, short, past
 * the small-string buffer), IN/NIN sets unsorted and with duplicates, bool
 * sets, raw and mapped enums, with and without sort, cursor and offset.
 *
 * Pure unit test (no DB / Redis).
 */

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "fixtures/generated/TestTicketEntity.h"
#include <jcailloux/relais/list/spec/CanonicalEncoding.h>
#include <jcailloux/relais/list/spec/ListQueryBuilder.h>

namespace decl = jcailloux::relais::list::spec;
using jcailloux::relais::list::SortDirection;
using jcailloux::relais::list::SortSpec;

namespace {

enum class Level : int16_t { Low = -300, Mid = 5, High = 300 };
enum class Flag : uint8_t { On = 1, Off = 2 };

struct Row {
    int64_t id = 0;
    int8_t i8 = 0;
    uint16_t u16 = 0;
    int32_t i32 = 0;
    uint32_t u32 = 0;
    int64_t i64 = 0;
    uint64_t u64 = 0;
    float f32 = 0.0f;
    double f64 = 0.0;
    bool flag_b = false;
    Flag flag = Flag::On;
    Level level = Level::Mid;
    std::optional<int32_t> opt;
    std::string name;

    // Filtered only: never read from the database.
    static std::optional<Row> fromRow(const jcailloux::relais::io::PgResult::Row&) {
        return std::nullopt;
    }
};

// Every scalar width and signedness, raw enums, an optional member, a string.
struct ScalarDesc {
    using Entity = Row;
    static constexpr auto filters = std::tuple{
        decl::Filter<"i8", &Row::i8, "i8", decl::Op::EQ>{},
        decl::Filter<"u16_min", &Row::u16, "u16", decl::Op::GE>{},
        decl::Filter<"i32_max", &Row::i32, "i32", decl::Op::LE>{},
        decl::Filter<"u32", &Row::u32, "u32", decl::Op::EQ>{},
        decl::Filter<"i64_gt", &Row::i64, "i64", decl::Op::GT>{},
        decl::Filter<"u64", &Row::u64, "u64", decl::Op::EQ>{},
        decl::Filter<"f32_min", &Row::f32, "f32", decl::Op::GE>{},
        decl::Filter<"f64_lt", &Row::f64, "f64", decl::Op::LT>{},
        decl::Filter<"flag_b", &Row::flag_b, "flag_b", decl::Op::EQ>{},
        decl::Filter<"flag", &Row::flag, "flag", decl::Op::EQ>{},
        decl::Filter<"level_ne", &Row::level, "level", decl::Op::NE>{},
        decl::Filter<"opt", &Row::opt, "opt", decl::Op::EQ>{},
        decl::Filter<"name", &Row::name, "name", decl::Op::EQ>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &Row::id, "id", SortDirection::Asc>{},
        decl::Sort<"i64", &Row::i64, "i64", SortDirection::Desc>{}
    };
};

// Set filters: strings IN, int64 NIN, bool IN, then a scalar after them.
struct SetDesc {
    using Entity = Row;
    static constexpr auto filters = std::tuple{
        decl::Filter<"names", &Row::name, "name", decl::Op::IN>{},
        decl::Filter<"not_i64", &Row::i64, "i64", decl::Op::NIN>{},
        decl::Filter<"flags_b", &Row::flag_b, "flag_b", decl::Op::IN>{},
        decl::Filter<"u32", &Row::u32, "u32", decl::Op::EQ>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"id", &Row::id, "id", SortDirection::Desc>{}
    };
};

// Mapped enum (Via<Codec>) filters, scalar and set, from the generator.
struct TicketDesc : ::entity::generated::TestTicketEntity::MappingType::ListDescriptor {
    using Entity = ::entity::generated::TestTicketEntity;
};

using relais_test::TicketState;

std::string hex(std::string_view bytes) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) {
        out += kDigits[c >> 4];
        out += kDigits[c & 0x0F];
    }
    return out;
}

template<typename D>
decl::TypedCursor<D> cursorOf(std::string_view token) {
    auto c = decl::TypedCursor<D>::decode(token);
    REQUIRE(c.has_value());
    return *c;
}

// Group key, page key and predicate blob of one params bundle, through every
// producer: seal(), the free functions, and the appenders behind a prefix.
template<typename D>
void checkParams(const decl::ListQueryParams<D>& p,
                 std::string_view group, std::string_view page, std::string_view predicate) {
    const auto q = decl::seal<D>(p);
    CHECK(hex(q.groupKey()) == group);
    CHECK(hex(q.cacheKey()) == page);
    CHECK(hex(decl::groupKey<D>(p.filters, p.sort)) == group);
    CHECK(hex(decl::cacheKey<D>(q.groupKey(), p)) == page);
    CHECK(hex(decl::encodeFilterSet<D>(p.filters)) == predicate);

    const std::string prefix = "p:";
    std::string appended = prefix;
    decl::appendGroupKey<D>(appended, q);
    CHECK(hex(appended) == hex(prefix) + std::string(group));
    appended = prefix;
    decl::appendPageKey<D>(appended, q);
    CHECK(hex(appended) == hex(prefix) + std::string(page));
}

}  // namespace

// #############################################################################
//
//  1. Keys and predicate blob
//
// #############################################################################

TEST_CASE("[ListKeyGolden] no filter, no sort", "[list][golden][unit]") {
    decl::ListQueryParams<ScalarDesc> p;
    p.limit = 10;
    checkParams(p,
        "0000000000000000000000000000",
        "00000000000000000000000000000a00",
        "00000000000000000000000000");
}

TEST_CASE("[ListKeyGolden] every scalar width, present", "[list][golden][unit]") {
    decl::ListQueryParams<ScalarDesc> p;
    auto& f = p.filters;
    f.get<"i8">() = int8_t{-5};
    f.get<"u16_min">() = uint16_t{0xBEEF};
    f.get<"i32_max">() = int32_t{-123456};
    f.get<"u32">() = uint32_t{0xDEADBEEF};
    f.get<"i64_gt">() = int64_t{-9000000000LL};
    f.get<"u64">() = uint64_t{0xFEDCBA9876543210ULL};
    f.get<"f32_min">() = 1.5f;
    f.get<"f64_lt">() = -2.25;
    f.get<"flag_b">() = true;
    f.get<"flag">() = Flag::Off;
    f.get<"level_ne">() = Level::Low;
    f.get<"opt">() = int32_t{42};
    f.get<"name">() = std::string("short");
    p.sort = SortSpec<size_t>{1, SortDirection::Desc};
    p.limit = 25;
    checkParams(p,
        "01fb01efbe01c01dfeff01efbeadde0100e68ee7fdffffff011032547698badc"
        "fe010000c03f0100000000000002c00101010201d4fe012a0000000105000000"
        "73686f727401010000000000000001",
        "01fb01efbe01c01dfeff01efbeadde0100e68ee7fdffffff011032547698badc"
        "fe010000c03f0100000000000002c00101010201d4fe012a0000000105000000"
        "73686f7274010100000000000000011900",
        "01fb01efbe01c01dfeff01efbeadde0100e68ee7fdffffff011032547698badc"
        "fe010000c03f0100000000000002c00101010201d4fe012a0000000105000000"
        "73686f7274");
}

TEST_CASE("[ListKeyGolden] strings: empty and past the small-string buffer",
          "[list][golden][unit]") {
    decl::ListQueryParams<ScalarDesc> empty;
    empty.filters.get<"name">() = std::string();
    empty.limit = 5;
    checkParams(empty,
        "000000000000000000000000010000000000",
        "0000000000000000000000000100000000000500",
        "0000000000000000000000000100000000");

    decl::ListQueryParams<ScalarDesc> longName;
    longName.filters.get<"name">() = std::string("a name well past fifteen bytes");
    longName.filters.get<"opt">() = int32_t{-1};
    longName.sort = SortSpec<size_t>{0, SortDirection::Asc};
    longName.limit = 50;
    checkParams(longName,
        "000000000000000000000001ffffffff011e00000061206e616d652077656c6c"
        "2070617374206669667465656e20627974657301000000000000000000",
        "000000000000000000000001ffffffff011e00000061206e616d652077656c6c"
        "2070617374206669667465656e206279746573010000000000000000003200",
        "000000000000000000000001ffffffff011e00000061206e616d652077656c6c"
        "2070617374206669667465656e206279746573");
}

TEST_CASE("[ListKeyGolden] pagination: cursor, offset, cursor over offset",
          "[list][golden][unit]") {
    decl::ListQueryParams<ScalarDesc> base;
    base.filters.get<"i64_gt">() = int64_t{7};
    base.sort = SortSpec<size_t>{1, SortDirection::Asc};
    base.limit = 10;

    auto cursor = base;
    cursor.cursor = cursorOf<ScalarDesc>("AAECAwQFBgcICQoL");
    checkParams(cursor,
        "00000000010700000000000000000000000000000001010000000000000000",
        "000000000107000000000000000000000000000000010100000000000000000a"
        "000c000000000102030405060708090a0b",
        "000000000107000000000000000000000000000000");

    auto offset = base;
    offset.offset = 40;
    checkParams(offset,
        "00000000010700000000000000000000000000000001010000000000000000",
        "000000000107000000000000000000000000000000010100000000000000000a"
        "004f28000000",
        "000000000107000000000000000000000000000000");

    auto both = cursor;
    both.offset = 40;
    checkParams(both,
        "00000000010700000000000000000000000000000001010000000000000000",
        "000000000107000000000000000000000000000000010100000000000000000a"
        "000c000000000102030405060708090a0b",
        "000000000107000000000000000000000000000000");
}

TEST_CASE("[ListKeyGolden] set filters: unsorted, duplicates, bool", "[list][golden][unit]") {
    decl::ListQueryParams<SetDesc> p;
    p.filters.get<"names">() = std::vector<std::string>{
        "travel", "art", "a category name past fifteen bytes", "art", ""};
    p.filters.get<"not_i64">() = std::vector<int64_t>{30, -2, 30, 1LL << 40};
    p.filters.get<"flags_b">() = std::vector<bool>{true, false, true};
    p.filters.get<"u32">() = uint32_t{9};
    p.sort = SortSpec<size_t>{0, SortDirection::Desc};
    p.limit = 20;
    checkParams(p,
        "01040000000000000022000000612063617465676f7279206e616d6520706173"
        "74206669667465656e206279746573030000006172740600000074726176656c"
        "0103000000feffffffffffffff1e000000000000000000000000010000010200"
        "00000001010900000001000000000000000001",
        "01040000000000000022000000612063617465676f7279206e616d6520706173"
        "74206669667465656e206279746573030000006172740600000074726176656c"
        "0103000000feffffffffffffff1e000000000000000000000000010000010200"
        "000000010109000000010000000000000000011400",
        "01040000000000000022000000612063617465676f7279206e616d6520706173"
        "74206669667465656e206279746573030000006172740600000074726176656c"
        "0103000000feffffffffffffff1e000000000000000000000000010000010200"
        "000000010109000000");

    decl::ListQueryParams<SetDesc> single;
    single.filters.get<"flags_b">() = std::vector<bool>{false};
    single.limit = 20;
    checkParams(single,
        "00000101000000000000",
        "000001010000000000001400",
        "000001010000000000");
}

TEST_CASE("[ListKeyGolden] mapped enum filters", "[list][golden][unit]") {
    decl::ListQueryParams<TicketDesc> p;
    p.filters.get<"queue">() = int64_t{3};
    p.filters.get<"state_in">() = std::vector<TicketState>{
        TicketState::Closed, TicketState::Open, TicketState::Closed};
    p.filters.get<"state_min">() = TicketState::Blocked;
    p.filters.get<"weight">() = int32_t{-8};
    p.sort = SortSpec<size_t>{1, SortDirection::Asc};
    p.limit = 10;
    checkParams(p,
        "01030000000000000000000102000000050000000a0000000000011400000000"
        "0001f8ffffff01010000000000000000",
        "01030000000000000000000102000000050000000a0000000000011400000000"
        "0001f8ffffff010100000000000000000a00",
        "01030000000000000000000102000000050000000a0000000000011400000000"
        "0001f8ffffff");
}

TEST_CASE("[ListKeyGolden] sealing canonicalizes the sets", "[list][golden][unit]") {
    auto raw = decl::ListQueryBuilder<SetDesc>{}
        .filter<"names">(std::vector<std::string>{"travel", "art", "", "art"})
        .filter<"not_i64">(std::vector<int64_t>{30, -2, 30, 1LL << 40})
        .filter<"flags_b">(std::vector<bool>{true, false, true})
        .limit(20)
        .build();
    auto canonical = decl::ListQueryBuilder<SetDesc>{}
        .filter<"names">(std::vector<std::string>{"", "art", "travel"})
        .filter<"not_i64">(std::vector<int64_t>{-2, 30, 1LL << 40})
        .filter<"flags_b">(std::vector<bool>{false, true})
        .limit(20)
        .build();

    CHECK(raw.groupKey() == canonical.groupKey());
    CHECK(raw.cacheKey() == canonical.cacheKey());
    CHECK(raw == canonical);
    CHECK(*raw.filters().get<"names">() == std::vector<std::string>{"", "art", "travel"});
    CHECK(*raw.filters().get<"not_i64">() == std::vector<int64_t>{-2, 30, 1LL << 40});
    CHECK(*raw.filters().get<"flags_b">() == std::vector<bool>{false, true});

    // Past the stack index array: same canonical form.
    std::vector<std::string> many, expected;
    for (int i = 40; i-- > 0;) many.push_back("category " + std::to_string(i % 17));
    for (int i = 0; i < 17; ++i) expected.push_back("category " + std::to_string(i));
    std::sort(expected.begin(), expected.end());
    const auto big = decl::ListQueryBuilder<SetDesc>{}.filter<"names">(many).build();
    CHECK(*big.filters().get<"names">() == expected);
    CHECK(big.groupKey() == decl::ListQueryBuilder<SetDesc>{}.filter<"names">(expected).build().groupKey());
}

// #############################################################################
//
//  2. Entity blob
//
// #############################################################################

TEST_CASE("[ListKeyGolden] entity blob", "[list][golden][unit]") {
    Row r;
    r.i8 = -5;
    r.u16 = 0xBEEF;
    r.i32 = -123456;
    r.u32 = 0xDEADBEEF;
    r.i64 = -9000000000LL;
    r.u64 = 0xFEDCBA9876543210ULL;
    r.f32 = 1.5f;
    r.f64 = -2.25;
    r.flag_b = true;
    r.flag = Flag::Off;
    r.level = Level::Low;
    r.name = "a name well past fifteen bytes";

    // Optional member null, then present.
    CHECK(hex(decl::encodeEntityFilterBlob<ScalarDesc>(r)) ==
          "01fb01efbe01c01dfeff01efbeadde0100e68ee7fdffffff011032547698badc"
          "fe010000c03f0100000000000002c00101010201d4fe00011e00000061206e61"
          "6d652077656c6c2070617374206669667465656e206279746573");
    r.opt = 42;
    CHECK(hex(decl::encodeEntityFilterBlob<ScalarDesc>(r)) ==
          "01fb01efbe01c01dfeff01efbeadde0100e68ee7fdffffff011032547698badc"
          "fe010000c03f0100000000000002c00101010201d4fe012a000000011e000000"
          "61206e616d652077656c6c2070617374206669667465656e206279746573");

    // Set filters encode the entity's single scalar value.
    CHECK(hex(decl::encodeEntityFilterBlob<SetDesc>(r)) ==
          "011e00000061206e616d652077656c6c2070617374206669667465656e206279"
          "7465730100e68ee7fdffffff010101efbeadde");

    ::entity::generated::TestTicketEntity t;
    t.queue_id = 3;
    t.state = TicketState::Blocked;
    t.weight = -8;
    CHECK(hex(decl::encodeEntityFilterBlob<TicketDesc>(t)) ==
          "0103000000000000000114000000011400000001140000000114000000011400"
          "000001140000000114000000011400000001f8ffffff");
}
