/**
 * test_list_cursor_width.cpp
 * Keyset cursor validation at the trust boundary.
 *
 * A cursor is the sort value plus the N primary-key components, int64 each:
 * exactly sizeof(int64_t) * (1 + N) bytes, N fixed per entity. Cursor::decode
 * rejects any non-empty token of another width, so a truncated or padded token
 * never reaches the keyset SQL (which would drop the keyset clause and serve
 * page 1 under a cache key claiming page N). The strict parser reports such a
 * token as InvalidCursor; the tolerant parser falls back to the first page.
 * No database: decoding and parsing only.
 */

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <unordered_map>

#include <catch2/catch_test_macros.hpp>

#include "fixtures/generated/TestArticleEntity.h"
#include "fixtures/generated/TestCompositeKeyListEntity.h"

#include "jcailloux/relais/list/ListQuery.h"
#include "jcailloux/relais/list/spec/GeneratedTraits.h"
#include "jcailloux/relais/list/spec/HttpQueryParser.h"
#include "jcailloux/relais/config/CacheConfig.h"
#include <jcailloux/relais/repository/Repo.h>

using namespace jcailloux::relais::list::spec;

namespace {

namespace rel = jcailloux::relais;
using ScalarRepo    = rel::Repo<entity::generated::TestArticleEntity, "test:cursor:scalar", rel::config::Local>;
using CompositeRepo = rel::Repo<entity::generated::TestCompositeKeyListEntity, "test:cursor:composite", rel::config::Local>;

using ScalarDesc    = ScalarRepo::ListDescriptorType;     // N = 1 → 16 bytes
using CompositeDesc = CompositeRepo::ListDescriptorType;  // N = 2 → 24 bytes

using Params = std::unordered_map<std::string, std::string>;

/// Base64 token of `width` bytes, each byte distinct so tokens differ by width.
std::string tokenOfWidth(std::size_t width) {
    jcailloux::relais::list::detail::Cursor c;
    for (std::size_t i = 0; i < width; ++i) c.data.push_back(static_cast<std::byte>(i + 1));
    return c.encode();
}

const std::string kScalarToken    = tokenOfWidth(16);
const std::string kCompositeToken = tokenOfWidth(24);
// 15 bytes: the last four characters of a 16-byte token dropped.
const std::string kTruncated = kScalarToken.substr(0, kScalarToken.size() - 4);

}  // namespace

// --- Encode ----------------------------------------------------------------

TEST_CASE("cursor encode is standard padded base64", "[list][cursor]") {
    using jcailloux::relais::list::detail::Cursor;
    auto bytes = [](std::initializer_list<int> v) {
        Cursor c;
        for (int b : v) c.data.push_back(static_cast<std::byte>(b));
        return c;
    };
    CHECK(bytes({0x01}).encode() == "AQ==");
    CHECK(bytes({0x01, 0x02}).encode() == "AQI=");
    CHECK(bytes({0x01, 0x02, 0x03}).encode() == "AQID");

    for (std::size_t width = 1; width <= 32; ++width) {
        Cursor c;
        for (std::size_t i = 0; i < width; ++i) c.data.push_back(static_cast<std::byte>(0xF0 + i));
        auto back = Cursor::decode(c.encode());
        REQUIRE(back.has_value());
        CHECK(*back == c);
    }
}

// --- Decode ----------------------------------------------------------------

TEST_CASE("cursor decode accepts exactly the format width", "[list][cursor]") {
    SECTION("a server-minted token, scalar key") {
        auto c = ScalarRepo::Cursor::decode(kScalarToken);
        REQUIRE(c.has_value());
        CHECK(c->size() == 16);
    }
    SECTION("a server-minted token, composite key") {
        auto c = CompositeRepo::Cursor::decode(kCompositeToken);
        REQUIRE(c.has_value());
        CHECK(c->size() == 24);
    }
    SECTION("an absent token is the first page") {
        auto c = ScalarRepo::Cursor::decode("");
        REQUIRE(c.has_value());
        CHECK(c->empty());
    }
}

TEST_CASE("cursor decode rejects any other width", "[list][cursor]") {
    CHECK_FALSE(ScalarRepo::Cursor::decode(kTruncated).has_value());
    CHECK_FALSE(ScalarRepo::Cursor::decode(tokenOfWidth(17)).has_value());
    // Non-empty tokens that decode to zero bytes: padding only, then 6 bits.
    CHECK_FALSE(ScalarRepo::Cursor::decode("=").has_value());
    CHECK_FALSE(ScalarRepo::Cursor::decode("A").has_value());
    CHECK_FALSE(ScalarRepo::Cursor::decode("!!!!").has_value());
    // Right type, wrong width: a key that changed shape between deploys.
    CHECK_FALSE(CompositeRepo::Cursor::decode(kScalarToken).has_value());
    CHECK_FALSE(ScalarRepo::Cursor::decode(kCompositeToken).has_value());
}

// --- Strict parser -----------------------------------------------------------

TEST_CASE("strict parser rejects an unreadable cursor", "[list][cursor][parse]") {
    SECTION("a valid token sets the cursor") {
        auto q = parseListQueryStrict<ScalarDesc>(Params{{"after", kScalarToken}});
        REQUIRE(q.has_value());
        CHECK(q->cursor().size() == 16);
    }
    SECTION("a token of the wrong width") {
        auto q = parseListQueryStrict<ScalarDesc>(Params{{"after", kTruncated}});
        REQUIRE_FALSE(q.has_value());
        CHECK(q.error().type == QueryValidationError::Type::InvalidCursor);
    }
    SECTION("a token outside the base64 alphabet") {
        auto q = parseListQueryStrict<ScalarDesc>(Params{{"after", "!!!!"}});
        REQUIRE_FALSE(q.has_value());
        CHECK(q.error().type == QueryValidationError::Type::InvalidCursor);
    }
    SECTION("the error names the parameter, never echoes the token") {
        auto q = parseListQueryStrict<ScalarDesc>(Params{{"after", kTruncated}});
        REQUIRE_FALSE(q.has_value());
        CHECK(q.error().field == "after");
        CHECK(q.error().message().find(kTruncated) == std::string::npos);
    }
    SECTION("an empty or absent token is the first page") {
        auto empty = parseListQueryStrict<ScalarDesc>(Params{{"after", ""}});
        REQUIRE(empty.has_value());
        CHECK(empty->cursor().empty());

        auto absent = parseListQueryStrict<ScalarDesc>(Params{});
        REQUIRE(absent.has_value());
        CHECK(absent->cursor().empty());
    }
    SECTION("an unreadable cursor is reported before a pagination conflict") {
        auto bad = parseListQueryStrict<ScalarDesc>(Params{{"after", kTruncated}, {"offset", "10"}});
        REQUIRE_FALSE(bad.has_value());
        CHECK(bad.error().type == QueryValidationError::Type::InvalidCursor);

        auto good = parseListQueryStrict<ScalarDesc>(Params{{"after", kScalarToken}, {"offset", "10"}});
        REQUIRE_FALSE(good.has_value());
        CHECK(good.error().type == QueryValidationError::Type::ConflictingPagination);
    }
}

// --- Tolerant parser -------------------------------------------------------

TEST_CASE("tolerant parser serves the first page under its own key", "[list][cursor][parse]") {
    const auto first = parseListQuery<ScalarDesc>(Params{});

    SECTION("a token of the wrong width is dropped") {
        auto q = parseListQuery<ScalarDesc>(Params{{"after", kTruncated}});
        CHECK(q.cursor().empty());
        CHECK(q.cacheKey() == first.cacheKey());
    }
    SECTION("a valid token is kept") {
        auto q = parseListQuery<ScalarDesc>(Params{{"after", kScalarToken}});
        CHECK(q.cursor().size() == 16);
        CHECK(q.cacheKey() != first.cacheKey());
    }
}

// --- Cache key ---------------------------------------------------------------

TEST_CASE("the cursor keys the page", "[list][cursor][cache]") {
    auto a = parseListQuery<ScalarDesc>(Params{{"after", kScalarToken}});
    auto b = parseListQuery<ScalarDesc>(Params{{"after", kScalarToken}});
    CHECK(a.cacheKey() == b.cacheKey());
    CHECK(a.groupKey() == parseListQuery<ScalarDesc>(Params{}).groupKey());
}
