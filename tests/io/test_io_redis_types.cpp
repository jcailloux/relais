#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/io/redis/RedisError.h>
#include <jcailloux/relais/io/redis/RedisResult.h>
#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/io/IoContext.h>
#include <functional>

using namespace jcailloux::relais::io;

// Mock IoContext
struct TestIo {
    using WatchHandle = int;
    int next_handle = 1;

    WatchHandle addWatch(int, IoEvent, std::function<void(IoEvent)>) { return next_handle++; }
    void removeWatch(WatchHandle) {}
    void updateWatch(WatchHandle, IoEvent) {}
    void post(std::function<void()>) {}
};

static_assert(IoContext<TestIo>);

// Verify RedisClient compiles with TestIo
using TestRedisClient = RedisClient<TestIo>;

// =============================================================================
// RedisResult tests (no server needed)
// =============================================================================

TEST_CASE("RedisResult default is nil", "[redis][result]") {
    RedisResult r;
    REQUIRE_FALSE(r.valid());
    REQUIRE(r.isNil());
    REQUIRE_FALSE(r.isString());
    REQUIRE_FALSE(r.isInteger());
    REQUIRE_FALSE(r.isArray());
}

TEST_CASE("RedisResult asStringView on nil returns empty", "[redis][result]") {
    RedisResult r;
    REQUIRE(r.asStringView().empty());
}

TEST_CASE("RedisResult asInteger on nil returns 0", "[redis][result]") {
    RedisResult r;
    REQUIRE(r.asInteger() == 0);
}

// =============================================================================
// RedisError hierarchy
// =============================================================================

TEST_CASE("RedisError hierarchy", "[redis][error]") {
    REQUIRE_THROWS_AS(throw RedisError("test"), std::runtime_error);
    REQUIRE_THROWS_AS(throw RedisConnectionError("conn lost"), RedisError);
}

// =============================================================================
// RedisClient compilation check
// =============================================================================

TEST_CASE("RedisClient compiles with mock IoContext", "[redis][client]") {
    REQUIRE(true);
}
