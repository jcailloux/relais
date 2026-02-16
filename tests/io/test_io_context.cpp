#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/io/IoContext.h>
#include <functional>
#include <vector>

using namespace jcailloux::relais::io;

// =============================================================================
// Mock IoContext for concept validation
// =============================================================================

struct MockIoContext {
    using WatchHandle = int;

    struct WatchEntry {
        int fd;
        IoEvent events;
        std::function<void(IoEvent)> callback;
    };

    std::vector<WatchEntry> watches;
    std::vector<std::function<void()>> posted;
    int next_handle = 1;

    WatchHandle addWatch(int fd, IoEvent events, std::function<void(IoEvent)> cb) {
        watches.push_back({fd, events, std::move(cb)});
        return next_handle++;
    }

    void removeWatch(WatchHandle /*handle*/) {}

    void updateWatch(WatchHandle /*handle*/, IoEvent /*events*/) {}

    void post(std::function<void()> cb) {
        posted.push_back(std::move(cb));
    }
};

// Compile-time verification that MockIoContext satisfies the concept
static_assert(jcailloux::relais::io::IoContext<MockIoContext>,
    "MockIoContext must satisfy IoContext concept");

// =============================================================================
// IoEvent bitmask tests
// =============================================================================

TEST_CASE("IoEvent bitmask operations", "[iocontext]") {
    SECTION("OR combines flags") {
        auto rw = IoEvent::Read | IoEvent::Write;
        REQUIRE(hasEvent(rw, IoEvent::Read));
        REQUIRE(hasEvent(rw, IoEvent::Write));
        REQUIRE_FALSE(hasEvent(rw, IoEvent::Error));
    }

    SECTION("AND extracts flags") {
        auto all = IoEvent::Read | IoEvent::Write | IoEvent::Error;
        REQUIRE((all & IoEvent::Read) == IoEvent::Read);
    }

    SECTION("None has no events") {
        REQUIRE_FALSE(hasEvent(IoEvent::None, IoEvent::Read));
        REQUIRE_FALSE(hasEvent(IoEvent::None, IoEvent::Write));
        REQUIRE_FALSE(hasEvent(IoEvent::None, IoEvent::Error));
    }
}

// =============================================================================
// MockIoContext behavioral tests
// =============================================================================

TEST_CASE("MockIoContext addWatch registers fd", "[iocontext]") {
    MockIoContext ctx;
    bool called = false;
    auto handle = ctx.addWatch(5, IoEvent::Read, [&](IoEvent) { called = true; });
    REQUIRE(handle == 1);
    REQUIRE(ctx.watches.size() == 1);
    REQUIRE(ctx.watches[0].fd == 5);

    // Simulate event
    ctx.watches[0].callback(IoEvent::Read);
    REQUIRE(called);
}

TEST_CASE("MockIoContext post schedules callback", "[iocontext]") {
    MockIoContext ctx;
    int value = 0;
    ctx.post([&] { value = 42; });
    REQUIRE(ctx.posted.size() == 1);
    ctx.posted[0]();
    REQUIRE(value == 42);
}
