#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/testing/IoContextConformance.h>
#include <jcailloux/relais/io/EpollIoContext.h>

#include <chrono>
#include <functional>
#include <utility>

// Validates that the bundled EpollIoContext honors the full IoContext semantic
// contract, and — just as importantly — proves the conformance harness itself
// works: each check rejects the defect it targets, and an adapter with no more
// than the contract passes them all.

using jcailloux::relais::io::EpollIoContext;
using jcailloux::relais::io::hasEvent;
using jcailloux::relais::io::IoEvent;
using jcailloux::relais::testing::ConformanceError;
using jcailloux::relais::testing::IoContextConformance;

namespace {
// The driver the harness needs: pump this loop on the calling thread until pred.
constexpr auto drive = [](EpollIoContext& io, auto pred) { io.runUntil(pred); };

// Forwards the whole IoContext surface to an EpollIoContext. Each faulty adapter
// below hides one method to reproduce a defect seen in a real adapter, so a
// check can be shown to reject it.
struct ForwardingIoContext {
    using WatchHandle = EpollIoContext::WatchHandle;
    using TimerToken  = EpollIoContext::TimerToken;

    WatchHandle addWatch(int fd, IoEvent events, std::function<void(IoEvent)> cb) {
        return inner.addWatch(fd, events, std::move(cb));
    }
    void removeWatch(WatchHandle h) { inner.removeWatch(h); }
    void updateWatch(WatchHandle h, IoEvent events) { inner.updateWatch(h, events); }
    void post(std::function<void()> cb) { inner.post(std::move(cb)); }
    template<typename Rep, typename Period>
    TimerToken postDelayed(std::chrono::duration<Rep, Period> delay, std::function<void()> cb) {
        return inner.postDelayed(delay, std::move(cb));
    }
    void cancelTimer(TimerToken t) { inner.cancelTimer(t); }

    EpollIoContext inner;
};

// Dispatches one event as a Read callback then a Write callback, without
// re-checking the watch in between: a trantor Channel with separate read and
// write callbacks. The callback stays alive across both calls, so the defect
// shows as a second call, not as a use-after-free.
struct SplitEventIoContext : ForwardingIoContext {
    WatchHandle addWatch(int fd, IoEvent events, std::function<void(IoEvent)> cb) {
        return inner.addWatch(fd, events, [cb = std::move(cb)](IoEvent ev) {
            if (hasEvent(ev, IoEvent::Read))  cb(IoEvent::Read);
            if (hasEvent(ev, IoEvent::Write)) cb(IoEvent::Write);
        });
    }
};

// Defers the whole removal to a later turn, a misreading of "defer teardown":
// the removal lands after a re-watch of the same fd number and tears down the
// new watch.
struct DeferredRemoveIoContext : ForwardingIoContext {
    void removeWatch(WatchHandle h) {
        inner.post([this, h] { inner.removeWatch(h); });
    }
};

// Runs a callback posted on the loop thread at once, as trantor's runInLoop
// does: work posted from a watch callback runs inside it.
struct InlinePostIoContext : ForwardingIoContext {
    void post(std::function<void()> cb) {
        if (inner.isInLoopThread()) cb();
        else inner.post(std::move(cb));
    }
};

// Runs a callback in place on whatever thread posts it: from another thread,
// it never reaches the loop thread.
struct PostInPlaceIoContext : ForwardingIoContext {
    void post(std::function<void()> cb) { cb(); }
};

// Only what the contract asks for: no cancelTimer, and a postDelayed that
// returns nothing.
struct MinimalIoContext {
    using WatchHandle = EpollIoContext::WatchHandle;

    WatchHandle addWatch(int fd, IoEvent events, std::function<void(IoEvent)> cb) {
        return inner.addWatch(fd, events, std::move(cb));
    }
    void removeWatch(WatchHandle h) { inner.removeWatch(h); }
    void updateWatch(WatchHandle h, IoEvent events) { inner.updateWatch(h, events); }
    void post(std::function<void()> cb) { inner.post(std::move(cb)); }
    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        inner.postDelayed(delay, std::move(cb));
    }

    EpollIoContext inner;
};
static_assert(jcailloux::relais::io::IoContext<MinimalIoContext>);

constexpr auto drive_inner = [](auto& io, auto pred) { io.inner.runUntil(pred); };
}  // namespace

TEST_CASE("IoContext conformance: post executes exactly once", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkPostExecutes(io, drive));
}

TEST_CASE("IoContext conformance: post ordering is FIFO", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkPostOrdering(io, drive));
}

TEST_CASE("IoContext conformance: re-post from callback", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkPostFromCallback(io, drive));
}

TEST_CASE("IoContext conformance: watch fires on readable fd", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkWatchReadable(io, drive));
}

TEST_CASE("IoContext conformance: updateWatch changes the mask", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkUpdateWatch(io, drive));
}

TEST_CASE("IoContext conformance: removeWatch stops delivery", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkRemoveWatch(io, drive));
}

TEST_CASE("IoContext conformance: a removed watch's fd number can be watched afresh", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkRewatchReusedFd(io, drive));
}

TEST_CASE("IoContext conformance: a post from a watch callback runs after it", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkPostFromWatchDeferred(io, drive));
}

TEST_CASE("IoContext conformance: the checks reject the adapter defects they target", "[io][conformance]") {
    SECTION("a removal deferred past a re-watch of the same fd number") {
        DeferredRemoveIoContext io;
        REQUIRE_NOTHROW(IoContextConformance::checkRemoveWatch(io, drive_inner));
        REQUIRE_THROWS_AS(IoContextConformance::checkRewatchReusedFd(io, drive_inner),
                          ConformanceError);
    }
    SECTION("a post run in place on the loop thread") {
        InlinePostIoContext io;
        REQUIRE_NOTHROW(IoContextConformance::checkPostFromCallback(io, drive_inner));
        REQUIRE_THROWS_AS(IoContextConformance::checkPostFromWatchDeferred(io, drive_inner),
                          ConformanceError);
    }
    SECTION("a post from another thread run on that thread") {
        PostInPlaceIoContext io;
        REQUIRE_THROWS_AS(IoContextConformance::checkCrossThreadPost(io, drive_inner),
                          ConformanceError);
    }
    SECTION("an event split into several callbacks meets the contract") {
        SplitEventIoContext io;
        REQUIRE_NOTHROW(IoContextConformance::runAll(io, drive_inner));
    }
}

TEST_CASE("IoContext conformance: an adapter without cancelTimer meets the contract", "[io][conformance]") {
    MinimalIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::runAll(io, drive_inner));
}

// The checks runAll no longer runs: properties relais relied on before, still
// public for the adapters that want them.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

TEST_CASE("IoContext conformance: removeWatch is reentrant from its callback", "[io][conformance][deprecated]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkRemoveWatchReentrant(io, drive));
}

TEST_CASE("IoContext conformance: removeWatch stops the rest of a multi-bit event", "[io][conformance][deprecated]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkRemoveWatchWithinEvent(io, drive));
}

TEST_CASE("IoContext conformance: the multi-bit check rejects an event split into several callbacks", "[io][conformance][deprecated]") {
    SplitEventIoContext io;
    REQUIRE_THROWS_AS(IoContextConformance::checkRemoveWatchWithinEvent(io, drive_inner),
                      ConformanceError);
}

TEST_CASE("IoContext conformance: cancelTimer stops a pending timer", "[io][conformance][deprecated]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkCancelTimer(io, drive));
}

TEST_CASE("IoContext conformance: cancelTimer at scale", "[io][conformance][deprecated]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkTimerArmCancelAtScale(io, drive));
}

#pragma GCC diagnostic pop

TEST_CASE("IoContext conformance: cross-thread post on loop thread", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkCrossThreadPost(io, drive));
}

TEST_CASE("IoContext conformance: postDelayed fires once", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkPostDelayedFires(io, drive));
}

TEST_CASE("IoContext conformance: full suite", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::runAll(io, drive));
}