#include <catch2/catch_test_macros.hpp>

#include <fixtures/ControllableIoContext.h>
#include <jcailloux/relais/testing/IoContextConformance.h>

#include <chrono>
#include <cstdint>

#include <sys/eventfd.h>
#include <unistd.h>

using jcailloux::relais::io::IoEvent;
using jcailloux::relais::io::test::ControllableIoContext;
using jcailloux::relais::testing::IoContextConformance;

namespace {
constexpr auto drive = [](ControllableIoContext& io, auto pred) { io.runUntil(pred); };
}  // namespace

// The wrapper is only useful if it is a faithful IoContext: forwarding must not
// break any semantic the harness checks. If this passes, every liveness test can
// use ControllableIoContext as a drop-in for EpollIoContext.
TEST_CASE("ControllableIoContext: full IoContext conformance", "[io][conformance][fixture]") {
    ControllableIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::runAll(io, drive));
}

// The fault it exists to inject: a held fd does not deliver its buffered
// readiness, but timers keep firing — the precondition for "committed +
// query_timeout". releasing re-delivers via level-triggered re-signal.
TEST_CASE("ControllableIoContext: holdReads withholds readiness; timers still fire",
          "[io][fixture]") {
    ControllableIoContext io;

    int efd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    REQUIRE(efd >= 0);

    int reads = 0;
    auto h = io.addWatch(efd, IoEvent::Read, [&](IoEvent) { ++reads; });

    // Hold reads, then make the fd readable: the watch must NOT fire while held.
    io.holdReads(efd);
    REQUIRE(io.isHeld(efd));
    uint64_t one = 1;
    [[maybe_unused]] auto w = ::write(efd, &one, sizeof(one));

    // A timer must still fire while the read is withheld — this is exactly what
    // lets a query_timeout expire on a connection whose result is being held.
    bool timer_fired = false;
    io.postDelayed(std::chrono::milliseconds(1), [&] { timer_fired = true; });
    io.runUntil([&] { return timer_fired; });

    REQUIRE(reads == 0);       // buffered data not delivered while held
    REQUIRE(timer_fired);      // timers unaffected by the read hold

    // Release: level-triggered epoll re-signals the still-pending data.
    io.releaseReads(efd);
    REQUIRE_FALSE(io.isHeld(efd));
    io.runUntil([&] { return reads > 0; });
    REQUIRE(reads >= 1);

    io.removeWatch(h);
    ::close(efd);
}

// holdReads must leave the wakeup counter and other fds alone — it is a pure
// read-mask flip, not a loop disturbance.
TEST_CASE("ControllableIoContext: hold/release issues no spurious wakeups",
          "[io][fixture]") {
    ControllableIoContext io;

    int efd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    REQUIRE(efd >= 0);
    auto h = io.addWatch(efd, IoEvent::Read, [&](IoEvent) {});

    // Drive once so loop_thread_ is set, then measure across a hold/release that
    // both run on the loop thread (no cross-thread post → no write(pipe)).
    io.post([&] {
        uint64_t before = io.loopWakeups();
        io.holdReads(efd);
        io.releaseReads(efd);
        REQUIRE(io.loopWakeups() == before);  // mask flips, no pipe write
    });
    bool done = false;
    io.post([&] { done = true; });
    io.runUntil([&] { return done; });

    io.removeWatch(h);
    ::close(efd);
}
