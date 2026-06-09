#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/testing/IoContextConformance.h>
#include <jcailloux/relais/io/EpollIoContext.h>

// Validates that the bundled EpollIoContext honors the full IoContext semantic
// contract, and — just as importantly — proves the conformance harness itself
// works, so adapter authors (e.g. a trantor shim for Drogon) can trust it.

using jcailloux::relais::io::EpollIoContext;
using jcailloux::relais::testing::IoContextConformance;

namespace {
// The driver the harness needs: pump this loop on the calling thread until pred.
constexpr auto drive = [](EpollIoContext& io, auto pred) { io.runUntil(pred); };
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

TEST_CASE("IoContext conformance: cross-thread post on loop thread", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::checkCrossThreadPost(io, drive));
}

TEST_CASE("IoContext conformance: full suite", "[io][conformance]") {
    EpollIoContext io;
    REQUIRE_NOTHROW(IoContextConformance::runAll(io, drive));
}