/**
 * test_maintenance.cpp
 *
 * The maintenance thread: the global sweep runs on it, never on the thread
 * that ticks. No database: a registry entry of the test (a probe) records
 * each sweep it takes part in, and can hold it.
 *
 *   1. A tick that asks for a sweep returns while the sweep runs elsewhere
 *   2. A cycle of ticks sweeps each chunk once
 *   3. A backlog is served up to one cycle
 *   4. A forced pass waiting for a sweep goes before the next one
 *   5. A sweep that throws is dropped, the next ones are served
 *   6. A reset drops the sweeps not served yet
 *   7. A requested pass returns at once, and goes before the next sweep
 *   8. Two requests made before the pass starts ask for one pass
 *   9. A request made during the pass asks for another
 *  10. A pass that throws is dropped, the next sweeps are served
 *  11. A reset drops the passes not served yet
 */

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#if defined(__linux__)
#include <pthread.h>
#endif

#include <jcailloux/relais/Log.h>
#include <jcailloux/relais/cache/GDSFPolicy.h>
#include <jcailloux/relais/repository/Repo.h>

#include "fixtures/RelaisTestAccessors.h"

namespace {

using jcailloux::relais::cache::GDSFPolicy;
using relais_test::TestInternals;
using namespace std::chrono_literals;

/// A registry entry that records each sweep it takes part in. Enrolled once;
/// armed for one test by its constructor, disarmed by its destructor.
struct Probe {
    struct Sweep {
        long chunk;
        std::thread::id thread;
        std::string thread_name;  // where the system names threads
    };

    static inline std::mutex mu;
    static inline std::vector<Sweep> sweeps;  // under mu
    static inline std::atomic<bool> armed{false};
    static inline std::atomic<bool> hold{false};
    static inline std::atomic<uint32_t> entered{0};
    static inline std::atomic<int> throws{0};  // sweeps left that throw

    static size_t sweep(long chunk) {
        if (!armed.load(std::memory_order_acquire)) return 0;
        std::string name;
#if defined(__linux__)
        char buf[16] = {};
        if (::pthread_getname_np(::pthread_self(), buf, sizeof buf) == 0) name = buf;
#endif
        {
            std::lock_guard lock(mu);
            sweeps.push_back({chunk, std::this_thread::get_id(), std::move(name)});
        }
        entered.fetch_add(1);
        entered.notify_all();
        // Bounded: a sweep run on the ticking thread would block it for good.
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (hold.load(std::memory_order_acquire)
               && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(1ms);
        }
        if (throws.load() > 0) {
            throws.fetch_sub(1);
            throw std::runtime_error("probe");
        }
        return 0;
    }

    static size_t size() { return 0; }

    Probe() {
        static std::once_flag enrolled;
        std::call_once(enrolled, [] {
            GDSFPolicy::instance().enroll({&sweep, &size, "test:probe"});
        });
        // No request pending, cursor and tick counter at 0.
        GDSFPolicy::instance().resetForTesting();
        {
            std::lock_guard lock(mu);
            sweeps.clear();
        }
        entered.store(0);
        hold.store(false);
        throws.store(0);
        armed.store(true, std::memory_order_release);
    }

    ~Probe() {
        hold.store(false, std::memory_order_release);
        GDSFPolicy::instance().awaitSweeps();
        armed.store(false, std::memory_order_release);
    }

    Probe(const Probe&) = delete;
    Probe& operator=(const Probe&) = delete;

    static std::vector<Sweep> taken() {
        std::lock_guard lock(mu);
        return sweeps;
    }

    /// True once n sweeps have entered the probe, within 2 s.
    static bool awaitEntered(uint32_t n) {
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (entered.load() < n) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::sleep_for(1ms);
        }
        return true;
    }
};

void tick(size_t n) {
    for (size_t i = 0; i < n; ++i) GDSFPolicy::instance().tickInsertion();
}

size_t ticksPerSweep() { return GDSFPolicy::kCleanupMask + 1; }

size_t chunkCount() {
    return static_cast<size_t>(GDSFPolicy::instance().chunkCount());
}

}  // namespace

// #############################################################################
//
//  1. A tick that asks for a sweep returns while the sweep runs elsewhere
//
// #############################################################################

TEST_CASE("Maintenance - the tick that asks for a sweep does not run it",
          "[maintenance]")
{
    Probe probe;
    Probe::hold.store(true);

    tick(ticksPerSweep() - 1);
    const auto t0 = std::chrono::steady_clock::now();
    tick(1);  // asks for the sweep
    const auto tick_time = std::chrono::steady_clock::now() - t0;

    // The probe holds the sweep for 2 s: a tick that ran it would last as long.
    CHECK(tick_time < 100ms);
    REQUIRE(Probe::awaitEntered(1));
    const auto sweeps = Probe::taken();
    REQUIRE(sweeps.size() == 1);
    CHECK(sweeps[0].chunk == 0);
    CHECK(sweeps[0].thread != std::this_thread::get_id());
}

// #############################################################################
//
//  2. A cycle of ticks sweeps each chunk once
//
// #############################################################################

TEST_CASE("Maintenance - a cycle of ticks sweeps each chunk once",
          "[maintenance]")
{
    Probe probe;

    tick(TestInternals::sweepCycleTicks());
    GDSFPolicy::instance().awaitSweeps();

    const auto sweeps = Probe::taken();
    REQUIRE(sweeps.size() == chunkCount());
    for (size_t i = 0; i < sweeps.size(); ++i) {
        CHECK(sweeps[i].chunk == static_cast<long>(i));
        CHECK(sweeps[i].thread == sweeps[0].thread);
    }
    CHECK(sweeps[0].thread != std::this_thread::get_id());
#if defined(__linux__)
    CHECK(sweeps[0].thread_name == "relais-sweep");
#endif
}

// #############################################################################
//
//  3. A backlog is served up to one cycle
//
// #############################################################################

TEST_CASE("Maintenance - a backlog is served up to one cycle",
          "[maintenance]")
{
    Probe probe;
    {
        // The maintenance thread waits for the exclusion: three cycles pile up.
        TestInternals::HoldSweepExclusion held;
        tick(3 * TestInternals::sweepCycleTicks());
    }
    GDSFPolicy::instance().awaitSweeps();

    // The oldest two cycles are dropped; each chunk is swept once.
    const auto sweeps = Probe::taken();
    REQUIRE(sweeps.size() == chunkCount());
    for (size_t i = 0; i < sweeps.size(); ++i) {
        CHECK(sweeps[i].chunk == static_cast<long>(i));
    }
}

// #############################################################################
//
//  4. A forced pass waiting for a sweep goes before the next one
//
// #############################################################################

TEST_CASE("Maintenance - a forced pass waiting for a sweep goes before the next one",
          "[maintenance][purge]")
{
    Probe probe;
    Probe::hold.store(true);
    tick(2 * ticksPerSweep());  // two sweeps: the first is held
    REQUIRE(Probe::awaitEntered(1));

    std::atomic<std::thread::id> pass_thread{};
    std::thread pass([&] {
        pass_thread.store(std::this_thread::get_id());
        (void)GDSFPolicy::instance().sweepEveryChunk(&Probe::sweep);
    });
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (TestInternals::passesWaiting() == 0
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    REQUIRE(TestInternals::passesWaiting() == 1);
    Probe::hold.store(false);
    pass.join();
    GDSFPolicy::instance().awaitSweeps();

    // The held sweep, the pass over every chunk, then the second sweep.
    const auto sweeps = Probe::taken();
    REQUIRE(sweeps.size() == 2 + chunkCount());
    const auto maintenance = sweeps.front().thread;
    CHECK(sweeps.front().chunk == 0);
    for (size_t i = 0; i < chunkCount(); ++i) {
        CHECK(sweeps[1 + i].thread == pass_thread.load());
        CHECK(sweeps[1 + i].chunk == static_cast<long>(i));
    }
    CHECK(sweeps.back().thread == maintenance);
    CHECK(sweeps.back().chunk == 1);
}

// #############################################################################
//
//  5. A sweep that throws is dropped, the next ones are served
//
// #############################################################################

namespace {
std::atomic<int> g_errors_logged{0};
void countErrors(jcailloux::relais::log::Level level, const char*, size_t) {
    if (level == jcailloux::relais::log::Level::Error) g_errors_logged.fetch_add(1);
}
}  // namespace

TEST_CASE("Maintenance - a sweep that throws is logged, and the next ones are served",
          "[maintenance]")
{
    namespace log = jcailloux::relais::log;
    const auto previous = log::getCallback();
    log::setCallback(&countErrors);
    g_errors_logged.store(0);
    {
        Probe probe;
        Probe::throws.store(1);

        tick(TestInternals::sweepCycleTicks());
        GDSFPolicy::instance().awaitSweeps();

        CHECK(Probe::taken().size() == chunkCount());
        CHECK(Probe::throws.load() == 0);
    }
    log::setCallback(previous);
    CHECK(g_errors_logged.load() == 1);
}

// #############################################################################
//
//  6. A reset drops the sweeps not served yet
//
// #############################################################################

TEST_CASE("Maintenance - a reset drops the sweeps not served yet",
          "[maintenance]")
{
    Probe probe;
    Probe::hold.store(true);
    tick(2 * ticksPerSweep());  // two sweeps: the first is held
    REQUIRE(Probe::awaitEntered(1));

    // Waits for the held sweep, and goes before the second one.
    std::thread reset([] { GDSFPolicy::instance().resetForTesting(); });
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (TestInternals::passesWaiting() == 0
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    REQUIRE(TestInternals::passesWaiting() == 1);
    Probe::hold.store(false);
    reset.join();
    GDSFPolicy::instance().awaitSweeps();

    CHECK(Probe::taken().size() == 1);
}

// #############################################################################
//
//  7. A requested pass returns at once, and goes before the next sweep
//
// #############################################################################

namespace {
void requestPass() { GDSFPolicy::instance().requestPass(&Probe::sweep); }
}  // namespace

TEST_CASE("Maintenance - a requested pass returns at once, and goes before the next sweep",
          "[maintenance][sweep-all]")
{
    Probe probe;
    Probe::hold.store(true);
    tick(2 * ticksPerSweep());  // two sweeps: the first is held
    REQUIRE(Probe::awaitEntered(1));

    const auto t0 = std::chrono::steady_clock::now();
    requestPass();
    const auto request_time = std::chrono::steady_clock::now() - t0;
    // The probe holds the sweep for 2 s: a request that waited for it would
    // last as long.
    CHECK(request_time < 100ms);
    Probe::hold.store(false);
    GDSFPolicy::instance().awaitSweeps();

    // The held sweep, the pass over every chunk, then the second sweep, all on
    // the maintenance thread.
    const auto sweeps = Probe::taken();
    REQUIRE(sweeps.size() == 2 + chunkCount());
    CHECK(sweeps.front().chunk == 0);
    for (size_t i = 0; i < chunkCount(); ++i) {
        CHECK(sweeps[1 + i].chunk == static_cast<long>(i));
    }
    CHECK(sweeps.back().chunk == 1);
    for (const auto& sweep : sweeps) CHECK(sweep.thread == sweeps.front().thread);
    CHECK(sweeps.front().thread != std::this_thread::get_id());
}

// #############################################################################
//
//  8. Two requests made before the pass starts ask for one pass
//
// #############################################################################

TEST_CASE("Maintenance - two requests made before the pass starts ask for one pass",
          "[maintenance][sweep-all]")
{
    Probe probe;
    Probe::hold.store(true);
    tick(ticksPerSweep());  // one sweep, held: the pass cannot start
    REQUIRE(Probe::awaitEntered(1));

    requestPass();
    requestPass();
    Probe::hold.store(false);
    GDSFPolicy::instance().awaitSweeps();

    CHECK(Probe::taken().size() == 1 + chunkCount());
}

// #############################################################################
//
//  9. A request made during the pass asks for another
//
// #############################################################################

TEST_CASE("Maintenance - a request made during the pass asks for another",
          "[maintenance][sweep-all]")
{
    Probe probe;
    Probe::hold.store(true);
    requestPass();  // starts at once, held on its first chunk
    REQUIRE(Probe::awaitEntered(1));

    requestPass();  // the second pass sweeps what the first may have missed
    Probe::hold.store(false);
    GDSFPolicy::instance().awaitSweeps();

    const auto sweeps = Probe::taken();
    REQUIRE(sweeps.size() == 2 * chunkCount());
    for (size_t i = 0; i < sweeps.size(); ++i) {
        CHECK(sweeps[i].chunk == static_cast<long>(i % chunkCount()));
    }
}

// #############################################################################
//
//  10. A pass that throws is dropped, the next sweeps are served
//
// #############################################################################

TEST_CASE("Maintenance - a pass that throws is logged, and the next sweeps are served",
          "[maintenance][sweep-all]")
{
    namespace log = jcailloux::relais::log;
    const auto previous = log::getCallback();
    log::setCallback(&countErrors);
    g_errors_logged.store(0);
    {
        Probe probe;
        Probe::throws.store(1);

        requestPass();
        GDSFPolicy::instance().awaitSweeps();
        CHECK(Probe::taken().size() == 1);  // the pass ends on the chunk that throws

        tick(TestInternals::sweepCycleTicks());
        GDSFPolicy::instance().awaitSweeps();
        CHECK(Probe::taken().size() == 1 + chunkCount());
    }
    log::setCallback(previous);
    CHECK(g_errors_logged.load() == 1);
}

// #############################################################################
//
//  11. A reset drops the passes not served yet
//
// #############################################################################

TEST_CASE("Maintenance - a reset drops the passes not served yet",
          "[maintenance][sweep-all]")
{
    Probe probe;
    Probe::hold.store(true);
    tick(ticksPerSweep());  // one sweep, held: the pass waits
    REQUIRE(Probe::awaitEntered(1));
    requestPass();

    // Waits for the held sweep, and goes before the pass.
    std::thread reset([] { GDSFPolicy::instance().resetForTesting(); });
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (TestInternals::passesWaiting() == 0
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    REQUIRE(TestInternals::passesWaiting() == 1);
    Probe::hold.store(false);
    reset.join();
    GDSFPolicy::instance().awaitSweeps();
    CHECK(Probe::taken().size() == 1);

    // Not merged with the pass dropped: a new request is served.
    requestPass();
    GDSFPolicy::instance().awaitSweeps();
    CHECK(Probe::taken().size() == 1 + chunkCount());
}
