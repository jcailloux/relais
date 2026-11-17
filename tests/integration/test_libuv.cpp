// relais on libuv, through the shipped LibuvIoContext: the adapter meets the
// IoContext contract, fires no timer before its delay however late the loop's
// clock, reports a socket error once, and leaves the loop nothing to close once
// destroyed; attach() binds relais to loops that a thread runs with uv_run, and
// leaves a loop nothing to do once detached, so that uv_run returns. The
// PostgreSQL scenarios of the adversarial loops run on libuv in
// test_libuv_loops.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/integration/libuv.h>
#include <jcailloux/relais/io/WhenAll.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/runtime/Attach.h>
#include <jcailloux/relais/testing/IoContextConformance.h>

#include <fixtures/LibuvTestLoop.h>
#include <fixtures/LivenessHarness.h>
#include <fixtures/TestConnInfo.h>

#include <uv.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <latch>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::test;
using namespace std::chrono_literals;
using jcailloux::relais::AttachError;
using jcailloux::relais::Attachment;
using jcailloux::relais::PgProvider;
using jcailloux::relais::testing::IoContextConformance;
using Clock = std::chrono::steady_clock;

namespace {

const char* conninfo() { return relais_test::kConnInfo; }

template<typename H>
uv_handle_t* asHandle(H* h) {
    return reinterpret_cast<uv_handle_t*>(h);
}

// A loop this thread drives, one turn at a time: a repeating timer of its own
// keeps it alive and bounds each turn's wait.
struct DrivenLoop {
    uv_loop_t loop;
    uv_timer_t tick;
    std::optional<LibuvIoContext> io;

    DrivenLoop() {
        uv_loop_init(&loop);
        uv_timer_init(&loop, &tick);
        uv_timer_start(&tick, [](uv_timer_t*) {}, 10, 10);
        io.emplace(&loop);
    }
    ~DrivenLoop() { close(); }

    DrivenLoop(const DrivenLoop&) = delete;
    DrivenLoop& operator=(const DrivenLoop&) = delete;

    void turn() { uv_run(&loop, UV_RUN_ONCE); }

    template<typename Pred>
    bool turnUntil(Pred pred, std::chrono::milliseconds limit = 1s) {
        const auto deadline = Clock::now() + limit;
        while (!pred()) {
            if (Clock::now() >= deadline) return false;
            turn();
        }
        return true;
    }

    // Destroys the adapter, then closes the loop: ends the process if a handle
    // is left on it.
    void close() {
        if (closed) return;
        closed = true;
        io.reset();
        if (!uv_is_closing(asHandle(&tick))) uv_close(asHandle(&tick), nullptr);
        closeLoop(loop);
    }

    bool closed = false;
};

// A loop as an application runs it: uv_run on a thread of its own, kept
// running by a handle of the application's until release(). The loop and the
// adapter are set up there, before uv_run, or on the building thread before
// that thread starts (BuiltHere); once uv_run has returned, and the RunningLoop
// is destroyed, the adapter is destroyed on the loop's thread and the loop
// closed.
class RunningLoop {
public:
    enum class Setup { OnLoopThread, BuiltHere };

    explicit RunningLoop(Setup setup = Setup::OnLoopThread) {
        auto setUp = [this] {
            uv_loop_init(&loop_);
            uv_async_init(&loop_, &keep_, [](uv_async_t* a) { uv_close(asHandle(a), nullptr); });
            io_.emplace(&loop_);
        };
        if (setup == Setup::BuiltHere) setUp();
        std::promise<void> built;
        auto ready = built.get_future();
        thread_ = std::jthread([this, &built, setup, setUp] {
            if (setup == Setup::OnLoopThread) setUp();
            built.set_value();
            uv_run(&loop_, UV_RUN_DEFAULT);
            returned_.store(true);
            close_.wait();
            io_.reset();
            closeLoop(loop_);
        });
        ready.wait();
    }

    ~RunningLoop() {
        release();
        if (!returnedWithin(10s)) {
            std::fprintf(stderr, "RunningLoop: uv_run did not return\n");
            std::abort();
        }
        close_.count_down();
    }

    RunningLoop(const RunningLoop&) = delete;
    RunningLoop& operator=(const RunningLoop&) = delete;

    LibuvIoContext& io() { return *io_; }

    // The application lets go of the loop: uv_run returns once nothing keeps
    // it running.
    void release() { uv_async_send(&keep_); }

    [[nodiscard]] bool returnedWithin(std::chrono::milliseconds limit) const {
        const auto deadline = Clock::now() + limit;
        while (!returned_.load()) {
            if (Clock::now() >= deadline) return false;
            std::this_thread::sleep_for(1ms);
        }
        return true;
    }

private:
    uv_loop_t loop_;
    uv_async_t keep_;
    std::optional<LibuvIoContext> io_;
    std::atomic<bool> returned_{false};
    std::latch close_{1};
    std::jthread thread_;  // last: joined first
};

Task<std::string> sixTimesSeven() {
    auto result = co_await PgProvider::query("SELECT 6 * 7");
    co_return std::string(result[0].rawValue(0));
}

Task<int32_t> doubled(int32_t n) {
    auto result = co_await PgProvider::queryParams("SELECT $1::int * 2", PgParams::make(n));
    co_return result[0].template get<int32_t>(0);
}

Task<void> together(std::vector<Task<void>> tasks) {
    co_await whenAll(std::move(tasks));
}

std::string attachError(LibuvIoContext& io) {
    try {
        auto relais = jcailloux::relais::attach(io, conninfo());
    } catch (const AttachError& e) {
        return e.what();
    }
    return {};
}

}  // namespace

TEST_CASE("libuv: LibuvIoContext meets the IoContext contract", "[libuv][conformance]") {
    DrivenLoop d;
    auto drive = [&d](LibuvIoContext&, auto pred) {
        while (!pred()) d.turn();
    };
    IoContextConformance::runAll(*d.io, drive);
}

TEST_CASE("libuv: attach binds relais to a loop one thread runs, and detach lets go",
          "[libuv][attach][integration]") {
    RunningLoop t;
    const int fds_before = countOpenFds();
    {
        auto relais = jcailloux::relais::attach(t.io(), conninfo(),
            {.pool = {.min_connections = 2, .max_connections = 2}});
        CHECK(runOn(t.io(), sixTimesSeven) == "42");

        // Concurrent requests share the loop's two connections.
        int ok = runOn(t.io(), []() -> Task<int> {
            int count = 0;
            auto one = [&count](int32_t i) -> Task<void> {
                if (co_await doubled(i) == 2 * i) ++count;
            };
            std::vector<Task<void>> requests;
            for (int32_t i = 0; i < 32; ++i) requests.push_back(one(i));
            co_await together(std::move(requests));
            co_return count;
        });
        CHECK(ok == 32);
        CHECK(callOn(t.io(), [] { return PgProvider::initialized(); }));
    }
    // Taken off: the loop thread no longer routes through relais, and nothing
    // relais opened on it is left.
    CHECK_FALSE(callOn(t.io(), [] { return PgProvider::initialized(); }));
    CHECK(countOpenFds() <= fds_before);

    // Attached again, to the same loop.
    auto relais = jcailloux::relais::attach(t.io(), conninfo(),
        {.pool = {.min_connections = 1, .max_connections = 1}});
    CHECK(runOn(t.io(), sixTimesSeven) == "42");
}

TEST_CASE("libuv: attach to a loop set up on another thread than the one running it",
          "[libuv][attach][integration]") {
    // Set up on this thread, run on another: the adapter takes the loop's
    // thread for the one that runs its callbacks, not the one that built it.
    RunningLoop t(RunningLoop::Setup::BuiltHere);
    auto relais = jcailloux::relais::attach(t.io(), conninfo(),
        {.pool = {.min_connections = 1, .max_connections = 1}});
    CHECK(runOn(t.io(), sixTimesSeven) == "42");
    CHECK_FALSE(t.io().isInLoopThread());
    CHECK(callOn(t.io(), [&] { return t.io().isInLoopThread(); }));
}

TEST_CASE("libuv: detach waits for a request in flight", "[libuv][attach][integration]") {
    RunningLoop t;
    auto relais = jcailloux::relais::attach(t.io(), conninfo(),
        {.pool = {.min_connections = 1, .max_connections = 1}});

    std::atomic<bool> started{false};
    std::atomic<int> outcome{0};  // 1: answered, -1: failed
    callOn(t.io(), [&] {
        [](std::atomic<bool>& s, std::atomic<int>& o) -> DetachedTask {
            s = true;
            try {
                auto r = co_await PgProvider::query("SELECT pg_sleep(0.3), 1");
                o = r.ok() ? 1 : -1;
            } catch (...) {
                o = -1;
            }
        }(started, outcome);
    });
    REQUIRE(started);

    const auto t0 = Clock::now();
    relais.detach();
    CHECK(outcome == 1);
    CHECK(Clock::now() - t0 >= 200ms);
}

TEST_CASE("libuv: attach refuses the loop's own thread", "[libuv][attach]") {
    RunningLoop t;
    std::string error = callOn(t.io(), [&] { return attachError(t.io()); });
    CHECK_THAT(error, Catch::Matchers::ContainsSubstring("loop's own thread"));
}

TEST_CASE("libuv: attach fails within a second on a loop that nobody runs", "[libuv][attach]") {
    uv_loop_t loop;
    uv_loop_init(&loop);
    std::optional<LibuvIoContext> io(std::in_place, &loop);
    const int fds = countOpenFds();

    const auto t0 = Clock::now();
    CHECK_THAT(attachError(*io), Catch::Matchers::ContainsSubstring("the loop must be running"));
    CHECK(Clock::now() - t0 < 3s);

    // What the check posted never ran: destroyed with the adapter.
    io.reset();
    closeLoop(loop);
    CHECK(countOpenFds() <= fds);
}

TEST_CASE("libuv: relais on several loops, one thread each", "[libuv][attach][integration]") {
    std::vector<std::unique_ptr<RunningLoop>> loops;
    std::vector<Attachment<LibuvIoContext>> attached;
    for (int i = 0; i < 3; ++i) {
        loops.push_back(std::make_unique<RunningLoop>());
        attached.push_back(jcailloux::relais::attach(loops.back()->io(), conninfo(),
            {.pool = {.min_connections = 1, .max_connections = 2}}));
    }

    // Each loop answers on its own pool, all of them at once.
    struct Answer {
        std::promise<int32_t> value;
        std::thread::id thread;
    };
    std::vector<Answer> answers(loops.size());
    for (std::size_t i = 0; i < loops.size(); ++i) {
        loops[i]->io().post([&answer = answers[i], n = static_cast<int32_t>(i) + 10] {
            [](Answer& a, int32_t n) -> DetachedTask {
                a.thread = std::this_thread::get_id();
                try {
                    a.value.set_value(co_await doubled(n));
                } catch (...) {
                    a.value.set_exception(std::current_exception());
                }
            }(answer, n);
        });
    }
    for (std::size_t i = 0; i < loops.size(); ++i) {
        auto value = answers[i].value.get_future();
        REQUIRE(value.wait_for(5s) == std::future_status::ready);
        CHECK(value.get() == 2 * (static_cast<int32_t>(i) + 10));
    }
    CHECK(answers[0].thread != answers[1].thread);
    CHECK(answers[1].thread != answers[2].thread);

    attached.clear();
    for (auto& t : loops)
        CHECK_FALSE(callOn(t->io(), [] { return PgProvider::initialized(); }));
}

TEST_CASE("libuv: uv_run returns once relais is detached", "[libuv][attach][integration]") {
    // relais's connections keep the loop running once the application lets go
    // of it, and its timers would keep it running up to query_timeout after a
    // request: detach leaves it nothing.
    SECTION("detached from another thread") {
        RunningLoop t;
        auto relais = jcailloux::relais::attach(t.io(), conninfo(),
            {.pool = {.min_connections = 2, .max_connections = 2}});
        CHECK(runOn(t.io(), sixTimesSeven) == "42");
        t.release();
        CHECK_FALSE(t.returnedWithin(200ms));
        CHECK(runOn(t.io(), sixTimesSeven) == "42");
        relais.detach();
        CHECK(t.returnedWithin(1s));
    }
    SECTION("detached from its own thread") {
        RunningLoop t;
        auto relais = jcailloux::relais::attach(t.io(), conninfo(),
            {.pool = {.min_connections = 2, .max_connections = 2}});
        CHECK(runOn(t.io(), sixTimesSeven) == "42");
        t.release();
        callOn(t.io(), [&] { relais.detach(); });  // starts the teardown, waits for nothing
        CHECK(t.returnedWithin(2s));
    }
}

TEST_CASE("libuv: postDelayed never fires before its delay, however late the loop's clock",
          "[libuv]") {
    // libuv times a timer from the loop's cached clock, which a busy turn
    // leaves behind: here by 8 ms.
    DrivenLoop d;
    struct Shot {
        std::chrono::nanoseconds delay;
        Clock::time_point armed{};
        std::optional<Clock::duration> took;
    };
    std::vector<Shot> shots{{0ns}, {100us}, {1ms}, {3ms}, {10ms}};
    d.io->post([&] {
        const auto busy_until = Clock::now() + 8ms;
        while (Clock::now() < busy_until) {}
        for (auto& s : shots) {
            s.armed = Clock::now();
            d.io->postDelayed(s.delay, [&s] { s.took = Clock::now() - s.armed; });
        }
    });
    REQUIRE(d.turnUntil([&] {
        for (const auto& s : shots)
            if (!s.took) return false;
        return true;
    }, 2s));
    for (const auto& s : shots) {
        INFO("delay " << s.delay.count() << " ns, fired after "
             << std::chrono::nanoseconds(*s.took).count() << " ns");
        CHECK(*s.took >= s.delay);
        CHECK(*s.took < s.delay + 50ms);
    }
}

TEST_CASE("libuv: a socket error is reported once, and updateWatch watches again", "[libuv]") {
    // A TCP peer that resets the connection: libuv stops a watch on POLLERR.
    const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    REQUIRE(listener >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof addr;
    REQUIRE(::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof addr) == 0);
    REQUIRE(::listen(listener, 1) == 0);
    REQUIRE(::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
    const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    REQUIRE(::connect(client, reinterpret_cast<sockaddr*>(&addr), sizeof addr) == 0);
    const int server = ::accept(listener, nullptr, nullptr);
    REQUIRE(server >= 0);
    const linger reset{1, 0};
    REQUIRE(::setsockopt(server, SOL_SOCKET, SO_LINGER, &reset, sizeof reset) == 0);
    ::close(server);

    DrivenLoop d;
    std::vector<IoEvent> seen;
    auto h = d.io->addWatch(client, IoEvent::Read, [&](IoEvent ev) { seen.push_back(ev); });
    REQUIRE(d.turnUntil([&] { return !seen.empty(); }));
    for (int i = 0; i < 5; ++i) d.turn();
    CHECK(seen == std::vector<IoEvent>{IoEvent::Error});

    d.io->updateWatch(h, IoEvent::Read);
    CHECK(d.turnUntil([&] { return seen.size() >= 2; }));

    d.io->removeWatch(h);
    d.close();
    ::close(client);
    ::close(listener);
}

TEST_CASE("libuv: what the adapter leaves pending dies with it, and the loop closes",
          "[libuv]") {
    SECTION("a removed watch, a pending timer, a posted callback") {
        DrivenLoop d;
        int sv[2];
        REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sv) == 0);
        bool fired = false;
        bool ran = false;
        d.io->removeWatch(d.io->addWatch(sv[0], IoEvent::Read, [](IoEvent) {}));
        d.io->postDelayed(1h, [&] { fired = true; });
        d.io->post([&] { ran = true; });
        d.close();  // ends the process if a handle is left on the loop
        CHECK_FALSE(fired);
        CHECK_FALSE(ran);
        CHECK(::close(sv[0]) == 0);  // the descriptor is relais's: never closed for it
        ::close(sv[1]);
    }
    SECTION("an adapter that one of its posted callbacks destroys runs none of the rest") {
        DrivenLoop d;
        bool second = false;
        d.io->post([&] { d.io.reset(); });
        d.io->post([&] { second = true; });
        for (int i = 0; i < 3; ++i) d.turn();
        CHECK_FALSE(d.io);
        CHECK_FALSE(second);
    }
    SECTION("an application that closes every handle of the loop first (uv_walk)") {
        // Out of order, but common: the adapter closes nothing twice, and
        // keeps its async handle (LSan sees it reachable).
        DrivenLoop d;
        d.io->post([] {});
        d.turn();
        uv_walk(&d.loop, [](uv_handle_t* h, void*) {
            if (!uv_is_closing(h)) uv_close(h, nullptr);
        }, nullptr);
        while (uv_loop_alive(&d.loop) != 0) uv_run(&d.loop, UV_RUN_NOWAIT);
        REQUIRE(uv_loop_close(&d.loop) == 0);
        d.closed = true;
        d.io.reset();
    }
}
