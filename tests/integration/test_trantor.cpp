// relais on trantor, through the shipped TrantorIoContext: the adapter meets
// the IoContext contract, and attach() binds relais to loops that trantor runs
// itself (EventLoopThread), as Drogon runs them. The same PostgreSQL scenarios
// as on the adversarial loops run on trantor in test_trantor_loops.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/integration/trantor.h>
#include <jcailloux/relais/io/WhenAll.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/runtime/Attach.h>
#include <jcailloux/relais/testing/IoContextConformance.h>

#include <trantor/net/EventLoop.h>
#include <trantor/net/EventLoopThread.h>
#include <trantor/net/EventLoopThreadPool.h>

#include <fixtures/LivenessHarness.h>
#include <fixtures/TestConnInfo.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::test;
using namespace std::chrono_literals;
using jcailloux::relais::AttachError;
using jcailloux::relais::PgProvider;
using jcailloux::relais::testing::IoContextConformance;

namespace {

const char* conninfo() { return relais_test::kConnInfo; }

// A trantor loop that this thread drives: one turn per loop() call.
struct DrivenLoop {
    trantor::EventLoop loop;
    TrantorIoContext io{&loop};

    DrivenLoop() { turn(); }  // trantor forgets this thread's loop when a loop() ends

    void turn() {
        loop.queueInLoop([this] { loop.quit(); });
        loop.loop();
    }
};

// A trantor loop as trantor runs it, on a thread of its own.
struct RunningLoop {
    trantor::EventLoopThread thread{"relais-test"};
    std::unique_ptr<TrantorIoContext> io;

    RunningLoop() {
        thread.run();
        io = std::make_unique<TrantorIoContext>(thread.getLoop());
    }
};

// The likeliest mistake in a trantor adapter: post() through runInLoop, which
// runs the callback at once when called on the loop thread. Its watches are
// freed at once, as TrantorIoContext frees them.
struct RunInLoopIoContext {
    using WatchHandle = TrantorIoContext::WatchHandle;

    explicit RunInLoopIoContext(trantor::EventLoop* loop) : inner(loop) {}

    WatchHandle addWatch(int fd, IoEvent mask, std::function<void(IoEvent)> cb) {
        return inner.addWatch(fd, mask, std::move(cb));
    }
    void updateWatch(WatchHandle h, IoEvent mask) { inner.updateWatch(h, mask); }
    void removeWatch(WatchHandle h) { inner.removeWatch(h); }
    void post(std::function<void()> cb) { inner.loop()->runInLoop(std::move(cb)); }
    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        inner.postDelayed(delay, std::move(cb));
    }
    [[nodiscard]] bool isInLoopThread() const noexcept { return inner.isInLoopThread(); }

    TrantorIoContext inner;
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

}  // namespace

TEST_CASE("trantor: TrantorIoContext meets the IoContext contract", "[trantor][conformance]") {
    DrivenLoop driven;
    auto drive = [&driven](TrantorIoContext&, auto pred) {
        while (!pred()) driven.turn();
    };
    IoContextConformance::runAll(driven.io, drive);
}

TEST_CASE("trantor: attach binds relais to a running loop, and detach lets go",
          "[trantor][attach][integration]") {
    RunningLoop t;
    const int fds_before = countOpenFds();
    {
        auto relais = jcailloux::relais::attach(*t.io, conninfo(),
            {.pool = {.min_connections = 2, .max_connections = 2}});
        CHECK(runOn(*t.io, sixTimesSeven) == "42");

        // Concurrent requests share the loop's two connections.
        int ok = runOn(*t.io, []() -> Task<int> {
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
        CHECK(callOn(*t.io, [] { return PgProvider::initialized(); }));
    }
    // Taken off: the loop thread no longer routes through relais, and nothing
    // relais opened on it is left.
    CHECK_FALSE(callOn(*t.io, [] { return PgProvider::initialized(); }));
    CHECK(countOpenFds() <= fds_before);
}

TEST_CASE("trantor: detach waits for a request in flight", "[trantor][attach][integration]") {
    RunningLoop t;
    auto relais = jcailloux::relais::attach(*t.io, conninfo(),
        {.pool = {.min_connections = 1, .max_connections = 1}});

    std::atomic<bool> started{false};
    std::atomic<int> outcome{0};  // 1: answered, -1: failed
    callOn(*t.io, [&] {
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

    const auto t0 = std::chrono::steady_clock::now();
    relais.detach();
    CHECK(outcome == 1);
    CHECK(std::chrono::steady_clock::now() - t0 >= 200ms);
}

TEST_CASE("trantor: attach refuses the loop's own thread", "[trantor][attach]") {
    RunningLoop t;
    std::string error = callOn(*t.io, [&]() -> std::string {
        try {
            auto relais = jcailloux::relais::attach(*t.io, conninfo());
        } catch (const AttachError& e) {
            return e.what();
        }
        return {};
    });
    CHECK_THAT(error, Catch::Matchers::ContainsSubstring("loop's own thread"));
}

TEST_CASE("trantor: attach rejects a post() that runs in place", "[trantor][attach]") {
    RunningLoop t;
    RunInLoopIoContext io(t.thread.getLoop());
    const int fds = countOpenFds();
    std::string error;
    try {
        auto relais = jcailloux::relais::attach(io, conninfo());
    } catch (const AttachError& e) {
        error = e.what();
    }
    CHECK_THAT(error, Catch::Matchers::ContainsSubstring("before that callback returned"));
    // The check lets go of its sockets once the watch's callback has returned.
    for (int i = 0; i < 100 && countOpenFds() != fds; ++i) {
        std::this_thread::sleep_for(2ms);
        callOn(*t.io, [] {});
    }
    CHECK(countOpenFds() == fds);
}

TEST_CASE("trantor: relais on every loop of an EventLoopThreadPool",
          "[trantor][attach][integration]") {
    trantor::EventLoopThreadPool pool(3, "relais-test");
    pool.start();
    std::vector<std::unique_ptr<TrantorIoContext>> loops;
    std::vector<jcailloux::relais::Attachment<TrantorIoContext>> attached;
    for (auto* loop : pool.getLoops()) {
        loops.push_back(std::make_unique<TrantorIoContext>(loop));
        attached.push_back(jcailloux::relais::attach(*loops.back(), conninfo(),
            {.pool = {.min_connections = 1, .max_connections = 2}}));
    }

    // Each loop answers on its own pool, all of them at once.
    struct Answer {
        std::promise<int32_t> value;
        std::thread::id thread;
    };
    std::vector<Answer> answers(loops.size());
    for (std::size_t i = 0; i < loops.size(); ++i) {
        loops[i]->post([&answer = answers[i], n = static_cast<int32_t>(i) + 10] {
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

    attached.clear();  // detaches every loop before the pool stops them
    for (auto& io : loops)
        CHECK_FALSE(callOn(*io, [] { return PgProvider::initialized(); }));
}
