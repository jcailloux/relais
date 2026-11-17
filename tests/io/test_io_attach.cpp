// relais::attach: relais bound to a loop that something else runs.
//
// The loop runs on a thread of its own (LoopThread), attach() is called from
// the test's thread. Covered: a bound loop serves requests through its
// providers, and detach waits for one in flight and leaves nothing behind;
// attach refuses the loop's own thread and a loop already attached; the startup
// check rejects a loop that breaks the IoContext contract, saying what it did
// not do, and lets go of what it opened.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/EpollIoContext.h>
#include <jcailloux/relais/runtime/Attach.h>

#include <fixtures/LivenessHarness.h>
#include <fixtures/TestConnInfo.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using jcailloux::relais::attach;
using jcailloux::relais::AttachConfig;
using jcailloux::relais::AttachError;
using jcailloux::relais::PgProvider;
using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::test;
using namespace std::chrono_literals;
using Catch::Matchers::ContainsSubstring;

namespace {

const char* conninfo() { return relais_test::kConnInfo; }

const AttachConfig kSmall{.pool = {.min_connections = 1, .max_connections = 2}};

Task<std::string> firstValue(const char* sql) {
    auto result = co_await PgProvider::query(sql);
    co_return std::string(result[0].rawValue(0));
}

bool bound() { return PgProvider::initialized(); }

// The whole IoContext surface of an EpollIoContext. Each loop below breaks one
// rule of the contract, as a real adapter might.
struct Forwarding {
    using WatchHandle = EpollIoContext::WatchHandle;

    WatchHandle addWatch(int fd, IoEvent ev, std::function<void(IoEvent)> cb) {
        return inner.addWatch(fd, ev, std::move(cb));
    }
    void removeWatch(WatchHandle h) { inner.removeWatch(h); }
    void updateWatch(WatchHandle h, IoEvent ev) { inner.updateWatch(h, ev); }
    void post(std::function<void()> cb) { inner.post(std::move(cb)); }
    void postDelayed(std::chrono::nanoseconds d, std::function<void()> cb) {
        inner.postDelayed(d, std::move(cb));
    }
    void runOnce(int timeout_ms) { inner.runOnce(timeout_ms); }

    EpollIoContext inner;
};

// Runs every posted callback at once, on the posting thread.
struct PostInPlace : Forwarding {
    void post(std::function<void()> cb) { cb(); }
};

// Runs a callback posted on the loop thread at once (trantor's runInLoop).
struct PostInPlaceOnLoop : Forwarding {
    void post(std::function<void()> cb) {
        if (inner.isInLoopThread()) cb();
        else inner.post(std::move(cb));
    }
};

// Swaps the first two callbacks posted.
struct SwappedPosts : Forwarding {
    void post(std::function<void()> cb) {
        std::lock_guard lock(m);
        if (!held && !swapped) {
            held = std::move(cb);
            return;
        }
        inner.post(std::move(cb));
        if (held) {
            inner.post(std::move(*held));
            held.reset();
            swapped = true;
        }
    }
    std::mutex m;
    std::optional<std::function<void()>> held;
    bool swapped = false;
};

// Runs posted callbacks in order, but each on a thread of its own, as an asio
// io_context that several threads run.
struct PostsOnManyThreads : Forwarding {
    void post(std::function<void()> cb) {
        std::lock_guard lock(m);
        threads.emplace_back([this, cb = std::move(cb), turn = posted++] {
            std::unique_lock serial(m);
            turn_cv.wait(serial, [&] { return ran == turn; });
            cb();
            ++ran;
            turn_cv.notify_all();
        });
    }
    ~PostsOnManyThreads() {
        for (auto& t : threads) t.join();
    }
    std::recursive_mutex m;  // a callback may post
    std::condition_variable_any turn_cv;
    std::size_t posted = 0;
    std::size_t ran = 0;
    std::vector<std::thread> threads;
};

// Never calls a watch.
struct DeafWatches : Forwarding {
    WatchHandle addWatch(int, IoEvent, std::function<void(IoEvent)>) { return -1; }
    void removeWatch(WatchHandle) {}
    void updateWatch(WatchHandle, IoEvent) {}
};

// Keeps the mask a watch was added with.
struct FixedMask : Forwarding {
    void updateWatch(WatchHandle, IoEvent) {}
};

// Removes a watch on a later turn: the removal lands after a new watch of the
// same descriptor number, and ends it.
struct DeferredRemoval : Forwarding {
    void removeWatch(WatchHandle h) {
        inner.post([this, h] { inner.removeWatch(h); });
    }
};

// Never stops a watch.
struct NoRemoval : Forwarding {
    void removeWatch(WatchHandle) {}
};

// Never runs a postDelayed callback.
struct SilentTimers : Forwarding {
    void postDelayed(std::chrono::nanoseconds, std::function<void()>) {}
};

// Once `late` is set, runs each posted callback 10 ms after its turn comes: a
// thread waiting on the loop wakes long before the next callback has run.
struct LatePosts : Forwarding {
    void post(std::function<void()> cb) {
        if (!late) {
            inner.post(std::move(cb));
            return;
        }
        inner.post([cb = std::move(cb)] {
            std::this_thread::sleep_for(10ms);
            cb();
        });
    }
    std::atomic<bool> late{false};
};

// What attach() reports on a loop of that kind; the check's descriptors are
// closed once the loop has run what the failure posted or armed.
template<typename Loop>
std::string rejection() {
    LoopThread<Loop> t;
    const int fds = countOpenFds();
    std::string what;
    try {
        (void)attach(t.loop(), conninfo(), kSmall);
    } catch (const AttachError& e) {
        what = e.what();
    }
    t.settle();
    for (int i = 0; i < 100 && countOpenFds() != fds; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        t.settle();
    }
    CHECK(countOpenFds() == fds);
    return what;
}

}  // namespace

TEST_CASE("attach: binds relais to a loop run by another thread", "[io][attach][integration]") {
    LoopThread<EpollIoContext> t;
    const int fds = countOpenFds();
    {
        auto relais = attach(t.loop(), conninfo(), kSmall);
        CHECK_FALSE(bound());  // the calling thread
        CHECK(t.call(bound));
        CHECK(t.call([&] { return relais.pool()->totalConnections(); }) == 1);
        CHECK(t.run([] { return firstValue("SELECT 40 + 2"); }) == "42");
    }
    CHECK_FALSE(t.call(bound));
    t.settle();  // the connections' watches retired
    CHECK(countOpenFds() == fds);
}

TEST_CASE("attach: detach waits for a request in flight", "[io][attach][integration]") {
    LoopThread<EpollIoContext> t;
    std::atomic<bool> started{false};
    std::atomic<bool> finished{false};
    {
        auto relais = attach(t.loop(), conninfo(), kSmall);
        t.loop().post([&] {
            [](std::atomic<bool>& started, std::atomic<bool>& finished) -> DetachedTask {
                started = true;
                (void)co_await PgProvider::query("SELECT pg_sleep(0.3)");
                finished = true;
            }(started, finished);
        });
        while (!started) std::this_thread::yield();
    }
    CHECK(finished);
    CHECK_FALSE(t.call(bound));
}

TEST_CASE("attach: once detach returns, nothing relais opened is left, even on a slow loop",
          "[io][attach][integration]") {
    LoopThread<LatePosts> t;
    const int fds = countOpenFds();
    auto relais = attach(t.loop(), conninfo(), kSmall);
    CHECK(t.run([] { return firstValue("SELECT 40 + 2"); }) == "42");
    t.loop().late = true;
    relais.detach();
    // The loop may stop here: the teardown ran what it posted, the removal of
    // each connection's watch and the closing of its descriptor among them.
    CHECK(countOpenFds() == fds);
}

TEST_CASE("attach: detach on the loop's own thread starts the teardown and returns",
          "[io][attach][integration]") {
    LoopThread<EpollIoContext> t;
    auto relais = attach(t.loop(), conninfo(), kSmall);
    const auto t0 = std::chrono::steady_clock::now();
    t.call([&] { relais.detach(); });
    CHECK(std::chrono::steady_clock::now() - t0 < 500ms);
    CHECK_FALSE(relais.pool());
    CHECK_FALSE(t.call(bound));
}

TEST_CASE("attach: refuses the loop's own thread at once", "[io][attach]") {
    LoopThread<EpollIoContext> t;
    t.settle();  // the loop knows its thread
    const auto what = t.call([&]() -> std::string {
        try {
            (void)attach(t.loop(), conninfo(), kSmall);
        } catch (const AttachError& e) {
            return e.what();
        }
        return {};
    });
    CHECK_THAT(what, ContainsSubstring("own thread"));
    CHECK_FALSE(t.call(bound));
}

TEST_CASE("attach: refuses a loop relais is already attached to", "[io][attach][integration]") {
    LoopThread<EpollIoContext> t;
    auto relais = attach(t.loop(), conninfo(), kSmall);
    std::string what;
    try {
        (void)attach(t.loop(), conninfo(), kSmall);
    } catch (const AttachError& e) {
        what = e.what();
    }
    CHECK_THAT(what, ContainsSubstring("already attached"));
    CHECK(t.run([] { return firstValue("SELECT 1"); }) == "1");
}

TEST_CASE("attach: the startup check rejects a loop that breaks the contract", "[io][attach]") {
    SECTION("a post run on the posting thread") {
        CHECK_THAT(rejection<PostInPlace>(), ContainsSubstring("on the thread that posted it"));
    }
    SECTION("posts run out of order") {
        CHECK_THAT(rejection<SwappedPosts>(), ContainsSubstring("out of order"));
    }
    SECTION("posts run on several threads") {
        CHECK_THAT(rejection<PostsOnManyThreads>(), ContainsSubstring("on different threads"));
    }
    SECTION("a post from a watch callback run inside it") {
        CHECK_THAT(rejection<PostInPlaceOnLoop>(), ContainsSubstring("before that callback returned"));
    }
    SECTION("a watch never called") {
        CHECK_THAT(rejection<DeafWatches>(), ContainsSubstring("turned readable was not reported"));
    }
    SECTION("updateWatch ignored") {
        CHECK_THAT(rejection<FixedMask>(), ContainsSubstring("updateWatch added Write"));
    }
    SECTION("a removal that lands after a new watch of the same number") {
        CHECK_THAT(rejection<DeferredRemoval>(), ContainsSubstring("reused descriptor number"));
    }
    SECTION("a removeWatch that does nothing") {
        CHECK_THAT(rejection<NoRemoval>(), ContainsSubstring("after its removeWatch returned"));
    }
    SECTION("a postDelayed callback never run") {
        CHECK_THAT(rejection<SilentTimers>(), ContainsSubstring("postDelayed callback did not run"));
    }
}

TEST_CASE("attach: the startup check gives up on a loop nobody runs", "[io][attach]") {
    EpollIoContext idle;
    const int fds = countOpenFds();
    const auto t0 = std::chrono::steady_clock::now();
    std::string what;
    try {
        (void)attach(idle, conninfo(), kSmall);
    } catch (const AttachError& e) {
        what = e.what();
    }
    CHECK(std::chrono::steady_clock::now() - t0 < 2s);
    CHECK_THAT(what, ContainsSubstring("did not run within 1 s"));
    pump(idle, 20ms);  // what the check posted runs now, and finds it over
    CHECK(countOpenFds() == fds);
}
