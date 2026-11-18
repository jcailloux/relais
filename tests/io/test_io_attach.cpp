// relais::attach: relais bound to a loop that something else runs.
//
// The loop runs on a thread of its own (LoopThread), attach() is called from
// the test's thread. Covered: a bound loop serves requests through its
// providers, and detach waits for one in flight and leaves nothing behind;
// attach refuses the loop's own thread and a loop already attached; the startup
// check rejects a loop that breaks the IoContext contract, saying what it did
// not do, and lets go of what it opened.
//
// relais::attachHere, on the loop's own thread: it binds that thread at once,
// requests wait for the pool's first connection, on_ready tells how the start
// ended, and a failed start fails the requests at once; the same through a
// descriptor that another loop watches (integration/fd.h). A loop found on
// another thread ends the process (cases run in a child process).

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <jcailloux/relais/Log.h>
#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/integration/fd.h>
#include <jcailloux/relais/io/EpollIoContext.h>
#include <jcailloux/relais/runtime/Attach.h>

#include <fixtures/LivenessHarness.h>
#include <fixtures/TestConnInfo.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

using jcailloux::relais::attach;
using jcailloux::relais::attachHere;
using jcailloux::relais::Attachment;
using jcailloux::relais::AttachConfig;
using jcailloux::relais::AttachError;
using jcailloux::relais::FdAttachment;
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

// -- attachHere --------------------------------------------------------------

// A database the server does not have: a connection fails at once.
const char* missingDatabase() {
    return "host=localhost port=5432 dbname=relais_no_such_database user=relais_test "
           "password=relais_test";
}

std::string what(const std::exception_ptr& error) {
    if (!error) return {};
    try {
        std::rethrow_exception(error);
    } catch (const std::exception& e) {
        return e.what();
    } catch (...) {
        return "?";
    }
}

// What on_ready was told: how often, with what, on which thread.
struct ReadyLog {
    std::mutex m;
    std::condition_variable cv;
    int calls = 0;
    std::exception_ptr error;
    std::thread::id thread;

    std::function<void(std::exception_ptr)> callback() {
        return [this](std::exception_ptr e) {
            std::lock_guard lock(m);
            ++calls;
            error = std::move(e);
            thread = std::this_thread::get_id();
            cv.notify_all();
        };
    }
    bool wait(std::chrono::milliseconds timeout = 10s) {
        std::unique_lock lock(m);
        return cv.wait_for(lock, timeout, [&] { return calls > 0; });
    }
    int count() {
        std::lock_guard lock(m);
        return calls;
    }
};

// How a query started on a loop thread ended, and when.
struct Outcome {
    std::mutex m;
    std::condition_variable cv;
    bool done = false;
    std::string value;
    std::exception_ptr error;
    std::chrono::steady_clock::time_point at;

    bool wait(std::chrono::milliseconds timeout = 10s) {
        std::unique_lock lock(m);
        return cv.wait_for(lock, timeout, [&] { return done; });
    }
    bool finished() {
        std::lock_guard lock(m);
        return done;
    }
};

DetachedTask queryInto(const char* sql, std::shared_ptr<Outcome> out) {
    std::string value;
    std::exception_ptr error;
    try {
        value = co_await firstValue(sql);
    } catch (...) {
        error = std::current_exception();
    }
    {
        std::lock_guard lock(out->m);
        out->value = std::move(value);
        out->error = error;
        out->at = std::chrono::steady_clock::now();
        out->done = true;
    }
    out->cv.notify_all();
}

// What PgProvider::query throws on the calling thread, at once.
std::string queryFailure() {
    try {
        (void)PgProvider::query("SELECT 1");
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

// Runs one posted callback per turn, after the inner loop's: what a callback
// posts is still queued when the turn that ran it returns.
struct OnePostPerTurn : Forwarding {
    void post(std::function<void()> cb) {
        {
            std::lock_guard lock(m);
            queue.push_back(std::move(cb));
        }
        inner.post([] {});  // wakes the loop
    }
    void runOnce(int timeout_ms) {
        inner.runOnce(timeout_ms);
        std::function<void()> next;
        {
            std::lock_guard lock(m);
            if (queue.empty()) return;
            next = std::move(queue.front());
            queue.pop_front();
        }
        next();
    }
    std::mutex m;
    std::deque<std::function<void()>> queue;
};

// Keeps what is posted until runHeld(); timers and watches go to the inner loop.
struct HeldPosts : Forwarding {
    void post(std::function<void()> cb) { held.push_back(std::move(cb)); }
    void runHeld() {
        auto now = std::move(held);
        held.clear();
        for (auto& cb : now) cb();
    }
    std::vector<std::function<void()>> held;
};

DetachedTask acquireInto(std::shared_ptr<PgPool<HeldPosts>> pool, std::string& out) {
    try {
        auto guard = co_await pool->acquire();
        out = "lent";
    } catch (const std::exception& e) {
        out = e.what();
    }
}

// Counts the watches relais adds.
template<typename Base>
struct CountedWatches : Base {
    using WatchHandle = typename Base::WatchHandle;
    WatchHandle addWatch(int fd, IoEvent ev, std::function<void(IoEvent)> cb) {
        ++added;
        return Base::addWatch(fd, ev, std::move(cb));
    }
    std::atomic<int> added{0};
};

// What relais logs, while it lives.
class LogCapture {
public:
    LogCapture() : previous_(jcailloux::relais::log::getCallback()) {
        std::lock_guard lock(m());
        lines().clear();
        jcailloux::relais::log::setCallback(&sink);
    }
    ~LogCapture() { jcailloux::relais::log::setCallback(previous_); }
    LogCapture(const LogCapture&) = delete;
    LogCapture& operator=(const LogCapture&) = delete;

    bool contains(const std::string& part) {
        std::lock_guard lock(m());
        for (const auto& line : lines())
            if (line.find(part) != std::string::npos) return true;
        return false;
    }

private:
    static std::mutex& m() {
        static std::mutex mutex;
        return mutex;
    }
    static std::vector<std::string>& lines() {
        static std::vector<std::string> all;
        return all;
    }
    static void sink(jcailloux::relais::log::Level, const char* msg, size_t len) {
        std::lock_guard lock(m());
        lines().emplace_back(msg, len);
    }
    jcailloux::relais::log::Callback previous_;
};

// Waits until relais has left the loop's thread: a teardown started on that
// thread ends on later turns.
template<typename Loop>
bool unboundWithin(LoopThread<Loop>& t, std::chrono::milliseconds limit = 2s) {
    const auto until = std::chrono::steady_clock::now() + limit;
    while (t.call(bound)) {
        if (std::chrono::steady_clock::now() >= until) return false;
        std::this_thread::sleep_for(2ms);
    }
    return true;
}

// Waits until fds is the count of open descriptors, the loop running what is
// left of a teardown meanwhile.
template<typename Loop>
bool fdsBackTo(LoopThread<Loop>& t, int fds) {
    for (int i = 0; i < 200 && countOpenFds() != fds; ++i) {
        std::this_thread::sleep_for(2ms);
        t.settle();
    }
    return countOpenFds() == fds;
}

// The hidden test case `name` of this executable, run in a child process: how
// it ended, and what it printed.
struct ChildEnd {
    int status = 0;
    std::string output;
};

ChildEnd runChild(const char* name) {
    int out[2];
    REQUIRE(::pipe(out) == 0);
    posix_spawn_file_actions_t actions;
    ::posix_spawn_file_actions_init(&actions);
    ::posix_spawn_file_actions_adddup2(&actions, out[1], STDOUT_FILENO);
    ::posix_spawn_file_actions_adddup2(&actions, out[1], STDERR_FILENO);
    ::posix_spawn_file_actions_addclose(&actions, out[0]);
    std::string exe = "test_io_attach";
    std::string test = name;
    char* argv[] = {exe.data(), test.data(), nullptr};
    pid_t child = -1;
    const int spawned = ::posix_spawn(&child, "/proc/self/exe", &actions, nullptr, argv, environ);
    ::posix_spawn_file_actions_destroy(&actions);
    ::close(out[1]);
    REQUIRE(spawned == 0);
    ChildEnd end;
    char buf[512];
    for (ssize_t n; (n = ::read(out[0], buf, sizeof buf)) > 0;) end.output.append(buf, static_cast<size_t>(n));
    ::close(out[0]);
    REQUIRE(::waitpid(child, &end.status, 0) == child);
    return end;
}

void requireAborted(const ChildEnd& end, const std::string& message) {
    INFO(end.output);
    CHECK(WIFSIGNALED(end.status));
    if (WIFSIGNALED(end.status)) CHECK(WTERMSIG(end.status) == SIGABRT);
    CHECK_THAT(end.output, ContainsSubstring(message));
}

// -- integration/fd.h --------------------------------------------------------

bool readable(int fd, std::chrono::milliseconds timeout = 0ms) {
    pollfd pfd{fd, POLLIN, 0};
    return ::poll(&pfd, 1, static_cast<int>(timeout.count())) == 1 && (pfd.revents & POLLIN);
}

// The loop of an application that knows relais by its descriptor alone: it
// calls poll() each time fd() turns readable, until `done`.
template<typename Done>
bool driveFd(FdAttachment& relais, Done done, std::chrono::milliseconds timeout = 10s) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (!done()) {
        if (std::chrono::steady_clock::now() >= until) return false;
        if (readable(relais.fd(), 10ms)) relais.poll();
    }
    return true;
}

DetachedTask afterYield(std::function<void()> then) {
    co_await PgProvider::yieldToLoop();
    then();
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

// An adapter that cannot tell its thread (no isInLoopThread): relais knows the
// loop's thread once it has bound it, and does not wait there for itself.
TEST_CASE("attach: detach on the loop's thread returns at once, whatever the adapter tells",
          "[io][attach][integration]") {
    LoopThread<Forwarding> t;
    const int fds = countOpenFds();
    auto relais = attach(t.loop(), conninfo(), kSmall);
    const auto t0 = std::chrono::steady_clock::now();
    t.call([&] { relais.detach(); });
    CHECK(std::chrono::steady_clock::now() - t0 < 500ms);
    CHECK(fdsBackTo(t, fds));
    CHECK_FALSE(t.call(bound));
}

// An acquire taken off the queue without a connection unwinds on a later
// turn: until then the pool is not quiescent, or a teardown would destroy
// what it is about to unwind through.
TEST_CASE("PgPool: an acquire woken without a connection keeps the pool busy until it resumes",
          "[io][attach][pool]") {
    HeldPosts io;
    SECTION("woken by a failed warm-up") {
        auto pool = PgPool<HeldPosts>::make(io, conninfo(), kSmall.pool);
        std::string out;
        acquireInto(pool, out);
        REQUIRE(pool->waiterCount() == 1);
        pool->failWarmUp(std::make_exception_ptr(std::runtime_error("no server")));
        CHECK(pool->waiterCount() == 0);
        CHECK_FALSE(pool->quiescent());
        io.runHeld();
        CHECK(out == "no server");
        CHECK(pool->quiescent());
    }
    SECTION("woken by acquire_timeout") {
        auto cfg = kSmall.pool;
        cfg.acquire_timeout = 20ms;
        auto pool = PgPool<HeldPosts>::make(io, conninfo(), cfg);
        std::string out;
        acquireInto(pool, out);
        REQUIRE(pool->waiterCount() == 1);
        const auto until = std::chrono::steady_clock::now() + 2s;
        while (pool->waiterCount() == 1 && std::chrono::steady_clock::now() < until) io.runOnce(10);
        REQUIRE(pool->waiterCount() == 0);
        CHECK_FALSE(pool->quiescent());
        io.runHeld();
        CHECK_THAT(out, ContainsSubstring("acquire_timeout"));
        CHECK(pool->quiescent());
    }
}

TEST_CASE("attachHere: binds the loop's thread at once, then connects the pool",
          "[io][attach][here][integration]") {
    LoopThread<EpollIoContext> t;
    const int fds = countOpenFds();
    ReadyLog ready;
    {
        struct AtOnce {
            Attachment<EpollIoContext> relais;
            bool bound = false;
            bool ready = false;
            std::size_t connections = 0;
        };
        auto at = t.call([&] {
            AtOnce r{attachHere(t.loop(), conninfo(), kSmall, ready.callback())};
            r.bound = bound();
            r.ready = r.relais.ready();
            r.connections = r.relais.pool()->totalConnections();
            return r;
        });
        CHECK(at.bound);
        CHECK_FALSE(at.ready);
        CHECK(at.connections == 0);

        REQUIRE(ready.wait());
        CHECK_FALSE(ready.error);
        CHECK(ready.thread == t.call([] { return std::this_thread::get_id(); }));
        CHECK(at.relais.ready());
        CHECK_FALSE(at.relais.error());
        CHECK(t.call([&] { return at.relais.pool()->totalConnections(); }) == 1);
        CHECK(t.run([] { return firstValue("SELECT 40 + 2"); }) == "42");
    }
    CHECK_FALSE(t.call(bound));
    CHECK(fdsBackTo(t, fds));
    CHECK(ready.count() == 1);
}

// The loop is not checked yet: no request may connect on its own.
TEST_CASE("attachHere: requests made before the pool is ready wait for its first connection",
          "[io][attach][here][integration]") {
    static const char* const kSql[] = {"SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4", "SELECT 5"};
    LoopThread<EpollIoContext> t;
    std::vector<std::shared_ptr<Outcome>> outs;
    for (std::size_t i = 0; i < std::size(kSql); ++i) outs.push_back(std::make_shared<Outcome>());
    struct Started {
        Attachment<EpollIoContext> relais;
        std::size_t waiting = 0;
    };
    auto started = t.call([&] {
        Started s{attachHere(t.loop(), conninfo(), kSmall)};
        for (std::size_t i = 0; i < outs.size(); ++i) queryInto(kSql[i], outs[i]);
        s.waiting = s.relais.pool()->waiterCount();
        return s;
    });
    CHECK(started.waiting == outs.size());
    for (std::size_t i = 0; i < outs.size(); ++i) {
        REQUIRE(outs[i]->wait());
        CHECK(what(outs[i]->error).empty());
        CHECK(outs[i]->value == std::to_string(i + 1));
    }
    // Once warmed up, the pool opened what the waiting requests needed.
    CHECK(t.call([&] { return started.relais.pool()->totalConnections(); }) == kSmall.pool.max_connections);
}

TEST_CASE("attachHere: one connection serves every request made before the pool was ready",
          "[io][attach][here][integration]") {
    static const char* const kSql[] = {"SELECT 1", "SELECT 2", "SELECT 3"};
    LoopThread<EpollIoContext> t;
    const AttachConfig single{.pool = {.min_connections = 1, .max_connections = 1}};
    std::vector<std::shared_ptr<Outcome>> outs;
    for (std::size_t i = 0; i < std::size(kSql); ++i) outs.push_back(std::make_shared<Outcome>());
    auto relais = t.call([&] {
        auto r = attachHere(t.loop(), conninfo(), single);
        for (std::size_t i = 0; i < outs.size(); ++i) queryInto(kSql[i], outs[i]);
        return r;
    });
    for (std::size_t i = 0; i < outs.size(); ++i) {
        REQUIRE(outs[i]->wait());
        CHECK(what(outs[i]->error).empty());
        CHECK(outs[i]->value == std::to_string(i + 1));
    }
    CHECK(t.call([&] { return relais.pool()->totalConnections(); }) == 1);
}

TEST_CASE("attachHere: requests made before the loop runs are served once it does",
          "[io][attach][here][integration]") {
    EpollIoContext io;  // this thread will run it
    const int fds = countOpenFds();
    {
        ReadyLog ready;
        auto relais = attachHere(io, conninfo(), kSmall, ready.callback());
        auto out = std::make_shared<Outcome>();
        queryInto("SELECT 7", out);
        CHECK(relais.pool()->waiterCount() == 1);
        const auto until = std::chrono::steady_clock::now() + 10s;
        while ((!out->finished() || ready.count() == 0) && std::chrono::steady_clock::now() < until)
            io.runOnce(10);
        CHECK(out->value == "7");
        CHECK(ready.count() == 1);
        relais.detach();  // the loop's thread: starts the teardown, which the loop runs
        pump(io, 200ms);
        CHECK_FALSE(bound());
    }
    CHECK(countOpenFds() == fds);
}

TEST_CASE("attachHere: with no connection to warm up, the pool is ready once the loop is checked",
          "[io][attach][here][integration]") {
    LoopThread<EpollIoContext> t;
    ReadyLog ready;
    const AttachConfig lazy{.pool = {.min_connections = 0, .max_connections = 2}};
    auto relais = t.call([&] { return attachHere(t.loop(), conninfo(), lazy, ready.callback()); });
    REQUIRE(ready.wait());
    CHECK_FALSE(ready.error);
    CHECK(t.call([&] { return relais.pool()->totalConnections(); }) == 0);
    CHECK(t.run([] { return firstValue("SELECT 3"); }) == "3");
    CHECK(t.call([&] { return relais.pool()->totalConnections(); }) == 1);
}

TEST_CASE("attachHere: a loop that fails its check fails the requests waiting at once",
          "[io][attach][here][integration]") {
    LoopThread<CountedWatches<NoRemoval>> t;
    LogCapture log;
    ReadyLog ready;
    auto out = std::make_shared<Outcome>();
    const auto t0 = std::chrono::steady_clock::now();
    auto relais = t.call([&] {
        auto r = attachHere(t.loop(), conninfo(), kSmall, ready.callback());
        queryInto("SELECT 1", out);
        return r;
    });
    REQUIRE(out->wait());
    CHECK(out->at - t0 < 1s);
    CHECK_THROWS_AS(std::rethrow_exception(out->error), AttachError);
    CHECK_THAT(what(out->error), ContainsSubstring("after its removeWatch returned"));

    REQUIRE(ready.wait());
    CHECK(what(ready.error) == what(out->error));
    CHECK(what(relais.error()) == what(out->error));
    CHECK_FALSE(relais.ready());
    CHECK(log.contains("could not start on this loop"));
    CHECK(t.loop().added == 2);  // the check's: no connection was tried
    // From then on, a call fails at once with it, and so does the pool.
    CHECK(t.call(queryFailure) == what(out->error));
    auto pool = relais.pool();
    const auto t1 = std::chrono::steady_clock::now();
    CHECK(t.run([pool]() -> Task<std::string> {
        try {
            auto guard = co_await pool->acquire();
            co_return "lent";
        } catch (const std::exception& e) {
            co_return e.what();
        }
    }) == what(out->error));
    CHECK(std::chrono::steady_clock::now() - t1 < 1s);
}

TEST_CASE("attachHere: a server relais cannot connect to fails the requests waiting at once",
          "[io][attach][here][integration]") {
    LoopThread<EpollIoContext> t;
    LogCapture log;
    ReadyLog ready;
    auto out = std::make_shared<Outcome>();
    const auto t0 = std::chrono::steady_clock::now();
    auto relais = t.call([&] {
        auto r = attachHere(t.loop(), missingDatabase(), kSmall, ready.callback());
        queryInto("SELECT 1", out);
        return r;
    });
    REQUIRE(out->wait());
    CHECK(out->at - t0 < kSmall.pool.acquire_timeout / 2);
    CHECK_THAT(what(out->error), ContainsSubstring("relais_no_such_database"));
    REQUIRE(ready.wait());
    CHECK(what(ready.error) == what(out->error));
    CHECK(what(relais.error()) == what(out->error));
    CHECK(log.contains("could not start on this loop"));
    CHECK(t.call(queryFailure) == what(out->error));
}

TEST_CASE("attachHere: refuses a thread relais is already attached to", "[io][attach][here][integration]") {
    LoopThread<EpollIoContext> t;
    auto relais = t.call([&] { return attachHere(t.loop(), conninfo(), kSmall); });
    const auto refusal = t.call([&]() -> std::string {
        try {
            (void)attachHere(t.loop(), conninfo(), kSmall);
        } catch (const AttachError& e) {
            return e.what();
        }
        return {};
    });
    CHECK_THAT(refusal, ContainsSubstring("already attached"));
    CHECK(t.run([] { return firstValue("SELECT 1"); }) == "1");
}

TEST_CASE("attachHere: detach before the pool is ready fails the requests waiting",
          "[io][attach][here][integration]") {
    LoopThread<EpollIoContext> t;
    const int fds = countOpenFds();
    ReadyLog ready;
    auto out = std::make_shared<Outcome>();
    t.call([&] {
        auto relais = attachHere(t.loop(), conninfo(), kSmall, ready.callback());
        queryInto("SELECT 1", out);
        relais.detach();  // the loop's thread: the teardown follows the first turn
    });
    REQUIRE(out->wait());
    CHECK_THAT(what(out->error), ContainsSubstring("left the loop before it was ready"));
    CHECK(unboundWithin(t));
    CHECK(fdsBackTo(t, fds));
    std::this_thread::sleep_for(50ms);
    t.settle();
    CHECK(ready.count() == 0);
}

// The boot has ended and on_ready is queued, not run yet, when detach() begins.
TEST_CASE("attachHere: on_ready never runs once detach has begun", "[io][attach][here][integration]") {
    OnePostPerTurn loop;  // this thread runs it
    ReadyLog ready;
    const AttachConfig lazy{.pool = {.min_connections = 0, .max_connections = 1}};
    auto relais = attachHere(loop, conninfo(), lazy, ready.callback());
    const auto until = std::chrono::steady_clock::now() + 10s;
    while (!relais.ready() && std::chrono::steady_clock::now() < until) loop.runOnce(1);
    REQUIRE(relais.ready());
    CHECK(ready.count() == 0);
    relais.detach();  // the loop's thread: starts the teardown
    const auto settled = std::chrono::steady_clock::now() + 200ms;
    while (std::chrono::steady_clock::now() < settled) loop.runOnce(1);
    CHECK(ready.count() == 0);
    CHECK_FALSE(bound());
}

// The check waits for a timer the loop never runs, until acquire_timeout: the
// teardown ends it at once.
TEST_CASE("attachHere: detach while the loop is being checked ends the check at once",
          "[io][attach][here][integration]") {
    LoopThread<SilentTimers> t;
    const int fds = countOpenFds();
    ReadyLog ready;
    auto relais = t.call([&] { return attachHere(t.loop(), conninfo(), kSmall, ready.callback()); });
    std::this_thread::sleep_for(100ms);  // the check waits for its timer by now
    const auto t0 = std::chrono::steady_clock::now();
    relais.detach();
    CHECK(std::chrono::steady_clock::now() - t0 < 1s);
    CHECK(fdsBackTo(t, fds));
    CHECK_FALSE(t.call(bound));
    CHECK(ready.count() == 0);
}

// A loop busy for longer than the 1 s of attach() just after its first turn,
// which starts the check: acquire_timeout bounds the check instead.
TEST_CASE("attachHere: the check of a loop busy as it starts is bounded by acquire_timeout",
          "[io][attach][here][integration]") {
    LoopThread<EpollIoContext> t;
    ReadyLog ready;
    auto relais = t.call([&] {
        auto r = attachHere(t.loop(), conninfo(), kSmall, ready.callback());
        t.loop().post([] { std::this_thread::sleep_for(1500ms); });
        return r;
    });
    REQUIRE(ready.wait());
    CHECK(what(ready.error).empty());
    CHECK(t.run([] { return firstValue("SELECT 1"); }) == "1");
}

TEST_CASE("attachHere child: attached on another thread than the loop's", "[.][attach-child]") {
    LoopThread<EpollIoContext> t;
    auto relais = attachHere(t.loop(), conninfo(), kSmall);  // the loop runs on t's thread
    std::this_thread::sleep_for(3s);
    std::fprintf(stderr, "relais ran on the loop\n");
    std::_Exit(3);
}

TEST_CASE("attachHere: a loop found on another thread than the attaching one ends the process",
          "[io][attach][here]") {
    requireAborted(runChild("attachHere child: attached on another thread than the loop's"),
                   "called on another thread than the one that runs the loop");
}

TEST_CASE("attachHere child: a post run in place", "[.][attach-child]") {
    PostInPlace loop;
    auto relais = attachHere(loop, conninfo(), kSmall);
    std::this_thread::sleep_for(3s);
    std::fprintf(stderr, "relais ran on the loop\n");
    std::_Exit(3);
}

// Its posts never reach the loop's thread: relais cannot even tell it there.
TEST_CASE("attachHere: a loop whose post() runs in place ends the process, saying so",
          "[io][attach][here]") {
    requireAborted(runChild("attachHere child: a post run in place"), "on the thread that posted it");
}

// -- through one descriptor (integration/fd.h) -------------------------------

TEST_CASE("attachHere (fd): relais served through a loop that watches its descriptor alone",
          "[io][attach][fd][integration]") {
    const int fds = countOpenFds();
    {
        ReadyLog ready;
        auto relais = attachHere(conninfo(), kSmall, ready.callback());
        CHECK(bound());
        CHECK_FALSE(relais.ready());
        auto out = std::make_shared<Outcome>();
        queryInto("SELECT 40 + 2", out);  // on this thread, outside poll()
        REQUIRE(driveFd(relais, [&] { return out->finished() && ready.count() > 0; }));
        CHECK(out->value == "42");
        CHECK_FALSE(ready.error);
        CHECK(ready.thread == std::this_thread::get_id());
        CHECK(relais.ready());
        CHECK(relais.pool()->totalConnections() == 1);
        relais.detach();
        CHECK(relais.fd() == -1);
        CHECK_FALSE(bound());
        relais.poll();  // nothing to do any more
    }
    CHECK(countOpenFds() == fds);
}

TEST_CASE("attachHere (fd): detach while the pool connects ends the connect",
          "[io][attach][fd][integration]") {
    const int fds = countOpenFds();
    {
        auto relais = attachHere(conninfo(), kSmall);
        relais.poll();  // the first turn: the pool starts connecting
        CHECK_FALSE(relais.ready());
        CHECK(countOpenFds() > fds + 1);  // relais's loop, and a connection under way
        relais.detach();
        CHECK_FALSE(bound());
    }
    CHECK(countOpenFds() == fds);
}

TEST_CASE("attachHere (fd): the descriptor is quiet while relais has nothing to do",
          "[io][attach][fd][integration]") {
    auto relais = attachHere(conninfo(), kSmall);
    auto out = std::make_shared<Outcome>();
    queryInto("SELECT 1", out);
    REQUIRE(driveFd(relais, [&] { return out->finished() && relais.ready(); }));
    REQUIRE(driveFd(relais, [&] { return !readable(relais.fd()); }));  // what the answer left
    CHECK_FALSE(readable(relais.fd(), 300ms));
}

// What this thread starts between two poll() calls posts there: the descriptor
// must say so, for nothing else wakes the loop that watches it.
TEST_CASE("attachHere (fd): work begun outside poll() makes the descriptor readable",
          "[io][attach][fd][integration]") {
    auto relais = attachHere(conninfo(), kSmall);
    REQUIRE(driveFd(relais, [&] { return relais.ready(); }));
    REQUIRE(driveFd(relais, [&] { return !readable(relais.fd()); }));
    bool resumed = false;
    afterYield([&] { resumed = true; });
    CHECK_FALSE(resumed);
    CHECK(readable(relais.fd(), 1s));
    relais.poll();
    CHECK(resumed);
}

TEST_CASE("attachHere (fd): poll() and detach() belong to the thread that attached relais",
          "[io][attach][fd][integration]") {
    auto relais = attachHere(conninfo(), kSmall);
    std::string from_poll, from_detach;
    std::thread([&] {
        try {
            relais.poll();
        } catch (const std::logic_error& e) {
            from_poll = e.what();
        }
        try {
            relais.detach();
        } catch (const std::logic_error& e) {
            from_detach = e.what();
        }
    }).join();
    CHECK_THAT(from_poll, ContainsSubstring("another thread"));
    CHECK_THAT(from_detach, ContainsSubstring("another thread"));

    std::string inside;
    afterYield([&] {
        try {
            relais.detach();
        } catch (const std::logic_error& e) {
            inside = e.what();
        }
    });
    REQUIRE(driveFd(relais, [&] { return !inside.empty(); }));
    CHECK_THAT(inside, ContainsSubstring("inside poll()"));

    auto out = std::make_shared<Outcome>();
    queryInto("SELECT 5", out);
    REQUIRE(driveFd(relais, [&] { return out->finished(); }));
    CHECK(out->value == "5");
}

TEST_CASE("attachHere (fd): a server relais cannot connect to fails the requests waiting at once",
          "[io][attach][fd][integration]") {
    ReadyLog ready;
    auto relais = attachHere(missingDatabase(), kSmall, ready.callback());
    auto out = std::make_shared<Outcome>();
    const auto t0 = std::chrono::steady_clock::now();
    queryInto("SELECT 1", out);
    REQUIRE(driveFd(relais, [&] { return out->finished() && ready.count() > 0; }));
    CHECK(out->at - t0 < kSmall.pool.acquire_timeout / 2);
    CHECK_THAT(what(out->error), ContainsSubstring("relais_no_such_database"));
    CHECK(what(ready.error) == what(out->error));
    CHECK(what(relais.error()) == what(out->error));
    CHECK(queryFailure() == what(out->error));
}

TEST_CASE("attachHere child: an FdAttachment destroyed on another thread", "[.][attach-child]") {
    auto* relais = new FdAttachment(attachHere(conninfo(), kSmall));
    relais->poll();  // its first turn, on this thread
    std::thread([relais] { delete relais; }).join();
    std::fprintf(stderr, "destroyed\n");
    std::_Exit(3);
}

TEST_CASE("attachHere (fd): an attachment destroyed on another thread ends the process",
          "[io][attach][fd]") {
    requireAborted(runChild("attachHere child: an FdAttachment destroyed on another thread"),
                   "than the one that called attachHere()");
}
