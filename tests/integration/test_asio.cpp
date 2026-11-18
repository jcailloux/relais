// relais on asio, through the shipped adapter: the adapter meets the IoContext
// contract and never lets a second thread run relais's callbacks, and
// attach(ctx, …) binds relais to an io_context that one thread runs, refuses
// one that several threads run, and leaves the io_context no work once
// detached, so that its run() returns. attachHere(ctx, …) binds the thread
// that runs it, and pins relais's callbacks there. Built with RELAIS_TEST_BOOST_ASIO,
// the same cases run on Boost.Asio (test_boost_asio). The PostgreSQL scenarios
// of the adversarial loops run on asio in test_asio_loops.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <fixtures/AsioFlavor.h>

#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/WhenAll.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/runtime/Attach.h>
#include <jcailloux/relais/testing/IoContextConformance.h>

#include <fixtures/LivenessHarness.h>
#include <fixtures/TestConnInfo.h>

#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <cstdint>
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
using relais_test::AsioIo;
namespace net = relais_test::net;

namespace {

const char* conninfo() { return relais_test::kConnInfo; }

using WorkGuard = net::executor_work_guard<net::io_context::executor_type>;

// An io_context as an application runs it: on threads of its own, kept running
// by a work guard until release().
class RunningContext {
public:
    explicit RunningContext(int threads = 1) {
        // asio opens its reactor's descriptors (epoll, eventfd, timerfd) with
        // the first timer or watched descriptor: before a test counts them.
        { net::steady_timer first(ctx); }
        for (int i = 0; i < threads; ++i) {
            threads_.emplace_back([this] {
                ctx.run();
                returned_.fetch_add(1);
            });
        }
        // Every thread inside run(): one callback each, all running at once.
        auto running = std::make_shared<std::latch>(threads);
        for (int i = 0; i < threads; ++i) net::post(ctx, [running] { running->arrive_and_wait(); });
        running->wait();
    }

    ~RunningContext() {
        work_.reset();
        ctx.stop();
        threads_.clear();
    }

    RunningContext(const RunningContext&) = delete;
    RunningContext& operator=(const RunningContext&) = delete;

    // From now on, run() returns once the io_context has no work left.
    void release() { work_.reset(); }

    // Every thread has left run() within `limit`.
    bool returnedWithin(std::chrono::milliseconds limit) const {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (returned_.load() != static_cast<int>(threads_.size())) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::sleep_for(1ms);
        }
        return true;
    }

    net::io_context ctx;

private:
    std::optional<WorkGuard> work_{net::make_work_guard(ctx)};
    std::atomic<int> returned_{0};
    std::vector<std::jthread> threads_;
};

// What callOn and runOn post through.
struct Poster {
    net::io_context& ctx;
    void post(std::function<void()> cb) { net::post(ctx, std::move(cb)); }
};

// The likeliest mistake in an asio adapter: post() through dispatch, which runs
// the callback at once on the io_context's thread.
struct DispatchingIo {
    using WatchHandle = AsioIo::WatchHandle;

    explicit DispatchingIo(net::io_context& ctx) : inner(ctx) {}

    WatchHandle addWatch(int fd, IoEvent mask, std::function<void(IoEvent)> cb) {
        return inner.addWatch(fd, mask, std::move(cb));
    }
    void updateWatch(WatchHandle h, IoEvent mask) { inner.updateWatch(h, mask); }
    void removeWatch(WatchHandle h) { inner.removeWatch(h); }
    void post(std::function<void()> cb) { net::dispatch(inner.context(), std::move(cb)); }
    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        inner.postDelayed(delay, std::move(cb));
    }
    [[nodiscard]] bool isInLoopThread() const noexcept { return inner.isInLoopThread(); }

    AsioIo inner;
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
    std::string exe = "test_asio";
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

std::string attachError(net::io_context& ctx) {
    try {
        auto relais = jcailloux::relais::attach(ctx, conninfo());
    } catch (const AttachError& e) {
        return e.what();
    }
    return {};
}

}  // namespace

TEST_CASE("asio: the shipped adapter meets the IoContext contract", "[asio][conformance]") {
    INFO(relais_test::kAsioFlavor);
    net::io_context ctx;
    auto work = net::make_work_guard(ctx);
    AsioIo io(ctx);
    auto drive = [&ctx](AsioIo&, auto pred) {
        while (!pred()) ctx.run_one_for(10ms);
    };
    IoContextConformance::runAll(io, drive);
}

TEST_CASE("asio: attach binds relais to an io_context one thread runs, and detach lets go",
          "[asio][attach][integration]") {
    RunningContext rc;
    Poster loop{rc.ctx};
    const int fds_before = countOpenFds();
    {
        auto relais = jcailloux::relais::attach(rc.ctx, conninfo(),
            {.pool = {.min_connections = 2, .max_connections = 2}});
        CHECK(runOn(loop, sixTimesSeven) == "42");

        // Concurrent requests share the loop's two connections.
        int ok = runOn(loop, []() -> Task<int> {
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
        CHECK(callOn(loop, [] { return PgProvider::initialized(); }));
    }
    // Taken off: the thread no longer routes through relais, and nothing relais
    // opened on it is left.
    CHECK_FALSE(callOn(loop, [] { return PgProvider::initialized(); }));
    CHECK(countOpenFds() <= fds_before);

    // Attached again, to the same io_context.
    auto relais = jcailloux::relais::attach(rc.ctx, conninfo(),
        {.pool = {.min_connections = 1, .max_connections = 1}});
    CHECK(runOn(loop, sixTimesSeven) == "42");
}

TEST_CASE("asio: detach waits for a request in flight", "[asio][attach][integration]") {
    RunningContext rc;
    Poster loop{rc.ctx};
    auto relais = jcailloux::relais::attach(rc.ctx, conninfo(),
        {.pool = {.min_connections = 1, .max_connections = 1}});

    std::atomic<bool> started{false};
    std::atomic<int> outcome{0};  // 1: answered, -1: failed
    callOn(loop, [&] {
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

TEST_CASE("asio: attach refuses the io_context's own thread", "[asio][attach]") {
    RunningContext rc;
    Poster loop{rc.ctx};
    std::string error = callOn(loop, [&] { return attachError(rc.ctx); });
    CHECK_THAT(error, Catch::Matchers::ContainsSubstring("loop's own thread"));
}

TEST_CASE("asio: attach refuses an io_context that several threads run", "[asio][attach]") {
    for (int threads : {2, 4}) {
        INFO(threads << " threads");
        RunningContext rc(threads);
        Poster loop{rc.ctx};
        const int fds = countOpenFds();
        const auto t0 = std::chrono::steady_clock::now();
        for (int attempt = 0; attempt < 20; ++attempt) {
            CHECK_THAT(attachError(rc.ctx),
                       Catch::Matchers::ContainsSubstring("several threads run this io_context"));
        }
        // Refused before relais connects, without waiting out a deadline.
        CHECK(std::chrono::steady_clock::now() - t0 < 2s);
        for (int i = 0; i < 100 && countOpenFds() != fds; ++i) {
            std::this_thread::sleep_for(2ms);
            callOn(loop, [] {});
        }
        CHECK(countOpenFds() == fds);
    }
}

// Run by the next case, in a process of its own: a thread joins the io_context
// after attach, where the probe could not see it, and gets one of relais's
// callbacks.
TEST_CASE("asio: a thread joins the io_context after attach", "[.][asio]") {
    net::io_context ctx;
    auto work = net::make_work_guard(ctx);
    std::promise<std::thread::id> first_id;
    std::jthread first([&] {
        first_id.set_value(std::this_thread::get_id());
        ctx.run();
    });
    const auto first_thread = first_id.get_future().get();
    auto relais = jcailloux::relais::attach(ctx, conninfo(),
        {.pool = {.min_connections = 1, .max_connections = 1}});
    std::jthread second([&ctx] { ctx.run(); });

    // On relais's thread: send a request, then hold that thread, so that only
    // the second one is left to take its answer.
    std::function<void()> hold = [&] {
        if (std::this_thread::get_id() != first_thread) return net::post(ctx, hold);
        []() -> DetachedTask {
            auto r = co_await PgProvider::query("SELECT pg_sleep(0.05)");
            (void)r;
        }();
        std::this_thread::sleep_for(3s);
    };
    net::post(ctx, hold);
    std::this_thread::sleep_for(2s);
    std::fprintf(stderr, "the second thread ran relais's callback\n");
    std::_Exit(3);  // no teardown: relais's state may be corrupted by now
}

TEST_CASE("asio: a thread that joins the io_context after attach ends the process",
          "[asio][attach]") {
    const auto end = runChild("asio: a thread joins the io_context after attach");
    INFO(end.output);
    CHECK(WIFSIGNALED(end.status));
    CHECK(WTERMSIG(end.status) == SIGABRT);
    CHECK_THAT(end.output, Catch::Matchers::ContainsSubstring("several threads run the io_context"));
}

// Before run(), on the thread that then runs it: a request made at once is
// served once run() starts, on_ready tells the pool is ready, and run()
// returns once relais, detached on that thread, has left.
TEST_CASE("asio: attachHere binds relais to the thread that runs the io_context",
          "[asio][attach][here][integration]") {
    net::io_context ctx;
    { net::steady_timer first(ctx); }  // asio's own descriptors, before counting
    const int fds = countOpenFds();
    struct State {
        std::optional<Attachment<AsioIo>> relais;
        std::string value;
        int ready_calls = 0;
        std::exception_ptr ready_error;
        std::thread::id ready_thread;
        void detachOnceDone() {
            if (!value.empty() && ready_calls > 0) relais->detach();  // only starts it
        }
    } state;
    state.relais.emplace(jcailloux::relais::attachHere(
        ctx, conninfo(), {.pool = {.min_connections = 1, .max_connections = 2}},
        [&state](std::exception_ptr e) {
            ++state.ready_calls;
            state.ready_error = e;
            state.ready_thread = std::this_thread::get_id();
            state.detachOnceDone();
        }));
    CHECK(PgProvider::initialized());
    [](State& s) -> DetachedTask {
        try {
            s.value = co_await sixTimesSeven();
        } catch (const std::exception& e) {
            s.value = e.what();
        }
        s.detachOnceDone();
    }(state);
    ctx.run_for(10s);
    CHECK(ctx.stopped());  // out of work, not out of time
    CHECK(state.value == "42");
    CHECK(state.ready_calls == 1);
    CHECK_FALSE(state.ready_error);
    CHECK(state.ready_thread == std::this_thread::get_id());
    CHECK_FALSE(PgProvider::initialized());
    state.relais.reset();
    CHECK(countOpenFds() == fds);
}

// A former attach pinned relais to the thread that ran the io_context then:
// attachHere pins the thread that runs it now.
TEST_CASE("asio: attachHere after an attach from a thread that ran the io_context before",
          "[asio][attach][here][integration]") {
    net::io_context ctx;
    {
        auto work = net::make_work_guard(ctx);
        std::jthread first([&ctx] { ctx.run(); });
        auto relais = jcailloux::relais::attach(ctx, conninfo(),
            {.pool = {.min_connections = 1, .max_connections = 1}});
        relais.detach();
        work.reset();
        first.join();
    }
    ctx.restart();
    std::string value;
    auto relais = jcailloux::relais::attachHere(ctx, conninfo(),
        {.pool = {.min_connections = 1, .max_connections = 1}});
    [](std::string& out, Attachment<AsioIo>& r) -> DetachedTask {
        try {
            out = co_await sixTimesSeven();
        } catch (const std::exception& e) {
            out = e.what();
        }
        r.detach();
    }(value, relais);
    ctx.run_for(10s);
    CHECK(ctx.stopped());
    CHECK(value == "42");
}

// Run by the next case, in a process of its own: attachHere on this thread,
// whose first turn pins relais here; then another thread alone runs the
// io_context, and gets relais's next callback.
TEST_CASE("asio: a second thread runs the io_context after attachHere", "[.][asio]") {
    net::io_context ctx;
    auto work = net::make_work_guard(ctx);
    auto relais = jcailloux::relais::attachHere(ctx, conninfo(),
        {.pool = {.min_connections = 1, .max_connections = 1}});
    ctx.run_one();  // relais's first turn, on this thread
    std::jthread second([&ctx] { ctx.run(); });
    std::this_thread::sleep_for(3s);
    std::fprintf(stderr, "the second thread ran relais's callback\n");
    std::_Exit(3);
}

TEST_CASE("asio: after attachHere, a second thread that runs the io_context ends the process",
          "[asio][attach][here]") {
    const auto end = runChild("asio: a second thread runs the io_context after attachHere");
    INFO(end.output);
    CHECK(WIFSIGNALED(end.status));
    CHECK(WTERMSIG(end.status) == SIGABRT);
    CHECK_THAT(end.output, Catch::Matchers::ContainsSubstring("several threads run the io_context"));
}

TEST_CASE("asio: attach rejects a post() that runs in place", "[asio][attach]") {
    RunningContext rc;
    DispatchingIo io(rc.ctx);
    const int fds = countOpenFds();
    std::string error;
    try {
        auto relais = jcailloux::relais::attach(io, conninfo());
    } catch (const AttachError& e) {
        error = e.what();
    }
    CHECK_THAT(error, Catch::Matchers::ContainsSubstring("before that callback returned"));
    // The check lets go of its sockets once the watch's callback has returned.
    Poster loop{rc.ctx};
    for (int i = 0; i < 100 && countOpenFds() != fds; ++i) {
        std::this_thread::sleep_for(2ms);
        callOn(loop, [] {});
    }
    CHECK(countOpenFds() == fds);
}

TEST_CASE("asio: attach fails within about a second on an io_context nobody runs",
          "[asio][attach]") {
    net::io_context ctx;
    const auto t0 = std::chrono::steady_clock::now();
    CHECK_THAT(attachError(ctx), Catch::Matchers::ContainsSubstring("must be running"));
    CHECK(std::chrono::steady_clock::now() - t0 < 3s);
}

TEST_CASE("asio: relais on several io_contexts, one thread each", "[asio][attach][integration]") {
    constexpr int kContexts = 3;
    std::vector<std::unique_ptr<RunningContext>> contexts;
    std::vector<Attachment<AsioIo>> attached;
    for (int i = 0; i < kContexts; ++i) {
        contexts.push_back(std::make_unique<RunningContext>());
        attached.push_back(jcailloux::relais::attach(contexts.back()->ctx, conninfo(),
            {.pool = {.min_connections = 1, .max_connections = 2}}));
    }

    // Each io_context answers on its own pool, all of them at once.
    struct Answer {
        std::promise<int32_t> value;
        std::thread::id thread;
    };
    std::vector<Answer> answers(kContexts);
    for (int i = 0; i < kContexts; ++i) {
        net::post(contexts[i]->ctx, [&answer = answers[i], n = i + 10] {
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
    for (int i = 0; i < kContexts; ++i) {
        auto value = answers[i].value.get_future();
        REQUIRE(value.wait_for(5s) == std::future_status::ready);
        CHECK(value.get() == 2 * (i + 10));
    }
    CHECK(answers[0].thread != answers[1].thread);
    CHECK(answers[1].thread != answers[2].thread);

    attached.clear();
    for (auto& rc : contexts) {
        Poster loop{rc->ctx};
        CHECK_FALSE(callOn(loop, [] { return PgProvider::initialized(); }));
    }
}

TEST_CASE("asio: run() returns once relais is detached", "[asio][attach][integration]") {
    // Default timeouts: a request leaves a loop timer armed for query_timeout
    // (30 s), which relais never cancels.
    SECTION("detached from another thread") {
        RunningContext rc;
        Poster loop{rc.ctx};
        auto relais = jcailloux::relais::attach(rc.ctx, conninfo(),
            {.pool = {.min_connections = 1, .max_connections = 1}});
        CHECK(runOn(loop, sixTimesSeven) == "42");
        rc.release();
        CHECK_FALSE(rc.returnedWithin(100ms));  // relais's connection keeps it running
        relais.detach();
        CHECK(rc.returnedWithin(2s));
    }
    SECTION("detached on the io_context's thread") {
        RunningContext rc;
        Poster loop{rc.ctx};
        auto relais = jcailloux::relais::attach(rc.ctx, conninfo(),
            {.pool = {.min_connections = 1, .max_connections = 1}});
        CHECK(runOn(loop, sixTimesSeven) == "42");
        rc.release();
        net::post(rc.ctx, [&relais] { relais.detach(); });  // only starts it
        CHECK(rc.returnedWithin(2s));
    }
}

TEST_CASE("asio: relais attaches again once another thread runs the io_context",
          "[asio][attach][integration]") {
    net::io_context ctx;
    Poster loop{ctx};
    std::latch end(1);  // keeps each thread alive, so that their ids differ
    struct Release {
        std::latch& end;
        ~Release() { end.count_down(); }  // before the threads are joined, even on failure
    };
    std::vector<std::jthread> threads;
    Release release{end};
    std::vector<std::thread::id> ran_on;
    std::atomic<int> returned{0};  // runs of run() over
    for (int round = 0; round < 2; ++round) {
        ctx.restart();
        auto work = std::make_optional(net::make_work_guard(ctx));
        threads.emplace_back([&ctx, &end, &returned] {
            ctx.run();
            returned.fetch_add(1);
            end.wait();
        });
        auto relais = jcailloux::relais::attach(ctx, conninfo(),
            {.pool = {.min_connections = 1, .max_connections = 1}});
        CHECK(runOn(loop, sixTimesSeven) == "42");
        ran_on.push_back(callOn(loop, [] { return std::this_thread::get_id(); }));
        relais.detach();
        work.reset();  // run() returns: relais left nothing behind
        while (returned.load() == round) std::this_thread::sleep_for(1ms);
    }
    CHECK(ran_on[0] != ran_on[1]);
}

TEST_CASE("asio: what the adapter leaves pending outlives it and its io_context",
          "[asio]") {
    SECTION("a timer pending when the adapter goes never calls back") {
        net::io_context ctx;
        bool ran = false;
        {
            AsioIo io(ctx);
            io.postDelayed(1h, [&ran] { ran = true; });
            io.postDelayed(0ms, [&ran] { ran = true; });
        }
        ctx.run_for(20ms);
        CHECK_FALSE(ran);
    }
    SECTION("a removed watch whose io_context ends before its aborted wait runs") {
        int sv[2];
        REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
        {
            net::io_context ctx;
            AsioIo io(ctx);
            auto watch = io.addWatch(sv[0], IoEvent::Read | IoEvent::Write, [](IoEvent) {});
            io.removeWatch(watch);  // freed with the io_context (LeakSanitizer)
        }
        CHECK(::close(sv[0]) == 0);  // released to relais, never closed
        ::close(sv[1]);
    }
}
