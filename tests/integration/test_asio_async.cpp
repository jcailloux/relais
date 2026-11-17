// relais::async on asio: a relais Task or Immediate as an asio asynchronous
// operation. Every completion token gets the request's value, or the exception
// it ended with; a call ready at once completes through the handler's
// immediate executor; a cancelled operation completes at once while its
// request runs on; the handler's own executor is kept busy until it completes;
// a steady loop allocates nothing for the bridge. Built with
// RELAIS_TEST_BOOST_ASIO, the same cases run on Boost.Asio
// (test_boost_asio_async).

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <fixtures/AllocationCounter.h>
#include <fixtures/AsioFlavor.h>
#include <fixtures/TestConnInfo.h>
#include <fixtures/TestRepositories.h>

#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/runtime/Attach.h>

#ifdef RELAIS_TEST_BOOST_ASIO
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/bind_immediate_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#define RELAIS_TEST_NET_VERSION BOOST_ASIO_VERSION
using NetSystemError = boost::system::system_error;
#else
#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/bind_cancellation_slot.hpp>
#include <asio/bind_executor.hpp>
#include <asio/bind_immediate_executor.hpp>
#include <asio/cancellation_signal.hpp>
#include <asio/co_spawn.hpp>
#include <asio/deferred.hpp>
#include <asio/detached.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/use_future.hpp>
#define RELAIS_TEST_NET_VERSION ASIO_VERSION
using NetSystemError = asio::system_error;
#endif

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

using namespace jcailloux::relais::io;
using namespace std::chrono_literals;
using jcailloux::relais::Attachment;
using jcailloux::relais::PgProvider;
using relais_test::AsioIo;
using relais_test::L1TestItemRepo;
namespace net = relais_test::net;
namespace relais = jcailloux::relais;

namespace {

using Clock = std::chrono::steady_clock;

// An io_context that one thread runs, with relais attached to it. An exception
// thrown out of run() is recorded, and the thread runs the io_context again.
class AttachedLoop {
public:
    AttachedLoop() {
        thread_ = std::thread([this] { runLoop(); });
        relais_ = relais::attach(ctx, relais_test::kConnInfo,
                                 {.pool = {.min_connections = 1, .max_connections = 2}});
    }

    ~AttachedLoop() {
        relais_.detach();
        work_.reset();
        ctx.stop();
        thread_.join();
    }

    AttachedLoop(const AttachedLoop&) = delete;
    AttachedLoop& operator=(const AttachedLoop&) = delete;

    // Runs `f` on the loop thread; returns its value, or rethrows its exception.
    template<typename F>
    auto call(F f) -> std::invoke_result_t<F&> {
        std::packaged_task<std::invoke_result_t<F&>()> task(std::move(f));
        auto result = task.get_future();
        net::post(ctx, [&task] { task(); });
        return result.get();
    }

    // Runs `coro` on the loop thread, as a coroutine of asio, and returns its value.
    template<typename T>
    T run(net::awaitable<T> coro) {
        return net::co_spawn(ctx, std::move(coro), net::use_future).get();
    }

    void detach() { relais_.detach(); }

    // The exceptions run() has thrown so far.
    std::vector<std::exception_ptr> escaped() {
        std::lock_guard lock(m_);
        return escaped_;
    }

    [[nodiscard]] std::thread::id thread() const { return thread_.get_id(); }

    net::io_context ctx;

private:
    void runLoop() {
        for (;;) {
            try {
                ctx.run();
                return;
            } catch (...) {
                std::lock_guard lock(m_);
                escaped_.push_back(std::current_exception());
            }
        }
    }

    net::executor_work_guard<net::io_context::executor_type> work_ = net::make_work_guard(ctx);
    std::thread thread_;
    Attachment<AsioIo> relais_;
    std::mutex m_;
    std::vector<std::exception_ptr> escaped_;
};

// Waits until `pred` holds, for `limit` at most.
template<typename Pred>
bool eventually(Pred pred, std::chrono::milliseconds limit = 2s) {
    const auto deadline = Clock::now() + limit;
    while (!pred()) {
        if (Clock::now() >= deadline) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

Task<int32_t> doubled(int32_t n) {
    auto result = co_await PgProvider::queryParams("SELECT $1::int * 2", PgParams::make(n));
    co_return result[0].template get<int32_t>(0);
}

// `value`, after `seconds` in PostgreSQL; sets `ran` once it has its answer.
Task<int32_t> slowly(double seconds, int32_t value, std::shared_ptr<std::atomic<bool>> ran) {
    auto result = co_await PgProvider::queryParams("SELECT pg_sleep($1), $2::int",
                                                   PgParams::make(seconds, value));
    ran->store(true);
    co_return result[0].template get<int32_t>(1);
}

Task<int32_t> divisionByZero() {
    auto result = co_await PgProvider::query("SELECT 1/0");
    co_return result[0].template get<int32_t>(0);
}

Task<void> failing() {
    co_await PgProvider::query("SELECT 1/0");
}

Task<void> touching(std::shared_ptr<std::atomic<int>> count) {
    co_await PgProvider::query("SELECT 1");
    ++*count;
}

// Never suspends.
Task<int32_t> computed(int32_t value) {
    co_return value;
}

// Sets `started` before its first suspension.
Task<int32_t> startingThenDoubled(bool& started, int32_t n) {
    started = true;
    co_return co_await doubled(n);
}

// N requests, in one Task.
Task<int64_t> manyDoubled(int32_t n) {
    int64_t sum = 0;
    for (int32_t i = 0; i < n; ++i) sum += co_await doubled(i);
    co_return sum;
}

bool aborted(std::exception_ptr e) {
    try {
        if (e) std::rethrow_exception(e);
    } catch (const NetSystemError& error) {
        return error.code() == net::error::operation_aborted;
    } catch (...) {
    }
    return false;
}

template<typename Exception>
bool holds(std::exception_ptr e) {
    try {
        if (e) std::rethrow_exception(e);
    } catch (const Exception&) {
        return true;
    } catch (...) {
    }
    return false;
}

// What a callback saw.
template<typename T>
struct Completion {
    std::exception_ptr error;
    T value{};
    std::thread::id thread;
};

}  // namespace

// ---------------------------------------------------------------------------

namespace {

net::awaitable<int32_t> awaitedThroughUseAwaitable(int32_t n) {
    co_return co_await relais::async(doubled(n), net::use_awaitable);
}

net::awaitable<int32_t> awaitedThroughDeferred(int32_t n) {
    co_return co_await relais::async(doubled(n), net::deferred);
}

net::awaitable<std::tuple<std::exception_ptr, int32_t>> awaitedAsTuple(int32_t n) {
    co_return co_await relais::async(doubled(n), net::as_tuple(net::use_awaitable));
}

net::awaitable<int> awaitedVoid() {
    auto count = std::make_shared<std::atomic<int>>(0);
    co_await relais::async(touching(count), net::use_awaitable);
    co_return count->load();
}

#if RELAIS_TEST_NET_VERSION >= 102900
net::awaitable<int32_t> awaitedThroughDefaultToken(int32_t n) {
    co_return co_await relais::async(doubled(n));
}
#endif

}  // namespace

TEST_CASE("asio async: every completion token gets the request's value", "[asio][async]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;

    SECTION("use_awaitable") {
        CHECK(loop.run(awaitedThroughUseAwaitable(21)) == 42);
    }
    SECTION("deferred, awaited") {
        CHECK(loop.run(awaitedThroughDeferred(4)) == 8);
    }
    SECTION("deferred, launched later with a callback") {
        std::promise<Completion<int32_t>> seen;
        loop.call([&] {
            auto op = relais::async(doubled(3), net::deferred);
            std::move(op)([&](std::exception_ptr e, int32_t v) {
                seen.set_value({e, v, std::this_thread::get_id()});
            });
        });
        auto c = seen.get_future().get();
        CHECK_FALSE(c.error);
        CHECK(c.value == 6);
    }
    SECTION("as_tuple") {
        auto [error, value] = loop.run(awaitedAsTuple(5));
        CHECK_FALSE(error);
        CHECK(value == 10);
    }
    SECTION("a callback, run on the loop thread") {
        std::promise<Completion<int32_t>> seen;
        loop.call([&] {
            relais::async(doubled(7), [&](std::exception_ptr e, int32_t v) {
                seen.set_value({e, v, std::this_thread::get_id()});
            });
        });
        auto c = seen.get_future().get();
        CHECK_FALSE(c.error);
        CHECK(c.value == 14);
        CHECK(c.thread == loop.thread());
    }
    SECTION("use_future") {
        auto future = loop.call([] { return relais::async(doubled(6), net::use_future); });
        CHECK(future.get() == 12);
    }
    SECTION("a Task<void>") {
        CHECK(loop.run(awaitedVoid()) == 1);
        std::promise<std::exception_ptr> seen;
        loop.call([&] {
            relais::async(touching(std::make_shared<std::atomic<int>>(0)),
                          [&](std::exception_ptr e) { seen.set_value(e); });
        });
        CHECK_FALSE(seen.get_future().get());
    }
#if RELAIS_TEST_NET_VERSION >= 102900
    SECTION("the default token, deferred") {
        CHECK(loop.run(awaitedThroughDefaultToken(9)) == 18);
    }
#endif
}

// ---------------------------------------------------------------------------

namespace {

struct RepositoryRoundTrip {
    bool miss_ready = true;
    bool hit_ready = false;
    std::string from_postgres;
    std::string from_cache;
    std::optional<size_t> erased;
};

net::awaitable<RepositoryRoundTrip> repositoryRoundTrip(relais_test::TestItemEntity item) {
    RepositoryRoundTrip seen;
    auto inserted = co_await relais::async(L1TestItemRepo::insert(item), net::use_awaitable);
    const auto id = inserted->id;
    L1TestItemRepo::evict(id);

    auto miss = L1TestItemRepo::find(id);
    seen.miss_ready = miss.await_ready();
    auto from_postgres = co_await relais::async(std::move(miss), net::use_awaitable);
    seen.from_postgres = from_postgres ? from_postgres->name : "";

    auto hit = L1TestItemRepo::find(id);
    seen.hit_ready = hit.await_ready();
    auto from_cache = co_await relais::async(std::move(hit), net::use_awaitable);
    seen.from_cache = from_cache ? from_cache->name : "";

    seen.erased = co_await relais::async(L1TestItemRepo::erase(id), net::use_awaitable);
    co_return seen;
}

}  // namespace

TEST_CASE("asio async: a repository call, from PostgreSQL, then from relais's cache",
          "[asio][async][integration]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;
    auto seen = loop.run(repositoryRoundTrip(relais_test::makeTestItem("relais::async", 7)));
    CHECK_FALSE(seen.miss_ready);
    CHECK(seen.from_postgres == "relais::async");
    CHECK(seen.hit_ready);
    CHECK(seen.from_cache == "relais::async");
    CHECK(seen.erased == std::optional<size_t>(1));
}

// ---------------------------------------------------------------------------

TEST_CASE("asio async: a call ready at once completes through the handler's immediate executor",
          "[asio][async]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;

    // Whether the callback ran before relais::async returned, and what it got.
    struct Ready {
        bool in_place = false;
        std::shared_future<Completion<int32_t>> completion;
    };
    auto complete = [&loop](auto&& start) {
        auto seen = std::make_shared<std::promise<Completion<int32_t>>>();
        Ready ready{.completion = seen->get_future().share()};
        ready.in_place = loop.call([&start, seen, completion = ready.completion] {
            start([seen](std::exception_ptr e, int32_t v) {
                seen->set_value({e, v, std::this_thread::get_id()});
            });
            return completion.wait_for(0s) == std::future_status::ready;
        });
        return ready;
    };
    auto bound = [&loop](auto callback) {
        return net::bind_immediate_executor(loop.ctx.get_executor(), std::move(callback));
    };

    SECTION("a hit in relais's cache: posted to the loop by default") {
        auto ready = complete([](auto cb) { relais::async(Immediate<int32_t>(3), std::move(cb)); });
        CHECK_FALSE(ready.in_place);
        CHECK(ready.completion.get().value == 3);
        CHECK(ready.completion.get().thread == loop.thread());
    }
    SECTION("a hit in relais's cache: in place with an immediate executor bound") {
        auto ready = complete([&](auto cb) { relais::async(Immediate<int32_t>(3), bound(std::move(cb))); });
        CHECK(ready.in_place);
        CHECK(ready.completion.get().value == 3);
    }
    SECTION("a Task built ready") {
        auto posted = complete([](auto cb) { relais::async(Task<int32_t>::fromValue(4), std::move(cb)); });
        CHECK_FALSE(posted.in_place);
        CHECK(posted.completion.get().value == 4);
        auto in_place =
            complete([&](auto cb) { relais::async(Task<int32_t>::fromValue(4), bound(std::move(cb))); });
        CHECK(in_place.in_place);
        CHECK(in_place.completion.get().value == 4);
    }
    SECTION("a Task that ends without suspending") {
        auto posted = complete([](auto cb) { relais::async(computed(5), std::move(cb)); });
        CHECK_FALSE(posted.in_place);
        CHECK(posted.completion.get().value == 5);
        auto in_place = complete([&](auto cb) { relais::async(computed(5), bound(std::move(cb))); });
        CHECK(in_place.in_place);
        CHECK(in_place.completion.get().value == 5);
    }
    SECTION("a request starts in place, and completes later") {
        bool started = false;
        auto later = complete([&started](auto cb) {
            relais::async(startingThenDoubled(started, 8), std::move(cb));
        });
        CHECK(started);
        CHECK_FALSE(later.in_place);
        CHECK(later.completion.get().value == 16);
    }

    // An immediate executor that nobody runs until the test does.
    net::io_context elsewhere;
    auto onElsewhere = [&elsewhere](std::promise<Completion<int32_t>>& seen) {
        return net::bind_immediate_executor(elsewhere.get_executor(),
            [&seen](std::exception_ptr e, int32_t v) {
                seen.set_value({e, v, std::this_thread::get_id()});
            });
    };
    SECTION("a hit goes to the immediate executor bound, wherever it runs") {
        std::promise<Completion<int32_t>> seen;
        loop.call([&] { relais::async(Immediate<int32_t>(6), onElsewhere(seen)); });
        auto completion = seen.get_future();
        CHECK(completion.wait_for(50ms) == std::future_status::timeout);
        elsewhere.run();
        REQUIRE(completion.wait_for(0s) == std::future_status::ready);
        auto c = completion.get();
        CHECK(c.value == 6);
        CHECK(c.thread == std::this_thread::get_id());
    }
    SECTION("an answer that comes later never goes to the immediate executor") {
        std::promise<Completion<int32_t>> seen;
        loop.call([&] { relais::async(doubled(10), onElsewhere(seen)); });
        auto completion = seen.get_future();
        REQUIRE(completion.wait_for(5s) == std::future_status::ready);
        auto c = completion.get();
        CHECK(c.value == 20);
        CHECK(c.thread == loop.thread());
    }
}

namespace {

// Where the stack is when called: the frame's address, which AddressSanitizer
// does not move to a heap of its own, as it may a local variable.
[[gnu::noinline]] std::uintptr_t stackHere() {
    return reinterpret_cast<std::uintptr_t>(__builtin_frame_address(0));
}

// How much deeper the stack is after `calls` calls ready at once, awaited in a
// row through `token`, than after the first.
template<typename Token>
net::awaitable<std::intptr_t> stackGrowth(int calls, Token token) {
    std::uintptr_t first = 0;
    std::uintptr_t last = 0;
    for (int i = 0; i < calls; ++i) {
        (void)co_await relais::async(Immediate<int32_t>(1), token);
        last = stackHere();
        if (i == 0) first = last;
    }
    co_return static_cast<std::intptr_t>(first - last);
}

net::awaitable<std::intptr_t> stackGrowthInPlace(int calls) {
    const auto ex = co_await net::this_coro::executor;
    co_return co_await stackGrowth(calls, net::bind_immediate_executor(ex, net::use_awaitable));
}

}  // namespace

TEST_CASE("asio async: calls ready at once, awaited in a row, do not deepen the stack",
          "[asio][async]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;
    CHECK(loop.run(stackGrowth(1000, net::use_awaitable)) < 4096);
    CHECK(loop.run(stackGrowth(1000, net::deferred)) < 4096);
    // In place, each resumes the coroutine one level deeper (about 2 KiB at
    // -O2): what asio does for any operation that completes there.
    CHECK(loop.run(stackGrowthInPlace(100)) > 32 * 1024);
}

// ---------------------------------------------------------------------------

namespace {

struct Failure {
    bool pg_error = false;
    std::string what;
};

net::awaitable<Failure> awaitedFailure() {
    Failure seen;
    try {
        (void)co_await relais::async(divisionByZero(), net::use_awaitable);
    } catch (const PgError& e) {
        seen.pg_error = true;
        seen.what = e.what();
    }
    co_return seen;
}

net::awaitable<bool> awaitedVoidFailure() {
    try {
        co_await relais::async(failing(), net::use_awaitable);
    } catch (const PgError&) {
        co_return true;
    }
    co_return false;
}

}  // namespace

TEST_CASE("asio async: the exception a request ends with reaches the handler", "[asio][async]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;

    SECTION("use_awaitable rethrows it") {
        CHECK(loop.run(awaitedFailure()).pg_error);
        CHECK(loop.run(awaitedVoidFailure()));
    }
    SECTION("a callback gets it, with a value-initialized value") {
        std::promise<Completion<int32_t>> seen;
        loop.call([&] {
            relais::async(divisionByZero(), [&](std::exception_ptr e, int32_t v) {
                seen.set_value({e, v, std::this_thread::get_id()});
            });
        });
        auto c = seen.get_future().get();
        CHECK(holds<PgError>(c.error));
        CHECK(c.value == 0);
    }
    SECTION("use_future rethrows it") {
        auto future = loop.call([] { return relais::async(divisionByZero(), net::use_future); });
        CHECK_THROWS_AS(future.get(), PgError);
    }
    // relais goes on.
    CHECK(loop.run(awaitedThroughUseAwaitable(2)) == 4);
}

// ---------------------------------------------------------------------------

namespace {

using namespace net::experimental::awaitable_operators;

struct Race {
    std::size_t winner = 0;
    std::chrono::milliseconds took{};
};

// A request of 500 ms against a timer of 20 ms.
net::awaitable<Race> raceAgainstTimer(std::shared_ptr<std::atomic<bool>> ran) {
    net::steady_timer timer(co_await net::this_coro::executor, 20ms);
    const auto t0 = Clock::now();
    auto result = co_await (relais::async(slowly(0.5, 1, ran), net::use_awaitable) ||
                            timer.async_wait(net::use_awaitable));
    co_return Race{result.index(),
                   std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0)};
}

// A cancellation of `type`, emitted while a request of 300 ms runs.
struct Cancelled {
    std::shared_ptr<std::atomic<bool>> ran = std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<net::cancellation_signal> signal = std::make_shared<net::cancellation_signal>();
    std::shared_ptr<std::atomic<int>> calls = std::make_shared<std::atomic<int>>(0);
    std::promise<Completion<int32_t>> seen;
    bool completed_inside_emit = false;
};

void startThenCancel(AttachedLoop& loop, Cancelled& c, net::cancellation_type type) {
    loop.call([&] {
        relais::async(slowly(0.3, 9, c.ran),
                      net::bind_cancellation_slot(c.signal->slot(),
                          [&c](std::exception_ptr e, int32_t v) {
                              if (c.calls->fetch_add(1) == 0)
                                  c.seen.set_value({e, v, std::this_thread::get_id()});
                          }));
        c.signal->emit(type);
        c.completed_inside_emit = c.calls->load() != 0;
        c.signal->emit(type);  // a second time
    });
}

}  // namespace

TEST_CASE("asio async: a cancelled operation completes at once, and its request runs on",
          "[asio][async][integration]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;

    SECTION("awaitable operators: the timer wins without waiting for the request") {
        auto ran = std::make_shared<std::atomic<bool>>(false);
        auto race = loop.run(raceAgainstTimer(ran));
        CHECK(race.winner == 1);
        CHECK(race.took < 300ms);
        CHECK_FALSE(ran->load());
        CHECK(eventually([&] { return ran->load(); }));
    }
    SECTION("terminal and partial cancellations abandon the operation") {
        auto type = GENERATE(net::cancellation_type::terminal, net::cancellation_type::partial,
                             net::cancellation_type::all);
        Cancelled c;
        const auto t0 = Clock::now();
        startThenCancel(loop, c, type);
        auto completion = c.seen.get_future().get();
        CHECK(Clock::now() - t0 < 200ms);
        CHECK_FALSE(c.completed_inside_emit);
        CHECK(aborted(completion.error));
        CHECK(completion.value == 0);
        CHECK(completion.thread == loop.thread());
        CHECK_FALSE(c.ran->load());
        // The request runs on; its result goes nowhere.
        CHECK(eventually([&] { return c.ran->load(); }));
        loop.call([] {});
        CHECK(c.calls->load() == 1);
    }
    SECTION("a total cancellation is not honoured: the request may still have its effects") {
        Cancelled c;
        startThenCancel(loop, c, net::cancellation_type::total);
        auto completion = c.seen.get_future().get();
        CHECK_FALSE(completion.error);
        CHECK(completion.value == 9);
        CHECK(c.ran->load());
        // The slot was emptied when the operation completed.
        loop.call([&] { c.signal->emit(net::cancellation_type::all); });
        CHECK(c.calls->load() == 1);
    }
    // relais goes on, and the next request has a connection.
    CHECK(loop.run(awaitedThroughUseAwaitable(3)) == 6);
}

// ---------------------------------------------------------------------------

TEST_CASE("asio async: off a thread relais is attached to, relais::async throws",
          "[asio][async]") {
    INFO(relais_test::kAsioFlavor);
    using Catch::Matchers::ContainsSubstring;
    AttachedLoop loop;

    SECTION("a thread of its own") {
        CHECK_THROWS_WITH(relais::async(doubled(1), net::use_future),
                          ContainsSubstring("not attached"));
    }
    SECTION("the thread of an io_context relais is not attached to") {
        net::io_context other;
        std::exception_ptr thrown;
        net::post(other, [&] {
            try {
                (void)relais::async(Immediate<int32_t>(1), net::use_future);
            } catch (...) {
                thrown = std::current_exception();
            }
        });
        other.run();
        CHECK(holds<std::logic_error>(thrown));
    }
    SECTION("the loop's thread, once relais is detached") {
        loop.detach();
        CHECK_THROWS_AS(loop.call([] { (void)relais::async(doubled(1), net::use_future); }),
                        std::logic_error);
    }
}

// ---------------------------------------------------------------------------

TEST_CASE("asio async: an exception the handler throws leaves run(), and relais whole",
          "[asio][async]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;
    struct HandlerFailure : std::runtime_error {
        using std::runtime_error::runtime_error;
    };

    SECTION("after a request") {
        loop.call([] {
            relais::async(doubled(1), [](std::exception_ptr, int32_t) {
                throw HandlerFailure("after a request");
            });
        });
    }
    SECTION("a call ready at once") {
        loop.call([] {
            relais::async(Immediate<int32_t>(1), [](std::exception_ptr, int32_t) {
                throw HandlerFailure("ready at once");
            });
        });
    }
    REQUIRE(eventually([&] { return !loop.escaped().empty(); }));
    CHECK(holds<HandlerFailure>(loop.escaped().front()));
    CHECK(loop.run(awaitedThroughUseAwaitable(5)) == 10);
}

// ---------------------------------------------------------------------------

TEST_CASE("asio async: the handler's own executor is kept busy until it completes",
          "[asio][async][integration]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;
    net::io_context other;  // run below, by this thread, with no work of its own
    std::optional<Completion<int32_t>> seen;
    loop.call([&] {
        relais::async(slowly(0.2, 11, std::make_shared<std::atomic<bool>>(false)),
                      net::bind_executor(other, [&](std::exception_ptr e, int32_t v) {
                          seen = Completion<int32_t>{e, v, std::this_thread::get_id()};
                      }));
    });
    other.run();  // returns once the handler has run
    REQUIRE(seen);
    CHECK_FALSE(seen->error);
    CHECK(seen->value == 11);
    CHECK(seen->thread == std::this_thread::get_id());
}

// ---------------------------------------------------------------------------

namespace {

// The allocations of this thread that `n` awaits of `op(i)` cost, warm.
template<typename Op>
net::awaitable<uint64_t> allocationsOf(int n, Op op) {
    for (int i = 0; i < 64; ++i) co_await op(i);
    const auto before = relais_test::threadAllocations().count;
    for (int i = 0; i < n; ++i) co_await op(i);
    co_return relais_test::threadAllocations().count - before;
}

struct SteadyAllocations {
    uint64_t posted_hits = 0;
    uint64_t bound_hits = 0;
    uint64_t deferred_hits = 0;
    uint64_t in_place_callbacks = 0;
    uint64_t ready_tasks = 0;
    uint64_t requests = 0;
    uint64_t deferred_requests = 0;
    uint64_t requests_in_one_task = 0;
};

net::awaitable<SteadyAllocations> steadyAllocations(int n) {
    const auto ex = co_await net::this_coro::executor;
    SteadyAllocations seen;
    seen.posted_hits = co_await allocationsOf(n, [](int) -> net::awaitable<void> {
        (void)co_await relais::async(Immediate<int32_t>(1), net::use_awaitable);
    });
    seen.bound_hits = co_await allocationsOf(n, [ex](int) -> net::awaitable<void> {
        (void)co_await relais::async(Immediate<int32_t>(1),
                                     net::bind_immediate_executor(ex, net::use_awaitable));
    });
    seen.deferred_hits = co_await allocationsOf(n, [](int) -> net::awaitable<void> {
        (void)co_await relais::async(Immediate<int32_t>(1), net::deferred);
    });
    seen.in_place_callbacks = co_await allocationsOf(n, [ex](int) -> net::awaitable<void> {
        relais::async(Immediate<int32_t>(1),
                      net::bind_immediate_executor(ex, [](std::exception_ptr, int32_t) {}));
        co_return;
    });
    seen.ready_tasks = co_await allocationsOf(n, [](int) -> net::awaitable<void> {
        (void)co_await relais::async(computed(1), net::use_awaitable);
    });
    seen.requests = co_await allocationsOf(n, [](int i) -> net::awaitable<void> {
        (void)co_await relais::async(doubled(i), net::use_awaitable);
    });
    seen.deferred_requests = co_await allocationsOf(n, [](int i) -> net::awaitable<void> {
        (void)co_await relais::async(doubled(i), net::deferred);
    });
    (void)co_await relais::async(manyDoubled(64), net::use_awaitable);
    const auto before = relais_test::threadAllocations().count;
    (void)co_await relais::async(manyDoubled(n), net::use_awaitable);
    seen.requests_in_one_task = relais_test::threadAllocations().count - before;
    co_return seen;
}

}  // namespace

TEST_CASE("asio async: a steady loop allocates nothing for relais::async",
          "[asio][async][integration]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;
    constexpr int n = 500;
    auto seen = loop.run(steadyAllocations(n));
    CHECK(seen.posted_hits == 0);
    CHECK(seen.bound_hits == 0);
    CHECK(seen.deferred_hits == 0);
    CHECK(seen.in_place_callbacks == 0);
    CHECK(seen.ready_tasks == 0);
    // A request costs what relais allocates for it, and no more: one operation
    // per request against one for all of them.
    INFO("per request: " << double(seen.requests) / n << ", in one task: "
                         << double(seen.requests_in_one_task) / n);
    CHECK(seen.requests <= seen.requests_in_one_task + 2);
    CHECK(seen.deferred_requests <= seen.requests_in_one_task + 2);
}

// ---------------------------------------------------------------------------

TEST_CASE("asio async: detach waits for an operation in flight", "[asio][async][integration]") {
    INFO(relais_test::kAsioFlavor);
    AttachedLoop loop;
    std::atomic<bool> completed{false};
    std::atomic<int32_t> value{0};
    loop.call([&] {
        relais::async(slowly(0.2, 13, std::make_shared<std::atomic<bool>>(false)),
                      [&](std::exception_ptr e, int32_t v) {
                          value = e ? -1 : v;
                          completed = true;
                      });
    });
    loop.detach();
    CHECK(completed);
    CHECK(value == 13);
}
