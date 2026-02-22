#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/batch/TimingEstimator.h>
#include <jcailloux/relais/io/pg/PgParams.h>

#include <coroutine>
#include <deque>
#include <utility>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::batch;

TEST_CASE("TimingEstimator: bootstrap state", "[io][batch]") {
    TimingEstimator est;

    REQUIRE(est.isPgBootstrapping());
    REQUIRE(est.isRedisBootstrapping());
    REQUIRE(est.isPgStale());
    REQUIRE(est.isRedisStale());
}

TEST_CASE("TimingEstimator: PG network time updates", "[io][batch]") {
    TimingEstimator est;

    // First measurement: direct assignment
    est.updatePgNetworkTime(100'000.0, 0.0);
    REQUIRE(est.pg_network_time_ns > 0.0);
    REQUIRE(est.pg_bootstrap_count == 1);
    REQUIRE_FALSE(est.isPgStale());

    // Subsequent measurements: EMA
    double prev = est.pg_network_time_ns;
    est.updatePgNetworkTime(200'000.0, 0.0);
    REQUIRE(est.pg_network_time_ns > prev);
    REQUIRE(est.pg_bootstrap_count == 2);
}

TEST_CASE("TimingEstimator: Redis network time updates", "[io][batch]") {
    TimingEstimator est;

    est.updateRedisNetworkTime(50'000.0);
    REQUIRE(est.redis_network_time_ns > 0.0);
    REQUIRE(est.redis_bootstrap_count == 1);
    REQUIRE_FALSE(est.isRedisStale());
}

TEST_CASE("TimingEstimator: SQL timing per key", "[io][batch]") {
    TimingEstimator est;
    est.pg_network_time_ns = 100'000.0;

    static constexpr const char* sql = "SELECT * FROM t WHERE id = ANY($1)";
    est.updateSqlTimingPerKey(sql, 10, 200'000.0);

    double rt = est.getRequestTime(sql);
    REQUIRE(rt > 0.0);

    // Second update: EMA should converge toward different value
    est.updateSqlTimingPerKey(sql, 5, 600'000.0);
    double rt2 = est.getRequestTime(sql);
    REQUIRE(rt2 > 0.0);
    // With alpha=0.1 and a much larger input, the value should shift
    REQUIRE(rt2 > rt);
}

TEST_CASE("TimingEstimator: bootstrap exits after threshold", "[io][batch]") {
    TimingEstimator est;

    for (int i = 0; i < TimingEstimator::kBootstrapThreshold; ++i) {
        REQUIRE(est.isPgBootstrapping());
        est.updatePgNetworkTime(100'000.0, 0.0);
    }

    REQUIRE_FALSE(est.isPgBootstrapping());
}

TEST_CASE("TimingEstimator: merge constraint (5x factor)", "[io][batch]") {
    TimingEstimator est;

    REQUIRE(est.canMergePg(100.0, 100.0));     // Same
    REQUIRE(est.canMergePg(100.0, 500.0));     // 5x exactly — OK
    REQUIRE_FALSE(est.canMergePg(100.0, 501.0)); // > 5x — no merge
    REQUIRE(est.canMergePg(0.0, 100.0));        // Zero → allow merge
}

// =============================================================================
// ConcurrencyGate unit tests
//
// ConcurrencyGate is private inside BatchScheduler<Io>. We use a standalone
// copy here to unit-test the coroutine semaphore logic in isolation — notably
// the double-increment bug where release() + await_resume() both incremented
// inflight, poisoning the counter after waiter cycles.
// =============================================================================

namespace {

struct TestGate {
    int max_concurrent;
    int inflight = 0;
    std::deque<std::coroutine_handle<>> waiters;

    struct Awaiter {
        TestGate* gate;
        bool await_ready() const noexcept {
            return gate->inflight < gate->max_concurrent;
        }
        void await_suspend(std::coroutine_handle<> h) {
            gate->waiters.push_back(h);
        }
        void await_resume() noexcept {
            ++gate->inflight;
        }
    };

    Awaiter acquire() { return {this}; }

    void release() {
        --inflight;
        if (!waiters.empty()) {
            auto next = waiters.front();
            waiters.pop_front();
            next.resume();
        }
    }
};

// Eager coroutine: starts immediately, suspends at final_suspend.
struct EagerCoro {
    struct promise_type {
        EagerCoro get_return_object() {
            return EagerCoro{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept {}
    };

    std::coroutine_handle<promise_type> handle;

    EagerCoro() noexcept : handle(nullptr) {}
    explicit EagerCoro(std::coroutine_handle<promise_type> h) noexcept : handle(h) {}
    ~EagerCoro() { if (handle) handle.destroy(); }

    EagerCoro(EagerCoro&& o) noexcept : handle(std::exchange(o.handle, nullptr)) {}
    EagerCoro& operator=(EagerCoro&& o) noexcept {
        if (this != &o) { if (handle) handle.destroy(); handle = std::exchange(o.handle, nullptr); }
        return *this;
    }
    EagerCoro(const EagerCoro&) = delete;
    EagerCoro& operator=(const EagerCoro&) = delete;
};

} // anonymous namespace

TEST_CASE("ConcurrencyGate: acquire increments inflight", "[io][batch]") {
    TestGate gate{2};
    REQUIRE(gate.inflight == 0);

    auto a1 = gate.acquire();
    REQUIRE(a1.await_ready());  // 0 < 2
    a1.await_resume();
    REQUIRE(gate.inflight == 1);

    auto a2 = gate.acquire();
    REQUIRE(a2.await_ready());  // 1 < 2
    a2.await_resume();
    REQUIRE(gate.inflight == 2);

    // Third acquire should block
    auto a3 = gate.acquire();
    REQUIRE_FALSE(a3.await_ready());  // 2 >= 2
}

TEST_CASE("ConcurrencyGate: release decrements inflight", "[io][batch]") {
    TestGate gate{2};

    auto a = gate.acquire();
    a.await_resume();
    REQUIRE(gate.inflight == 1);

    gate.release();
    REQUIRE(gate.inflight == 0);
}

TEST_CASE("ConcurrencyGate: release transfers slot without double-increment", "[io][batch]") {
    TestGate gate{1};

    // Acquire the single slot
    auto a = gate.acquire();
    a.await_resume(); // inflight = 1

    // Create a waiter coroutine — it blocks at co_await gate.acquire()
    int waiter_state = 0;
    auto waiter = [](TestGate& g, int& state) -> EagerCoro {
        co_await g.acquire(); // suspends: 1 >= 1
        state = 1;
    }(gate, waiter_state);

    REQUIRE(waiter_state == 0);
    REQUIRE(gate.waiters.size() == 1);
    REQUIRE(gate.inflight == 1);

    // Release the slot → should transfer to waiter
    gate.release();

    // Waiter resumed: await_resume did ++inflight
    REQUIRE(waiter_state == 1);
    REQUIRE(gate.inflight == 1); // Transfer: 1→0 (release) → 0→1 (await_resume)
    // BUG REGRESSION: with double-increment in release(), inflight would be 2

    gate.release();
    REQUIRE(gate.inflight == 0);
}

TEST_CASE("ConcurrencyGate: chained releases maintain invariant", "[io][batch]") {
    TestGate gate{1};
    constexpr int N = 50;
    int acquired_count = 0;

    // Acquire initial slot
    auto a = gate.acquire();
    a.await_resume(); // inflight = 1

    // Create N waiters that each acquire and immediately release
    std::vector<EagerCoro> waiters;
    waiters.reserve(N);
    for (int i = 0; i < N; ++i) {
        waiters.push_back([](TestGate& g, int& count) -> EagerCoro {
            co_await g.acquire();
            ++count;
            g.release();
        }(gate, acquired_count));
    }

    REQUIRE(gate.waiters.size() == static_cast<size_t>(N));
    REQUIRE(acquired_count == 0);

    // Release initial slot → chain reaction: w0 acquire+release → w1 → ... → wN-1
    gate.release();

    REQUIRE(acquired_count == N);
    REQUIRE(gate.inflight == 0);
    REQUIRE(gate.waiters.empty());
    // BUG REGRESSION: with double-increment, inflight would be N after chain
}

// =============================================================================
// PgParams equality — used for write coalescing deduplication
// =============================================================================

TEST_CASE("PgParams: equality of identical params", "[io][batch][coalesce]") {
    auto a = PgParams::make(42, "hello");
    auto b = PgParams::make(42, "hello");
    REQUIRE(a == b);
}

TEST_CASE("PgParams: inequality on different values", "[io][batch][coalesce]") {
    auto a = PgParams::make(42, "hello");
    auto b = PgParams::make(42, "world");
    REQUIRE_FALSE(a == b);
}

TEST_CASE("PgParams: inequality on different count", "[io][batch][coalesce]") {
    auto a = PgParams::make(42);
    auto b = PgParams::make(42, "extra");
    REQUIRE_FALSE(a == b);
}

TEST_CASE("PgParams: equality with null params", "[io][batch][coalesce]") {
    PgParams a;
    a.params.push_back(PgParam::null());
    a.params.push_back(PgParam::text("ok"));

    PgParams b;
    b.params.push_back(PgParam::null());
    b.params.push_back(PgParam::text("ok"));

    REQUIRE(a == b);
}

TEST_CASE("PgParams: null vs non-null inequality", "[io][batch][coalesce]") {
    PgParams a;
    a.params.push_back(PgParam::null());

    PgParams b;
    b.params.push_back(PgParam::text(""));

    REQUIRE_FALSE(a == b);
}

TEST_CASE("PgParams: empty params are equal", "[io][batch][coalesce]") {
    PgParams a;
    PgParams b;
    REQUIRE(a == b);
}

TEST_CASE("PgParams: composite key params equality", "[io][batch][coalesce]") {
    auto a = PgParams::fromKey(std::tuple{1L, std::string("partition_a")});
    auto b = PgParams::fromKey(std::tuple{1L, std::string("partition_a")});
    REQUIRE(a == b);

    auto c = PgParams::fromKey(std::tuple{1L, std::string("partition_b")});
    REQUIRE_FALSE(a == c);
}
