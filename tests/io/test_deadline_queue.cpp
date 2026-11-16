// The deadline queues that bound relais's I/O waits: a counting loop, no
// PostgreSQL. Run it under ASan too: a deadline is a node linked into a queue,
// and the tests below destroy one or the other while linked.

#include <catch2/catch_test_macros.hpp>

#include <jcailloux/relais/io/DeadlineQueue.h>

#include <fixtures/ControllableIoContext.h>

#include <chrono>
#include <vector>

using namespace jcailloux::relais::io;
using namespace jcailloux::relais::io::detail;
using namespace jcailloux::relais::io::test;
using namespace std::chrono_literals;

using Io = ControllableIoContext;
using Queue = DeadlineQueue<Io>;
using Clock = std::chrono::steady_clock;

namespace {

// A bounded operation that records when its deadline expired.
struct Probe {
    std::vector<int>* order = nullptr;
    int id = 0;
    Clock::time_point armed_at{};
    Clock::time_point expired_at{};
    bool expired = false;
    Deadline deadline{this, &Probe::onExpire};

    static void onExpire(void* p) {
        auto* self = static_cast<Probe*>(p);
        self->expired = true;
        self->expired_at = Clock::now();
        if (self->order) self->order->push_back(self->id);
    }

    void arm(Queue& q) {
        armed_at = Clock::now();
        q.arm(deadline);
    }
};

void pumpFor(Io& io, std::chrono::milliseconds window) {
    const auto end = Clock::now() + window;
    while (Clock::now() < end) io.runOnce(5);
}

template<typename Pred>
void pumpUntil(Io& io, Pred pred, std::chrono::milliseconds budget = 2s) {
    const auto end = Clock::now() + budget;
    while (!pred() && Clock::now() < end) io.runOnce(5);
}

}  // namespace

TEST_CASE("DeadlineQueue: a zero duration bounds nothing", "[io][deadline]") {
    Io io;
    CHECK(Queue::make(io, 0ns) == nullptr);
    CHECK(Queue::make(io, -1ms) == nullptr);
}

TEST_CASE("DeadlineQueue: deadlines expire in arming order, never early", "[io][deadline]") {
    Io io;
    auto q = Queue::make(io, 40ms);
    std::vector<int> order;
    Probe a{&order, 1}, b{&order, 2}, c{&order, 3};

    a.arm(*q);
    pumpFor(io, 10ms);
    b.arm(*q);
    pumpFor(io, 10ms);
    c.arm(*q);
    pumpUntil(io, [&] { return c.expired; });

    REQUIRE(order == std::vector<int>{1, 2, 3});
    for (const Probe* p : {&a, &b, &c}) {
        CHECK(p->expired_at - p->armed_at >= 40ms);
        CHECK_FALSE(p->deadline.armed());
    }
}

TEST_CASE("DeadlineQueue: operations that end in time arm one loop timer in all",
          "[io][deadline]") {
    Io io;
    auto q = Queue::make(io, 30s);

    for (int i = 0; i < 1000; ++i) {
        Probe p;
        p.arm(*q);
        io.runOnce(0);
        p.deadline.disarm();
    }

    CHECK(io.armedTimers() == 1);
    CHECK(io.cancelledTimers() == 0);
    CHECK(q->timerPending());
    CHECK(q->empty());
}

TEST_CASE("DeadlineQueue: a disarmed head leaves the timer to re-arm for the next deadline",
          "[io][deadline]") {
    Io io;
    auto q = Queue::make(io, 50ms);
    Probe a, b;

    a.arm(*q);
    pumpFor(io, 20ms);
    b.arm(*q);
    a.deadline.disarm();
    pumpUntil(io, [&] { return b.expired; });

    CHECK_FALSE(a.expired);
    REQUIRE(b.expired);
    CHECK(b.expired_at - b.armed_at >= 50ms);
    CHECK(io.armedTimers() == 2);  // for a, then from its expiry for b
}

TEST_CASE("DeadlineQueue: at rest, the queue holds no timer", "[io][deadline]") {
    Io io;
    auto q = Queue::make(io, 20ms);
    Probe a;

    a.arm(*q);
    a.deadline.disarm();
    pumpFor(io, 60ms);

    CHECK_FALSE(a.expired);
    CHECK_FALSE(q->timerPending());
    CHECK(io.pendingTimerCount() == 0);
    CHECK(io.armedTimers() == 1);
}

TEST_CASE("DeadlineQueue: an expiry may arm a deadline of its own queue", "[io][deadline]") {
    struct Chain {
        Queue* q;
        Probe* next;
        Deadline deadline{this, &Chain::onExpire};
        static void onExpire(void* p) {
            auto* self = static_cast<Chain*>(p);
            self->next->arm(*self->q);
        }
    };

    Io io;
    auto q = Queue::make(io, 20ms);
    Probe b;
    Chain a{q.get(), &b};

    q->arm(a.deadline);
    pumpUntil(io, [&] { return b.expired; });

    REQUIRE(b.expired);
    CHECK(b.expired_at - b.armed_at >= 20ms);
}

TEST_CASE("DeadlineQueue: a deadline destroyed while armed unlinks itself", "[io][deadline]") {
    Io io;
    auto q = Queue::make(io, 20ms);
    Probe b;
    {
        Probe a;
        a.arm(*q);
    }
    b.arm(*q);
    pumpUntil(io, [&] { return b.expired; });

    REQUIRE(b.expired);
    CHECK(q->empty());
}

TEST_CASE("DeadlineQueue: a queue destroyed before its timer fires lets its deadlines go",
          "[io][deadline]") {
    Io io;
    Probe a;  // an operation abandoned with its queue
    {
        auto q = Queue::make(io, 10ms);
        a.arm(*q);
    }
    CHECK_FALSE(a.deadline.armed());

    pumpFor(io, 40ms);  // the queue's timer fires and finds nothing
    CHECK_FALSE(a.expired);
    CHECK(io.pendingTimerCount() == 0);
}
