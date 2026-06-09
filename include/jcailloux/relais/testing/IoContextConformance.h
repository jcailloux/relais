#ifndef JCX_RELAIS_TESTING_IO_CONTEXT_CONFORMANCE_H
#define JCX_RELAIS_TESTING_IO_CONTEXT_CONFORMANCE_H

#include "jcailloux/relais/io/IoContext.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/eventfd.h>
#include <unistd.h>

// =============================================================================
// IoContext conformance harness — the executable contract for the `IoContext`
// extension point.
//
// The `IoContext` concept (io/IoContext.h) constrains only method *signatures*.
// The semantic contract an adapter must honor — post() runs the callback once,
// on the loop thread, and wakes a blocked loop; watches fire with the right
// event mask on the loop thread; updateWatch/removeWatch take effect — cannot be
// expressed by a concept. This harness encodes it as runnable checks.
//
// Who uses it:
//   - relais CI runs it against EpollIoContext (test_io_context_conformance).
//   - An adapter author (e.g. a trantor::EventLoop shim for Drogon) instantiates
//     it against their type to prove the shim is correct before wiring relais
//     pools onto it. The harness is Catch-free (throws ConformanceError) so it
//     drops into any test framework.
//
// What the author must supply, besides the IoContext instance:
//   - `drive(io, pred)` — pump the loop ON THE CALLING THREAD until pred()
//     returns true. The loop's internal wait MUST be bounded (return to re-check
//     pred periodically) so the cross-thread check cannot hang. For
//     EpollIoContext: `[](auto& io, auto p){ io.runUntil(p); }`.
//
// Usage:
//   EpollIoContext io;
//   IoContextConformance::runAll(io,
//       [](auto& c, auto pred){ c.runUntil(pred); });
// =============================================================================

namespace jcailloux::relais::testing {

using io::hasEvent;
using io::IoEvent;

struct ConformanceError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

namespace detail {

inline void require(bool cond, const std::string& what) {
    if (!cond) throw ConformanceError(what);
}

// A self-resetting eventfd usable as a watchable fd: write() makes it readable,
// read() clears it. Almost always writable, so it also exercises Write masks.
struct EventFd {
    int fd = -1;
    EventFd() {
        fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        require(fd >= 0, "eventfd creation failed (harness setup)");
    }
    ~EventFd() { if (fd >= 0) ::close(fd); }
    EventFd(const EventFd&) = delete;
    EventFd& operator=(const EventFd&) = delete;

    void signal() {
        uint64_t one = 1;
        [[maybe_unused]] auto _ = ::write(fd, &one, sizeof(one));
    }
    void clear() {
        uint64_t v;
        [[maybe_unused]] auto _ = ::read(fd, &v, sizeof(v));
    }
};

// Forces `turns` full loop iterations by chaining one post per turn (a callback
// posted from within drainPosted runs on the *next* turn). After N turns, any
// ready fd will have been serviced — lets negative checks ("must NOT fire") be
// asserted without sleeping.
template<io::IoContext Io, typename Drive>
void pump(Io& io, Drive&& drive, int turns) {
    std::atomic<int> remaining{turns};
    std::function<void()> step = [&] {
        if (remaining.fetch_sub(1) > 1) io.post(step);
    };
    io.post(step);
    drive(io, [&] { return remaining.load() <= 0; });
}

}  // namespace detail

struct IoContextConformance {
    // -- post() -------------------------------------------------------------

    // C1: a posted callback runs exactly once.
    template<io::IoContext Io, typename Drive>
    static void checkPostExecutes(Io& io, Drive drive) {
        int calls = 0;
        io.post([&] { ++calls; });
        drive(io, [&] { return calls > 0; });
        detail::pump(io, drive, 2);  // give a duplicate a chance to show up
        detail::require(calls == 1,
            "post(): callback must run exactly once, observed " + std::to_string(calls));
    }

    // C2: callbacks run in the order they were posted (FIFO).
    template<io::IoContext Io, typename Drive>
    static void checkPostOrdering(Io& io, Drive drive) {
        std::vector<int> order;
        for (int i = 0; i < 8; ++i) io.post([&, i] { order.push_back(i); });
        drive(io, [&] { return order.size() >= 8; });
        for (int i = 0; i < 8; ++i)
            detail::require(order[i] == i,
                "post(): callbacks must run FIFO; out of order at index " + std::to_string(i));
    }

    // C3: a callback posted from inside a callback runs on a later turn.
    template<io::IoContext Io, typename Drive>
    static void checkPostFromCallback(Io& io, Drive drive) {
        bool inner = false;
        io.post([&] { io.post([&] { inner = true; }); });
        drive(io, [&] { return inner; });
        detail::require(inner, "post(): re-posting from within a callback must be honored");
    }

    // -- watches ------------------------------------------------------------

    // C4: addWatch(Read) fires when the fd becomes readable, with the Read bit set.
    template<io::IoContext Io, typename Drive>
    static void checkWatchReadable(Io& io, Drive drive) {
        detail::EventFd efd;
        bool fired = false;
        IoEvent seen = IoEvent::None;
        auto h = io.addWatch(efd.fd, IoEvent::Read, [&](IoEvent ev) {
            fired = true;
            seen = ev;
        });

        detail::pump(io, drive, 3);
        detail::require(!fired, "addWatch(Read): must not fire while fd is not readable");

        efd.signal();
        drive(io, [&] { return fired; });
        detail::require(hasEvent(seen, IoEvent::Read),
            "addWatch(Read): callback must report the Read event");

        io.removeWatch(h);
        efd.clear();
    }

    // C5: updateWatch changes the active mask on the SAME handle (no re-add).
    template<io::IoContext Io, typename Drive>
    static void checkUpdateWatch(Io& io, Drive drive) {
        detail::EventFd efd;
        int reads = 0, writes = 0;
        // Watch Read only on a non-readable fd → nothing fires.
        auto h = io.addWatch(efd.fd, IoEvent::Read, [&](IoEvent ev) {
            if (hasEvent(ev, IoEvent::Read))  ++reads;
            if (hasEvent(ev, IoEvent::Write)) ++writes;
        });
        detail::pump(io, drive, 3);
        detail::require(reads == 0 && writes == 0,
            "addWatch(Read) on a non-readable fd must stay silent");

        // Flip the same handle to Write: an eventfd is writable, so it must
        // start firing — proving updateWatch took effect.
        io.updateWatch(h, IoEvent::Write);
        drive(io, [&] { return writes > 0; });
        detail::require(writes > 0, "updateWatch(Write): Write events must be delivered");
        detail::require(reads == 0,
            "updateWatch: Read must not fire after the mask drops it");

        io.removeWatch(h);
    }

    // C6: after removeWatch, the callback no longer fires.
    template<io::IoContext Io, typename Drive>
    static void checkRemoveWatch(Io& io, Drive drive) {
        detail::EventFd efd;
        int fired = 0;
        auto h = io.addWatch(efd.fd, IoEvent::Read, [&](IoEvent) { ++fired; });
        efd.signal();
        drive(io, [&] { return fired > 0; });
        efd.clear();

        io.removeWatch(h);
        efd.signal();
        detail::pump(io, drive, 3);
        detail::require(fired == 1,
            "removeWatch(): callback must not fire after removal, observed "
            + std::to_string(fired) + " calls");
        efd.clear();
    }

    // -- cross-thread -------------------------------------------------------

    // C7: post() from another thread runs the callback ON the loop thread and
    // promptly wakes a blocked loop. This is the property cross-loop bridges
    // (and most hand-rolled adapters) get wrong.
    template<io::IoContext Io, typename Drive>
    static void checkCrossThreadPost(Io& io, Drive drive) {
        std::atomic<bool> stop{false};
        std::atomic<bool> fired{false};
        std::atomic<bool> on_loop_thread{false};

        std::thread loop([&] { drive(io, [&] { return stop.load(); }); });

        auto loop_id = loop.get_id();
        io.post([&, loop_id] {
            on_loop_thread = (std::this_thread::get_id() == loop_id);
            fired = true;
        });

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!fired.load() && std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();

        bool ok_fired = fired.load();
        bool ok_thread = on_loop_thread.load();

        stop = true;
        io.post([] {});  // wake the loop so it observes `stop`
        loop.join();

        detail::require(ok_fired,
            "post() from another thread: callback never ran (loop not woken?)");
        detail::require(ok_thread,
            "post() from another thread: callback must run on the loop thread");
    }

    // -- timers -------------------------------------------------------------

    // C8: postDelayed(delay, cb) runs cb exactly once, after the delay. (The
    // BatchScheduler uses this to flush a batch on an adaptive deadline.)
    template<io::IoContext Io, typename Drive>
    static void checkPostDelayedFires(Io& io, Drive drive) {
        int calls = 0;
        io.postDelayed(std::chrono::milliseconds(1), [&] { ++calls; });
        drive(io, [&] { return calls > 0; });
        detail::pump(io, drive, 2);  // a duplicate would surface here
        detail::require(calls == 1,
            "postDelayed(): timer must fire exactly once, observed "
            + std::to_string(calls));
    }

    // C9: cancelTimer(token) stops a pending timer. A later, uncancelled "fence"
    // timer bounds the wait: once it fires, the cancelled timer's earlier
    // deadline has passed, so any firing means cancel failed.
    template<io::IoContext Io, typename Drive>
    static void checkCancelTimer(Io& io, Drive drive) {
        int cancelled = 0;
        bool fence = false;
        auto token = io.postDelayed(std::chrono::milliseconds(1), [&] { ++cancelled; });
        io.cancelTimer(token);
        io.postDelayed(std::chrono::milliseconds(3), [&] { fence = true; });
        drive(io, [&] { return fence; });
        detail::require(cancelled == 0,
            "cancelTimer(): a cancelled timer must not fire, observed "
            + std::to_string(cancelled));
    }

    // -- aggregate ----------------------------------------------------------

    template<io::IoContext Io, typename Drive>
    static void runAll(Io& io, Drive drive) {
        checkPostExecutes(io, drive);
        checkPostOrdering(io, drive);
        checkPostFromCallback(io, drive);
        checkWatchReadable(io, drive);
        checkUpdateWatch(io, drive);
        checkRemoveWatch(io, drive);
        checkCrossThreadPost(io, drive);
        checkPostDelayedFires(io, drive);
        checkCancelTimer(io, drive);
    }
};

}  // namespace jcailloux::relais::testing

#endif  // JCX_RELAIS_TESTING_IO_CONTEXT_CONFORMANCE_H