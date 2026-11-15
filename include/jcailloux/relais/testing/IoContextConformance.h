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
#include <sys/socket.h>
#include <unistd.h>

// =============================================================================
// IoContext conformance harness — the executable contract for the `IoContext`
// extension point.
//
// The `IoContext` concept (io/IoContext.h) constrains only method *signatures*.
// The semantic contract an adapter must honor — post() runs the callback once,
// on the loop thread, and wakes a blocked loop; watches fire with the right
// event mask on the loop thread and follow the open file, not the fd number;
// updateWatch takes effect, and removeWatch stops delivery at once, even for the
// event being dispatched — cannot be expressed by a concept. This harness
// encodes it as runnable checks.
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

// Drives the loop until pred() holds or `timeout` elapses. A check whose event a
// faulty adapter never delivers then fails instead of hanging; it relies on the
// bounded wait `drive` already requires. Waiting for the event itself, rather
// than for a number of turns, also holds on a loop that runs callbacks posted
// from a callback within the same turn (trantor does), where pump() spans a
// single turn.
template<io::IoContext Io, typename Drive, typename Pred>
void driveFor(Io& io, Drive&& drive, Pred&& pred, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    drive(io, [&] { return pred() || std::chrono::steady_clock::now() >= deadline; });
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

    // C10: removeWatch() called FROM INSIDE the watch's own callback must be
    // safe (no use-after-free) and stop further delivery. This is not a corner
    // case: relais self-removes a watch from its I/O callback whenever the
    // connection's interest changes (e.g. EOF / error on a PgConnection), so the
    // loop regains control AFTER the callback returns and must not touch any
    // per-watch state the callback just tore down. An adapter that destroys
    // loop-owned watch state — or invokes the callback through storage it then
    // frees — synchronously inside removeWatch corrupts memory here. The fix is
    // to defer teardown: invoke through a copy/stable reference, and free the
    // watch's resources after the current event has been fully processed (e.g.
    // trantor: queueInLoop(delete), epoll: copy the callback out before calling).
    template<io::IoContext Io, typename Drive>
    static void checkRemoveWatchReentrant(Io& io, Drive drive) {
        detail::EventFd efd;
        int fired = 0;
        typename Io::WatchHandle h{};
        h = io.addWatch(efd.fd, IoEvent::Read, [&](IoEvent) {
            io.removeWatch(h);  // self-removal from within the watch callback
            // Touch a captured reference AFTER self-removal: if the adapter
            // invoked us through storage it just freed (the erased watch entry),
            // this read is a use-after-free that ASan flags. A conforming adapter
            // keeps the callback alive for the whole call (copy/deferred free).
            ++fired;
        });
        efd.signal();
        drive(io, [&] { return fired > 0; });
        efd.clear();

        // The watch is gone; renewed readiness must neither refire nor crash.
        efd.signal();
        detail::pump(io, drive, 3);
        detail::require(fired == 1,
            "removeWatch() from within the watch callback: must remove cleanly "
            "and not refire, observed " + std::to_string(fired) + " calls");
        efd.clear();
    }

    // C12: a watch follows the open file, not the fd number. From a watch's own
    // callback, close its fd and open a new one — the kernel hands back the
    // lowest free number, so in practice the same one — then removeWatch the old
    // handle and addWatch the new fd: the new file's events must reach the new
    // watch. A PgConnection runs this exact sequence when libpq replaces its
    // socket mid-connect (TLS or GSS fallback, next host or address). It fails an
    // adapter that lets per-fd state outlive the close: a removal applied after
    // the re-add, or an addWatch turned into a mask update on a registration the
    // kernel already dropped, leaves the new socket unwatched until the connect
    // times out.
    template<io::IoContext Io, typename Drive>
    static void checkRewatchReusedFd(Io& io, Drive drive) {
        detail::EventFd a;
        int b = -1;  // the replacement fd, closed below
        int fired_old = 0, fired_new = 0;
        typename Io::WatchHandle h_old{}, h_new{};
        h_old = io.addWatch(a.fd, IoEvent::Read, [&](IoEvent) {
            if (++fired_old > 1) return;
            ::close(a.fd);
            a.fd = -1;  // closed here, so ~EventFd must not close the number again
            b = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            io.removeWatch(h_old);
            if (b < 0) return;  // reported as a setup failure below
            uint64_t one = 1;
            [[maybe_unused]] auto _ = ::write(b, &one, sizeof(one));
            h_new = io.addWatch(b, IoEvent::Read, [&](IoEvent) {
                ++fired_new;
                io.removeWatch(h_new);
            });
        });
        a.signal();
        detail::driveFor(io, drive, [&] { return fired_new > 0; }, std::chrono::seconds(1));
        detail::pump(io, drive, 3);  // a duplicate would surface here

        if (b >= 0) {
            if (fired_new == 0) io.removeWatch(h_new);
            ::close(b);
        }
        detail::require(fired_old == 0 || b >= 0, "eventfd creation failed (harness setup)");
        detail::require(fired_old == 1 && fired_new == 1,
            "re-watch of a closed and reused fd number: the new fd's events must "
            "reach the new watch exactly once, observed " + std::to_string(fired_new)
            + " calls (old watch: " + std::to_string(fired_old) + ")");
    }

    // C13: removeWatch() stops delivery at once, including for the rest of the
    // event being dispatched. A socket both readable and writable raises one
    // event carrying both bits; a watch on Read|Write that removes itself from
    // its callback must be called once. It fails an adapter that dispatches one
    // event as several callbacks (read, then write) without re-checking the watch
    // in between, as trantor's Channel does when an adapter sets its read and
    // write callbacks separately. relais removes a watch and then resumes a
    // coroutine from the same callback, so a second call runs against a
    // coroutine that has moved on, or a frame already destroyed.
    template<io::IoContext Io, typename Drive>
    static void checkRemoveWatchWithinEvent(Io& io, Drive drive) {
        int sv[2];
        detail::require(
            ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sv) == 0,
            "socketpair creation failed (harness setup)");
        const char byte = 0;
        [[maybe_unused]] auto _ = ::write(sv[1], &byte, 1);  // sv[0]: readable and writable

        int calls = 0;
        typename Io::WatchHandle h{};
        h = io.addWatch(sv[0], IoEvent::Read | IoEvent::Write, [&](IoEvent) {
            ++calls;
            io.removeWatch(h);
        });
        detail::driveFor(io, drive, [&] { return calls > 0; }, std::chrono::seconds(1));
        detail::pump(io, drive, 3);  // a refire on a later turn would surface here

        if (calls == 0) io.removeWatch(h);
        ::close(sv[0]);
        ::close(sv[1]);
        detail::require(calls == 1,
            "removeWatch() within an event carrying several bits: no further call "
            "may follow for that event, observed " + std::to_string(calls) + " calls");
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

    // C11: arm+cancel at scale. Arming many timers then cancelling them all must
    // leave none firing — and, on a conforming adapter, must not accumulate state
    // (a cancel removes the timer, it does not tombstone it until its deadline).
    // This encodes the complexity expectation in the IoContext contract: relais
    // arms+cancels a timer per in-flight I/O wait, so an adapter that defers
    // cancellation to maturity would grow O(timeout) state at QPS. A later
    // uncancelled fence proves the subsystem is still live after the churn.
    template<io::IoContext Io, typename Drive>
    static void checkTimerArmCancelAtScale(Io& io, Drive drive) {
        constexpr int N = 2000;
        int fired = 0;
        std::vector<typename Io::TimerToken> tokens;
        tokens.reserve(N);
        for (int i = 0; i < N; ++i)
            tokens.push_back(
                io.postDelayed(std::chrono::milliseconds(1), [&] { ++fired; }));
        for (auto t : tokens) io.cancelTimer(t);

        bool fence = false;
        io.postDelayed(std::chrono::milliseconds(3), [&] { fence = true; });
        drive(io, [&] { return fence; });

        detail::require(fired == 0,
            "cancelTimer() at scale: no cancelled timer may fire, observed "
            + std::to_string(fired));
    }

    // -- aggregate ----------------------------------------------------------

    // Checks are numbered in the order they were added; runAll groups them by
    // subject and runs C1-C6, C10, C12, C13, C7-C9, C11.
    template<io::IoContext Io, typename Drive>
    static void runAll(Io& io, Drive drive) {
        checkPostExecutes(io, drive);
        checkPostOrdering(io, drive);
        checkPostFromCallback(io, drive);
        checkWatchReadable(io, drive);
        checkUpdateWatch(io, drive);
        checkRemoveWatch(io, drive);
        checkRemoveWatchReentrant(io, drive);
        checkRewatchReusedFd(io, drive);
        checkRemoveWatchWithinEvent(io, drive);
        checkCrossThreadPost(io, drive);
        checkPostDelayedFires(io, drive);
        checkCancelTimer(io, drive);
        checkTimerArmCancelAtScale(io, drive);
    }
};

}  // namespace jcailloux::relais::testing

#endif  // JCX_RELAIS_TESTING_IO_CONTEXT_CONFORMANCE_H