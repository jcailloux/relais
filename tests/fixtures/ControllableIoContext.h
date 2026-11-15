#ifndef JCX_RELAIS_TEST_CONTROLLABLE_IO_CONTEXT_H
#define JCX_RELAIS_TEST_CONTROLLABLE_IO_CONTEXT_H

#include <jcailloux/relais/io/EpollIoContext.h>
#include <jcailloux/relais/io/IoContext.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace jcailloux::relais::io::test {

// ControllableIoContext — a fault-injecting wrapper over the bundled
// EpollIoContext for the liveness/timeout suite. It is a *faithful* IoContext
// (passes IoContextConformance): every concept method forwards to the inner
// loop, its callbacks wrapped only to be counted, so the wrapper is a drop-in
// for EpollIoContext anywhere a test drives the loop.
//
// On top of that it can withhold read-readiness from a single fd while leaving
// timers and every other fd untouched — the exact shape of "the result arrived
// but the connection didn't read it in time", which is what turns a committed
// write into a client-side query_timeout (scenario A).
//
// Mechanism: EpollIoContext watches are level-triggered (EPOLLIN, no EPOLLET),
// so dropping the Read bit via updateWatch suppresses delivery of already-
// buffered bytes, and restoring it re-signals them on the next loop turn. We
// track the mask relais last asked for per fd and re-apply it with Read masked
// off while a hold is active, so an addWatch or updateWatch issued during a
// hold cannot accidentally lift it.
//
// It also counts the watches added or removed inside an event dispatch, that
// is while a watch or timer callback runs: relais makes these changes from
// posted tasks only, so an adapter may free a watch synchronously.
//
// Loop-thread affinity matches EpollIoContext: the watch/hold methods touch the
// fixture's own maps and must run on the loop thread; post()/postDelayed()/
// cancelTimer() forward to the (thread-safe) inner loop and add no shared state.
//
// Test-only — lives entirely in tests/fixtures, nothing here is in include/.
class ControllableIoContext {
public:
    using Inner       = EpollIoContext;
    using WatchHandle = Inner::WatchHandle;  // == int (the fd itself)
    using TimerToken  = Inner::TimerToken;
    using Clock       = Inner::Clock;

    ControllableIoContext() = default;
    ControllableIoContext(const ControllableIoContext&) = delete;
    ControllableIoContext& operator=(const ControllableIoContext&) = delete;

    // -- IoContext concept surface (forwarding) ------------------------------

    WatchHandle addWatch(int fd, IoEvent events, std::function<void(IoEvent)> cb) {
        noteWatchChange();
        ++added_watches_;
        requested_[fd] = events;
        last_added_fd_ = fd;
        return inner_.addWatch(fd, effective(fd, events),
            [this, cb = std::move(cb)](IoEvent ev) {
                Dispatch d{*this};
                cb(ev);
            });
    }

    void removeWatch(WatchHandle handle) {
        noteWatchChange();
        // Clear the fixture's per-fd state so a recycled fd starts clean.
        requested_.erase(handle);
        held_.erase(handle);
        inner_.removeWatch(handle);
    }

    void updateWatch(WatchHandle handle, IoEvent events) {
        requested_[handle] = events;
        inner_.updateWatch(handle, effective(handle, events));
    }

    void post(std::function<void()> cb) { inner_.post(std::move(cb)); }

    template<typename Rep, typename Period>
    TimerToken postDelayed(std::chrono::duration<Rep, Period> delay,
                           std::function<void()> cb) {
        return inner_.postDelayed(delay, [this, cb = std::move(cb)] {
            Dispatch d{*this};
            cb();
        });
    }

    void cancelTimer(TimerToken token) { inner_.cancelTimer(token); }

    // -- fault injection ----------------------------------------------------

    // Withhold read-readiness on `fd`: buffered bytes stay undelivered until
    // releaseReads(fd). Timers and every other fd keep working. If `fd` has no
    // watch yet the hold latches and applies on the next addWatch/updateWatch.
    void holdReads(int fd) {
        held_.insert(fd);
        reapply(fd);
    }

    // Restore read-readiness; level-triggered epoll re-signals buffered bytes
    // on the next loop turn. No-op if `fd` was not held.
    void releaseReads(int fd) {
        if (held_.erase(fd) == 0) return;
        reapply(fd);  // re-push the real (un-masked) mask
    }

    [[nodiscard]] bool isHeld(int fd) const { return held_.contains(fd); }

    // Every addWatch so far.
    [[nodiscard]] std::size_t addedWatches() const noexcept { return added_watches_; }

    // Watches added or removed while a watch or timer callback ran.
    [[nodiscard]] std::size_t watchChangesInDispatch() const noexcept {
        return changes_in_dispatch_;
    }

    // The fd of the most recent addWatch — the connection a pool/client just
    // established. Stable for that connection's lifetime, so a test reads it
    // right after create()/connect() to learn which fd to hold, without a
    // friend or an fd accessor on the pooled types.
    [[nodiscard]] int lastAddedFd() const noexcept { return last_added_fd_; }

    // -- loop driving + diagnostics (forwarded) -----------------------------

    void run() { inner_.run(); }
    void stop() { inner_.stop(); }
    void runOnce(int timeout_ms = 0) { inner_.runOnce(timeout_ms); }

    template<typename Pred>
    void runUntil(Pred&& pred) { inner_.runUntil(std::forward<Pred>(pred)); }

    [[nodiscard]] bool isInLoopThread() const noexcept { return inner_.isInLoopThread(); }

    // The write(pipe) counter the timeout tests rely on: each cross-thread wakeup
    // is one write(pipe). Forwarded from production rather than duplicated.
    [[nodiscard]] uint64_t loopWakeups() const noexcept { return inner_.loopWakeups(); }
    [[nodiscard]] std::size_t pendingTimerCount() const { return inner_.pendingTimerCount(); }

    [[nodiscard]] Inner& inner() noexcept { return inner_; }

private:
    struct Dispatch {
        ControllableIoContext& io;
        explicit Dispatch(ControllableIoContext& c) noexcept : io(c) { ++io.dispatch_depth_; }
        ~Dispatch() { --io.dispatch_depth_; }
        Dispatch(const Dispatch&) = delete;
        Dispatch& operator=(const Dispatch&) = delete;
    };

    void noteWatchChange() noexcept {
        if (dispatch_depth_ > 0) ++changes_in_dispatch_;
    }

    // The mask actually pushed to epoll: what relais asked for, minus Read while
    // the fd is held.
    [[nodiscard]] IoEvent effective(int fd, IoEvent requested) const {
        if (!held_.contains(fd)) return requested;
        return static_cast<IoEvent>(
            static_cast<uint8_t>(requested) & ~static_cast<uint8_t>(IoEvent::Read));
    }

    void reapply(int fd) {
        auto it = requested_.find(fd);
        if (it != requested_.end())
            inner_.updateWatch(fd, effective(fd, it->second));
    }

    Inner inner_;
    std::unordered_map<int, IoEvent> requested_;  // last mask relais asked for, per fd
    std::unordered_set<int> held_;                // fds with reads currently withheld
    int last_added_fd_ = -1;                      // fd of the most recent addWatch
    int dispatch_depth_ = 0;                      // watch or timer callbacks running
    std::size_t changes_in_dispatch_ = 0;         // addWatch/removeWatch among them
    std::size_t added_watches_ = 0;
};

static_assert(IoContext<ControllableIoContext>,
              "ControllableIoContext must remain a faithful IoContext");

}  // namespace jcailloux::relais::io::test

#endif  // JCX_RELAIS_TEST_CONTROLLABLE_IO_CONTEXT_H
