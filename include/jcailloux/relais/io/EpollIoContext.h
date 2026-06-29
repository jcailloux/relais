#ifndef JCX_RELAIS_IO_EPOLL_IO_CONTEXT_H
#define JCX_RELAIS_IO_EPOLL_IO_CONTEXT_H

#include <jcailloux/relais/io/IoContext.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <memory_resource>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <time.h>

namespace jcailloux::relais::io {

// EpollIoContext — production epoll-based event loop with thread-safe posting
// and timer support via timerfd.
//
// Thread-safety model:
// - post() and postDelayed() are safe to call from any thread
// - All other methods must be called from the event loop thread
// - The event loop wakes up via a pipe when post() is called from another thread

class EpollIoContext {
public:
    using WatchHandle = int;
    using Clock = std::chrono::steady_clock;
    using TimerToken = uint64_t;

    EpollIoContext() {
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0)
            throw std::runtime_error("epoll_create1 failed");

        // Wakeup pipe for thread-safe post()
        int fds[2];
        if (pipe2(fds, O_NONBLOCK | O_CLOEXEC) < 0) {
            ::close(epoll_fd_);
            throw std::runtime_error("pipe2 failed");
        }
        pipe_read_ = fds[0];
        pipe_write_ = fds[1];

        // Watch the pipe read end
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = pipe_read_;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, pipe_read_, &ev) < 0) {
            ::close(pipe_read_);
            ::close(pipe_write_);
            ::close(epoll_fd_);
            throw std::runtime_error("epoll_ctl pipe failed");
        }

        // Timer fd for postDelayed()
        timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (timer_fd_ < 0) {
            ::close(pipe_read_);
            ::close(pipe_write_);
            ::close(epoll_fd_);
            throw std::runtime_error("timerfd_create failed");
        }

        epoll_event tev{};
        tev.events = EPOLLIN;
        tev.data.fd = timer_fd_;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &tev) < 0) {
            ::close(timer_fd_);
            ::close(pipe_read_);
            ::close(pipe_write_);
            ::close(epoll_fd_);
            throw std::runtime_error("epoll_ctl timerfd failed");
        }
    }

    ~EpollIoContext() {
        if (timer_fd_ >= 0) ::close(timer_fd_);
        if (pipe_read_ >= 0) ::close(pipe_read_);
        if (pipe_write_ >= 0) ::close(pipe_write_);
        if (epoll_fd_ >= 0) ::close(epoll_fd_);
    }

    EpollIoContext(const EpollIoContext&) = delete;
    EpollIoContext& operator=(const EpollIoContext&) = delete;

    WatchHandle addWatch(int fd, IoEvent events, std::function<void(IoEvent)> cb) {
        // Generation guard: a fresh generation per addWatch lets the
        // dispatch loop reject a stale event harvested by epoll_wait for an fd
        // that was closed and re-watched *within the same runOnce* (re-entrant
        // drainPosted via the pipe branch). The generation rides in the high 32
        // bits of epoll_data.u64; the fd stays in the low 32.
        uint32_t gen = ++watch_gen_seq_;

        epoll_event ev{};
        ev.events = toEpoll(events);
        ev.data.u64 = packData(gen, fd);

        watches_[fd] = {events, gen, std::move(cb)};

        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
                watches_.erase(fd);
                throw std::runtime_error("epoll_ctl ADD/MOD failed");
            }
        }
        return fd;
    }

    void removeWatch(WatchHandle handle) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, handle, nullptr);
        watches_.erase(handle);
    }

    void updateWatch(WatchHandle handle, IoEvent events) {
        auto it = watches_.find(handle);
        if (it == watches_.end()) return;

        it->second.events = events;

        epoll_event ev{};
        ev.events = toEpoll(events);
        // Same watch, same generation — only the mask changes.
        ev.data.u64 = packData(it->second.generation, handle);
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, handle, &ev);
    }

    /// Thread-safe: post a callback to be executed on the event loop thread.
    void post(std::function<void()> cb) {
        {
            std::lock_guard lock(post_mutex_);
            post_queue_.push_back(std::move(cb));
        }
        // Loop-local post needs no wakeup: a callback queued from the loop
        // thread is drained within the current runOnce (its trailing
        // drainPosted()) or, failing that, on the very next iteration —
        // computeTimeout() returns 0 while post_queue_ is non-empty, so the loop
        // never sleeps with work pending. The pipe write is only needed to
        // interrupt a loop blocked in epoll_wait on another thread (cross-thread
        // post). Mirrors the loop-local arm in postDelayed().
        if (!isInLoopThread()) wakeLoop();
    }

    /// Thread-safe: schedule a callback after a delay. Returns a token for cancellation.
    template<typename Rep, typename Period>
    TimerToken postDelayed(std::chrono::duration<Rep, Period> delay, std::function<void()> cb) {
        auto deadline = Clock::now() + delay;
        auto token = next_timer_token_.fetch_add(1, std::memory_order_relaxed);
        {
            std::lock_guard lock(post_mutex_);
            auto it = timers_.emplace(deadline, TimerEntry{token, std::move(cb)});
            by_token_.emplace(token, it);
        }
        // Loop-local arm needs no syscall: runOnce re-arms the timerfd via
        // fireExpiredTimers()->rearmTimerfd() before its next epoll_wait, so a
        // postDelayed issued on the loop thread is already accounted for. The
        // pipe wakeup is only needed to interrupt a loop blocked in another
        // thread (cross-thread arm).
        if (!isInLoopThread()) wakeLoop();
        return token;
    }

    /// Thread-safe: cancel a pending timer. No-op if already fired or not found.
    /// Removes the entry outright (O(log n)) — no tombstone is left to accumulate.
    void cancelTimer(TimerToken token) {
        std::lock_guard lock(post_mutex_);
        auto it = by_token_.find(token);
        if (it == by_token_.end()) return;  // already fired or unknown
        timers_.erase(it->second);
        by_token_.erase(it);
    }

    /// Run the event loop until stop() is called.
    void run() {
        stopped_.store(false, std::memory_order_relaxed);
        while (!stopped_.load(std::memory_order_relaxed)) {
            runOnce(computeTimeout());
        }
    }

    /// Run until a predicate is satisfied.
    template<typename Pred>
    void runUntil(Pred&& pred) {
        while (!pred()) {
            runOnce(computeTimeout());
        }
    }

    /// Stop the event loop (thread-safe).
    void stop() {
        stopped_.store(true, std::memory_order_relaxed);
        wakeLoop();  // in case blocked in epoll_wait
    }

    /// True if called from the thread currently driving the loop. Lets callers
    /// (e.g. PgProvider::init) assert thread-affinity invariants in debug.
    [[nodiscard]] bool isInLoopThread() const noexcept {
        return loop_thread_.load(std::memory_order_relaxed) == std::this_thread::get_id();
    }

    /// Diagnostic: number of pipe wakeups issued (cross-thread post/postDelayed
    /// and stop). A loop-local postDelayed arms the timerfd without a wakeup, so
    /// this stays flat under loop-thread arming — exercised by the unit tests.
    [[nodiscard]] uint64_t loopWakeups() const noexcept {
        return wakeups_.load(std::memory_order_relaxed);
    }

    /// Diagnostic: timers currently in flight (excludes cancelled ones, which are
    /// removed outright rather than tombstoned). Used to assert no accumulation.
    [[nodiscard]] size_t pendingTimerCount() const {
        std::lock_guard lock(post_mutex_);
        return timers_.size();
    }

    /// Run one iteration of the event loop.
    void runOnce(int timeout_ms = 0) {
        loop_thread_.store(std::this_thread::get_id(), std::memory_order_relaxed);
        drainPosted();
        fireExpiredTimers();

        // fireExpiredTimers() above may have run a timer that posts a loop-local
        // callback (e.g. a deferred timeout-resume). Such a post does not wake
        // the loop — the wakeup is gated on cross-thread posts — and the
        // epoll_wait timeout was fixed before those timers ran. Re-check the
        // queue and poll instead of blocking, so the trailing drainPosted() runs
        // the callback this iteration rather than after the full timeout. Only
        // matters when we were about to block (timeout_ms != 0).
        if (timeout_ms != 0) {
            std::lock_guard lock(post_mutex_);
            if (!post_queue_.empty()) timeout_ms = 0;
        }

        static constexpr int MAX_EVENTS = 64;
        epoll_event events[MAX_EVENTS];
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout_ms);

        for (int i = 0; i < n; ++i) {
            uint64_t data = events[i].data.u64;
            int fd = unpackFd(data);

            if (fd == pipe_read_) {
                // Drain the wakeup pipe
                char buf[64];
                while (::read(pipe_read_, buf, sizeof(buf)) > 0) {}
                drainPosted();
                continue;
            }

            if (fd == timer_fd_) {
                // Drain the timerfd
                uint64_t expirations;
                [[maybe_unused]] auto _ = ::read(timer_fd_, &expirations, sizeof(expirations));
                fireExpiredTimers();
                continue;
            }

            auto it = watches_.find(fd);
            // Generation guard: an earlier iteration of this same loop
            // may have closed `fd` and a re-watch re-bound the number to a new
            // connection. The event in events[] still carries the *old*
            // generation, so a mismatch means it is stale on a recycled fd —
            // skip it rather than deliver it to the wrong watch.
            if (it != watches_.end() && it->second.generation == unpackGen(data)) {
                auto io_events = fromEpoll(events[i].events);
                // Copy the callback out before invoking: relais legitimately
                // calls removeWatch(fd) from inside its own callback (e.g. a
                // connection self-removes on EOF), which erases this map node and
                // would free the std::function — and its captured state — while
                // it executes. The stack-local copy keeps the target alive for
                // the whole call regardless of self-removal. Small captures hit
                // std::function's SBO (no heap alloc), and this is the I/O event
                // path, never the L1-hit hot path.
                auto cb = it->second.callback;
                cb(io_events);
            }
        }

        drainPosted();
        fireExpiredTimers();
    }

private:
    // epoll_data.u64 layout: high 32 bits = watch generation, low 32 = fd.
    static uint64_t packData(uint32_t gen, int fd) noexcept {
        return (static_cast<uint64_t>(gen) << 32) | static_cast<uint32_t>(fd);
    }
    static int unpackFd(uint64_t data) noexcept {
        return static_cast<int>(static_cast<uint32_t>(data));
    }
    static uint32_t unpackGen(uint64_t data) noexcept {
        return static_cast<uint32_t>(data >> 32);
    }

    void wakeLoop() {
        char byte = 1;
        [[maybe_unused]] auto _ = ::write(pipe_write_, &byte, 1);
        wakeups_.fetch_add(1, std::memory_order_relaxed);
    }

    static uint32_t toEpoll(IoEvent events) {
        uint32_t e = 0;
        if (hasEvent(events, IoEvent::Read))  e |= EPOLLIN;
        if (hasEvent(events, IoEvent::Write)) e |= EPOLLOUT;
        if (hasEvent(events, IoEvent::Error)) e |= EPOLLERR;
        return e;
    }

    static IoEvent fromEpoll(uint32_t events) {
        IoEvent e = IoEvent::None;
        if (events & EPOLLIN)  e |= IoEvent::Read;
        if (events & EPOLLOUT) e |= IoEvent::Write;
        if (events & (EPOLLERR | EPOLLHUP)) e |= IoEvent::Error;
        return e;
    }

    struct WatchEntry {
        IoEvent events;
        uint32_t generation;
        std::function<void(IoEvent)> callback;
    };

    struct TimerEntry {
        TimerToken token;
        std::function<void()> callback;
    };

    void drainPosted() {
        std::deque<std::function<void()>> local;
        {
            std::lock_guard lock(post_mutex_);
            local.swap(post_queue_);
        }
        for (auto& cb : local) {
            cb();
        }
    }

    void fireExpiredTimers() {
        auto now = Clock::now();

        // Move matured timers out under lock, then fire them unlocked (a
        // callback may re-enter postDelayed/cancelTimer). fire_scratch_ is a
        // reused member: loop-thread only, capacity retained → no per-fire alloc.
        fire_scratch_.clear();
        {
            std::lock_guard lock(post_mutex_);
            for (auto it = timers_.begin(); it != timers_.end() && it->first <= now; ) {
                by_token_.erase(it->second.token);
                fire_scratch_.push_back(std::move(it->second));
                it = timers_.erase(it);
            }
        }

        for (auto& entry : fire_scratch_) {
            entry.callback();
        }

        rearmTimerfd();
    }

    void rearmTimerfd() {
        std::lock_guard lock(post_mutex_);
        struct itimerspec its{};

        if (!timers_.empty()) {
            auto deadline = timers_.begin()->first;
            auto now = Clock::now();
            if (deadline <= now) {
                // Fire ASAP
                its.it_value.tv_nsec = 1;
            } else {
                auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now);
                its.it_value.tv_sec = ns.count() / 1'000'000'000;
                its.it_value.tv_nsec = ns.count() % 1'000'000'000;
            }
        }
        // its = {{0,0},{0,0}} disarms the timer if queue is empty
        timerfd_settime(timer_fd_, 0, &its, nullptr);
    }

    int computeTimeout() const {
        // If there are posted callbacks, don't block
        {
            std::lock_guard lock(post_mutex_);
            if (!post_queue_.empty()) return 0;
        }
        // Default: block up to 100ms (timerfd handles precise wakeups)
        return 100;
    }

    int epoll_fd_ = -1;
    int pipe_read_ = -1;
    int pipe_write_ = -1;
    int timer_fd_ = -1;

    std::unordered_map<int, WatchEntry> watches_;
    uint32_t watch_gen_seq_ = 0;  // loop-thread only (addWatch)

    mutable std::mutex post_mutex_;
    std::deque<std::function<void()>> post_queue_;

    // Timer subsystem: deadline-ordered multimap + token→iterator index, both
    // backed by a pooled resource. arm = emplace (O(log n)), cancel = erase via
    // the index (O(log n), no tombstone), fire = pop from begin(). The pool
    // recycles freed nodes → zero steady-state malloc on arm/cancel. Declared
    // before the containers so it outlives them (and is built first).
    std::pmr::unsynchronized_pool_resource timer_pool_;
    std::pmr::multimap<Clock::time_point, TimerEntry> timers_{&timer_pool_};
    std::pmr::unordered_map<TimerToken,
        std::pmr::multimap<Clock::time_point, TimerEntry>::iterator> by_token_{&timer_pool_};
    std::vector<TimerEntry> fire_scratch_;  // reused by fireExpiredTimers (loop-thread)

    std::atomic<uint64_t> next_timer_token_{1};
    std::atomic<uint64_t> wakeups_{0};
    std::atomic<bool> stopped_{false};
    std::atomic<std::thread::id> loop_thread_{};
};

static_assert(IoContext<EpollIoContext>);

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_EPOLL_IO_CONTEXT_H
