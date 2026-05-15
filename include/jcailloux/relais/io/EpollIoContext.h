#ifndef JCX_RELAIS_IO_EPOLL_IO_CONTEXT_H
#define JCX_RELAIS_IO_EPOLL_IO_CONTEXT_H

#include <jcailloux/relais/io/IoContext.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <queue>
#include <set>
#include <stdexcept>
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
        epoll_event ev{};
        ev.events = toEpoll(events);
        ev.data.fd = fd;

        watches_[fd] = {events, std::move(cb)};

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
        ev.data.fd = handle;
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, handle, &ev);
    }

    /// Thread-safe: post a callback to be executed on the event loop thread.
    void post(std::function<void()> cb) {
        {
            std::lock_guard lock(post_mutex_);
            post_queue_.push_back(std::move(cb));
        }
        // Wakeup the event loop
        char byte = 1;
        [[maybe_unused]] auto _ = ::write(pipe_write_, &byte, 1);
    }

    /// Thread-safe: schedule a callback after a delay. Returns a token for cancellation.
    template<typename Rep, typename Period>
    TimerToken postDelayed(std::chrono::duration<Rep, Period> delay, std::function<void()> cb) {
        auto deadline = Clock::now() + delay;
        auto token = next_timer_token_.fetch_add(1, std::memory_order_relaxed);
        {
            std::lock_guard lock(post_mutex_);
            timer_queue_.push({deadline, token, std::move(cb)});
        }
        // Wakeup to re-evaluate the nearest deadline
        char byte = 1;
        [[maybe_unused]] auto _ = ::write(pipe_write_, &byte, 1);
        return token;
    }

    /// Thread-safe: cancel a pending timer. No-op if already fired or not found.
    void cancelTimer(TimerToken token) {
        std::lock_guard lock(post_mutex_);
        cancelled_tokens_.insert(token);
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
        // Wakeup in case blocked in epoll_wait
        char byte = 1;
        [[maybe_unused]] auto _ = ::write(pipe_write_, &byte, 1);
    }

    /// Run one iteration of the event loop.
    void runOnce(int timeout_ms = 0) {
        drainPosted();
        fireExpiredTimers();

        static constexpr int MAX_EVENTS = 64;
        epoll_event events[MAX_EVENTS];
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout_ms);

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;

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
            if (it != watches_.end()) {
                auto io_events = fromEpoll(events[i].events);
                it->second.callback(io_events);
            }
        }

        drainPosted();
        fireExpiredTimers();
    }

private:
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
        std::function<void(IoEvent)> callback;
    };

    struct TimerEntry {
        Clock::time_point deadline;
        TimerToken token;
        std::function<void()> callback;

        bool operator>(const TimerEntry& o) const noexcept {
            return deadline > o.deadline;
        }
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

        // Move matured timers out under lock
        std::vector<TimerEntry> to_fire;
        {
            std::lock_guard lock(post_mutex_);
            while (!timer_queue_.empty() && timer_queue_.top().deadline <= now) {
                auto entry = std::move(const_cast<TimerEntry&>(timer_queue_.top()));
                timer_queue_.pop();
                if (cancelled_tokens_.erase(entry.token)) continue;
                to_fire.push_back(std::move(entry));
            }
        }

        for (auto& entry : to_fire) {
            entry.callback();
        }

        rearmTimerfd();
    }

    void rearmTimerfd() {
        std::lock_guard lock(post_mutex_);
        struct itimerspec its{};

        if (!timer_queue_.empty()) {
            auto deadline = timer_queue_.top().deadline;
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

    mutable std::mutex post_mutex_;
    std::deque<std::function<void()>> post_queue_;
    std::priority_queue<TimerEntry, std::vector<TimerEntry>, std::greater<TimerEntry>> timer_queue_;
    std::set<TimerToken> cancelled_tokens_;
    std::atomic<uint64_t> next_timer_token_{1};
    std::atomic<bool> stopped_{false};
};

static_assert(IoContext<EpollIoContext>);

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_EPOLL_IO_CONTEXT_H
