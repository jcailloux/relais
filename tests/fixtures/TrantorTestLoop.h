#ifndef JCX_RELAIS_TEST_TRANTOR_TEST_LOOP_H
#define JCX_RELAIS_TEST_TRANTOR_TEST_LOOP_H

#include <jcailloux/relais/integration/trantor.h>
#include <jcailloux/relais/io/IoContext.h>

#include <trantor/net/Channel.h>
#include <trantor/net/EventLoop.h>

#include <sys/timerfd.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <unordered_set>
#include <utility>

namespace jcailloux::relais::io::test {

// TrantorTestLoop — the shipped TrantorIoContext on a trantor::EventLoop of its
// own, which a test drives turn by turn as it drives EpollIoContext:
// runOnce(timeout) runs the loop until a turn has run one of relais's
// callbacks, or until the timeout. Like AdversarialIoContext, it counts what
// relais must never do (breaches()): add or remove a watch inside an event
// dispatch, change or remove a handle it does not hold, call anything but
// post() from another thread.
//
// Loop-thread affinity: the thread that builds it drives it.
class TrantorTestLoop {
public:
    using WatchHandle = TrantorIoContext::WatchHandle;

    TrantorTestLoop() : io_(&loop_) {
        // trantor allows one EventLoop per thread, and forgets it only when a
        // loop() returns: let the next one be built on this thread.
        loop_.queueInLoop([this] { loop_.quit(); });
        loop_.loop();

        turn_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        turn_timer_ = std::make_unique<::trantor::Channel>(&loop_, turn_fd_);
        turn_timer_->setReadCallback([this] {
            std::uint64_t expirations = 0;
            [[maybe_unused]] auto n = ::read(turn_fd_, &expirations, sizeof expirations);
            loop_.quit();
        });
        turn_timer_->enableReading();
    }

    ~TrantorTestLoop() {
        turn_timer_->disableAll();
        turn_timer_->remove();
        turn_timer_.reset();
        ::close(turn_fd_);
    }

    TrantorTestLoop(const TrantorTestLoop&) = delete;
    TrantorTestLoop& operator=(const TrantorTestLoop&) = delete;

    // -- IoContext concept surface -------------------------------------------

    WatchHandle addWatch(int fd, IoEvent mask, std::function<void(IoEvent)> cb) {
        checkLoopThread();
        noteWatchChange();
        auto h = io_.addWatch(fd, mask, [this, cb = std::move(cb)](IoEvent ev) {
            Dispatch guard{*this};
            cb(ev);
        });
        watches_.insert(h);
        return h;
    }

    void updateWatch(WatchHandle h, IoEvent mask) {
        checkLoopThread();
        if (!watches_.contains(h)) return breach();
        io_.updateWatch(h, mask);
    }

    void removeWatch(WatchHandle h) {
        checkLoopThread();
        noteWatchChange();
        if (watches_.erase(h) == 0) return breach();
        io_.removeWatch(h);
    }

    void post(std::function<void()> cb) {
        io_.post([this, cb = std::move(cb)] {
            cb();
            loop_.quit();  // ends this turn
        });
    }

    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        checkLoopThread();
        armed_timers_.fetch_add(1, std::memory_order_relaxed);
        io_.postDelayed(delay, [this, cb = std::move(cb)] {
            Dispatch guard{*this};
            cb();
        });
    }

    [[nodiscard]] bool isInLoopThread() const noexcept { return io_.isInLoopThread(); }

    // -- driving and diagnostics ---------------------------------------------

    void runOnce(int timeout_ms = 0) {
        armTurn(timeout_ms);
        loop_.loop();
        disarmTurn();
    }

    template<typename Pred>
    void runUntil(Pred&& pred) {
        while (!pred()) runOnce(10);
    }

    [[nodiscard]] std::size_t breaches() const noexcept {
        return breaches_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::size_t armedTimers() const noexcept {
        return armed_timers_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::size_t liveWatches() const noexcept { return watches_.size(); }

private:
    // A watch or timer callback; the turn ends after it.
    struct Dispatch {
        TrantorTestLoop& loop;
        explicit Dispatch(TrantorTestLoop& l) noexcept : loop(l) { ++loop.dispatch_depth_; }
        ~Dispatch() {
            --loop.dispatch_depth_;
            loop.loop_.quit();
        }
        Dispatch(const Dispatch&) = delete;
        Dispatch& operator=(const Dispatch&) = delete;
    };

    void armTurn(int timeout_ms) {
        itimerspec spec{};
        if (timeout_ms <= 0) {
            spec.it_value.tv_nsec = 1;
        } else {
            spec.it_value.tv_sec = timeout_ms / 1000;
            spec.it_value.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1'000'000;
        }
        ::timerfd_settime(turn_fd_, 0, &spec, nullptr);
    }

    // Also forgets an expiry not read yet.
    void disarmTurn() {
        itimerspec spec{};
        ::timerfd_settime(turn_fd_, 0, &spec, nullptr);
    }

    void checkLoopThread() {
        if (std::this_thread::get_id() != owner_) breach();
    }
    void noteWatchChange() {
        if (dispatch_depth_ > 0) breach();
    }
    void breach() noexcept { breaches_.fetch_add(1, std::memory_order_relaxed); }

    ::trantor::EventLoop loop_;
    TrantorIoContext io_;
    int turn_fd_ = -1;
    std::unique_ptr<::trantor::Channel> turn_timer_;

    std::unordered_set<WatchHandle> watches_;
    const std::thread::id owner_ = std::this_thread::get_id();
    int dispatch_depth_ = 0;
    std::atomic<std::size_t> armed_timers_{0};
    std::atomic<std::size_t> breaches_{0};
};

}  // namespace jcailloux::relais::io::test

#endif  // JCX_RELAIS_TEST_TRANTOR_TEST_LOOP_H
