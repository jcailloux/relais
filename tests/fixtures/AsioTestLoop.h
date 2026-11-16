#ifndef JCX_RELAIS_TEST_ASIO_TEST_LOOP_H
#define JCX_RELAIS_TEST_ASIO_TEST_LOOP_H

#include <fixtures/AsioFlavor.h>

#include <jcailloux/relais/io/IoContext.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <thread>
#include <unordered_set>
#include <utility>

namespace jcailloux::relais::io::test {

// AsioTestLoop — the shipped asio adapter on an io_context of its own, which a
// test drives handler by handler as it drives EpollIoContext: runOnce(timeout)
// runs one handler, or waits up to the timeout for one. Like
// AdversarialIoContext, it counts what relais must never do (breaches()): add
// or remove a watch inside an event dispatch, change or remove a handle it does
// not hold, call anything but post() from another thread.
//
// Loop-thread affinity: the thread that builds it drives it.
class AsioTestLoop {
    using Inner = relais_test::AsioIo;

public:
    using WatchHandle = Inner::WatchHandle;

    AsioTestLoop() {
        // asio opens its reactor's descriptors (epoll, eventfd, timerfd) with
        // the first timer or watched descriptor: before a test counts them.
        relais_test::net::steady_timer first(ctx_);
    }
    AsioTestLoop(const AsioTestLoop&) = delete;
    AsioTestLoop& operator=(const AsioTestLoop&) = delete;

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

    void post(std::function<void()> cb) { io_.post(std::move(cb)); }

    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        checkLoopThread();
        armed_timers_.fetch_add(1, std::memory_order_relaxed);
        io_.postDelayed(delay, [this, cb = std::move(cb)] {
            Dispatch guard{*this};
            cb();
        });
    }

    void detached() {
        checkLoopThread();
        io_.detached();
    }

    [[nodiscard]] bool isInLoopThread() const noexcept {
        return std::this_thread::get_id() == owner_;
    }

    // -- driving and diagnostics ---------------------------------------------

    void runOnce(int timeout_ms = 0) {
        if (timeout_ms <= 0) {
            ctx_.poll_one();
        } else {
            ctx_.run_one_for(std::chrono::milliseconds(timeout_ms));
        }
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
    // A watch or timer callback.
    struct Dispatch {
        AsioTestLoop& loop;
        explicit Dispatch(AsioTestLoop& l) noexcept : loop(l) { ++loop.dispatch_depth_; }
        ~Dispatch() { --loop.dispatch_depth_; }
        Dispatch(const Dispatch&) = delete;
        Dispatch& operator=(const Dispatch&) = delete;
    };

    void checkLoopThread() {
        if (std::this_thread::get_id() != owner_) breach();
    }
    void noteWatchChange() {
        if (dispatch_depth_ > 0) breach();
    }
    void breach() noexcept { breaches_.fetch_add(1, std::memory_order_relaxed); }

    relais_test::net::io_context ctx_{1};
    relais_test::net::executor_work_guard<relais_test::net::io_context::executor_type> work_ =
        relais_test::net::make_work_guard(ctx_);
    Inner io_{ctx_};

    std::unordered_set<WatchHandle> watches_;
    const std::thread::id owner_ = std::this_thread::get_id();
    int dispatch_depth_ = 0;
    std::atomic<std::size_t> armed_timers_{0};
    std::atomic<std::size_t> breaches_{0};
};

}  // namespace jcailloux::relais::io::test

#endif  // JCX_RELAIS_TEST_ASIO_TEST_LOOP_H
