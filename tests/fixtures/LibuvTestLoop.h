#ifndef JCX_RELAIS_TEST_LIBUV_TEST_LOOP_H
#define JCX_RELAIS_TEST_LIBUV_TEST_LOOP_H

#include <jcailloux/relais/integration/libuv.h>
#include <jcailloux/relais/io/IoContext.h>

#include <uv.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <optional>
#include <thread>
#include <unordered_set>
#include <utility>

namespace jcailloux::relais::io::test {

// Runs `loop` until no handle is left to close, then closes it: a handle still
// open there is one somebody leaked, and ends the process.
inline void closeLoop(uv_loop_t& loop) {
    for (int turn = 0; turn < 1000 && uv_loop_alive(&loop) != 0; ++turn)
        uv_run(&loop, UV_RUN_NOWAIT);
    if (const int rc = uv_loop_close(&loop); rc != 0) {
        std::fprintf(stderr, "libuv: a handle is still open on the loop (%s)\n", uv_strerror(rc));
        std::abort();
    }
}

// LibuvTestLoop — the shipped LibuvIoContext on a loop of its own, which a test
// drives turn by turn as it drives EpollIoContext: runOnce(timeout) runs one
// turn of the loop, which waits up to the timeout for something to do. Like
// AdversarialIoContext, it counts what relais must never do (breaches()): add
// or remove a watch inside an event dispatch, change or remove a handle it does
// not hold, call anything but post() from another thread. Its destructor
// closes the loop, which no handle relais opened may outlive.
//
// Loop-thread affinity: the thread that builds it drives it.
class LibuvTestLoop {
public:
    using WatchHandle = LibuvIoContext::WatchHandle;

    LibuvTestLoop() {
        uv_loop_init(&loop_);
        uv_timer_init(&loop_, &turn_);
        io_.emplace(&loop_);
    }

    ~LibuvTestLoop() {
        io_.reset();
        uv_close(reinterpret_cast<uv_handle_t*>(&turn_), nullptr);
        closeLoop(loop_);
    }

    LibuvTestLoop(const LibuvTestLoop&) = delete;
    LibuvTestLoop& operator=(const LibuvTestLoop&) = delete;

    // -- IoContext concept surface -------------------------------------------

    WatchHandle addWatch(int fd, IoEvent mask, std::function<void(IoEvent)> cb) {
        checkLoopThread();
        noteWatchChange();
        auto h = io_->addWatch(fd, mask, [this, cb = std::move(cb)](IoEvent ev) {
            Dispatch guard{*this};
            cb(ev);
        });
        watches_.insert(h);
        return h;
    }

    void updateWatch(WatchHandle h, IoEvent mask) {
        checkLoopThread();
        if (!watches_.contains(h)) return breach();
        io_->updateWatch(h, mask);
    }

    void removeWatch(WatchHandle h) {
        checkLoopThread();
        noteWatchChange();
        if (watches_.erase(h) == 0) return breach();
        io_->removeWatch(h);
    }

    void post(std::function<void()> cb) { io_->post(std::move(cb)); }

    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        checkLoopThread();
        armed_timers_.fetch_add(1, std::memory_order_relaxed);
        io_->postDelayed(delay, [this, cb = std::move(cb)] {
            Dispatch guard{*this};
            cb();
        });
    }

    void detached() {
        checkLoopThread();
        io_->detached();
    }

    [[nodiscard]] bool isInLoopThread() const noexcept { return io_->isInLoopThread(); }

    // -- driving and diagnostics ---------------------------------------------

    // One turn; the turn's timer keeps the loop alive and bounds its wait.
    void runOnce(int timeout_ms = 0) {
        uv_timer_start(&turn_, [](uv_timer_t*) {}, timeout_ms > 0 ? timeout_ms : 0, 0);
        uv_run(&loop_, UV_RUN_ONCE);
        uv_timer_stop(&turn_);
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
        LibuvTestLoop& loop;
        explicit Dispatch(LibuvTestLoop& l) noexcept : loop(l) { ++loop.dispatch_depth_; }
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

    uv_loop_t loop_;
    uv_timer_t turn_;
    std::optional<LibuvIoContext> io_;

    std::unordered_set<WatchHandle> watches_;
    const std::thread::id owner_ = std::this_thread::get_id();
    int dispatch_depth_ = 0;
    std::atomic<std::size_t> armed_timers_{0};
    std::atomic<std::size_t> breaches_{0};
};

}  // namespace jcailloux::relais::io::test

#endif  // JCX_RELAIS_TEST_LIBUV_TEST_LOOP_H
