#ifndef JCX_RELAIS_TEST_ADVERSARIAL_IO_CONTEXT_H
#define JCX_RELAIS_TEST_ADVERSARIAL_IO_CONTEXT_H

#include <jcailloux/relais/io/EpollIoContext.h>
#include <jcailloux/relais/io/IoContext.h>

#include <sys/stat.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace jcailloux::relais::io::test {

// What an AdversarialIoContext does on top of the bare contract.
enum Adversity : unsigned {
    kNone           = 0,
    // One event dispatched as one callback per bit (Read, then Write, then
    // Error), through the same watch, not looked up again in between: a
    // trantor Channel with separate read and write callbacks.
    kSplitEvents    = 1u << 0,
    // Watches woken for nothing: every event delivered twice, and every watch
    // called with its whole mask right after it is added or changed, then
    // every millisecond. Far noisier than any real loop.
    kSpurious       = 1u << 1,
    // Timers fire on the next 10 ms boundary after their delay, never on it.
    kCoarseTimers   = 1u << 2,
    kAll            = kSplitEvents | kSpurious | kCoarseTimers,
};

// AdversarialIoContext — an IoContext that takes every latitude the contract
// leaves an adapter, and counts every breach of what relais promises it in
// return. relais must work on it as on EpollIoContext, which it wraps.
//
// In every variant:
//   - a watch's callback runs in place, never copied, and removeWatch
//     destroys it at once: freeing synchronously is allowed, because relais
//     adds and removes watches outside any event dispatch;
//   - no cancelTimer, and postDelayed returns nothing;
//   - breaches() counts what relais must never do: add or remove a watch
//     inside an event dispatch (a watch or timer callback), change or remove a
//     handle it does not hold, close or replace a watched descriptor before its
//     removeWatch, call anything but post() from another thread.
//
// Loop-thread affinity: the thread that builds it drives it.
template<unsigned H>
class AdversarialIoContext {
public:
    using WatchHandle = uint64_t;

    AdversarialIoContext() {
        if constexpr ((H & kSpurious) != 0) armSpuriousTick();
    }
    AdversarialIoContext(const AdversarialIoContext&) = delete;
    AdversarialIoContext& operator=(const AdversarialIoContext&) = delete;

    // -- IoContext concept surface -------------------------------------------

    WatchHandle addWatch(int fd, IoEvent mask, std::function<void(IoEvent)> cb) {
        checkLoopThread();
        noteWatchChange();
        auto w = std::make_unique<Watch>();
        w->fd = fd;
        w->mask = mask;
        w->cb = std::move(cb);
        struct stat st{};
        if (::fstat(fd, &st) == 0) {
            w->dev = st.st_dev;
            w->ino = st.st_ino;
        } else {
            breach();  // not an open descriptor
        }
        const WatchHandle h = ++next_handle_;
        w->inner = inner_.addWatch(fd, mask, [this, h](IoEvent ev) {
            dispatch(h, ev);
            if constexpr ((H & kSpurious) != 0) dispatch(h, ev);  // again, for nothing
        });
        watches_.emplace(h, std::move(w));
        if constexpr ((H & kSpurious) != 0) postSpurious(h);
        return h;
    }

    void updateWatch(WatchHandle h, IoEvent mask) {
        checkLoopThread();
        auto it = watches_.find(h);
        if (it == watches_.end()) {
            breach();
            return;
        }
        it->second->mask = mask;
        inner_.updateWatch(it->second->inner, mask);
        if constexpr ((H & kSpurious) != 0) postSpurious(h);
    }

    void removeWatch(WatchHandle h) {
        checkLoopThread();
        noteWatchChange();
        auto it = watches_.find(h);
        if (it == watches_.end()) {
            breach();
            return;
        }
        if (!stillOpen(*it->second)) breach();
        inner_.removeWatch(it->second->inner);
        watches_.erase(it);  // the callback dies here, even mid-call
    }

    void post(std::function<void()> cb) { inner_.post(std::move(cb)); }

    template<typename Rep, typename Period>
    void postDelayed(std::chrono::duration<Rep, Period> delay, std::function<void()> cb) {
        checkLoopThread();
        armed_timers_.fetch_add(1, std::memory_order_relaxed);
        auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
        if constexpr ((H & kCoarseTimers) != 0)
            d = (d / kCoarseTick + 1) * kCoarseTick;
        inner_.postDelayed(d, [this, cb = std::move(cb)] {
            Dispatch guard{*this};
            cb();
        });
    }

    // -- driving and diagnostics ---------------------------------------------

    void runOnce(int timeout_ms = 0) { inner_.runOnce(timeout_ms); }

    template<typename Pred>
    void runUntil(Pred&& pred) { inner_.runUntil(std::forward<Pred>(pred)); }

    /// Every breach of relais's side of the contract so far.
    [[nodiscard]] std::size_t breaches() const noexcept {
        return breaches_.load(std::memory_order_relaxed);
    }
    /// Watch callbacks called for nothing, split events not counted.
    [[nodiscard]] std::size_t spuriousCalls() const noexcept { return spurious_calls_; }
    /// Events dispatched as more than one callback.
    [[nodiscard]] std::size_t splitEvents() const noexcept { return split_events_; }
    [[nodiscard]] std::size_t armedTimers() const noexcept {
        return armed_timers_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::size_t liveWatches() const noexcept { return watches_.size(); }

private:
    static constexpr auto kCoarseTick = std::chrono::milliseconds(10);
    static constexpr auto kSpuriousTick = std::chrono::milliseconds(1);

    struct Watch {
        int fd = -1;
        IoEvent mask = IoEvent::None;
        std::function<void(IoEvent)> cb;
        EpollIoContext::WatchHandle inner{};
        dev_t dev{};
        ino_t ino{};
    };

    struct Dispatch {
        AdversarialIoContext& io;
        explicit Dispatch(AdversarialIoContext& c) noexcept : io(c) { ++io.dispatch_depth_; }
        ~Dispatch() { --io.dispatch_depth_; }
        Dispatch(const Dispatch&) = delete;
        Dispatch& operator=(const Dispatch&) = delete;
    };

    void dispatch(WatchHandle h, IoEvent ev) {
        auto it = watches_.find(h);
        if (it == watches_.end()) return;  // removed earlier in this turn
        Watch* w = it->second.get();
        Dispatch guard{*this};
        if constexpr ((H & kSplitEvents) != 0) {
            int calls = 0;
            for (IoEvent bit : {IoEvent::Read, IoEvent::Write, IoEvent::Error}) {
                if (!hasEvent(ev, bit)) continue;
                ++calls;
                w->cb(bit);  // through `w`: a removal in the call before is not seen
            }
            if (calls > 1) ++split_events_;
        } else {
            w->cb(ev);
        }
    }

    void spurious(WatchHandle h) {
        auto it = watches_.find(h);
        if (it == watches_.end()) return;
        ++spurious_calls_;
        dispatch(h, it->second->mask);
    }

    void postSpurious(WatchHandle h) {
        inner_.post([this, h] { spurious(h); });
    }

    void armSpuriousTick() {
        inner_.postDelayed(kSpuriousTick, [this] {
            std::vector<WatchHandle> handles;
            handles.reserve(watches_.size());
            for (const auto& [h, w] : watches_) handles.push_back(h);
            for (WatchHandle h : handles) spurious(h);
            armSpuriousTick();
        });
    }

    // The descriptor still names the file it named when watched.
    static bool stillOpen(const Watch& w) {
        struct stat st{};
        return ::fstat(w.fd, &st) == 0 && st.st_dev == w.dev && st.st_ino == w.ino;
    }

    void checkLoopThread() {
        if (std::this_thread::get_id() != owner_) breach();
    }

    void noteWatchChange() {
        if (dispatch_depth_ > 0) breach();
    }

    void breach() noexcept { breaches_.fetch_add(1, std::memory_order_relaxed); }

    EpollIoContext inner_;
    std::unordered_map<WatchHandle, std::unique_ptr<Watch>> watches_;
    WatchHandle next_handle_ = 0;
    const std::thread::id owner_ = std::this_thread::get_id();
    int dispatch_depth_ = 0;
    std::size_t spurious_calls_ = 0;
    std::size_t split_events_ = 0;
    std::atomic<std::size_t> armed_timers_{0};
    std::atomic<std::size_t> breaches_{0};
};

}  // namespace jcailloux::relais::io::test

#endif  // JCX_RELAIS_TEST_ADVERSARIAL_IO_CONTEXT_H
