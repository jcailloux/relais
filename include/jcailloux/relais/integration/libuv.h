#ifndef JCX_RELAIS_INTEGRATION_LIBUV_H
#define JCX_RELAIS_INTEGRATION_LIBUV_H

// relais on a libuv loop. Including this header needs libuv; relais itself
// never does.
//
//   jcailloux::relais::io::LibuvIoContext io(loop);          // on the loop's thread
//   auto relais = jcailloux::relais::attach(io, conninfo);   // from another thread
//
// relais binds to the thread that runs the loop, where repository calls then
// run: one thread per loop, as libuv requires. With several loops, one per
// thread, build an adapter for each and attach relais to each.

#include <uv.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/detail/DrainBuffer.h"

namespace jcailloux::relais::io {

// LibuvIoContext — the IoContext of a uv_loop_t: a watch is a uv_poll_t on
// relais's own descriptor, post() queues the callback for a uv_async_t of the
// adapter's, postDelayed() is a uv_timer_t. relais watches each connection once,
// from its connect to its close, so a request adds no handle, no posted
// callback and no timer, and no system call: libuv watches in level-triggered
// epoll, and relais changes a watch's mask only to wait for a full socket.
//
// libuv lets only the loop's thread touch the loop, uv_async_send aside: build
// the adapter on that thread (before uv_run, or from one of the loop's
// callbacks), or while no thread runs the loop; destroy it the same way, after
// relais has left the loop, and before closing the loop's handles (uv_walk).
// It closes its handles then, and libuv frees them on the loop's next turn, as
// for any handle: run the loop once more before uv_loop_close.
//
// Its handles keep the loop running as any handle does, its async handle
// aside: relais's watches while relais has connections, its timers while they
// are pending (detach drops them). Something of the application's must keep
// the loop running for attach() to find it running.
class LibuvIoContext {
public:
    class Watch {
        friend class LibuvIoContext;

        explicit Watch(std::function<void(IoEvent)> cb) noexcept : cb_(std::move(cb)) {}

        uv_poll_t poll_;
        std::function<void(IoEvent)> cb_;
        IoEvent mask_ = IoEvent::None;  // what libuv watches for
    };
    using WatchHandle = Watch*;

    explicit LibuvIoContext(uv_loop_t* loop) : loop_(loop), posted_(new Posted) {
        if (int rc = uv_async_init(loop_, &posted_->async, &runPosted); rc != 0) {
            delete posted_;
            throw std::system_error(-rc, std::generic_category(), "relais: uv_async_init");
        }
        posted_->async.data = posted_;
        uv_unref(handle(&posted_->async));
    }

    // Its timers still pending never fire, and what is still posted never runs:
    // destroyed here, while the adapter is whole.
    ~LibuvIoContext() {
        dropDelayed();
        std::vector<std::function<void()>> unrun;
        {
            std::lock_guard lock(posted_->m);
            unrun.swap(posted_->queued);
        }
        posted_->orphaned = true;
        uv_handle_t* async = handle(&posted_->async);
        if (!uv_is_closing(async)) {
            uv_close(async, [](uv_handle_t* h) { delete static_cast<Posted*>(h->data); });
        } else {
            keepForever(posted_);
        }
    }

    LibuvIoContext(const LibuvIoContext&) = delete;
    LibuvIoContext& operator=(const LibuvIoContext&) = delete;

    [[nodiscard]] uv_loop_t* loop() const noexcept { return loop_; }

    // True on the thread that ran the adapter's posted callbacks last: the
    // loop's, once it has run one.
    [[nodiscard]] bool isInLoopThread() const noexcept {
        return posted_->thread.load(std::memory_order_relaxed) == std::this_thread::get_id();
    }

    WatchHandle addWatch(int fd, IoEvent mask, std::function<void(IoEvent)> cb) {
        auto watch = std::unique_ptr<Watch>(new Watch(std::move(cb)));
        if (int rc = uv_poll_init(loop_, &watch->poll_, fd); rc != 0)
            throw std::system_error(-rc, std::generic_category(), "relais: uv_poll_init");
        watch->poll_.data = watch.get();
        // An initialized handle is the loop's until closed: freed by its close.
        if (mask != IoEvent::None) {
            if (int rc = watchFor(*watch, mask); rc != 0) {
                removeWatch(watch.release());
                throw std::system_error(-rc, std::generic_category(), "relais: uv_poll_start");
            }
        }
        return watch.release();
    }

    // libuv takes a descriptor out of its epoll set and puts it back on every
    // change of mask: two system calls, made only for a new mask.
    void updateWatch(WatchHandle watch, IoEvent mask) {
        if (mask != watch->mask_) (void)watchFor(*watch, mask);
    }

    // uv_close stops the watch at once; libuv frees the handle on its next turn.
    void removeWatch(WatchHandle watch) {
        watch->cb_ = nullptr;
        close(&watch->poll_, [](uv_handle_t* h) { delete static_cast<Watch*>(h->data); });
    }

    void post(std::function<void()> cb) {
        {
            std::lock_guard lock(posted_->m);
            posted_->queued.push_back(std::move(cb));
        }
        uv_async_send(&posted_->async);
    }

    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        auto* delayed = new Delayed(*this, std::move(cb));
        uv_timer_init(loop_, &delayed->timer_);
        delayed->timer_.data = delayed;
        uv_timer_start(&delayed->timer_, &runDelayed, timeoutFor(delay), 0);
    }

    // relais has left the loop: the delayed callbacks it left pending would do
    // nothing, and would keep uv_run from returning until they fire.
    void detached() { dropDelayed(); }

private:
    // What the async handle reaches: the posted callbacks, freed with the
    // handle, which libuv frees after the adapter is gone.
    struct Posted {
        uv_async_t async;
        std::mutex m;
        std::vector<std::function<void()>> queued;   // guarded by m
        // Swaps buffers with queued on each run, so that both keep their
        // capacity; loop thread.
        detail::DrainBuffer<std::function<void()>> running;
        std::atomic<std::thread::id> thread{};       // the last to run them
        bool orphaned = false;                       // the adapter is gone; loop thread
    };

    struct Delayed {
        Delayed(LibuvIoContext& owner, std::function<void()> cb) noexcept
            : owner_(owner), cb_(std::move(cb)), next_(owner.delayed_) {
            if (next_ != nullptr) next_->prev_ = this;
            owner.delayed_ = this;
        }
        void unlink() noexcept {
            (prev_ != nullptr ? prev_->next_ : owner_.delayed_) = next_;
            if (next_ != nullptr) next_->prev_ = prev_;
        }

        LibuvIoContext& owner_;
        uv_timer_t timer_;
        std::function<void()> cb_;
        Delayed* prev_ = nullptr;
        Delayed* next_;
    };

    template<typename H>
    static uv_handle_t* handle(H* h) noexcept {
        return reinterpret_cast<uv_handle_t*>(h);
    }

    // A handle the application already closed (uv_walk over the loop) is never
    // closed twice: libuv asserts against it.
    template<typename H>
    static void close(H* h, uv_close_cb freed) noexcept {
        if (!uv_is_closing(handle(h))) uv_close(handle(h), freed);
    }

    // The application closed the async handle itself (uv_walk over the loop):
    // libuv may hold it until a turn of the loop that may never come, or the
    // loop may be gone. Kept, still reachable, for the life of the process.
    static void keepForever(Posted* posted) {
        static std::mutex m;
        static auto* kept = new std::vector<std::unique_ptr<Posted>>;
        std::lock_guard lock(m);
        kept->emplace_back(posted);
    }

    // The callbacks below are noexcept: an exception would otherwise unwind
    // through libuv, which is C, and leave the loop's state broken.

    static void runPosted(uv_async_t* async) noexcept {
        auto& posted = *static_cast<Posted*>(async->data);
        posted.thread.store(std::this_thread::get_id(), std::memory_order_relaxed);
        detail::DrainBuffer<std::function<void()>>::Use use(posted.running);
        auto& running = use.items();
        {
            std::lock_guard lock(posted.m);
            running.swap(posted.queued);
        }
        // What these post goes to `queued`, and to the next turn. One of them
        // may destroy the adapter: the rest then never run.
        for (auto& cb : running) {
            if (posted.orphaned) break;
            cb();
        }
    }

    static int watchFor(Watch& watch, IoEvent mask) noexcept {
        watch.mask_ = mask;
        int events = 0;
        if (hasEvent(mask, IoEvent::Read)) events |= UV_READABLE;
        if (hasEvent(mask, IoEvent::Write)) events |= UV_WRITABLE;
        return uv_poll_start(&watch.poll_, events, &ready);  // 0 stops it
    }

    static void ready(uv_poll_t* poll, int status, int events) noexcept {
        auto& watch = *static_cast<Watch*>(poll->data);
        IoEvent ev = IoEvent::None;
        if (status < 0) {
            // libuv stopped the watch (POLLERR): reported once, as Error;
            // updateWatch watches again.
            watch.mask_ = IoEvent::None;
            ev = IoEvent::Error;
        } else {
            if (events & (UV_READABLE | UV_DISCONNECT | UV_PRIORITIZED)) ev |= IoEvent::Read;
            if (events & UV_WRITABLE) ev |= IoEvent::Write;
        }
        watch.cb_(ev);
    }

    // libuv starts a timer from the loop's cached time, in whole milliseconds,
    // which lags the clock by what the loop has run since it last read it: a
    // timer of `delay` would fire up to that much early. Due instead at the
    // first loop millisecond at or after now + delay, by the clock of
    // std::chrono::steady_clock: never early, and late by libuv's millisecond.
    [[nodiscard]] uint64_t timeoutFor(std::chrono::nanoseconds delay) const noexcept {
        constexpr int64_t kMs = 1'000'000;
        constexpr int64_t kLongest = std::chrono::nanoseconds(std::chrono::days(3650)).count();
        const auto wait = static_cast<uint64_t>(std::clamp<int64_t>(delay.count(), 0, kLongest));
        if (wait == 0) return 0;  // any later turn is late enough
        const uint64_t due = (uv_hrtime() + wait + (kMs - 1)) / kMs;
        const uint64_t now = uv_now(loop_);
        return due > now ? due - now : 0;
    }

    static void runDelayed(uv_timer_t* timer) noexcept {
        auto& delayed = *static_cast<Delayed*>(timer->data);
        auto cb = std::move(delayed.cb_);
        delayed.unlink();
        close(&delayed.timer_, &freeDelayed);
        cb();
    }

    static void freeDelayed(uv_handle_t* h) noexcept { delete static_cast<Delayed*>(h->data); }

    void dropDelayed() noexcept {
        while (Delayed* delayed = delayed_) {
            delayed->unlink();
            delayed->cb_ = nullptr;
            close(&delayed->timer_, &freeDelayed);
        }
    }

    uv_loop_t* loop_;
    Posted* posted_;               // freed by its handle's close
    Delayed* delayed_ = nullptr;   // the pending ones, loop thread only
};

static_assert(IoContext<LibuvIoContext>);

}  // namespace jcailloux::relais::io

#endif  // JCX_RELAIS_INTEGRATION_LIBUV_H
