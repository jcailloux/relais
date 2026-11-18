#ifndef JCX_RELAIS_INTEGRATION_TRANTOR_H
#define JCX_RELAIS_INTEGRATION_TRANTOR_H

// relais on a trantor::EventLoop, the loop Drogon runs. Including this header
// needs trantor; relais itself never does.
//
//   jcailloux::relais::io::TrantorIoContext io(loop);
//   auto relais = jcailloux::relais::attach(io, conninfo);  // from another thread
//   auto relais = jcailloux::relais::attachHere(io, conninfo);  // or, on its thread
//
// On Drogon, integration/drogon.h does this for every IO loop.

#include <poll.h>

#include <chrono>
#include <functional>
#include <utility>

#include <trantor/net/Channel.h>
#include <trantor/net/EventLoop.h>

#include "jcailloux/relais/io/IoContext.h"

namespace jcailloux::relais::io {

// TrantorIoContext — the IoContext of a trantor::EventLoop, which must outlive
// it: a watch is a trantor::Channel, post() is queueInLoop, postDelayed() is
// runAfter. relais watches each connection once, from its connect to its close,
// so a request adds no Channel, no posted callback and no timer.
class TrantorIoContext {
public:
    class Watch {
        friend class TrantorIoContext;

        Watch(::trantor::EventLoop* loop, int fd, std::function<void(IoEvent)> cb)
            : channel_(loop, fd), cb_(std::move(cb)) {
            // One call per event, with all its bits.
            channel_.setEventCallback([this] { cb_(fromTrantor(channel_.revents())); });
        }

        ::trantor::Channel channel_;
        std::function<void(IoEvent)> cb_;
    };
    using WatchHandle = Watch*;

    explicit TrantorIoContext(::trantor::EventLoop* loop) noexcept : loop_(loop) {}

    TrantorIoContext(const TrantorIoContext&) = delete;
    TrantorIoContext& operator=(const TrantorIoContext&) = delete;

    [[nodiscard]] ::trantor::EventLoop* loop() const noexcept { return loop_; }

    [[nodiscard]] bool isInLoopThread() const noexcept { return loop_->isInLoopThread(); }

    WatchHandle addWatch(int fd, IoEvent mask, std::function<void(IoEvent)> cb) {
        auto* watch = new Watch(loop_, fd, std::move(cb));
        watch->channel_.updateEvents(toTrantor(mask));
        return watch;
    }

    void updateWatch(WatchHandle watch, IoEvent mask) {
        watch->channel_.updateEvents(toTrantor(mask));
    }

    // Frees the watch at once: relais never removes one inside an event
    // dispatch, so trantor holds no pointer to its Channel.
    void removeWatch(WatchHandle watch) {
        watch->channel_.disableAll();
        watch->channel_.remove();
        delete watch;
    }

    // queueInLoop, not runInLoop: on the loop thread, runInLoop runs the
    // callback at once, before the one posting it has returned.
    void post(std::function<void()> cb) { loop_->queueInLoop(std::move(cb)); }

    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        loop_->runAfter(std::chrono::duration<double>(delay).count(), std::move(cb));
    }

private:
    static int toTrantor(IoEvent mask) noexcept {
        int events = ::trantor::Channel::kNoneEvent;
        if (hasEvent(mask, IoEvent::Read)) events |= ::trantor::Channel::kReadEvent;
        if (hasEvent(mask, IoEvent::Write)) events |= ::trantor::Channel::kWriteEvent;
        return events;
    }

    // trantor reports poll(2) bits, whatever its poller.
    static IoEvent fromTrantor(int revents) noexcept {
        IoEvent ev = IoEvent::None;
        if (revents & (POLLIN | POLLPRI)) ev |= IoEvent::Read;
        if (revents & POLLOUT) ev |= IoEvent::Write;
        if (revents & (POLLERR | POLLHUP | POLLNVAL)) ev |= IoEvent::Error;
        return ev;
    }

    ::trantor::EventLoop* loop_;
};

static_assert(IoContext<TrantorIoContext>);

}  // namespace jcailloux::relais::io

#endif  // JCX_RELAIS_INTEGRATION_TRANTOR_H
