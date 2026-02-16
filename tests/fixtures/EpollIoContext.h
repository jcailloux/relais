#ifndef JCX_RELAIS_IO_TEST_EPOLL_IO_CONTEXT_H
#define JCX_RELAIS_IO_TEST_EPOLL_IO_CONTEXT_H

#include <jcailloux/relais/io/IoContext.h>

#include <cassert>
#include <cstring>
#include <deque>
#include <functional>
#include <stdexcept>
#include <unordered_map>

#include <sys/epoll.h>
#include <unistd.h>

namespace jcailloux::relais::io::test {

// EpollIoContext — minimal epoll-based event loop for integration tests

class EpollIoContext {
public:
    using WatchHandle = int;

    EpollIoContext() {
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0)
            throw std::runtime_error("epoll_create1 failed");
    }

    ~EpollIoContext() {
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

    void post(std::function<void()> cb) {
        posted_.push_back(std::move(cb));
    }

    void run() {
        while (!watches_.empty() || !posted_.empty()) {
            runOnce(100);
        }
    }

    template<typename Pred>
    void runUntil(Pred&& pred) {
        while (!pred()) {
            runOnce(50);
        }
    }

    void runOnce(int timeout_ms = 0) {
        while (!posted_.empty()) {
            auto cb = std::move(posted_.front());
            posted_.pop_front();
            cb();
        }

        if (watches_.empty()) return;

        static constexpr int MAX_EVENTS = 16;
        epoll_event events[MAX_EVENTS];
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout_ms);

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            auto it = watches_.find(fd);
            if (it != watches_.end()) {
                auto io_events = fromEpoll(events[i].events);
                it->second.callback(io_events);
            }
        }

        while (!posted_.empty()) {
            auto cb = std::move(posted_.front());
            posted_.pop_front();
            cb();
        }
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

    int epoll_fd_ = -1;
    std::unordered_map<int, WatchEntry> watches_;
    std::deque<std::function<void()>> posted_;
};

static_assert(IoContext<EpollIoContext>);

} // namespace jcailloux::relais::io::test

#endif // JCX_RELAIS_IO_TEST_EPOLL_IO_CONTEXT_H
