#ifndef JCX_RELAIS_IO_LOOP_SELF_CHECK_H
#define JCX_RELAIS_IO_LOOP_SELF_CHECK_H

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "jcailloux/relais/io/IoContext.h"

namespace jcailloux::relais::io::detail {

// LoopSelfCheck — a few milliseconds spent on a loop, before relais binds to
// it, doing with a socket pair what relais does with a connection, and checking
// the loop answers as the IoContext contract says:
//   - two callbacks posted from another thread run in order, on another thread,
//     and wake the loop;
//   - a watched socket that turns readable is reported, on that same thread;
//   - a callback posted from a watch callback runs after it returns;
//   - updateWatch from a watch callback adds Write, which is reported;
//   - from a posted task, removeWatch, a new file under the same descriptor
//     number and a new watch on it: the new watch is called;
//   - a removed watch is never called again (its socket turns readable, and a
//     postDelayed callback closes the check a little later).
//
// Each step is bounded by one deadline, kTimeout from the start: a step still
// awaited then fails, naming what the loop did not do. The check never adds or
// removes a watch inside an event dispatch, and lets go of its socket pairs on
// the loop even when it fails, through a posted task (a timer, when post() runs
// in place); a loop that never runs that task keeps them.
template<IoContext Io>
class LoopSelfCheck : public std::enable_shared_from_this<LoopSelfCheck<Io>> {
    using Clock = std::chrono::steady_clock;
    using Handle = typename Io::WatchHandle;
    struct Tag {};

public:
    static constexpr std::chrono::seconds kTimeout{1};

    LoopSelfCheck(Tag, Io& io) : io_(io) {}

    // Starts the check on io's loop. Called from another thread than the loop's.
    static std::shared_ptr<LoopSelfCheck> start(Io& io) {
        auto check = std::make_shared<LoopSelfCheck>(Tag{}, io);
        check->caller_ = std::this_thread::get_id();
        check->deadline_ = Clock::now() + kTimeout;
        io.post([check] { check->firstPost(); });
        io.post([check] { check->secondPost(); });
        return check;
    }

    // Blocks until the check ends: nothing if the loop passed it, else what the
    // loop failed to do.
    std::optional<std::string> wait() {
        std::unique_lock lock(m_);
        if (!cv_.wait_until(lock, deadline_, [this] { return done_; })) {
            done_ = true;
            failure_ = stalled(step_);
            lock.unlock();
            io_.post([self = this->shared_from_this()] { self->letGo(); });
            lock.lock();
        }
        if (failure_.empty()) return std::nullopt;
        return failure_;
    }

private:
    enum class Step { Post, Readable, Writable, Rewatch, Quiet };

    static const char* stalled(Step step) noexcept {
        switch (step) {
        case Step::Post:
            return "a callback posted from another thread did not run within 1 s: the loop must "
                   "be running, on another thread than the one attaching relais, and post() "
                   "must wake it";
        case Step::Readable:
            return "a watched socket that turned readable was not reported (addWatch, Read)";
        case Step::Writable:
            return "a writable socket was not reported after updateWatch added Write";
        case Step::Rewatch:
            return "a new watch on a reused descriptor number was not called: removeWatch must "
                   "take effect when it returns, and not affect a later addWatch of that number";
        case Step::Quiet:
            return "a postDelayed callback did not run";
        }
        return "";
    }

    // -- loop thread ----------------------------------------------------------

    void firstPost() {
        if (finished()) return;
        loop_ = std::this_thread::get_id();
        if (loop_ == caller_)
            return fail("post() ran the callback on the thread that posted it, not on the "
                        "loop's: it must queue it for the loop");
        first_ran_ = true;
    }

    void secondPost() {
        if (finished()) return;
        if (!first_ran_) return fail("posted callbacks ran out of order");
        if (!onLoop())
            return fail("posted callbacks ran on different threads: relais needs a loop that "
                        "one thread runs");
        if (!openPair(a_, b_)) return;
        advance(Step::Readable);
        if (!watch(1)) return;
        poke(b_);
    }

    void onEvent(int id, IoEvent ev) {
        if (finished()) return;
        if (!onLoop()) return fail("a watch callback ran off the loop thread");
        if (id != watched_) return fail("a watch was called after its removeWatch returned");
        const Step step = currentStep();
        if (id == 1 && step == Step::Readable) {
            if (!hasEvent(ev, IoEvent::Read | IoEvent::Error) || !drain(a_)) return;
            in_callback_ = true;
            io_.post([self = this->shared_from_this()] { self->postedFromCallback(); });
            if (finished()) {  // it ran in place and failed: the watch is left alone
                in_callback_ = false;
                return;
            }
            advance(Step::Writable);
            io_.updateWatch(*handle_, IoEvent::Read | IoEvent::Write);
            in_callback_ = false;
        } else if (id == 1 && step == Step::Writable) {
            if (!hasEvent(ev, IoEvent::Write)) return;
            io_.updateWatch(*handle_, IoEvent::Read);
            advance(Step::Rewatch);
            io_.post([self = this->shared_from_this()] { self->rewatch(); });
        } else if (id == 2 && step == Step::Rewatch) {
            if (!hasEvent(ev, IoEvent::Read | IoEvent::Error) || !drain(a_)) return;
            advance(Step::Quiet);
            io_.post([self = this->shared_from_this()] { self->unwatch(); });
        }
        // Anything else is a call for nothing, which the contract allows.
    }

    void postedFromCallback() {
        if (finished()) return;
        // A posted letGo would run in place too, and remove the watch inside its
        // own callback: a timer lets go of it once the callback has returned.
        if (in_callback_)
            return fail("a callback posted from a watch callback ran before that callback "
                        "returned: post() must queue it, never run it in place",
                        LetGo::AfterDelay);
        posted_ran_ = true;
    }

    // What a connection step does: unwatch the socket, put a new file under its
    // number, watch it again — in one posted task.
    void rewatch() {
        if (finished()) return;
        if (!posted_ran_) return fail("posted callbacks ran out of order");
        unwatchNow();
        int c = -1;
        if (!openPair(c, b2_)) return;
        // dup2 makes a_ name c's socket and closes the first one, atomically.
        const bool moved = ::dup2(c, a_) >= 0 && ::fcntl(a_, F_SETFD, FD_CLOEXEC) == 0;
        const int saved = errno;
        ::close(c);
        if (!moved) return fail(std::string("could not reuse a descriptor number: ") + std::strerror(saved));
        ::close(b_);
        b_ = -1;
        if (!watch(2)) return;
        poke(b2_);
    }

    void unwatch() {
        if (finished()) return;
        unwatchNow();
        poke(b2_);  // readable again: the removed watch must stay silent
        io_.postDelayed(std::chrono::milliseconds(2),
                        [self = this->shared_from_this()] { self->quietEnd(); });
    }

    void quietEnd() {
        if (finished()) return;
        if (!onLoop()) return fail("a postDelayed callback ran off the loop thread");
        closeAll();
        std::lock_guard lock(m_);
        done_ = true;
        cv_.notify_all();
    }

    // Lets go of what a failed check holds: posted, so never inside a dispatch.
    void letGo() {
        unwatchNow();
        closeAll();
    }

    enum class LetGo { Posted, AfterDelay };

    void fail(std::string what, LetGo how = LetGo::Posted) {
        {
            std::lock_guard lock(m_);
            if (done_) return;
            done_ = true;
            failure_ = std::move(what);
            cv_.notify_all();
        }
        auto let_go = [self = this->shared_from_this()] { self->letGo(); };
        if (how == LetGo::Posted) {
            io_.post(std::move(let_go));
        } else {
            io_.postDelayed(std::chrono::milliseconds(1), std::move(let_go));
        }
    }

    bool watch(int id) {
        try {
            handle_ = io_.addWatch(a_, IoEvent::Read,
                [self = this->shared_from_this(), id](IoEvent ev) { self->onEvent(id, ev); });
        } catch (const std::exception& e) {
            fail(std::string("addWatch failed on a socket: ") + e.what());
            return false;
        } catch (...) {
            fail("addWatch failed on a socket");
            return false;
        }
        watched_ = id;
        return true;
    }

    void unwatchNow() {
        if (!handle_) return;
        io_.removeWatch(*handle_);
        handle_.reset();
        watched_ = 0;
    }

    bool openPair(int& x, int& y) {
        int fds[2];
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            fail(std::string("could not open a socket pair: ") + std::strerror(errno));
            return false;
        }
        for (int fd : fds) {
            ::fcntl(fd, F_SETFD, FD_CLOEXEC);
            ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
        }
        x = fds[0];
        y = fds[1];
        return true;
    }

    static void poke(int fd) noexcept {
        const char byte = 1;
        [[maybe_unused]] auto n = ::write(fd, &byte, 1);
    }

    // True if something was there to read.
    static bool drain(int fd) noexcept {
        char buf[16];
        bool read_any = false;
        while (::read(fd, buf, sizeof buf) > 0) read_any = true;
        return read_any;
    }

    void closeAll() noexcept {
        for (int* fd : {&a_, &b_, &b2_}) {
            if (*fd >= 0) ::close(*fd);
            *fd = -1;
        }
    }

    bool onLoop() const noexcept { return std::this_thread::get_id() == loop_; }

    bool finished() {
        std::lock_guard lock(m_);
        return done_;
    }

    Step currentStep() {
        std::lock_guard lock(m_);
        return step_;
    }

    void advance(Step step) {
        std::lock_guard lock(m_);
        step_ = step;
    }

    Io& io_;
    std::thread::id caller_;
    Clock::time_point deadline_;

    // Shared with the waiting thread.
    std::mutex m_;
    std::condition_variable cv_;
    bool done_ = false;
    Step step_ = Step::Post;
    std::string failure_;

    // Loop thread only.
    std::thread::id loop_;
    bool first_ran_ = false;
    bool in_callback_ = false;
    bool posted_ran_ = false;
    std::optional<Handle> handle_;
    int watched_ = 0;  // id of the live watch, 0 for none
    int a_ = -1, b_ = -1, b2_ = -1;
};

}  // namespace jcailloux::relais::io::detail

#endif  // JCX_RELAIS_IO_LOOP_SELF_CHECK_H
