#ifndef JCX_RELAIS_INTEGRATION_FD_H
#define JCX_RELAIS_INTEGRATION_FD_H

// relais on any other loop, through one descriptor (Linux): relais runs a loop
// of its own, an io::EpollIoContext, which the application's loop drives.
//
//   auto relais = jcailloux::relais::attachHere(conninfo);  // on the loop's thread
//   // watch relais.fd() for reading, and call relais.poll() whenever it is readable
//
// With GLib, for one:
//
//   g_unix_fd_add(relais.fd(), G_IO_IN, [](int, GIOCondition, gpointer r) -> gboolean {
//       static_cast<jcailloux::relais::FdAttachment*>(r)->poll();
//       return G_SOURCE_CONTINUE;
//   }, &relais);
//
// relais binds the thread that calls attachHere(), where repository calls then
// run, and poll() too. The descriptor is readable while relais has work, and
// level-triggered: poll() does what is ready and returns, and the loop calls
// it again while the descriptor stays readable. Each poll() costs one
// epoll_wait; a request served from relais's cache needs none.

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/io/EpollIoContext.h"
#include "jcailloux/relais/runtime/Attach.h"

namespace jcailloux::relais {

class FdAttachment;

/// Binds relais to the calling thread, which runs another loop, through a
/// descriptor that loop watches (FdAttachment): call it on that thread, from
/// one of the loop's callbacks or before the loop starts there. Returns at
/// once; repository calls made on this thread from then on wait for the
/// pool's first connection. `on_ready`, if given, runs once from poll(): with
/// nothing once the pool has its first connections (min_connections), or with
/// the connection's error. A failure is also logged, and every PostgreSQL call
/// made on this thread, then or from then on, fails with it, until detach().
/// on_ready must not throw, nor detach or destroy the attachment; it never
/// runs once detach() has begun. Throws AttachError if relais is attached to
/// this thread already.
[[nodiscard]] inline FdAttachment attachHere(std::string conninfo, AttachConfig cfg = {},
                                             std::function<void(std::exception_ptr)> on_ready = {});

/// FdAttachment — relais on a loop of its own, which another loop drives
/// through fd(). Everything but ready() and error() runs on the thread that
/// called attachHere(). Stop watching fd() before detach() or destruction,
/// which close it.
class FdAttachment {
public:
    FdAttachment() noexcept = default;
    FdAttachment(FdAttachment&&) noexcept = default;
    FdAttachment& operator=(FdAttachment&& other) noexcept {
        if (this != &other) {
            end();
            io_ = std::move(other.io_);
            binding_ = std::move(other.binding_);
            thread_ = other.thread_;
        }
        return *this;
    }
    FdAttachment(const FdAttachment&) = delete;
    FdAttachment& operator=(const FdAttachment&) = delete;

    /// Takes relais off, as detach(); ends the process on another thread than
    /// the one that attached it, or inside poll().
    ~FdAttachment() { end(); }

    /// The descriptor to watch for reading; -1 once detached.
    [[nodiscard]] int fd() const noexcept { return io_ ? io_->fd() : -1; }

    /// Runs what relais has to do, without waiting, and leaves fd() readable
    /// if work remains. On the thread that attached relais only: throws
    /// std::logic_error elsewhere. Nothing to do once detached.
    void poll() {
        if (!io_) return;
        requireOwnThread("poll");
        ++polling_;
        struct Unwind {
            int& depth;
            ~Unwind() { --depth; }
        } unwind{polling_};
        io_->poll();
    }

    /// The pool has its first connections (min_connections).
    [[nodiscard]] bool ready() const { return binding_ && binding_->ready(); }

    /// What relais could not start with, if anything; empty once detached.
    [[nodiscard]] std::exception_ptr error() const {
        return binding_ ? binding_->error() : nullptr;
    }

    /// The connection pool; empty once detached.
    [[nodiscard]] std::shared_ptr<io::PgPool<io::EpollIoContext>> pool() const {
        return binding_ ? binding_->pool() : nullptr;
    }

    /// Takes relais off: runs relais's loop until the requests in flight end
    /// (each bounded by its timeouts), unbinds this thread, and closes the
    /// pool and fd(). Blocks until done. On the thread that attached relais,
    /// outside poll(): throws std::logic_error otherwise. Idempotent.
    void detach() {
        if (!binding_) return;
        requireOwnThread("detach");
        if (polling_ > 0)
            throw std::logic_error("relais: FdAttachment::detach() called from inside poll(): "
                                   "call it once poll() has returned");
        teardown();
    }

private:
    friend FdAttachment attachHere(std::string conninfo, AttachConfig cfg,
                                   std::function<void(std::exception_ptr)> on_ready);
    using Binding = detail::LoopBinding<io::EpollIoContext>;

    void requireOwnThread(const char* what) const {
        if (std::this_thread::get_id() != thread_)
            throw std::logic_error(std::string("relais: FdAttachment::") + what
                                   + "() called on another thread than the one that called "
                                     "attachHere(): relais runs on that one only");
    }

    // detach() for the destructor and the move assignment, which cannot throw.
    void end() noexcept {
        if (!binding_) return;
        if (std::this_thread::get_id() != thread_)
            abortWith("relais: an FdAttachment was destroyed on another thread than the one "
                      "that called attachHere(): relais runs on that one only");
        if (polling_ > 0)
            abortWith("relais: an FdAttachment was destroyed from inside its own poll()");
        teardown();
    }

    // This thread is the loop's: it runs the loop until the teardown ends.
    void teardown() noexcept {
        auto binding = std::move(binding_);
        binding->beginRelease();
        const auto until = std::chrono::steady_clock::now() + binding->teardownBound();
        while (!binding->released() && std::chrono::steady_clock::now() < until) io_->runOnce(10);
        if (!binding->released()) {
            // Never seen: the teardown ends within its bound. What relais
            // holds stays, with the loop it needs.
            RELAIS_LOG_WARN << "relais: the teardown did not end in time: its loop stays open";
            binding->keepForever();
            (void)io_.release();
            return;
        }
        io_.reset();
    }

    [[noreturn, gnu::cold]] static void abortWith(const char* message) noexcept {
        RELAIS_LOG_ERROR << message;
        std::fprintf(stderr, "%s\n", message);
        std::abort();
    }

    std::unique_ptr<io::EpollIoContext> io_;
    std::shared_ptr<Binding> binding_;
    std::thread::id thread_;
    int polling_ = 0;  // depth of poll() calls in progress
};

inline FdAttachment attachHere(std::string conninfo, AttachConfig cfg,
                               std::function<void(std::exception_ptr)> on_ready) {
    FdAttachment attachment;
    attachment.io_ = std::make_unique<io::EpollIoContext>();
    attachment.binding_ = FdAttachment::Binding::startHere(*attachment.io_, std::move(conninfo), cfg,
                                                           FdAttachment::Binding::Check::None,
                                                           std::move(on_ready));
    attachment.thread_ = std::this_thread::get_id();
    return attachment;
}

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INTEGRATION_FD_H
