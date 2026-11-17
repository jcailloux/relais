#ifndef JCX_RELAIS_INTEGRATION_DETAIL_BASIC_ASIO_H
#define JCX_RELAIS_INTEGRATION_DETAIL_BASIC_ASIO_H

// What integration/asio.h (asio) and integration/boost_asio.h (Boost.Asio)
// share, written once over `Net`, the flavour's types and functions. Include
// one of those two headers, not this one.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/runtime/Attach.h"

namespace jcailloux::relais::io {

namespace detail {

// Another thread than relais's ran one of its callbacks: relais's state belongs
// to one thread, and would be corrupted there without a sign. The message goes
// to relais's log and to stderr, which an asynchronous log may not reach in
// time. Out of line: the check that calls it costs a comparison.
[[noreturn, gnu::cold, gnu::noinline]] inline void abortOnSecondThread() noexcept {
    static constexpr char kMessage[] =
        "relais: a second thread ran one of relais's callbacks: several threads run the "
        "io_context relais is attached to, and relais needs one that a single thread runs "
        "(one io_context per thread, or io::IoPool)";
    RELAIS_LOG_ERROR << kMessage;
    std::fprintf(stderr, "%s\n", kMessage);
    std::abort();
}

}  // namespace detail

// BasicAsioIoContext — the IoContext of an asio io_context that one thread
// runs. A watch is a stream_descriptor on relais's own descriptor, released
// (never closed) by removeWatch; post() is asio::post; postDelayed() is a
// steady_timer.
//
// asio reports readiness once per async_wait, so a watch waits again after
// each callback: asio then re-arms the descriptor in its epoll set
// (EPOLL_CTL_MOD), one system call per wake-up, as for any async_wait. A
// request adds no allocation: asio recycles the memory of each wait.
//
// attach(ctx, …) refuses an io_context that several threads already run, and
// pins relais to the thread it finds running it (pinThread): a thread that
// starts running the io_context later, and runs one of relais's callbacks,
// ends the process (see detail::abortOnSecondThread). An adapter built by hand
// is pinned by no one, unless pinThread is called.
//
// The io_context must outlive what relais does on it. relais::attach(ctx, …)
// keeps one adapter per io_context, as an asio service; one built by hand is
// destroyed on the loop thread, or once the io_context has stopped.
template<typename Net>
class BasicAsioIoContext {
    using Context = typename Net::io_context;
    using Descriptor = typename Net::descriptor;
    using Timer = typename Net::timer;
    using ErrorCode = typename Net::error_code;
    using WaitType = typename Descriptor::wait_type;

public:
    class Watch {
        friend class BasicAsioIoContext;

        Watch(BasicAsioIoContext& io, int fd, IoEvent mask, std::function<void(IoEvent)> cb)
            : io_(io), descriptor_(io.ctx_, fd), mask_(mask), cb_(std::move(cb)) {}

        BasicAsioIoContext& io_;
        Descriptor descriptor_;
        IoEvent mask_;
        std::function<void(IoEvent)> cb_;
        int waits_ = 0;  // wait handlers asio has not destroyed yet
        bool reading_ = false;  // a wait for Read is pending
        bool writing_ = false;  // a wait for Write is pending
        bool dispatching_ = false;
        bool removed_ = false;
    };
    using WatchHandle = Watch*;

    explicit BasicAsioIoContext(Context& ctx) noexcept : ctx_(ctx) {}

    // A timer still pending outlives it, and then does nothing.
    ~BasicAsioIoContext() {
        for (Delayed* d = delayed_; d != nullptr; d = d->next_) {
            d->owner_ = nullptr;
            d->dropped_ = true;
            d->timer_.cancel();
        }
    }

    BasicAsioIoContext(const BasicAsioIoContext&) = delete;
    BasicAsioIoContext& operator=(const BasicAsioIoContext&) = delete;

    [[nodiscard]] Context& context() const noexcept { return ctx_; }

    // True on a thread inside the io_context's run().
    [[nodiscard]] bool isInLoopThread() const noexcept {
        return ctx_.get_executor().running_in_this_thread();
    }

    WatchHandle addWatch(int fd, IoEvent mask, std::function<void(IoEvent)> cb) {
        auto* watch = new Watch(*this, fd, mask, std::move(cb));
        try {
            wait(*watch);
        } catch (...) {
            removeWatch(watch);
            throw;
        }
        return watch;
    }

    // From the watch's own callback, waits once that returns.
    void updateWatch(WatchHandle watch, IoEvent mask) {
        watch->mask_ = mask;
        if (!watch->dispatching_) wait(*watch);
    }

    // The descriptor is released, not closed: it is relais's. The waits still
    // pending end aborted, and the last of them frees the watch.
    void removeWatch(WatchHandle watch) {
        watch->removed_ = true;
        watch->cb_ = nullptr;
        (void)watch->descriptor_.release();
        if (watch->waits_ == 0) delete watch;
    }

    void post(std::function<void()> cb) {
        Net::post(ctx_, [this, cb = std::move(cb)] {
            checkThread();
            cb();
        });
    }

    void postDelayed(std::chrono::nanoseconds delay, std::function<void()> cb) {
        auto* delayed = new Delayed(*this, delay, std::move(cb));
        delayed->timer_.async_wait([ref = DelayedRef(delayed)](const ErrorCode& ec) {
            Delayed& d = *ref.get();
            if (ec || d.dropped_) return;
            d.owner_->checkThread();
            std::exchange(d.cb_, nullptr)();
        });
    }

    // relais has left the loop: the delayed callbacks it left pending would do
    // nothing, and the io_context would wait for them before run() returns.
    void detached() {
        for (Delayed* d = delayed_; d != nullptr; d = d->next_) {
            d->dropped_ = true;
            d->timer_.cancel();
        }
    }

    // relais's callbacks run on `thread` from now on, and on no other. attach
    // pins the thread running the io_context when relais attaches to it, which
    // may change from one attach to the next.
    void pinThread(std::thread::id thread) noexcept {
        pinned_.store(thread, std::memory_order_relaxed);
    }

private:
    void checkThread() const noexcept {
        const auto pinned = pinned_.load(std::memory_order_relaxed);
        if (pinned != std::thread::id{} && pinned != std::this_thread::get_id()) [[unlikely]]
            detail::abortOnSecondThread();
    }

    // Keeps a watch alive for a wait handler, whether asio runs the handler or
    // destroys it unrun (its io_context ending): the last one frees a removed
    // watch.
    class WaitRef {
    public:
        explicit WaitRef(Watch* watch) noexcept : watch_(watch) { ++watch_->waits_; }
        WaitRef(WaitRef&& other) noexcept : watch_(std::exchange(other.watch_, nullptr)) {}
        WaitRef& operator=(WaitRef&&) = delete;
        ~WaitRef() {
            if (watch_ != nullptr && --watch_->waits_ == 0 && watch_->removed_) delete watch_;
        }
        [[nodiscard]] Watch* get() const noexcept { return watch_; }

    private:
        Watch* watch_;
    };

    // Waits for every direction of the mask not waited for yet.
    static void wait(Watch& watch) {
        if (hasEvent(watch.mask_, IoEvent::Read) && !watch.reading_)
            waitFor(watch, Descriptor::wait_read);
        if (hasEvent(watch.mask_, IoEvent::Write) && !watch.writing_)
            waitFor(watch, Descriptor::wait_write);
    }

    static void waitFor(Watch& watch, WaitType type) {
        watch.descriptor_.async_wait(type, [ref = WaitRef(&watch), type](const ErrorCode& ec) {
            ready(*ref.get(), type, ec);
        });
        (type == Descriptor::wait_read ? watch.reading_ : watch.writing_) = true;
    }

    static void ready(Watch& watch, WaitType type, const ErrorCode& ec) {
        const bool read = type == Descriptor::wait_read;
        (read ? watch.reading_ : watch.writing_) = false;
        if (watch.removed_) return;
        const IoEvent bit = read ? IoEvent::Read : IoEvent::Write;
        if (!hasEvent(watch.mask_, bit)) return;  // no longer watched for
        watch.io_.checkThread();
        watch.dispatching_ = true;
        // Only release() aborts a wait, and a removed watch returned above: an
        // error means asio cannot watch the descriptor. Reported once, not
        // waited for again.
        watch.cb_(ec ? IoEvent::Error : bit);
        watch.dispatching_ = false;
        if (watch.removed_) return;
        if (ec) {
            watch.mask_ = watch.mask_ & (read ? IoEvent::Write : IoEvent::Read);
            return;
        }
        wait(watch);
    }

    struct Delayed {
        Delayed(BasicAsioIoContext& owner, std::chrono::nanoseconds delay,
                std::function<void()> cb)
            : owner_(&owner),
              timer_(owner.ctx_, std::chrono::ceil<typename Timer::duration>(delay)),
              cb_(std::move(cb)) {
            next_ = owner.delayed_;
            if (next_ != nullptr) next_->prev_ = this;
            owner.delayed_ = this;
        }
        ~Delayed() {
            if (owner_ == nullptr) return;
            (prev_ != nullptr ? prev_->next_ : owner_->delayed_) = next_;
            if (next_ != nullptr) next_->prev_ = prev_;
        }
        Delayed(const Delayed&) = delete;
        Delayed& operator=(const Delayed&) = delete;

        BasicAsioIoContext* owner_;  // null once the adapter is gone
        Timer timer_;
        std::function<void()> cb_;
        Delayed* prev_ = nullptr;
        Delayed* next_ = nullptr;
        bool dropped_ = false;
    };

    // Frees its timer with the handler, run or not.
    class DelayedRef {
    public:
        explicit DelayedRef(Delayed* delayed) noexcept : delayed_(delayed) {}
        DelayedRef(DelayedRef&& other) noexcept : delayed_(std::exchange(other.delayed_, nullptr)) {}
        DelayedRef& operator=(DelayedRef&&) = delete;
        ~DelayedRef() { delete delayed_; }
        [[nodiscard]] Delayed* get() const noexcept { return delayed_; }

    private:
        Delayed* delayed_;
    };

    Context& ctx_;
    std::atomic<std::thread::id> pinned_{};  // none: unchecked
    Delayed* delayed_ = nullptr;  // the pending ones, loop thread only
};

}  // namespace jcailloux::relais::io

namespace jcailloux::relais::detail {

// The adapter of an io_context, which keeps it as an asio service: built by the
// first attach to that io_context, destroyed with it.
template<typename Net>
class AsioRelaisService : public Net::execution_context::service {
public:
    inline static typename Net::execution_context::id id;

    explicit AsioRelaisService(typename Net::io_context& ctx)
        : Net::execution_context::service(ctx), io_(ctx) {}

    [[nodiscard]] io::BasicAsioIoContext<Net>& io() noexcept { return io_; }

private:
    void shutdown() override {}

    io::BasicAsioIoContext<Net> io_;
};

struct AsioProbe {
    std::optional<std::string> refusal;  // what attach must refuse, if anything
    std::thread::id thread;              // the thread that ran the first callback
};

// Whether a thread besides the one running the first of two posted callbacks
// runs `ctx`: the first holds its thread for up to `window` while the second
// is posted, which another thread running ctx then takes. A single thread
// costs attach the whole window. A thread that is not inside run() yet goes
// unseen. From a thread outside ctx's run().
template<typename Net>
AsioProbe probeThreads(typename Net::io_context& ctx, std::chrono::milliseconds window) {
    struct Probe {
        std::mutex m;
        std::condition_variable cv;
        bool holding = false;
        bool ran = false;
        std::thread::id holder;
        std::thread::id runner;
    };
    auto probe = std::make_shared<Probe>();
    std::unique_lock lock(probe->m);
    Net::post(ctx, [probe, window] {
        std::unique_lock held(probe->m);
        probe->holder = std::this_thread::get_id();
        probe->holding = true;
        probe->cv.notify_all();
        probe->cv.wait_for(held, window, [&] { return probe->ran; });
    });
    if (!probe->cv.wait_for(lock, std::chrono::seconds(1), [&] { return probe->holding; })) {
        return {"a callback posted from another thread did not run within 1 s: the io_context "
                "must be running, on another thread than the one attaching relais",
                {}};
    }
    Net::post(ctx, [probe] {
        std::lock_guard ran(probe->m);
        probe->runner = std::this_thread::get_id();
        probe->ran = true;
        probe->cv.notify_all();
    });
    // Right after the first, at worst, unless the io_context stopped meanwhile.
    if (!probe->cv.wait_for(lock, window + std::chrono::seconds(1), [&] { return probe->ran; })) {
        return {"a callback posted from another thread did not run within 1 s: the io_context "
                "must keep running",
                probe->holder};
    }
    if (probe->runner == probe->holder) return {std::nullopt, probe->holder};
    return {"several threads run this io_context, and relais needs one that a single thread "
            "runs: run one io_context per thread and attach relais to each, or give relais "
            "threads of its own (io::IoPool)",
            probe->holder};
}

inline constexpr std::chrono::milliseconds kAsioProbeWindow{2};

template<typename Net>
[[nodiscard]] Attachment<io::BasicAsioIoContext<Net>>
attachAsio(typename Net::io_context& ctx, std::string conninfo, AttachConfig cfg) {
    using Io = io::BasicAsioIoContext<Net>;
    Io& io = Net::template use_service<AsioRelaisService<Net>>(ctx).io();
    refuseLoopThread(io);
    const AsioProbe probe = probeThreads<Net>(ctx, kAsioProbeWindow);
    if (probe.refusal) throw AttachError("relais cannot run on this io_context: " + *probe.refusal);
    io.pinThread(probe.thread);
    return relais::attach<Io>(io, std::move(conninfo), cfg);
}

// ---------------------------------------------------------------------------
// relais::async — a relais Task or Immediate as an asio asynchronous
// operation, which integration/asio.h and integration/boost_asio.h declare.

// What a relais awaitable yields: Task<T> and Immediate<T> yield T.
template<typename Pending>
using AwaitedOf = decltype(std::declval<Pending&>().await_resume());

// The completion signature of relais::async, as co_spawn's: the exception the
// operation ended with, if any, then its value.
template<typename T>
struct AsyncSignatureOf {
    using type = void(std::exception_ptr, T);
};

template<>
struct AsyncSignatureOf<void> {
    using type = void(std::exception_ptr);
};

template<typename T>
using AsyncSignature = typename AsyncSignatureOf<T>::type;

// What an operation completes with: on an exception, a value-initialized T.
template<typename T>
struct AsyncResult {
    std::exception_ptr error;
    T value{};
};

template<>
struct AsyncResult<void> {
    std::exception_ptr error;
};

// Awaits `pending` into a result, never throwing.
template<typename Pending>
auto settle(Pending& pending) -> AsyncResult<AwaitedOf<Pending>> {
    using T = AwaitedOf<Pending>;
    AsyncResult<T> result;
    try {
        if constexpr (std::is_void_v<T>) {
            pending.await_resume();
        } else {
            result.value = pending.await_resume();
        }
    } catch (...) {
        result.error = std::current_exception();
    }
    return result;
}

// `handler`, bound to the arguments `result` completes it with.
template<typename Net, typename Handler, typename T>
auto bindResult(Handler&& handler, AsyncResult<T>&& result) {
    if constexpr (std::is_void_v<T>) {
        return Net::append(std::forward<Handler>(handler), std::move(result.error));
    } else {
        return Net::append(std::forward<Handler>(handler), std::move(result.error),
                           std::move(result.value));
    }
}

// Completes `handler` with `result` during the initiating function, as asio's
// operations do: through the handler's immediate executor, by default a post
// to the loop.
template<typename Net, typename Handler, typename T>
void completeImmediately(Handler&& handler, const typename Net::io_context::executor_type& io_ex,
                         AsyncResult<T>&& result) {
    auto immediate = Net::immediate_executor(handler, io_ex);  // before the handler moves
    Net::dispatch(immediate, bindResult<Net>(std::forward<Handler>(handler), std::move(result)));
}

// The handler side of an operation that did not complete at once: it holds
// the handler, keeps its executor's work outstanding, and answers a
// cancellation. Lives in the operation's driver, on the loop thread.
//
// A cancellation abandons the operation: the handler completes at once with
// operation_aborted, the request runs on and its result is dropped, relais's
// cache staying valid. Honoured for the terminal and partial types: the
// request may still have its effects, so not for the total type.
template<typename Net, typename Handler, typename T>
class AsyncOperation {
    using IoExecutor = typename Net::io_context::executor_type;
    using CancellationType = typename Net::cancellation_type;
    using WorkGuard = decltype(Net::work_guard(std::declval<Handler&>(), std::declval<IoExecutor&>()));
    using Slot = decltype(Net::cancellation_slot(std::declval<Handler&>()));

public:
    AsyncOperation(Handler handler, const IoExecutor& io_ex)
        : handler_(std::move(handler)),
          io_ex_(io_ex),
          work_(Net::work_guard(handler_, io_ex_)),
          slot_(Net::cancellation_slot(handler_)) {
        if (slot_.is_connected()) slot_.template emplace<Cancellation>(this);
    }

    AsyncOperation(const AsyncOperation&) = delete;
    AsyncOperation& operator=(const AsyncOperation&) = delete;

    // Completes the handler with `result`, unless the operation was abandoned:
    // through the handler's immediate executor while the initiating function
    // runs, else as asio completes an operation of the loop (in place, from
    // the loop thread, when the handler runs on the loop).
    void complete(bool immediately, AsyncResult<T>&& result) {
        if (abandoned_) return;
        if (slot_.is_connected()) slot_.clear();
        if (immediately) {
            completeImmediately<Net>(std::move(handler_), io_ex_, std::move(result));
        } else {
            Net::dispatch(io_ex_, bindResult<Net>(std::move(handler_), std::move(result)));
        }
    }

private:
    // Sits in the handler's cancellation slot until the operation completes.
    class Cancellation {
    public:
        explicit Cancellation(AsyncOperation* op) noexcept : op_(op) {}

        void operator()(CancellationType type) {
            constexpr auto honoured = CancellationType::terminal | CancellationType::partial;
            if (op_ != nullptr && (type & honoured) != CancellationType::none)
                std::exchange(op_, nullptr)->abandon();
        }

    private:
        AsyncOperation* op_;  // null once it abandoned the operation
    };

    // Posted, as asio's own operations do: never inside the emitter's call.
    void abandon() {
        abandoned_ = true;
        AsyncResult<T> aborted{Net::aborted()};
        Net::post(io_ex_, bindResult<Net>(std::move(handler_), std::move(aborted)));
        work_.reset();
    }

    Handler handler_;
    IoExecutor io_ex_;
    WorkGuard work_;
    Slot slot_;
    bool abandoned_ = false;
};

// The coroutine that awaits a relais awaitable for an operation, frame from
// the thread's FramePool. It starts within the initiating function and runs
// until the awaitable first suspends; it ends once it has completed the
// handler, or with the request if the operation was abandoned.
struct AsyncDriver {
    struct promise_type : io::detail::PooledFrame {
        // The initiating function's, while it runs the driver: set once the
        // driver ends there.
        bool* ended_initiating = nullptr;

        AsyncDriver get_return_object() noexcept {
            return {std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        // Only an allocation failure gets here, installing the cancellation
        // handler or completing: the handler would never complete, and
        // whoever awaits it would hang.
        void unhandled_exception() noexcept { std::terminate(); }
    };

    // True if the driver ends while the initiating function runs: it ends
    // right after. Never suspends.
    struct EndingInitiation {
        bool initiating = false;

        bool await_ready() const noexcept { return false; }
        bool await_suspend(std::coroutine_handle<promise_type> h) noexcept {
            if (bool* ended = std::exchange(h.promise().ended_initiating, nullptr)) {
                *ended = true;
                initiating = true;
            }
            return false;
        }
        bool await_resume() const noexcept { return initiating; }
    };

    // From the initiating function.
    void start() && {
        bool ended = false;
        handle.promise().ended_initiating = &ended;
        handle.resume();
        if (!ended) handle.promise().ended_initiating = nullptr;
    }

    std::coroutine_handle<promise_type> handle;
};

template<typename Net, typename Handler, typename Pending>
AsyncDriver driveAsync(Pending pending, Handler handler,
                       typename Net::io_context::executor_type io_ex) {
    using T = AwaitedOf<Pending>;
    AsyncOperation<Net, Handler, T> op(std::move(handler), io_ex);
    AsyncResult<T> result;
    try {
        if constexpr (std::is_void_v<T>) {
            co_await pending;
        } else {
            result.value = co_await pending;
        }
    } catch (...) {
        result.error = std::current_exception();
    }
    op.complete(co_await AsyncDriver::EndingInitiation{}, std::move(result));
}

// The initiation of relais::async. Runs the awaitable where it is initiated:
// on the loop thread, where relais::async is called.
template<typename Net>
class AsyncInitiation {
public:
    using executor_type = typename Net::io_context::executor_type;

    explicit AsyncInitiation(const executor_type& io_ex) noexcept : io_ex_(io_ex) {}

    [[nodiscard]] executor_type get_executor() const noexcept { return io_ex_; }

    template<typename Handler, typename Pending>
    void operator()(Handler&& handler, Pending pending) const {
        if (pending.await_ready()) {  // done already, as a hit in relais's cache
            completeImmediately<Net>(std::forward<Handler>(handler), io_ex_, settle(pending));
            return;
        }
        driveAsync<Net, std::decay_t<Handler>>(std::move(pending), std::forward<Handler>(handler),
                                               io_ex_)
            .start();
    }

private:
    executor_type io_ex_;
};

template<typename Net, typename Token, typename Pending>
auto asyncOn(Pending pending, Token& token) {
    using T = AwaitedOf<Pending>;
    static_assert(std::is_void_v<T> || std::is_default_constructible_v<T>,
                  "relais::async completes with (std::exception_ptr, T), as asio::co_spawn: "
                  "T must be default-constructible");
    auto* loop = attachedLoop<io::BasicAsioIoContext<Net>>();
    if (loop == nullptr) {
        throw std::logic_error(
            "relais::async() called on a thread relais is not attached to: call it on the "
            "thread that runs an io_context relais::attach() bound");
    }
    return Net::template async_initiate<AsyncSignature<T>, Token>(
        AsyncInitiation<Net>(loop->context().get_executor()), token, std::move(pending));
}

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_INTEGRATION_DETAIL_BASIC_ASIO_H
