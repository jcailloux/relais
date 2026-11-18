#ifndef JCX_RELAIS_RUNTIME_ATTACH_H
#define JCX_RELAIS_RUNTIME_ATTACH_H

#include <algorithm>
#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <coroutine>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/PgProvider.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/LoopSelfCheck.h"
#include "jcailloux/relais/io/batch/BatchScheduler.h"
#include "jcailloux/relais/io/pg/PgPool.h"

namespace jcailloux::relais {

// What attach() sets up on a loop: its connection pool, and the sends it keeps
// in flight (as IoPoolConfig::max_concurrent_per_worker).
struct AttachConfig {
    io::PgPoolConfig pool{};
    int max_concurrent = 8;
};

// Thrown by attach() when relais cannot be bound to a loop: the loop failed the
// check run on it first (the message says what it did not do), attach() was
// called on the loop's own thread, or relais is already attached to that loop.
// attachHere() throws it for a thread relais is already attached to, and gives
// it to on_ready for a loop that fails the check.
struct AttachError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

namespace detail {

// The LoopBinding whose providers this thread holds, if any.
inline const void*& attachedHere() noexcept {
    thread_local const void* binding = nullptr;
    return binding;
}

// The loop relais is attached to on this thread, if it is an Io: set once
// relais is bound there, until its teardown.
template<io::IoContext Io>
Io*& attachedLoop() noexcept {
    thread_local Io* loop = nullptr;
    return loop;
}

// Ends the process: relais's state belongs to a thread the loop does not run
// relais on. The message goes to relais's log and to stderr, which an
// asynchronous log may not reach in time.
[[noreturn, gnu::cold, gnu::noinline]] inline void abortOffLoop(const std::string& message) noexcept {
    RELAIS_LOG_ERROR << message;
    std::fprintf(stderr, "%s\n", message.c_str());
    std::abort();
}

inline constexpr char kAttachedOffLoop[] =
    "relais: relais::attachHere() was called on another thread than the one that runs the "
    "loop: call it on that thread, from one of the loop's callbacks or before the loop "
    "starts there";

inline std::string describe(const std::exception_ptr& error) {
    try {
        std::rethrow_exception(error);
    } catch (const std::exception& e) {
        return e.what();
    } catch (...) {
        return "an exception of unknown type";
    }
}

// LoopBinding — relais bound to one loop: the check of the loop, the pool and
// the scheduler created on it, the loop thread's providers bound to them, the
// pool's first connections, and the ordered teardown.
//
// attach() posts each phase onto the loop and awaits it from another thread,
// so that IoPool binds all its loops at once. attachHere() binds the calling
// thread at once, the loop's: what remains runs from the loop's first turn,
// with no one waiting, and on_ready tells how it ended.
//
// What relais creates on the loop is destroyed there: by the teardown, which
// the loop must run. A loop that does not run it keeps it for good (see
// keepForever).
template<io::IoContext Io>
class LoopBinding : public std::enable_shared_from_this<LoopBinding<Io>> {
    using Clock = std::chrono::steady_clock;
    using Pool = io::PgPool<Io>;
    using Scheduler = io::batch::detail::BatchScheduler<Io>;
    using SelfCheck = io::detail::LoopSelfCheck<Io>;
    struct Tag {};

public:
    using OnReady = std::function<void(std::exception_ptr)>;

    // How the loop is checked before the pool connects.
    enum class Check {
        Caller,  // by the thread calling attach(), which waits for it
        Aside,   // from a thread of relais's own, for attachHere()
        None,    // not at all: relais's own loop (the descriptor of integration/fd.h)
    };

    LoopBinding(Tag, Io& io, std::string conninfo, AttachConfig cfg, Check check, OnReady on_ready)
        : io_(io), conninfo_(std::move(conninfo)), cfg_(cfg), check_(check),
          on_ready_(std::move(on_ready)) {}

    LoopBinding(const LoopBinding&) = delete;
    LoopBinding& operator=(const LoopBinding&) = delete;

    // A teardown runs the check thread's end: only a binding whose loop never
    // ran its teardown still has it, and waits for it here.
    ~LoopBinding() {
        if (checker_.joinable()) checker_.join();
    }

    // attach(): starts checking the loop. From another thread than the loop's.
    static std::shared_ptr<LoopBinding> start(Io& io, std::string conninfo, AttachConfig cfg) {
        auto binding = std::make_shared<LoopBinding>(Tag{}, io, std::move(conninfo), cfg,
                                                     Check::Caller, OnReady{});
        binding->caller_check_ = SelfCheck::start(io);
        return binding;
    }

    // attachHere(): binds the calling thread, the loop's, at once; the loop's
    // first turn goes on. Throws AttachError for a thread relais is attached to.
    static std::shared_ptr<LoopBinding> startHere(Io& io, std::string conninfo, AttachConfig cfg,
                                                  Check check, OnReady on_ready) {
        auto binding = std::make_shared<LoopBinding>(Tag{}, io, std::move(conninfo), cfg, check,
                                                     std::move(on_ready));
        binding->bind();
        try {
            io.post([binding] { binding->firstTurn(); });
        } catch (...) {
            binding->unbind();
            throw;
        }
        return binding;
    }

    // Waits for the check; throws AttachError saying what the loop failed.
    void awaitCheck() {
        auto failure = caller_check_->wait();
        caller_check_.reset();
        if (failure) throw AttachError("relais cannot run on this loop: " + *failure);
    }

    // Binds the loop thread's providers and warms the pool up, on the loop.
    void startBoot() {
        io_.post([self = this->shared_from_this()] { boot(self, Bind::Now); });
    }

    // Waits for the boot, until `deadline` if given: false once it passes.
    // Rethrows what failed the boot.
    bool awaitBoot(std::optional<Clock::time_point> deadline = std::nullopt) {
        std::unique_lock lock(m_);
        auto booted = [this] { return booted_; };
        if (!deadline) {
            cv_.wait(lock, booted);
        } else if (!cv_.wait_until(lock, *deadline, booted)) {
            return false;
        }
        if (boot_error_) std::rethrow_exception(boot_error_);
        return true;
    }

    // Set once the loop thread is bound, until the teardown.
    [[nodiscard]] const std::shared_ptr<Pool>& pool() const noexcept { return pool_; }

    // The pool connected its first connections.
    [[nodiscard]] bool ready() const {
        std::lock_guard lock(m_);
        return booted_ && !boot_error_;
    }

    // What ended the boot in failure, if it did.
    [[nodiscard]] std::exception_ptr error() const {
        std::lock_guard lock(m_);
        return boot_error_;
    }

    // True on the loop's own thread: the one relais bound, once it has, or
    // what the loop says, when it can tell.
    [[nodiscard]] bool onLoopThread() const noexcept {
        const auto bound = loop_thread_.load(std::memory_order_relaxed);
        if (bound != std::thread::id{}) return bound == std::this_thread::get_id();
        if constexpr (requires(Io& i) { { i.isInLoopThread() } -> std::convertible_to<bool>; })
            return io_.isInLoopThread();
        return false;
    }

    // Posts the ordered teardown, whatever the phase reached: a boot still
    // running is ended, a bound loop drains, then everything is destroyed on
    // the loop. Idempotent.
    void beginRelease() {
        {
            std::lock_guard lock(m_);
            if (release_ != Release::None) return;
            release_ = Release::Posted;
        }
        io_.post([self = this->shared_from_this()] { self->release(); });
    }

    // Waits for the teardown: true once done. A loop that does not start it
    // within kReleaseStart (stopped, or stuck) keeps what relais holds there
    // for good; false then, and false if it runs past grace() as well.
    bool awaitRelease() {
        std::unique_lock lock(m_);
        if (!cv_.wait_for(lock, kReleaseStart, [this] { return release_ != Release::Posted; })) {
            release_ = Release::Abandoned;
            lock.unlock();
            keepForever();
            RELAIS_LOG_WARN << "relais: the loop did not run relais's teardown within 1 s "
                               "(stopped?): its pool stays open";
            return false;
        }
        if (!cv_.wait_for(lock, grace() + kReleaseStart,
                          [this] { return release_ == Release::Done; })) {
            RELAIS_LOG_WARN << "relais: the loop's teardown did not end in time";
            return false;
        }
        return true;
    }

    // The teardown has ended.
    [[nodiscard]] bool released() const {
        std::lock_guard lock(m_);
        return release_ == Release::Done;
    }

    // How long a teardown takes at most, once the loop runs it.
    [[nodiscard]] std::chrono::milliseconds teardownBound() const noexcept {
        return grace() + kReleaseStart;
    }

    // What the binding holds on its loop must not be destroyed on another
    // thread: kept, still reachable, for the life of the process.
    void keepForever() {
        static std::mutex m;
        static auto* kept = new std::vector<std::shared_ptr<const void>>;
        std::lock_guard lock(m);
        kept->push_back(this->shared_from_this());
    }

private:
    enum class Release { None, Posted, Running, Done, Abandoned };
    enum class Bind { Now, Done };

    static constexpr auto kReleaseStart = std::chrono::seconds(1);
    // How often the teardown looks again for what still runs: a foreign loop
    // offers no hook after each of its turns.
    static constexpr auto kDrainTick = std::chrono::milliseconds(10);

    // An eager coroutine that frees itself when it ends. While it is suspended,
    // the binding holds its handle, so that the teardown can end it on the loop.
    struct BootTask {
        struct promise_type {
            BootTask get_return_object() noexcept { return {}; }
            std::suspend_never initial_suspend() noexcept { return {}; }
            std::suspend_never final_suspend() noexcept { return {}; }
            void return_void() noexcept {}
            void unhandled_exception() noexcept {}  // the body catches everything
        };
    };

    // Hands the awaiting coroutine's handle over, without suspending it.
    struct OwnHandle {
        std::coroutine_handle<>* out;
        bool await_ready() const noexcept { return false; }
        bool await_suspend(std::coroutine_handle<> h) const noexcept {
            *out = h;
            return false;
        }
        void await_resume() const noexcept {}
    };

    [[nodiscard]] bool releasing() const {
        std::lock_guard lock(m_);
        return release_ != Release::None;
    }

    // The loop thread, or the thread that will run the loop: the pool, which
    // lends nothing before it warms up, the scheduler, and this thread's
    // providers bound to them. Touches nothing of the loop.
    void bind() {
        if (attachedHere() != nullptr || PgProvider::initialized())
            throw AttachError("relais is already attached to this loop");
        auto pool = Pool::make(io_, conninfo_, cfg_.pool);
        auto scheduler = std::make_shared<Scheduler>(io_, pool, cfg_.max_concurrent);
        PgProvider::bindBatcher<Io>(scheduler);
        attachedHere() = this;
        attachedLoop<Io>() = &io_;
        pool_ = std::move(pool);
        scheduler_ = std::move(scheduler);
        loop_thread_.store(std::this_thread::get_id(), std::memory_order_relaxed);
    }

    // The thread bind() bound: lets go of its providers.
    void unbind() noexcept {
        if (attachedHere() != this) return;
        PgProvider::reset();
        attachedHere() = nullptr;
        attachedLoop<Io>() = nullptr;
    }

    // Loop thread. `self` keeps the binding alive for as long as the frame.
    static BootTask boot(std::shared_ptr<LoopBinding> self, Bind bind) {
        co_await OwnHandle{&self->boot_frame_};
        std::exception_ptr error;
        try {
            if (bind == Bind::Now) self->bind();
            co_await self->pool_->warmUp();
        } catch (...) {
            error = std::current_exception();
        }
        self->boot_frame_ = {};
        self->bootEnded(std::move(error));
    }

    // attachHere(), the loop's first turn: the thread it runs on, then the
    // check of the loop, or the pool's first connections.
    void firstTurn() {
        const auto here = std::this_thread::get_id();
        if (here != loop_thread_.load(std::memory_order_relaxed)) abortOffLoop(kAttachedOffLoop);
        // An adapter that checks the thread of each callback of relais's.
        if constexpr (requires(Io& i, std::thread::id t) { i.pinThread(t); }) io_.pinThread(here);
        if (releasing()) return;
        if (check_ == Check::None) {
            boot(this->shared_from_this(), Bind::Done);
            return;
        }
        try {
            startChecker();
        } catch (...) {
            failBeforeWarmUp(std::current_exception());
        }
    }

    // The check of a loop whose own thread attached relais: a thread of
    // relais's plays the thread outside the loop, and keeps the deadline;
    // what it finds is posted back. The deadline is acquire_timeout, 1 s at
    // least: a loop busy as it starts is not taken for a broken one sooner
    // than a request on it would time out.
    void startChecker() {
        const auto timeout = std::max<std::chrono::milliseconds>(SelfCheck::kTimeout,
                                                                  cfg_.pool.acquire_timeout);
        auto check = SelfCheck::make(io_, SelfCheck::Starter::Aside, timeout);
        // Never the binding's last owner: the binding joins this thread, which
        // must not end it.
        std::weak_ptr<LoopBinding> weak = this->weak_from_this();
        checker_ = std::thread([check, weak, &io = io_] {
            check->begin();
            auto failure = check->wait();
            io.post([weak, failure = std::move(failure)]() mutable {
                if (auto self = weak.lock()) self->checked(std::move(failure));
            });
        });
        aside_check_ = std::move(check);
    }

    // Loop thread: what the check thread found, as its last act. A loop whose
    // post() runs it elsewhere cannot even be told it failed.
    void checked(std::optional<std::string> failure) {
        if (std::this_thread::get_id() != loop_thread_.load(std::memory_order_relaxed)) {
            abortOffLoop("relais: relais cannot run on this loop: "
                         + failure.value_or("a callback posted to it ran on another thread"));
        }
        if (checker_.joinable()) checker_.join();
        aside_check_.reset();
        if (releasing()) return;
        if (failure) {
            failBeforeWarmUp(std::make_exception_ptr(
                AttachError("relais cannot run on this loop: " + *failure)));
            return;
        }
        boot(this->shared_from_this(), Bind::Done);
    }

    // Loop thread: the pool never warmed up.
    void failBeforeWarmUp(std::exception_ptr error) {
        pool_->failWarmUp(error);
        bootEnded(std::move(error));
    }

    // Loop thread. A failure has failed the pool already.
    void bootEnded(std::exception_ptr error) {
        {
            std::lock_guard lock(m_);
            if (booted_) return;
            booted_ = true;
            boot_error_ = error;
        }
        cv_.notify_all();
        if (check_ == Check::Caller) return;  // attach() waits for it and tells
        if (error) {
            RELAIS_LOG_ERROR << "relais: could not start on this loop: " << describe(error);
            // Every call made on this thread from then on fails with it.
            if (attachedHere() == this) PgProvider::bindFailure(error);
        }
        if (on_ready_) {
            io_.post([self = this->shared_from_this(), error] { self->tellReady(error); });
        }
    }

    // Loop thread, from a turn of its own: never once the teardown has begun.
    void tellReady(const std::exception_ptr& error) noexcept {
        if (releasing()) return;
        if (auto on_ready = std::exchange(on_ready_, nullptr)) on_ready(error);
    }

    // Loop thread: a boot still running ends, failed, with what it waits for:
    // its check, its connect, and the acquires waiting for the pool.
    void stopBoot() {
        if (aside_check_) {
            aside_check_->abandon("relais left the loop");
            if (checker_.joinable()) checker_.join();
            aside_check_.reset();
        }
        // Destroying a suspended boot ends its connect here, on the loop.
        if (auto frame = std::exchange(boot_frame_, {})) frame.destroy();
        auto error = std::make_exception_ptr(AttachError("relais left the loop before it was ready"));
        {
            std::lock_guard lock(m_);
            if (booted_) return;
            booted_ = true;
            boot_error_ = error;
        }
        cv_.notify_all();
        if (pool_) pool_->failWarmUp(error);
    }

    // Loop thread.
    void release() {
        {
            std::lock_guard lock(m_);
            if (release_ == Release::Abandoned) return;
            release_ = Release::Running;
        }
        cv_.notify_all();
        stopBoot();
        drain_until_ = Clock::now() + grace();
        drain();
    }

    // Until nothing relais started on this loop is still running — its detached
    // coroutines, the batches, every request on the pool, whoever awaits it —
    // or until grace() has passed.
    void drain() {
        if (scheduler_ && !scheduler_->quiescentForTeardown() && Clock::now() < drain_until_) {
            io_.postDelayed(kDrainTick, [self = this->shared_from_this()] {
                self->io_.post([self] { self->drain(); });
            });
            return;
        }
        const bool drained = !scheduler_ || scheduler_->quiescentForTeardown();
        unbind();
        scheduler_.reset();
        pool_.reset();
        // Destroying the pool posts the removal of its connections' watches:
        // the teardown ends after them, in posting order, so that a loop
        // stopped as soon as detach() returns keeps nothing of relais.
        io_.post([self = this->shared_from_this(), drained] { self->finish(drained); });
    }

    // Loop thread, once what the teardown posted has run.
    void finish(bool drained) {
        // Nothing of relais runs on the loop any more, and the timers it armed
        // that are still pending do nothing; what the drain abandoned may
        // still need its own.
        if constexpr (requires(Io& i) { i.detached(); }) {
            if (drained) io_.detached();
        }
        {
            std::lock_guard lock(m_);
            release_ = Release::Done;
        }
        cv_.notify_all();
    }

    // Long enough for one request blocked on a bounded wait to time out and
    // end. Requests queued behind a saturated pool against a blackholed server
    // time out one after another, longer than that, and a timeout of 0 never
    // does: a frame still suspended at the end is abandoned, with what it holds.
    [[nodiscard]] std::chrono::milliseconds grace() const noexcept {
        return std::max(cfg_.pool.acquire_timeout, cfg_.pool.query_timeout)
             + std::chrono::milliseconds(500);
    }

    Io& io_;
    const std::string conninfo_;
    const AttachConfig cfg_;
    const Check check_;
    std::shared_ptr<SelfCheck> caller_check_;  // the attaching thread's
    std::atomic<std::thread::id> loop_thread_{};  // set by bind()

    // Shared with the waiting thread.
    mutable std::mutex m_;
    std::condition_variable cv_;
    bool booted_ = false;
    std::exception_ptr boot_error_;
    Release release_ = Release::None;

    // Loop thread; pool_ also read once booted_ is seen, or on the thread that
    // attachHere() bound.
    OnReady on_ready_;
    std::shared_ptr<SelfCheck> aside_check_;
    std::thread checker_;  // runs aside_check_
    std::coroutine_handle<> boot_frame_;
    std::shared_ptr<Pool> pool_;
    std::shared_ptr<Scheduler> scheduler_;
    Clock::time_point drain_until_;
};

// attach() waits for the loop: on the loop's own thread, it never would.
template<io::IoContext Io>
void refuseLoopThread(Io& io) {
    if constexpr (requires(Io& i) { { i.isInLoopThread() } -> std::convertible_to<bool>; }) {
        if (io.isInLoopThread())
            throw AttachError("relais::attach() called on the loop's own thread: it waits for "
                              "that loop, so call it from another thread, or call "
                              "relais::attachHere()");
    }
}

// Takes relais off every loop at once, each waiting there for its requests in
// flight. On a loop's own thread, starts that loop's teardown without waiting
// for it.
template<io::IoContext Io>
void releaseAll(std::span<const std::shared_ptr<LoopBinding<Io>>> bindings) noexcept {
    for (const auto& binding : bindings) {
        if (binding) binding->beginRelease();
    }
    for (const auto& binding : bindings) {
        if (binding && !binding->onLoopThread()) (void)binding->awaitRelease();
    }
}

// Binds relais to every loop at once, from another thread than theirs: all are
// checked, then all boot, each boot bounded by `deadline` when given. A loop
// that fails the check fails them all before any boot. Once they boot, an error
// or the deadline takes relais off every loop: the error is rethrown, the
// deadline gives nullopt.
template<io::IoContext Io>
[[nodiscard]] std::optional<std::vector<std::shared_ptr<LoopBinding<Io>>>>
bindAll(std::span<Io* const> loops, const std::string& conninfo, const AttachConfig& cfg,
        std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt) {
    std::vector<std::shared_ptr<LoopBinding<Io>>> bindings;
    bindings.reserve(loops.size());
    for (Io* io : loops) bindings.push_back(LoopBinding<Io>::start(*io, conninfo, cfg));

    std::exception_ptr error;
    for (const auto& binding : bindings) {
        try {
            binding->awaitCheck();
        } catch (...) {
            if (!error) error = std::current_exception();
        }
    }
    if (error) std::rethrow_exception(error);

    for (const auto& binding : bindings) binding->startBoot();
    bool timed_out = false;
    for (const auto& binding : bindings) {
        try {
            if (!binding->awaitBoot(deadline)) timed_out = true;
        } catch (...) {
            if (!error) error = std::current_exception();
        }
    }
    if (timed_out || error) {
        releaseAll<Io>(bindings);  // ends, on its loop, a boot still connecting
        if (timed_out) return std::nullopt;
        std::rethrow_exception(error);
    }
    return bindings;
}

}  // namespace detail

// Attachment — relais bound to a loop by attach() or attachHere(). Destroying
// it, or detach(), takes relais off the loop.
template<io::IoContext Io>
class Attachment {
public:
    Attachment() noexcept = default;
    explicit Attachment(std::shared_ptr<detail::LoopBinding<Io>> binding) noexcept
        : binding_(std::move(binding)) {}

    Attachment(Attachment&&) noexcept = default;
    Attachment& operator=(Attachment&& other) noexcept {
        if (this != &other) {
            detach();
            binding_ = std::move(other.binding_);
        }
        return *this;
    }
    Attachment(const Attachment&) = delete;
    Attachment& operator=(const Attachment&) = delete;

    ~Attachment() { detach(); }

    /// The loop's connection pool; empty once detached.
    [[nodiscard]] std::shared_ptr<io::PgPool<Io>> pool() const {
        return binding_ ? binding_->pool() : nullptr;
    }

    /// The pool has its first connections (min_connections): always, once
    /// attach() has returned; false once detached.
    [[nodiscard]] bool ready() const { return binding_ && binding_->ready(); }

    /// What relais could not start with, if attachHere() failed to: the
    /// loop's check, or a connection. Empty once detached.
    [[nodiscard]] std::exception_ptr error() const {
        return binding_ ? binding_->error() : nullptr;
    }

    /// Takes relais off the loop, which must still be running: waits there for
    /// the requests in flight (each bounded by its timeouts), unbinds the loop
    /// thread, and closes the pool. Blocks until done, except on the loop's own
    /// thread, where it only starts it, and the loop must go on running until
    /// it ends; once done, the loop may stop at once. Idempotent.
    void detach() noexcept {
        if (auto binding = std::move(binding_)) {
            binding->beginRelease();
            if (!binding->onLoopThread()) (void)binding->awaitRelease();
        }
    }

private:
    std::shared_ptr<detail::LoopBinding<Io>> binding_;
};

/// Binds relais to the loop `io` runs: checks the loop in a few milliseconds
/// (AttachError names what it fails), creates the pool on it, and binds that
/// loop thread, where repository calls then run. Blocks until done; call it
/// once per loop, from another thread than the loop's, while it runs. The
/// returned Attachment takes relais off the loop; `io` must outlive it.
template<io::IoContext Io>
[[nodiscard]] Attachment<Io> attach(Io& io, std::string conninfo, AttachConfig cfg = {}) {
    detail::refuseLoopThread(io);
    Io* const loops[] = {&io};
    auto bound = detail::bindAll<Io>(loops, conninfo, cfg);  // no deadline: never empty
    return Attachment<Io>(std::move(bound->front()));
}

/// Binds relais to the loop `io` runs, on the calling thread, which runs it:
/// from one of its callbacks, or before it starts there. Returns at once.
/// Repository calls made on this thread from then on wait for the pool's first
/// connection. On the loop's first turn, relais checks the loop (as attach()
/// does, bounded by acquire_timeout, 1 s at least), then connects the pool.
///
/// `on_ready`, if given, then runs on the loop thread, once: with nothing once
/// the pool has its first connections (min_connections), or with what failed —
/// an AttachError naming what the loop failed, or the connection's error. A
/// failure is also logged, and every PostgreSQL call made on this thread, then
/// or from then on, fails with it, until detach(). on_ready must not throw;
/// it never runs once detach() has begun.
///
/// A loop that relais's first turn finds on another thread ends the process:
/// relais's state belongs to the thread that called attachHere(). Throws
/// AttachError if relais is attached to this thread already. `io` must
/// outlive the returned Attachment, and the loop must run relais's teardown
/// (see Attachment::detach).
template<io::IoContext Io>
[[nodiscard]] Attachment<Io> attachHere(Io& io, std::string conninfo, AttachConfig cfg = {},
                                        std::function<void(std::exception_ptr)> on_ready = {}) {
    using Binding = detail::LoopBinding<Io>;
    return Attachment<Io>(Binding::startHere(io, std::move(conninfo), cfg, Binding::Check::Aside,
                                             std::move(on_ready)));
}

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_RUNTIME_ATTACH_H
