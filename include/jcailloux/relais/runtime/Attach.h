#ifndef JCX_RELAIS_RUNTIME_ATTACH_H
#define JCX_RELAIS_RUNTIME_ATTACH_H

#include <algorithm>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <coroutine>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
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
struct AttachError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

namespace detail {

// The LoopBinding whose providers this thread holds, if any.
inline const void*& attachedHere() noexcept {
    thread_local const void* binding = nullptr;
    return binding;
}

// LoopBinding — relais bound to one loop: the check of the loop, then the pool
// and the scheduler created on it, the loop thread's providers bound to them,
// and the ordered teardown. Each phase is posted onto the loop and awaited from
// another thread, so that IoPool binds all its loops at once.
//
// What relais creates on the loop is destroyed there: by the teardown, which
// the loop must run. A loop that does not run it keeps it for good (see
// keepForever).
template<io::IoContext Io>
class LoopBinding : public std::enable_shared_from_this<LoopBinding<Io>> {
    using Clock = std::chrono::steady_clock;
    using Pool = io::PgPool<Io>;
    using Scheduler = io::batch::detail::BatchScheduler<Io>;
    struct Tag {};

public:
    LoopBinding(Tag, Io& io, std::string conninfo, AttachConfig cfg)
        : io_(io), conninfo_(std::move(conninfo)), cfg_(cfg) {}

    LoopBinding(const LoopBinding&) = delete;
    LoopBinding& operator=(const LoopBinding&) = delete;

    // Starts checking the loop. From another thread than the loop's.
    static std::shared_ptr<LoopBinding> start(Io& io, std::string conninfo, AttachConfig cfg) {
        auto binding = std::make_shared<LoopBinding>(Tag{}, io, std::move(conninfo), cfg);
        binding->check_ = io::detail::LoopSelfCheck<Io>::start(io);
        return binding;
    }

    // Waits for the check; throws AttachError saying what the loop failed.
    void awaitCheck() {
        auto failure = check_->wait();
        check_.reset();
        if (failure) throw AttachError("relais cannot run on this loop: " + *failure);
    }

    // Creates the pool and the scheduler on the loop, and binds the loop
    // thread's providers to them.
    void startBoot() {
        io_.post([self = this->shared_from_this()] { boot(self); });
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

    // Set by a successful boot, until the teardown.
    [[nodiscard]] const std::shared_ptr<Pool>& pool() const noexcept { return pool_; }

    // True on the loop's own thread, when the loop can tell.
    [[nodiscard]] bool onLoopThread() const noexcept {
        if constexpr (requires(Io& i) { { i.isInLoopThread() } -> std::convertible_to<bool>; })
            return io_.isInLoopThread();
        return false;
    }

    // Posts the ordered teardown, whatever the phase reached: a boot still
    // connecting is ended, a bound loop drains, then everything is destroyed on
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

private:
    enum class Release { None, Posted, Running, Done, Abandoned };

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

    // Loop thread. `self` keeps the binding alive for as long as the frame.
    static BootTask boot(std::shared_ptr<LoopBinding> self) {
        co_await OwnHandle{&self->boot_frame_};
        std::exception_ptr error;
        try {
            if (attachedHere() != nullptr || PgProvider::initialized())
                throw AttachError("relais is already attached to this loop");
            attachedHere() = self.get();
            auto pool = co_await Pool::create(self->io_, self->conninfo_, self->cfg_.pool);
            auto scheduler = std::make_shared<Scheduler>(self->io_, pool, self->cfg_.max_concurrent);
            PgProvider::bindBatcher<Io>(scheduler);
            self->pool_ = std::move(pool);
            self->scheduler_ = std::move(scheduler);
        } catch (...) {
            if (attachedHere() == self.get()) attachedHere() = nullptr;
            error = std::current_exception();
        }
        self->boot_frame_ = {};
        {
            std::lock_guard lock(self->m_);
            self->booted_ = true;
            self->boot_error_ = std::move(error);
        }
        self->cv_.notify_all();
    }

    // Loop thread.
    void release() {
        {
            std::lock_guard lock(m_);
            if (release_ == Release::Abandoned) return;
            release_ = Release::Running;
        }
        cv_.notify_all();
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
        // Destroying a suspended boot ends its connect here, on the loop.
        if (auto frame = std::exchange(boot_frame_, {})) frame.destroy();
        if (scheduler_) {
            PgProvider::reset();
            scheduler_.reset();
        }
        if (attachedHere() == this) attachedHere() = nullptr;
        pool_.reset();
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

    // What the binding holds on its loop must not be destroyed on another
    // thread: kept, still reachable, for the life of the process.
    void keepForever() {
        static std::mutex m;
        static auto* kept = new std::vector<std::shared_ptr<const void>>;
        std::lock_guard lock(m);
        kept->push_back(this->shared_from_this());
    }

    Io& io_;
    const std::string conninfo_;
    const AttachConfig cfg_;
    std::shared_ptr<io::detail::LoopSelfCheck<Io>> check_;  // the starting thread's

    // Shared with the waiting thread.
    std::mutex m_;
    std::condition_variable cv_;
    bool booted_ = false;
    std::exception_ptr boot_error_;
    Release release_ = Release::None;

    // Loop thread; pool_ also read once booted_ is seen.
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
                              "that loop, so call it from another thread");
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

// Attachment — relais bound to a loop by attach(). Destroying it, or detach(),
// takes relais off the loop.
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

    /// Takes relais off the loop, which must still be running: waits there for
    /// the requests in flight (each bounded by its timeouts), unbinds the loop
    /// thread, and closes the pool. Blocks until done, except on the loop's own
    /// thread, where it only starts it; idempotent.
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

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_RUNTIME_ATTACH_H
