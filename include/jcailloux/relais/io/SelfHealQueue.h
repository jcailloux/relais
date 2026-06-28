#ifndef JCX_RELAIS_IO_SELF_HEAL_QUEUE_H
#define JCX_RELAIS_IO_SELF_HEAL_QUEUE_H

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/IoContext.h"
#include "jcailloux/relais/io/redis/RedisPool.h"

namespace jcailloux::relais::io {

// SelfHealQueue — deferred retry of cache evictions that could not reach Redis.
//
// When a confirmed or uncertain mutation cannot confirm its L2 eviction (Redis
// unreachable, UNLINK timed out), the phantom — the pre-mutation value, or a
// deleted row — survives in L2, and a concurrent read can re-store it into the
// process-shared L1. Left alone, that staleness is bounded only by l1_ttl (and
// is permanent when l1_ttl=0). This queue bounds it to seconds: it re-runs the
// eviction off the hot path until the UNLINK confirms.
//
// One instance per loop thread (owned by that thread's BatchScheduler). All
// state is loop-thread-local — enqueue and the drain both run on the loop, run
// to completion, never preempt — so no atomics, no locks.
//
// Each retry is an opaque `Task<bool>()` built by the repo layer: it performs
// bump-gen → UNLINK L2 → evict L1 (L1 last — its generation bump closes the
// read-fill straddle only after the UNLINK lands) and returns whether the UNLINK
// confirmed. The queue knows nothing of entities; it dedupes by the Redis key
// string (globally unique — it carries the repo name prefix).
//
// Bounds (anti-OOM under a long outage at write QPS):
//   - dedup by key: a key already pending is a no-op refresh, so the footprint
//     is the number of DISTINCT stale keys, not the number of attempts.
//   - overflow: past kMaxKeys distinct keys the surplus is dropped with a loud
//     log and degrades on l1/l2_ttl — never unbounded silent growth.
//   - cadence: one recurring timer (not one per key), bounded exponential
//     backoff while Redis stays down, reset to the base interval on progress.
//   - drain stops at the first failed retry in a pass: a down Redis costs ~one
//     bounded UNLINK per pass, not one per pending key.
//
// Cross-instance staleness (another process's separate L1) is out of reach here
// and remains bounded by l1_ttl (operator policy), as documented in the design.
template<IoContext Io>
class SelfHealQueue {
public:
    /// An opaque eviction retry: bump-gen → UNLINK L2 → evict L1, returning
    /// whether the UNLINK confirmed. Re-tried until it returns true.
    using Retry = std::function<Task<bool>()>;

    SelfHealQueue(Io& io, std::shared_ptr<RedisPool<Io>> pool) noexcept
        : io_(io), pool_(std::move(pool)) {}

    // Non-movable: a stable address is required (the recurring timer callback
    // captures `this`). Held as a BatchScheduler member, which is never moved.
    SelfHealQueue(const SelfHealQueue&) = delete;
    SelfHealQueue& operator=(const SelfHealQueue&) = delete;
    SelfHealQueue(SelfHealQueue&&) = delete;
    SelfHealQueue& operator=(SelfHealQueue&&) = delete;

    ~SelfHealQueue() {
        // Stop the recurring drain so a pending tick cannot fire on a destroyed
        // queue. An already-running drain task is the broader teardown concern
        // shared with the batch fire timers (addressed with them).
        if (armed_) io_.cancelTimer(timer_);
    }

    /// Register (or refresh) a deferred eviction for `key`. Off the hot path —
    /// called only when an eviction's UNLINK did not confirm.
    void enqueue(std::string key, Retry retry) {
        auto it = pending_.find(key);
        if (it != pending_.end()) {
            // Already pending: keep the latest gestures (idempotent UNLINK, but a
            // newer mutation may carry a fresher closure). No new timer.
            it->second = std::move(retry);
            return;
        }
        if (pending_.size() >= kMaxKeys) {
            RELAIS_LOG_ERROR << "SelfHealQueue: overflow at " << kMaxKeys
                << " distinct stale keys — dropping '" << key
                << "', it degrades on l1/l2_ttl until Redis recovers and the queue drains";
            return;
        }
        pending_.emplace(std::move(key), std::move(retry));
        if (!armed_ && !draining_) arm(kBaseDelay);
    }

    [[nodiscard]] size_t pending() const noexcept { return pending_.size(); }

    /// Begin ordered teardown: stop re-arming the recurring drain so the owning
    /// loop can pump to quiescence. A drain already executing runs to completion
    /// (bounded by redis_query_timeout); it just will not schedule another pass —
    /// without this, a queue draining against a down Redis would re-arm forever
    /// and teardown could never settle. Idempotent; loop-thread only.
    void beginShutdown() noexcept {
        shutting_down_ = true;
        if (armed_) { io_.cancelTimer(timer_); armed_ = false; }
    }

    /// True while a drain pass is executing. Ordered teardown waits on this so a
    /// drain suspended on Redis I/O is drained, not abandoned mid-flight.
    [[nodiscard]] bool draining() const noexcept { return draining_; }

private:
    void arm(std::chrono::nanoseconds delay) {
        // Ordered teardown: never (re-)arm once shutting down. Guards both the
        // enqueue path and drainTask's own tail re-arm, so the recurring drain
        // stops and the loop can pump to quiescence.
        if (shutting_down_) return;
        armed_ = true;
        timer_ = io_.postDelayed(delay, [this] {
            armed_ = false;
            if (!draining_ && !pending_.empty()) drainTask();
        });
    }

    /// One drain pass: rebuild dead Redis connections, then retry pending keys.
    /// Fire-and-forget on the loop; re-arms itself while work remains.
    ///
    /// Lifetime of the `this` touched after each co_await: this detached frame is
    /// resumed only by the loop that owns this queue, and that loop outlives the
    /// owning scheduler (the scheduler is torn down only after its worker thread is
    /// joined). A drain still suspended at teardown is therefore never resumed — it
    /// cannot dereference a freed queue. It is deliberately NOT anchored with a
    /// shared_ptr to the scheduler: that would turn a suspended-at-teardown frame
    /// into a retention cycle (frame -> scheduler -> this queue -> frame) leaking
    /// the whole scheduler, strictly worse than the parked frame the dtor already
    /// flags as the broader in-flight-detached concern shared with the fire timers.
    DetachedTask drainTask() {
        draining_ = true;

        // Recreate slots poisoned during the outage so the retries below dispatch
        // to a live connection. Single-flight + quiescence-gated inside the pool;
        // it can connect synchronously, so it runs before any retry this pass.
        if (pool_) {
            try { co_await pool_->reviveDeadClients(); } catch (...) {}
        }

        // Snapshot the keys: a retry's own I/O may enqueue new keys during the
        // awaits below; those are handled on the next pass, not mid-iteration.
        std::vector<std::string> keys;
        keys.reserve(pending_.size());
        for (const auto& kv : pending_) keys.push_back(kv.first);

        bool progressed = false;
        for (const auto& k : keys) {
            auto it = pending_.find(k);
            if (it == pending_.end()) continue;   // erased meanwhile
            Retry retry = it->second;             // copy: `it` may be invalidated across the await
            bool ok = false;
            try {
                ok = co_await retry();
            } catch (...) {
                ok = false;
            }
            if (ok) {
                pending_.erase(k);
                progressed = true;
            } else {
                // Redis is still unreachable — stop hammering N bounded UNLINKs
                // this pass; the rest stay queued for the next one.
                break;
            }
        }

        draining_ = false;
        if (!pending_.empty()) {
            // Reset on progress (Redis is responding), back off otherwise.
            backoff_ = progressed ? kBaseDelay : std::min(backoff_ * 2, kMaxDelay);
            arm(backoff_);
        } else {
            backoff_ = kBaseDelay;
        }
        co_return;
    }

    static constexpr std::chrono::nanoseconds kBaseDelay = std::chrono::milliseconds(100);
    static constexpr std::chrono::nanoseconds kMaxDelay  = std::chrono::seconds(2);
    static constexpr size_t kMaxKeys = 100'000;

    Io& io_;
    std::shared_ptr<RedisPool<Io>> pool_;
    std::unordered_map<std::string, Retry> pending_;
    typename Io::TimerToken timer_{};
    std::chrono::nanoseconds backoff_ = kBaseDelay;
    bool armed_ = false;          // a one-shot drain tick is pending
    bool draining_ = false;       // a drain task is currently executing
    bool shutting_down_ = false;  // set by beginShutdown(): arm() becomes a no-op
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_SELF_HEAL_QUEUE_H
