#ifndef JCX_RELAIS_CACHE_GDSF_POLICY_H
#define JCX_RELAIS_CACHE_GDSF_POLICY_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <mutex>
#include <shared_mutex>
#include <stop_token>
#include <thread>
#include <vector>

#include <utils/epoch.h>
#include <utils/threads/thread_id_pool.h>

#include "jcailloux/relais/cache/CacheMetadata.h"
#include "jcailloux/relais/cache/L1Ledger.h"
#include "jcailloux/relais/cache/Metrics.h"
#include "jcailloux/relais/runtime/RuntimeThread.h"
#include "jcailloux/relais/Log.h"

#ifndef RELAIS_GDSF_ENABLED
#define RELAIS_GDSF_ENABLED 0
#endif

#ifndef RELAIS_CLEANUP_FREQUENCY_LOG2
#define RELAIS_CLEANUP_FREQUENCY_LOG2 9
#endif

#ifdef RELAIS_BUILDING_TESTS
namespace relais_test { struct TestInternals; }
#endif

namespace jcailloux::relais::cache {

// =========================================================================
// Configuration
// =========================================================================

struct GDSFConfig {
    float decay_rate = 0.95f;                // x0.95 per sweep; after 14 sweeps: count ~ 49%
    float histogram_alpha = 0.3f;            // EMA smoothing for histogram merges
    size_t max_memory = 0;                   // L1 memory budget in bytes (0 = from env / unlimited)
    long chunk_count = 8;                    // number of chunks for all ChunkMaps (uniform)
};

namespace detail {

// =========================================================================
// fast_log2_approx — IEEE 754 bit manipulation (~1-2ns, branchless)
// =========================================================================

inline float fast_log2_approx(float x) {
    uint32_t bits;
    std::memcpy(&bits, &x, 4);
    return static_cast<float>(bits >> 23) - 127.0f
         + static_cast<float>(bits & 0x7FFFFF) * (1.0f / 8388608.0f);
}

// =========================================================================
// ScoreHistogram — 128 log2 buckets for memory-aware eviction
// =========================================================================
//
// Covers scores from 2^-10 (~0.001) to 2^23.25 (~10M).
// Each bucket stores cumulative bytes of entries in that score range.
// O(1) recording via fast_log2_approx, O(N) threshold computation.
// Size: 128 x 8B = 1KB.

struct ScoreHistogram {
    static constexpr int N = 128;                 // 128 log2 buckets covering [2^-10 .. 2^23.25]
    static constexpr float kLogMin = -10.0f;     // log2(0.001) ~ -10
    static constexpr float kLogMax = 23.25f;     // log2(10M) ~ 23.25
    static constexpr float kInvStep = static_cast<float>(N) / (kLogMax - kLogMin);

    uint64_t bytes[N] = {};

    void reset() { std::memset(bytes, 0, sizeof(bytes)); }

    /// Record an entry (score, byte size) into the appropriate bucket.
    void record(float score, size_t entry_bytes) {
        int idx = (score <= 0.0f) ? 0
            : std::clamp(static_cast<int>((fast_log2_approx(score) - kLogMin) * kInvStep),
                         0, N - 1);
        bytes[idx] += entry_bytes;
    }

    /// Find the threshold score such that entries below it total >= target_bytes.
    /// Walks buckets low-to-high, accumulating bytes.
    /// Returns 0 on cold start (empty histogram) to avoid nuclear eviction.
    /// Returns exp2(kLogMax) when the histogram is populated but the target
    /// exceeds its total — evict everything in this chunk; the while(isOverBudget)
    /// loop will sweep more chunks as needed.
    float thresholdForBytes(size_t target_bytes) const {
        if (target_bytes == 0) return 0.0f;
        uint64_t cumul = 0;
        for (int i = 0; i < N; ++i) {
            cumul += bytes[i];
            if (cumul >= target_bytes) {
                float log_val = kLogMin + static_cast<float>(i + 1) / kInvStep;
                return std::exp2(log_val);
            }
        }
        // Cold start (empty histogram): return 0 to avoid nuclear eviction.
        // Populated but insufficient: return max threshold to evict all
        // entries in this chunk — the caller's loop handles multi-chunk convergence.
        return (cumul > 0) ? std::exp2(kLogMax) : 0.0f;
    }

    /// Exponential moving average merge: this = alpha * newer + (1 - alpha) * this.
    void mergeEMA(const ScoreHistogram& newer, float alpha) {
        float one_minus_alpha = 1.0f - alpha;
        for (int i = 0; i < N; ++i) {
            bytes[i] = static_cast<uint64_t>(
                alpha * static_cast<float>(newer.bytes[i])
              + one_minus_alpha * static_cast<float>(bytes[i]));
        }
    }
};

// =========================================================================
// Type-erased repo entry for the global registry
// =========================================================================

struct RepoRegistryEntry {
    size_t (*sweep_fn)(long chunk_id);  // sweeps the chunk, returns the entries it removed
    size_t (*size_fn)();              // current L1 cache size (entry count)
    const char* name;                 // compile-time repo name (for logging)
};

}  // namespace detail

// =========================================================================
// GDSFPolicy — global singleton managing GDSF eviction coordination
// =========================================================================
//
// Leaking singleton (never destroyed) to avoid static destruction order
// issues: cache entry dtors may fire after static singletons are destroyed.
//
// Thread-safe: all public methods are safe to call concurrently.
//
// Eviction strategy:
//   1. Each repo sweeps 1 chunk (ghost decay, TTL expiration, score-based eviction)
//   2. Building histogram merged into persistent histogram_ via EMA
//
// The global sweep runs on the maintenance thread, never on a thread that
// serves requests: a tick asks for it and returns (see tickInsertion). One
// sweep at a time for the whole process: sweep_mu_ excludes the maintenance
// thread's sweeps and the forced passes.

class GDSFPolicy {
public:
    /// Compile-time GDSF toggle. Controls if constexpr guards in LocalRepo/ListMixin.
    /// When false, all GDSF code paths (metadata, scoring, ghosts) are eliminated.
    static constexpr bool enabled = RELAIS_GDSF_ENABLED;

    /// Compile-time cleanup frequency: sweep every 2^N insertions.
    /// 0 = disabled. Default 9 = every 512 insertions.
    /// The mask is an immediate in the `and` instruction — sub-nanosecond check.
    static constexpr uint8_t kCleanupFrequencyLog2 = RELAIS_CLEANUP_FREQUENCY_LOG2;
    static constexpr size_t kCleanupMask = kCleanupFrequencyLog2 > 0
        ? (size_t{1} << kCleanupFrequencyLog2) - 1 : ~size_t{0};

    static GDSFPolicy& instance() {
        static auto* p = new GDSFPolicy();
        return *p;
    }

    /// Configure the policy. Call once at startup before any repo access.
    /// Excluded from the sweeps, which read it.
    void configure(const GDSFConfig& cfg) {
        std::lock_guard exclusion(sweep_mu_);
        config_ = cfg;
        if (cfg.max_memory > 0) max_memory_ = cfg.max_memory;
    }

    const GDSFConfig& config() const { return config_; }

    /// Runtime L1 memory budget (bytes). Read once from RELAIS_L1_MAX_MEMORY env
    /// var at construction, overridable via configure(). Returns 0 if unset (no limit).
    size_t maxMemory() const { return max_memory_; }

    /// True when the bytes the L1 holds (+ extra) would exceed the budget:
    /// held + admitted_since_refresh + extra ≥ budget. The held bytes are read
    /// from the L1 ledger at each refresh (see refreshHeldBytes); what was
    /// admitted since bridges the gap. Memory the L1 does not hold never
    /// counts, whatever the allocator.
    /// Pass est_bytes as extra to pre-check that an admission won't overshoot.
    /// Cost: 2 relaxed loads + add (~2ns). Only called on cache miss path.
    bool isOverBudget(uint64_t extra = 0) const {
        if (max_memory_ == 0) return false;
        return held_bytes_.load(std::memory_order_relaxed)
             + admitted_since_refresh_.load(std::memory_order_relaxed)
             + extra
             >= max_memory_;
    }

    /// Record admitted bytes.  Called on real L1 insertion only (not ghosts).
    void recordAdmission(uint32_t est_bytes) {
        admitted_since_refresh_.fetch_add(est_bytes, std::memory_order_relaxed);
    }

    /// Reads the bytes the L1 holds (L1Ledger::sum) into the value the budget
    /// compares, and restarts the admissions it bridges: the sum holds them.
    /// An admission under way meanwhile is seen at the next refresh at the
    /// latest. Called by each sweep, before and after it evicts, and every
    /// 100 ms by RuntimeThread. Cost: one cache line per thread.
    void refreshHeldBytes() noexcept {
        if constexpr (enabled) {
            admitted_since_refresh_.store(0, std::memory_order_relaxed);
            // A sum read while other threads allocate and free moves: near an
            // empty cache it can read below zero for an instant.
            const int64_t held = detail::L1Ledger::sum();
            held_bytes_.store(held > 0 ? static_cast<uint64_t>(held) : 0,
                              std::memory_order_relaxed);
        }
    }

    /// Number of chunks (uniform across all ChunkMaps).
    long chunkCount() const { return config_.chunk_count; }

    /// Constant decay rate for temporal aging of access counts.
    /// Applied to every entry during each sweep pass.
    ///
    /// Decay controls score aging (how fast old access patterns fade).
    /// A constant rate preserves relative score differentiation across
    /// sweeps: after N sweeps at rate r, ratio A/B is unchanged.
    /// Pressure-adaptive decay destroyed differentiation under load
    /// (0.25^3 ≈ 0.016 — all scores converge to zero in 3 sweeps).
    float decayRate() const {
        return config_.decay_rate;
    }

    // =====================================================================
    // Deterministic Cleanup Trigger
    // =====================================================================

    /// Tick the global insertion counter. Every kCleanupMask+1 ticks, asks the
    /// maintenance thread for a global sweep and returns: the sweep never runs
    /// on the ticking thread. Three tick sources:
    ///   - Real L1 insertion (under budget) — maintenance sweeps
    ///   - New ghost creation (over budget, first appearance) — tracking
    ///   - Qualified ghost re-access (over budget, score ≥ threshold) — eviction demand
    void tickInsertion() {
        if (kCleanupFrequencyLog2 > 0
                && (insertion_counter_.fetch_add(1, std::memory_order_relaxed)
                    & kCleanupMask) == kCleanupMask) {
            requestSweep();
        }
    }

    // =====================================================================
    // Repo Registry
    // =====================================================================

    /// Register a repo for global coordination (threshold, sweep).
    /// Called once per LocalRepo instantiation via std::call_once.
    /// The first enrollment starts the maintenance thread. The owner of an
    /// entry calls stopMaintenance() before it destroys what its sweep
    /// touches.
    void enroll(detail::RepoRegistryEntry entry) {
        {
            std::unique_lock lock(registry_mutex_);
            registry_.push_back(std::move(entry));
        }
        std::lock_guard lock(maintenance_mu_);
        if (!maintenance_.joinable() && !maintenance_stopped_) startMaintenance();
    }

    /// Stops the maintenance thread and waits for the sweep in progress.
    /// Called as the first enrolled tier is destroyed at exit: no sweep runs
    /// once a tier, a tracker or the epoch goes. Final: no sweep is served
    /// after it; the ticks keep counting. Idempotent.
    void stopMaintenance() noexcept {
        std::jthread thread;
        {
            std::lock_guard lock(maintenance_mu_);
            maintenance_stopped_ = true;
            thread = std::move(maintenance_);
        }
        if (!thread.joinable()) return;
        thread.request_stop();
        // Wakes the thread where it waits for a request.
        requested_.fetch_add(1, std::memory_order_release);
        requested_.notify_one();
        thread.join();
    }

    size_t nbRepos() const {
        std::shared_lock lock(registry_mutex_);
        return registry_.size();
    }

    // =====================================================================
    // Threshold (cached, updated during sweep)
    // =====================================================================

    /// Current eviction threshold. Set by sweep(), read by cleanup predicates.
    float threshold() const {
        return cached_threshold_.load(std::memory_order_relaxed);
    }

    // =====================================================================
    // Cycle Interval (EMA-smoothed full sweep cycle time)
    // =====================================================================

    /// Average time for a full sweep cycle (all chunks), in microseconds.
    /// EMA-smoothed (α=0.2, half-life ≈ 3 cycles).
    float avgCycleIntervalUs() const {
        return avg_cycle_interval_us_.load(std::memory_order_relaxed);
    }

    // =====================================================================
    // Histogram Recording (during sweep, protected by sweep_mu_)
    // =====================================================================

    /// Record an entry into the building histogram during sweep.
    /// Called by cleanup predicates for ALL entries (evicted + kept).
    /// Only called during a sweep or a forced pass, both under sweep_mu_.
    void recordEntry(float score, size_t entry_bytes) {
        building_histogram_.record(score, entry_bytes);
    }

    // =====================================================================
    // Global Sweep
    // =====================================================================

    /// Forced pass: `sweep_fn` (one repo's sweep) on every chunk, on the
    /// calling thread, excluded from the global sweep. It removes what a sweep
    /// removes, against the current threshold, ages the counts and the ghosts
    /// as a sweep does, and records into the building histogram, which the
    /// next global sweep resets before it records: the threshold does not
    /// move. Waits for the sweep or pass in progress; the maintenance thread
    /// lets it go before its next sweep. Returns the entries removed.
    size_t sweepEveryChunk(size_t (*sweep_fn)(long chunk_id)) {
        auto exclusion = lockForPass();
        return passLocked(sweep_fn);
    }

    /// Asks the maintenance thread for the forced pass of `sweep_fn` (see
    /// sweepEveryChunk) and returns: the caller never waits for a sweep. A
    /// request made while the pass of `sweep_fn` is queued asks for nothing
    /// more; the thread takes the pass off the queue as it starts it, so a
    /// request made during the pass asks for another, which sees what the
    /// first one missed. The passes asked for go before the next sweep. None
    /// is served once the thread is stopped.
    /// Cost: a lock, a look over the queue (one slot per repo at most), and,
    /// unless merged, a push, an increment and a notify.
    void requestPass(size_t (*sweep_fn)(long chunk_id)) {
        {
            // The pass sees what was written before the request: the thread
            // takes it under this lock.
            std::lock_guard lock(passes_mu_);
            if (std::find(pass_queue_.begin(), pass_queue_.end(), sweep_fn)
                    != pass_queue_.end()) {
                return;
            }
            pass_queue_.push_back(sweep_fn);
            // Under passes_mu_: a pass in the queue is counted in requested_
            // (see serveSweeps).
            requested_.fetch_add(1, std::memory_order_release);
        }
        requested_.notify_one();
    }

#if RELAIS_ENABLE_METRICS
    const detail::SweepCounters& sweepCounters() const noexcept { return sweep_counters_; }
    detail::SweepCounters& sweepCounters() noexcept { return sweep_counters_; }
#endif

private:
    GDSFPolicy() : max_memory_(readMaxMemoryFromEnv()) {
        // Without GDSF nothing reads the held bytes: no hook.
        // Release-store the hook last: a background-thread reader that acquire-
        // loads it then sees this fully-constructed policy.
        if constexpr (enabled) {
            runtime::detail::RuntimeThread::on_tick.store(
                [] () noexcept { GDSFPolicy::instance().refreshHeldBytes(); },
                std::memory_order_release);
        }
    }

    /// Global sweep: iterates all repos, sweeps one chunk per repo. The caller
    /// holds sweep_mu_.
    void sweepLocked() {
#if RELAIS_ENABLE_METRICS
        auto sweep_t0 = std::chrono::steady_clock::now();
#endif

        // 1. Advance global chunk cursor — all repos sweep the SAME chunk.
        long chunk_id = sweep_cursor_.fetch_add(1, std::memory_order_relaxed)
                        % config_.chunk_count;

        // 2. Track cycle interval (full rotation of all chunks).
        if (chunk_id == 0) {
            auto now = std::chrono::steady_clock::now();
            if (last_cycle_tp_ != std::chrono::steady_clock::time_point{}) {
                auto elapsed_us = static_cast<float>(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        now - last_cycle_tp_).count());
                float prev = avg_cycle_interval_us_.load(std::memory_order_relaxed);
                avg_cycle_interval_us_.store(
                    prev == 0.0f ? elapsed_us
                                 : kCycleAlpha * elapsed_us + (1.0f - kCycleAlpha) * prev,
                    std::memory_order_relaxed);
            }
            last_cycle_tp_ = now;
        }

        // 3. Compute eviction threshold from the bytes the L1 holds beyond
        //    the budget. The histogram records what an eviction gives back to
        //    the ledger (each entry's own count), so the two are in one unit.
        //    The histogram represents one average chunk (EMA-smoothed), so
        //    thresholdForBytes(overshoot) returns the score below which enough
        //    bytes exist to cover the full overshoot.  When overshoot exceeds
        //    the chunk's total, the threshold goes nuclear (evict all) — this
        //    is correct bang-bang behaviour that converges quickly.
        //    Histogram discretisation naturally over-targets (~×1.2 per bucket),
        //    compensating for inter-sweep admissions.
        {
            float threshold = 0.0f;
            if constexpr (enabled) {
                refreshHeldBytes();
                const uint64_t held = held_bytes_.load(std::memory_order_relaxed);
                if (max_memory_ > 0 && held > max_memory_) {
                    threshold = histogram_.thresholdForBytes(held - max_memory_);
                }
            }
            cached_threshold_.store(threshold, std::memory_order_relaxed);
        }

        // 4. Sweep all repos on that chunk (records into building_histogram_)
        building_histogram_.reset();
        {
            std::shared_lock rlock(registry_mutex_);
            for (const auto& entry : registry_) {
                entry.sweep_fn(chunk_id);
            }
        }

        // 5. Merge building histogram into persistent (EMA)
        histogram_.mergeEMA(building_histogram_, config_.histogram_alpha);

        // 6. Read the bytes the L1 holds once evicted entries are freed, so
        //    isOverBudget() reflects them at once.
        refreshHeldBytes();

#if RELAIS_ENABLE_METRICS
        auto sweep_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - sweep_t0).count());
        sweep_counters_.record(sweep_ns);
#endif
    }

    /// Asks the maintenance thread for one global sweep. The notify makes a
    /// system call only when the thread sleeps: 1-2 µs, once every
    /// kCleanupMask+1 ticks.
    void requestSweep() noexcept {
        // Release: the sweep that serves this request sees what the ticking
        // thread wrote before it (the entry it stored, the modification it
        // tracked).
        requested_.fetch_add(1, std::memory_order_release);
        requested_.notify_one();
    }

    /// Starts the maintenance thread, under maintenance_mu_, as the first tier
    /// enrolls. The epoch and parlay's pool of thread ids are built first, on
    /// this thread, while that tier is under construction: they outlive every
    /// enrolled tier, and so the thread, stopped as the first one goes.
    void startMaintenance() {
        (void)epoch::internal::get_epoch();
        (void)parlay::num_thread_ids();
        maintenance_ = std::jthread([this](std::stop_token stop) { serveSweeps(stop); });
    }

    /// The forced pass: sweep_fn on every chunk. The caller holds sweep_mu_.
    size_t passLocked(size_t (*sweep_fn)(long chunk_id)) {
        size_t removed = 0;
        for (long chunk_id = 0; chunk_id < config_.chunk_count; ++chunk_id) {
            removed += sweep_fn(chunk_id);
        }
        // As a sweep ends: what the pass freed is seen by isOverBudget() at once.
        refreshHeldBytes();
        return removed;
    }

    /// The maintenance thread, until stopped: the passes asked for (see
    /// requestPass), then one global sweep per request. More than a cycle of
    /// sweeps behind (a sweep per chunk), it drops the oldest requests: each
    /// chunk is still swept once per cycle of ticks, and the delay never
    /// exceeds one cycle.
    ///
    /// requested_ counts the sweeps and the passes asked for, served_ those
    /// served: with no pass queued, their difference is the sweeps due.
    void serveSweeps(const std::stop_token& stop) {
        runtime::detail::nameThisThread("relais-sweep");
        // Holds parlay's pool of thread ids until the thread ends.
        (void)parlay::my_thread_id();
        for (;;) {
            const uint32_t requested = requested_.load(std::memory_order_acquire);
            if (stop.stop_requested()) return;
            if (requested == served_.load(std::memory_order_relaxed)) {
                requested_.wait(requested, std::memory_order_acquire);
                continue;
            }
            yieldToPasses();
            std::lock_guard exclusion(sweep_mu_);
            uint32_t served = served_.load(std::memory_order_relaxed);
            size_t (*pass)(long) = nullptr;
            uint32_t due;
            {
                std::lock_guard lock(passes_mu_);
                if (!pass_queue_.empty()) {
                    // Off the queue as it starts: a request from now on asks
                    // for another pass.
                    pass = pass_queue_.front();
                    pass_queue_.erase(pass_queue_.begin());
                }
                due = requested_.load(std::memory_order_acquire);
            }
            if (pass) {
                try {
                    (void)passLocked(pass);
                } catch (...) {
                    logSweepFailure("a requested sweep pass");
                }
            } else {
                if (due == served) continue;  // a reset dropped them
                const auto cycle = static_cast<uint32_t>(config_.chunk_count);
                if (due - served > cycle) served = due - cycle;
                try {
                    sweepLocked();
                } catch (...) {
                    // A sweep that fails (an allocation) is dropped, and the
                    // next one starts over: the thread must not end.
                    logSweepFailure("a maintenance sweep");
                }
            }
            // Release: a thread that waits for this sweep or pass sees its
            // effects.
            served_.store(served + 1, std::memory_order_release);
            served_.notify_all();
        }
    }

    /// Takes sweep_mu_ for a forced pass. While a pass waits for it, the
    /// maintenance thread does not start another sweep: the pass waits for
    /// the sweep in progress at most.
    [[nodiscard]] std::unique_lock<std::mutex> lockForPass() {
        passes_waiting_.fetch_add(1);
        // Once the lock is held (or its wait has thrown).
        struct Arrived {
            std::atomic<uint32_t>& waiting;
            ~Arrived() {
                waiting.fetch_sub(1);
                waiting.notify_all();
            }
        } arrived{passes_waiting_};
        return std::unique_lock(sweep_mu_);
    }

    /// Waits while a forced pass waits for sweep_mu_: a pass counts itself
    /// before it waits for the lock, and goes before the next sweep.
    void yieldToPasses() noexcept {
        for (uint32_t waiting; (waiting = passes_waiting_.load()) != 0; ) {
            passes_waiting_.wait(waiting);
        }
    }

    /// Called in a catch handler, on the maintenance thread. Logging may
    /// allocate: it never throws out of here.
    static void logSweepFailure(const char* what) noexcept {
        try {
            try {
                throw;
            } catch (const std::exception& e) {
                RELAIS_LOG_ERROR << "relais: " << what << " failed: " << e.what();
            } catch (...) {
                RELAIS_LOG_ERROR << "relais: " << what << " failed";
            }
        } catch (...) {}
    }

    static size_t readMaxMemoryFromEnv() {
        if (auto* env = std::getenv("RELAIS_L1_MAX_MEMORY")) {
            char* end = nullptr;
            auto v = std::strtoull(env, &end, 10);
            if (end != env && v > 0) return static_cast<size_t>(v);
        }
        return 0;
    }

    /// Reset all global state for test isolation, between two sweeps. The
    /// sweeps and passes not served yet are dropped: none is pending after it.
    void reset() {
        auto exclusion = lockForPass();
        cached_threshold_.store(0.0f, std::memory_order_relaxed);
        histogram_.reset();
        building_histogram_.reset();
        insertion_counter_.store(0, std::memory_order_relaxed);
        sweep_cursor_.store(0, std::memory_order_relaxed);
        last_cycle_tp_ = {};
        avg_cycle_interval_us_.store(0.0f, std::memory_order_relaxed);
        refreshHeldBytes();
#if RELAIS_ENABLE_METRICS
        sweep_counters_.reset();
#endif
        {
            std::lock_guard lock(passes_mu_);
            pass_queue_.clear();
            served_.store(requested_.load(std::memory_order_relaxed), std::memory_order_release);
        }
        served_.notify_all();
        // Registry and max_memory_ intentionally NOT cleared.
    }

    GDSFConfig config_{};
    size_t max_memory_;

    // Repo registry (reader-writer lock: enroll=write, threshold/sweep=read)
    mutable std::shared_mutex registry_mutex_;
    std::vector<detail::RepoRegistryEntry> registry_;

    // Histogram-based threshold
    detail::ScoreHistogram histogram_{};           // persistent, EMA-smoothed
    detail::ScoreHistogram building_histogram_{};  // temporary, rebuilt each sweep
    std::atomic<float> cached_threshold_{0.0f};

    // Deterministic insertion counter (replaces probabilistic hash-based trigger)
    std::atomic<uint32_t> insertion_counter_{0};

    // Global chunk cursor — all repos sweep the same chunk per sweep round.
    std::atomic<uint32_t> sweep_cursor_{0};

    // Cycle interval tracking (protected by sweep_mu_)
    static constexpr float kCycleAlpha = 0.2f;  // EMA α — half-life ≈ 3 cycles
    std::chrono::steady_clock::time_point last_cycle_tp_{};
    std::atomic<float> avg_cycle_interval_us_{0.0f};

    // The bytes the L1 holds, as the last refresh read them from the ledger,
    // and the bytes admitted since (see refreshHeldBytes). isOverBudget()
    // compares held + admitted to the budget.
    std::atomic<uint64_t> held_bytes_{0};
    std::atomic<uint64_t> admitted_since_refresh_{0};

    // One sweep at a time: held by the maintenance thread for each global
    // sweep, and by each forced pass. Never taken on the request path.
    std::mutex sweep_mu_;
    // Forced passes waiting for sweep_mu_ (see lockForPass).
    std::atomic<uint32_t> passes_waiting_{0};

    // Global sweeps requested by the ticks, and passes by requestPass, served
    // by the maintenance thread. served_ changes under sweep_mu_ only.
    std::atomic<uint32_t> requested_{0};
    std::atomic<uint32_t> served_{0};

    // The passes asked for and not started, oldest first; one per sweep_fn at
    // most. A push and its increment of requested_ are made under passes_mu_.
    std::mutex passes_mu_;
    std::vector<size_t (*)(long chunk_id)> pass_queue_;

    // The maintenance thread. The policy is never destroyed: the thread is
    // stopped by stopMaintenance() only.
    std::mutex maintenance_mu_;
    std::jthread maintenance_;
    bool maintenance_stopped_ = false;

#if RELAIS_ENABLE_METRICS
    detail::SweepCounters sweep_counters_;
#endif

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
public:
    /// Reset all global state for test isolation (test-only).
    void resetForTesting() { reset(); }

    /// Waits until the maintenance thread has served the sweeps and the passes
    /// requested before the call; at once if it does not run (test-only).
    void awaitSweeps() {
        const uint32_t due = requested_.load(std::memory_order_relaxed);
        {
            std::lock_guard lock(maintenance_mu_);
            if (!maintenance_.joinable()) return;
        }
        for (uint32_t served;
             static_cast<int32_t>(due - (served = served_.load(std::memory_order_acquire))) > 0; ) {
            served_.wait(served, std::memory_order_acquire);
        }
    }

    /// awaitSweeps(), then one global sweep on the calling thread, under the
    /// exclusion (test-only).
    void sweepNow() {
        awaitSweeps();
        auto exclusion = lockForPass();
        sweepLocked();
    }

    /// Expose histograms for testing.
    detail::ScoreHistogram& persistentHistogram() { return histogram_; }
    const detail::ScoreHistogram& persistentHistogram() const { return histogram_; }
    detail::ScoreHistogram& buildingHistogram() { return building_histogram_; }
#endif
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_GDSF_POLICY_H
