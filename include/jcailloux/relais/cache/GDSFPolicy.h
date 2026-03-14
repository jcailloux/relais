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
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "jcailloux/relais/cache/CacheMetadata.h"
#include "jcailloux/relais/cache/Metrics.h"
#include "jcailloux/relais/runtime/CachedHeap.h"
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
    bool (*sweep_fn)(long chunk_id);  // cleanup given chunk, returns true if evicted something
    size_t (*size_fn)();              // current L1 cache size (entry count)
    const char* name;                 // compile-time repo name (for logging)
};

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
    void configure(const GDSFConfig& cfg) {
        config_ = cfg;
        if (cfg.max_memory > 0) max_memory_ = cfg.max_memory;
    }

    const GDSFConfig& config() const { return config_; }

    /// Runtime L1 memory budget (bytes). Read once from RELAIS_L1_MAX_MEMORY env
    /// var at construction, overridable via configure(). Returns 0 if unset (no limit).
    size_t maxMemory() const { return max_memory_; }

    /// True when estimated live heap (+ extra) would exceed the budget.
    /// heap_cached + admitted_since_tick + extra ≥ budget.
    /// CachedHeap is refreshed every ~100ms; admitted_since_tick_ bridges
    /// the gap by tracking bytes admitted since the last refresh.
    /// Pass est_bytes as extra to pre-check that an admission won't overshoot.
    /// Cost: 2 relaxed loads + add (~2ns). Only called on cache miss path.
    bool isOverBudget(uint64_t extra = 0) const {
        if (max_memory_ == 0) return false;
        return runtime::CachedHeap::bytes()
             + admitted_since_tick_.load(std::memory_order_relaxed)
             + extra
             >= max_memory_;
    }

    /// Record admitted bytes.  Called on real L1 insertion only (not ghosts).
    void recordAdmission(uint32_t est_bytes) {
        admitted_since_tick_.fetch_add(est_bytes, std::memory_order_relaxed);
    }

    /// Reset admitted counter after a fresh heap measurement.
    /// Called by RuntimeThread (every ~100ms) and by sweep (step 6).
    void onHeapRefresh() {
        admitted_since_tick_.store(0, std::memory_order_relaxed);
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

    /// Tick the global insertion counter. Fires a global sweep every
    /// kCleanupMask+1 ticks. Three tick sources:
    ///   - Real L1 insertion (under budget) — maintenance sweeps
    ///   - New ghost creation (over budget, first appearance) — tracking
    ///   - Qualified ghost re-access (over budget, score ≥ threshold) — eviction demand
    void tickInsertion() {
        if (kCleanupFrequencyLog2 > 0
                && (insertion_counter_.fetch_add(1, std::memory_order_relaxed)
                    & kCleanupMask) == kCleanupMask) {
            sweep();
        }
    }

    // =====================================================================
    // Repo Registry
    // =====================================================================

    /// Register a repo for global coordination (threshold, sweep).
    /// Called once per LocalRepo instantiation via std::call_once.
    void enroll(RepoRegistryEntry entry) {
        std::unique_lock lock(registry_mutex_);
        registry_.push_back(std::move(entry));
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
    // Histogram Recording (during sweep, protected by sweep_flag_)
    // =====================================================================

    /// Record an entry into the building histogram during sweep.
    /// Called by cleanup predicates for ALL entries (evicted + kept).
    /// Only called during sweep which is serialized by sweep_flag_.
    void recordEntry(float score, size_t entry_bytes) {
        building_histogram_.record(score, entry_bytes);
    }

    // =====================================================================
    // Global Sweep
    // =====================================================================

    /// Global sweep: iterates all repos, sweeps one chunk per repo.
    /// Uses atomic_flag for instant abandon if a sweep is already in progress.
    void sweep() {
        if (sweep_flag_.test_and_set(std::memory_order_acquire)) return;

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

        // 3. Compute eviction threshold from heap pressure.
        //    The histogram represents one average chunk (EMA-smoothed), so
        //    thresholdForBytes(overshoot) returns the score below which enough
        //    bytes exist to cover the full overshoot.  When overshoot exceeds
        //    the chunk's total, the threshold goes nuclear (evict all) — this
        //    is correct bang-bang behaviour that converges quickly.
        //    Histogram discretisation naturally over-targets (~×1.2 per bucket),
        //    compensating for inter-sweep admissions.
        {
            float threshold = 0.0f;
            if (max_memory_ > 0) {
                auto heap = runtime::CachedHeap::bytes();
                if (heap > max_memory_) {
                    threshold = histogram_.thresholdForBytes(heap - max_memory_);
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

        // 6. Refresh heap + reset admitted counter so isOverBudget()
        //    immediately reflects the post-eviction state.
        runtime::CachedHeap::tick();
        onHeapRefresh();

#if RELAIS_ENABLE_METRICS
        auto sweep_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - sweep_t0).count());
        sweep_counters_.record(sweep_ns);
#endif

        sweep_flag_.clear(std::memory_order_release);
    }

#if RELAIS_ENABLE_METRICS
    const SweepCounters& sweepCounters() const noexcept { return sweep_counters_; }
    SweepCounters& sweepCounters() noexcept { return sweep_counters_; }
#endif

private:
    GDSFPolicy() : max_memory_(readMaxMemoryFromEnv()) {
        runtime::RuntimeThread::on_heap_refresh = [] () noexcept {
            GDSFPolicy::instance().onHeapRefresh();
        };
    }

    static size_t readMaxMemoryFromEnv() {
        if (auto* env = std::getenv("RELAIS_L1_MAX_MEMORY")) {
            char* end = nullptr;
            auto v = std::strtoull(env, &end, 10);
            if (end != env && v > 0) return static_cast<size_t>(v);
        }
        return 0;
    }

    /// Reset all global state for test isolation.
    void reset() {
        cached_threshold_.store(0.0f, std::memory_order_relaxed);
        histogram_.reset();
        building_histogram_.reset();
        insertion_counter_.store(0, std::memory_order_relaxed);
        sweep_cursor_.store(0, std::memory_order_relaxed);
        last_cycle_tp_ = {};
        avg_cycle_interval_us_.store(0.0f, std::memory_order_relaxed);
        admitted_since_tick_.store(0, std::memory_order_relaxed);
#if RELAIS_ENABLE_METRICS
        sweep_counters_.reset();
#endif
        // Registry and max_memory_ intentionally NOT cleared.
    }

    GDSFConfig config_{};
    size_t max_memory_;

    // Repo registry (reader-writer lock: enroll=write, threshold/sweep=read)
    mutable std::shared_mutex registry_mutex_;
    std::vector<RepoRegistryEntry> registry_;

    // Histogram-based threshold
    ScoreHistogram histogram_{};              // persistent, EMA-smoothed
    ScoreHistogram building_histogram_{};     // temporary, rebuilt each sweep
    std::atomic<float> cached_threshold_{0.0f};

    // Deterministic insertion counter (replaces probabilistic hash-based trigger)
    std::atomic<uint32_t> insertion_counter_{0};

    // Global chunk cursor — all repos sweep the same chunk per sweep round.
    std::atomic<uint32_t> sweep_cursor_{0};

    // Cycle interval tracking (protected by sweep_flag_, single writer)
    static constexpr float kCycleAlpha = 0.2f;  // EMA α — half-life ≈ 3 cycles
    std::chrono::steady_clock::time_point last_cycle_tp_{};
    std::atomic<float> avg_cycle_interval_us_{0.0f};

    // Admitted bytes since last CachedHeap refresh.  Reset by onHeapRefresh()
    // (called by RuntimeThread every ~100ms and by sweep after eviction).
    // isOverBudget() uses heap + admitted ≥ budget to bridge measurement gaps.
    std::atomic<uint64_t> admitted_since_tick_{0};

    // Sweep serialization — lock-free, guaranteed on all platforms
    std::atomic_flag sweep_flag_{};

#if RELAIS_ENABLE_METRICS
    SweepCounters sweep_counters_;
#endif

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
public:
    /// Reset all global state for test isolation (test-only).
    void resetForTesting() { reset(); }

    /// Expose histograms for testing.
    ScoreHistogram& persistentHistogram() { return histogram_; }
    const ScoreHistogram& persistentHistogram() const { return histogram_; }
    ScoreHistogram& buildingHistogram() { return building_histogram_; }
#endif
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_GDSF_POLICY_H
