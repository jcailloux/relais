#ifndef JCX_RELAIS_CACHE_GDSF_POLICY_H
#define JCX_RELAIS_CACHE_GDSF_POLICY_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "jcailloux/relais/cache/GDSFMetadata.h"
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
    float decay_rate = 0.95f;
    float histogram_alpha = 0.3f;            // EMA smoothing for histogram merges
    float admission_pressure = 0.95f;        // ghost gate activates at this pressure (0.0–1.0)
    size_t memory_counter_slots = 64;        // must be power of 2, <= 64
    size_t max_memory = 0;                   // L1 memory budget in bytes (0 = from env / unlimited)
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
    static constexpr int N = 128;
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
    /// Returns 0 when the histogram has no data (cold start: build histogram
    /// before evicting, rather than nuking everything with exp2(kLogMax)).
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
        // Histogram has less data than target — either cold start (no data)
        // or target exceeds the histogram's resolution (one chunk).
        // Return 0 to avoid nuclear eviction; the caller (scaleAndThreshold)
        // is expected to scale the target to the histogram's resolution.
        return 0.0f;
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
    bool (*sweep_fn)();         // cleanup one chunk, returns true if evicted something
    size_t (*size_fn)();        // current L1 cache size (entry count)
    const char* name;           // compile-time repo name (for logging)
};

// =========================================================================
// GDSFPolicy — global singleton managing GDSF eviction coordination
// =========================================================================
//
// Leaking singleton (never destroyed) to avoid static destruction order
// issues: CachedWrapper dtors may fire after static singletons are destroyed.
//
// Thread-safe: all public methods are safe to call concurrently.
//
// Eviction strategy:
//   1. Compute usage_ratio = totalMemory / maxMemory()
//   2. eviction_target_pct(usage_ratio) -> % of memory to free
//   3. histogram_.thresholdForBytes(pct * budget) -> score threshold
//   4. Each repo sweeps 1 chunk, evicting entries with score < threshold
//   5. Building histogram merged into persistent histogram_ via EMA

class GDSFPolicy {
    static constexpr size_t kMaxMemorySlots = 64;

public:
    /// Compile-time GDSF toggle. Controls if constexpr guards in CachedRepo/ListMixin.
    /// When false, all GDSF code paths (metadata, CachedWrapper, scoring) are eliminated.
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
        assert((cfg.memory_counter_slots & (cfg.memory_counter_slots - 1)) == 0
               && "memory_counter_slots must be power of 2");
        assert(cfg.memory_counter_slots <= kMaxMemorySlots
               && "memory_counter_slots exceeds maximum");
        config_ = cfg;
        memory_slot_count_ = cfg.memory_counter_slots;
        if (cfg.max_memory > 0) max_memory_ = cfg.max_memory;
    }

    const GDSFConfig& config() const { return config_; }

    /// Runtime L1 memory budget (bytes). Read once from RELAIS_L1_MAX_MEMORY env
    /// var at construction, overridable via configure(). Returns 0 if unset (no limit).
    size_t maxMemory() const { return max_memory_; }

    /// Memory pressure ratio: totalMemory / maxMemory, clamped to [0, ∞).
    /// Returns 0 when no budget is configured.
    float memoryPressure() const {
        size_t budget = max_memory_;
        if (budget == 0) return 0.0f;
        return static_cast<float>(std::max(int64_t(0), totalMemory()))
             / static_cast<float>(budget);
    }

    /// Pressure-adaptive decay rate: 0.95 − 0.7 × pressure³.
    /// At zero pressure: 0.95 (gentle decay, entries retained longer).
    /// At full pressure: 0.25 (aggressive decay, faster eviction).
    /// Clamped to [0.01, 0.95] to avoid zero/negative values.
    float decayRate() const {
        float p = memoryPressure();
        float rate = 0.95f - 0.7f * p * p * p;
        return std::clamp(rate, 0.01f, 0.95f);
    }

    // =====================================================================
    // Deterministic Cleanup Trigger
    // =====================================================================

    /// Tick the global insertion counter. Fires a global sweep every
    /// kCleanupMask+1 insertions, or immediately when over budget.
    /// Called from every L1 cache insertion (putInCache, ghost creation, list put).
    void tickInsertion() {
        if (kCleanupFrequencyLog2 > 0
                && (insertion_counter_.fetch_add(1, std::memory_order_relaxed)
                    & kCleanupMask) == kCleanupMask) {
            sweep();
        } else if (isOverBudget()) {
            sweep();
        }
    }

    // =====================================================================
    // Repo Registry
    // =====================================================================

    /// Register a repo for global coordination (threshold, sweep).
    /// Called once per CachedRepo instantiation via std::call_once.
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
    // Eviction Target
    // =====================================================================
    //
    // Three-zone continuous quadratic curve:
    //   < 50% usage  ->  0% eviction (no pressure)
    //   50-80% usage ->  0% to 5% eviction (gentle quadratic)
    //   80-100% usage -> 5% to 25% eviction (aggressive quadratic)

    static float eviction_target_pct(float usage) {
        if (usage < 0.50f) return 0.0f;
        if (usage < 0.80f) {
            float t = (usage - 0.50f) / 0.30f;    // 0 -> 1
            return 0.05f * t * t;                   // 0% -> 5%, convex
        }
        float t = std::min((usage - 0.80f) / 0.20f, 1.0f);  // 0 -> 1, clamped
        return 0.05f + 0.20f * t * t;                       // 5% -> 25%, convex
    }

    // =====================================================================
    // Memory Tracking (striped counter)
    // =====================================================================

    /// Charge or discharge memory. Positive = allocation, negative = deallocation.
    void charge(int64_t delta) {
        static thread_local uint32_t tl_idx = static_cast<uint32_t>(
            std::hash<std::thread::id>{}(std::this_thread::get_id()));
        size_t slot = tl_idx++ & (memory_slot_count_ - 1);
        memory_slots_[slot].value.fetch_add(delta, std::memory_order_relaxed);
    }

    /// Sum of all memory counter slots (approximate under contention).
    int64_t totalMemory() const {
        int64_t total = 0;
        for (size_t i = 0; i < memory_slot_count_; ++i) {
            total += memory_slots_[i].value.load(std::memory_order_relaxed);
        }
        return total;
    }

    bool isOverBudget() const {
        return max_memory_ > 0
            && totalMemory() > static_cast<int64_t>(max_memory_);
    }

    /// Memory pressure >= 50% — eviction curve reference point.
    bool hasMemoryPressure() const {
        size_t budget = max_memory_;
        if (budget == 0) return false;
        float usage = static_cast<float>(std::max(int64_t(0), totalMemory()))
                    / static_cast<float>(budget);
        return usage >= 0.50f;
    }

    /// Admission pressure — ghost gate activates above this threshold.
    /// Below this, all fetches are cached directly and the sweep handles eviction.
    /// Default: 0.95 (95% memory usage).
    bool hasAdmissionPressure() const {
        size_t budget = max_memory_;
        if (budget == 0) return false;
        float usage = static_cast<float>(std::max(int64_t(0), totalMemory()))
                    / static_cast<float>(budget);
        return usage >= config_.admission_pressure;
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
    /// Runs a second pass if memory is still over budget after the first.
    void sweep() {
        if (sweep_flag_.test_and_set(std::memory_order_acquire)) return;

        // 1. Compute eviction target from current memory usage
        float usage_ratio = 0.0f;
        size_t budget = max_memory_;
        if constexpr (enabled) {
            if (budget > 0) {
                usage_ratio = static_cast<float>(std::max(int64_t(0), totalMemory()))
                            / static_cast<float>(budget);
            }
        }
        float pct = eviction_target_pct(usage_ratio);
        size_t bytes_to_free = (pct > 0.0f && budget > 0)
            ? static_cast<size_t>(pct * static_cast<float>(budget))
            : 0;

        // 2. Derive threshold from persistent histogram (EMA-smoothed).
        //    The histogram represents ~1 chunk (EMA of per-chunk snapshots).
        //    Scale bytes_to_free to the histogram's resolution to avoid
        //    nuclear threshold when bytes_to_free > histogram total.
        cached_threshold_.store(
            scaleAndThreshold(bytes_to_free),
            std::memory_order_relaxed);

        // 3. Sweep all repos (each cleans 1 chunk, records into building_histogram_)
        building_histogram_.reset();
        {
            std::shared_lock rlock(registry_mutex_);
            for (const auto& entry : registry_) {
                entry.sweep_fn();
            }
        }

        // 4. Merge building histogram into persistent (EMA)
        histogram_.mergeEMA(building_histogram_, config_.histogram_alpha);

        // 5. Second pass if still over budget
        if constexpr (enabled) {
            if (isOverBudget()) {
                RELAIS_LOG_WARN << "GDSF: over budget after sweep ("
                                << totalMemory() << " / " << budget
                                << "), running second pass";

                // Recompute threshold with max pressure
                float new_pct = eviction_target_pct(1.0f);
                size_t new_bytes = static_cast<size_t>(
                    new_pct * static_cast<float>(budget));
                cached_threshold_.store(
                    scaleAndThreshold(new_bytes),
                    std::memory_order_relaxed);

                building_histogram_.reset();
                {
                    std::shared_lock rlock(registry_mutex_);
                    for (const auto& entry : registry_) {
                        entry.sweep_fn();
                    }
                }
                histogram_.mergeEMA(building_histogram_, config_.histogram_alpha);
            }
        }

        sweep_flag_.clear(std::memory_order_release);
    }

private:
    /// Scale bytes_to_free to the histogram's resolution and compute threshold.
    /// The persistent histogram represents ~1 chunk (EMA of per-chunk snapshots).
    /// Without scaling, bytes_to_free (a fraction of the global budget) often
    /// exceeds the histogram's total, causing thresholdForBytes to return
    /// exp2(kLogMax) — a nuclear threshold that wipes entire chunks.
    /// Scaling: per_chunk_target = bytes_to_free × (hist_total / totalMemory).
    float scaleAndThreshold(size_t bytes_to_free) const {
        if (bytes_to_free == 0) return 0.0f;
        uint64_t hist_total = 0;
        for (int i = 0; i < ScoreHistogram::N; ++i)
            hist_total += histogram_.bytes[i];
        if (hist_total == 0) return 0.0f;       // cold start: build histogram first
        auto tmem = std::max(int64_t(1), totalMemory());
        auto per_chunk = static_cast<size_t>(
            static_cast<double>(bytes_to_free)
            * static_cast<double>(hist_total)
            / static_cast<double>(tmem));
        return histogram_.thresholdForBytes(std::max(per_chunk, size_t(1)));
    }

    GDSFPolicy() : max_memory_(readMaxMemoryFromEnv()) {}

    static size_t readMaxMemoryFromEnv() {
        if (auto* env = std::getenv("RELAIS_L1_MAX_MEMORY")) {
            char* end = nullptr;
            auto v = std::strtoull(env, &end, 10);
            if (end != env && v > 0) return static_cast<size_t>(v);
        }
        return 0;
    }

    /// Reset all global state for test isolation.
    /// Call AFTER evicting all cache entries (so dtor discharge doesn't go negative).
    void reset() {
        cached_threshold_.store(0.0f, std::memory_order_relaxed);
        histogram_.reset();
        building_histogram_.reset();
        for (size_t i = 0; i < kMaxMemorySlots; ++i) {
            memory_slots_[i].value.store(0, std::memory_order_relaxed);
        }
        insertion_counter_.store(0, std::memory_order_relaxed);
        // Registry and max_memory_ intentionally NOT cleared.
    }

    GDSFConfig config_{};
    size_t memory_slot_count_{kMaxMemorySlots};
    size_t max_memory_;

    // Striped memory counter — one slot per cache line to eliminate false sharing.
    // Each slot is 64-byte aligned (one cache line), costing 4 KB total (negligible).
    struct alignas(64) MemorySlot {
        std::atomic<int64_t> value{0};
    };
    MemorySlot memory_slots_[kMaxMemorySlots];

    // Repo registry (reader-writer lock: enroll=write, threshold/sweep=read)
    mutable std::shared_mutex registry_mutex_;
    std::vector<RepoRegistryEntry> registry_;

    // Histogram-based threshold
    ScoreHistogram histogram_{};              // persistent, EMA-smoothed
    ScoreHistogram building_histogram_{};     // temporary, rebuilt each sweep
    std::atomic<float> cached_threshold_{0.0f};

    // Deterministic insertion counter (replaces probabilistic hash-based trigger)
    std::atomic<uint32_t> insertion_counter_{0};

    // Sweep serialization — lock-free, guaranteed on all platforms
    std::atomic_flag sweep_flag_{};

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
