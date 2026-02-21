#ifndef JCX_RELAIS_CACHE_GDSF_POLICY_H
#define JCX_RELAIS_CACHE_GDSF_POLICY_H

#include <atomic>
#include <cassert>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "jcailloux/relais/cache/GDSFMetadata.h"
#include "jcailloux/relais/Log.h"

#ifndef RELAIS_L1_MAX_MEMORY
#define RELAIS_L1_MAX_MEMORY 0
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
    float correction_alpha = 0.3f;
    size_t memory_counter_slots = 64;           // must be power of 2, <= 64
};

// =========================================================================
// Type-erased repo entry for the global registry
// =========================================================================

struct RepoRegistryEntry {
    bool (*sweep_fn)();         // cleanup one chunk, returns true if evicted
    size_t (*size_fn)();        // current L1 cache size (entry count)
    float (*repo_score_fn)();   // current repo_score atomic
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

class GDSFPolicy {
    static constexpr size_t kMaxMemorySlots = 64;
    static constexpr size_t kDecayTableSize = 65;  // indices 0..64

public:
    /// Compile-time memory budget from CMake define.
    /// 0 = GDSF disabled (default). CachedRepo uses this for if constexpr guards.
    static constexpr size_t kMaxMemory = RELAIS_L1_MAX_MEMORY;

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
        computeDecayTable(cfg.decay_rate);
    }

    const GDSFConfig& config() const { return config_; }

    // =====================================================================
    // Repo Registry
    // =====================================================================

    /// Register a repo for global coordination (threshold, emergency cleanup).
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
    // Generation & Decay
    // =====================================================================

    uint32_t generation() const {
        return global_generation_.load(std::memory_order_relaxed);
    }

    /// Called by each repo after a cleanup cycle.
    /// When all repos have ticked once, global_generation increments.
    void tick() {
        uint32_t count = global_counter_.fetch_add(1, std::memory_order_relaxed) + 1;
        size_t nb;
        {
            std::shared_lock lock(registry_mutex_);
            nb = registry_.size();
        }
        if (nb > 0 && count >= nb) {
            global_counter_.store(0, std::memory_order_relaxed);
            global_generation_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    /// Apply lazy decay to an entry's score based on generation gap.
    /// Uses CAS loop to avoid losing concurrent fetch_add increments.
    /// Accepts any GDSF-enabled metadata via GDSFScoreData base reference.
    void decay(const GDSFScoreData& meta) const {
        uint32_t current_gen = global_generation_.load(std::memory_order_relaxed);
        uint32_t last_gen = meta.last_generation.load(std::memory_order_relaxed);

        if (current_gen == last_gen) return;

        uint32_t age = std::min(current_gen - last_gen, uint32_t(64));
        float factor = decay_table_[age];

        meta.last_generation.store(current_gen, std::memory_order_relaxed);

        float old_score = meta.score.load(std::memory_order_relaxed);
        float new_score;
        do {
            new_score = old_score * factor;
        } while (!meta.score.compare_exchange_weak(
            old_score, new_score, std::memory_order_relaxed));
    }

    /// Pre-computed decay factor for a given age (exposed for testing).
    float decayFactor(uint32_t age) const {
        return decay_table_[std::min(age, uint32_t(64))];
    }

    // =====================================================================
    // Threshold
    // =====================================================================

    /// Compute global eviction threshold: weighted average of repo scores
    /// × correction × pressure_factor.
    float threshold() const {
        std::shared_lock lock(registry_mutex_);
        if (registry_.empty()) return 0.0f;

        float weighted_sum = 0.0f;
        size_t total_size = 0;

        for (const auto& entry : registry_) {
            size_t sz = entry.size_fn();
            weighted_sum += entry.repo_score_fn() * static_cast<float>(sz);
            total_size += sz;
        }

        if (total_size == 0) return 0.0f;
        return (weighted_sum / static_cast<float>(total_size))
               * correction_.load(std::memory_order_relaxed)
               * pressureFactor();
    }

    /// Update the correction coefficient after a cleanup cycle.
    /// actual_avg = mean score of survivors after decay.
    /// estimated_avg = repo_score before cleanup (biased by lazy decay).
    void updateCorrection(float actual_avg, float estimated_avg) {
        if (estimated_avg <= 0.0f) return;

        float ratio = actual_avg / estimated_avg;
        float old_corr = correction_.load(std::memory_order_relaxed);
        float new_corr = config_.correction_alpha * ratio
                       + (1.0f - config_.correction_alpha) * old_corr;

        // CAS without retry — EMA converges naturally under contention
        correction_.compare_exchange_weak(old_corr, new_corr, std::memory_order_relaxed);
    }

    float correction() const {
        return correction_.load(std::memory_order_relaxed);
    }

    // =====================================================================
    // Pressure Factor
    // =====================================================================

    /// Memory pressure factor: quadratic scaling from 0 (empty) to 1 (at budget).
    /// Multiplied to threshold: low usage → lax eviction, high usage → aggressive.
    float pressureFactor() const {
        if constexpr (kMaxMemory == 0) return 1.0f;
        else {
            float budget = static_cast<float>(kMaxMemory);
            float usage = static_cast<float>(std::max(int64_t(0), totalMemory()));
            float ratio = usage / budget;
            return std::min(1.0f, ratio * ratio);
        }
    }

    // =====================================================================
    // Memory Tracking (striped counter)
    // =====================================================================

    /// Charge or discharge memory. Positive = allocation, negative = deallocation.
    void charge(int64_t delta) {
        static thread_local uint32_t tl_idx = static_cast<uint32_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
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
        return totalMemory() > static_cast<int64_t>(kMaxMemory);
    }

    // =====================================================================
    // Global Sweep
    // =====================================================================

    /// Global sweep: iterates all repos, sweeps one chunk per repo.
    /// Uses atomic_flag for instant abandon if a sweep is already in progress.
    /// Runs a second pass if memory is still over budget after the first.
    void sweep() {
        if (sweep_flag_.test_and_set(std::memory_order_acquire)) return;

        {
            std::shared_lock rlock(registry_mutex_);
            for (const auto& entry : registry_) {
                entry.sweep_fn();
            }
        }
        global_generation_.fetch_add(1, std::memory_order_relaxed);

        // Second pass if over budget (without releasing the flag)
        if constexpr (kMaxMemory > 0) {
            if (isOverBudget()) {
                RELAIS_LOG_WARN << "GDSF: over budget after sweep ("
                                << totalMemory() << " / " << kMaxMemory
                                << "), running second pass";
                std::shared_lock rlock(registry_mutex_);
                for (const auto& entry : registry_) {
                    entry.sweep_fn();
                }
                global_generation_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        sweep_flag_.clear(std::memory_order_release);
    }

private:
    GDSFPolicy() {
        computeDecayTable(config_.decay_rate);
    }

    void computeDecayTable(float decay_rate) {
        decay_table_[0] = 1.0f;
        for (size_t i = 1; i < kDecayTableSize; ++i) {
            decay_table_[i] = decay_table_[i - 1] * decay_rate;
        }
    }

    /// Reset all global state for test isolation.
    /// Call AFTER evicting all cache entries (so dtor discharge doesn't go negative).
    void reset() {
        global_generation_.store(0, std::memory_order_relaxed);
        global_counter_.store(0, std::memory_order_relaxed);
        correction_.store(1.0f, std::memory_order_relaxed);
        for (size_t i = 0; i < kMaxMemorySlots; ++i) {
            memory_slots_[i].value.store(0, std::memory_order_relaxed);
        }
        // Registry intentionally NOT cleared — repos are static objects.
    }

    GDSFConfig config_{};
    float decay_table_[kDecayTableSize]{};
    size_t memory_slot_count_{kMaxMemorySlots};

    // Striped memory counter — one slot per cache line to eliminate false sharing.
    // Each slot is 64-byte aligned (one cache line), costing 4 KB total (negligible).
    struct alignas(64) MemorySlot {
        std::atomic<int64_t> value{0};
    };
    MemorySlot memory_slots_[kMaxMemorySlots];

    // Repo registry (reader-writer lock: enroll=write, threshold/sweep=read)
    mutable std::shared_mutex registry_mutex_;
    std::vector<RepoRegistryEntry> registry_;

    // Global generation tracking
    std::atomic<uint32_t> global_generation_{0};
    std::atomic<uint32_t> global_counter_{0};

    // Correction coefficient (EMA)
    std::atomic<float> correction_{1.0f};

    // Sweep serialization — lock-free, guaranteed on all platforms
    std::atomic_flag sweep_flag_{};

#ifdef RELAIS_BUILDING_TESTS
    friend struct ::relais_test::TestInternals;
public:
    /// Reset all global state for test isolation (test-only).
    void resetForTesting() { reset(); }
#endif
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_GDSF_POLICY_H
