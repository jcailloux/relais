#ifndef JCX_RELAIS_CACHE_METRICS_H
#define JCX_RELAIS_CACHE_METRICS_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#if RELAIS_ENABLE_METRICS
#define RELAIS_METRICS_INC(counter) (counter).increment()
#else
#define RELAIS_METRICS_INC(counter) ((void)0)
#endif

namespace jcailloux::relais::cache {

/// Striped atomic counter — 8 cache-line-aligned slots to minimize contention.
/// Total footprint: ~512 bytes per counter.
///
/// Slot selection uses a sequential TLS index (0,1,2,...) instead of
/// hash(thread_id) & 7 — on glibc, pthread_t addresses are 8MB-aligned so
/// the low 3 bits are always 0, mapping all threads to the same slot.
struct StripedCounter {
    static constexpr unsigned kSlots = 8;
    static constexpr unsigned kMask = kSlots - 1;

    struct alignas(64) Slot {
        std::atomic<uint64_t> value{0};
    };

    Slot slots[kSlots];

    void increment() noexcept {
        static std::atomic<unsigned> next_idx{0};
        static thread_local unsigned my_idx =
            next_idx.fetch_add(1, std::memory_order_relaxed) & kMask;
        // Relaxed load+store instead of fetch_add: compiles to plain mov+inc+mov
        // on x86 (no lock prefix). Safe because each thread has its own slot
        // (≤8 threads); with >8 threads some increments may be lost — acceptable
        // for metrics counters.
        auto& s = slots[my_idx].value;
        s.store(s.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t load() const noexcept {
        uint64_t total = 0;
        for (unsigned i = 0; i < kSlots; ++i)
            total += slots[i].value.load(std::memory_order_relaxed);
        return total;
    }

    void reset() noexcept {
        for (unsigned i = 0; i < kSlots; ++i)
            slots[i].value.store(0, std::memory_order_relaxed);
    }
};

/// Sweep duration counters — simple atomics (sweeps are serialized by sweep_flag_).
struct SweepCounters {
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> total_ns{0};
    std::atomic<uint64_t> last_ns{0};
    std::atomic<uint64_t> max_ns{0};

    void record(uint64_t duration_ns) noexcept {
        count.fetch_add(1, std::memory_order_relaxed);
        total_ns.fetch_add(duration_ns, std::memory_order_relaxed);
        last_ns.store(duration_ns, std::memory_order_relaxed);
        auto prev = max_ns.load(std::memory_order_relaxed);
        while (duration_ns > prev
               && !max_ns.compare_exchange_weak(prev, duration_ns,
                       std::memory_order_relaxed)) {}
    }

    void reset() noexcept {
        count.store(0, std::memory_order_relaxed);
        total_ns.store(0, std::memory_order_relaxed);
        last_ns.store(0, std::memory_order_relaxed);
        max_ns.store(0, std::memory_order_relaxed);
    }
};

/// L1 cache hit/miss counter pair.
struct L1Counters {
    StripedCounter hits;
    StripedCounter misses;
};

/// L2 cache hit/miss counter pair.
struct L2Counters {
    StripedCounter hits;
    StripedCounter misses;
};

/// Immutable snapshot of all cache metrics for a Repo instantiation.
struct MetricsSnapshot {
    uint64_t l1_hits = 0;
    uint64_t l1_misses = 0;
    uint64_t l2_hits = 0;
    uint64_t l2_misses = 0;
    uint64_t list_l1_hits = 0;
    uint64_t list_l1_misses = 0;
    uint64_t list_l2_hits = 0;
    uint64_t list_l2_misses = 0;
    uint64_t sweep_count = 0;
    uint64_t sweep_total_ns = 0;
    uint64_t sweep_last_ns = 0;
    uint64_t sweep_max_ns = 0;

    [[nodiscard]] double l1HitRatio() const noexcept {
        auto total = l1_hits + l1_misses;
        return total ? static_cast<double>(l1_hits) / static_cast<double>(total) : 0.0;
    }

    [[nodiscard]] double l2HitRatio() const noexcept {
        auto total = l2_hits + l2_misses;
        return total ? static_cast<double>(l2_hits) / static_cast<double>(total) : 0.0;
    }

    [[nodiscard]] double listL1HitRatio() const noexcept {
        auto total = list_l1_hits + list_l1_misses;
        return total ? static_cast<double>(list_l1_hits) / static_cast<double>(total) : 0.0;
    }

    [[nodiscard]] double listL2HitRatio() const noexcept {
        auto total = list_l2_hits + list_l2_misses;
        return total ? static_cast<double>(list_l2_hits) / static_cast<double>(total) : 0.0;
    }

    /// Average sweep duration in microseconds (0 if no sweeps).
    [[nodiscard]] double sweepAvgUs() const noexcept {
        return sweep_count > 0
            ? static_cast<double>(sweep_total_ns) / static_cast<double>(sweep_count) / 1000.0
            : 0.0;
    }
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_METRICS_H
