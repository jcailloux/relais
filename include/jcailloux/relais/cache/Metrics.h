#ifndef JCX_RELAIS_CACHE_METRICS_H
#define JCX_RELAIS_CACHE_METRICS_H

#include <atomic>
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
struct StripedCounter {
    static constexpr unsigned kSlots = 8;
    static constexpr unsigned kMask = kSlots - 1;

    struct alignas(64) Slot {
        std::atomic<uint64_t> value{0};
    };

    Slot slots[kSlots];

    void increment() noexcept {
        auto idx = std::hash<std::thread::id>{}(std::this_thread::get_id()) & kMask;
        slots[idx].value.fetch_add(1, std::memory_order_relaxed);
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
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_METRICS_H
