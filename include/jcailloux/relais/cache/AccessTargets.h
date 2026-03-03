#ifndef JCX_RELAIS_CACHE_ACCESS_TARGETS_H
#define JCX_RELAIS_CACHE_ACCESS_TARGETS_H

#include <atomic>
#include <cstdint>

namespace jcailloux::relais::cache {

/// Per-chunk fixed-size array of atomic<void*> for safe TL counter indirection.
/// Direct-mapped by hash bits. Allocated lazily on first access.
///
/// Thread safety:
/// - data_ is set once via CAS in ensureAllocated(), never changes after
/// - registerAndSlot/unregisterTarget: concurrent writers to the same slot
///   → last writer wins (benign for GDSF scoring)
/// - No resize, no deferred free — data_ pointer is stable for TLS caching
struct AccessTargets {
    static constexpr uint8_t kLog2 = 14;                        // 16K slots
    static constexpr uint32_t kCapacity = uint32_t{1} << kLog2; // 16384
    static constexpr uint32_t kMask = kCapacity - 1;

    std::atomic<std::atomic<void*>*> data_{nullptr};

    ~AccessTargets() {
        delete[] data_.load(std::memory_order_relaxed);
    }

    AccessTargets() = default;
    AccessTargets(const AccessTargets&) = delete;
    AccessTargets& operator=(const AccessTargets&) = delete;

    /// Return slot for hash. nullptr if not allocated.
    /// Cold path: 1 load(acquire).
    std::atomic<void*>* slot(size_t hash) const {
        auto* d = data_.load(std::memory_order_acquire);
        if (!d) [[unlikely]] return nullptr;
        return &d[(hash >> 16) & kMask];
    }

    /// Lazy alloc on first use. CAS prevents double-alloc + memory leak.
    /// _strong: cold path (once per chunk lifetime), no benefit from _weak.
    void ensureAllocated() {
        if (data_.load(std::memory_order_acquire)) return;
        auto* d = new std::atomic<void*>[kCapacity]{};
        std::atomic<void*>* expected = nullptr;
        if (!data_.compare_exchange_strong(expected, d,
                std::memory_order_release, std::memory_order_relaxed)) {
            delete[] d;  // another thread won the race
        }
    }

    /// Register a target and return the slot pointer.
    /// With TLS-cached data_: 1 store(relaxed) + return. ~0.3ns.
    /// Without TLS cache: 1 load(acquire) + 1 store(relaxed) + return.
    /// Relaxed store: the pointed-to GDSFScoreData is already published via
    /// ParlayHash's own release/acquire. Worst case: flush sees stale ptr
    /// or nullptr → benign.
    std::atomic<void*>* registerAndSlot(size_t hash, void* ptr) {
        auto* d = data_.load(std::memory_order_acquire);
        if (!d) [[unlikely]] {
            ensureAllocated();
            d = data_.load(std::memory_order_acquire);
        }
        auto& cell = d[(hash >> 16) & kMask];
        cell.store(ptr, std::memory_order_relaxed);
        return &cell;
    }

    /// Unregister a target: CAS to nullptr (only if still pointing to ptr).
    /// _weak: benign if spurious fail (collision partner keeps its slot).
    void unregisterTarget(size_t hash, void* ptr) {
        auto* d = data_.load(std::memory_order_acquire);
        if (!d) return;
        void* expected = ptr;
        d[(hash >> 16) & kMask].compare_exchange_weak(
            expected, nullptr,
            std::memory_order_release, std::memory_order_relaxed);
    }
};

}  // namespace jcailloux::relais::cache
#endif
