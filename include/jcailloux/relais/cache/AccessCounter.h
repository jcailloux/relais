#ifndef JCAILLOUX_RELAIS_ACCESSCOUNTER_H
#define JCAILLOUX_RELAIS_ACCESSCOUNTER_H

#include <atomic>
#include <cstdint>
#include <new>

namespace jcailloux::relais::cache {

/// Result of AccessCounterMap::record() — indicates why the caller should act.
enum class RecordResult : uint8_t {
    kOk = 0,         // recorded successfully, no action needed
    kCountFull = 1,   // count reached 255 — surgical flush needed
    kMapFull = 2,     // map >= 75% full or probe failure — bulk flush needed
};

// =============================================================================
// ConcurrentStack<T> — lock-free Treiber stack for AccessCounterMap pooling
// =============================================================================

template<typename T>
class ConcurrentStack {
    struct Node {
        T* value;
        Node* next;
    };
    std::atomic<Node*> head_{nullptr};

public:
    void push(T* val) {
        auto* node = new Node{val, nullptr};
        node->next = head_.load(std::memory_order_relaxed);
        while (!head_.compare_exchange_weak(node->next, node,
                std::memory_order_release, std::memory_order_relaxed)) {}
    }

    T* try_pop() {
        auto* node = head_.load(std::memory_order_acquire);
        while (node) {
            if (head_.compare_exchange_weak(node, node->next,
                    std::memory_order_acquire, std::memory_order_relaxed)) {
                auto* val = node->value;
                delete node;
                return val;
            }
        }
        return nullptr;
    }
};

// =============================================================================
// AccessCounterMap — open-addressing hash map for thread-local access counting
//
// Fixed-size: kCapLog2 = 12 → 4096 slots (32KB per map). No grow/shrink.
//
// Tagged pointer slots: each slot is a single std::atomic<uintptr_t> encoding
// both the key pointer (bits 0-55) and the local counter (bits 56-63).
// On x86-64 user-space, heap pointers use ≤47 bits (4-level paging) or ≤56 bits
// (5-level/LA57), so bits 56-63 are always zero for standard allocations.
//
// Single load per probe (vs 2 with separate key/count arrays), halving cache
// line accesses and keeping the hot path compact for inlining.
//
// Relaxed atomics: on x86 these compile to plain mov. Formally correct for
// the case where flush_all() iterates maps owned by active threads.
// =============================================================================

class AccessCounterMap {
    std::atomic<uintptr_t>* slots_;
    // size_ is read by flush_all (via size()) while the owning thread modifies
    // it in record()/flush(). Relaxed atomic (plain mov on x86) — zero overhead.
    std::atomic<uint16_t> size_{0};
    std::atomic<bool> flush_active_{false};  // try-lock: owner flush vs flush_all

    static constexpr int kCountShift = 56;
    static constexpr uintptr_t kPtrMask = (uintptr_t{1} << kCountShift) - 1;

public:
    static constexpr uint8_t kCapLog2 = 12;              // 4096 slots
    static constexpr uint16_t kCapacity = uint16_t{1} << kCapLog2;  // 4096
    static constexpr uint16_t kMask = kCapacity - 1;

    // Intrusive pointers for pool (Treiber stack) and registry (lock-free list)
    AccessCounterMap* pool_next_{nullptr};
    std::atomic<AccessCounterMap*> reg_next_{nullptr};

    AccessCounterMap() {
        slots_ = new std::atomic<uintptr_t>[kCapacity];
        for (uint16_t i = 0; i < kCapacity; ++i)
            slots_[i].store(0, std::memory_order_relaxed);
    }

    ~AccessCounterMap() {
        delete[] slots_;
    }

    AccessCounterMap(const AccessCounterMap&) = delete;
    AccessCounterMap& operator=(const AccessCounterMap&) = delete;

    /// Hot path: record an access to entry_ptr.
    /// Returns kOk normally, kCountFull if count=255, kMapFull if >= 75% full.
    /// Single atomic load per probe, single store on match — minimal cache footprint.
    RecordResult record(void* entry_ptr, size_t hash) {
        uint16_t slot = static_cast<uint16_t>(hash) & kMask;
        uintptr_t target = reinterpret_cast<uintptr_t>(entry_ptr);

        for (uint16_t i = 0; i < kCapacity; ++i) {
            uintptr_t tagged = slots_[slot].load(std::memory_order_relaxed);
            uintptr_t ptr_bits = tagged & kPtrMask;

            if (ptr_bits == target) {
                uint8_t c = static_cast<uint8_t>(tagged >> kCountShift);
                if (c == 255) [[unlikely]] return RecordResult::kCountFull;
                slots_[slot].store(
                    target | (static_cast<uintptr_t>(c + 1) << kCountShift),
                    std::memory_order_relaxed);
                return RecordResult::kOk;
            }
            if (tagged == 0) {
                slots_[slot].store(
                    target | (uintptr_t{1} << kCountShift),
                    std::memory_order_relaxed);
                uint16_t sz = size_.load(std::memory_order_relaxed) + 1;
                size_.store(sz, std::memory_order_relaxed);
                return sz >= (kCapacity - (kCapacity >> 2))  // 75% = 3072
                    ? RecordResult::kMapFull : RecordResult::kOk;
            }
            slot = (slot + 1) & kMask;
        }
        return RecordResult::kMapFull;
    }

    /// Surgical flush of a single entry identified by slot_ptr + hash.
    /// CAS-based: zeroes the count bits on match, calls fn with the old count.
    /// Returns the count that was flushed (0 if not found or lock contention).
    /// Uses flush_active_ try-lock: a concurrent try_flush may be iterating
    /// slots_, so we must not overlap.
    template<typename Fn>
    uint8_t flush_one(void* slot_ptr, size_t hash, Fn&& fn) {
        bool expected = false;
        if (!flush_active_.compare_exchange_strong(expected, true,
                std::memory_order_acquire, std::memory_order_relaxed))
            return 0;  // another flush in progress — it will drain our count

        uint16_t idx = static_cast<uint16_t>(hash) & kMask;
        uintptr_t target = reinterpret_cast<uintptr_t>(slot_ptr);
        uint8_t result = 0;

        for (uint16_t i = 0; i < kCapacity; ++i) {
            uintptr_t tagged = slots_[idx].load(std::memory_order_relaxed);
            uintptr_t ptr_bits = tagged & kPtrMask;
            if (ptr_bits == target) {
                uint8_t count = static_cast<uint8_t>(tagged >> kCountShift);
                if (count > 0) {
                    if (slots_[idx].compare_exchange_strong(
                            tagged, target, std::memory_order_relaxed))
                        fn(slot_ptr, count);
                }
                result = count;
                break;
            }
            if (tagged == 0) break;
            idx = (idx + 1) & kMask;
        }

        flush_active_.store(false, std::memory_order_release);
        return result;
    }

    /// Try to flush this map. Returns false if another flush is in progress.
    template<typename Fn>
    bool try_flush(Fn&& fn) {
        bool expected = false;
        if (!flush_active_.compare_exchange_strong(expected, true,
                std::memory_order_acquire, std::memory_order_relaxed))
            return false;
        flush(std::forward<Fn>(fn));
        flush_active_.store(false, std::memory_order_release);
        return true;
    }

    uint16_t size() const { return size_.load(std::memory_order_relaxed); }

private:
    /// Drain all slots, call fn(void*, uint8_t), clear.
    /// Must be called under try-lock (flush_active_).
    template<typename Fn>
    void flush(Fn&& fn) {
        for (uint16_t i = 0; i < kCapacity; ++i) {
            uintptr_t tagged = slots_[i].load(std::memory_order_relaxed);
            if (tagged != 0) {
                void* ptr = reinterpret_cast<void*>(tagged & kPtrMask);
                uint8_t count = static_cast<uint8_t>(tagged >> kCountShift);
                if (count > 0) fn(ptr, count);
                slots_[i].store(0, std::memory_order_relaxed);
            }
        }
        size_.store(0, std::memory_order_relaxed);
    }
};

// =============================================================================
// ChunkAccessCounter — per-chunk pool + registry for AccessCounterMaps
//
// Pool: Treiber stack for map reuse across thread lifetimes.
// Registry: lock-free intrusive list of ALL maps (for flush_all iteration).
// =============================================================================

class ChunkAccessCounter {
    ConcurrentStack<AccessCounterMap> pool_;
    std::atomic<AccessCounterMap*> registry_{nullptr};

public:
    /// Acquire a map: try pool first, otherwise allocate + register.
    AccessCounterMap* acquire() {
        auto* m = pool_.try_pop();
        if (!m) {
            m = new AccessCounterMap();
            // Register in the intrusive list (push to head, lock-free)
            auto* expected = registry_.load(std::memory_order_relaxed);
            m->reg_next_.store(expected, std::memory_order_relaxed);
            while (!registry_.compare_exchange_weak(
                        expected, m,
                        std::memory_order_release,
                        std::memory_order_relaxed)) {
                m->reg_next_.store(expected, std::memory_order_relaxed);
            }
        }
        return m;
    }

    /// Release a map back to the pool.
    void release(AccessCounterMap* m) {
        pool_.push(m);
    }

    /// Flush all registered maps: iterate the intrusive registry, flush each.
    /// Skips maps whose owner is concurrently flushing (counts are being
    /// drained anyway, so no data is lost).
    template<typename Fn>
    void flush_all(Fn&& fn) {
        auto* m = registry_.load(std::memory_order_acquire);
        while (m) {
            m->try_flush(fn);
            m = m->reg_next_.load(std::memory_order_acquire);
        }
    }

    /// Surgical flush of a single slot across all registered maps.
    /// Iterates the registry and calls flush_one on each map.
    template<typename Fn>
    void flush_one_all(void* slot_ptr, size_t hash, Fn&& fn) {
        auto* m = registry_.load(std::memory_order_acquire);
        while (m) {
            m->flush_one(slot_ptr, hash, fn);
            m = m->reg_next_.load(std::memory_order_acquire);
        }
    }
};

}  // namespace jcailloux::relais::cache

#endif //JCAILLOUX_RELAIS_ACCESSCOUNTER_H
