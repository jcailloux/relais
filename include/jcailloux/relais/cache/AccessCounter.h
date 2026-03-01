#ifndef JCAILLOUX_RELAIS_ACCESSCOUNTER_H
#define JCAILLOUX_RELAIS_ACCESSCOUNTER_H

#include <atomic>
#include <cstdint>
#include <new>

namespace jcailloux::relais::cache {

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
    uint16_t size_{0};
    uint8_t cap_log2_{4};  // start at 16 slots

    static constexpr int kCountShift = 56;
    static constexpr uintptr_t kPtrMask = (uintptr_t{1} << kCountShift) - 1;

public:
    // Intrusive pointers for pool (Treiber stack) and registry (lock-free list)
    AccessCounterMap* pool_next_{nullptr};
    std::atomic<AccessCounterMap*> reg_next_{nullptr};

    AccessCounterMap() {
        allocSlots(cap_log2_);
    }

    ~AccessCounterMap() {
        delete[] slots_;
    }

    AccessCounterMap(const AccessCounterMap&) = delete;
    AccessCounterMap& operator=(const AccessCounterMap&) = delete;

    /// Hot path: record an access to entry_ptr. Returns true if half-full (flush needed).
    /// Single atomic load per probe, single store on match — minimal cache footprint.
    bool record(void* entry_ptr, size_t hash) {
        uint16_t cap = capacity();
        uint16_t mask = cap - 1;
        uint16_t slot = static_cast<uint16_t>(hash) & mask;
        uintptr_t target = reinterpret_cast<uintptr_t>(entry_ptr);

        for (uint16_t i = 0; i < cap; ++i) {
            uintptr_t tagged = slots_[slot].load(std::memory_order_relaxed);
            uintptr_t ptr_bits = tagged & kPtrMask;

            if (ptr_bits == target) {
                // Found: increment local counter (count in bits 56-63)
                uint8_t c = static_cast<uint8_t>(tagged >> kCountShift);
                if (c == 255) [[unlikely]] return true;
                slots_[slot].store(
                    target | (static_cast<uintptr_t>(c + 1) << kCountShift),
                    std::memory_order_relaxed);
                return false;
            }
            if (tagged == 0) {
                // Empty slot: insert with count=1
                slots_[slot].store(
                    target | (uintptr_t{1} << kCountShift),
                    std::memory_order_relaxed);
                ++size_;
                return size_ >= (cap >> 1);
            }
            slot = (slot + 1) & mask;
        }
        // Table full (shouldn't happen if we flush at half-full)
        return true;
    }

    /// Flush: iterate all slots, call fn(void*, uint8_t) for non-empty entries, then clear.
    /// Optionally resize to new_cap_log2 (0 = keep current, or grow/shrink).
    template<typename Fn>
    void flush(Fn&& fn, uint8_t new_cap_log2 = 0) {
        uint16_t cap = capacity();
        for (uint16_t i = 0; i < cap; ++i) {
            uintptr_t tagged = slots_[i].load(std::memory_order_relaxed);
            if (tagged != 0) {
                void* ptr = reinterpret_cast<void*>(tagged & kPtrMask);
                uint8_t count = static_cast<uint8_t>(tagged >> kCountShift);
                if (count > 0) fn(ptr, count);
                slots_[i].store(0, std::memory_order_relaxed);
            }
        }
        size_ = 0;

        if (new_cap_log2 != 0 && new_cap_log2 != cap_log2_) {
            delete[] slots_;
            allocSlots(new_cap_log2);
            cap_log2_ = new_cap_log2;
        }
    }

    uint16_t size() const { return size_; }
    uint8_t capLog2() const { return cap_log2_; }
    uint16_t capacity() const { return uint16_t{1} << cap_log2_; }

private:
    void allocSlots(uint8_t log2) {
        uint16_t cap = uint16_t{1} << log2;
        slots_ = new std::atomic<uintptr_t>[cap];
        for (uint16_t i = 0; i < cap; ++i)
            slots_[i].store(0, std::memory_order_relaxed);
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
    template<typename Fn>
    void flush_all(Fn&& fn) {
        auto* m = registry_.load(std::memory_order_acquire);
        while (m) {
            // Compute optimal capacity for next period:
            // grow x2 if half-full, shrink if <1/8 used (min 16 slots)
            uint8_t new_log2 = 0;
            uint16_t sz = m->size();
            uint8_t cur_log2 = m->capLog2();
            if (sz >= (m->capacity() >> 1) && cur_log2 < 12) {
                new_log2 = cur_log2 + 1;  // grow
            } else if (sz < (m->capacity() >> 3) && cur_log2 > 4) {
                new_log2 = cur_log2 - 1;  // shrink
            }
            m->flush(fn, new_log2);
            m = m->reg_next_.load(std::memory_order_acquire);
        }
    }
};

}  // namespace jcailloux::relais::cache

#endif //JCAILLOUX_RELAIS_ACCESSCOUNTER_H