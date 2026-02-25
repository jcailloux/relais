#ifndef JCX_RELAIS_CACHE_GHOST_ENTRY_H
#define JCX_RELAIS_CACHE_GHOST_ENTRY_H

#include <atomic>
#include <cstdint>

namespace jcailloux::relais::cache {

// =============================================================================
// GhostData — compact placeholder for evicted/rejected cache entries (4 bytes)
//
// Packed layout: [has_json:1][has_binary:1][estimated_bytes:30]
// Max representable size: ~1 GiB (2^30). Sufficient for any single entity.
//
// Stored in GhostCacheEntry (inside ChunkMap) alongside GDSFScoreData metadata.
// Total overhead: ~4 bytes per ghost (was 8).
// =============================================================================

struct GhostData {
    std::atomic<uint32_t> packed{0};

    static constexpr uint32_t kBytesMask = 0x3FFF'FFFFu;  // bits 0–29
    static constexpr uint32_t kBinaryBit = 1u << 30;
    static constexpr uint32_t kJsonBit   = 1u << 31;

    GhostData() = default;
    GhostData(uint32_t bytes, uint8_t f)
        : packed((bytes & kBytesMask)
               | ((f & 1) ? kBinaryBit : 0)
               | ((f & 2) ? kJsonBit   : 0)) {}

    // Manual copy/move (std::atomic is non-copyable)
    GhostData(const GhostData& o)
        : packed(o.packed.load(std::memory_order_relaxed)) {}
    GhostData(GhostData&& o) noexcept
        : packed(o.packed.load(std::memory_order_relaxed)) {}
    GhostData& operator=(const GhostData& o) {
        packed.store(o.packed.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
        return *this;
    }
    GhostData& operator=(GhostData&& o) noexcept {
        packed.store(o.packed.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
        return *this;
    }

    uint32_t estimated_bytes() const {
        return packed.load(std::memory_order_relaxed) & kBytesMask;
    }

    uint8_t flags() const {
        auto v = packed.load(std::memory_order_relaxed);
        return static_cast<uint8_t>(
            ((v & kBinaryBit) ? 1 : 0) | ((v & kJsonBit) ? 2 : 0));
    }

    /// Atomic store of both bytes and flags (single store).
    void store(uint32_t bytes, uint8_t f) {
        packed.store((bytes & kBytesMask)
                   | ((f & 1) ? kBinaryBit : 0)
                   | ((f & 2) ? kJsonBit   : 0),
                     std::memory_order_relaxed);
    }
};

/// Memory hook for non-cached entities (REJECT path).
/// Accumulates lazy serialization costs into the ghost's estimated_bytes.
/// fetch_add on low 30 bits — safe as long as entity size < 1 GiB.
inline void ghostMemoryHook(void* ctx, int64_t delta) {
    if (delta <= 0) return;
    auto* ghost = static_cast<GhostData*>(ctx);
    ghost->packed.fetch_add(static_cast<uint32_t>(delta),
                            std::memory_order_relaxed);
}

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_GHOST_ENTRY_H