#ifndef JCX_RELAIS_CACHE_GHOST_ENTRY_H
#define JCX_RELAIS_CACHE_GHOST_ENTRY_H

#include <atomic>
#include <cstdint>

namespace jcailloux::relais::cache {

// =============================================================================
// GhostData — compact placeholder for evicted/rejected cache entries
//
// Stored in GhostCacheEntry (inside ChunkMap) alongside GDSFScoreData metadata.
// Tracks estimated memory cost and serialization flags without holding the
// actual entity data. Total overhead: ~8 bytes per ghost.
// =============================================================================

struct GhostData {
    std::atomic<uint32_t> estimated_bytes{0};  // memory if cached
    std::atomic<uint8_t> flags{0};             // bit 0: has_binary, bit 1: has_json

    GhostData() = default;
    GhostData(uint32_t bytes, uint8_t f) : estimated_bytes(bytes), flags(f) {}

    // Manual copy/move (std::atomic is non-copyable)
    GhostData(const GhostData& o)
        : estimated_bytes(o.estimated_bytes.load(std::memory_order_relaxed))
        , flags(o.flags.load(std::memory_order_relaxed)) {}
    GhostData(GhostData&& o) noexcept
        : estimated_bytes(o.estimated_bytes.load(std::memory_order_relaxed))
        , flags(o.flags.load(std::memory_order_relaxed)) {}
    GhostData& operator=(const GhostData& o) {
        estimated_bytes.store(o.estimated_bytes.load(std::memory_order_relaxed),
                              std::memory_order_relaxed);
        flags.store(o.flags.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
        return *this;
    }
    GhostData& operator=(GhostData&& o) noexcept {
        estimated_bytes.store(o.estimated_bytes.load(std::memory_order_relaxed),
                              std::memory_order_relaxed);
        flags.store(o.flags.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
        return *this;
    }
};

/// Memory hook for non-cached entities (REJECT path).
/// Accumulates lazy serialization costs into the ghost's estimated_bytes.
inline void ghostMemoryHook(void* ctx, int64_t delta) {
    if (delta <= 0) return;
    auto* ghost = static_cast<GhostData*>(ctx);
    ghost->estimated_bytes.fetch_add(static_cast<uint32_t>(delta),
                                     std::memory_order_relaxed);
}

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_GHOST_ENTRY_H
