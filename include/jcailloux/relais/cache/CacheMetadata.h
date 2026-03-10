#ifndef JCX_RELAIS_CACHE_METADATA_H
#define JCX_RELAIS_CACHE_METADATA_H

#include <algorithm>
#include <atomic>
#include <cstdint>

namespace jcailloux::relais::cache {

// =============================================================================
// GDSFScoreData — shared base for GDSF-enabled metadata variants
// =============================================================================
//
// Single atomic field: access_count (fixed-point, scale=16).
// Score is computed on-the-fly in cleanupPredicate:
//   score = access_count × avg_cost / memoryUsage
//
// bumpScore = fetch_add(kCountScale) — one lock xadd, zero CAS.
// Decay is applied inline during cleanup (single writer per chunk).
//
// Inherited by CacheMetadata<true, false> and CacheMetadata<true, true>.
// Size: 4 bytes (was 8 bytes with score + last_generation).

struct GDSFScoreData {
    static constexpr uint32_t kCountScale = 16;
    static constexpr float kUpdatePenalty = 0.95f;

    mutable std::atomic<uint32_t> access_count{0};

    GDSFScoreData() = default;
    explicit GDSFScoreData(uint32_t count) : access_count(count) {}

    /// Raw access count (full 32 bits, no masking).
    uint32_t rawCount() const {
        return access_count.load(std::memory_order_relaxed);
    }

    /// Compute GDSF score on-the-fly: access_count × avg_cost / memoryUsage.
    /// Called in cleanupPredicate where value.memoryUsage() is available.
    float computeScore(float avg_cost, size_t memory_usage) const {
        return static_cast<float>(rawCount())
             * avg_cost
             / static_cast<float>(std::max(memory_usage, size_t{1}));
    }

    /// Merge access history from old entry on upsert.
    /// Applies kUpdatePenalty so frequently-updated entities see score erode.
    void mergeFrom(const GDSFScoreData& old) {
        uint32_t c = old.access_count.load(std::memory_order_relaxed);
        access_count.store(
            static_cast<uint32_t>(static_cast<float>(c) * kUpdatePenalty),
            std::memory_order_relaxed);
    }

    /// Self-reference for concept detection (GDSFAware).
    GDSFScoreData& gdsfData() { return *this; }
    const GDSFScoreData& gdsfData() const { return *this; }

    // --- Manual copy/move (std::atomic is non-copyable) ---

    GDSFScoreData(const GDSFScoreData& o)
        : access_count(o.access_count.load(std::memory_order_relaxed)) {}

    GDSFScoreData& operator=(const GDSFScoreData& o) {
        access_count.store(o.access_count.load(std::memory_order_relaxed),
                          std::memory_order_relaxed);
        return *this;
    }

    GDSFScoreData(GDSFScoreData&& o) noexcept
        : access_count(o.access_count.load(std::memory_order_relaxed)) {}

    GDSFScoreData& operator=(GDSFScoreData&& o) noexcept {
        access_count.store(o.access_count.load(std::memory_order_relaxed),
                          std::memory_order_relaxed);
        return *this;
    }
};

// =============================================================================
// CacheMetadata<WithGDSF, WithTTL> — 4 specializations
// =============================================================================
//
// Selected at compile time in LocalRepo via:
//   using Metadata = cache::CacheMetadata<HasGDSF, HasTTL>;
//
// Sizes (TTL compressed to uint32_t seconds):
//   <false, false>  0 bytes (EBO via [[no_unique_address]] in ChunkMap)
//   <false, true>   4 bytes (TTL only)
//   <true,  false>  4 bytes (GDSF only, inherits GDSFScoreData)
//   <true,  true>   8 bytes (GDSF 4B + TTL 4B, no padding)

template<bool WithGDSF, bool WithTTL>
struct CacheMetadata;

// ---------------------------------------------------------------------------
// (false, false) — empty: no GDSF, no TTL
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<false, false> {
    void mergeFrom(const CacheMetadata&) {}
};

// ---------------------------------------------------------------------------
// (false, true) — TTL only (4 bytes)
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<false, true> {
    uint32_t ttl_expiration_sec{0};  // seconds since steady_clock epoch; 0 = no TTL

    bool isExpired(uint32_t now_sec) const {
        return ttl_expiration_sec != 0 && now_sec > ttl_expiration_sec;
    }

    void mergeFrom(const CacheMetadata&) {}
};

// ---------------------------------------------------------------------------
// (true, false) — GDSF only (4 bytes, inherits GDSFScoreData)
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<true, false> : GDSFScoreData {
    CacheMetadata() = default;
    CacheMetadata(uint32_t count, uint32_t = 0) : GDSFScoreData(count) {}
};

// ---------------------------------------------------------------------------
// (true, true) — GDSF + TTL (4B + 4B = 8B, no padding)
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<true, true> : GDSFScoreData {
    uint32_t ttl_expiration_sec{0};  // seconds since steady_clock epoch; 0 = no TTL

    CacheMetadata() = default;
    CacheMetadata(uint32_t count, uint32_t ttl_sec)
        : GDSFScoreData(count), ttl_expiration_sec(ttl_sec) {}

    bool isExpired(uint32_t now_sec) const {
        return ttl_expiration_sec != 0 && now_sec > ttl_expiration_sec;
    }

    void mergeFrom(const CacheMetadata& old) {
        GDSFScoreData::mergeFrom(old);
    }

    // --- Copy/move: base + ttl field ---

    CacheMetadata(const CacheMetadata& o)
        : GDSFScoreData(o), ttl_expiration_sec(o.ttl_expiration_sec) {}

    CacheMetadata& operator=(const CacheMetadata& o) {
        GDSFScoreData::operator=(o);
        ttl_expiration_sec = o.ttl_expiration_sec;
        return *this;
    }

    CacheMetadata(CacheMetadata&& o) noexcept
        : GDSFScoreData(std::move(o)), ttl_expiration_sec(o.ttl_expiration_sec) {}

    CacheMetadata& operator=(CacheMetadata&& o) noexcept {
        GDSFScoreData::operator=(std::move(o));
        ttl_expiration_sec = o.ttl_expiration_sec;
        return *this;
    }
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_METADATA_H
