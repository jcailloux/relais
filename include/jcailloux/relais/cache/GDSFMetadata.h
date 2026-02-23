#ifndef JCX_RELAIS_CACHE_GDSF_METADATA_H
#define JCX_RELAIS_CACHE_GDSF_METADATA_H

#include <algorithm>
#include <atomic>
#include <chrono>
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

    /// Compute GDSF score on-the-fly: access_count × avg_cost / memoryUsage.
    /// Called in cleanupPredicate where value.memoryUsage() is available.
    float computeScore(float avg_cost, size_t memory_usage) const {
        return static_cast<float>(access_count.load(std::memory_order_relaxed))
             * avg_cost
             / static_cast<float>(std::max(memory_usage, size_t{1}));
    }

    /// Merge access history from old entry on upsert.
    /// Applies kUpdatePenalty so frequently-updated entities see score erode.
    void mergeFrom(const GDSFScoreData& old) {
        uint32_t c = old.access_count.load(std::memory_order_relaxed);
        access_count.store(static_cast<uint32_t>(static_cast<float>(c) * kUpdatePenalty),
                          std::memory_order_relaxed);
    }

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
// Selected at compile time in CachedRepo via:
//   using Metadata = cache::CacheMetadata<HasGDSF, HasTTL>;
//
// Sizes:
//   <false, false>  0 bytes (EBO via [[no_unique_address]] in ChunkMap)
//   <false, true>   8 bytes (TTL only)
//   <true,  false>  4 bytes (GDSF only, inherits GDSFScoreData)
//   <true,  true>  16 bytes (GDSF 4B + TTL 8B, padded to 16B)

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
// (false, true) — TTL only (8 bytes)
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<false, true> {
    int64_t ttl_expiration_rep{0};  // steady_clock rep; 0 = no TTL

    bool isExpired(std::chrono::steady_clock::time_point now) const {
        return ttl_expiration_rep != 0
            && now.time_since_epoch().count() > ttl_expiration_rep;
    }

    void mergeFrom(const CacheMetadata&) {}
};

// ---------------------------------------------------------------------------
// (true, false) — GDSF only (4 bytes, inherits GDSFScoreData)
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<true, false> : GDSFScoreData {
    CacheMetadata() = default;
    CacheMetadata(uint32_t count, int64_t = 0) : GDSFScoreData(count) {}
};

// ---------------------------------------------------------------------------
// (true, true) — GDSF + TTL (4B + 8B = 12B, padded to 16B)
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<true, true> : GDSFScoreData {
    int64_t ttl_expiration_rep{0};  // steady_clock rep; 0 = no TTL

    CacheMetadata() = default;
    CacheMetadata(uint32_t count, int64_t ttl_rep)
        : GDSFScoreData(count), ttl_expiration_rep(ttl_rep) {}

    bool isExpired(std::chrono::steady_clock::time_point now) const {
        return ttl_expiration_rep != 0
            && now.time_since_epoch().count() > ttl_expiration_rep;
    }

    void mergeFrom(const CacheMetadata& old) {
        GDSFScoreData::mergeFrom(old);
    }

    // --- Copy/move: base + ttl field ---

    CacheMetadata(const CacheMetadata& o)
        : GDSFScoreData(o), ttl_expiration_rep(o.ttl_expiration_rep) {}

    CacheMetadata& operator=(const CacheMetadata& o) {
        GDSFScoreData::operator=(o);
        ttl_expiration_rep = o.ttl_expiration_rep;
        return *this;
    }

    CacheMetadata(CacheMetadata&& o) noexcept
        : GDSFScoreData(std::move(o)), ttl_expiration_rep(o.ttl_expiration_rep) {}

    CacheMetadata& operator=(CacheMetadata&& o) noexcept {
        GDSFScoreData::operator=(std::move(o));
        ttl_expiration_rep = o.ttl_expiration_rep;
        return *this;
    }
};

// =============================================================================
// Legacy alias (used by ListCacheMetadataImpl)
// =============================================================================
using GDSFMetadata = CacheMetadata<true, true>;

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_GDSF_METADATA_H
