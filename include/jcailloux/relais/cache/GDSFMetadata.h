#ifndef JCX_RELAIS_CACHE_GDSF_METADATA_H
#define JCX_RELAIS_CACHE_GDSF_METADATA_H

#include <atomic>
#include <chrono>
#include <cstdint>

namespace jcailloux::relais::cache {

// =============================================================================
// GDSFScoreData — shared base for GDSF-enabled metadata variants
// =============================================================================
//
// Contains the two atomic fields needed by GDSFPolicy::decay() and score bumps.
// Inherited by CacheMetadata<true, false> and CacheMetadata<true, true>.

struct GDSFScoreData {
    mutable std::atomic<float> score{0.0f};
    mutable std::atomic<uint32_t> last_generation{0};

    GDSFScoreData() = default;
    GDSFScoreData(float s, uint32_t g) : score(s), last_generation(g) {}

    // --- Manual copy/move (std::atomic is non-copyable) ---

    GDSFScoreData(const GDSFScoreData& o)
        : score(o.score.load(std::memory_order_relaxed))
        , last_generation(o.last_generation.load(std::memory_order_relaxed)) {}

    GDSFScoreData& operator=(const GDSFScoreData& o) {
        score.store(o.score.load(std::memory_order_relaxed), std::memory_order_relaxed);
        last_generation.store(o.last_generation.load(std::memory_order_relaxed), std::memory_order_relaxed);
        return *this;
    }

    GDSFScoreData(GDSFScoreData&& o) noexcept
        : score(o.score.load(std::memory_order_relaxed))
        , last_generation(o.last_generation.load(std::memory_order_relaxed)) {}

    GDSFScoreData& operator=(GDSFScoreData&& o) noexcept {
        score.store(o.score.load(std::memory_order_relaxed), std::memory_order_relaxed);
        last_generation.store(o.last_generation.load(std::memory_order_relaxed), std::memory_order_relaxed);
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
//   <true,  false>  8 bytes (GDSF only, inherits GDSFScoreData)
//   <true,  true>  16 bytes (GDSF + TTL)

template<bool WithGDSF, bool WithTTL>
struct CacheMetadata;

// ---------------------------------------------------------------------------
// (false, false) — empty: no GDSF, no TTL
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<false, false> {};

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
};

// ---------------------------------------------------------------------------
// (true, false) — GDSF only (8 bytes, inherits GDSFScoreData)
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<true, false> : GDSFScoreData {
    CacheMetadata() = default;
    CacheMetadata(float s, uint32_t g, int64_t = 0) : GDSFScoreData(s, g) {}
};

// ---------------------------------------------------------------------------
// (true, true) — GDSF + TTL (16 bytes)
// ---------------------------------------------------------------------------
template<>
struct CacheMetadata<true, true> : GDSFScoreData {
    int64_t ttl_expiration_rep{0};  // steady_clock rep; 0 = no TTL

    CacheMetadata() = default;
    CacheMetadata(float s, uint32_t g, int64_t ttl_rep)
        : GDSFScoreData(s, g), ttl_expiration_rep(ttl_rep) {}

    bool isExpired(std::chrono::steady_clock::time_point now) const {
        return ttl_expiration_rep != 0
            && now.time_since_epoch().count() > ttl_expiration_rep;
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
