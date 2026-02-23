#ifndef JCX_RELAIS_CACHE_CACHED_WRAPPER_H
#define JCX_RELAIS_CACHE_CACHED_WRAPPER_H

#include <cstdint>
#include <utility>
#include "jcailloux/relais/cache/GDSFPolicy.h"

namespace jcailloux::relais::cache {

// =============================================================================
// CachedWrapper<Entity> — L1 cache entry with automatic memory tracking
//
// Subclasses Entity (EntityWrapper<Struct, Mapping>) and installs a memory
// hook so that:
//   - Construction charges memoryUsage() + extra_overhead to GDSFPolicy
//   - Destruction discharges the same amount
//   - Lazy json()/binary() buffer generation charges additional memory
//
// Stored inside ChunkMap::CacheEntry by-value; epoch-based reclamation
// ensures the destructor fires at the correct time.
// =============================================================================

template<typename Entity>
class CachedWrapper final : public Entity {
public:
    CachedWrapper(Entity&& entity, size_t extra_overhead)
        : Entity(std::move(entity))
        , extra_overhead_(extra_overhead)
    {
        this->memory_hook_ = &chargeHook;
        chargeHook(static_cast<int64_t>(this->memoryUsage()));
    }

    /// Full memory cost of this cache entry (entity + overhead).
    /// Includes: struct + dynamicSize (heap) + lazy BEVE/JSON buffers + cache overhead.
    [[nodiscard]] size_t memoryUsage() const {
        return Entity::memoryUsage() + extra_overhead_;
    }

    /// Move constructor: transfers memory tracking to the new object.
    /// The moved-from object will not discharge memory on destruction.
    CachedWrapper(CachedWrapper&& o) noexcept
        : Entity(static_cast<Entity&&>(std::move(o)))
        , extra_overhead_(o.extra_overhead_)
    {
        this->memory_hook_ = o.memory_hook_;
        o.memory_hook_ = nullptr;
        o.extra_overhead_ = 0;
    }

    ~CachedWrapper() {
        if (this->memory_hook_) {
            this->memory_hook_(-static_cast<int64_t>(this->memoryUsage()));
        }
    }

    CachedWrapper(const CachedWrapper&) = delete;
    CachedWrapper& operator=(const CachedWrapper&) = delete;
    CachedWrapper& operator=(CachedWrapper&&) = delete;

private:
    size_t extra_overhead_;

    static void chargeHook(int64_t delta) {
        GDSFPolicy::instance().charge(delta);
    }
};

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_CACHED_WRAPPER_H
