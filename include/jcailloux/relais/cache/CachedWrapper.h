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
// Always stored behind shared_ptr<const Entity>; the control block retains
// the CachedWrapper type so the destructor fires correctly.
// =============================================================================

template<typename Entity>
class CachedWrapper final : public Entity {
public:
    CachedWrapper(Entity&& entity, size_t extra_overhead)
        : Entity(std::move(entity))
        , extra_overhead_(extra_overhead)
    {
        this->memory_hook_ = &chargeHook;
        chargeHook(static_cast<int64_t>(this->memoryUsage() + extra_overhead_));
    }

    ~CachedWrapper() {
        chargeHook(-static_cast<int64_t>(this->memoryUsage() + extra_overhead_));
    }

    CachedWrapper(const CachedWrapper&) = delete;
    CachedWrapper(CachedWrapper&&) = delete;
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
