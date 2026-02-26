#ifndef JCX_RELAIS_CACHE_TAGGED_ENTRY_H
#define JCX_RELAIS_CACHE_TAGGED_ENTRY_H

#include <cstdint>
#include <type_traits>

namespace jcailloux::relais::cache {

// =============================================================================
// TaggedEntry — 8-byte tagged pointer encoding ghost data inline
//
// Encoding (64 bits):
//   Bit  0      : ghost flag (1 = ghost, 0 = real pointer)
//   Bits 1-2    : has_binary, has_json (2 flags)
//   Bits 3-32   : estimated_bytes (30 bits = 1 GiB max)
//   Bits 33-63  : access_count (31 bits)
//
// Real pointers from epoch::memory_pool::New() are always >= 8-byte aligned
// (alignof(max_align_t) >= 8 on 64-bit), so bit 0 is always 0 for valid
// pointers.
//
// trivially_copyable — stored by value in ParlayHash bucket buffers.
// =============================================================================

struct TaggedEntry {
    uintptr_t bits{0};

    static constexpr uintptr_t kGhostBit     = 1ULL;
    static constexpr uintptr_t kBinaryBit    = 1ULL << 1;
    static constexpr uintptr_t kJsonBit      = 1ULL << 2;
    static constexpr int       kBytesShift   = 3;
    static constexpr uintptr_t kBytesMask    = 0x3FFF'FFFFULL;  // 30 bits
    static constexpr int       kCountShift   = 33;
    static constexpr uintptr_t kCountMask    = 0x7FFF'FFFFULL;  // 31 bits

    TaggedEntry() = default;
    explicit TaggedEntry(uintptr_t b) : bits(b) {}

    // --- Factories ---

    template<typename T>
    static TaggedEntry fromReal(T* ptr) {
        return TaggedEntry{reinterpret_cast<uintptr_t>(ptr)};
    }

    static TaggedEntry fromGhost(uint32_t count, uint32_t estimated_bytes, uint8_t flags) {
        uintptr_t b = kGhostBit;
        if (flags & 0x01) b |= kBinaryBit;
        if (flags & 0x02) b |= kJsonBit;
        b |= (static_cast<uintptr_t>(estimated_bytes & kBytesMask) << kBytesShift);
        b |= (static_cast<uintptr_t>(count & kCountMask) << kCountShift);
        return TaggedEntry{b};
    }

    // --- Predicates ---

    bool isGhost() const { return (bits & kGhostBit) != 0; }
    bool isReal()  const { return bits != 0 && !isGhost(); }
    bool empty()   const { return bits == 0; }

    explicit operator bool() const { return bits != 0; }

    // --- Real pointer access ---

    template<typename T>
    T* asReal() const {
        if (!isReal()) return nullptr;
        return reinterpret_cast<T*>(bits);
    }

    // --- Ghost data extractors ---

    uint32_t ghostCount() const {
        return static_cast<uint32_t>((bits >> kCountShift) & kCountMask);
    }

    uint32_t ghostBytes() const {
        return static_cast<uint32_t>((bits >> kBytesShift) & kBytesMask);
    }

    uint8_t ghostFlags() const {
        return static_cast<uint8_t>(
            ((bits & kBinaryBit) ? 0x01 : 0) |
            ((bits & kJsonBit)   ? 0x02 : 0));
    }

    // --- Immutable ghost mutations ---

    TaggedEntry withGhostCount(uint32_t count) const {
        uintptr_t b = bits & ~(kCountMask << kCountShift);
        b |= (static_cast<uintptr_t>(count & kCountMask) << kCountShift);
        return TaggedEntry{b};
    }

    TaggedEntry withGhostBytes(uint32_t estimated_bytes, uint8_t flags) const {
        uintptr_t b = bits & ~((kBytesMask << kBytesShift) | kBinaryBit | kJsonBit);
        b |= (static_cast<uintptr_t>(estimated_bytes & kBytesMask) << kBytesShift);
        if (flags & 0x01) b |= kBinaryBit;
        if (flags & 0x02) b |= kJsonBit;
        return TaggedEntry{b};
    }
};

static_assert(sizeof(TaggedEntry) == 8, "TaggedEntry must be 8 bytes");
static_assert(std::is_trivially_copyable_v<TaggedEntry>, "TaggedEntry must be trivially copyable");

}  // namespace jcailloux::relais::cache

#endif  // JCX_RELAIS_CACHE_TAGGED_ENTRY_H
