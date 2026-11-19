#ifndef JCX_RELAIS_BLOCK_BYTES_H
#define JCX_RELAIS_BLOCK_BYTES_H

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#if defined(__APPLE__)
#include <malloc/malloc.h>
#elif defined(__FreeBSD__)
#include <malloc_np.h>
#elif defined(__linux__) || defined(_WIN32)
#include <malloc.h>
#endif

#include "jcailloux/relais/Log.h"

namespace jcailloux::relais::detail {

// =============================================================================
// blockBytes — the room a block takes at the allocator
// =============================================================================
//
// An allocator serves a request from a size class at least as large, and some
// allocators keep a header in front of each block. That room is what a block
// keeps from the rest of the process, so it is what the L1 weighs. It is read
// from the allocator in use, never assumed, so it holds for any allocator,
// version or tuning:
//   - a block whose pointer relais holds (a string's characters, a vector's
//     array, a container node, an entry's pool block) is read on itself: its
//     usable size, as the allocator reports it. An allocator may hand out more
//     than the class of the request: glibc gives a free block 16 bytes larger
//     whole when the rest would be too small to split, which depends on the
//     allocator's history;
//   - a block without its pointer at hand (a bucket array, or an entry's pool
//     block weighed before the entry exists) weighs the class of its request.
//     The classes up to kTableLimit are read once, on first use, into a table;
//     beyond, such a block weighs its request;
//   - the header in front of a block, which no allocator reports, is the one
//     written rule (headerOf).
// A block keeps its size for its whole life, and the table is frozen once
// read: a block weighs the same from its allocation to its release.
//
// Assumes relais's blocks come from malloc, as the default operator new does.
// Where the platform cannot report a block's usable size, every block weighs
// its request.

/// How relais reads an allocator: allocate and release a block, and report a
/// block's usable size (nullptr where the platform cannot).
struct AllocatorProbe {
    void* (*allocate)(size_t);
    void (*release)(void*);
    size_t (*usable)(const void*);
};

/// The process's malloc.
inline constexpr AllocatorProbe kMallocProbe{
    [](size_t n) { return std::malloc(n); },
    [](void* p) { std::free(p); },
#if defined(__APPLE__)
    [](const void* p) -> size_t { return malloc_size(p); },
#elif defined(_WIN32)
    [](const void* p) -> size_t { return _msize(const_cast<void*>(p)); },
#elif defined(__linux__) || defined(__FreeBSD__)
    [](const void* p) -> size_t { return malloc_usable_size(const_cast<void*>(p)); },
#else
    nullptr,
#endif
};

class BlockSizes {
public:
    /// The largest request whose class the table holds.
    static constexpr size_t kTableLimit = 16 * 1024;

    /// The process's malloc, read on first use, then frozen.
    static const BlockSizes& process() {
#ifdef RELAIS_BUILDING_TESTS
        if (const BlockSizes* sizes = testSizes()) return *sizes;
#endif
        static const BlockSizes sizes{kMallocProbe};
        return sizes;
    }

    /// Reads the classes of `a` up to kTableLimit: about 16,000 blocks, none
    /// over any mmap threshold, each released at once.
    explicit BlockSizes(const AllocatorProbe& a) : usable_(a.usable) {
        if (usable_ == nullptr) {
            // Nothing to read: each request is its own weight.
            shift_ = 0;
            for (size_t n = 1; n <= kTableLimit; ++n) table_[n - 1] = static_cast<uint32_t>(n);
            RELAIS_LOG_DEBUG << "relais: this platform does not report the usable size of a "
                                "block; the L1 weighs each block at the bytes it requests";
            return;
        }

        // The class of a request: the least usable size among a few blocks of
        // it allocated together. A larger free block may serve one of them,
        // never a smaller one.
        auto classOf = [&](size_t n) -> size_t {
            std::array<void*, 4> blocks{};
            size_t least = SIZE_MAX;
            for (void*& p : blocks) {
                p = a.allocate(n);
                if (p != nullptr) least = std::min(least, usable_(p));
            }
            for (void* p : blocks) {
                if (p != nullptr) a.release(p);
            }
            return least == SIZE_MAX ? n : least;
        };

        // Size classes fall on multiples of 8 and never shrink as the request
        // grows: equal classes at both ends of an 8-byte slice of requests mean
        // one class for the whole slice. An allocator whose ends differ (one
        // that reports the requested size, as a sanitizer does) is read byte by
        // byte.
        shift_ = 3;
        for (size_t i = 0; i < (kTableLimit >> shift_); ++i) {
            size_t first = classOf((i << shift_) + 1);
            size_t last = classOf((i + 1) << shift_);
            if (first != last) {
                shift_ = 0;
                break;
            }
            table_[i] = static_cast<uint32_t>(last);
        }
        if (shift_ == 0) {
            for (size_t n = 1; n <= kTableLimit; ++n) table_[n - 1] = static_cast<uint32_t>(classOf(n));
        }

        // A size word in front of each block shows in every class (headerOf);
        // one class without it rules the word out.
        size_t entries = kTableLimit >> shift_;
        size_word_ = true;
        for (size_t i = 0; i < entries; ++i) size_word_ = size_word_ && table_[i] % 16 == kWord;
        for (size_t i = 0; i < entries; ++i) table_[i] += static_cast<uint32_t>(headerOf(table_[i]));
    }

    /// The room a block of n requested bytes takes when the block is not at
    /// hand: its class and header, or n beyond the table. 0 for no block.
    [[nodiscard]] size_t bytes(size_t n) const noexcept {
        if (n == 0) return 0;
        if (n <= kTableLimit) return table_[(n - 1) >> shift_];
        return n;
    }

    /// The room `block`, of n requested bytes, takes: read on the block.
    [[nodiscard]] size_t bytes(size_t n, const void* block) const noexcept {
        if (n == 0) return 0;
        if (usable_ == nullptr || block == nullptr) return bytes(n);
        size_t u = usable_(block);
        return u + headerOf(u);
    }

    /// The header in front of a block whose usable size is `usable`.
    ///
    /// An allocator that keeps a size word in front of each block (glibc)
    /// shows it in its classes: its blocks start on 16-byte boundaries, so each
    /// spans a multiple of 16 bytes, yet every small class is 8 modulo 16. The
    /// other 8 bytes are the size word. A block it maps on its own (glibc,
    /// beyond its mmap threshold) carries two words in front, and a usable size
    /// of 0 modulo 16. An allocator that keeps its metadata out of the block
    /// (jemalloc, mimalloc) has small classes of 0 modulo 16 too: no header.
    [[nodiscard]] size_t headerOf(size_t usable) const noexcept {
        if (!size_word_) return 0;
        return usable % 16 == kWord ? kWord : 2 * kWord;
    }

    /// 8 when one class covers each 8-byte slice of requests, 1 when the table
    /// is read byte by byte.
    [[nodiscard]] size_t granularity() const noexcept { return size_t{1} << shift_; }

    /// Whether the allocator keeps a size word in front of each block.
    [[nodiscard]] bool hasSizeWord() const noexcept { return size_word_; }

#ifdef RELAIS_BUILDING_TESTS
    /// Tests only: the calling thread weighs its blocks with `sizes` instead
    /// of the process's (nullptr: the process's again).
    static void setTestSizes(const BlockSizes* sizes) noexcept { testSizes() = sizes; }
#endif

private:
    static constexpr size_t kWord = sizeof(size_t);

#ifdef RELAIS_BUILDING_TESTS
    static const BlockSizes*& testSizes() noexcept {
        static thread_local const BlockSizes* sizes = nullptr;
        return sizes;
    }
#endif

    size_t (*usable_)(const void*);
    bool size_word_ = false;
    unsigned shift_ = 3;
    // One entry per slice of requests: 2,048 used of 16,384 when slices hold.
    // Left uninitialized: the constructor writes every entry it reads, and the
    // process's table, a static, touches only those pages.
    std::array<uint32_t, kTableLimit> table_;
};

/// The room a block of n requested bytes takes when the block is not at hand:
/// the class of n and its header (0: no block).
[[nodiscard]] inline size_t blockBytes(size_t n) noexcept {
    return BlockSizes::process().bytes(n);
}

/// The room `block`, of n requested bytes, takes: read on the block.
[[nodiscard]] inline size_t blockBytes(size_t n, const void* block) noexcept {
    return BlockSizes::process().bytes(n, block);
}

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_BLOCK_BYTES_H
