#ifndef JCX_RELAIS_TEST_ALLOCATION_COUNTER_H
#define JCX_RELAIS_TEST_ALLOCATION_COUNTER_H

// Replaces the global operator new and delete, to count what each thread
// allocates and still holds: include in exactly one translation unit of a test
// executable. The array, nothrow and sized forms come through these two; the
// over-aligned ones are not counted.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <vector>

#include <malloc.h>

namespace relais_test {

struct Allocations {
    std::uint64_t count = 0;        // allocations made so far
    std::int64_t live_bytes = 0;    // usable bytes allocated, less what this thread freed
};

inline constinit thread_local Allocations tl_allocations{};

// The calling thread's: compare two readings taken on the same thread.
[[nodiscard]] inline Allocations threadAllocations() noexcept { return tl_allocations; }

/// A block allocated and not yet freed, with the bytes asked for it.
struct HeldBlock {
    void* block = nullptr;
    std::size_t requested = 0;
};

/// While set, the blocks this thread allocates and still holds.
inline constinit thread_local std::vector<HeldBlock>* tl_held_blocks = nullptr;

/// Lists, for its lifetime, the blocks the calling thread allocates and still
/// holds.
class RecordHeldBlocks {
public:
    RecordHeldBlocks() {
        blocks_.reserve(256);
        tl_held_blocks = &blocks_;
    }
    ~RecordHeldBlocks() { stop(); }
    RecordHeldBlocks(const RecordHeldBlocks&) = delete;
    RecordHeldBlocks& operator=(const RecordHeldBlocks&) = delete;

    void stop() noexcept {
        if (tl_held_blocks == &blocks_) tl_held_blocks = nullptr;
    }
    [[nodiscard]] const std::vector<HeldBlock>& blocks() const noexcept { return blocks_; }

private:
    std::vector<HeldBlock> blocks_;
};

}  // namespace relais_test

void* operator new(std::size_t size) {
    void* p = std::malloc(size == 0 ? 1 : size);
    if (p == nullptr) throw std::bad_alloc();
    auto& counted = relais_test::tl_allocations;
    ++counted.count;
    counted.live_bytes += static_cast<std::int64_t>(malloc_usable_size(p));
    if (auto* held = relais_test::tl_held_blocks) {
        relais_test::tl_held_blocks = nullptr;  // the list's own growth is not recorded
        held->push_back({p, size});
        relais_test::tl_held_blocks = held;
    }
    return p;
}

void operator delete(void* p) noexcept {
    if (p == nullptr) return;
    relais_test::tl_allocations.live_bytes -= static_cast<std::int64_t>(malloc_usable_size(p));
    if (auto* held = relais_test::tl_held_blocks) {
        std::erase_if(*held, [p](const relais_test::HeldBlock& b) { return b.block == p; });
    }
    std::free(p);
}

void operator delete(void* p, std::size_t) noexcept { ::operator delete(p); }

#endif  // JCX_RELAIS_TEST_ALLOCATION_COUNTER_H
