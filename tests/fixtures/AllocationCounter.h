#ifndef JCX_RELAIS_TEST_ALLOCATION_COUNTER_H
#define JCX_RELAIS_TEST_ALLOCATION_COUNTER_H

// Replaces the global operator new and delete, to count what each thread
// allocates and still holds: include in exactly one translation unit of a test
// executable. The array, nothrow and sized forms come through these two; the
// over-aligned ones are not counted.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>

#include <malloc.h>

namespace relais_test {

struct Allocations {
    std::uint64_t count = 0;            // allocations made so far
    std::int64_t live_bytes = 0;        // allocated, less what this thread freed
    std::uint64_t requested_bytes = 0;  // bytes asked for so far, before rounding
};

inline constinit thread_local Allocations tl_allocations{};

// The calling thread's: compare two readings taken on the same thread.
[[nodiscard]] inline Allocations threadAllocations() noexcept { return tl_allocations; }

}  // namespace relais_test

void* operator new(std::size_t size) {
    void* p = std::malloc(size == 0 ? 1 : size);
    if (p == nullptr) throw std::bad_alloc();
    auto& counted = relais_test::tl_allocations;
    ++counted.count;
    counted.requested_bytes += size;
    counted.live_bytes += static_cast<std::int64_t>(malloc_usable_size(p));
    return p;
}

void operator delete(void* p) noexcept {
    if (p == nullptr) return;
    relais_test::tl_allocations.live_bytes -= static_cast<std::int64_t>(malloc_usable_size(p));
    std::free(p);
}

void operator delete(void* p, std::size_t) noexcept { ::operator delete(p); }

#endif  // JCX_RELAIS_TEST_ALLOCATION_COUNTER_H
