#ifndef JCX_RELAIS_TEST_ALLOCATOR_H
#define JCX_RELAIS_TEST_ALLOCATOR_H

// The malloc a test process runs under, as the test's registration states it:
// RELAIS_TEST_ALLOCATOR=jemalloc or mimalloc, with that library preloaded;
// glibc's otherwise; a sanitizer's in a sanitizer build. The test states each
// one's block header from what it knows of that allocator, independently of
// what relais reads.

#include <cstddef>
#include <cstdlib>
#include <stdexcept>
#include <string_view>

namespace relais_test {

enum class TestMalloc { Glibc, Jemalloc, Mimalloc, Sanitizer };

#if defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define RELAIS_TEST_SANITIZED_MALLOC 1
#endif
#endif
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define RELAIS_TEST_SANITIZED_MALLOC 1
#endif

[[nodiscard]] inline TestMalloc testMalloc() {
#if defined(RELAIS_TEST_SANITIZED_MALLOC)
    return TestMalloc::Sanitizer;
#else
    const char* name = std::getenv("RELAIS_TEST_ALLOCATOR");
    if (name == nullptr) return TestMalloc::Glibc;
    std::string_view n{name};
    if (n == "jemalloc") return TestMalloc::Jemalloc;
    if (n == "mimalloc") return TestMalloc::Mimalloc;
    throw std::invalid_argument("RELAIS_TEST_ALLOCATOR: unknown allocator");
#endif
}

/// The header in front of a block of the heap: glibc keeps its size word
/// there; jemalloc and mimalloc keep their metadata out of the block; a
/// sanitizer reports the requested size, with nothing around it.
[[nodiscard]] inline size_t heapBlockHeader() {
    return testMalloc() == TestMalloc::Glibc ? sizeof(size_t) : 0;
}

/// The same, for a block the allocator maps on its own: glibc keeps two words
/// in front of it.
[[nodiscard]] inline size_t mappedBlockHeader() {
    return testMalloc() == TestMalloc::Glibc ? 2 * sizeof(size_t) : 0;
}

}  // namespace relais_test

#endif  // JCX_RELAIS_TEST_ALLOCATOR_H
