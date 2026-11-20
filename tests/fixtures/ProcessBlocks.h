#ifndef JCX_RELAIS_TEST_PROCESS_BLOCKS_H
#define JCX_RELAIS_TEST_PROCESS_BLOCKS_H

// Follows, for the whole process, each block a recording thread allocates,
// until any thread frees it, and the room these blocks take: include in
// exactly one translation unit of an executable linked with
// -Wl,--wrap=malloc,--wrap=calloc,--wrap=free. Every malloc, calloc and free
// called from the executable's own code passes through the wrappers below,
// the header-only epoch pool and parlay included (the compiler turns a malloc
// followed by a memset to zero into a calloc); so does every operator new and
// delete, replaced here to call them. The over-aligned forms are not replaced: they
// stay invisible (thread-specific counters, the pools' per-thread arrays).

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <new>
#include <unordered_map>
#include <utility>

#include <malloc.h>

#include "fixtures/TestAllocator.h"

extern "C" void* __real_malloc(size_t n);
extern "C" void* __real_calloc(size_t count, size_t n);
extern "C" void __real_free(void* p);

namespace relais_test {

/// The room a block takes at the allocator, as the test knows it: its usable
/// size and, under glibc, the header in front of it: one word, or two for a
/// block glibc maps on its own, which it marks in its size word.
inline size_t blockRoom(void* p) {
    static const TestMalloc kMalloc = testMalloc();
    size_t usable = malloc_usable_size(p);
    if (kMalloc != TestMalloc::Glibc) return usable;
    constexpr size_t kIsMapped = 0x2;
    size_t size_word = static_cast<const size_t*>(p)[-1];
    return usable + ((size_word & kIsMapped) ? mappedBlockHeader() : heapBlockHeader());
}

/// An allocator that bypasses the wrappers: the record's own memory.
template<typename T>
struct UnrecordedAllocator {
    using value_type = T;
    UnrecordedAllocator() = default;
    template<typename U> UnrecordedAllocator(const UnrecordedAllocator<U>&) noexcept {}
    T* allocate(size_t n) {
        void* p = __real_malloc(n * sizeof(T));
        if (p == nullptr) throw std::bad_alloc();
        return static_cast<T*>(p);
    }
    void deallocate(T* p, size_t) noexcept { __real_free(p); }
    template<typename U> bool operator==(const UnrecordedAllocator<U>&) const noexcept { return true; }
};

/// Whether the calling thread's allocations are recorded.
inline constinit thread_local bool tl_recording = false;

/// Whether anything was ever recorded: frees are looked up from then on.
inline constinit std::atomic<bool> g_recorded{false};

class ProcessBlocks {
public:
    static ProcessBlocks& instance() {
        static ProcessBlocks* blocks = new (__real_malloc(sizeof(ProcessBlocks))) ProcessBlocks();
        return *blocks;
    }

    void allocated(void* p) {
        size_t room = blockRoom(p);
        std::lock_guard lock(mu_);
        live_.emplace(p, room);
        room_ += static_cast<int64_t>(room);
    }

    void freed(void* p) {
        std::lock_guard lock(mu_);
        auto it = live_.find(p);
        if (it == live_.end()) return;
        room_ -= static_cast<int64_t>(it->second);
        live_.erase(it);
    }

    /// The room the recorded blocks still alive take.
    [[nodiscard]] int64_t room() {
        std::lock_guard lock(mu_);
        return room_;
    }

    /// How many recorded blocks are still alive.
    [[nodiscard]] size_t count() {
        std::lock_guard lock(mu_);
        return live_.size();
    }

private:
    std::mutex mu_;
    std::unordered_map<void*, size_t, std::hash<void*>, std::equal_to<void*>,
                       UnrecordedAllocator<std::pair<void* const, size_t>>> live_;
    int64_t room_ = 0;
};

/// Records the calling thread's allocations for its lifetime.
class Recording {
public:
    Recording() noexcept : previous_(tl_recording) {
        g_recorded.store(true, std::memory_order_relaxed);
        tl_recording = true;
    }
    ~Recording() { tl_recording = previous_; }
    Recording(const Recording&) = delete;
    Recording& operator=(const Recording&) = delete;

private:
    bool previous_;
};

/// Stops recording the calling thread's allocations for its lifetime.
class NotRecording {
public:
    NotRecording() noexcept : previous_(tl_recording) { tl_recording = false; }
    ~NotRecording() { tl_recording = previous_; }
    NotRecording(const NotRecording&) = delete;
    NotRecording& operator=(const NotRecording&) = delete;

private:
    bool previous_;
};

}  // namespace relais_test

extern "C" void* __wrap_malloc(size_t n) {
    void* p = __real_malloc(n);
    if (p != nullptr && relais_test::tl_recording) relais_test::ProcessBlocks::instance().allocated(p);
    return p;
}

extern "C" void* __wrap_calloc(size_t count, size_t n) {
    void* p = __real_calloc(count, n);
    if (p != nullptr && relais_test::tl_recording) relais_test::ProcessBlocks::instance().allocated(p);
    return p;
}

extern "C" void __wrap_free(void* p) {
    if (p != nullptr && relais_test::g_recorded.load(std::memory_order_relaxed))
        relais_test::ProcessBlocks::instance().freed(p);
    __real_free(p);
}

void* operator new(std::size_t size) {
    void* p = std::malloc(size == 0 ? 1 : size);
    if (p == nullptr) throw std::bad_alloc();
    return p;
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

#endif  // JCX_RELAIS_TEST_PROCESS_BLOCKS_H
