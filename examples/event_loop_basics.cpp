// event_loop_basics.cpp — the relais I/O runtime in miniature. No database.
//
// Shows the building blocks every relais runtime is made of:
//   1. an EpollIoContext event loop running on its own thread,
//   2. a lazy Task (coroutine) driven to completion ON that loop,
//   3. spawnOn — the thread-safe bridge that kicks work onto the loop from
//      another thread and hands the result back,
//   4. a postDelayed one-shot timer fired by the loop.
//
// This is exactly the machinery IoPool multiplies across N cores (see
// iopool_nloop.cpp) and that a foreign-loop adapter plugs into
// (see docs/io-context-adapters.md).
//
// Build: cmake -B .build/dev -DRELAIS_BUILD_EXAMPLES=ON
//        cmake --build .build/dev --target example_event_loop_basics
// Run:   ./.build/dev/example_event_loop_basics

#include <jcailloux/relais/io/EpollIoContext.h>
#include <jcailloux/relais/io/Task.h>
#include <jcailloux/relais/runtime/Spawn.h>

#include <chrono>
#include <future>
#include <iostream>
#include <thread>

using namespace jcailloux::relais;
using io::EpollIoContext;
using io::Task;

// A lazy coroutine: nothing runs until it is co_awaited on a loop.
Task<int> compute(int x) {
    co_return x * 2;
}

int main() {
    EpollIoContext io;

    // One "runtime loop": run the event loop on its own thread.
    std::thread loop([&] { io.run(); });

    std::cout << "main thread : " << std::this_thread::get_id() << '\n';
    std::cout << "loop thread : " << loop.get_id() << "\n\n";

    // 1) Cross-thread bridge. compute(21) is lazy; spawnOn drives it on the loop
    //    and invokes the callback ON the loop thread with the outcome. We funnel
    //    that back to main with a promise/future.
    std::promise<int> result;
    auto result_fut = result.get_future();
    spawnOn(io, compute(21), [&result](Outcome<int> r) {
        std::cout << "compute ran on : " << std::this_thread::get_id()
                  << "  (the loop thread)\n";
        result.set_value(r ? *r : -1);
    });
    std::cout << "compute(21) = " << result_fut.get() << "\n\n";

    // 2) A one-shot timer fired by the loop after a delay (thread-safe to post
    //    from here; it wakes the loop).
    std::promise<void> timer_done;
    auto timer_fut = timer_done.get_future();
    io.postDelayed(std::chrono::milliseconds(20), [&timer_done] {
        std::cout << "timer fired (~20ms later) on the loop thread\n";
        timer_done.set_value();
    });
    timer_fut.wait();

    io.stop();
    loop.join();
    std::cout << "\ndone.\n";
    return 0;
}