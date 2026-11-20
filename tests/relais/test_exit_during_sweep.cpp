/**
 * test_exit_during_sweep.cpp
 *
 * Returns from main while the maintenance thread sweeps. The sweep in progress
 * must end before the repositories' state is destroyed, and none may start
 * after. No database; exit code 0 on success.
 *
 * A registry entry of the test sleeps inside the sweep, then sweeps the
 * repository. A sentinel built before the repository's state is destroyed
 * after it: the sweep that wakes up and finds the sentinel gone ran past the
 * repository's state, and fails the test. The sentinel then waits, so such a
 * sweep has time to wake up before the process ends.
 */

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include <jcailloux/relais/cache/GDSFPolicy.h>

#include "fixtures/TestRepositories.h"

namespace {

using jcailloux::relais::cache::GDSFPolicy;
using Repo = relais_test::TestArticleListRepo;
using namespace std::chrono_literals;

std::atomic<bool> g_sentinel_gone{false};
std::atomic<bool> g_sweep_entered{false};

struct Sentinel {
    ~Sentinel() {
        g_sentinel_gone.store(true);
        std::this_thread::sleep_for(500ms);
    }
};

size_t slowSweep(long chunk_id) {
    g_sweep_entered.store(true);
    g_sweep_entered.notify_all();
    std::this_thread::sleep_for(200ms);
    if (g_sentinel_gone.load()) {
        std::fputs("a sweep ran after the repositories' state was destroyed\n", stderr);
        std::_Exit(1);
    }
    return Repo::sweep(chunk_id);
}

size_t noEntries() { return 0; }

}  // namespace

int main() {
    static Sentinel sentinel;

    (void)Repo::purge();  // builds the repository's state: entities and list
    GDSFPolicy::instance().enroll({&slowSweep, &noEntries, "test:slow"});

    for (size_t i = 0; i <= GDSFPolicy::kCleanupMask; ++i) {
        GDSFPolicy::instance().tickInsertion();
    }
    g_sweep_entered.wait(false);
    return 0;  // the sweep is in progress
}
