// A container whose allocator reports its blocks to the L1 ledger must not be
// weighed by heapBytes as well: its blocks would be counted twice.
#include <vector>

#include <jcailloux/relais/cache/L1Ledger.h>

using Counted = std::vector<int, jcailloux::relais::cache::detail::LedgerAllocator<int>>;

size_t count(const Counted& v) {
    return jcailloux::relais::detail::heapBytes(v);
}
