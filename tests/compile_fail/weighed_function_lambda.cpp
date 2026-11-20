// A lambda kept by the L1 must not compile: what it captures is hidden from
// heapBytes, and it would weigh 0. A named struct says what it holds.
#include <string>

#include <jcailloux/relais/WeighedFunction.h>

jcailloux::relais::detail::WeighedFunction<bool(int)> rowsOf(std::string name) {
    return [name = std::move(name)](int) { return !name.empty(); };
}
