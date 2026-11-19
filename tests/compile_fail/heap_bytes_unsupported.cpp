// A member whose type no heapBytes rule covers must not compile: counting it
// as 0 would let the entry pass for lighter than it is.
#include <list>

#include <jcailloux/relais/HeapBytes.h>

struct Holder {
    std::list<int> values;
};

size_t count(const Holder& h) {
    return jcailloux::relais::detail::heapBytes(h);
}
