// Default LeakSanitizer suppressions for every test executable.
//
// parlay_hash stores its entry pointers tagged with 16 hash bits in the high bits
// (IndirectEntries::tag_ptr). LSan does not recognise a tagged word as a
// reference, so an entry still live in a static L1 cache at exit is reported as
// a direct leak. The rule is scoped to parlay's epoch pool allocator, which
// allocates exactly those entries; leaks from any other allocation site are
// still reported.
//
// The sanitizer runtime calls this hook at startup, so the rule applies to a
// directly launched executable as well as under CTest. Without ASan the symbol
// is never referenced.

extern "C" const char* __lsan_default_suppressions() {
    return "leak:epoch::internal::memory_pool*::allocate_wrapper\n";
}
