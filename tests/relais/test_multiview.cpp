/**
 * test_multiview.cpp
 * Pure unit tests for cache::MultiView<E> — no DB, no Redis, no event loop.
 *
 * MultiView is the guarded aggregate returned by findMany(): N positional
 * slots, each either absent (nullptr), pointing at an external L1 slot, or
 * pointing into its own owned_ fallback vector. The load-bearing invariant is
 * that owned_ is reserved up-front so adopt() never reallocates and dangles a
 * pointer already handed out. Moving the view must also keep owned_-pointing
 * slots valid (vector move preserves element addresses).
 */

#include <string>

#include <catch2/catch_test_macros.hpp>

#include "jcailloux/relais/cache/MultiView.h"

using jcailloux::relais::cache::MultiView;

namespace {

// Entity stand-in with a heap field, so a stale pointer / accidental copy is
// observable (the string would be moved-from or differ).
struct Probe {
    int id = 0;
    std::string name;
    bool operator==(const Probe&) const = default;
};

}  // namespace

TEST_CASE("MultiView default is empty", "[multiview]") {
    MultiView<Probe> v;
    REQUIRE(v.size() == 0);
    REQUIRE(v.empty());
    // take_guard() on an empty view is a no-op move, must not crash.
    auto g = v.take_guard();
    (void)g;
}

TEST_CASE("MultiView sized starts all-absent", "[multiview]") {
    MultiView<Probe> v(3);
    REQUIRE(v.size() == 3);
    REQUIRE_FALSE(v.empty());
    for (size_t i = 0; i < v.size(); ++i) {
        REQUIRE(v[i] == nullptr);
        REQUIRE_FALSE(v.has(i));
        REQUIRE_FALSE(static_cast<bool>(v[i]));  // operator bool by position
    }
}

TEST_CASE("MultiView points at external entities (zero-copy)", "[multiview]") {
    Probe a{1, "alice"};
    Probe b{2, "bob"};

    MultiView<Probe> v(3);
    v.pointAt(0, &a);
    // slot 1 left absent
    v.pointAt(2, &b);

    REQUIRE(v.has(0));
    REQUIRE_FALSE(v.has(1));
    REQUIRE(v.has(2));

    // Same address — proof we point, not copy.
    REQUIRE(v[0] == &a);
    REQUIRE(v[2] == &b);
    REQUIRE(*v[0] == a);
    REQUIRE(v[1] == nullptr);
}

TEST_CASE("MultiView adopt keeps owned_ pointers stable (no realloc)", "[multiview]") {
    MultiView<Probe> v(4);
    v.reserveOwned(4);  // freeze capacity before taking any pointer

    v.adopt(0, Probe{10, "ten"});
    const Probe* first = v[0];           // capture pointer into owned_

    // Further adopts must NOT reallocate owned_ → first stays valid.
    v.adopt(1, Probe{11, "eleven"});
    v.adopt(2, Probe{12, "twelve"});
    v.adopt(3, Probe{13, "thirteen"});

    REQUIRE(v[0] == first);              // address unchanged across 3 adopts
    REQUIRE(*v[0] == Probe{10, "ten"});
    REQUIRE(*v[3] == Probe{13, "thirteen"});

    // A duplicate output position can alias an already-owned entry, exactly as
    // LocalRepo does for repeated ids — pointAt the same pointer, no second copy.
    v.pointAt(3, v[0]);
    REQUIRE(v[3] == first);
}

TEST_CASE("MultiView mixes external and owned slots", "[multiview]") {
    Probe external{99, "ext"};

    MultiView<Probe> v(3);
    v.reserveOwned(2);
    v.pointAt(0, &external);
    v.adopt(1, Probe{1, "owned-one"});
    v.adopt(2, Probe{2, "owned-two"});

    REQUIRE(v[0] == &external);          // external untouched
    REQUIRE(*v[1] == Probe{1, "owned-one"});
    REQUIRE(*v[2] == Probe{2, "owned-two"});
}

TEST_CASE("MultiView move preserves owned_ slots", "[multiview]") {
    MultiView<Probe> v(2);
    v.reserveOwned(2);
    v.adopt(0, Probe{1, "one"});
    v.adopt(1, Probe{2, "two"});

    SECTION("move-construct") {
        MultiView<Probe> moved(std::move(v));
        REQUIRE(moved.size() == 2);
        // vector move keeps element addresses → owned_-pointing slots valid
        REQUIRE(*moved[0] == Probe{1, "one"});
        REQUIRE(*moved[1] == Probe{2, "two"});
    }

    SECTION("move-assign") {
        MultiView<Probe> target;
        target = std::move(v);
        REQUIRE(target.size() == 2);
        REQUIRE(*target[0] == Probe{1, "one"});
        REQUIRE(*target[1] == Probe{2, "two"});
    }
}

TEST_CASE("MultiView iteration yields positional pointers", "[multiview]") {
    Probe a{1, "a"};
    MultiView<Probe> v(3);
    v.pointAt(1, &a);

    size_t present = 0, absent = 0;
    for (const Probe* p : v) {
        if (p) ++present; else ++absent;
    }
    REQUIRE(present == 1);
    REQUIRE(absent == 2);
}
