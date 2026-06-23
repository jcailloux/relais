#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/PgProvider.h>

#include <stdexcept>

using jcailloux::relais::PgProvider;

// =============================================================================
// Pre-init contract: calling a PgProvider entry point before init() on this
// thread throws std::logic_error (it does NOT assert/abort). This holds in
// every build configuration, including Release where assert() is compiled out.
//
// The five non-coroutine entry points throw synchronously on call. The Redis
// helpers (redis/redisDynamic) are coroutines with suspend_always
// initial_suspend — their throw surfaces on co_await, not on call — so they
// require a running loop and are covered by the integration suites.
// =============================================================================

TEST_CASE("PgProvider entry points throw logic_error before init", "[pg-provider][preinit]") {
    // Ensure an unbound thread regardless of test ordering.
    PgProvider::reset();
    REQUIRE_FALSE(PgProvider::initialized());

    const jcailloux::relais::io::PgParams params;

    SECTION("query") {
        REQUIRE_THROWS_AS(PgProvider::query("SELECT 1"), std::logic_error);
    }
    SECTION("queryParams") {
        REQUIRE_THROWS_AS(PgProvider::queryParams("SELECT 1", params), std::logic_error);
    }
    SECTION("entityQueryParams") {
        REQUIRE_THROWS_AS(
            PgProvider::entityQueryParams("SELECT 1", "SELECT 1", params),
            std::logic_error);
    }
    SECTION("entityQueryParamsMany") {
        REQUIRE_THROWS_AS(
            PgProvider::entityQueryParamsMany("SELECT 1", "SELECT 1", {}),
            std::logic_error);
    }
    SECTION("queryWrite") {
        REQUIRE_THROWS_AS(PgProvider::queryWrite("UPDATE t SET x=1", params),
                          std::logic_error);
    }
}
