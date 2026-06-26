#include <catch2/catch_test_macros.hpp>

#include <fixtures/PgProbe.h>

using jcailloux::relais::io::test::PgProbe;

namespace {
const char* conninfo() {
    return "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test";
}
}  // namespace

// Smoke test: PgProbe must read DB ground truth over a channel independent of
// the relais pool/cache. The liveness/coherence suites rely on it to tell
// "committed" from "rolled back" after a client-side timeout.
TEST_CASE("PgProbe: reads DB state over an independent channel", "[io][integration][fixture]") {
    PgProbe probe(conninfo());

    auto r = probe.query("SELECT 1 AS one");
    REQUIRE(r.rows() == 1);
    REQUIRE(r.cols() == 1);
    REQUIRE_FALSE(r.empty());
    REQUIRE(r.get(0, 0) == "1");

    auto nullr = probe.query("SELECT NULL::int");
    REQUIRE(nullr.isNull(0, 0));
    REQUIRE_FALSE(nullr.getOpt(0, 0).has_value());
}

// exists() is the presence check the erase/insert coherence tests will use.
TEST_CASE("PgProbe: exists() reflects ground-truth presence", "[io][integration][fixture]") {
    PgProbe probe(conninfo());
    REQUIRE(probe.exists("(VALUES (1)) AS t(x)", "x = 1"));
    REQUIRE_FALSE(probe.exists("(VALUES (1)) AS t(x)", "x = 2"));
}

// terminateBackends() with a filter that matches nothing must be a safe no-op —
// the session-kill RST injection path of §10.4 T1, exercised without collateral.
TEST_CASE("PgProbe: terminateBackends with no match is a no-op", "[io][integration][fixture]") {
    PgProbe probe(conninfo());
    REQUIRE(probe.terminateBackends("application_name = 'no_such_app_xyz_relais'") == 0);
}
