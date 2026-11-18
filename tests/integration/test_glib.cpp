// relais on a GLib main loop, which relais ships no adapter for: through the
// descriptor of integration/fd.h, as on any other loop. The loop knows relais
// by g_unix_fd_add alone, as the documentation shows: requests made before the
// loop runs and from its callbacks are served, on_ready runs from the loop,
// relais leaves the loop idle once nothing is left to do, and detach leaves no
// descriptor behind.

#include <catch2/catch_test_macros.hpp>

#include <glib-unix.h>
#include <glib.h>

#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/integration/fd.h>

#include <fixtures/LivenessHarness.h>
#include <fixtures/TestConnInfo.h>

#include <chrono>
#include <exception>
#include <string>
#include <thread>

using jcailloux::relais::FdAttachment;
using jcailloux::relais::PgProvider;
using jcailloux::relais::io::DetachedTask;
using jcailloux::relais::io::Task;
using jcailloux::relais::io::test::countOpenFds;

namespace {

Task<std::string> firstValue(const char* sql) {
    auto result = co_await PgProvider::query(sql);
    co_return std::string(result[0].rawValue(0));
}

struct State {
    GMainLoop* loop = nullptr;
    FdAttachment* relais = nullptr;
    int polls = 0;
    int ready_calls = 0;
    std::exception_ptr ready_error;
    std::thread::id ready_thread;
    std::string before_run;   // asked for before the loop runs
    std::string from_loop;    // asked for from one of the loop's callbacks

    void quitOnceDone() {
        if (ready_calls > 0 && !before_run.empty() && !from_loop.empty()) g_main_loop_quit(loop);
    }
};

DetachedTask ask(const char* sql, std::string& out, State& state) {
    try {
        out = co_await firstValue(sql);
    } catch (const std::exception& e) {
        out = std::string("error: ") + e.what();
    }
    state.quitOnceDone();
}

// Runs the loop for `duration`, or until something quits it first.
void runFor(GMainLoop* loop, std::chrono::milliseconds duration) {
    const guint timeout = g_timeout_add(static_cast<guint>(duration.count()), [](gpointer l) -> gboolean {
        g_main_loop_quit(static_cast<GMainLoop*>(l));
        return G_SOURCE_REMOVE;
    }, loop);
    g_main_loop_run(loop);
    if (GSource* source = g_main_context_find_source_by_id(nullptr, timeout)) g_source_destroy(source);
}

}  // namespace

TEST_CASE("glib: relais on a GLib main loop, through its descriptor", "[glib][fd][integration]") {
    GMainLoop* loop = g_main_loop_new(nullptr, FALSE);
    runFor(loop, std::chrono::milliseconds(1));  // GLib's own descriptors, before counting
    const int fds = countOpenFds();
    {
        State state{.loop = loop};
        auto relais = jcailloux::relais::attachHere(
            relais_test::kConnInfo, {.pool = {.min_connections = 1, .max_connections = 2}},
            [&state](std::exception_ptr e) {
                ++state.ready_calls;
                state.ready_error = e;
                state.ready_thread = std::this_thread::get_id();
                state.quitOnceDone();
            });
        state.relais = &relais;

        // What the documentation shows.
        const guint watch = g_unix_fd_add(relais.fd(), G_IO_IN, [](int, GIOCondition, gpointer data) -> gboolean {
            auto& s = *static_cast<State*>(data);
            ++s.polls;
            s.relais->poll();
            return G_SOURCE_CONTINUE;
        }, &state);

        ask("SELECT 6 * 7", state.before_run, state);
        g_idle_add([](gpointer data) -> gboolean {
            auto& s = *static_cast<State*>(data);
            ask("SELECT 40 + 2", s.from_loop, s);
            return G_SOURCE_REMOVE;
        }, &state);

        runFor(loop, std::chrono::seconds(10));
        CHECK(state.before_run == "42");
        CHECK(state.from_loop == "42");
        CHECK(state.ready_calls == 1);
        CHECK_FALSE(state.ready_error);
        CHECK(state.ready_thread == std::this_thread::get_id());
        CHECK(relais.ready());

        // Nothing left to do: the descriptor stays quiet, and the loop idle.
        runFor(loop, std::chrono::milliseconds(50));
        const int polls = state.polls;
        runFor(loop, std::chrono::milliseconds(300));
        CHECK(state.polls == polls);

        g_source_remove(watch);  // stop watching it before detach() closes it
        relais.detach();
        CHECK_FALSE(PgProvider::initialized());
    }
    CHECK(countOpenFds() == fds);
    g_main_loop_unref(loop);
}
