#ifndef JCX_RELAIS_INTEGRATION_DROGON_H
#define JCX_RELAIS_INTEGRATION_DROGON_H

// relais on Drogon. Including this header needs Drogon; relais itself never
// does.
//
//   jcailloux::relais::attachDrogon(conninfo);
//   drogon::app().addListener("0.0.0.0", 8080).setThreadNum(4).run();
//
// Handlers then call relais directly: each IO loop has its own pool.

#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <drogon/HttpAppFramework.h>
#include <drogon/plugins/Plugin.h>

#include "jcailloux/relais/integration/trantor.h"
#include "jcailloux/relais/runtime/Attach.h"

namespace jcailloux::relais {

namespace detail {

// What attachDrogon() binds. Never destroyed: Drogon's loops and app() end with
// the process, in an order relais does not control.
struct DrogonLoops {
    std::mutex m;
    bool requested = false;
    std::vector<std::unique_ptr<io::TrantorIoContext>> loops;
    std::vector<std::shared_ptr<LoopBinding<io::TrantorIoContext>>> bindings;
};

inline DrogonLoops& drogonLoops() {
    static auto* loops = new DrogonLoops;
    return *loops;
}

// Drogon shuts its plugins down when it quits, on its main loop, before it
// stops the IO loops: relais leaves them there, while they still run.
class DrogonTeardown final : public ::drogon::Plugin<DrogonTeardown> {
public:
    void initAndStart(const Json::Value&) override {}

    void shutdown() override {
        auto& state = drogonLoops();
        std::vector<std::shared_ptr<LoopBinding<io::TrantorIoContext>>> bindings;
        {
            std::lock_guard lock(state.m);
            bindings = std::move(state.bindings);
            state.bindings.clear();
        }
        releaseAll<io::TrantorIoContext>(bindings);
    }
};

}  // namespace detail

/// Binds relais to every IO loop of drogon::app(), where its handlers run, as
/// attach() does for one loop: once Drogon has started them, before it serves
/// a request. If a loop cannot be bound (it fails the check, or PostgreSQL
/// does not answer within cfg.pool.acquire_timeout), Drogon quits without
/// serving and drogon::app().run() throws that error. When Drogon quits,
/// relais leaves its loops before they stop, each waiting for its requests in
/// flight.
///
/// Call it once, before drogon::app().run(). Drogon's main loop, where its
/// beginning advices and app().getLoop() timers run, is not bound.
inline void attachDrogon(std::string conninfo, AttachConfig cfg = {}) {
    auto& app = ::drogon::app();
    auto& state = detail::drogonLoops();
    if (app.isRunning())
        throw AttachError("relais::attachDrogon() called while Drogon runs: call it before "
                          "drogon::app().run()");
    {
        std::lock_guard lock(state.m);
        if (state.requested) throw AttachError("relais::attachDrogon() called twice");
        state.requested = true;
    }
    app.addPlugin(detail::DrogonTeardown::classTypeName(), {}, Json::Value());
    app.registerBeginningAdvice([conninfo = std::move(conninfo), cfg] {
        auto& app = ::drogon::app();
        auto& state = detail::drogonLoops();
        std::vector<io::TrantorIoContext*> loops;
        {
            std::lock_guard lock(state.m);
            for (std::size_t i = 0; i < app.getThreadNum(); ++i) {
                state.loops.push_back(std::make_unique<io::TrantorIoContext>(app.getIOLoop(i)));
                loops.push_back(state.loops.back().get());
            }
        }
        try {
            auto bindings = *detail::bindAll<io::TrantorIoContext>(loops, conninfo, cfg);
            std::lock_guard lock(state.m);
            state.bindings = std::move(bindings);
        } catch (...) {
            // Drogon quits first, which stops its loops and servers, and then
            // run() throws the error. Thrown from here, it would leave them
            // running for the process to destroy from the wrong thread.
            app.getLoop()->runOnQuit(
                [error = std::current_exception()] { std::rethrow_exception(error); });
            app.quit();
        }
    });
}

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INTEGRATION_DROGON_H
