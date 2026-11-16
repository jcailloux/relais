#ifndef JCX_RELAIS_INTEGRATION_ASIO_H
#define JCX_RELAIS_INTEGRATION_ASIO_H

// relais on an asio::io_context (standalone asio; integration/boost_asio.h for
// Boost.Asio). Including this header needs asio; relais itself never does.
//
//   asio::io_context ctx;                      // run by one thread, elsewhere
//   auto relais = jcailloux::relais::attach(ctx, conninfo);
//
// relais binds to the thread that runs the io_context, where repository calls
// then run: one thread per io_context. With several io_contexts, one per
// thread, attach relais to each.

#include <string>
#include <utility>

#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/steady_timer.hpp>

#include "jcailloux/relais/integration/detail/basic_asio.h"

namespace jcailloux::relais::io {

namespace detail {

struct StandaloneAsio {
    using execution_context = ::asio::execution_context;
    using io_context = ::asio::io_context;
    using descriptor = ::asio::posix::stream_descriptor;
    using timer = ::asio::steady_timer;
    using error_code = ::asio::error_code;

    template<typename F>
    static void post(io_context& ctx, F&& f) {
        ::asio::post(ctx, std::forward<F>(f));
    }

    template<typename Service>
    static Service& use_service(io_context& ctx) {
        return ::asio::use_service<Service>(ctx);
    }
};

}  // namespace detail

// The IoContext of an asio::io_context; see BasicAsioIoContext.
using AsioIoContext = BasicAsioIoContext<detail::StandaloneAsio>;

static_assert(IoContext<AsioIoContext>);

}  // namespace jcailloux::relais::io

namespace jcailloux::relais {

/// Binds relais to `ctx`, which one thread runs, as attach(io, …) binds it to a
/// loop: checks the io_context in a few milliseconds (AttachError if several
/// threads run it, or names what it fails), creates the pool on it, and binds
/// its thread, where repository calls then run. Blocks until done; call it
/// from another thread, while `ctx` runs (a work guard keeps run() from
/// returning first). `ctx` must outlive the returned Attachment.
[[nodiscard]] inline Attachment<io::AsioIoContext>
attach(::asio::io_context& ctx, std::string conninfo, AttachConfig cfg = {}) {
    return detail::attachAsio<io::detail::StandaloneAsio>(ctx, std::move(conninfo), cfg);
}

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INTEGRATION_ASIO_H
