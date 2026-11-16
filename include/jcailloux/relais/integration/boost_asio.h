#ifndef JCX_RELAIS_INTEGRATION_BOOST_ASIO_H
#define JCX_RELAIS_INTEGRATION_BOOST_ASIO_H

// relais on a boost::asio::io_context (Boost.Asio; integration/asio.h for
// standalone asio). Including this header needs Boost; relais itself never
// does.
//
//   boost::asio::io_context ctx;               // run by one thread, elsewhere
//   auto relais = jcailloux::relais::attach(ctx, conninfo);
//
// relais binds to the thread that runs the io_context, where repository calls
// then run: one thread per io_context. With several io_contexts, one per
// thread, attach relais to each.

#include <string>
#include <utility>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>

#include "jcailloux/relais/integration/detail/basic_asio.h"

namespace jcailloux::relais::io {

namespace detail {

struct BoostAsio {
    using execution_context = ::boost::asio::execution_context;
    using io_context = ::boost::asio::io_context;
    using descriptor = ::boost::asio::posix::stream_descriptor;
    using timer = ::boost::asio::steady_timer;
    using error_code = ::boost::system::error_code;

    template<typename F>
    static void post(io_context& ctx, F&& f) {
        ::boost::asio::post(ctx, std::forward<F>(f));
    }

    template<typename Service>
    static Service& use_service(io_context& ctx) {
        return ::boost::asio::use_service<Service>(ctx);
    }
};

}  // namespace detail

// The IoContext of a boost::asio::io_context; see BasicAsioIoContext.
using BoostAsioIoContext = BasicAsioIoContext<detail::BoostAsio>;

static_assert(IoContext<BoostAsioIoContext>);

}  // namespace jcailloux::relais::io

namespace jcailloux::relais {

/// Binds relais to `ctx`, which one thread runs, as attach(io, …) binds it to a
/// loop: checks the io_context in a few milliseconds (AttachError if several
/// threads run it, or names what it fails), creates the pool on it, and binds
/// its thread, where repository calls then run. Blocks until done; call it
/// from another thread, while `ctx` runs (a work guard keeps run() from
/// returning first). `ctx` must outlive the returned Attachment.
[[nodiscard]] inline Attachment<io::BoostAsioIoContext>
attach(::boost::asio::io_context& ctx, std::string conninfo, AttachConfig cfg = {}) {
    return detail::attachAsio<io::detail::BoostAsio>(ctx, std::move(conninfo), cfg);
}

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INTEGRATION_BOOST_ASIO_H
