#ifndef JCX_RELAIS_INTEGRATION_BOOST_ASIO_H
#define JCX_RELAIS_INTEGRATION_BOOST_ASIO_H

// relais on a boost::asio::io_context (Boost.Asio; integration/asio.h for
// standalone asio). Including this header needs Boost; relais itself never
// does.
//
//   boost::asio::io_context ctx;               // run by one thread, elsewhere
//   auto relais = jcailloux::relais::attach(ctx, conninfo);
//
// or, on the thread that runs it (from a handler, or before run()):
//
//   auto relais = jcailloux::relais::attachHere(ctx, conninfo);
//
// relais binds to the thread that runs the io_context, where repository calls
// then run: one thread per io_context. With several io_contexts, one per
// thread, attach relais to each. A coroutine of Boost.Asio, or any completion
// token, awaits a repository call through relais::async (Boost 1.82 or later):
//
//   auto item = co_await jcailloux::relais::async(ItemRepo::find(id),
//                                                 boost::asio::use_awaitable);

#include <exception>
#include <functional>
#include <string>
#include <utility>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/version.hpp>

#if BOOST_ASIO_VERSION >= 102700
#include <boost/asio/append.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_immediate_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/system/system_error.hpp>
#endif

#include "jcailloux/relais/integration/detail/basic_asio.h"

namespace jcailloux::relais::io {

namespace detail {

struct BoostAsio {
    using execution_context = ::boost::asio::execution_context;
    using io_context = ::boost::asio::io_context;
    using descriptor = ::boost::asio::posix::stream_descriptor;
    using timer = ::boost::asio::steady_timer;
    using error_code = ::boost::system::error_code;

    // To an io_context or an executor.
    template<typename Target, typename F>
    static void post(Target&& target, F&& f) {
        ::boost::asio::post(std::forward<Target>(target), std::forward<F>(f));
    }

    template<typename Service>
    static Service& use_service(io_context& ctx) {
        return ::boost::asio::use_service<Service>(ctx);
    }

#if BOOST_ASIO_VERSION >= 102700
    // What relais::async needs besides.
    using cancellation_type = ::boost::asio::cancellation_type;

    template<typename Signature, typename Token, typename Initiation, typename... Args>
    static auto async_initiate(Initiation&& initiation, Token& token, Args&&... args) {
        return ::boost::asio::async_initiate<Token, Signature>(
            std::forward<Initiation>(initiation), token, std::forward<Args>(args)...);
    }

    template<typename Handler, typename... Values>
    static auto append(Handler&& handler, Values&&... values) {
        return ::boost::asio::append(std::forward<Handler>(handler),
                                     std::forward<Values>(values)...);
    }

    template<typename Executor, typename Token>
    static void dispatch(const Executor& ex, Token&& token) {
        ::boost::asio::dispatch(ex, std::forward<Token>(token));
    }

    template<typename Handler, typename Executor>
    static auto immediate_executor(const Handler& handler, const Executor& ex) noexcept {
        return ::boost::asio::get_associated_immediate_executor(handler, ex);
    }

    template<typename Handler>
    static auto cancellation_slot(const Handler& handler) noexcept {
        return ::boost::asio::get_associated_cancellation_slot(handler);
    }

    template<typename Handler, typename Executor>
    static auto work_guard(const Handler& handler, const Executor& ex) {
        return ::boost::asio::make_work_guard(handler, ex);
    }

    static std::exception_ptr aborted() {
        return std::make_exception_ptr(
            ::boost::system::system_error(::boost::asio::error::operation_aborted));
    }
#endif
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

/// Binds relais to `ctx` on the calling thread, which runs it, as
/// attachHere(io, …) binds it to a loop: from one of its handlers, or before
/// run() starts there. Returns at once; see attachHere(io, …) for on_ready and
/// what follows. relais's callbacks then run on this thread only: another
/// thread that runs one of them ends the process. `ctx` must outlive the
/// returned Attachment.
[[nodiscard]] inline Attachment<io::BoostAsioIoContext>
attachHere(::boost::asio::io_context& ctx, std::string conninfo, AttachConfig cfg = {},
           std::function<void(std::exception_ptr)> on_ready = {}) {
    return detail::attachAsioHere<io::detail::BoostAsio>(ctx, std::move(conninfo), cfg,
                                                       std::move(on_ready));
}

#if BOOST_ASIO_VERSION >= 102700

/// relais::async of integration/asio.h, on Boost.Asio. With both headers in one
/// translation unit, every call to it is ambiguous: call it where one is.
template<typename T,
         ::boost::asio::completion_token_for<detail::AsyncSignature<T>> Token =
             ::boost::asio::default_completion_token_t<::boost::asio::io_context::executor_type>>
auto async(io::Task<T> task, Token&& token = Token()) {
    return detail::asyncOn<io::detail::BoostAsio, Token>(std::move(task), token);
}

template<typename T,
         ::boost::asio::completion_token_for<detail::AsyncSignature<T>> Token =
             ::boost::asio::default_completion_token_t<::boost::asio::io_context::executor_type>>
auto async(io::Immediate<T> call, Token&& token = Token()) {
    return detail::asyncOn<io::detail::BoostAsio, Token>(std::move(call), token);
}

#endif

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INTEGRATION_BOOST_ASIO_H
