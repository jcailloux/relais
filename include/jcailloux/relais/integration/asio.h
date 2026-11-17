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
// thread, attach relais to each. A coroutine of asio, or any completion token,
// awaits a repository call through relais::async (asio 1.27 or later):
//
//   auto item = co_await jcailloux::relais::async(ItemRepo::find(id), asio::use_awaitable);

#include <exception>
#include <string>
#include <utility>

#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/posix/stream_descriptor.hpp>
#include <asio/steady_timer.hpp>
#include <asio/version.hpp>

#if ASIO_VERSION >= 102700
#include <asio/append.hpp>
#include <asio/associated_cancellation_slot.hpp>
#include <asio/associated_immediate_executor.hpp>
#include <asio/async_result.hpp>
#include <asio/cancellation_type.hpp>
#include <asio/dispatch.hpp>
#include <asio/error.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/system_error.hpp>
#endif

#include "jcailloux/relais/integration/detail/basic_asio.h"

namespace jcailloux::relais::io {

namespace detail {

struct StandaloneAsio {
    using execution_context = ::asio::execution_context;
    using io_context = ::asio::io_context;
    using descriptor = ::asio::posix::stream_descriptor;
    using timer = ::asio::steady_timer;
    using error_code = ::asio::error_code;

    // To an io_context or an executor.
    template<typename Target, typename F>
    static void post(Target&& target, F&& f) {
        ::asio::post(std::forward<Target>(target), std::forward<F>(f));
    }

    template<typename Service>
    static Service& use_service(io_context& ctx) {
        return ::asio::use_service<Service>(ctx);
    }

#if ASIO_VERSION >= 102700
    // What relais::async needs besides.
    using cancellation_type = ::asio::cancellation_type;

    template<typename Signature, typename Token, typename Initiation, typename... Args>
    static auto async_initiate(Initiation&& initiation, Token& token, Args&&... args) {
        return ::asio::async_initiate<Token, Signature>(std::forward<Initiation>(initiation), token,
                                                        std::forward<Args>(args)...);
    }

    template<typename Handler, typename... Values>
    static auto append(Handler&& handler, Values&&... values) {
        return ::asio::append(std::forward<Handler>(handler), std::forward<Values>(values)...);
    }

    template<typename Executor, typename Token>
    static void dispatch(const Executor& ex, Token&& token) {
        ::asio::dispatch(ex, std::forward<Token>(token));
    }

    template<typename Handler, typename Executor>
    static auto immediate_executor(const Handler& handler, const Executor& ex) noexcept {
        return ::asio::get_associated_immediate_executor(handler, ex);
    }

    template<typename Handler>
    static auto cancellation_slot(const Handler& handler) noexcept {
        return ::asio::get_associated_cancellation_slot(handler);
    }

    template<typename Handler, typename Executor>
    static auto work_guard(const Handler& handler, const Executor& ex) {
        return ::asio::make_work_guard(handler, ex);
    }

    static std::exception_ptr aborted() {
        return std::make_exception_ptr(::asio::system_error(::asio::error::operation_aborted));
    }
#endif
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

#if ASIO_VERSION >= 102700

/// Runs `task` as an asio asynchronous operation, which completes with
/// (std::exception_ptr, T), as co_spawn does, through whatever `token` asks
/// for: a coroutine (asio::use_awaitable, asio::deferred, the default from asio
/// 1.29), a callback, a future. Call it on the thread of an io_context relais
/// is attached to (std::logic_error otherwise), where `task` starts at once.
///
/// What `task` refers to must outlive the operation, as an asio buffer does.
/// The handler runs on its associated executor, by default the io_context.
/// Cancelled (terminal or partial), the operation completes at once with
/// asio::error::operation_aborted, and the request runs on: its result is
/// dropped, relais's cache stays valid. With integration/boost_asio.h in the
/// same translation unit, every call to it is ambiguous.
template<typename T,
         ::asio::completion_token_for<detail::AsyncSignature<T>> Token =
             ::asio::default_completion_token_t<::asio::io_context::executor_type>>
auto async(io::Task<T> task, Token&& token = Token()) {
    return detail::asyncOn<io::detail::StandaloneAsio, Token>(std::move(task), token);
}

/// As above, for a call that may be ready at once, as a hit in relais's cache:
/// the handler then completes through its immediate executor, by default posted
/// to the io_context. asio::bind_immediate_executor completes it in place, and
/// a coroutine then resumes one level deeper on the stack for each such call
/// in a row, as with any asio operation.
template<typename T,
         ::asio::completion_token_for<detail::AsyncSignature<T>> Token =
             ::asio::default_completion_token_t<::asio::io_context::executor_type>>
auto async(io::Immediate<T> call, Token&& token = Token()) {
    return detail::asyncOn<io::detail::StandaloneAsio, Token>(std::move(call), token);
}

#endif

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_INTEGRATION_ASIO_H
