#ifndef JCX_RELAIS_TEST_ASIO_FLAVOR_H
#define JCX_RELAIS_TEST_ASIO_FLAVOR_H

// The asio a test runs on: Boost.Asio when built with RELAIS_TEST_BOOST_ASIO,
// standalone asio otherwise. The tests name it `net`, its adapter AsioIo.

#ifdef RELAIS_TEST_BOOST_ASIO
#include <jcailloux/relais/integration/boost_asio.h>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

namespace relais_test {
namespace net = ::boost::asio;
using AsioIo = ::jcailloux::relais::io::BoostAsioIoContext;
inline constexpr const char* kAsioFlavor = "Boost.Asio";
}  // namespace relais_test
#else
#include <jcailloux/relais/integration/asio.h>

#include <asio/dispatch.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>

namespace relais_test {
namespace net = ::asio;
using AsioIo = ::jcailloux::relais::io::AsioIoContext;
inline constexpr const char* kAsioFlavor = "asio";
}  // namespace relais_test
#endif

#endif  // JCX_RELAIS_TEST_ASIO_FLAVOR_H
