#ifndef JCX_RELAIS_IO_TEST_EPOLL_IO_CONTEXT_H
#define JCX_RELAIS_IO_TEST_EPOLL_IO_CONTEXT_H

// Redirect to the promoted library version.
// Test code uses jcailloux::relais::io::test::EpollIoContext — alias below.
#include <jcailloux/relais/io/EpollIoContext.h>

namespace jcailloux::relais::io::test {
    using EpollIoContext = jcailloux::relais::io::EpollIoContext;
} // namespace jcailloux::relais::io::test

#endif // JCX_RELAIS_IO_TEST_EPOLL_IO_CONTEXT_H
