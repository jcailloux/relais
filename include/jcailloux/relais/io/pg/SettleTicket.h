#ifndef JCX_RELAIS_IO_PG_SETTLE_TICKET_H
#define JCX_RELAIS_IO_PG_SETTLE_TICKET_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "jcailloux/relais/io/Task.h"

namespace jcailloux::relais::io {

// How relais settles the outcome of an uncertain write: a write abandoned on a
// timeout or a lost connection may still commit server-side, after the cache
// was evicted. Settling waits until it can no longer commit.
enum class UncertainSettle : uint8_t {
    Terminate,  // terminate the backend that ran the write (direct connection)
    Await,      // wait for that backend to leave on its own
    Delay,      // wait a fixed delay (behind a transaction pooler: no stable backend)
    Off,        // no settling: the immediate eviction only
};

// Settling as configured on a connection; the pool fills it from PgPoolConfig.
struct SettleConfig {
    UncertainSettle mode = UncertainSettle::Off;
    std::chrono::milliseconds delay{0};  // Delay only; 0 = the server's statement_timeout
};

// Added to the server's statement_timeout when it bounds a write: the statement
// may start a little after it was sent, and its commit follows its end.
inline constexpr std::chrono::milliseconds kSettleMargin{1000};

// A server backend, named by pid AND start time: a pid alone may be reused by a
// later backend once the first one exits.
struct BackendId {
    int32_t pid = 0;            // 0 = unknown
    std::string backend_start;  // timestamptz text, compared server-side

    [[nodiscard]] bool known() const noexcept { return pid != 0; }
};

// What settling learned of an uncertain write. Aborted and Committed are read
// from the server; Unknown whenever it cannot tell which transaction was the
// write's, or what became of it.
enum class WriteOutcome : uint8_t { Unknown, Aborted, Committed };

// Carried by a PgUncertainError raised on the write path: names what must end
// before the write's outcome is final. Every caller that received the same
// failure shares one ticket.
struct SettleTicket {
    enum class State : uint8_t { Pending, Settled };

    UncertainSettle mode = UncertainSettle::Terminate;
    BackendId backend;                  // Terminate / Await
    std::chrono::milliseconds delay{0}; // Delay
    State state = State::Pending;
    WriteOutcome outcome = WriteOutcome::Unknown;  // set before state becomes Settled
    // Writes of the failure whose end was never read, in one pipeline. With
    // more than one, the transaction the backend runs may be any of them, and
    // those ahead of it may have committed.
    uint32_t unresolved = 1;
    // Started once settled, in registration order: the precautionary
    // evictions, again, and the work that waited for the outcome (reading the
    // row as committed). Each receives the outcome. A failing replay does not
    // stop the others.
    std::vector<std::function<Task<void>(WriteOutcome)>> replays;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_SETTLE_TICKET_H
