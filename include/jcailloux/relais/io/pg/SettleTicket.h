#ifndef JCX_RELAIS_IO_PG_SETTLE_TICKET_H
#define JCX_RELAIS_IO_PG_SETTLE_TICKET_H

#include <chrono>
#include <cstdint>
#include <string>

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
    std::chrono::milliseconds delay{0};  // Delay only; 0 = no bound, nothing to settle
};

// A server backend, named by pid AND start time: a pid alone may be reused by a
// later backend once the first one exits.
struct BackendId {
    int32_t pid = 0;            // 0 = unknown
    std::string backend_start;  // timestamptz text, compared server-side

    [[nodiscard]] bool known() const noexcept { return pid != 0; }
};

// Carried by a PgUncertainError raised on the write path: names what must end
// before the write's outcome is final. Every caller that received the same
// failure shares one ticket.
struct SettleTicket {
    enum class State : uint8_t { Pending, Settled };

    UncertainSettle mode = UncertainSettle::Terminate;
    BackendId backend;                  // Terminate / Await
    std::chrono::milliseconds delay{0}; // Delay
    State state = State::Pending;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_SETTLE_TICKET_H
