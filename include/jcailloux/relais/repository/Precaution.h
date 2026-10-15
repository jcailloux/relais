#ifndef JCX_RELAIS_REPOSITORY_PRECAUTION_H
#define JCX_RELAIS_REPOSITORY_PRECAUTION_H

#include <memory>
#include <utility>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/SettleTicket.h"

namespace jcailloux::relais::detail {

// A precautionary eviction follows an uncertain write: the write may still
// commit after it, and a read in between may refill the cache with the old
// value. Each cache tier runs its eviction at once AND registers it on the
// write's settle ticket, which runs it again once the write can no longer
// commit. On a ticket already settled (or without one), the eviction runs at
// once only: nothing can commit after it.
//
// Registered before it runs: the ticket may settle while the eviction is
// suspended, and the replay must not be lost. The eviction owns what it uses
// (key, copies): its replay runs after the caller's frame is gone.

template<typename Fn>
io::Task<void> runOwned(Fn evict) {
    co_await evict();
}

template<typename Fn>
void registerReplay(const std::shared_ptr<io::SettleTicket>& ticket, const Fn& replay) {
    if (ticket && ticket->state == io::SettleTicket::State::Pending)
        ticket->replays.emplace_back([replay] { return runOwned(replay); });
}

// Asynchronous eviction: `evict` returns io::Task<void>. Await it after the
// handler (co_await is illegal in a catch).
template<typename Fn>
io::Task<void> precaution(std::shared_ptr<io::SettleTicket> ticket, Fn evict) {
    registerReplay(ticket, evict);
    co_await evict();
}

// Synchronous eviction (cannot suspend): usable inside the handler.
template<typename Fn>
void precautionSync(const std::shared_ptr<io::SettleTicket>& ticket, Fn evict) {
    registerReplay(ticket, [evict]() -> io::Task<void> { evict(); co_return; });
    evict();
}

} // namespace jcailloux::relais::detail

#endif // JCX_RELAIS_REPOSITORY_PRECAUTION_H
