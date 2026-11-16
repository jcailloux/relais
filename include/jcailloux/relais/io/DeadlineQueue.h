#ifndef JCX_RELAIS_IO_DEADLINE_QUEUE_H
#define JCX_RELAIS_IO_DEADLINE_QUEUE_H

#include <algorithm>
#include <cassert>
#include <chrono>
#include <memory>

#include "jcailloux/relais/io/IoContext.h"

namespace jcailloux::relais::io::detail {

class DeadlineList;

// Deadline — the bound on one operation, linked into a DeadlineQueue while armed.
//
// It lives inside what it bounds (a connection, a queued acquire): arming and
// disarming allocate nothing and call nothing on the loop. When it expires, the
// queue unlinks it, then calls `expire(owner)`. Loop-thread only.
class Deadline {
public:
    using Clock = std::chrono::steady_clock;
    using Expire = void (*)(void* owner);

    Deadline(void* owner, Expire expire) noexcept : owner_(owner), expire_(expire) {}
    ~Deadline() { disarm(); }

    Deadline(const Deadline&) = delete;
    Deadline& operator=(const Deadline&) = delete;

    [[nodiscard]] bool armed() const noexcept { return list_ != nullptr; }

    /// Unlinks the deadline, in O(1); no-op when it is not armed.
    inline void disarm() noexcept;

private:
    friend class DeadlineList;

    DeadlineList* list_ = nullptr;  // non-null while armed
    Deadline* prev_ = nullptr;
    Deadline* next_ = nullptr;
    Clock::time_point at_{};
    void* owner_;
    Expire expire_;
};

// DeadlineList — armed deadlines, earliest first.
class DeadlineList {
public:
    DeadlineList() = default;
    DeadlineList(const DeadlineList&) = delete;
    DeadlineList& operator=(const DeadlineList&) = delete;

    [[nodiscard]] bool empty() const noexcept { return head_ == nullptr; }

protected:
    // A deadline still armed belongs to an operation abandoned with its loop (a
    // coroutine frame never resumed): it is let go, so that destroying it later
    // touches nothing here.
    ~DeadlineList() {
        while (head_) unlink(*head_);
    }

    // `at` is not earlier than the tail's: the list stays in expiry order.
    void pushBack(Deadline& d, Deadline::Clock::time_point at) noexcept {
        assert(!d.list_ && "a deadline armed twice");
        assert((!tail_ || tail_->at_ <= at) && "deadlines out of order");
        d.list_ = this;
        d.at_ = at;
        d.prev_ = tail_;
        d.next_ = nullptr;
        (tail_ ? tail_->next_ : head_) = &d;
        tail_ = &d;
    }

    [[nodiscard]] Deadline::Clock::time_point headAt() const noexcept {
        assert(head_);
        return head_->at_;
    }

    // Unlinks the head and runs its expiry, if it is due at `now`. False when
    // nothing is due.
    bool expireHead(Deadline::Clock::time_point now) {
        Deadline* d = head_;
        if (!d || d->at_ > now) return false;
        unlink(*d);
        d->expire_(d->owner_);
        return true;
    }

private:
    friend class Deadline;

    void unlink(Deadline& d) noexcept {
        (d.prev_ ? d.prev_->next_ : head_) = d.next_;
        (d.next_ ? d.next_->prev_ : tail_) = d.prev_;
        d.list_ = nullptr;
        d.prev_ = d.next_ = nullptr;
    }

    Deadline* head_ = nullptr;
    Deadline* tail_ = nullptr;
};

inline void Deadline::disarm() noexcept {
    if (list_) list_->unlink(*this);
}

// DeadlineQueue — the deadlines of one duration, expired by a single loop timer.
//
// All of a queue's deadlines have the same duration, so they expire in the
// order they were armed: arming appends, disarming unlinks, both O(1), with no
// call on the loop. One loop timer is armed for the head and never cancelled:
// when it fires, it expires the deadlines that are due and re-arms for the next
// one, if any. A disarmed head leaves that timer pending, to fire for nothing.
// Under continuous load that is at most one timer, and one wakeup, per
// duration; at rest, none after the last.
//
// The timer holds the queue weakly: a queue destroyed before its timer fires
// leaves it nothing to do. Loop-thread only.
template<IoContext Io>
class DeadlineQueue final
    : public DeadlineList
    , public std::enable_shared_from_this<DeadlineQueue<Io>> {
    struct Private { explicit Private() = default; };

public:
    using Clock = Deadline::Clock;

    /// A queue bounding operations by `duration`; null for 0, which bounds nothing.
    static std::shared_ptr<DeadlineQueue> make(Io& io, std::chrono::nanoseconds duration) {
        if (duration.count() <= 0) return nullptr;
        return std::make_shared<DeadlineQueue>(Private{}, io, duration);
    }

    DeadlineQueue(Private, Io& io, std::chrono::nanoseconds duration) noexcept
        : io_(io), duration_(duration) {}

    [[nodiscard]] std::chrono::nanoseconds duration() const noexcept { return duration_; }

    /// Arms `d`, not armed yet, to expire duration() from now unless disarmed
    /// first. Throws only when the loop cannot arm the queue's timer; `d` is
    /// then left unarmed.
    void arm(Deadline& d) {
        pushBack(d, Clock::now() + duration_);
        if (timer_pending_) return;
        try {
            armTimer();
        } catch (...) {
            d.disarm();
            throw;
        }
    }

    /// Diagnostic: the queue's loop timer is pending.
    [[nodiscard]] bool timerPending() const noexcept { return timer_pending_; }

private:
    void armTimer() {
        const auto delay = std::max(headAt() - Clock::now(), Clock::duration::zero());
        io_.postDelayed(std::chrono::nanoseconds(delay), [q = this->weak_from_this()] {
            if (auto self = q.lock()) self->onTimer();
        });
        timer_pending_ = true;
    }

    void onTimer() {
        timer_pending_ = false;
        const auto now = Clock::now();
        while (expireHead(now)) {}
        // An expiry may have armed a deadline, and with it the timer.
        if (!empty() && !timer_pending_) armTimer();
    }

    Io& io_;
    std::chrono::nanoseconds duration_;
    bool timer_pending_ = false;
};

}  // namespace jcailloux::relais::io::detail

#endif // JCX_RELAIS_IO_DEADLINE_QUEUE_H
