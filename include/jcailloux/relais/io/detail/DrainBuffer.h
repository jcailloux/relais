#ifndef JCX_RELAIS_IO_DETAIL_DRAIN_BUFFER_H
#define JCX_RELAIS_IO_DETAIL_DRAIN_BUFFER_H

#include <cstddef>
#include <vector>

namespace jcailloux::relais::io::detail {

// Where a loop runs the callbacks it drains from a queue: taken under the
// queue's lock, run outside it, so that they may queue more. The buffer keeps
// its capacity from one drain to the next, so that a steady loop allocates
// nothing to drain; a burst that grew it past kKept is freed once run, rather
// than held for the life of the loop. Loop thread only.
//
// A callback may run the loop itself, and so drain again while the buffer is
// in use: that drain gets a buffer of its own, and the rest of the outer one
// runs after it.
template<typename T>
class DrainBuffer {
public:
    static constexpr std::size_t kKept = 1024;

    // The buffer of one drain. It ends empty, whether the drain runs to its end
    // or a callback throws: what that callback left unrun is destroyed.
    class Use {
    public:
        explicit Use(DrainBuffer& buffer) noexcept
            : shared_(buffer.in_use_ ? nullptr : &buffer) {
            if (shared_ != nullptr) shared_->in_use_ = true;
        }

        ~Use() {
            if (shared_ == nullptr) return;
            auto& items = shared_->items_;
            if (items.capacity() > kKept) {
                std::vector<T>().swap(items);
            } else {
                items.clear();
            }
            shared_->in_use_ = false;
        }

        Use(const Use&) = delete;
        Use& operator=(const Use&) = delete;

        [[nodiscard]] std::vector<T>& items() noexcept {
            return shared_ != nullptr ? shared_->items_ : own_;
        }

    private:
        DrainBuffer* shared_;  // null while a drain further up uses it
        std::vector<T> own_;
    };

private:
    std::vector<T> items_;
    bool in_use_ = false;
};

}  // namespace jcailloux::relais::io::detail

#endif  // JCX_RELAIS_IO_DETAIL_DRAIN_BUFFER_H
