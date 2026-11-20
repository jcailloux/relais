#ifndef JCX_RELAIS_WEIGHED_FUNCTION_H
#define JCX_RELAIS_WEIGHED_FUNCTION_H

#include <cstddef>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include "jcailloux/relais/BlockBytes.h"
#include "jcailloux/relais/HeapBytes.h"

namespace jcailloux::relais::detail {

// =============================================================================
// WeighedFunction<R(Args...)> — a copyable callable that knows what it holds
// =============================================================================
//
// What std::function does, for a callable the L1 keeps: the callable lives in
// one block of its own, made by `new` (read on it), and heapBytes weighs what
// it owns, through its type, known when it is stored. A lambda has no rule in
// heapBytes and does not compile here: a callable the L1 keeps is a named
// struct whose members say what it holds. A copy copies the callable into a
// block of its own. Empty: no callable, nothing held.

template<typename Signature>
class WeighedFunction;

template<typename R, typename... Args>
class WeighedFunction<R(Args...)> {
    struct Callable {
        virtual ~Callable() = default;
        virtual R call(Args... args) const = 0;
        [[nodiscard]] virtual Callable* clone() const = 0;
        /// Its own block and what the callable owns.
        [[nodiscard]] virtual size_t heldBytes() const = 0;
    };

    template<typename F>
    struct Model final : Callable {
        F f;

        explicit Model(F fn) : f(std::move(fn)) {}

        R call(Args... args) const override { return std::invoke(f, std::forward<Args>(args)...); }
        [[nodiscard]] Callable* clone() const override { return new Model(*this); }
        // `new Model` made the block: `this` is its start.
        [[nodiscard]] size_t heldBytes() const override {
            return blockBytes(sizeof(Model), this) + heapBytes(f);
        }
    };

    std::unique_ptr<Callable> callable_;

public:
    WeighedFunction() noexcept = default;

    template<typename F>
        requires (!std::is_same_v<std::remove_cvref_t<F>, WeighedFunction>)
              && std::is_invocable_r_v<R, const std::decay_t<F>&, Args...>
    WeighedFunction(F&& f)  // NOLINT(google-explicit-constructor): a callable converts
        : callable_(new Model<std::decay_t<F>>(std::forward<F>(f))) {}

    WeighedFunction(const WeighedFunction& other)
        : callable_(other.callable_ ? other.callable_->clone() : nullptr) {}
    WeighedFunction& operator=(const WeighedFunction& other) {
        if (this != &other) callable_.reset(other.callable_ ? other.callable_->clone() : nullptr);
        return *this;
    }
    WeighedFunction(WeighedFunction&&) noexcept = default;
    WeighedFunction& operator=(WeighedFunction&&) noexcept = default;

    R operator()(Args... args) const { return callable_->call(std::forward<Args>(args)...); }

    explicit operator bool() const noexcept { return callable_ != nullptr; }

    /// sizeof, plus the callable's block and what it owns (see heapBytes).
    [[nodiscard]] size_t memoryUsage() const {
        return sizeof(*this) + (callable_ ? callable_->heldBytes() : 0);
    }
};

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_WEIGHED_FUNCTION_H
