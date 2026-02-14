#ifndef JCX_RELAIS_LOG_H
#define JCX_RELAIS_LOG_H

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace jcailloux::relais::log {

// =============================================================================
// Log — Configurable logging abstraction for relais
//
// Replaces trantor::Logger (LOG_ERROR, LOG_WARN, LOG_DEBUG) with macros
// that the application can configure via a callback. No dependency on any
// framework — the application provides the routing at startup.
//
// Usage:
//   RELAIS_LOG_ERROR << "MyRepo: DB error - " << e.what();
//   RELAIS_LOG_WARN  << "RedisCache GET error: " << e.what();
//   RELAIS_LOG_DEBUG << "MyRepo: warming up L1 cache...";
//
// Configuration (in application startup):
//   jcailloux::relais::log::setCallback([](Level level, const char* msg, size_t len) {
//       LOG_ERROR << std::string_view(msg, len);  // route to trantor
//   });
// =============================================================================

enum class Level : uint8_t { Debug, Warn, Error };

/// Log callback type. The application provides this to route logs.
using Callback = void(*)(Level level, const char* msg, size_t len);

namespace detail {
    inline Callback& ref() noexcept {
        static Callback cb = nullptr;
        return cb;
    }
}  // namespace detail

/// Set the log callback. Pass nullptr to disable logging.
inline void setCallback(Callback cb) noexcept { detail::ref() = cb; }

/// Get the current log callback.
inline Callback getCallback() noexcept { return detail::ref(); }

// =============================================================================
// LogStream — accumulates a log message and dispatches on destruction
// =============================================================================

class LogStream {
public:
    explicit LogStream(Level level) noexcept : level_(level) {}

    ~LogStream() {
        if (auto cb = getCallback()) {
            cb(level_, buf_.data(), buf_.size());
        }
    }

    LogStream(const LogStream&) = delete;
    LogStream& operator=(const LogStream&) = delete;

    LogStream& operator<<(const char* s) {
        if (s) buf_ += s;
        return *this;
    }

    LogStream& operator<<(std::string_view s) {
        buf_.append(s.data(), s.size());
        return *this;
    }

    LogStream& operator<<(const std::string& s) {
        buf_ += s;
        return *this;
    }

    LogStream& operator<<(char c) {
        buf_ += c;
        return *this;
    }

    template<typename T> requires std::integral<T> && (!std::same_as<T, char>)
    LogStream& operator<<(T val) {
        buf_ += std::to_string(val);
        return *this;
    }

    LogStream& operator<<(double val) {
        buf_ += std::to_string(val);
        return *this;
    }

private:
    Level level_;
    std::string buf_;
};

}  // namespace jcailloux::relais::log

// =============================================================================
// Macros — drop-in replacements for trantor LOG_* macros
// =============================================================================

#define RELAIS_LOG_ERROR ::jcailloux::relais::log::LogStream(::jcailloux::relais::log::Level::Error)
#define RELAIS_LOG_WARN  ::jcailloux::relais::log::LogStream(::jcailloux::relais::log::Level::Warn)
#define RELAIS_LOG_DEBUG ::jcailloux::relais::log::LogStream(::jcailloux::relais::log::Level::Debug)

#endif  // JCX_RELAIS_LOG_H
