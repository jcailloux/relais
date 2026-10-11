#ifndef JCX_RELAIS_IO_REDIS_ERROR_H
#define JCX_RELAIS_IO_REDIS_ERROR_H

#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>

namespace jcailloux::relais::io {

class RedisError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class RedisConnectionError : public RedisError {
public:
    using RedisError::RedisError;
};

// Client-side Redis I/O timeout: an operation exceeded RedisPoolConfig::query_timeout.
// Derives from RedisError so catch(RedisError&) covers both a silent hang and a clean
// Redis-down (RST). Cache-tier semantics, not DB: a read (GET) timeout degrades to an
// L2 miss -> fetch L3 (never a false "absent"); a write/evict is best-effort + self-heal.
class RedisQueryTimeout : public RedisError {
public:
    RedisQueryTimeout() : RedisError("redis operation timed out") {}
    explicit RedisQueryTimeout(const std::string& detail)
        : RedisError("redis operation timed out: " + detail) {}
};

// The server has no script cached under the SHA1 given to EVALSHA (reply prefix
// NOSCRIPT): load it with SCRIPT LOAD, or send the body with EVAL.
class RedisNoScriptError : public RedisError {
public:
    using RedisError::RedisError;
};

// The exception for an error reply from the server, whichever path carried the
// command (direct send or pipelined batch): RedisNoScriptError for NOSCRIPT,
// RedisError otherwise. The server's message is kept as what().
[[nodiscard]] inline std::exception_ptr redisReplyError(std::string message) {
    if (std::string_view(message).starts_with("NOSCRIPT"))
        return std::make_exception_ptr(RedisNoScriptError(std::move(message)));
    return std::make_exception_ptr(RedisError(std::move(message)));
}

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_ERROR_H
