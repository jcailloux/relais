#ifndef JCX_RELAIS_IO_REDIS_ERROR_H
#define JCX_RELAIS_IO_REDIS_ERROR_H

#include <stdexcept>
#include <string>

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

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_ERROR_H
