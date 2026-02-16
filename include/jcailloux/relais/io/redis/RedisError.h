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

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_ERROR_H
