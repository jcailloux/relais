#ifndef JCX_RELAIS_IO_PG_ERROR_H
#define JCX_RELAIS_IO_PG_ERROR_H

#include <stdexcept>
#include <string>
#include <type_traits>

namespace jcailloux::relais::io {

class PgError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class PgNoRows : public PgError {
public:
    PgNoRows() : PgError("query returned no rows") {}
    explicit PgNoRows(const std::string& sql)
        : PgError("query returned no rows: " + sql) {}
};

class PgConnectionError : public PgError {
public:
    using PgError::PgError;
};

// Client-side acquire timeout: acquire() exceeded PgPoolConfig::acquire_timeout
// (queue wait or connect handshake). Distinct from a server-refused connection.
// Deterministic: the DB was never touched, so a retry is safe.
class PgPoolTimeout : public PgError {
public:
    PgPoolTimeout() : PgError("connection acquire timed out") {}
    explicit PgPoolTimeout(const std::string& detail)
        : PgError("connection acquire timed out: " + detail) {}
};

// Uncertain outcome: the request was in flight when the wait failed, so the
// server may have committed it. The write path catches this before catch(PgError&)
// and evicts by precaution; a plain PgError means the DB is unchanged (nullopt).
class PgUncertainError : public PgError {
public:
    using PgError::PgError;
};

// Client-side query timeout: an I/O wait exceeded PgPoolConfig::query_timeout.
class PgQueryTimeout : public PgUncertainError {
public:
    PgQueryTimeout() : PgUncertainError("query timed out") {}
    explicit PgQueryTimeout(const std::string& detail)
        : PgUncertainError("query timed out: " + detail) {}
};

// The connection dropped (RST/EOF) after the request was flushed or while its
// result was read — the server may have committed first. A drop before the
// request was sent stays a deterministic PgError/PgConnectionError.
class PgConnectionLost : public PgUncertainError {
public:
    PgConnectionLost() : PgUncertainError("connection lost mid-request") {}
    explicit PgConnectionLost(const std::string& detail)
        : PgUncertainError("connection lost mid-request: " + detail) {}
};

// Lock the category: the write path evicts on any PgUncertainError, so a timeout
// and a post-send drop must live under it and deterministic failures must not.
static_assert(std::is_base_of_v<PgError, PgUncertainError>);
static_assert(std::is_base_of_v<PgUncertainError, PgQueryTimeout>);
static_assert(std::is_base_of_v<PgUncertainError, PgConnectionLost>);
static_assert(!std::is_base_of_v<PgUncertainError, PgPoolTimeout>);
static_assert(!std::is_base_of_v<PgUncertainError, PgConnectionError>);

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_ERROR_H
