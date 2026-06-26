#ifndef JCX_RELAIS_IO_PG_ERROR_H
#define JCX_RELAIS_IO_PG_ERROR_H

#include <stdexcept>
#include <string>

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
class PgPoolTimeout : public PgError {
public:
    PgPoolTimeout() : PgError("connection acquire timed out") {}
    explicit PgPoolTimeout(const std::string& detail)
        : PgError("connection acquire timed out: " + detail) {}
};

// Client-side query timeout: an I/O wait exceeded PgPoolConfig::query_timeout.
// Derives from PgError so existing catch(PgError&) keeps catching it; the write
// path layers a catch(PgQueryTimeout&) *before* that base catch to treat it as
// `uncertain` (DB may have committed) rather than a deterministic failure.
class PgQueryTimeout : public PgError {
public:
    PgQueryTimeout() : PgError("query timed out") {}
    explicit PgQueryTimeout(const std::string& detail)
        : PgError("query timed out: " + detail) {}
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_ERROR_H
