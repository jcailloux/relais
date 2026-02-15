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

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_ERROR_H
