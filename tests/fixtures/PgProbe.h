#ifndef JCX_RELAIS_TEST_PG_PROBE_H
#define JCX_RELAIS_TEST_PG_PROBE_H

#include <libpq-fe.h>

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

namespace jcailloux::relais::io::test {

// PgProbe — a direct, synchronous libpq connection that bypasses the relais
// pool, BatchScheduler and cache entirely. After a client-side timeout (which
// poisons the relais connection and may have left the cache holding a
// precautionary eviction), the only trustworthy answer to "did the write
// actually commit?" is the database itself, read over an independent channel.
// PgProbe is that channel: it never touches relais state, so what it returns is
// ground truth (scenarios A & B).
//
// Synchronous on purpose — it runs on the test thread, off the loop under test,
// so it can be queried at any point without interleaving with that loop.
//
// Test-only — lives in tests/fixtures, nothing here is in include/.
class PgProbe {
public:
    explicit PgProbe(const char* conninfo) {
        conn_ = PQconnectdb(conninfo);
        if (!conn_ || PQstatus(conn_) != CONNECTION_OK) {
            std::string msg = conn_ ? PQerrorMessage(conn_) : "PQconnectdb returned null";
            if (conn_) PQfinish(conn_);
            conn_ = nullptr;
            throw std::runtime_error("PgProbe connect failed: " + msg);
        }
    }

    ~PgProbe() { if (conn_) PQfinish(conn_); }

    PgProbe(const PgProbe&) = delete;
    PgProbe& operator=(const PgProbe&) = delete;
    PgProbe(PgProbe&& o) noexcept : conn_(o.conn_) { o.conn_ = nullptr; }
    PgProbe& operator=(PgProbe&& o) noexcept {
        if (this != &o) { if (conn_) PQfinish(conn_); conn_ = o.conn_; o.conn_ = nullptr; }
        return *this;
    }

    // Owned result wrapper — thin, RAII over PGresult.
    class Result {
    public:
        explicit Result(PGresult* r) noexcept : res_(r) {}
        ~Result() { if (res_) PQclear(res_); }
        Result(Result&& o) noexcept : res_(o.res_) { o.res_ = nullptr; }
        Result& operator=(Result&& o) noexcept {
            if (this != &o) { if (res_) PQclear(res_); res_ = o.res_; o.res_ = nullptr; }
            return *this;
        }
        Result(const Result&) = delete;
        Result& operator=(const Result&) = delete;

        [[nodiscard]] int rows() const { return PQntuples(res_); }
        [[nodiscard]] int cols() const { return PQnfields(res_); }
        [[nodiscard]] bool empty() const { return rows() == 0; }
        [[nodiscard]] bool isNull(int row, int col = 0) const {
            return PQgetisnull(res_, row, col) != 0;
        }
        [[nodiscard]] std::string get(int row, int col = 0) const {
            return std::string(PQgetvalue(res_, row, col),
                               static_cast<std::size_t>(PQgetlength(res_, row, col)));
        }
        [[nodiscard]] std::optional<std::string> getOpt(int row, int col = 0) const {
            if (isNull(row, col)) return std::nullopt;
            return get(row, col);
        }

    private:
        PGresult* res_;
    };

    // Run a query; throws std::runtime_error on a DB-side error.
    Result query(const std::string& sql) {
        PGresult* r = PQexec(conn_, sql.c_str());
        check(r, sql);
        return Result(r);
    }

    // True iff at least one row of `table` matches the boolean `where` clause —
    // ground-truth presence check for "did this id survive an erase / appear
    // after an insert". `where` is trusted (test-authored), not sanitized.
    bool exists(const std::string& table, const std::string& where) {
        auto r = query("SELECT 1 FROM " + table + " WHERE " + where + " LIMIT 1");
        return !r.empty();
    }

    // Inject a clean server-side disconnect by terminating every backend whose
    // pg_stat_activity row matches `filter` (a boolean SQL expression, e.g.
    // "application_name = 'relais_under_test'"). This is the no-root, no-proxy
    // way to deliver the read-path "DB down propre" RST. Returns the
    // number of backends signalled. The probe's own backend is excluded by
    // default unless the filter selects it.
    int terminateBackends(const std::string& filter) {
        auto r = query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE pid <> pg_backend_pid() AND (" + filter + ")");
        return r.rows();
    }

    [[nodiscard]] PGconn* raw() noexcept { return conn_; }

private:
    void check(PGresult* r, const std::string& sql) {
        ExecStatusType st = r ? PQresultStatus(r) : PGRES_FATAL_ERROR;
        if (st != PGRES_TUPLES_OK && st != PGRES_COMMAND_OK) {
            std::string msg = r ? PQresultErrorMessage(r) : PQerrorMessage(conn_);
            if (r) PQclear(r);
            throw std::runtime_error("PgProbe query failed: " + sql + " :: " + msg);
        }
    }

    PGconn* conn_ = nullptr;
};

}  // namespace jcailloux::relais::io::test

#endif  // JCX_RELAIS_TEST_PG_PROBE_H
