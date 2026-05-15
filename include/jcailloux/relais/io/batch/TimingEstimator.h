#ifndef JCX_RELAIS_IO_BATCH_TIMING_ESTIMATOR_H
#define JCX_RELAIS_IO_BATCH_TIMING_ESTIMATOR_H

#include <chrono>
#include <cstdint>
#include <unordered_map>

namespace jcailloux::relais::io::batch {

// TimingEstimator — adaptive estimation of network and per-query costs
// for batch scheduling decisions.
//
// Maintains:
// - pg_network_time_ns / redis_network_time_ns: EMA of network round-trip
// - Per-SQL timing: request_time_ns per query type (identified by SQL pointer)
// - Bootstrap counter: first 10 queries are sent immediately
// - Staleness detection: > 5 min without single-query measurement → recalibrate

struct TimingEstimator {
    using Clock = std::chrono::steady_clock;

    // Network round-trip time (EMA alpha = 0.01)
    double pg_network_time_ns = 0;
    double redis_network_time_ns = 0;

    // Bootstrap: first N queries are sent immediately to calibrate
    static constexpr int kBootstrapThreshold = 5;
    int pg_bootstrap_count = 0;
    int redis_bootstrap_count = 0;

    // Staleness: if last single-entity batch was > 5 min ago, send immediately
    static constexpr auto kStalenessThreshold = std::chrono::minutes(5);
    Clock::time_point pg_last_single_batch{};
    Clock::time_point redis_last_single_batch{};

    // Per-SQL query timing (keyed by SQL pointer — unique per statement per repo)
    struct SqlTiming {
        double request_time_ns = 0;  // EMA of per-query processing time
        int sample_count = 0;        // 0 → first measurement: direct assignment
    };
    std::unordered_map<const char*, SqlTiming> sql_timings;

    [[nodiscard]] bool isPgBootstrapping() const noexcept {
        return pg_bootstrap_count < kBootstrapThreshold;
    }

    [[nodiscard]] bool isRedisBootstrapping() const noexcept {
        return redis_bootstrap_count < kBootstrapThreshold;
    }

    [[nodiscard]] bool isPgStale() const noexcept {
        if (pg_last_single_batch == Clock::time_point{}) return true;
        return (Clock::now() - pg_last_single_batch) > kStalenessThreshold;
    }

    [[nodiscard]] bool isRedisStale() const noexcept {
        if (redis_last_single_batch == Clock::time_point{}) return true;
        return (Clock::now() - redis_last_single_batch) > kStalenessThreshold;
    }

    /// Get estimated per-query cost for a SQL statement (ns).
    [[nodiscard]] double getRequestTime(const char* sql) const noexcept {
        auto it = sql_timings.find(sql);
        if (it == sql_timings.end()) return 0;
        return it->second.request_time_ns;
    }

    /// Check if two batches can merge (request_time within 5x factor).
    [[nodiscard]] bool canMergePg(double cost_a_ns, double cost_b_ns) const noexcept {
        if (cost_a_ns <= 0 || cost_b_ns <= 0) return true;
        double ratio = cost_a_ns > cost_b_ns
            ? cost_a_ns / cost_b_ns
            : cost_b_ns / cost_a_ns;
        return ratio <= 5.0;
    }

    // =========================================================================
    // Update methods — called when batch results return
    // =========================================================================

    /// Update PG network time from a single-query batch.
    /// measured_ns = total wall-clock time for the batch.
    /// repo_request_time_ns = estimated processing time for the single query.
    void updatePgNetworkTime(double measured_ns, double repo_request_time_ns) {
        double net = measured_ns - repo_request_time_ns;
        if (net < 0) net = measured_ns * 0.5; // Fallback if estimate is off

        if (pg_bootstrap_count == 0) {
            pg_network_time_ns = net;
        } else {
            pg_network_time_ns += 0.01 * (net - pg_network_time_ns);
        }

        ++pg_bootstrap_count;
        pg_last_single_batch = Clock::now();
    }

    /// Update Redis network time from a single-command batch.
    void updateRedisNetworkTime(double measured_ns) {
        if (redis_bootstrap_count == 0) {
            redis_network_time_ns = measured_ns;
        } else {
            redis_network_time_ns += 0.01 * (measured_ns - redis_network_time_ns);
        }

        ++redis_bootstrap_count;
        redis_last_single_batch = Clock::now();
    }

    /// Update per-SQL request time from a batch result.
    /// sql: statement pointer (unique per repo query type).
    /// batch_size: number of queries from this repo in the batch.
    /// total_batch_size: total queries in the entire batch.
    /// measured_ns: wall-clock time for the segment (inter-result interval).
    void updateSqlTiming(const char* sql, int batch_size, int total_batch_size,
                         double measured_ns)
    {
        auto& timing = sql_timings[sql];
        double per_query = (measured_ns - pg_network_time_ns) /
                           static_cast<double>(batch_size);
        if (per_query < 0) per_query = measured_ns / static_cast<double>(batch_size);

        if (timing.sample_count == 0) {
            timing.request_time_ns = per_query;
        } else {
            double alpha_base = 0.1;
            double alpha = alpha_base *
                static_cast<double>(batch_size) / static_cast<double>(total_batch_size);
            timing.request_time_ns += alpha * (per_query - timing.request_time_ns);
        }
        ++timing.sample_count;
    }

    /// Update per-SQL request time from an ANY batch result
    /// (cost per key for SELECT ... WHERE id = ANY).
    void updateSqlTimingPerKey(const char* sql, int n_keys, double segment_time_ns) {
        auto& timing = sql_timings[sql];
        double per_key = (segment_time_ns - pg_network_time_ns) /
                         static_cast<double>(n_keys);
        if (per_key < 0) per_key = segment_time_ns / static_cast<double>(n_keys);

        if (timing.sample_count == 0) {
            timing.request_time_ns = per_key;
        } else {
            timing.request_time_ns += 0.1 * (per_key - timing.request_time_ns);
        }
        ++timing.sample_count;
    }
};

} // namespace jcailloux::relais::io::batch

#endif // JCX_RELAIS_IO_BATCH_TIMING_ESTIMATOR_H
