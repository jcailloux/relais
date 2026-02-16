/**
 * test_helper.h
 * Test utilities for relais integration tests.
 *
 * Uses EpollIoContext as the event loop driver.
 * A background thread runs the event loop; sync() dispatches coroutines
 * to it via eventfd + promise/future, allowing safe concurrent usage.
 */

#pragma once

#include <jcailloux/relais/io/Task.h>
#include <jcailloux/relais/io/pg/PgPool.h>
#include <jcailloux/relais/io/pg/PgResult.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/io/redis/RedisClient.h>
#include <jcailloux/relais/DbProvider.h>

#include <fixtures/EpollIoContext.h>
#include <fixtures/TestRunner.h>

#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>
#include <atomic>
#include <chrono>
#include <coroutine>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <sys/eventfd.h>
#include <unistd.h>

namespace relais_test {

namespace io = jcailloux::relais::io;
using IoCtx = io::test::EpollIoContext;

// =============================================================================
// DetachedHandle — fire-and-forget eager coroutine
//
// Starts immediately (initial_suspend = never) and self-destructs on completion
// (final_suspend = never). Used to spawn coroutines on the event loop thread.
// =============================================================================

struct DetachedHandle {
    struct promise_type {
        DetachedHandle get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
};

// =============================================================================
// TestEventLoop — thread-safe wrapper around EpollIoContext
//
// Provides cross-thread dispatch to a background event loop thread via eventfd.
// Allows sync() to be safely called from multiple threads simultaneously.
// =============================================================================

class TestEventLoop {
public:
    explicit TestEventLoop(IoCtx& io) : io_(io) {
        wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wakeup_fd_ < 0)
            throw std::runtime_error("eventfd creation failed");

        wakeup_handle_ = io_.addWatch(
            wakeup_fd_, io::IoEvent::Read,
            [this](io::IoEvent) {
                // Drain the eventfd
                uint64_t val;
                [[maybe_unused]] auto r = ::read(wakeup_fd_, &val, sizeof(val));
                // Process dispatched callbacks
                processQueue();
            }
        );
    }

    ~TestEventLoop() {
        stop();
        io_.removeWatch(wakeup_handle_);
        if (wakeup_fd_ >= 0) ::close(wakeup_fd_);
    }

    TestEventLoop(const TestEventLoop&) = delete;
    TestEventLoop& operator=(const TestEventLoop&) = delete;

    void start() {
        if (running_) return;
        running_ = true;
        thread_ = std::jthread([this](std::stop_token st) {
            while (!st.stop_requested()) {
                io_.runOnce(100);
            }
        });
    }

    void stop() {
        if (!running_) return;
        running_ = false;
        if (thread_.joinable()) {
            thread_.request_stop();
            // Wake up epoll_wait so the thread can see stop_requested
            uint64_t val = 1;
            [[maybe_unused]] auto r = ::write(wakeup_fd_, &val, sizeof(val));
            thread_.join();
        }
    }

    bool running() const noexcept { return running_; }

    template<typename F>
    void dispatch(F&& f) {
        {
            std::lock_guard lock(mutex_);
            queue_.emplace_back(std::forward<F>(f));
        }
        // Wake up the event loop thread
        uint64_t val = 1;
        [[maybe_unused]] auto r = ::write(wakeup_fd_, &val, sizeof(val));
    }

private:
    void processQueue() {
        decltype(queue_) local;
        {
            std::lock_guard lock(mutex_);
            local.swap(queue_);
        }
        for (auto& cb : local) {
            cb();
        }
    }

    IoCtx& io_;
    int wakeup_fd_ = -1;
    IoCtx::WatchHandle wakeup_handle_{};
    std::mutex mutex_;
    std::deque<std::move_only_function<void()>> queue_;
    std::jthread thread_;
    bool running_ = false;
};

// =============================================================================

namespace detail {
    inline std::atomic<bool>& isInitialized() {
        static std::atomic<bool> initialized{false};
        return initialized;
    }

    inline IoCtx& testIo() {
        static IoCtx io;
        return io;
    }

    inline TestEventLoop& testLoop() {
        static TestEventLoop loop(testIo());
        return loop;
    }

    inline std::shared_ptr<io::PgClient<IoCtx>>& testPg() {
        static std::shared_ptr<io::PgClient<IoCtx>> pg;
        return pg;
    }

    inline std::shared_ptr<io::RedisClient<IoCtx>>& testRedis() {
        static std::shared_ptr<io::RedisClient<IoCtx>> redis;
        return redis;
    }

    inline void cleanup() {
        if (isInitialized().exchange(false)) {
            testLoop().stop();
            jcailloux::relais::DbProvider::reset();
            testRedis().reset();
            testPg().reset();
        }
    }
}

// Forward declaration
inline void initTest();

// Catch2 event listener to handle initialization and cleanup
class RelaisTestListener : public Catch::EventListenerBase {
public:
    using Catch::EventListenerBase::EventListenerBase;

    void testRunStarting(Catch::TestRunInfo const&) override {
        initTest();
    }

    void testRunEnded(Catch::TestRunStats const&) override {
        detail::cleanup();
    }
};

#ifndef RELAIS_COMBINED_TESTS
CATCH_REGISTER_LISTENER(RelaisTestListener)
#endif

/**
 * Run a Task<T> synchronously.
 * Dispatches the coroutine to the background event loop thread and blocks
 * the calling thread until completion. Safe to call from multiple threads.
 */
template<typename T>
T sync(io::Task<T> task) {
    std::promise<T> promise;
    auto future = promise.get_future();

    detail::testLoop().dispatch([t = std::move(task), p = std::move(promise)]() mutable {
        [](io::Task<T> t, std::promise<T> p) -> DetachedHandle {
            try {
                p.set_value(co_await std::move(t));
            } catch (...) {
                p.set_exception(std::current_exception());
            }
        }(std::move(t), std::move(p));
    });

    return future.get();
}

inline void sync(io::Task<void> task) {
    std::promise<void> promise;
    auto future = promise.get_future();

    detail::testLoop().dispatch([t = std::move(task), p = std::move(promise)]() mutable {
        [](io::Task<void> t, std::promise<void> p) -> DetachedHandle {
            try {
                co_await std::move(t);
                p.set_value();
            } catch (...) {
                p.set_exception(std::current_exception());
            }
        }(std::move(t), std::move(p));
    });

    future.get();
}

/**
 * Execute a query and return the PgResult.
 */
inline io::PgResult execQuery(const char* sql) {
    return sync(detail::testPg()->query(sql));
}

/**
 * Execute a parameterized query and return the PgResult.
 */
template<typename... Args>
io::PgResult execQueryArgs(const char* sql, Args&&... args) {
    return sync(detail::testPg()->queryArgs(sql, std::forward<Args>(args)...));
}

/**
 * Execute raw SQL synchronously (fire-and-forget).
 */
inline void execSql(const std::string& sql) {
    execQuery(sql.c_str());
}

/**
 * Get the connection string for the test database.
 */
inline const char* getConnInfo() {
    return "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test";
}

/**
 * Initialize I/O for integration tests.
 * Phase 1: synchronous init (PgPool, Redis, DbProvider) via runTask().
 * Phase 2: start background event loop thread for concurrent sync() calls.
 */
inline void initTest() {
    if (detail::isInitialized().exchange(true)) {
        return;  // Already initialized
    }

    auto& io = detail::testIo();

    // Phase 1: Synchronous initialization (no background thread yet)
    auto pool = io::test::runTask(io,
        io::PgPool<IoCtx>::create(io, getConnInfo(), 2, 4));

    detail::testPg() = std::make_shared<io::PgClient<IoCtx>>(pool);

    std::shared_ptr<io::RedisClient<IoCtx>> redis;
    try {
        // Try Unix socket first (lower latency)
        redis = io::test::runTask(io,
            io::RedisClient<IoCtx>::connectUnix(io, "/run/redis/redis.sock"));
    } catch (...) {
        // Fall back to TCP
        redis = io::test::runTask(io,
            io::RedisClient<IoCtx>::connect(io, "127.0.0.1", 6379));
    }
    detail::testRedis() = redis;

    jcailloux::relais::DbProvider::init(pool, redis);

    // Phase 2: Start background event loop thread
    detail::testLoop().start();
}

/**
 * Flush all keys from Redis (for test isolation).
 */
inline void flushRedis() {
    auto& redis = detail::testRedis();
    if (!redis) return;

    try {
        sync(redis->exec("FLUSHDB"));
    } catch (...) {
        // Ignore flush errors
    }
}

/**
 * Wait for a specified duration (for cache expiration tests).
 */
inline void waitForExpiration(std::chrono::milliseconds duration) {
    std::this_thread::sleep_for(duration);
}

/**
 * RAII class for test isolation.
 * Clears all test data and caches before and after each test.
 */
class TransactionGuard {
public:
    TransactionGuard() {
        cleanup();
    }

    ~TransactionGuard() {
        try {
            cleanup();
        } catch (...) {
            // Ignore cleanup errors in destructor
        }
    }

    TransactionGuard(const TransactionGuard&) = delete;
    TransactionGuard& operator=(const TransactionGuard&) = delete;
    TransactionGuard(TransactionGuard&&) = delete;
    TransactionGuard& operator=(TransactionGuard&&) = delete;

private:
    static void cleanup() {
        if (!jcailloux::relais::DbProvider::initialized()) {
            return;
        }
        flushRedis();

        // Delete all test data (order matters due to FK constraints)
        auto& pg = detail::testPg();
        try { sync(pg->query("DELETE FROM relais_test_events")); } catch (...) {}
        try { sync(pg->query("DELETE FROM relais_test_purchases")); } catch (...) {}
        try { sync(pg->query("DELETE FROM relais_test_articles")); } catch (...) {}
        try { sync(pg->query("DELETE FROM relais_test_users")); } catch (...) {}
        try { sync(pg->query("DELETE FROM relais_test_items")); } catch (...) {}
    }
};

// =============================================================================
// Test Item Helpers
// =============================================================================

inline int64_t insertTestItem(
    const std::string& name,
    int32_t value = 0,
    const std::optional<std::string>& description = std::nullopt,
    bool is_active = true
) {
    if (description.has_value()) {
        auto result = execQueryArgs(
            "INSERT INTO relais_test_items (name, value, description, is_active) "
            "VALUES ($1, $2, $3, $4) RETURNING id",
            name, value, *description, is_active
        );
        return result[0].get<int64_t>(0);
    } else {
        auto result = execQueryArgs(
            "INSERT INTO relais_test_items (name, value, is_active) "
            "VALUES ($1, $2, $3) RETURNING id",
            name, value, is_active
        );
        return result[0].get<int64_t>(0);
    }
}

inline void deleteTestItem(int64_t id) {
    execQueryArgs("DELETE FROM relais_test_items WHERE id = $1", id);
}

inline void updateTestItem(int64_t id, const std::string& name, int32_t value) {
    execQueryArgs(
        "UPDATE relais_test_items SET name = $1, value = $2 WHERE id = $3",
        name, value, id
    );
}

// =============================================================================
// Test User Helpers
// =============================================================================

inline int64_t insertTestUser(
    const std::string& username,
    const std::string& email,
    int32_t balance = 0
) {
    auto result = execQueryArgs(
        "INSERT INTO relais_test_users (username, email, balance) "
        "VALUES ($1, $2, $3) RETURNING id",
        username, email, balance
    );
    return result[0].get<int64_t>(0);
}

inline void deleteTestUser(int64_t id) {
    execQueryArgs("DELETE FROM relais_test_users WHERE id = $1", id);
}

inline void updateTestUserBalance(int64_t id, int32_t balance) {
    execQueryArgs(
        "UPDATE relais_test_users SET balance = $1 WHERE id = $2",
        balance, id
    );
}

// =============================================================================
// Test Purchase Helpers
// =============================================================================

inline int64_t insertTestPurchase(
    int64_t user_id,
    const std::string& product_name,
    int32_t amount,
    const std::string& status = "pending"
) {
    auto result = execQueryArgs(
        "INSERT INTO relais_test_purchases (user_id, product_name, amount, status) "
        "VALUES ($1, $2, $3, $4) RETURNING id",
        user_id, product_name, amount, status
    );
    return result[0].get<int64_t>(0);
}

inline void deleteTestPurchase(int64_t id) {
    execQueryArgs("DELETE FROM relais_test_purchases WHERE id = $1", id);
}

inline void updateTestPurchase(int64_t id, int32_t amount, const std::string& status) {
    execQueryArgs(
        "UPDATE relais_test_purchases SET amount = $1, status = $2 WHERE id = $3",
        amount, status, id
    );
}

inline void updateTestPurchaseUserId(int64_t id, int64_t new_user_id) {
    execQueryArgs(
        "UPDATE relais_test_purchases SET user_id = $1 WHERE id = $2",
        new_user_id, id
    );
}

// =============================================================================
// Test Article Helpers
// =============================================================================

inline int64_t insertTestArticle(
    const std::string& category,
    int64_t author_id,
    const std::string& title,
    int32_t view_count = 0,
    bool is_published = false
) {
    if (is_published) {
        auto result = execQueryArgs(
            "INSERT INTO relais_test_articles (category, author_id, title, view_count, is_published, published_at) "
            "VALUES ($1, $2, $3, $4, $5, NOW()) RETURNING id",
            category, author_id, title, view_count, is_published
        );
        return result[0].get<int64_t>(0);
    } else {
        auto result = execQueryArgs(
            "INSERT INTO relais_test_articles (category, author_id, title, view_count, is_published) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING id",
            category, author_id, title, view_count, is_published
        );
        return result[0].get<int64_t>(0);
    }
}

inline void deleteTestArticle(int64_t id) {
    execQueryArgs("DELETE FROM relais_test_articles WHERE id = $1", id);
}

inline void updateTestArticle(int64_t id, const std::string& title, int32_t view_count) {
    execQueryArgs(
        "UPDATE relais_test_articles SET title = $1, view_count = $2 WHERE id = $3",
        title, view_count, id
    );
}

inline void publishTestArticle(int64_t id) {
    execQueryArgs(
        "UPDATE relais_test_articles SET is_published = true, published_at = NOW() WHERE id = $1",
        id
    );
}

inline void updateTestArticleCategory(int64_t id, const std::string& category) {
    execQueryArgs(
        "UPDATE relais_test_articles SET category = $1 WHERE id = $2",
        category, id
    );
}

// =============================================================================
// Test Event Helpers
// =============================================================================

inline int64_t insertTestEvent(
    const std::string& region,
    int64_t user_id,
    const std::string& title,
    int32_t priority = 0
) {
    auto result = execQueryArgs(
        "INSERT INTO relais_test_events (region, user_id, title, priority) "
        "VALUES ($1, $2, $3, $4) RETURNING id",
        region, user_id, title, priority
    );
    return result[0].get<int64_t>(0);
}

inline void deleteTestEvent(int64_t id) {
    execQueryArgs("DELETE FROM relais_test_events WHERE id = $1", id);
}

inline void updateTestEvent(int64_t id, const std::string& title, int32_t priority) {
    execQueryArgs(
        "UPDATE relais_test_events SET title = $1, priority = $2 WHERE id = $3",
        title, priority, id
    );
}

// =============================================================================
// Cache Testing Utilities
// =============================================================================

template<typename Repo>
size_t getCacheSize() {
    return Repo::size();
}

template<typename Repo>
void forcePurge() {
    Repo::purge();
}

template<typename Repo>
void trySweep() {
    Repo::trySweep();
}

} // namespace relais_test
