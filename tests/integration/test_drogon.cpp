// relais on Drogon, through attachDrogon(): every IO loop is bound before
// Drogon listens, handlers reach PostgreSQL through relais on the loop they run
// on, and quit() takes relais off each loop while it still runs, after the
// requests in flight there. A loop relais cannot be bound to makes Drogon quit
// without serving, and run() throw.
//
// drogon::app() runs once per process: ctest runs each case in a process of
// its own, and a second case run in the same process is skipped.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <drogon/drogon.h>

#include <jcailloux/relais/Log.h>
#include <jcailloux/relais/PgProvider.h>
#include <jcailloux/relais/integration/drogon.h>
#include <jcailloux/relais/io/pg/PgError.h>
#include <jcailloux/relais/io/pg/PgParams.h>
#include <jcailloux/relais/repository/Repo.h>

#include <fixtures/LivenessHarness.h>
#include <fixtures/PgProbe.h>
#include <fixtures/TestConnInfo.h>
#include <fixtures/generated/TestItemEntity.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <fstream>
#include <future>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using jcailloux::relais::AttachError;
using jcailloux::relais::PgProvider;
using jcailloux::relais::io::PgParams;
using jcailloux::relais::io::test::PgProbe;
using jcailloux::relais::io::test::SilentTcpServer;

// trantor's EventLoop::quit() sets the loop's quit flag, then reads the loop
// again to wake it, while the loop's thread, seeing the flag, may already have
// destroyed it: Drogon's quit() calls it on every IO loop. A race inside
// trantor, with no relais frame on either side.
#if defined(__SANITIZE_THREAD__)
extern "C" const char* __tsan_default_suppressions() {
    return "race:trantor::EventLoop::quit\n";
}
#endif

namespace {

using Items = jcailloux::relais::Repo<entity::generated::TestItemEntity, "drogon:items">;

const char* conninfo() { return relais_test::kConnInfo; }

// drogon::app() runs once per process.
bool claimApp() {
    static std::atomic<bool> claimed{false};
    return !claimed.exchange(true);
}

// What relais warned about.
std::mutex warnings_mutex;
std::vector<std::string> warnings;

void recordWarning(jcailloux::relais::log::Level level, const char* msg, size_t len) {
    if (level == jcailloux::relais::log::Level::Debug) return;
    std::lock_guard lock(warnings_mutex);
    warnings.emplace_back(msg, len);
}

std::vector<std::string> recordedWarnings() {
    std::lock_guard lock(warnings_mutex);
    return warnings;
}

uint16_t freePort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
    socklen_t len = sizeof addr;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    ::close(fd);
    return ntohs(addr.sin_port);
}

// A connected socket to 127.0.0.1:port, or -1.
int connectTo(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0) {
        ::close(fd);
        return -1;
    }
    timeval timeout{.tv_sec = 5, .tv_usec = 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
    return fd;
}

bool listening(uint16_t port) {
    const int fd = connectTo(port);
    if (fd < 0) return false;
    ::close(fd);
    return true;
}

bool sendGet(int fd, const std::string& path) {
    const std::string request =
        "GET " + path + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    return ::send(fd, request.data(), request.size(), MSG_NOSIGNAL) ==
           static_cast<ssize_t>(request.size());
}

// One GET on a connection of its own: the body of a 200 answer, or nothing.
std::optional<std::string> get(uint16_t port, const std::string& path) {
    const int fd = connectTo(port);
    if (fd < 0) return std::nullopt;
    std::string response;
    if (sendGet(fd, path)) {
        char buf[4096];
        ssize_t n;
        while ((n = ::recv(fd, buf, sizeof buf, 0)) > 0) response.append(buf, static_cast<size_t>(n));
    }
    ::close(fd);
    if (!response.starts_with("HTTP/1.1 200")) return std::nullopt;
    const auto body = response.find("\r\n\r\n");
    if (body == std::string::npos) return std::nullopt;
    return response.substr(body + 4);
}

// drogon::app() configured and run on a thread of its own, as main() runs it:
// everything Drogon is touched there first, so that its main loop belongs to
// that thread.
class DrogonThread {
public:
    template<typename Setup>
    explicit DrogonThread(Setup setup) {
        auto ended = ended_.get_future();
        ended_future_ = std::move(ended);
        thread_ = std::thread([this, setup = std::move(setup)]() mutable {
            try {
                setup(drogon::app());
                ended_.set_value(nullptr);
            } catch (...) {
                ended_.set_value(std::current_exception());
            }
        });
    }

    ~DrogonThread() {
        if (thread_.joinable()) thread_.detach();  // still running: the case has failed
    }

    // How run() ended: nothing when it returned, else what it threw; empty if
    // it has not ended within `timeout`.
    std::optional<std::exception_ptr> awaitEnd(std::chrono::seconds timeout) {
        if (ended_future_.wait_for(timeout) != std::future_status::ready) return std::nullopt;
        thread_.join();
        return ended_future_.get();
    }

private:
    std::promise<std::exception_ptr> ended_;
    std::future<std::exception_ptr> ended_future_;
    std::thread thread_;
};

// The names of this process's threads, but relais's own ("relais-…"), which
// start with the first repository and live as long as the process.
std::multiset<std::string> threadNames() {
    std::multiset<std::string> names;
    DIR* d = ::opendir("/proc/self/task");
    if (!d) return names;
    while (auto* entry = ::readdir(d)) {
        if (entry->d_name[0] == '.') continue;
        std::ifstream comm(std::string("/proc/self/task/") + entry->d_name + "/comm");
        std::string name;
        std::getline(comm, name);
        if (!name.starts_with("relais-")) names.insert(name);
    }
    ::closedir(d);
    return names;
}

int backendsNamed(PgProbe& probe, const std::string& application_name) {
    auto r = probe.query("SELECT count(*) FROM pg_stat_activity WHERE application_name = '" +
                         application_name + "'");
    return std::stoi(r.get(0));
}

}  // namespace

TEST_CASE("drogon: handlers reach PostgreSQL through relais on their IO loop and quit "
          "waits for them",
          "[drogon][integration]") {
    if (!claimApp()) SKIP("drogon::app() runs once per process: run this case alone");
    jcailloux::relais::log::setCallback(recordWarning);

    const std::string app_name = "relais_drogon_test";
    const std::string info = std::string(conninfo()) + " application_name=" + app_name;
    PgProbe probe(conninfo());
    probe.query("DELETE FROM relais_test_items WHERE name LIKE 'drogon_item_%'");
    std::vector<int64_t> ids;
    for (int k = 0; k < 4; ++k) {
        auto r = probe.query("INSERT INTO relais_test_items (name, value) VALUES ('drogon_item_" +
                             std::to_string(k) + "', 0) RETURNING id");
        ids.push_back(std::stoll(r.get(0)));
    }

    constexpr std::size_t kIoLoops = 3;
    const uint16_t port = freePort();
    const auto threads_before = threadNames();
    std::promise<std::string> main_loop_error;
    std::atomic<int> sleep_started{0}, sleep_answered{0}, sleep_failed{0};

    DrogonThread app([&](drogon::HttpAppFramework& a) {
        jcailloux::relais::attachDrogon(info, {.pool = {.min_connections = 1, .max_connections = 2}});

        // After relais's own advice: the IO loops are bound, the main loop is not.
        a.registerBeginningAdvice([&] {
            try {
                (void)PgProvider::query("SELECT 1");
                main_loop_error.set_value("");
            } catch (const std::logic_error& e) {
                main_loop_error.set_value(e.what());
            }
        });

        // n * 2, whether the handler resumed on the thread it started on, and
        // the IO loop it ran on.
        a.registerHandler("/double/{1}",
            [](drogon::HttpRequestPtr, int n) -> drogon::Task<drogon::HttpResponsePtr> {
                const auto before = std::this_thread::get_id();
                auto r = co_await PgProvider::queryParams("SELECT $1::int * 2",
                                                          PgParams::make(int32_t{n}));
                const bool same = std::this_thread::get_id() == before;
                auto* loop = trantor::EventLoop::getEventLoopOfCurrentThread();
                auto resp = drogon::HttpResponse::newHttpResponse();
                resp->setBody(std::to_string(r[0].get<int32_t>(0)) + (same ? " same " : " moved ") +
                              std::to_string(loop ? loop->index() : 999));
                co_return resp;
            });

        // Reads an item through the repository's cache, adds one, writes it
        // back, and answers what a fresh read gives.
        a.registerHandler("/bump/{1}",
            [](drogon::HttpRequestPtr, int64_t id) -> drogon::Task<drogon::HttpResponsePtr> {
                auto resp = drogon::HttpResponse::newHttpResponse();
                auto item = co_await Items::find(id);
                if (!item) {
                    resp->setStatusCode(drogon::k404NotFound);
                    co_return resp;
                }
                auto copy = *item;
                ++copy.value;
                auto affected = co_await Items::update(id, copy);
                auto again = co_await Items::find(id);
                if (!affected || *affected != 1 || !again) {
                    resp->setStatusCode(drogon::k500InternalServerError);
                    co_return resp;
                }
                resp->setBody(std::to_string(again->value));
                co_return resp;
            });

        // A request still waiting on PostgreSQL when Drogon quits.
        a.registerHandler("/sleep",
            [&](drogon::HttpRequestPtr) -> drogon::Task<drogon::HttpResponsePtr> {
                ++sleep_started;
                try {
                    auto r = co_await PgProvider::query("SELECT pg_sleep(0.4), 1");
                    ++(r.ok() ? sleep_answered : sleep_failed);
                } catch (...) {
                    ++sleep_failed;
                }
                co_return drogon::HttpResponse::newHttpResponse();
            });

        a.setLogLevel(trantor::Logger::kWarn)
            .disableSigtermHandling()
            .addListener("127.0.0.1", port)
            .setThreadNum(kIoLoops)
            .run();
    });

    // Drogon listens once every IO loop is bound.
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!listening(port) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(10ms);
    REQUIRE(listening(port));
    CHECK_THAT(main_loop_error.get_future().get(),
               Catch::Matchers::ContainsSubstring("not attached"));

    // Many connections at once, spread over the IO loops.
    constexpr int kClients = 6, kPerClient = 4;
    std::vector<std::vector<std::optional<std::string>>> doubles(kClients);
    {
        std::vector<std::jthread> clients;
        for (int c = 0; c < kClients; ++c) {
            clients.emplace_back([&, c] {
                for (int i = 0; i < kPerClient; ++i)
                    doubles[c].push_back(get(port, "/double/" + std::to_string(c * 100 + i)));
            });
        }
    }
    std::set<std::string> loops_seen;
    for (int c = 0; c < kClients; ++c) {
        REQUIRE(doubles[c].size() == kPerClient);
        for (int i = 0; i < kPerClient; ++i) {
            REQUIRE(doubles[c][i].has_value());
            const std::string expected = std::to_string(2 * (c * 100 + i)) + " same ";
            REQUIRE(doubles[c][i]->starts_with(expected));
            const std::string loop = doubles[c][i]->substr(expected.size());
            CHECK(std::stoul(loop) < kIoLoops);
            loops_seen.insert(loop);
        }
    }
    CHECK(loops_seen.size() >= 2);

    // Each client bumps its own item, one request after another, each on a
    // connection of its own: the cache stays right from loop to loop.
    constexpr int kBumps = 5;
    std::vector<std::vector<std::optional<std::string>>> bumps(ids.size());
    {
        std::vector<std::jthread> clients;
        for (std::size_t k = 0; k < ids.size(); ++k) {
            clients.emplace_back([&, k] {
                for (int i = 0; i < kBumps; ++i)
                    bumps[k].push_back(get(port, "/bump/" + std::to_string(ids[k])));
            });
        }
    }
    for (std::size_t k = 0; k < ids.size(); ++k) {
        REQUIRE(bumps[k].size() == kBumps);
        for (int i = 0; i < kBumps; ++i) {
            REQUIRE(bumps[k][i].has_value());
            CHECK(*bumps[k][i] == std::to_string(i + 1));
        }
        auto r = probe.query("SELECT value FROM relais_test_items WHERE id = " +
                             std::to_string(ids[k]));
        CHECK(r.get(0) == std::to_string(kBumps));
    }

    // Drogon quits while a request waits on PostgreSQL: relais lets it end
    // before closing that loop's pool.
    const int sleeper = connectTo(port);
    REQUIRE(sleeper >= 0);
    REQUIRE(sendGet(sleeper, "/sleep"));
    while (sleep_started == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(5ms);
    REQUIRE(sleep_started == 1);
    const auto quit_at = std::chrono::steady_clock::now();
    drogon::app().quit();
    auto ended = app.awaitEnd(15s);
    ::close(sleeper);
    REQUIRE(ended.has_value());
    if (*ended) std::rethrow_exception(*ended);
    CHECK(std::chrono::steady_clock::now() - quit_at < 5s);
    CHECK(sleep_answered == 1);
    CHECK(sleep_failed == 0);
    CHECK(threadNames() == threads_before);  // Drogon's threads have ended

    // relais closed every connection it opened, and warned of nothing.
    int left = backendsNamed(probe, app_name);
    for (int i = 0; i < 100 && left != 0; ++i) {
        std::this_thread::sleep_for(20ms);
        left = backendsNamed(probe, app_name);
    }
    CHECK(left == 0);
    CHECK(recordedWarnings().empty());

    probe.query("DELETE FROM relais_test_items WHERE name LIKE 'drogon_item_%'");
    jcailloux::relais::log::setCallback(nullptr);
}

TEST_CASE("drogon: run() throws what kept relais off a loop once Drogon has quit",
          "[drogon][integration]") {
    if (!claimApp()) SKIP("drogon::app() runs once per process: run this case alone");

    SilentTcpServer silent;  // accepts the connection, never answers
    const uint16_t port = freePort();
    // A sanitizer's runtime starts a thread of its own on the first one the
    // process creates: not Drogon's.
    std::thread([] {}).join();
    const auto threads_before = threadNames();
    std::string second_call;

    const auto t0 = std::chrono::steady_clock::now();
    DrogonThread app([&](drogon::HttpAppFramework& a) {
        jcailloux::relais::attachDrogon(silent.conninfo(),
            {.pool = {.min_connections = 1, .max_connections = 1, .acquire_timeout = 300ms}});
        try {
            jcailloux::relais::attachDrogon(silent.conninfo());
        } catch (const AttachError& e) {
            second_call = e.what();
        }
        a.setLogLevel(trantor::Logger::kFatal)
            .disableSigtermHandling()
            .addListener("127.0.0.1", port)
            .setThreadNum(2)
            .run();
    });

    auto ended = app.awaitEnd(10s);
    REQUIRE(ended.has_value());
    REQUIRE(*ended != nullptr);
    CHECK_THROWS_AS(std::rethrow_exception(*ended), jcailloux::relais::io::PgPoolTimeout);
    CHECK(std::chrono::steady_clock::now() - t0 < 3s);
    CHECK_THAT(second_call, Catch::Matchers::ContainsSubstring("called twice"));
    CHECK_FALSE(listening(port));
    CHECK(threadNames() == threads_before);  // Drogon quit: its threads have ended
}
