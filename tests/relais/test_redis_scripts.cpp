/**
 * test_redis_scripts.cpp
 *
 * The Lua script registry (cache/RedisScripts.h) and RedisCache::evalScript
 * against a live server:
 *   - SCRIPT LOAD of every registered script returns exactly its compile-time
 *     SHA1: the digest matches the server's, and every script compiles;
 *   - every script runs once on minimal data without a Lua error. The list
 *     scripts are composed from shared fragments that read the prologue's
 *     locals; a fragment placed before them would read a global, which Redis
 *     rejects only when the line runs. The data below reaches those lines: one
 *     group with one filter (schema, n_filters) and one header-less page
 *     (hdr_size), which every list script deletes;
 *   - every RedisCache operation that runs a script sends EVALSHA, and after
 *     SCRIPT FLUSH falls back to EVAL exactly once: same result, same effect,
 *     the script cached again;
 *   - the fallback inside a pipelined batch, on arguments the caller has
 *     already destroyed (the call converts them to owned strings);
 *   - no fallback on any other error reply, nor when Redis does not answer.
 *
 * SCRIPT FLUSH empties the script cache of the whole server: another process
 * may then fall back too, but the fallback counter is per process, so the
 * assertions below only see this process's calls.
 */

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "fixtures/test_helper.h"
#include "jcailloux/relais/config/CacheConfig.h"
#include "fixtures/ControllableIoContext.h"
#include "fixtures/RelaisTestAccessors.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/cache/RedisCache.h"
#include "jcailloux/relais/cache/RedisScripts.h"
#include "jcailloux/relais/io/WhenAll.h"
#include "jcailloux/relais/io/batch/BatchScheduler.h"
#include "jcailloux/relais/io/batch/TimingEstimator.h"
#include "jcailloux/relais/io/redis/RedisPool.h"

using namespace relais_test;
using namespace std::chrono_literals;
namespace scripts = jcailloux::relais::cache::scripts;
using jcailloux::relais::PgProvider;
using jcailloux::relais::cache::RedisCache;
namespace list = jcailloux::relais::list;

namespace {

io::Task<io::RedisResult> run(std::vector<std::string> args) {
    co_return co_await PgProvider::redisDynamic(std::move(args));
}

std::string evalScript(const io::RedisScript& s, const std::vector<std::string>& keys,
                       const std::vector<std::string>& argv) {
    std::vector<std::string> args{"EVAL", std::string(s.body), std::to_string(keys.size())};
    args.insert(args.end(), keys.begin(), keys.end());
    args.insert(args.end(), argv.begin(), argv.end());
    auto r = sync(run(std::move(args)));
    return std::to_string(r.asInteger());
}

const std::string kPrefix = "test:scripts:grp:";
// One filter: type '4' (int32), op '='. Both group and entity leave it unset.
const std::string kSchema = "4=";
const std::string kUnset(1, '\0');
const std::string kGroup = kPrefix + kUnset;
const std::string kMaster = "test:scripts:master";
const std::string kTracking = "test:scripts:tracking";

// One group (sort index 0) tracking one page that has no list header: every
// list script must treat it as possibly affected and delete it.
void seedGroup() {
    sync(run({"HSET", kMaster, kGroup, "0"}));
    sync(run({"SADD", kGroup + ":_keys", "test:scripts:page"}));
    sync(run({"SET", "test:scripts:page", "x"}));
    sync(run({"SADD", kTracking, "test:scripts:page2"}));
    sync(run({"SET", "test:scripts:page2", "x"}));
}

struct Invocation {
    const io::RedisScript* script;
    std::vector<std::string> keys;
    std::vector<std::string> argv;
    std::string expected;
};

std::vector<Invocation> invocations() {
    const std::string prefixLen = std::to_string(kPrefix.size());
    const std::string hdr = std::to_string(list::kListBoundsHeaderSize);
    const std::string gen = "test:scripts:gen";
    return {
        {&scripts::kBumpGenMany, {gen}, {"1", "2"}, "2"},
        {&scripts::kSetIfGen, {gen, "test:scripts:e1"}, {"9", "1", "payload", "60"}, "0"},
        {&scripts::kSetManyIfGen, {gen, "test:scripts:e2"}, {"60", "9", "0", "payload"}, "1"},
        {&scripts::kListStore, {"test:scripts:lp", "test:scripts:lg:_keys", kMaster},
         {"payload", "60", "test:scripts:lg", "3"}, "1"},
        {&scripts::kInvalidateListGroup, {kTracking}, {}, "1"},
        {&scripts::kInvalidateListGroupSelective, {kTracking}, {"5", hdr}, "1"},
        {&scripts::kInvalidateListGroupSelectiveUpdate, {kTracking}, {"1", "5", hdr}, "1"},
        {&scripts::kInvalidateListGroupsSelective, {kMaster},
         {prefixLen, hdr, kSchema, kUnset, "5"}, "1"},
        {&scripts::kInvalidateListGroupsByPredicate, {kMaster},
         {prefixLen, hdr, kSchema, kUnset, "1", "9"}, "1"},
        {&scripts::kInvalidateListGroupsSelectiveUpdate, {kMaster},
         {prefixLen, hdr, kSchema, kUnset, "5", kUnset, "7"}, "1"},
    };
}

uint64_t reloads() { return TestInternals::scriptReloads(); }

void scriptFlush() { sync(run({"SCRIPT", "FLUSH"})); }

void scriptLoad(const io::RedisScript& s) { sync(run({"SCRIPT", "LOAD", std::string(s.body)})); }

bool scriptCached(const io::RedisScript& s) {
    auto r = sync(run({"SCRIPT", "EXISTS", std::string(s.sha1())}));
    return r.isArray() && r.arraySize() == 1 && r.at(0).asInteger() == 1;
}

bool exists(const std::string& key) {
    return sync(run({"EXISTS", key})).asInteger() == 1;
}

std::string getText(const std::string& key) {
    auto r = sync(run({"GET", key}));
    return r.isNil() ? std::string("<nil>") : r.asString();
}

int64_t hget(const std::string& key, const std::string& field) {
    auto r = sync(run({"HGET", key, field}));
    return r.isNil() ? 0 : std::stoll(r.asString());
}

// =============================================================================
// Every RedisCache operation that runs a script, on the data seedGroup() lays.
// =============================================================================

const std::string kGen = "test:scripts:gen";
const std::string kPage = "test:scripts:page";
const std::array<std::size_t, 3> kSlots{1, 2, 3};

struct Op {
    const char* name;
    const io::RedisScript* script;
    io::Task<int64_t> (*call)();
    int64_t expected;
    void (*checkEffect)();
};

io::Task<int64_t> opBumpGenMany() {
    co_await RedisCache::bumpGenMany(kGen, kSlots);
    co_return 0;
}
void effectBumpGenMany() {
    // Exactly one HINCRBY per slot, even when the call fell back to EVAL.
    for (auto slot : kSlots) CHECK(hget(kGen, std::to_string(slot)) == 1);
}

io::Task<int64_t> opSetIfGen() {
    co_return co_await RedisCache::setIfGen(kGen, "test:scripts:e1", 9, 0, "payload", 60) ? 1 : 0;
}
void effectSetIfGen() {
    CHECK(getText("test:scripts:e1") == "payload");
    const auto ttl = sync(run({"TTL", "test:scripts:e1"})).asInteger();
    CHECK(ttl > 0);
    CHECK(ttl <= 60);
}

io::Task<int64_t> opSetManyIfGen() {
    const std::array entries{RedisCache::GenFill{"test:scripts:e2", 9, 0, "payload"}};
    co_return static_cast<int64_t>(co_await RedisCache::setManyIfGen(kGen, entries, 60));
}
void effectSetManyIfGen() { CHECK(getText("test:scripts:e2") == "payload"); }

const std::string kListPage = "test:scripts:lp";
const std::string kListGroup = "test:scripts:lg";

io::Task<int64_t> opListStore() {
    co_return co_await RedisCache::storeListPage(kListPage, "payload", 60, kListGroup,
                                                 kMaster, 3) ? 1 : 0;
}
void effectListStore() {
    CHECK(getText(kListPage) == "payload");
    const auto ttl = sync(run({"TTL", kListPage})).asInteger();
    CHECK(ttl > 0);
    CHECK(ttl <= 60);
    CHECK(sync(run({"SISMEMBER", kListGroup + ":_keys", kListPage})).asInteger() == 1);
    CHECK(hget(kMaster, kListGroup) == 3);
}

void effectPageDeleted() { CHECK_FALSE(exists(kPage)); }

io::Task<int64_t> opGroup() {
    co_return static_cast<int64_t>(co_await RedisCache::invalidateListGroup(kGroup));
}
io::Task<int64_t> opGroupSelective() {
    co_return static_cast<int64_t>(co_await RedisCache::invalidateListGroupSelective(kGroup, 5));
}
io::Task<int64_t> opGroupSelectiveUpdate() {
    co_return static_cast<int64_t>(
        co_await RedisCache::invalidateListGroupSelectiveUpdate(kGroup, 1, 5));
}
io::Task<int64_t> opGroupsSelective() {
    co_return static_cast<int64_t>(co_await RedisCache::invalidateListGroupsSelective(
        kMaster, kPrefix.size(), kSchema, kUnset, "5"));
}
io::Task<int64_t> opGroupsByPredicate() {
    co_return static_cast<int64_t>(co_await RedisCache::invalidateListGroupsByPredicate(
        kMaster, kPrefix.size(), kSchema, kUnset, "1", "9"));
}
io::Task<int64_t> opGroupsSelectiveUpdate() {
    co_return static_cast<int64_t>(co_await RedisCache::invalidateListGroupsSelectiveUpdate(
        kMaster, kPrefix.size(), kSchema, kUnset, "5", kUnset, "7"));
}

const std::array kOps{
    Op{"bumpGenMany", &scripts::kBumpGenMany, opBumpGenMany, 0, effectBumpGenMany},
    Op{"setIfGen", &scripts::kSetIfGen, opSetIfGen, 1, effectSetIfGen},
    Op{"setManyIfGen", &scripts::kSetManyIfGen, opSetManyIfGen, 1, effectSetManyIfGen},
    Op{"storeListPage", &scripts::kListStore, opListStore, 1, effectListStore},
    Op{"invalidateListGroup", &scripts::kInvalidateListGroup, opGroup, 1, effectPageDeleted},
    Op{"invalidateListGroupSelective", &scripts::kInvalidateListGroupSelective,
       opGroupSelective, 1, effectPageDeleted},
    Op{"invalidateListGroupSelectiveUpdate", &scripts::kInvalidateListGroupSelectiveUpdate,
       opGroupSelectiveUpdate, 1, effectPageDeleted},
    Op{"invalidateListGroupsSelective", &scripts::kInvalidateListGroupsSelective,
       opGroupsSelective, 1, effectPageDeleted},
    Op{"invalidateListGroupsByPredicate", &scripts::kInvalidateListGroupsByPredicate,
       opGroupsByPredicate, 1, effectPageDeleted},
    Op{"invalidateListGroupsSelectiveUpdate", &scripts::kInvalidateListGroupsSelectiveUpdate,
       opGroupsSelectiveUpdate, 1, effectPageDeleted},
};

// =============================================================================
// A private scheduler on the test thread, for the cases that need to see its
// pipelined counter or to cut Redis. PgProvider's bindings are thread_local, so
// it does not disturb the harness loop that sync() runs on.
// =============================================================================

using Io = io::test::ControllableIoContext;

struct ScriptEnv {
    Io io;
    std::shared_ptr<io::RedisClient<Io>> redis;
    std::shared_ptr<io::batch::BatchScheduler<Io>> batcher;
    int redis_fd = -1;

    explicit ScriptEnv(std::chrono::milliseconds query_timeout = 200ms) {
        redis = drive(io::RedisClient<Io>::connect(io, "127.0.0.1", 6379, query_timeout));
        redis_fd = io.lastAddedFd();
        auto pool = std::make_shared<io::RedisPool<Io>>(io::RedisPool<Io>::fromClients({redis}));
        batcher = std::make_shared<io::batch::BatchScheduler<Io>>(io, nullptr, pool, 8);
        PgProvider::bindBatcher<Io>(batcher, true);
    }

    ~ScriptEnv() {
        // Let a timed-out or still-running frame unwind before the loop goes.
        io.releaseReads(redis_fd);
        batcher->beginShutdown();
        pump(600ms);
        PgProvider::reset();
    }

    ScriptEnv(const ScriptEnv&) = delete;
    ScriptEnv& operator=(const ScriptEnv&) = delete;

    void pump(std::chrono::milliseconds window) {
        const auto deadline = std::chrono::steady_clock::now() + window;
        while (std::chrono::steady_clock::now() < deadline) io.runOnce(10);
    }

    template<typename T>
    T drive(io::Task<T> task, std::chrono::milliseconds budget = 5s) {
        std::optional<T> out;
        std::exception_ptr ex;
        bool done = false;
        auto starter = [](io::Task<T> t, std::optional<T>& o, std::exception_ptr& e,
                          bool& d) -> io::DetachedTask {
            try { o.emplace(co_await std::move(t)); } catch (...) { e = std::current_exception(); }
            d = true;
        };
        starter(std::move(task), out, ex, done);
        const auto deadline = std::chrono::steady_clock::now() + budget;
        while (!done && std::chrono::steady_clock::now() < deadline) io.runOnce(10);
        REQUIRE(done);
        if (ex) std::rethrow_exception(ex);
        return std::move(*out);
    }
};

io::Task<int64_t> integerOf(io::Task<io::RedisResult> t) {
    auto r = co_await std::move(t);
    co_return r.asInteger();
}

io::Task<int64_t> countOf(io::Task<size_t> t) {
    co_return static_cast<int64_t>(co_await std::move(t));
}

io::Task<std::vector<int64_t>> gather(std::vector<io::Task<int64_t>> tasks) {
    co_return co_await io::whenAll(std::span(tasks));
}

io::Task<int64_t> isValue(io::Task<io::RedisResult> t, std::string expected) {
    auto r = co_await std::move(t);
    co_return !r.isNil() && r.asString() == expected ? 1 : 0;
}

// Warnings logged while a test runs (the callback is a plain function pointer).
struct WarnCapture {
    static inline std::mutex mutex;
    static inline std::vector<std::string> lines;
    jcailloux::relais::log::Callback previous;

    WarnCapture() : previous(jcailloux::relais::log::getCallback()) {
        std::lock_guard lock(mutex);
        lines.clear();
        jcailloux::relais::log::setCallback(
            [](jcailloux::relais::log::Level level, const char* msg, size_t len) {
                if (level != jcailloux::relais::log::Level::Warn) return;
                std::lock_guard lock(mutex);
                lines.emplace_back(msg, len);
            });
    }
    ~WarnCapture() { jcailloux::relais::log::setCallback(previous); }

    bool any(std::string_view a, std::string_view b) const {
        std::lock_guard lock(mutex);
        return std::ranges::any_of(lines, [&](const std::string& l) {
            return l.find(a) != std::string::npos && l.find(b) != std::string::npos;
        });
    }
};

}  // namespace

TEST_CASE("SCRIPT LOAD returns each registered script's compile-time SHA1",
          "[integration][redis][redis-script]")
{
    TransactionGuard tx;
    for (const auto* s : scripts::kAllScripts) {
        auto r = sync(run({"SCRIPT", "LOAD", std::string(s->body)}));
        REQUIRE(r.asString() == s->sha1());
    }
}

TEST_CASE("every registered script runs on minimal data without a Lua error",
          "[integration][redis][redis-script]")
{
    const auto all = invocations();
    REQUIRE(all.size() == scripts::kAllScripts.size());
    for (const auto* s : scripts::kAllScripts)
        REQUIRE(std::ranges::count(all, s, &Invocation::script) == 1);

    for (const auto& inv : all) {
        TransactionGuard tx;
        seedGroup();
        CAPTURE(inv.script->sha1());
        REQUIRE(evalScript(*inv.script, inv.keys, inv.argv) == inv.expected);
    }
}

TEST_CASE("each script operation sends EVALSHA when the script is cached",
          "[integration][redis][redis-script]")
{
    for (const auto& op : kOps) {
        CAPTURE(op.name);
        TransactionGuard tx;
        seedGroup();
        scriptLoad(*op.script);
        const auto before = reloads();
        CHECK(sync(op.call()) == op.expected);
        op.checkEffect();
        CHECK(reloads() == before);
    }
}

TEST_CASE("after SCRIPT FLUSH each script operation falls back to EVAL once",
          "[integration][redis][redis-script]")
{
    for (const auto& op : kOps) {
        CAPTURE(op.name);
        {
            TransactionGuard tx;
            seedGroup();
            scriptFlush();
            const auto before = reloads();
            // The result is the EVAL's, the effect happened once (bumpGenMany: +1
            // per slot, setIfGen: one write with its TTL).
            CHECK(sync(op.call()) == op.expected);
            op.checkEffect();
            CHECK(reloads() >= before + 1);
            CHECK(scriptCached(*op.script));
        }
        {
            // The fallback cached the script: the next call needs none.
            TransactionGuard tx;
            seedGroup();
            const auto before = reloads();
            CHECK(sync(op.call()) == op.expected);
            op.checkEffect();
            CHECK(reloads() == before);
        }
    }
}

TEST_CASE("an error reply other than NOSCRIPT does not fall back",
          "[integration][redis][redis-script]")
{
    TransactionGuard tx;
    WarnCapture warns;
    // The master key is a string, not a hash: HGETALL fails with WRONGTYPE.
    sync(run({"SET", kMaster, "x"}));
    scriptLoad(scripts::kInvalidateListGroupsSelective);
    const auto before = reloads();
    CHECK(sync(opGroupsSelective()) == 0);
    CHECK(reloads() == before);
    CHECK(warns.any("invalidateListGroupsSelective error", "WRONGTYPE"));
}

TEST_CASE("a Redis that does not answer gets no fallback",
          "[integration][redis][redis-script]")
{
    TransactionGuard tx;
    seedGroup();
    scriptFlush();
    ScriptEnv env(200ms);
    env.io.holdReads(env.redis_fd);
    const auto before = reloads();
    CHECK(env.drive(opGroupsSelective()) == 0);
    CHECK(reloads() == before);
    env.io.releaseReads(env.redis_fd);
}

TEST_CASE("the fallback works inside a pipelined batch, on arguments already destroyed",
          "[integration][redis][redis-script]")
{
    constexpr int kGroups = 8;
    TransactionGuard tx;

    // The public operations and PgProvider::redis are lazy coroutines taking
    // references: their arguments must outlive the gather below.
    std::vector<std::string> masters, pages, readKeys, writeKeys;
    const std::string sortValue = "5";
    for (int i = 0; i < kGroups; ++i) {
        const std::string prefix = "test:scripts:p" + std::to_string(i) + ":";
        const std::string group = prefix + kUnset;
        masters.push_back("test:scripts:m" + std::to_string(i));
        pages.push_back("test:scripts:pg" + std::to_string(i));
        sync(run({"HSET", masters.back(), group, "0"}));
        sync(run({"SADD", group + ":_keys", pages.back()}));
        sync(run({"SET", pages.back(), "x"}));
        readKeys.push_back("test:scripts:v" + std::to_string(i));
        writeKeys.push_back("test:scripts:w" + std::to_string(i));
        sync(run({"SET", readKeys.back(), "val" + std::to_string(i)}));
    }
    const size_t prefixLen = std::string("test:scripts:p0:").size();

    ScriptEnv env;
    // Past the bootstrap, a command sent while another is in flight joins a batch.
    for (int i = 0; i < io::batch::TimingEstimator::kBootstrapThreshold + 2; ++i)
        env.drive(PgProvider::redis("PING"));
    scriptFlush();
    const auto reloadsBefore = reloads();
    const auto pipelinedBefore = env.batcher->redisPipelinedEntries();

    std::vector<io::Task<int64_t>> tasks;
    for (int i = 0; i < kGroups; ++i) {
        if (i % 2 == 0) {
            tasks.push_back(countOf(RedisCache::invalidateListGroupsSelective(
                masters[i], prefixLen, kSchema, kUnset, sortValue)));
        } else {
            // Every argument is a temporary gone by the time the task runs, and
            // again when its EVAL fallback resends them.
            tasks.push_back(integerOf(RedisCache::evalScript(
                scripts::kInvalidateListGroupsSelective, 1,
                std::string(masters[i]), std::to_string(prefixLen),
                static_cast<int>(list::kListBoundsHeaderSize),
                std::string(kSchema), std::string(kUnset), std::string("5"))));
        }
        tasks.push_back(isValue(PgProvider::redis("GET", readKeys[i]),
                                "val" + std::to_string(i)));
        tasks.push_back(isValue(PgProvider::redis("SET", writeKeys[i], "w"), "OK"));
    }
    const auto results = env.drive(gather(std::move(tasks)));

    CHECK(env.batcher->redisPipelinedEntries() > pipelinedBefore);
    CHECK(reloads() >= reloadsBefore + 1);
    for (std::size_t i = 0; i < results.size(); ++i) {
        CAPTURE(i);
        CHECK(results[i] == 1);
    }
    for (int i = 0; i < kGroups; ++i) {
        CAPTURE(i);
        CHECK_FALSE(exists(pages[i]));
        CHECK(getText(writeKeys[i]) == "w");
    }
}
