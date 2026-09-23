/**
 * test_conditional_writes_contention.cpp
 *
 * Conditional writes raced from several event loops at once. Each loop is an
 * IoPool worker with its own PostgreSQL and Redis connections; the repositories
 * and their L1 are shared by all of them. The database decides every race, so
 * the outcome is exact whatever the interleaving:
 *
 *   A. claim — one row, ten rows, Exact pairs over five rows: no row taken
 *      twice, a refused Exact claim writes nothing.
 *   B. task queue — UpTo consumers drain every task exactly once.
 *   C. preemption — Wait claims moving held rows while SkipLocked claims take
 *      free ones: each version of a row is claimed once.
 *   D. compare-and-set by version — exactly one winner.
 *
 * On every loop a reader evicts and re-reads the contended rows while the race
 * runs. Once the loops are stopped, find agrees with the database through L1
 * and through L2.
 *
 * Catch2 assertions are not thread-safe: the loops only collect outcomes, and
 * every CHECK runs on the test thread.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include <algorithm>
#include <coroutine>
#include <cstdint>
#include <future>
#include <latch>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"
#include "jcailloux/relais/io/IoPool.h"
#include "jcailloux/relais/io/WhenAll.h"
#include "jcailloux/relais/runtime/Spawn.h"

// #############################################################################
//
//  ThreadSanitizer suppressions (compiled into the binary)
//
//  Same scope as test_concurrency.cpp: parlayhash readers copy a bucket
//  snapshot unsynchronized, then validate it against the live head and retry.
//  Correct by construction, invisible to TSan. Only parlayhash frames match.
//
// #############################################################################

#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#    define RELAIS_TSAN_BUILD 1
#  endif
#endif
#if defined(__SANITIZE_THREAD__)
#  define RELAIS_TSAN_BUILD 1
#endif

#ifdef RELAIS_TSAN_BUILD
extern "C" const char* __tsan_default_suppressions() {
    return
        "race:parlay::parlay_hash\n"
        "race:parlay::DirectEntries\n";
}
#endif

using namespace relais_test;

namespace jr = jcailloux::relais;
using jr::entity::set;
using jr::entity::increment;
using jr::entity::when;
using jr::entity::eq;

using SlotF = TestSlotEntity::Field;
using Ids = std::vector<int64_t>;

namespace {

constexpr int kLoops = 4;

constexpr jr::ClaimOptions kClaimChanges{.returns = jr::Returns::Changes};
constexpr jr::ClaimOptions kWaitChanges{.lock = jr::Lock::Wait, .returns = jr::Returns::Changes};
constexpr jr::ClaimOptions kUpTo{.mode = jr::ClaimMode::UpTo};

template<typename Repo>
constexpr bool kHasL1 = Repo::config.cache_level == jr::config::CacheLevel::L1
                     || Repo::config.cache_level == jr::config::CacheLevel::L1_L2;

template<typename T> struct TaskValue;
template<typename T> struct TaskValue<io::Task<T>> { using type = T; };

template<typename Op>
using OpResult = typename TaskValue<std::invoke_result_t<Op, int64_t>>::type;

/// Re-queues the coroutine behind the loop's pending I/O, so a reader that
/// keeps hitting L1 cannot starve the writers sharing its loop.
struct YieldTo {
    io::IoPool::Io& io;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const { io.post([h] { h.resume(); }); }
    void await_resume() const noexcept {}
};

/// Evicts and re-reads `ids` until no writer is left on this loop: every read
/// goes past L1, and its fill races the writes.
template<typename Repo>
io::Task<void> reread(io::IoPool::Io& io, Ids ids, const int& running) {
    while (running > 0) {
        for (auto id : ids) {
            if (running == 0) break;
            if constexpr (kHasL1<Repo>) TestInternals::evict<Repo>(id);
            (void)co_await Repo::find(id);
            co_await YieldTo{io};
        }
        co_await YieldTo{io};
    }
}

template<typename R>
struct LoopOutcome {
    std::vector<R> values;
    std::vector<std::string> errors;
};

/// On one loop: `count` concurrent op(tag) calls, tagged loop * 1000 + i + 1
/// (never 0), and a reader of `watched`. An exception is recorded as an error. Each
/// call holds its own copy of `op` while awaiting it, so `op` may capture.
template<typename Repo, typename Op>
io::Task<LoopOutcome<OpResult<Op>>> raceOnLoop(
    io::IoPool::Io& io, int loop, int count, Op op, Ids watched)
{
    using R = OpResult<Op>;
    LoopOutcome<R> out;
    out.values.resize(count);
    int running = count;

    std::vector<io::Task<void>> tasks;
    tasks.reserve(count + 1);
    for (int i = 0; i < count; ++i) {
        tasks.push_back([](Op op, int64_t tag, R& slot, std::vector<std::string>& errors,
                           int& running) -> io::Task<void> {
            try {
                slot = co_await op(tag);
            } catch (const std::exception& e) {
                errors.emplace_back(e.what());
            } catch (...) {
                errors.emplace_back("unknown exception");
            }
            --running;
        }(op, int64_t{loop} * 1000 + i + 1, out.values[i], out.errors, running));
    }
    tasks.push_back(reread<Repo>(io, std::move(watched), running));
    co_await io::whenAll(std::move(tasks));
    co_return out;
}

template<typename T>
io::Task<T> afterAll(std::latch& start, io::Task<T> task) {
    start.arrive_and_wait();  // every loop enters the race at once
    co_return co_await std::move(task);
}

/// Several event loops sharing the process: the repositories and their L1 are
/// common, each loop owns its PostgreSQL and Redis connections.
class Loops {
public:
    Loops() {
        io::IoPoolConfig cfg;
        cfg.num_workers = kLoops;
        cfg.pg_conninfo = getConnInfo();
        cfg.redis = io::RedisWorkerConfig{.conns_per_worker = 2};
        cfg.pin_to_cores = false;
        cfg.pg_min_conns_per_worker = 2;
        cfg.pg_max_conns_per_worker = 4;
        pool_ = io::IoPool::create(cfg);
    }

    ~Loops() { stop(); }

    Loops(const Loops&) = delete;
    Loops& operator=(const Loops&) = delete;

    /// Runs `per_loop` op(tag) calls on every loop at once, with a reader of
    /// `watched` on each, and returns all the results.
    template<typename Repo, typename Op>
    std::vector<OpResult<Op>> race(int per_loop, Op op, const Ids& watched) {
        using R = OpResult<Op>;
        std::latch start{kLoops};
        std::vector<std::promise<LoopOutcome<R>>> done(kLoops);
        std::vector<std::future<LoopOutcome<R>>> results;
        for (auto& p : done) results.push_back(p.get_future());

        for (int w = 0; w < kLoops; ++w) {
            auto& loop_io = pool_->workerIo(w);
            jr::spawnOn(loop_io,
                afterAll(start, raceOnLoop<Repo>(loop_io, w, per_loop, op, watched)),
                [&p = done[w]](jr::Outcome<LoopOutcome<R>> r) {
                    if (r) p.set_value(std::move(*r));
                    else p.set_exception(r.error());
                });
        }

        std::vector<R> all;
        std::string errors;
        for (auto& f : results) {
            auto outcome = f.get();
            for (auto& e : outcome.errors) errors += e + "\n";
            for (auto& v : outcome.values) all.push_back(std::move(v));
        }
        REQUIRE(errors == "");
        return all;
    }

    /// Stops the loops, draining their detached cache work.
    void stop() {
        if (pool_) pool_->stop();
    }

private:
    std::unique_ptr<io::IoPool> pool_;
};

// =============================================================================
// Fixture rows and checks
// =============================================================================

Ids insertFree(int64_t group, int count) {
    auto r = execQueryArgs(
        "INSERT INTO relais_test_slots (group_id) "
        "SELECT $1 FROM generate_series(1, $2) RETURNING id",
        group, count);
    Ids ids;
    for (size_t i = 0; i < r.rows(); ++i) ids.push_back(r[i].get<int64_t>(0));
    std::sort(ids.begin(), ids.end());
    return ids;
}

auto freeIn(int64_t group) {
    return when(eq<SlotF::group_id>(group), eq<SlotF::state>(SlotState::Free));
}

TestSlotEntity dbRow(int64_t id) {
    auto v = sync(UncachedTestSlotRepo::find(id));
    REQUIRE(v);
    return *v;
}

bool sameSlot(const TestSlotEntity& a, const TestSlotEntity& b) {
    return a.id == b.id && a.group_id == b.group_id && a.state == b.state
        && a.holder == b.holder && a.expires_at == b.expires_at
        && a.counter == b.counter && a.version == b.version && a.priority == b.priority;
}

std::string describe(const TestSlotEntity& e) {
    return "{id " + std::to_string(e.id) + ", state " + std::to_string(static_cast<int>(e.state))
         + ", holder " + (e.holder ? std::to_string(*e.holder) : "null")
         + ", counter " + std::to_string(e.counter) + ", version " + std::to_string(e.version) + "}";
}

/// find agrees with the database for every row: through L1 when it holds the
/// row, then through L2 once L1 is evicted.
template<typename Repo>
void checkCoherent(const Ids& ids) {
    std::string mismatches;
    auto compare = [&](const char* tier, const TestSlotEntity& db, const auto& seen) {
        if (seen && sameSlot(db, *seen)) return;
        mismatches += std::string(tier) + ": db " + describe(db) + " seen "
                    + (seen ? describe(*seen) : "absent") + "\n";
    };
    for (auto id : ids) {
        auto db = dbRow(id);
        compare("find", db, sync(Repo::find(id)));
        if constexpr (kHasL1<Repo>) {
            TestInternals::evict<Repo>(id);
            compare("find below L1", db, sync(Repo::find(id)));
        }
    }
    CHECK(mismatches == "");
}

template<typename E>
Ids idsOf(const std::vector<E>& rows) {
    Ids ids;
    for (const auto& e : rows) ids.push_back(e.id);
    return ids;
}

/// Claimed ids, flattened; each claimed id appears once when no row was taken twice.
Ids flatten(const std::vector<Ids>& claims) {
    Ids all;
    for (const auto& c : claims) all.insert(all.end(), c.begin(), c.end());
    std::sort(all.begin(), all.end());
    return all;
}

bool distinct(const Ids& sorted) {
    return std::adjacent_find(sorted.begin(), sorted.end()) == sorted.end();
}

}  // namespace

// #############################################################################
//
//  A. claim — no row taken twice
//
// #############################################################################

TEMPLATE_TEST_CASE("[contention] claim(1) on one row: exactly one winner",
                   "[claim][integration][concurrency]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto ids = insertFree(1, 1);
    Loops loops;

    auto claims = loops.race<Repo>(16, [](int64_t tag) -> io::Task<Ids> {
        auto rows = co_await Repo::claim(freeIn(1), 1,
            set<SlotF::state>(SlotState::Taken), set<SlotF::holder>(tag),
            increment<SlotF::version>(1));
        if (!rows) throw std::runtime_error("claim error");
        Ids out;
        for (const auto& e : *rows) out.push_back(e.holder.value_or(-1));
        co_return out;
    }, ids);
    loops.stop();

    // Each winner reports the holder it wrote: exactly one, and it is the DB's.
    auto holders = flatten(claims);
    REQUIRE(holders.size() == 1);
    auto row = dbRow(ids[0]);
    CHECK(row.state == SlotState::Taken);
    CHECK(row.holder == std::optional<int64_t>{holders[0]});
    CHECK(row.version == 1);
    checkCoherent<Repo>(ids);
}

TEMPLATE_TEST_CASE("[contention] claim(1) on ten rows: ten winners, disjoint rows",
                   "[claim][integration][concurrency]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto ids = insertFree(1, 10);
    Loops loops;

    // A claimer that finds every remaining row locked comes back empty, but
    // each locked row is taken by its locker: with more claimers than rows,
    // every row is taken.
    auto claims = loops.race<Repo>(16, [](int64_t tag) -> io::Task<Ids> {
        auto rows = co_await Repo::claim(freeIn(1), 1,
            set<SlotF::state>(SlotState::Taken), set<SlotF::holder>(tag),
            increment<SlotF::version>(1));
        if (!rows) throw std::runtime_error("claim error");
        co_return idsOf(*rows);
    }, ids);
    loops.stop();

    auto taken = flatten(claims);
    CHECK(std::count_if(claims.begin(), claims.end(),
                        [](const Ids& c) { return !c.empty(); }) == 10);
    CHECK(taken == ids);  // sorted: every row once
    for (auto id : ids) {
        auto row = dbRow(id);
        CHECK(row.state == SlotState::Taken);
        CHECK(row.version == 1);
    }
    checkCoherent<Repo>(ids);
}

TEMPLATE_TEST_CASE("[contention] Exact claim(2) on five rows: pairs or nothing",
                   "[claim][integration][concurrency]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto ids = insertFree(1, 5);
    Loops loops;

    struct Claimed { int64_t tag = 0; Ids ids; };
    auto claims = loops.race<Repo>(16, [](int64_t tag) -> io::Task<Claimed> {
        auto rows = co_await Repo::claim(freeIn(1), 2,
            set<SlotF::state>(SlotState::Taken), set<SlotF::holder>(tag),
            increment<SlotF::version>(1));
        if (!rows) throw std::runtime_error("claim error");
        co_return Claimed{tag, idsOf(*rows)};
    }, ids);

    // Every success is a pair; no row is in two of them. A claimer seeing
    // fewer than two free rows unlocked is refused, even if rows remain.
    std::map<int64_t, int64_t> holder_of;
    int winners = 0;
    for (const auto& c : claims) {
        if (c.ids.empty()) continue;
        ++winners;
        CHECK(c.ids.size() == 2);
        for (auto id : c.ids) CHECK(holder_of.emplace(id, c.tag).second);
    }
    CHECK(winners <= 2);

    // A refused claim wrote nothing: each taken row carries its winner's tag,
    // and the others are still free for a claim run once the race is over.
    for (auto id : ids) {
        auto row = dbRow(id);
        if (auto it = holder_of.find(id); it != holder_of.end()) {
            CHECK(row.state == SlotState::Taken);
            CHECK(row.holder == std::optional<int64_t>{it->second});
            CHECK(row.version == 1);
        } else {
            CHECK(row.state == SlotState::Free);
            CHECK(row.version == 0);
        }
    }
    auto rest = sync(Repo::template claim<kUpTo>(freeIn(1), 5,
        set<SlotF::state>(SlotState::Taken), increment<SlotF::version>(1)));
    REQUIRE(rest);
    CHECK(rest->size() == 5 - holder_of.size());
    loops.stop();
    checkCoherent<Repo>(ids);
}

// #############################################################################
//
//  B. Task queue — every task exactly once
//
// #############################################################################

TEMPLATE_TEST_CASE("[contention] UpTo consumers drain a task queue exactly once",
                   "[claim][integration][concurrency]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    constexpr int kTasks = 1000;
    auto ids = insertFree(7, kTasks);
    Loops loops;

    // Two consumers per loop, batches of up to 10, until the queue looks empty.
    // An empty batch while tasks remain means every remaining task is locked
    // by a consumer that will take it.
    auto batches = loops.race<Repo>(2, [](int64_t tag) -> io::Task<Ids> {
        Ids mine;
        for (;;) {
            auto rows = co_await Repo::template claim<kUpTo>(freeIn(7), 10,
                set<SlotF::state>(SlotState::Taken), set<SlotF::holder>(tag),
                increment<SlotF::counter>(1));
            if (!rows) throw std::runtime_error("claim error");
            if (rows->empty()) break;
            for (const auto& e : *rows) mine.push_back(e.id);
        }
        co_return mine;
    }, Ids(ids.begin(), ids.begin() + 32));
    loops.stop();

    CHECK(flatten(batches) == ids);
    auto counts = execQueryArgs(
        "SELECT count(*) FILTER (WHERE state = 'taken' AND counter = 1), count(*) "
        "FROM relais_test_slots WHERE group_id = $1", int64_t{7});
    CHECK(counts[0].get<int64_t>(0) == kTasks);
    CHECK(counts[0].get<int64_t>(1) == kTasks);
    checkCoherent<Repo>(ids);
}

// #############################################################################
//
//  C. Preemption waiting on held rows, claims skipping locked ones
//
// #############################################################################

TEMPLATE_TEST_CASE("[contention] Wait preemption against SkipLocked claims",
                   "[claim][integration][concurrency]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    using Change = jr::Change<TestSlotEntity>;
    TransactionGuard guard;
    auto ids = insertFree(1, 8);
    Loops loops;

    // Even tags take a free row (SkipLocked); odd tags move a held row to
    // themselves, waiting for its current writer. Every write bumps the version.
    auto changes = loops.race<Repo>(16, [](int64_t tag) -> io::Task<std::vector<Change>> {
        std::optional<std::vector<Change>> rows;
        if (tag % 2 == 0) {
            rows = co_await Repo::template claim<kClaimChanges>(freeIn(1), 1,
                set<SlotF::state>(SlotState::Held), set<SlotF::holder>(tag),
                increment<SlotF::version>(1));
        } else {
            rows = co_await Repo::template claim<kWaitChanges>(
                when(eq<SlotF::group_id>(int64_t{1}), eq<SlotF::state>(SlotState::Held)), 1,
                set<SlotF::holder>(tag), increment<SlotF::version>(1));
        }
        if (!rows) throw std::runtime_error("claim error");
        co_return std::move(*rows);
    }, ids);
    loops.stop();

    // Per row, the claims form one chain: each saw the version and the holder
    // the previous one committed, so no version was claimed twice.
    std::map<int64_t, std::vector<Change>> per_row;
    for (auto& batch : changes)
        for (auto& c : batch) per_row[c.after.id].push_back(std::move(c));

    for (auto id : ids) {
        auto row = dbRow(id);
        auto& chain = per_row[id];
        std::sort(chain.begin(), chain.end(), [](const Change& a, const Change& b) {
            return a.before.version < b.before.version;
        });
        CHECK(row.version == static_cast<int64_t>(chain.size()));
        int from_free = 0;
        for (size_t k = 0; k < chain.size(); ++k) {
            CHECK(chain[k].before.version == static_cast<int64_t>(k));
            CHECK(chain[k].after.version == static_cast<int64_t>(k + 1));
            if (chain[k].before.state == SlotState::Free) ++from_free;
            if (k > 0) CHECK(chain[k].before.holder == chain[k - 1].after.holder);
        }
        CHECK(from_free <= 1);
        if (!chain.empty()) CHECK(row.holder == chain.back().after.holder);
    }
    checkCoherent<Repo>(ids);
}

// #############################################################################
//
//  D. Compare-and-set by version
//
// #############################################################################

TEMPLATE_TEST_CASE("[contention] compare-and-set by version: exactly one winner",
                   "[patchIf][integration][concurrency]",
                   UncachedTestSlotRepo, FullCacheTestSlotRepo)
{
    using Repo = TestType;
    TransactionGuard guard;
    auto ids = insertFree(1, 1);
    Loops loops;

    // Same SQL, same params but the holder: -1 error, 0 refused, else the tag.
    auto outcomes = loops.race<Repo>(16, [id = ids[0]](int64_t tag) -> io::Task<int64_t> {
        auto v = co_await Repo::patchIf(id, when(eq<SlotF::version>(int64_t{0})),
            set<SlotF::holder>(tag), increment<SlotF::version>(1));
        if (!v) throw std::runtime_error("patchIf error");
        co_return *v ? tag : 0;
    }, ids);
    loops.stop();

    Ids winners;
    for (auto t : outcomes) if (t != 0) winners.push_back(t);
    REQUIRE(winners.size() == 1);
    auto row = dbRow(ids[0]);
    CHECK(row.version == 1);
    CHECK(row.holder == std::optional<int64_t>{winners[0]});
    checkCoherent<Repo>(ids);
}
