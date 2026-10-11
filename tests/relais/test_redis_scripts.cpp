/**
 * test_redis_scripts.cpp
 *
 * The Lua script registry (cache/RedisScripts.h) against a live server:
 *   - SCRIPT LOAD of every registered script returns exactly its compile-time
 *     SHA1: the digest matches the server's, and every script compiles;
 *   - every script runs once on minimal data without a Lua error. The list
 *     scripts are composed from shared fragments that read the prologue's
 *     locals; a fragment placed before them would read a global, which Redis
 *     rejects only when the line runs. The data below reaches those lines: one
 *     group with one filter (schema, n_filters) and one header-less page
 *     (hdr_size), which every list script deletes.
 */

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <string>
#include <vector>

#include "fixtures/test_helper.h"
#include "jcailloux/relais/cache/RedisCache.h"
#include "jcailloux/relais/cache/RedisScripts.h"

using namespace relais_test;
namespace scripts = jcailloux::relais::cache::scripts;
using jcailloux::relais::PgProvider;
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
