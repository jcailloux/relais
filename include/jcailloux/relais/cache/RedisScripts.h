#ifndef JCX_RELAIS_REDISSCRIPTS_H
#define JCX_RELAIS_REDISSCRIPTS_H

#include <array>
#include <cstddef>
#include <string_view>

#include "jcailloux/relais/io/redis/RedisScript.h"

/// Every Lua script relais runs on Redis, in one place. Each one is preceded by its
/// contract: KEYS, ARGV, return value. The list invalidation scripts are composed at
/// compile time from shared fragments, so a change to the filter or page-header
/// format is written once.
namespace jcailloux::relais::cache::scripts {

/// Concatenation of string views with static storage into one static buffer.
template<const std::string_view&... Parts>
struct LuaConcat {
    static constexpr std::size_t kSize = (Parts.size() + ...);
    static constexpr std::array<char, kSize> kBuf = [] {
        std::array<char, kSize> out{};
        std::size_t pos = 0;
        auto put = [&](std::string_view part) {
            for (char c : part) out[pos++] = c;
        };
        (put(Parts), ...);
        return out;
    }();
    static constexpr std::string_view kView{kBuf.data(), kSize};
};

// =============================================================================
// Generation slots (conditional L2 fill)
// =============================================================================

/// KEYS[1] generation hash; ARGV[1..n] slots. HINCRBY +1 on each slot.
/// Returns n.
inline constexpr io::RedisScript kBumpGenMany = io::makeRedisScript(
    "for i = 1, #ARGV do redis.call('HINCRBY', KEYS[1], ARGV[i], 1) end\n"
    "return #ARGV");

/// KEYS[1] generation hash, KEYS[2] entity key; ARGV[1] slot, ARGV[2] generation
/// snapshot, ARGV[3] payload, ARGV[4] TTL in seconds. Writes the payload with its TTL
/// iff the slot still holds the snapshot (an absent slot counts as 0).
/// Returns 1 if written, 0 otherwise.
inline constexpr io::RedisScript kSetIfGen = io::makeRedisScript(R"lua(
local cur = redis.call('HGET', KEYS[1], ARGV[1])
cur = cur and tonumber(cur) or 0
if cur == tonumber(ARGV[2]) then
    redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[4])
    return 1
end
return 0
)lua");

/// KEYS[1] generation hash, KEYS[2..n+1] entity keys; ARGV[1] TTL in seconds, then
/// one (slot, snapshot, payload) triple per entity key, in KEYS order. Each entry is
/// written as kSetIfGen would. Returns the number of entries written.
inline constexpr io::RedisScript kSetManyIfGen = io::makeRedisScript(R"lua(
local ttl = ARGV[1]
local n = #KEYS - 1
local count = 0
for i = 1, n do
    local base = 1 + (i - 1) * 3
    local slot = ARGV[base + 1]
    local snap = tonumber(ARGV[base + 2])
    local payload = ARGV[base + 3]
    local cur = redis.call('HGET', KEYS[1], slot)
    cur = cur and tonumber(cur) or 0
    if cur == snap then
        redis.call('SET', KEYS[i + 1], payload, 'EX', ttl)
        count = count + 1
    end
end
return count
)lua");

// =============================================================================
// List page store
// =============================================================================

/// KEYS[1] page, KEYS[2] tracking set [, KEYS[3] master hash]; ARGV[1] payload,
/// ARGV[2] TTL in seconds [, ARGV[3] group key, ARGV[4] sort index]. With a master
/// hash, records the group's sort index there; adds the page to the tracking set,
/// whose TTL is set only if it has none (a later page does not extend it); writes
/// the page with its TTL. A script runs alone but is not rolled back: the page is
/// written last, so a failed step leaves no page that an invalidation cannot find.
/// Returns 1.
inline constexpr io::RedisScript kListStore = io::makeRedisScript(R"lua(
if #KEYS == 3 then
    redis.call('HSET', KEYS[3], ARGV[3], ARGV[4])
end
redis.call('SADD', KEYS[2], KEYS[1])
redis.call('EXPIRE', KEYS[2], ARGV[2], 'NX')
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
)lua");

// =============================================================================
// One list group (tracking set "<group>:_keys")
// =============================================================================

/// KEYS[1] tracking set. Unlinks every tracked page, then the set.
/// Returns the number of pages unlinked.
inline constexpr io::RedisScript kInvalidateListGroup = io::makeRedisScript(R"lua(
local keys = redis.call('SMEMBERS', KEYS[1])
local count = 0
for _, key in ipairs(keys) do
    redis.call('UNLINK', key)
    count = count + 1
end
redis.call('UNLINK', KEYS[1])
return count
)lua");

/// KEYS[1] tracking set; ARGV[1] entity sort value, ARGV[2] page header size.
/// Deletes the pages whose sort range may hold the value (a page without a valid
/// header is deleted); unlinks the set once every page is gone.
/// Returns the number of pages deleted.
inline constexpr io::RedisScript kInvalidateListGroupSelective = io::makeRedisScript(R"lua(
local keys = redis.call('SMEMBERS', KEYS[1])
local entity_val = tonumber(ARGV[1])
local hdr_size = tonumber(ARGV[2])
local count = 0

local function read_int64(s, off)
    local b1,b2,b3,b4,b5,b6,b7,b8 = string.byte(s, off+1, off+8)
    local val = b1 + b2*256 + b3*65536 + b4*16777216
              + b5*4294967296 + b6*1099511627776
              + b7*281474976710656 + b8*72057594037927936
    if val >= 2^63 then val = val - 2^64 end
    return val
end

for _, page_key in ipairs(keys) do
    local hdr = redis.call('GETRANGE', page_key, 0, hdr_size - 1)
    local should_del = true

    if #hdr >= hdr_size and string.byte(hdr, 1) == 0x53 and string.byte(hdr, 2) == 0x52 then
        local first = read_int64(hdr, 2)
        local last  = read_int64(hdr, 10)
        local flags = string.byte(hdr, 19)
        local is_desc       = (flags % 2) == 1
        local is_first_page = (math.floor(flags / 2) % 2) == 1
        local is_incomplete = (math.floor(flags / 4) % 2) == 1
        local is_offset     = (math.floor(flags / 8) % 2) == 0

        if is_offset then
            if is_incomplete then
                should_del = true
            elseif is_desc then
                should_del = (entity_val >= last)
            else
                should_del = (entity_val <= last)
            end
        else
            if is_first_page and is_incomplete then
                should_del = true
            elseif is_desc then
                if is_first_page then
                    should_del = (entity_val >= last)
                elseif is_incomplete then
                    should_del = (entity_val <= first)
                else
                    should_del = (entity_val <= first and entity_val >= last)
                end
            else
                if is_first_page then
                    should_del = (entity_val <= last)
                elseif is_incomplete then
                    should_del = (entity_val >= first)
                else
                    should_del = (entity_val >= first and entity_val <= last)
                end
            end
        end
    end

    if should_del then
        redis.call('DEL', page_key)
        redis.call('SREM', KEYS[1], page_key)
        count = count + 1
    end
end

if count == #keys then redis.call('UNLINK', KEYS[1]) end
return count
)lua");

/// KEYS[1] tracking set; ARGV[1] old sort value, ARGV[2] new sort value, ARGV[3]
/// page header size. Same as kInvalidateListGroupSelective for an entity whose sort
/// value moved from old to new. Returns the number of pages deleted.
inline constexpr io::RedisScript kInvalidateListGroupSelectiveUpdate = io::makeRedisScript(R"lua(
local keys = redis.call('SMEMBERS', KEYS[1])
local old_val = tonumber(ARGV[1])
local new_val = tonumber(ARGV[2])
local hdr_size = tonumber(ARGV[3])
local range_min = math.min(old_val, new_val)
local range_max = math.max(old_val, new_val)
local count = 0

local function read_int64(s, off)
    local b1,b2,b3,b4,b5,b6,b7,b8 = string.byte(s, off+1, off+8)
    local val = b1 + b2*256 + b3*65536 + b4*16777216
              + b5*4294967296 + b6*1099511627776
              + b7*281474976710656 + b8*72057594037927936
    if val >= 2^63 then val = val - 2^64 end
    return val
end

for _, page_key in ipairs(keys) do
    local hdr = redis.call('GETRANGE', page_key, 0, hdr_size - 1)
    local should_del = true

    if #hdr >= hdr_size and string.byte(hdr, 1) == 0x53 and string.byte(hdr, 2) == 0x52 then
        local first = read_int64(hdr, 2)
        local last  = read_int64(hdr, 10)
        local flags = string.byte(hdr, 19)
        local is_desc       = (flags % 2) == 1
        local is_first_page = (math.floor(flags / 2) % 2) == 1
        local is_incomplete = (math.floor(flags / 4) % 2) == 1
        local is_offset     = (math.floor(flags / 8) % 2) == 0

        if is_offset then
            local page_min = is_desc and last or first
            local page_max = is_desc and first or last
            if is_incomplete then
                should_del = (page_min <= range_max)
            else
                should_del = (page_min <= range_max) and (range_min <= page_max)
            end
        else
            local function in_range(val)
                if is_first_page and is_incomplete then return true end
                if is_desc then
                    if is_first_page then return val >= last end
                    if is_incomplete then return val <= first end
                    return val <= first and val >= last
                else
                    if is_first_page then return val <= last end
                    if is_incomplete then return val >= first end
                    return val >= first and val <= last
                end
            end
            should_del = in_range(old_val) or in_range(new_val)
        end
    end

    if should_del then
        redis.call('DEL', page_key)
        redis.call('SREM', KEYS[1], page_key)
        count = count + 1
    end
end

if count == #keys then redis.call('UNLINK', KEYS[1]) end
return count
)lua");

// =============================================================================
// Every list group of a repository (master hash: group key -> sort index)
// =============================================================================
//
// Composition order is prologue, fragments, body: the fragments read the
// prologue's locals (`schema`, `n_filters`, `hdr_size`) as upvalues. Placed before
// the prologue, they would read globals, which Redis rejects at run time only.

/// Binary decoding of filter values: u32, int, i64, width, skip, num, skipset, eqval.
inline constexpr std::string_view kLuaDecode = R"lua(
local function u32(s, p)
    local b1,b2,b3,b4 = string.byte(s, p, p+3)
    if not b4 then return 0 end
    return b1 + b2*256 + b3*65536 + b4*16777216
end

-- Little-endian integer of `w` bytes. A negative one is summed from its
-- complement, so it stays exact down to -2^53, as a positive one up to 2^53.
local function int(s, p, w, signed)
    local neg = signed and (string.byte(s, p + w - 1) or 0) >= 128
    local v = 0
    for i = p + w - 1, p, -1 do
        local b = string.byte(s, i)
        if not b then return 0 end
        if neg then b = 255 - b end
        v = v * 256 + b
    end
    if neg then return -v - 1 end
    return v
end

local function i64(s, p) return int(s, p, 8, true) end

-- Byte width of a fixed-size schema type (any type but 's').
local function width(ft)
    if ft == 56 or ft == 85 or ft == 100 then return 8        -- '8' 'U' 'd'
    elseif ft == 52 or ft == 117 or ft == 102 then return 4   -- '4' 'u' 'f'
    elseif ft == 50 or ft == 119 then return 2                -- '2' 'w'
    else return 1 end                                         -- '1'
end

local function skip(s, pos, ft)
    if ft == 115 then return pos + 4 + u32(s, pos) end
    return pos + width(ft)
end

-- Integer of an ordered schema type ('2' 'w' '4' 'u' '8' 'U'), signed or not
-- as the type says. The matcher orders these types only: for '1' 'f' 'd' this
-- returns nil and an ordering filter matches, so the page is invalidated.
local function num(s, p, ft)
    if ft == 49 or ft == 102 or ft == 100 then return nil end
    return int(s, p, width(ft), ft == 56 or ft == 52 or ft == 50)
end

-- Skip a set: [count:u32][elem×count]. pos points at the count.
local function skipset(s, pos, ft)
    local n = u32(s, pos); pos = pos + 4
    for i = 1, n do pos = skip(s, pos, ft) end
    return pos
end

-- Equality of one element at p1 (in s1) against one element at p2 (in s2).
local function eqval(s1, p1, s2, p2, ft)
    if ft == 115 then
        local l1 = u32(s1, p1); local l2 = u32(s2, p2)
        return l1 == l2 and (l1 == 0 or string.sub(s1, p1+4, p1+3+l1) == string.sub(s2, p2+4, p2+3+l2))
    end
    local w = width(ft)
    return string.sub(s1, p1, p1+w-1) == string.sub(s2, p2, p2+w-1)
end
)lua";

/// Entity against group filters: cmpin, cmp, fmatch. Needs `schema`, `n_filters`.
inline constexpr std::string_view kLuaEntityMatch = R"lua(
-- IN membership: entity scalar (at ep) ∈ group set (at gp = count). Mirrors the
-- L3 `= ANY` / L1 ranges::find verdict on the canonicalized set.
local function cmpin(bin, gp, blob, ep, ft)
    local n = u32(bin, gp); gp = gp + 4
    for i = 1, n do
        if eqval(bin, gp, blob, ep, ft) then return true end
        gp = skip(bin, gp, ft)
    end
    return false
end

local function cmp(bin, gp, blob, ep, ft, fo)
    if ft == 115 then
        local gl = u32(bin, gp); local el = u32(blob, ep)
        if fo == 61 then
            return gl == el and (gl == 0 or string.sub(bin, gp+4, gp+3+gl) == string.sub(blob, ep+4, ep+3+el))
        elseif fo == 33 then
            return gl ~= el or (gl > 0 and string.sub(bin, gp+4, gp+3+gl) ~= string.sub(blob, ep+4, ep+3+el))
        end
        return true
    end
    local w = width(ft)
    if fo == 61 then return string.sub(bin, gp, gp+w-1) == string.sub(blob, ep, ep+w-1)
    elseif fo == 33 then return string.sub(bin, gp, gp+w-1) ~= string.sub(blob, ep, ep+w-1) end
    local gv = num(bin, gp, ft)
    if gv == nil then return true end
    local ev = num(blob, ep, ft)
    if fo == 62 then return ev > gv
    elseif fo == 71 then return ev >= gv
    elseif fo == 60 then return ev < gv
    elseif fo == 76 then return ev <= gv end
    return true
end

local function fmatch(bin, blob)
    local gp = 1; local ep = 1
    for f = 0, n_filters - 1 do
        local ft = string.byte(schema, f*2+1)
        local fo = string.byte(schema, f*2+2)
        if gp > #bin or ep > #blob then break end
        local gx = string.byte(bin, gp); local ex = string.byte(blob, ep)
        gp = gp + 1; ep = ep + 1
        local is_set = (fo == 64 or fo == 35)              -- IN (@) or NOT IN (#): same wire format
        if gx == 0 then
            if ex == 1 then ep = skip(blob, ep, ft) end
        elseif ex == 0 then
            -- shared cursor advance: skipset for both set ops, skip otherwise.
            -- IN and NIN cannot diverge on alignment (mitigates risk #1).
            if is_set then gp = skipset(bin, gp, ft) else gp = skip(bin, gp, ft) end
            -- null entity matches neither EQ nor IN nor NOT IN (SQL 3-valued logic)
            if fo == 61 or is_set then return false end
        elseif is_set then
            local hit = cmpin(bin, gp, blob, ep, ft)        -- entity in the group's set?
            -- IN rejects non-members; NIN rejects members (only NIN-specific line)
            if (fo == 64 and not hit) or (fo == 35 and hit) then return false end
            gp = skipset(bin, gp, ft); ep = skip(blob, ep, ft)
        else
            if not cmp(bin, gp, blob, ep, ft, fo) then return false end
            gp = skip(bin, gp, ft); ep = skip(blob, ep, ft)
        end
    end
    return true
end
)lua";

/// Predicate against group filters: setdisjoint, pmatch. Needs `schema`, `n_filters`.
inline constexpr std::string_view kLuaPredicateMatch = R"lua(
-- Two sets (both [count][elem×count]) share no element?
local function setdisjoint(s1, p1, s2, p2, ft)
    local n1 = u32(s1, p1); p1 = p1 + 4
    for i = 1, n1 do
        local q = p2
        local n2 = u32(s2, q); q = q + 4
        for j = 1, n2 do
            if eqval(s1, p1, s2, q, ft) then return false end
            q = skip(s2, q, ft)
        end
        p1 = skip(s1, p1, ft)
    end
    return true
end

-- Predicate↔group compatibility. bin = group filter bytes, blob = predicate
-- filter bytes (same encoding). Prune (return false) only when both present and
-- provably disjoint: EQ values differ, or IN sets share no element. Absent on
-- either side is a wildcard; comparison / NE / NIN never prune.
local function pmatch(bin, blob)
    local gp = 1; local pp = 1
    for f = 0, n_filters - 1 do
        local ft = string.byte(schema, f*2+1)
        local fo = string.byte(schema, f*2+2)
        if gp > #bin or pp > #blob then break end
        local gx = string.byte(bin, gp); local px = string.byte(blob, pp)
        gp = gp + 1; pp = pp + 1
        local is_set = (fo == 64 or fo == 35)
        if px == 0 then
            if gx == 1 then
                if is_set then gp = skipset(bin, gp, ft) else gp = skip(bin, gp, ft) end
            end
        elseif gx == 0 then
            if is_set then pp = skipset(blob, pp, ft) else pp = skip(blob, pp, ft) end
        elseif is_set then
            if fo == 64 and setdisjoint(bin, gp, blob, pp, ft) then return false end
            gp = skipset(bin, gp, ft); pp = skipset(blob, pp, ft)
        elseif fo == 61 then
            if not eqval(bin, gp, blob, pp, ft) then return false end
            gp = skip(bin, gp, ft); pp = skip(blob, pp, ft)
        else
            gp = skip(bin, gp, ft); pp = skip(blob, pp, ft)
        end
    end
    return true
end
)lua";

/// Page header holds one sort value: chk. Needs `hdr_size`.
inline constexpr std::string_view kLuaPagePoint = R"lua(
local function chk(pk, ev)
    local hdr = redis.call('GETRANGE', pk, 0, hdr_size - 1)
    if #hdr < hdr_size or string.byte(hdr, 1) ~= 0x53 or string.byte(hdr, 2) ~= 0x52 then
        return true
    end
    local first = i64(hdr, 3); local last = i64(hdr, 11)
    local fl = string.byte(hdr, 19)
    local desc = (fl % 2) == 1
    local fp = (math.floor(fl / 2) % 2) == 1
    local inc = (math.floor(fl / 4) % 2) == 1
    local off = (math.floor(fl / 8) % 2) == 0
    if off then
        if inc then return true end
        if desc then return ev >= last else return ev <= last end
    else
        if fp or inc then return true end
        if desc then
            return ev <= first and ev >= last
        else
            return ev >= first and ev <= last
        end
    end
end
)lua";

inline constexpr std::string_view kGroupsSelectivePrologue = R"lua(
local master = KEYS[1]
local prefix_len = tonumber(ARGV[1])
local hdr_size = tonumber(ARGV[2])
local schema = ARGV[3]
local eblob = ARGV[4]
local sort_csv = ARGV[5]

local sort_vals = {}
for v in string.gmatch(sort_csv, '([^,]+)') do
    sort_vals[#sort_vals + 1] = tonumber(v)
end
local n_filters = #schema / 2
local total = 0
)lua";
inline constexpr std::string_view kGroupsSelectiveBody = R"lua(
local groups = redis.call('HGETALL', master)
if not groups or #groups == 0 then return 0 end

for gi = 1, #groups, 2 do
    local gk = groups[gi]
    local si = tonumber(groups[gi + 1])
    local bin = string.sub(gk, prefix_len + 1)
    if fmatch(bin, eblob) then
        local ev = sort_vals[si + 1] or 0
        local tk = gk .. ':_keys'
        local pages = redis.call('SMEMBERS', tk)
        if pages and #pages > 0 then
            local c = 0
            for _, pk in ipairs(pages) do
                if chk(pk, ev) then
                    redis.call('DEL', pk)
                    redis.call('SREM', tk, pk)
                    c = c + 1
                end
            end
            if c == #pages then redis.call('UNLINK', tk) end
            total = total + c
        end
    end
end
return total
)lua";

/// KEYS[1] master hash; ARGV[1] group-key prefix length, ARGV[2] page header size,
/// ARGV[3] filter schema, ARGV[4] entity filter blob, ARGV[5] entity sort values
/// (CSV, indexed by sort index). For each group whose filters match the entity
/// (created or deleted), deletes the pages whose sort range may hold its sort value.
/// Returns the number of pages deleted.
inline constexpr io::RedisScript kInvalidateListGroupsSelective = io::makeRedisScript(
    LuaConcat<kGroupsSelectivePrologue, kLuaDecode, kLuaEntityMatch, kLuaPagePoint,
              kGroupsSelectiveBody>::kView);

inline constexpr std::string_view kGroupsByPredicatePrologue = R"lua(
local master = KEYS[1]
local prefix_len = tonumber(ARGV[1])
local hdr_size = tonumber(ARGV[2])
local schema = ARGV[3]
local pblob = ARGV[4]

local lo_vals = {}
for v in string.gmatch(ARGV[5], '([^,]+)') do lo_vals[#lo_vals + 1] = tonumber(v) end
local hi_vals = {}
for v in string.gmatch(ARGV[6], '([^,]+)') do hi_vals[#hi_vals + 1] = tonumber(v) end
local n_filters = #schema / 2
local total = 0
)lua";
inline constexpr std::string_view kGroupsByPredicateBody = R"lua(
-- Range overlap: does [lo,hi] overlap the page's effective sort range? Range
-- generalization of `chk` (the lo==hi case is the per-entity point check).
local function chk_range(pk, lo, hi)
    local hdr = redis.call('GETRANGE', pk, 0, hdr_size - 1)
    if #hdr < hdr_size or string.byte(hdr, 1) ~= 0x53 or string.byte(hdr, 2) ~= 0x52 then
        return true
    end
    local first = i64(hdr, 3); local last = i64(hdr, 11)
    local fl = string.byte(hdr, 19)
    local desc = (fl % 2) == 1
    local fp = (math.floor(fl / 2) % 2) == 1
    local inc = (math.floor(fl / 4) % 2) == 1
    local off = (math.floor(fl / 8) % 2) == 0
    if off then
        if inc then return true end
        if desc then return hi >= last else return lo <= last end
    else
        if fp or inc then return true end
        if desc then
            return lo <= first and hi >= last
        else
            return lo <= last and hi >= first
        end
    end
end

local groups = redis.call('HGETALL', master)
if not groups or #groups == 0 then return 0 end

for gi = 1, #groups, 2 do
    local gk = groups[gi]
    local si = tonumber(groups[gi + 1])
    local bin = string.sub(gk, prefix_len + 1)
    if pmatch(bin, pblob) then
        local lo = lo_vals[si + 1]
        local hi = hi_vals[si + 1]
        if not lo then lo = -9223372036854775808 end
        if not hi then hi = 9223372036854775807 end
        local tk = gk .. ':_keys'
        local pages = redis.call('SMEMBERS', tk)
        if pages and #pages > 0 then
            local c = 0
            for _, pk in ipairs(pages) do
                if chk_range(pk, lo, hi) then
                    redis.call('DEL', pk)
                    redis.call('SREM', tk, pk)
                    c = c + 1
                end
            end
            if c == #pages then redis.call('UNLINK', tk) end
            total = total + c
        end
    end
end
return total
)lua";

/// KEYS[1] master hash; ARGV[1] group-key prefix length, ARGV[2] page header size,
/// ARGV[3] filter schema, ARGV[4] predicate filter blob, ARGV[5] and ARGV[6] lower and
/// upper sort bounds (CSV, indexed by sort index; absent = unbounded). For each group
/// the predicate may overlap, deletes the pages whose sort range overlaps the bounds.
/// Returns the number of pages deleted.
inline constexpr io::RedisScript kInvalidateListGroupsByPredicate = io::makeRedisScript(
    LuaConcat<kGroupsByPredicatePrologue, kLuaDecode, kLuaPredicateMatch,
              kGroupsByPredicateBody>::kView);

inline constexpr std::string_view kGroupsSelectiveUpdatePrologue = R"lua(
local master = KEYS[1]
local prefix_len = tonumber(ARGV[1])
local hdr_size = tonumber(ARGV[2])
local schema = ARGV[3]
local new_blob = ARGV[4]
local new_csv = ARGV[5]
local old_blob = ARGV[6]
local old_csv = ARGV[7]

local new_sv = {}
for v in string.gmatch(new_csv, '([^,]+)') do new_sv[#new_sv+1] = tonumber(v) end
local old_sv = {}
for v in string.gmatch(old_csv, '([^,]+)') do old_sv[#old_sv+1] = tonumber(v) end
local n_filters = #schema / 2
local total = 0
)lua";
inline constexpr std::string_view kGroupsSelectiveUpdateBody = R"lua(
local function chk_range(pk, v1, v2)
    local hdr = redis.call('GETRANGE', pk, 0, hdr_size - 1)
    if #hdr < hdr_size or string.byte(hdr, 1) ~= 0x53 or string.byte(hdr, 2) ~= 0x52 then
        return true
    end
    local first = i64(hdr, 3); local last = i64(hdr, 11)
    local fl = string.byte(hdr, 19)
    local desc = (fl % 2) == 1
    local fp = (math.floor(fl / 2) % 2) == 1
    local inc = (math.floor(fl / 4) % 2) == 1
    local off = (math.floor(fl / 8) % 2) == 0
    local rmin = math.min(v1, v2)
    local rmax = math.max(v1, v2)
    if off then
        local pmin = desc and last or first
        local pmax = desc and first or last
        if inc then return pmin <= rmax end
        return (pmin <= rmax) and (rmin <= pmax)
    else
        local function inr(val)
            if fp or inc then return true end
            if desc then return val <= first and val >= last
            else return val >= first and val <= last end
        end
        return inr(v1) or inr(v2)
    end
end

local groups = redis.call('HGETALL', master)
if not groups or #groups == 0 then return 0 end

for gi = 1, #groups, 2 do
    local gk = groups[gi]
    local si = tonumber(groups[gi + 1])
    local bin = string.sub(gk, prefix_len + 1)
    local nm = fmatch(bin, new_blob)
    local om = fmatch(bin, old_blob)
    if nm or om then
        local nv = new_sv[si + 1] or 0
        local ov = old_sv[si + 1] or 0
        local tk = gk .. ':_keys'
        local pages = redis.call('SMEMBERS', tk)
        if pages and #pages > 0 then
            local c = 0
            for _, pk in ipairs(pages) do
                local del = false
                if nm and om then
                    if nv == ov then del = chk(pk, nv)
                    else del = chk_range(pk, ov, nv) end
                elseif nm then del = chk(pk, nv)
                else del = chk(pk, ov) end
                if del then
                    redis.call('DEL', pk)
                    redis.call('SREM', tk, pk)
                    c = c + 1
                end
            end
            if c == #pages then redis.call('UNLINK', tk) end
            total = total + c
        end
    end
end
return total
)lua";

/// KEYS[1] master hash; ARGV[1] group-key prefix length, ARGV[2] page header size,
/// ARGV[3] filter schema, ARGV[4] new entity filter blob, ARGV[5] new sort values,
/// ARGV[6] old entity filter blob, ARGV[7] old sort values (CSV, indexed by sort
/// index). For each group matching the old or the new entity, deletes the pages
/// the move may affect: those that may hold the old or the new sort value and,
/// when both match, the offset pages between them.
/// Returns the number of pages deleted.
inline constexpr io::RedisScript kInvalidateListGroupsSelectiveUpdate = io::makeRedisScript(
    LuaConcat<kGroupsSelectiveUpdatePrologue, kLuaDecode, kLuaEntityMatch, kLuaPagePoint,
              kGroupsSelectiveUpdateBody>::kView);

// =============================================================================
// Registry
// =============================================================================

inline constexpr std::array kAllScripts{
    &kBumpGenMany,
    &kSetIfGen,
    &kSetManyIfGen,
    &kListStore,
    &kInvalidateListGroup,
    &kInvalidateListGroupSelective,
    &kInvalidateListGroupSelectiveUpdate,
    &kInvalidateListGroupsSelective,
    &kInvalidateListGroupsByPredicate,
    &kInvalidateListGroupsSelectiveUpdate,
};

consteval bool allDigestsDistinct() {
    for (std::size_t i = 0; i < kAllScripts.size(); ++i)
        for (std::size_t j = i + 1; j < kAllScripts.size(); ++j)
            if (kAllScripts[i]->sha1() == kAllScripts[j]->sha1()) return false;
    return true;
}
static_assert(allDigestsDistinct(), "two Lua scripts share a digest");

}  // namespace jcailloux::relais::cache::scripts

#endif  // JCX_RELAIS_REDISSCRIPTS_H
