# Concepts — how relais fits together

The mental model behind relais, in one pass. It explains *what the pieces are
and why they connect the way they do* — no signatures (those live in
[api-reference.md](api-reference.md)), no implementation depth (that is
[internals.md](internals.md)).

Five ideas carry the whole library:

1. [Entity / mapping decoupling](#1--entity--mapping-decoupling) — a pure struct, a generated mapping, one wrapper at the API edge.
2. [The compile-time mixin tower](#2--the-compile-time-mixin-tower) — the repository is assembled from your config; what you don't enable isn't there.
3. [The read / write flow](#3--the-read--write-flow) — L1 → L2 → L3 on reads; invalidate-then-write on writes.
4. [The shared-nothing runtime](#4--the-shared-nothing-runtime) — one event loop per thread, no cross-thread hops.
5. [Type-safety by concepts](#5--type-safety-by-concepts) — capability is a compile-time property; misuse doesn't compile.

A closing [performance model](#performance-model) ties the three speed levers together.

---

## 1 — Entity / mapping decoupling

relais splits an entity into three layers, each owned by a different concern:

- **The struct** — a plain, framework-agnostic C++ aggregate (`struct User { int64_t id; std::string email; … };`). It knows nothing about SQL, caching, or relais. It is yours to share across your codebase, serialize, or pass to unrelated code.
- **The mapping** — generated from `@relais` annotations on the struct. It holds everything the struct deliberately doesn't: the table name, the SQL row↔struct conversions, the key extractor, field metadata, and (optionally) a list descriptor. You never write it by hand — the generator emits `{Name}Entity.h`.
- **`Entity<Struct, Mapping>`** — the wrapper that appears at the API boundary. It *inherits* the struct (so it is still your data) and bolts on key access, row mapping, and on-demand JSON/BEVE serialization by delegating to the mapping.

Why the split: one struct definition serves your domain logic, wire format, and cache without pulling the ORM into every translation unit; the generated code stays isolated in the mapping, and the only relais-aware type appears exactly where you talk to a repository.

> Write the struct and run the generator → [entities.md](entities.md). The exact
> `Entity<Struct, Mapping>` surface → [api-reference.md › Entity and concepts](api-reference.md#entity-and-concepts).

---

## 2 — The compile-time mixin tower

A repository is not one class — it is a stack of layers, each adding one tier or
one behavior, assembled by the compiler from your `CacheConfig` and the entity's
traits:

```
         Repo<Entity, "Name", Cfg, Invalidations...>   ← the type you name
                          │
   InvalidationMixin   ← only if Invalidations... is non-empty
                          │
        ListMixin      ← only if the entity has a ListDescriptor
                          │
        LocalRepo      ← L1 (RAM)   — only if Cfg selects L1 or L1_L2
                          │
        RedisRepo      ← L2 (Redis) — only if Cfg selects L2 or L1_L2
                          │
         PgRepo        ← L3 (PostgreSQL) — always present
```

Each layer defines the same method names as the one below it and calls down with
`Base::method(...)`. This is plain C++ name-hiding — **no virtual functions, no
CRTP**. A `find` on the top type resolves, at compile time, to the right chain of
calls; the optimizer sees straight through it.

The consequence is the single most important property of the library:

> **What you don't configure doesn't exist in the binary.** A `config::Local`
> repo has no `RedisRepo` layer — there is no dead Redis branch to skip at
> runtime, because the code was never generated. Add `Invalidate...` descriptors
> and the `InvalidationMixin` appears; declare a `ListDescriptor` and list
> caching appears. You pay, in code size and in cycles, for exactly the tiers and
> features you turned on.

The presets name the common towers: `Uncached` (PgRepo only), `Local` (+ L1),
`Redis` (+ L2), `Both` (L1 + L2).

> The full layer-by-layer assembly is in
> [internals.md](internals.md); the presets and every `CacheConfig` field are in
> [api-reference.md › CacheConfig](api-reference.md#cacheconfig).

---

## 3 — The read / write flow

**Reads climb the tower.** A `find` checks the fastest tier first and falls
through on a miss, back-filling each tier it passed on the way up:

```
L1 hit  → return                              (in-process RAM, no I/O)
L1 miss → L2 hit  → store into L1 → return    (Redis round-trip)
       └→ L2 miss → DB query → store L2 → store L1 → return
```

A hit on L1 is a synchronous, frameless lookup; only a real L2/L3 miss does
asynchronous I/O. On a miss the fetched value is stored back into the tiers it
passed — awaited before the read returns, with a recheck that drops the store if a
concurrent write invalidated the key mid-fetch (so a back-fill is never stale).
(Batched warm-fills, on the multi-key path, are instead detached fire-and-forget.)

**Writes go the other way: invalidate, then commit.** A mutation commits to
PostgreSQL, then *evicts* the affected entries from L2 and L1 (and bumps the list
caches) rather than overwriting them. The next read re-fetches and re-fills. This
"invalidate-then-reload" is the safe default (`UpdateStrategy::InvalidateAndLazyReload`)
— it can never leave a stale value cached, because it leaves *nothing* cached.
(An optimistic `PopulateImmediately` write-through exists for read-after-write-heavy
paths; see [api-reference.md › CacheConfig](api-reference.md#cacheconfig).)

**Staleness is bounded by timing, never by scope.** Cross-target and
cross-instance invalidation propagate detached (fire-and-forget), so a dependent
cache can lag the source by a few milliseconds — but relais never over-invalidates:
eviction is always *point-targeted* at the exact affected keys (never `purgeAll`),
so a write to one entity never flushes unrelated hot entries.

> The eviction model, partition hints, and `patch` semantics →
> [caching.md](caching.md). Cross-entity and cross-list invalidation →
> [invalidation.md](invalidation.md).

---

## 4 — The shared-nothing runtime

Because a real miss does asynchronous I/O, repository calls don't touch
PostgreSQL or Redis on the calling thread — they run **on an event loop** and
route through `thread_local` providers (each owning that loop's connection pools).
The runtime model is shared-nothing:

- **One event loop per worker thread**, typically pinned to a core. N workers = N
  independent loops.
- **Each loop owns its own resources** — its PostgreSQL pool, its Redis pool, its
  batch scheduler, bound to that thread's `thread_local` providers.
- **A request stays on its loop, end to end.** No locks on the hot path, no
  cross-thread handoff between the coroutine and its I/O. Throughput scales
  ~linearly with cores at unchanged per-request latency.

```
   core 1            core 2            core 3
  ┌───────┐         ┌───────┐         ┌───────┐
  │ loop  │         │ loop  │         │ loop  │     each: own PG pool,
  │  +PG  │         │  +PG  │         │  +PG  │      Redis pool, scheduler
  │ +Redis│         │ +Redis│         │ +Redis│      — nothing shared
  └───────┘         └───────┘         └───────┘
```

The one rule that follows from this: **a repository call must run on a loop
thread, and the providers must be bound on that thread.** The bundled `IoPool`
does this for you — it spins up N loops and binds each one's providers during
construction. If you bring your own event loop (a web framework already owns the
threads your requests run on), you bind the providers once per loop yourself and
relais runs inline on it via an `IoContext` adapter.

> Stand up the runtime and the binding rule → [runtime.md](runtime.md).
> Co-locating relais on a foreign loop → [foreign-event-loops.md](foreign-event-loops.md).
> `IoPool`, `Task`, and the `IoContext` concept →
> [api-reference.md › Runtime and I/O](api-reference.md#runtime-and-io).

---

## 5 — Type-safety by concepts

What a repository *can do* is a property of its entity, checked at compile time.
A hierarchy of C++20 concepts gates each method with a `requires` clause, so an
operation the entity doesn't support is **absent from the API** — not a runtime
error, not a failed instantiation deep in a template, but a method that simply
isn't there to call.

The concepts branch off a common root by capability — caching and mutation are
**independent** branches, not a single line:

```
                   ┌─ CacheableEntity   (+ serializable → the cache layers need it)
   ReadableEntity ─┤
   (read from DB)  └─ MutableEntity ──── CreatableEntity
                      (+ writable:         (+ a key: full
                       insert/update)       insert with cache populate)
```

So a repo can be cacheable without being mutable (a read-only cached view) or
mutable without being cacheable (`Uncached` writes) — each capability is gated
on its own. Plus orthogonal capability flags: a `ListDescriptor` unlocks list queries, a
`FilterSet` unlocks the predicate `eraseWhere`/`invalidateWhere`, a field-update
trait unlocks `patch`, a partition hint unlocks single-partition deletes.

The practical upshot:

- Writing to a `read_only` repo, or to one whose entity has no updatable column,
  is a compile error — the write methods don't exist on that type.
- `patch(id, set<Field::x>(…))` only compiles when the entity exposes a `Field`
  enum, and `setNull<F>()` only when `F` is actually nullable.
- A list cursor minted for one entity cannot be passed to another's query — the
  cursor is phantom-typed to its descriptor.

> What each concept requires and what it unlocks →
> [api-reference.md › Entity and concepts](api-reference.md#entity-and-concepts).

---

## Performance model

Speed in relais comes from three independent choices, in order of impact. The
first two are the built-in path; the third is an advanced opt-in.

1. **The right cache preset.** An L1 hit is an in-process sharded shardmap lookup
   (~50 ns, no syscall, no I/O, no thread hop); only real L2/L3 misses do async I/O. Choosing the
   tier per entity (`Local`/`Both` for hot reads, `Redis` for cross-instance
   shared data) is the largest lever — see [§2](#2--the-compile-time-mixin-tower)
   and [§3](#3--the-read--write-flow).
2. **The shared-nothing runtime.** N loops, one per core, each self-contained;
   a request never hops threads, so throughput scales ~linearly with cores at
   unchanged latency. This is the `IoPool` default and reaches the L1-hit latency
   above on its own — see [§4](#4--the-shared-nothing-runtime).
3. **Co-location on a foreign loop *(optional)*.** Relevant only when a web
   framework (Drogon/asio/libuv) already owns the loops your requests run on:
   bridging each call to a separate `IoPool` then costs a ~3 µs thread hop, even
   on an L1 hit. Running relais inline on those existing loops removes it, via a
   small [`IoContext` adapter](foreign-event-loops.md). If relais drives your
   runtime, you don't need this.

---

> **Next:** to *use* relais, go to [api-reference.md](api-reference.md) (every
> signature) or a task guide ([entities](entities.md), [caching](caching.md),
> [lists](lists.md), [invalidation](invalidation.md), [runtime](runtime.md)). To
> *dissect* it, go to [internals.md](internals.md).
