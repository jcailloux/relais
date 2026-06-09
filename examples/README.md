# Examples

Runnable, CI-compiled counterparts to the snippets in
[`docs/runtime-and-threading.md`](../docs/runtime-and-threading.md). Off by
default; enable with `-DRELAIS_BUILD_EXAMPLES=ON`.

| Example | Needs DB? | Shows |
|---|---|---|
| `event_loop_basics.cpp` | no | An `EpollIoContext` loop on its own thread, a coroutine driven on it, the `spawnOn` cross-thread bridge, a `postDelayed` timer — the machinery every runtime is built from |
| `iopool_nloop.cpp` | yes (PostgreSQL + Redis) | `IoPool`, the built-in shared-nothing N-loop runtime: each worker binds its own pool and routes queries on its own thread (distinct PG backends → shared-nothing) |

```bash
cmake -B .build/dev -DRELAIS_BUILD_EXAMPLES=ON
cmake --build .build/dev --target example_event_loop_basics
./.build/dev/example_event_loop_basics            # no database needed

# iopool_nloop reads PG* env (libpq):
PGHOST=localhost PGDATABASE=relais_test PGUSER=relais_test PGPASSWORD=relais_test \
  ./.build/dev/example_iopool_nloop
```