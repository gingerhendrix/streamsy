# platform-matrix

One Streamsy app that runs on Bun, Node and Deno, with one storage backend for
each path prefix. The runner starts all three runtimes and checks every
`runtime × backend` pair with the official Durable Streams conformance suite
and the memory-server HTTP smoke.

## Layout

```text
src/backends.ts       storage Layers from config: memory, sqlite
src/app.ts            one web handler per mounted backend, picked by the first path segment
src/server.ts         Serve.layer over the app, port from PORT
src/bun.ts            Bun entry (SQLite on bun:sqlite)
src/node.ts           Node entry (SQLite on node:sqlite)
src/deno.ts           Deno entry (SQLite on node:sqlite with nodeSqliteCompat)
scripts/run-matrix.ts the runner
```

Each backend has its own storage graph. A stream made under `/memory` does not
exist under `/sqlite`.

## Run the matrix

Node and Deno load the built `@streamsy/*` packages, so build first:

```bash
bun install
bun run build
bun run --cwd examples/platform-matrix matrix
```

The runner needs `node` on `PATH` for the Node row. For the Deno row it uses
`deno` on `PATH`, or else `mise exec deno@2 -- deno`. A runtime that it cannot
find is a row of skips.

The runner prints a table like this one, and exits non-zero when any cell
fails:

```text
| runtime | memory                                   | sqlite                                   | pg   | mysql |
| ------- | ---------------------------------------- | ---------------------------------------- | ---- | ----- |
| bun     | pass (332 passed, 6 skipped; smoke pass) | pass (332 passed, 6 skipped; smoke pass) | skip | skip  |
| node    | pass (332 passed, 6 skipped; smoke pass) | pass (332 passed, 6 skipped; smoke pass) | skip | skip  |
| deno    | pass (332 passed, 6 skipped; smoke pass) | pass (332 passed, 6 skipped; smoke pass) | skip | skip  |
```

It writes `result.json`, each entry's server log and each check's raw log to
`.matrix/<timestamp>/` in this folder. Git ignores `.matrix/`.

| Variable           | Default                            | Use                                                               |
| ------------------ | ---------------------------------- | ----------------------------------------------------------------- |
| `MATRIX_BASE_PORT` | `4101`                             | Bun listens on this port, Node on the next, Deno on the one after |
| `MATRIX_OUT`       | `examples/platform-matrix/.matrix` | Parent folder of the dated result folder                          |

## URLs

| Runtime | memory                         | sqlite                         |
| ------- | ------------------------------ | ------------------------------ |
| Bun     | `http://127.0.0.1:4101/memory` | `http://127.0.0.1:4101/sqlite` |
| Node    | `http://127.0.0.1:4102/memory` | `http://127.0.0.1:4102/sqlite` |
| Deno    | `http://127.0.0.1:4103/memory` | `http://127.0.0.1:4103/sqlite` |

`GET /healthz` on each port names the runtime and the mounted backends:

```json
{ "runtime": "bun", "backends": ["memory", "sqlite"], "unmounted": {} }
```

Any other first path segment answers 404.

## How a cell becomes skip

The runner checks the backends `memory`, `sqlite`, `pg` and `mysql` on each
runtime. It reads `/healthz` and runs only the backends that the entry mounted.

| Backend       | Mounted when                                                                         | Skip reason                        |
| ------------- | ------------------------------------------------------------------------------------ | ---------------------------------- |
| `memory`      | always                                                                               |                                    |
| `sqlite`      | `SQLITE_PATH` is set. The runner sets it to `<runtime>.sqlite` in the result folder. | `SQLITE_PATH is not set`           |
| `pg`, `mysql` | not in this app yet                                                                  | `the app has no <backend> backend` |

A skip does not fail the run.

## Run one entry

```bash
PORT=4101 SQLITE_PATH=./bun.sqlite bun src/bun.ts
PORT=4102 SQLITE_PATH=./node.sqlite node src/node.ts
PORT=4103 SQLITE_PATH=./deno.sqlite deno run --allow-all --no-lock src/deno.ts
```

`LONG_POLL_TIMEOUT_MS` sets the long-poll timeout of every backend. The runner
sets it to 1500, so the suite does not wait 30 seconds for each timeout.

Then point the suite or the smoke at one base URL:

```bash
SERVER_BASE_URL=http://127.0.0.1:4101/sqlite bun run --cwd packages/conformance-tests test:deployed
BASE_URL=http://127.0.0.1:4101/sqlite bun run --cwd examples/memory-server smoke:http
```
