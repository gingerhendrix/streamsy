# platform-matrix

One Streamsy app that runs on Bun, Node and Deno, with one storage backend for
each path prefix. The runner starts all three runtimes and checks every
`runtime × backend` pair with the official Durable Streams conformance suite
and the memory-server HTTP smoke. On Bun, the Postgres and MySQL cells also run
the Bugsy HTTP smoke.

## Layout

```text
src/backends.ts       storage Layers from config: memory, sqlite, pg, mysql
src/app.ts            one web handler per mounted backend, picked by the first path segment
src/server.ts         Serve.layer over the app, port from PORT
src/bun.ts            Bun entry (SQLite on bun:sqlite)
src/node.ts           Node entry (SQLite on node:sqlite)
src/deno.ts           Deno entry (SQLite on node:sqlite with nodeSqliteCompat)
scripts/run-matrix.ts the runner
```

Each backend has its own storage graph. A stream made under `/memory` does not
exist under `/sqlite`. Postgres runs on `@effect/sql-pg` and MySQL on
`@effect/sql-mysql2`. Both drivers are pure JS, so every runtime uses the same
ones.

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

With no database URL set, the runner prints a table like this one. It exits
non-zero when any cell fails:

```text
| runtime | memory                                   | sqlite                                   | pg   | mysql |
| ------- | ---------------------------------------- | ---------------------------------------- | ---- | ----- |
| bun     | pass (332 passed, 6 skipped; smoke pass) | pass (332 passed, 6 skipped; smoke pass) | skip | skip  |
| node    | pass (332 passed, 6 skipped; smoke pass) | pass (332 passed, 6 skipped; smoke pass) | skip | skip  |
| deno    | pass (332 passed, 6 skipped; smoke pass) | pass (332 passed, 6 skipped; smoke pass) | skip | skip  |
```

It writes `result.json`, each entry's server log and each check's raw log to
`.matrix/<timestamp>/` in this folder. Git ignores `.matrix/`.

| Variable             | Default                            | Use                                                               |
| -------------------- | ---------------------------------- | ----------------------------------------------------------------- |
| `MATRIX_BASE_PORT`   | `4101`                             | Bun listens on this port, Node on the next, Deno on the one after |
| `MATRIX_OUT`         | `examples/platform-matrix/.matrix` | Parent folder of the dated result folder                          |
| `STREAMSY_PG_URL`    | unset                              | Postgres server for the `pg` cells; unset skips them              |
| `STREAMSY_MYSQL_URL` | unset                              | MySQL server for the `mysql` cells; unset skips them              |

## Postgres and MySQL cells

The `pg` and `mysql` cells need a database server. The repo's `compose.yaml`
runs Postgres 17 on port 54329 and MySQL 8.4 on port 33069:

```bash
bun run db:up
export STREAMSY_PG_URL=postgres://streamsy:streamsy@127.0.0.1:54329/streamsy
export STREAMSY_MYSQL_URL=mysql://streamsy:streamsy@127.0.0.1:33069/streamsy
bun run --cwd examples/platform-matrix matrix
bun run db:down -v
```

Before any entry starts, the runner connects to each set URL and runs
`SELECT 1`. When a set URL does not answer, the run fails at once with the
URL and the driver error, for example:

```text
DatabaseError: STREAMSY_PG_URL is set, but postgres://streamsy:***@127.0.0.1:54399/streamsy does not answer: PgConnection: Failed to connect: connect ECONNREFUSED 127.0.0.1:54399
```

Each runtime gets its own fresh database on each server, named
`streamsy_matrix_<runtime>_<run id>`. The run id comes from the result folder
name. The runner passes the URL of that database to that runtime's entry only,
so no two cells share streams or migrations. It drops the databases when the
runtime's cells end, also when a cell fails or you press Ctrl-C. The user in
the URL needs the right to create and drop databases. The compose MySQL user
may do so for databases named `streamsy_%`.

### Bugsy smoke

On the Bun row, each `pg` and `mysql` cell runs a third check: the
Bugsy HTTP smoke (`examples/bugsy`, `bun run smoke:http`). Bugsy
has only a Bun entry, so the Node and Deno rows do not run it.
The smoke starts its own Bugsy server, sends a command, restarts the
server on the same database, and checks that the checkpoint and the row
resumed. A failed smoke fails the cell:

```text
| bun | ... | pass (332 passed, 6 skipped; smoke pass; bugsy pass) | pass (332 passed, 6 skipped; smoke pass; bugsy pass) |
```

The smoke gets its own fresh database, `streamsy_matrix_issues_<run id>`, and
only that server's URL: the runner unsets the other dialect's variable, because
Bugsy refuses both. The runner drops the database after the smoke.
The table notes name the database, `result.json` has a `bugsy` record
in the cell, and the raw output is in `bun-<backend>.bugsy.log`.

The runner never prints a password. The logs, the table notes and
`result.json` show each database URL with the password as `***`.

## URLs

| Runtime | memory                         | sqlite                         | pg                         | mysql                         |
| ------- | ------------------------------ | ------------------------------ | -------------------------- | ----------------------------- |
| Bun     | `http://127.0.0.1:4101/memory` | `http://127.0.0.1:4101/sqlite` | `http://127.0.0.1:4101/pg` | `http://127.0.0.1:4101/mysql` |
| Node    | `http://127.0.0.1:4102/memory` | `http://127.0.0.1:4102/sqlite` | `http://127.0.0.1:4102/pg` | `http://127.0.0.1:4102/mysql` |
| Deno    | `http://127.0.0.1:4103/memory` | `http://127.0.0.1:4103/sqlite` | `http://127.0.0.1:4103/pg` | `http://127.0.0.1:4103/mysql` |

`GET /healthz` on each port names the runtime and the mounted backends:

```json
{ "runtime": "bun", "backends": ["memory", "sqlite"], "unmounted": {} }
```

Any other first path segment answers 404.

## How a cell becomes skip

The runner checks the backends `memory`, `sqlite`, `pg` and `mysql` on each
runtime. It reads `/healthz` and runs only the backends that the entry mounted.

| Backend  | Mounted when                                                                         | Skip reason                     |
| -------- | ------------------------------------------------------------------------------------ | ------------------------------- |
| `memory` | always                                                                               |                                 |
| `sqlite` | `SQLITE_PATH` is set. The runner sets it to `<runtime>.sqlite` in the result folder. | `SQLITE_PATH is not set`        |
| `pg`     | `STREAMSY_PG_URL` is set. The runner sets it to the runtime's fresh database.        | `STREAMSY_PG_URL is not set`    |
| `mysql`  | `STREAMSY_MYSQL_URL` is set. The runner sets it to the runtime's fresh database.     | `STREAMSY_MYSQL_URL is not set` |

A skip does not fail the run.

## Run one entry

```bash
PORT=4101 SQLITE_PATH=./bun.sqlite bun src/bun.ts
PORT=4102 SQLITE_PATH=./node.sqlite node src/node.ts
PORT=4103 SQLITE_PATH=./deno.sqlite deno run --allow-all --no-lock src/deno.ts
```

Set `STREAMSY_PG_URL` or `STREAMSY_MYSQL_URL` too, to mount `/pg` or `/mysql`.
The entry migrates the database in the URL, so give it a database of its own.
`LONG_POLL_TIMEOUT_MS` sets the long-poll timeout of every backend. The runner
sets it to 1500, so the suite does not wait 30 seconds for each timeout.

Then point the suite or the smoke at one base URL:

```bash
SERVER_BASE_URL=http://127.0.0.1:4101/sqlite bun run --cwd packages/conformance-tests test:deployed
BASE_URL=http://127.0.0.1:4101/sqlite bun run --cwd examples/memory-server smoke:http
```
