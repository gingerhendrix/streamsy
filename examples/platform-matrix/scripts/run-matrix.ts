/**
 * Runs the platform matrix: starts the Bun, Node and Deno entries, then runs
 * the official conformance suite and the memory-server HTTP smoke against
 * every `runtime × backend` URL. Prints a pass, fail or skip table and writes
 * `result.json` and one raw log for each check to `.matrix/<timestamp>/`.
 *
 * A backend that an entry does not mount is a skip. Any failed cell, or an
 * entry that does not start, exits non-zero.
 *
 * When `STREAMSY_PG_URL` or `STREAMSY_MYSQL_URL` is set, the runner checks
 * that the database answers before any entry starts, and fails the run when it
 * does not. Each runtime then gets its own fresh database on that server,
 * named `streamsy_matrix_<runtime>_<run id>`. The runner drops it when the
 * runtime's cells end, also on failure and Ctrl-C. Logs, the table and
 * `result.json` show database URLs with the password redacted.
 *
 * On the Bun row, each pg and mysql cell also runs the Bugsy HTTP
 * smoke, in its own fresh database `streamsy_matrix_issues_<run id>`, dropped
 * after the smoke. Bugsy has only a Bun entry.
 *
 * env: MATRIX_BASE_PORT (default 4101; Bun, Node and Deno take three ports
 * from it), MATRIX_OUT (default `<package>/.matrix`), STREAMSY_PG_URL,
 * STREAMSY_MYSQL_URL.
 */
import { stripVTControlCharacters } from "node:util";
import { BunRuntime, BunServices } from "@effect/platform-bun";
import { MysqlClient } from "@effect/sql-mysql2";
import { PgClient } from "@effect/sql-pg";
import { SqlStorage } from "@streamsy/storage";
import {
  Cause,
  Config,
  Console,
  Context,
  DateTime,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Path,
  Redacted,
  Schedule,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import type { Reactivity } from "effect/reactivity";
import { SqlClient, type SqlError } from "effect/sql";

/** The backends of the plan, in table order. */
const BACKENDS = ["memory", "sqlite", "pg", "mysql"] as const;

type Status = "pass" | "fail" | "skip";

interface Counts {
  readonly passed: number;
  readonly failed: number;
  readonly skipped: number;
}

interface Check {
  readonly status: "pass" | "fail";
  readonly exitCode: number | null;
  readonly log: string;
}

interface Cell {
  readonly runtime: string;
  readonly backend: string;
  readonly status: Status;
  readonly baseUrl?: string;
  /** The fresh database of a pg or mysql cell, with the password redacted. */
  readonly database?: string | undefined;
  readonly reason?: string;
  /** The suite counts, or null when the output has no vitest summary. */
  readonly conformance?: Check & { readonly counts: Counts | null };
  readonly smoke?: Check;
  /** The Bugsy smoke and its own database, on the Bun pg and mysql cells. */
  readonly bugsy?: (Check & { readonly database: string }) | undefined;
}

interface Runtime {
  readonly name: "bun" | "node" | "deno";
  readonly port: number;
  /** The entry command, or none when the runtime binary is missing. */
  readonly command: Option.Option<ReadonlyArray<string>>;
}

const Health = Schema.Struct({
  runtime: Schema.String,
  backends: Schema.Array(Schema.String),
  unmounted: Schema.Record(Schema.String, Schema.String),
});

/** A SQL server backend of the matrix, and the env variable with its base URL. */
interface Dialect {
  readonly backend: "pg" | "mysql";
  readonly variable: string;
  readonly client: (
    url: Redacted.Redacted,
  ) => Effect.Effect<SqlClient.SqlClient, SqlError.SqlError, Scope.Scope | Reactivity.Reactivity>;
  readonly dropDatabase: (name: string) => string;
}

const DIALECTS: ReadonlyArray<Dialect> = [
  {
    backend: "pg",
    variable: "STREAMSY_PG_URL",
    client: (url) => PgClient.make({ url, maxConnections: 1 }),
    // FORCE ends any session that an entry left open.
    dropDatabase: (name) => `DROP DATABASE IF EXISTS ${name} WITH (FORCE)`,
  },
  {
    backend: "mysql",
    variable: "STREAMSY_MYSQL_URL",
    client: (url) => MysqlClient.make({ url, maxConnections: 1 }),
    dropDatabase: (name) => `DROP DATABASE IF EXISTS ${name}`,
  },
];

/** The runtime that runs the Bugsy smoke. Bugsy has only a Bun entry. */
const BUGSY_RUNTIME: Runtime["name"] = "bun";

/** A dialect whose URL is set, with an open admin connection to its base database. */
interface Server {
  readonly dialect: Dialect;
  readonly url: Redacted.Redacted;
  readonly admin: SqlClient.SqlClient;
}

/** A database made for one runtime or check. `url` holds the password; `shown` does not. */
interface Database {
  readonly dialect: Dialect;
  readonly url: string;
  readonly shown: string;
}

/** A URL with its password replaced by `***`, for logs, the table and the JSON. */
const redactUrl = (url: string): string => {
  if (!URL.canParse(url)) return "<not a URL>";
  const parsed = new URL(url);
  if (parsed.password !== "") parsed.password = "***";
  return parsed.toString();
};

/** The same server URL with another database name as its path. */
const withDatabase = (url: string, name: string): string => {
  const parsed = new URL(url);
  parsed.pathname = `/${name}`;
  return parsed.toString();
};

/** The SQL message and the driver message, for example `...: connect ECONNREFUSED`. */
const errorMessage = (error: SqlError.SqlError): string => {
  const driver = error.cause.cause;
  const detail = driver instanceof Error && driver.message !== "" ? `: ${driver.message}` : "";
  return `${error.message}${detail}`;
};

class DatabaseError extends Schema.TaggedError<DatabaseError>()("DatabaseError", {
  message: Schema.String,
}) {}

class EntryStartError extends Schema.TaggedError<EntryStartError>()("EntryStartError", {
  message: Schema.String,
}) {}

const MatrixConfig = Config.all({
  basePort: Config.Port("MATRIX_BASE_PORT").pipe(Config.withDefault(4101)),
  out: Config.String("MATRIX_OUT").pipe(Config.option),
  pg: Config.Redacted("STREAMSY_PG_URL").pipe(Config.option),
  mysql: Config.Redacted("STREAMSY_MYSQL_URL").pipe(Config.option),
});

const which = (binary: string) => Option.fromNullishOr(Bun.which(binary));

/** Deno on `PATH` (CI), else a Deno 2 through mise (local), else none. */
const denoCommand = (args: ReadonlyArray<string>) =>
  Option.match(which("deno"), {
    onSome: () => Option.some(["deno", ...args]),
    onNone: () =>
      Option.map(which("mise"), () => ["mise", "exec", "deno@2", "--", "deno", ...args]),
  });

const runtimes = (basePort: number): ReadonlyArray<Runtime> => [
  { name: "bun", port: basePort, command: Option.some(["bun", "src/bun.ts"]) },
  {
    name: "node",
    port: basePort + 1,
    command: Option.map(which("node"), () => ["node", "src/node.ts"]),
  },
  {
    name: "deno",
    port: basePort + 2,
    command: denoCommand(["run", "--allow-all", "--no-lock", "src/deno.ts"]),
  },
];

/** Reads the vitest summary line, for example `Tests  332 passed | 6 skipped (338)`. */
const parseCounts = (output: string): Counts | null => {
  const line = /^\s*Tests\s+(.+)$/m.exec(stripVTControlCharacters(output))?.[1];
  if (line === undefined) return null;
  const count = (label: string) => Number(new RegExp(`(\\d+) ${label}`).exec(line)?.[1] ?? 0);
  return { passed: count("passed"), failed: count("failed"), skipped: count("skipped") };
};

const skip = (runtime: Runtime, backend: string, reason: string): Cell => ({
  runtime: runtime.name,
  backend,
  status: "skip",
  reason,
});

const fail = (runtime: Runtime, backend: string, reason: string): Cell => ({
  runtime: runtime.name,
  backend,
  status: "fail",
  reason,
});

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const config = yield* MatrixConfig;

  const packageDir = path.resolve(import.meta.dir, "..");
  const repoDir = path.resolve(packageDir, "../..");
  const stamp = DateTime.formatIso(yield* DateTime.now).replace(/[:.]/g, "-");
  const outDir = path.join(
    Option.getOrElse(config.out, () => path.join(packageDir, ".matrix")),
    stamp,
  );
  yield* fs.makeDirectory(outDir, { recursive: true });
  yield* Console.log(`platform matrix: writing results to ${outDir}`);
  // A database name part from the result folder name, for example `20261002t104535346z`.
  const runId = stamp.toLowerCase().replace(/[^a-z0-9]/g, "");

  /**
   * Opens an admin connection to a set URL and checks that the server answers
   * (plan 3.2 step 1). The connection stays open for the run, to make and drop
   * the per-runtime databases.
   */
  const connect = (dialect: Dialect, url: Redacted.Redacted) =>
    Effect.gen(function* () {
      const shown = redactUrl(Redacted.value(url));
      const admin = yield* Layer.build(SqlStorage.clientLayer(dialect.client(url))).pipe(
        Effect.map(Context.get(SqlClient.SqlClient)),
        Effect.tap((sql) => sql.unsafe("SELECT 1")),
        Effect.mapError(
          (error) =>
            new DatabaseError({
              message: `${dialect.variable} is set, but ${shown} does not answer: ${errorMessage(error)}`,
            }),
        ),
        Effect.timeoutOrElse({
          duration: "15 seconds",
          orElse: () =>
            Effect.fail(
              new DatabaseError({
                message: `${dialect.variable} is set, but ${shown} did not answer in 15 seconds`,
              }),
            ),
        }),
      );
      yield* Console.log(`${dialect.backend}: ${shown} answers`);
      return { dialect, url, admin } satisfies Server;
    });

  const servers = yield* Effect.forEach(DIALECTS, (dialect) =>
    Option.match(config[dialect.backend], {
      onNone: () => Effect.succeed(Option.none<Server>()),
      onSome: (url) => Effect.map(connect(dialect, url), Option.some),
    }),
  ).pipe(Effect.map((found) => found.flatMap(Option.toArray)));

  /**
   * Makes a fresh database `streamsy_matrix_<owner>_<run id>`, and drops it when
   * the scope closes. The owner is a runtime, or `issues` for Bugsy.
   */
  const freshDatabase = (server: Server, owner: string) =>
    Effect.gen(function* () {
      const name = `streamsy_matrix_${owner}_${runId}`;
      const url = withDatabase(Redacted.value(server.url), name);
      const shown = redactUrl(url);
      yield* Effect.acquireRelease(
        server.admin.unsafe(`CREATE DATABASE ${name}`).pipe(
          Effect.mapError(
            (error) =>
              new DatabaseError({
                message: `could not create ${shown}: ${errorMessage(error)}`,
              }),
          ),
        ),
        () =>
          server.admin.unsafe(server.dialect.dropDatabase(name)).pipe(
            Effect.tap(() => Console.log(`  ${server.dialect.backend}: dropped ${shown}`)),
            Effect.catchCause((cause) =>
              Console.error(
                `  ${server.dialect.backend}: could not drop ${shown}\n${Cause.pretty(cause)}`,
              ),
            ),
          ),
      );
      yield* Console.log(`  ${server.dialect.backend}: created ${shown}`);
      return { dialect: server.dialect, url, shown } satisfies Database;
    });

  /** Runs a command to its end and writes its combined output to `log`. */
  const run = (
    command: ReadonlyArray<string>,
    options: {
      readonly cwd: string;
      // An `undefined` value unsets an inherited variable.
      readonly env: Record<string, string | undefined>;
      readonly log: string;
    },
  ) =>
    Effect.scoped(
      Effect.gen(function* () {
        const [executable = "", ...args] = command;
        const handle = yield* spawner.spawn(
          ChildProcess.make(executable, args, {
            cwd: options.cwd,
            env: options.env,
            extendEnv: true,
            forceKillAfter: "10 seconds",
          }),
        );
        const output = yield* handle.all.pipe(Stream.decodeText, Stream.mkString);
        const exitCode = yield* handle.exitCode;
        yield* fs.writeFileString(path.join(outDir, options.log), output);
        return { exitCode: Number(exitCode), output };
      }),
    );

  /** Starts an entry in the run scope and waits until `/healthz` names its runtime. */
  const start = (
    runtime: Runtime,
    command: ReadonlyArray<string>,
    databases: ReadonlyArray<Database>,
  ) =>
    Effect.gen(function* () {
      const [executable = "", ...args] = command;
      const handle = yield* spawner.spawn(
        ChildProcess.make(executable, args, {
          cwd: packageDir,
          env: {
            PORT: String(runtime.port),
            SQLITE_PATH: path.join(outDir, `${runtime.name}.sqlite`),
            // The protocol default is 30 s, which makes the suite slow.
            LONG_POLL_TIMEOUT_MS: "1500",
            // Only this runtime's databases. An unset URL stays unset, so its cells skip.
            ...Object.fromEntries(
              databases.map((database) => [database.dialect.variable, database.url]),
            ),
          },
          extendEnv: true,
          forceKillAfter: "10 seconds",
        }),
      );
      yield* handle.all.pipe(
        Stream.run(fs.sink(path.join(outDir, `${runtime.name}.server.log`))),
        Effect.ignore,
        Effect.forkScoped,
      );
      yield* Effect.addFinalizer(() =>
        Effect.ignore(handle.kill({ killSignal: "SIGTERM", forceKillAfter: "10 seconds" })),
      );
      const url = `http://127.0.0.1:${runtime.port}`;
      const healthy = HttpClient.get(`${url}/healthz`).pipe(
        Effect.flatMap(HttpClientResponse.schemaBodyJson(Health)),
        // Node binds the port before the routes serve. A request in that gap
        // is accepted and never answered, so each attempt has its own timeout.
        Effect.timeout("2 seconds"),
        Effect.filterOrFail(
          (health) => health.runtime === runtime.name,
          (health) =>
            new EntryStartError({
              message: `port ${runtime.port} answered as ${health.runtime}, not ${runtime.name}`,
            }),
        ),
        Effect.retry(Schedule.spaced("250 millis")),
      );
      const exitedEarly = handle.exitCode.pipe(
        Effect.flatMap((code) =>
          Effect.fail(new EntryStartError({ message: `exited with ${code} before it answered` })),
        ),
      );
      // A first `mise exec deno@2` can download Deno, so allow a minute.
      return yield* Effect.raceFirst(healthy, exitedEarly).pipe(
        Effect.timeoutOrElse({
          duration: "60 seconds",
          orElse: () =>
            Effect.fail(new EntryStartError({ message: `did not answer at ${url}/healthz` })),
        }),
        Effect.map((health) => ({ url, health })),
      );
    });

  const check = (
    command: ReadonlyArray<string>,
    cwd: string,
    env: Record<string, string | undefined>,
    log: string,
  ) =>
    run(command, { cwd, env, log }).pipe(
      Effect.map(({ exitCode, output }) => ({
        status: exitCode === 0 ? ("pass" as const) : ("fail" as const),
        exitCode,
        log,
        output,
      })),
    );

  /**
   * The Bugsy smoke in a fresh database of `server`, dropped after the
   * smoke. The smoke starts and restarts its own server, so it gets only this
   * server's URL; the other dialect's variable is unset.
   */
  const bugsySmoke = (server: Server, name: string) =>
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* freshDatabase(server, "issues");
        const smoke = yield* check(
          ["bun", "run", "smoke:http"],
          path.join(repoDir, "examples/bugsy"),
          Object.fromEntries(
            DIALECTS.map((dialect) => [
              dialect.variable,
              dialect === server.dialect ? database.url : undefined,
            ]),
          ),
          `${name}.bugsy.log`,
        );
        return {
          status: smoke.status,
          exitCode: smoke.exitCode,
          log: smoke.log,
          database: database.shown,
        };
      }),
    );

  const runCell = (
    runtime: Runtime,
    url: string,
    backend: string,
    database?: Database,
    server?: Server,
  ) =>
    Effect.gen(function* () {
      const baseUrl = `${url}/${backend}`;
      const name = `${runtime.name}-${backend}`;
      const conformance = yield* check(
        ["bun", "run", "test:deployed"],
        path.join(repoDir, "packages/conformance-tests"),
        { SERVER_BASE_URL: baseUrl },
        `${name}.conformance.log`,
      );
      const smoke = yield* check(
        ["bun", "run", "smoke:http"],
        path.join(repoDir, "examples/memory-server"),
        { BASE_URL: baseUrl },
        `${name}.smoke.log`,
      );
      const bugsy =
        runtime.name === BUGSY_RUNTIME && server !== undefined
          ? yield* bugsySmoke(server, name)
          : undefined;
      const status: Status =
        conformance.status === "pass" &&
        smoke.status === "pass" &&
        (bugsy === undefined || bugsy.status === "pass")
          ? "pass"
          : "fail";
      const cell: Cell = {
        runtime: runtime.name,
        backend,
        status,
        baseUrl,
        database: database?.shown,
        conformance: {
          status: conformance.status,
          exitCode: conformance.exitCode,
          log: conformance.log,
          counts: parseCounts(conformance.output),
        },
        smoke: { status: smoke.status, exitCode: smoke.exitCode, log: smoke.log },
        bugsy,
      };
      yield* Console.log(`  ${name}: ${describe(cell)}`);
      return cell;
    });

  const runRuntime = (runtime: Runtime) =>
    Option.match(runtime.command, {
      onNone: () =>
        Effect.succeed(
          BACKENDS.map((backend) => skip(runtime, backend, `${runtime.name} is not installed`)),
        ),
      onSome: (command) =>
        Effect.scoped(
          Effect.gen(function* () {
            yield* Console.log(`${runtime.name}: starting ${command.join(" ")}`);
            // Made before the entry starts, so the entry stops before they drop.
            const databases = yield* Effect.forEach(servers, (server) =>
              freshDatabase(server, runtime.name),
            );
            const started = yield* Effect.exit(start(runtime, command, databases));
            if (Exit.isFailure(started)) {
              const reason = `${runtime.name} entry did not start; see ${runtime.name}.server.log`;
              yield* Console.error(`${reason}\n${Cause.pretty(started.cause)}`);
              return BACKENDS.map((backend) => fail(runtime, backend, reason));
            }
            const { url, health } = started.value;
            return yield* Effect.forEach(BACKENDS, (backend) =>
              health.backends.includes(backend)
                ? runCell(
                    runtime,
                    url,
                    backend,
                    databases.find((database) => database.dialect.backend === backend),
                    servers.find((server) => server.dialect.backend === backend),
                  )
                : Effect.succeed(
                    skip(
                      runtime,
                      backend,
                      health.unmounted[backend] ?? `the app has no ${backend} backend`,
                    ),
                  ),
            );
          }),
        ),
    });

  // One runtime at a time: the suite already runs 20 tests at once per cell.
  const cells = (yield* Effect.forEach(runtimes(config.basePort), runRuntime)).flat();

  yield* Console.log(`\n${table(cells)}`);
  const result = {
    startedAt: stamp,
    backends: BACKENDS,
    databases: Object.fromEntries(
      servers.map((server) => [server.dialect.backend, redactUrl(Redacted.value(server.url))]),
    ),
    cells,
    ok: cells.every((cell) => cell.status !== "fail"),
  };
  const json = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown, { space: 2 }))(
    result,
  );
  yield* fs.writeFileString(path.join(outDir, "result.json"), `${json}\n`);
  yield* Console.log(`\nresult: ${path.join(outDir, "result.json")}`);
  return yield* result.ok
    ? Effect.void
    : Effect.fail(new MatrixFailed({ message: "one or more matrix cells failed" }));
});

class MatrixFailed extends Schema.TaggedError<MatrixFailed>()("MatrixFailed", {
  message: Schema.String,
}) {}

/**
 * A cell for the table, for example `pass (332 passed, 6 skipped; smoke pass)`,
 * with `; bugsy pass` when the cell ran the Bugsy smoke.
 */
const describe = (cell: Cell): string => {
  const counts = cell.conformance?.counts;
  const details = [
    ...(counts === undefined || counts === null
      ? []
      : [
          `${counts.passed} passed` +
            (counts.failed > 0 ? `, ${counts.failed} failed` : "") +
            `, ${counts.skipped} skipped`,
        ]),
    ...(cell.smoke === undefined ? [] : [`smoke ${cell.smoke.status}`]),
    ...(cell.bugsy === undefined ? [] : [`bugsy ${cell.bugsy.status}`]),
  ];
  return details.length === 0 ? cell.status : `${cell.status} (${details.join("; ")})`;
};

const table = (cells: ReadonlyArray<Cell>): string => {
  const rows = [...new Set(cells.map((cell) => cell.runtime))];
  const header = ["runtime", ...BACKENDS];
  const body = rows.map((runtime) => [
    runtime,
    ...BACKENDS.map((backend) => {
      const cell = cells.find((c) => c.runtime === runtime && c.backend === backend);
      return cell === undefined ? "" : describe(cell);
    }),
  ]);
  const widths = header.map((_, i) =>
    Math.max(...[header, ...body].map((row) => (row[i] ?? "").length)),
  );
  const line = (row: ReadonlyArray<string>) =>
    `| ${row.map((value, i) => value.padEnd(widths[i] ?? 0)).join(" | ")} |`;
  const notes = cells
    .filter((cell) => cell.reason !== undefined)
    .map((cell) => `- ${cell.runtime} ${cell.backend}: ${cell.status}, ${cell.reason}`);
  const databases = cells.flatMap((cell) => [
    ...(cell.database === undefined
      ? []
      : [`- ${cell.runtime} ${cell.backend}: database ${cell.database}`]),
    ...(cell.bugsy === undefined
      ? []
      : [`- ${cell.runtime} ${cell.backend}: Bugsy database ${cell.bugsy.database}`]),
  ]);
  return [
    line(header),
    line(widths.map((width) => "-".repeat(width))),
    ...body.map(line),
    "",
    "Counts are the official suite. Smoke is the memory-server HTTP smoke.",
    "Bugsy is the Bugsy HTTP smoke, on the Bun pg and mysql cells.",
    ...notes,
    ...databases,
  ].join("\n");
};

program.pipe(
  // The admin connections and the run's databases live in this scope.
  Effect.scoped,
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This script is the entry point.
  Effect.provide(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer)),
  BunRuntime.runMain,
);
