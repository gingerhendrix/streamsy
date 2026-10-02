/**
 * Runs the platform matrix: starts the Bun, Node and Deno entries, then runs
 * the official conformance suite and the memory-server HTTP smoke against
 * every `runtime × backend` URL. Prints a pass, fail or skip table and writes
 * `result.json` and one raw log for each check to `.matrix/<timestamp>/`.
 *
 * A backend that an entry does not mount is a skip. Any failed cell, or an
 * entry that does not start, exits non-zero.
 *
 * env: MATRIX_BASE_PORT (default 4101; Bun, Node and Deno take three ports
 * from it), MATRIX_OUT (default `<package>/.matrix`).
 */
import { stripVTControlCharacters } from "node:util";
import { BunRuntime, BunServices } from "@effect/platform-bun";
import {
  Cause,
  Config,
  Console,
  DateTime,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Path,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

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
  readonly reason?: string;
  /** The suite counts, or null when the output has no vitest summary. */
  readonly conformance?: Check & { readonly counts: Counts | null };
  readonly smoke?: Check;
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

class EntryStartError extends Schema.TaggedError<EntryStartError>()("EntryStartError", {
  message: Schema.String,
}) {}

const MatrixConfig = Config.all({
  basePort: Config.Port("MATRIX_BASE_PORT").pipe(Config.withDefault(4101)),
  out: Config.String("MATRIX_OUT").pipe(Config.option),
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

  /** Runs a command to its end and writes its combined output to `log`. */
  const run = (
    command: ReadonlyArray<string>,
    options: { readonly cwd: string; readonly env: Record<string, string>; readonly log: string },
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
  const start = (runtime: Runtime, command: ReadonlyArray<string>) =>
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
    env: Record<string, string>,
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

  const runCell = (runtime: Runtime, url: string, backend: string) =>
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
      const status: Status =
        conformance.status === "pass" && smoke.status === "pass" ? "pass" : "fail";
      const cell: Cell = {
        runtime: runtime.name,
        backend,
        status,
        baseUrl,
        conformance: {
          status: conformance.status,
          exitCode: conformance.exitCode,
          log: conformance.log,
          counts: parseCounts(conformance.output),
        },
        smoke: { status: smoke.status, exitCode: smoke.exitCode, log: smoke.log },
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
            const started = yield* Effect.exit(start(runtime, command));
            if (Exit.isFailure(started)) {
              const reason = `${runtime.name} entry did not start; see ${runtime.name}.server.log`;
              yield* Console.error(`${reason}\n${Cause.pretty(started.cause)}`);
              return BACKENDS.map((backend) => fail(runtime, backend, reason));
            }
            const { url, health } = started.value;
            return yield* Effect.forEach(BACKENDS, (backend) =>
              health.backends.includes(backend)
                ? runCell(runtime, url, backend)
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

/** A cell for the table, for example `pass (332 passed, 6 skipped; smoke pass)`. */
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
  return [
    line(header),
    line(widths.map((width) => "-".repeat(width))),
    ...body.map(line),
    "",
    "Counts are the official suite. Smoke is the memory-server HTTP smoke.",
    ...notes,
  ].join("\n");
};

program.pipe(
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This script is the entry point.
  Effect.provide(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer)),
  BunRuntime.runMain,
);
