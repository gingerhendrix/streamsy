import { BunHttpServer } from "@effect/platform-bun";
import { Console, Effect, Exit, Fiber, Schedule, Schema, Stream } from "effect";
import { HttpClient, HttpServer } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

/** The server process exited, or its port did not answer, before it was ready. */
export class ServerStartError extends Schema.TaggedError<ServerStartError>()("ServerStartError", {
  message: Schema.String,
}) {}

/** The server did not stop cleanly: its exit code was not 0, or it wrote to stderr. */
export class UncleanStop extends Schema.TaggedError<UncleanStop>()("UncleanStop", {
  message: Schema.String,
  exitCode: Schema.NullOr(Schema.Int),
  stderr: Schema.String,
}) {}

export interface SpawnServerOptions {
  /** Working directory of the server process. */
  readonly cwd: string;
  /** Entry file, passed as the only argument to `command`. */
  readonly entry: string;
  /** Extra environment. `PORT` is always set to the server port. */
  readonly env?: Readonly<Record<string, string>>;
  /** Runtime that starts the entry; default `bun`. */
  readonly command?: string;
  /** Port to listen on; default a free port. Pass the old port to restart a server. */
  readonly port?: number;
  /**
   * Whether the status of `GET /` means the server is ready; default any status.
   * A listener can answer 404 before the routes are mounted.
   */
  readonly isReady?: (status: number) => boolean;
}

export interface Server {
  readonly port: number;
  /** `http://127.0.0.1:<port>` */
  readonly url: string;
  /** Sends SIGTERM and waits for exit. Fails unless the exit code is 0 and stderr is empty. */
  readonly stop: Effect.Effect<void, UncleanStop>;
}

/** A port that was free a moment ago: the OS picks it for a listener that closes at once. */
export const freePort = Effect.scoped(
  Effect.map(BunHttpServer.make({ port: 0 }), (server) =>
    Number(new URL(HttpServer.formatAddress(server.address)).port),
  ),
);

const collect = (stream: Stream.Stream<Uint8Array, unknown>) =>
  stream.pipe(
    Stream.decodeText,
    Stream.mkString,
    Effect.orElseSucceed(() => ""),
  );

/**
 * Starts `command entry` with `PORT` set and waits until the port answers HTTP.
 *
 * The scope owns the process. When the scope closes, the server gets SIGTERM
 * and must exit 0 with empty stderr. Effect finalizers cannot fail, so on a
 * successful scope an unclean stop is a defect that holds `UncleanStop`. Call
 * `stop` to get the same check as a typed failure.
 */
export const spawnServer = Effect.fn("spawnServer")(function* (options: SpawnServerOptions) {
  const port = options.port ?? (yield* freePort);
  const url = `http://127.0.0.1:${port}`;
  const label = `${options.command ?? "bun"} ${options.entry}`;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const handle = yield* spawner.spawn(
    ChildProcess.make(options.command ?? "bun", [options.entry], {
      cwd: options.cwd,
      env: { ...options.env, PORT: String(port) },
      extendEnv: true,
      forceKillAfter: "10 seconds",
    }),
  );
  // Read both pipes from the start, so a chatty server never blocks on a full pipe.
  const stdout = yield* Effect.forkScoped(collect(handle.stdout));
  const stderr = yield* Effect.forkScoped(collect(handle.stderr));

  const stopProcess = Effect.gen(function* () {
    yield* Effect.ignore(handle.kill({ killSignal: "SIGTERM", forceKillAfter: "10 seconds" }));
    const exit = yield* Effect.exit(handle.exitCode);
    const exitCode = Exit.isSuccess(exit) ? exit.value : null;
    const output = yield* Fiber.join(stdout);
    const errors = yield* Fiber.join(stderr);
    if (output.trim() !== "") yield* Console.log(output.trim());
    const clean = exitCode === 0 && errors.trim() === "";
    return yield* clean
      ? Effect.void
      : Effect.fail(
          new UncleanStop({
            message:
              `${label} stopped with exit code ${exitCode ?? "none (killed by a signal)"}` +
              (errors.trim() === "" ? "" : ` and wrote to stderr:\n${errors.trim()}`),
            exitCode,
            stderr: errors,
          }),
        );
  });
  const stopOnce = yield* Effect.cached(stopProcess);
  let stopRequested = false;
  const stop = Effect.suspend(() => {
    stopRequested = true;
    return stopOnce;
  });
  yield* Effect.addFinalizer((exit) => {
    if (stopRequested) return Effect.void;
    // A failed program already exits non-zero; report the stop but keep the first error.
    return Exit.isSuccess(exit)
      ? Effect.orDie(stop)
      : stop.pipe(Effect.catch((error) => Effect.logWarning(error.message)));
  });

  const isReady = options.isReady ?? (() => true);
  const ready = HttpClient.get(url).pipe(
    Effect.flatMap((response) =>
      Effect.flatMap(response.text, () =>
        isReady(response.status)
          ? Effect.void
          : Effect.fail(new ServerStartError({ message: `${url} answered ${response.status}` })),
      ),
    ),
    Effect.retry(Schedule.spaced("100 millis")),
  );
  // A server that exits before it answers failed to start; `stop` reads its exit code and stderr.
  const exitedEarly = Effect.ignore(handle.exitCode).pipe(
    Effect.andThen(stop),
    Effect.as(""),
    Effect.catch((error) => Effect.succeed(`: ${error.message}`)),
    Effect.flatMap((detail) =>
      Effect.fail(
        new ServerStartError({ message: `${label} exited before ${url} answered${detail}` }),
      ),
    ),
  );
  yield* Effect.raceFirst(ready, exitedEarly).pipe(
    Effect.timeoutOrElse({
      duration: "10 seconds",
      orElse: () =>
        Effect.fail(new ServerStartError({ message: `${label} did not answer at ${url}` })),
    }),
  );

  const server: Server = { port, url, stop };
  return server;
});
