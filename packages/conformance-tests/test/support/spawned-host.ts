/**
 * Starts `src/hosts/<runtime>.ts` as a child process and waits for the JSON
 * line that names its URL. Node and Deno run the built `@streamsy/*` packages
 * from the workspace `node_modules`, so run `bun run build` first.
 */
import { NodeCrypto } from "@effect/platform-node";
import { Context, Crypto, Effect, Layer } from "effect";
import { spawn, type ChildProcess } from "node:child_process";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

export type Runtime = "node" | "deno";

const packageRoot = fileURLToPath(new URL("../..", import.meta.url));

/** The command that runs a host entry, or undefined when the runtime is missing. */
export const runtimeCommand = (runtime: Runtime): ReadonlyArray<string> | undefined => {
  if (runtime === "node") {
    return Bun.which("node") === null ? undefined : ["node", "src/hosts/node.ts"];
  }

  const deno = ["run", "--allow-all", "--no-lock", "src/hosts/deno.ts"];

  if (Bun.which("deno") !== null) return ["deno", ...deno];

  // Deno is not always installed globally. mise fetches a Deno 2 on first use.
  if (Bun.which("mise") !== null) return ["mise", "exec", "deno@2", "--", "deno", ...deno];

  return undefined;
};

export interface SpawnedHost {
  readonly url: string;
  /** Sends SIGTERM and resolves with the exit code. */
  readonly stop: () => Promise<number | null>;
  readonly output: () => string;
}

const exited = (child: ChildProcess): Promise<number | null> =>
  child.exitCode !== null || child.signalCode !== null
    ? Promise.resolve(child.exitCode)
    : Effect.runPromise(
        Effect.callback<number | null>((resume) => {
          const onExit = (code: number | null) => resume(Effect.succeed(code));
          child.once("exit", onExit);

          return Effect.sync(() => {
            child.removeListener("exit", onExit);
          });
        }),
      );

export const spawnHost = (
  command: ReadonlyArray<string>,
  env: Readonly<Record<string, string>>,
  timeoutMs = 60_000,
): Promise<SpawnedHost> => {
  const scratch = process.env.STREAMSY_STORAGE_SCRATCH ?? tmpdir();

  const crypto = Context.get(
    Effect.runSync(Effect.scoped(Layer.build(NodeCrypto.layer))),
    Crypto.Crypto,
  );

  const filename = `${scratch}/${command[0]}-host-${process.pid}-${Effect.runSync(crypto.randomUUIDv4)}.sqlite`;
  const [executable, ...args] = command;

  if (executable === undefined) throw new Error("Expected a host command");

  const child = spawn(executable, args, {
    cwd: packageRoot,
    env: { ...process.env, SQLITE_PATH: filename, PORT: "0", ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let output = "";

  const removeDatabase = () => {
    for (const suffix of ["", "-wal", "-shm", "-journal"])
      rmSync(filename + suffix, { force: true });
  };

  const stop = () =>
    Effect.runPromise(
      Effect.gen(function* () {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
        const code = yield* Effect.promise(() => exited(child));
        removeDatabase();

        return code;
      }),
    );

  const onStderr = (chunk: Buffer) => {
    output += chunk.toString();
  };

  const awaitReady = Effect.callback<SpawnedHost>((resume) => {
    const onStdout = (chunk: Buffer) => {
      output += chunk.toString();
      const match = /\{"listening":"([^"]+)"\}/.exec(output);

      if (match?.[1] !== undefined)
        resume(Effect.succeed({ url: match[1], stop, output: () => output }));
    };

    const onExit = (code: number | null) => {
      removeDatabase();
      resume(Effect.die(new Error(`Host exited with ${code} before it listened:\n${output}`)));
    };

    child.stderr?.on("data", onStderr);
    child.stdout?.on("data", onStdout);
    child.once("exit", onExit);

    return Effect.sync(() => {
      // Output and database cleanup remain live after readiness until the child exits.
      child.removeListener("exit", onExit);
      child.once("exit", removeDatabase);
    });
  });

  return Effect.runPromise(
    awaitReady.pipe(
      Effect.timeoutOrElse({
        duration: timeoutMs,
        orElse: () =>
          Effect.sync(() => {
            child.kill("SIGKILL");
          }).pipe(
            Effect.andThen(
              Effect.die(new Error(`Host did not start within ${timeoutMs} ms:\n${output}`)),
            ),
          ),
      }),
    ),
  );
};
