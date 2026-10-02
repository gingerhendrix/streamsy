// oxlint-disable effecttsgo/async-function -- Test support owns a child process at the Promise edge of vitest hooks.
/**
 * Starts `src/hosts/<runtime>.ts` as a child process and waits for the JSON
 * line that names its URL. Node and Deno run the built `@streamsy/*` packages
 * from the workspace `node_modules`, so run `bun run build` first.
 */
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
    : new Promise((resolve) => child.once("exit", (code) => resolve(code)));

export const spawnHost = (
  command: ReadonlyArray<string>,
  env: Readonly<Record<string, string>>,
  timeoutMs = 60_000,
): Promise<SpawnedHost> => {
  const scratch = process.env.STREAMSY_STORAGE_SCRATCH ?? tmpdir();
  const filename = `${scratch}/${command[0]}-host-${process.pid}-${crypto.randomUUID()}.sqlite`;
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
  const stop = async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    const code = await exited(child);
    removeDatabase();
    return code;
  };
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`Host did not start within ${timeoutMs} ms:\n${output}`));
    }, timeoutMs);
    child.stderr?.on("data", (chunk: Buffer) => {
      output += chunk.toString();
    });
    child.stdout?.on("data", (chunk: Buffer) => {
      output += chunk.toString();
      const match = /\{"listening":"([^"]+)"\}/.exec(output);
      if (match?.[1] !== undefined) {
        clearTimeout(timer);
        resolve({ url: match[1], stop, output: () => output });
      }
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      removeDatabase();
      reject(new Error(`Host exited with ${code} before it listened:\n${output}`));
    });
  });
};
