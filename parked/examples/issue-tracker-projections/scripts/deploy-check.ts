/* oxlint-disable effecttsgo/global-console, effecttsgo/new-promise, effecttsgo/node-builtin-import, effecttsgo/process-env -- This Bun check executable spawns child processes with the Node-compatible child-process and path APIs, wrapping each one in the Promise its exit event settles, reads the stage and credential environment directly, and reports every step to the invoking terminal. */
/**
 * Validate a deployable topology without touching production.
 *
 * Alchemy v2 replaces v1's "no read-only operation" gap with a real
 * `alchemy plan`, so this check is meaningfully stronger than its v1 version:
 *
 *   1. typecheck, which compiles `alchemy.run.ts` against the installed
 *      alchemy@2 declarations and therefore validates every resource spelling;
 *   2. build the Worker bundle and the browser assets;
 *   3. run the stack-shape tests, which import the v2 program and assert it is
 *      a description with the expected resources;
 *   4. `alchemy plan`, which evaluates the stack for real and prints the
 *      resources a deploy would create;
 *   5. audit any applied Alchemy state for runtime identifiers.
 *
 * It then reports whether deploy credentials are present, without using them.
 *
 * It never applies infrastructure — `plan` is read-only, and `deploy:demo` is
 * the separate explicit step.
 */
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");

await step("typecheck (validates the Alchemy v2 program and resource spelling)", "bun", [
  "run",
  "typecheck",
]);
await step("build worker bundle and assets", "bun", ["run", "build"]);
await step("assert the Alchemy v2 stack shape", "bunx", [
  "vitest",
  "--run",
  "test/alchemy-stack.test.ts",
]);
await step("plan the stack (read-only)", "bunx", [
  "alchemy",
  "plan",
  "--stage",
  process.env.STAGE ?? "check",
]);
await step("audit applied Alchemy state for runtime identifiers", "bun", ["run", "audit:state"]);

const credentials =
  (process.env.CLOUDFLARE_API_TOKEN ?? process.env.CLOUDFLARE_API_KEY ?? "").length > 0;
console.log(
  credentials
    ? "\ndeploy:check passed — credentials present, no infrastructure was changed"
    : "\ndeploy:check passed — no Cloudflare credentials configured, so a live deploy is blocked",
);

function step(label: string, command: string, args: readonly string[]): Promise<void> {
  console.log(`\n==> ${label}`);
  return new Promise((resolve, reject) => {
    const child = spawn(command, [...args], { cwd: packageDir, stdio: "inherit" });
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: node:child_process returns an EventEmitter at runtime; Bun 1.4's ambient Node compatibility declaration omits these inherited overloads.
    const events = child as typeof child & {
      on(event: "error", listener: (error: Error) => void): void;
      on(event: "exit", listener: (code: number | null) => void): void;
    };
    events.on("error", reject);
    events.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${label} exited with code ${String(code)}`)),
    );
  });
}
