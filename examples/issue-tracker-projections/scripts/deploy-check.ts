/**
 * Validate a deployable topology without touching production.
 *
 * alchemy@0.82.1 has no read-only plan operation, so this check does the three
 * things that can be verified offline:
 *
 *   1. typecheck, which compiles `alchemy.run.ts` against the pinned Alchemy
 *      declarations and therefore validates every resource spelling;
 *   2. build the Worker bundle and the browser assets;
 *   3. audit any local Alchemy state for runtime identifiers;
 *   4. report whether deploy credentials are present, without using them.
 *
 * It never applies infrastructure. `deploy:demo` is the separate explicit step.
 */
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");

await step("typecheck (validates the Alchemy program and resource spelling)", "bun", [
  "run",
  "typecheck",
]);
await step("build worker bundle and assets", "bun", ["run", "build"]);
await step("audit Alchemy state for runtime identifiers", "bun", ["run", "audit:state"]);

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
    child.on("error", reject);
    child.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${label} exited with code ${String(code)}`)),
    );
  });
}
