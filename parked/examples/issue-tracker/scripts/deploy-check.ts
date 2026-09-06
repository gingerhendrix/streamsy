/* oxlint-disable effecttsgo/global-console, effecttsgo/node-builtin-import, effecttsgo/process-env -- This local Bun verification executable resolves its package directory, spawns the package's checks, and reports them to the invoking terminal. */
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const stage = process.env.STAGE ?? "integration-3b-plan-check";

await step("typecheck topology and derived Worker environment", "bun", ["run", "typecheck"]);
await step("build browser assets and Cloudflare Worker", "bun", ["run", "build:deployment"]);
await step("import and inspect the plan-only topology", "bunx", [
  "vitest",
  "--run",
  "test/alchemy-stack.test.ts",
]);
await step("evaluate the Alchemy plan without applying resources", "bunx", [
  "alchemy",
  "plan",
  "--stage",
  stage,
]);

console.log(`\ndeploy:check passed for read-only stage ${stage}; no resources were applied`);

function step(label: string, command: string, args: readonly string[]): Promise<void> {
  console.log(`\n==> ${label}`);
  const result = Bun.spawnSync([command, ...args], {
    cwd: packageDir,
    stdout: "inherit",
    stderr: "inherit",
  });
  if (result.exitCode !== 0) {
    throw new Error(`${label} exited with code ${String(result.exitCode)}`);
  }
  return Promise.resolve();
}
