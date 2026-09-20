import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

// Runs every workspace `test:unit` script one at a time. `bun run --filter`
// runs them in parallel, which starves the process-spawning suites on a
// two-core CI runner past their timeouts.
const repoRoot = join(import.meta.dirname, "..");
// SAFETY: the root manifest is this repository's own file; `bun install` has
// already parsed the same `workspaces.packages` array.
const rootManifest = JSON.parse(readFileSync(join(repoRoot, "package.json"), "utf8")) as {
  readonly workspaces: { readonly packages: ReadonlyArray<string> };
};

const workspaceDirs = rootManifest.workspaces.packages.flatMap((pattern) => {
  if (!pattern.endsWith("/*")) throw new Error(`Unsupported workspace pattern: ${pattern}`);
  const parent = pattern.slice(0, -2);
  return readdirSync(join(repoRoot, parent), { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => `${parent}/${entry.name}`)
    .sort();
});

for (const dir of workspaceDirs) {
  const manifestPath = join(repoRoot, dir, "package.json");
  if (!existsSync(manifestPath)) continue;
  // SAFETY: a workspace manifest is a package.json that `bun install` accepted;
  // only the optional scripts map is read.
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as {
    readonly scripts?: Record<string, string>;
  };
  if (manifest.scripts?.["test:unit"] === undefined) continue;
  console.log(`== ${dir}`);
  const result = spawnSync("bun", ["run", "--cwd", dir, "test:unit"], {
    cwd: repoRoot,
    stdio: "inherit",
  });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
