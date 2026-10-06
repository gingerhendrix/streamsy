import { Schema } from "effect";
import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

// Runs every workspace `test:unit` script one at a time. `bun run --filter`
// runs them in parallel, which starves the process-spawning suites on a
// two-core CI runner past their timeouts.
const repoRoot = join(import.meta.dirname, "..");

const rootManifest = Schema.decodeSync(
  Schema.fromJsonString(
    Schema.Struct({ workspaces: Schema.Struct({ packages: Schema.Array(Schema.String) }) }),
  ),
)(readFileSync(join(repoRoot, "package.json"), "utf8"));

const workspaceDirs = rootManifest.workspaces.packages.flatMap((pattern) => {
  if (!pattern.endsWith("/*")) throw new Error(`Unsupported workspace pattern: ${pattern}`);
  const parent = pattern.slice(0, -2);

  return readdirSync(join(repoRoot, parent), { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => `${parent}/${entry.name}`)
    .toSorted();
});

for (const dir of workspaceDirs) {
  const manifestPath = join(repoRoot, dir, "package.json");

  if (!existsSync(manifestPath)) continue;

  const manifest = Schema.decodeSync(
    Schema.fromJsonString(
      Schema.Struct({ scripts: Schema.optional(Schema.Record(Schema.String, Schema.String)) }),
    ),
  )(readFileSync(manifestPath, "utf8"));

  if (manifest.scripts?.["test:unit"] === undefined) continue;
  console.log(`== ${dir}`);

  const result = spawnSync("bun", ["run", "--cwd", dir, "test:unit"], {
    cwd: repoRoot,
    stdio: "inherit",
  });

  if (result.error !== undefined) throw result.error;

  if (result.status !== 0) process.exit(result.status ?? 1);
}
