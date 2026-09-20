import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { effectAreas } from "./check-lint-policy.ts";

const repoRoot = join(import.meta.dirname, "..");
const result = spawnSync(
  "./node_modules/.bin/oxlint",
  ["--config", ".oxlintrc.effect.json", ...effectAreas, ...process.argv.slice(2)],
  { cwd: repoRoot, stdio: "inherit" },
);
if (result.error !== undefined) throw result.error;
process.exit(result.status ?? 1);
