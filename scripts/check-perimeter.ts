/** Batch 4 boundary. Expand the scans as the old graph is replaced. */
import { spawnSync } from "node:child_process";
import { join } from "node:path";

const repoRoot = join(import.meta.dirname, "..");
const excluded = ["parked/**", "docs/**", "site/content/**"];

const scan = (label: string, pattern: string, extraGlobs: Array<string> = []): boolean => {
  const result = spawnSync(
    "rg",
    [
      "--line-number",
      "--hidden",
      "--glob",
      "!.git/**",
      ...excluded.flatMap((path) => ["--glob", `!${path}`]),
      ...extraGlobs.flatMap((glob) => ["--glob", glob]),
      "--",
      pattern,
      ".",
    ],
    { cwd: repoRoot, encoding: "utf8" },
  );
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0 && result.status !== 1) {
    throw new Error(`rg failed for ${label}: ${result.stderr}`);
  }
  const passed = result.status === 1;
  console.log(`${passed ? "ok  " : "FAIL"} ${label}`);
  if (!passed) process.stdout.write(result.stdout);
  return passed;
};

// These packages keep their existing runner while the old core stays intact.
// Remove this list in Batch 6; it is not an exception for any new package.
const oldRunnerPackages = ["core", "http-client", "streams", "projection", "state", "storage"];
const sourceGlobs = ["*.{ts,tsx,js,jsx,mts,cts,mjs,cjs}"];
const checks = [
  scan(
    "no parked example paths in live inputs",
    String.raw`(?:^|[^\w-])(?:parked/|examples/(?:issue-tracker(?:-demo|-projections)?|risk-demo|memory-server)(?:/|["']))`,
    // Ignore declarations of the exclusion itself, but still scan root scripts.
    ["!.oxlintrc.json", "!.oxfmtrc.json", "!scripts/check-perimeter.ts"],
  ),
  scan("no retired package names", String.raw`@streamsy/(?:sinks|tanstack-db)\b`),
  scan(
    "no Vitest imports outside conformance and the old graph",
    String.raw`(?:\bfrom\s*|\bimport\s*(?:\(\s*)?|\brequire\s*\(\s*)["']vitest(?:/[^"']*)?["']`,
    [
      ...sourceGlobs,
      "!packages/conformance-tests/**",
      // Existing Hacker News tests move to Bun with the example rewrite in Batch 5.
      "!examples/hackernews-newest-stream/src/server/newest-poller.test.ts",
      "!examples/hackernews-newest-stream/src/server/story-index-projection.test.ts",
      ...oldRunnerPackages.map((name) => `!packages/${name}/**`),
    ],
  ),
  scan(
    "core-next has no Promise, async, abort, timer or cloning plumbing",
    String.raw`\b(?:Promise|async|AbortSignal|AbortController|setTimeout|structuredClone)\b|tryPromise|Effect\.promise|\.then\(`,
    ["packages/core-next/src/**", "!*.test.ts"],
  ),
  scan(
    "authored core-next HTTP does not construct Web streams",
    String.raw`new\s+ReadableStream\b`,
    ["packages/core-next/src/http/**", "!*.test.ts"],
  ),
  scan("no Effect Vitest integration", "@effect[/]vitest"),
];
console.log(`Temporary Vitest exceptions until Batch 6: ${oldRunnerPackages.join(", ")}.`);
console.log("Temporary Vitest exceptions until Batch 5: the two existing Hacker News test files.");
if (checks.some((passed) => !passed)) process.exit(1);
console.log("Perimeter checks passed (Batch 4 scope).");
