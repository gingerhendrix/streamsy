/** Final code perimeter. Site content joins the gate in Batch 7. */
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const repoRoot = join(import.meta.dirname, "..");
const inventory = spawnSync(
  "git",
  ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
  {
    cwd: repoRoot,
    encoding: "utf8",
  },
);
if (inventory.status !== 0) throw new Error("Cannot inventory repository inputs");
const inputFiles = inventory.stdout.split("\0").filter(Boolean);
const matches = (path: string, pattern: string): boolean =>
  new Bun.Glob(pattern).match(pattern.includes("/") ? path : (path.split("/").at(-1) ?? path));
const scan = (label: string, pattern: string, globs: Array<string> = []): boolean => {
  const includes = globs.filter((glob) => !glob.startsWith("!"));
  const excludes = globs.filter((glob) => glob.startsWith("!")).map((glob) => glob.slice(1));
  const paths = inputFiles.filter(
    (path) =>
      (includes.length === 0 || includes.some((glob) => matches(path, glob))) &&
      !excludes.some((glob) => matches(path, glob)),
  );
  if (paths.length === 0) throw new Error(`Empty perimeter scope: ${label}`);
  const result = spawnSync("rg", ["--line-number", "--", pattern, ...paths], {
    cwd: repoRoot,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0 && result.status !== 1)
    throw new Error(`rg failed for ${label}: ${result.stderr}`);
  const passed = result.status === 1;
  console.log(`${passed ? "ok  " : "FAIL"} ${label}`);
  if (!passed) process.stdout.write(result.stdout);
  return passed;
};
const historical = ["!parked/**", "!docs/**", "!site/content/**"];
const authored = ["packages/*/src/**", "!*.test.ts"];
// These are existing published Bun test registration kits, not live runtime owners.
const testKits = [
  "packages/core/src/testing/storage-contract.ts",
  "packages/views/src/store/conformance.ts",
];
const checks = [
  scan("no temporary package alias anywhere", ["core", "next"].join("-")),
  scan(
    "no parked paths in live inputs",
    String.raw`(?:^|[^\w-])(?:parked/|examples/(?:issue-tracker(?:-demo|-projections)?|risk-demo|memory-server)(?:/|["']))`,
    [...historical, "!.oxlintrc.json", "!.oxfmtrc.json", "!scripts/check-perimeter.ts"],
  ),
  scan(
    "no retired package imports",
    String.raw`@streamsy/(?:http-client|state|streams|projection|tanstack-db|sinks|storage|core/json)\b`,
    historical,
  ),
  scan(
    "no retired core APIs",
    [
      "createStreamProtocol",
      "createMemoryStorageAdapter",
      "createHttpHandler",
      "directProtocolClient",
      "bindStream",
      "StorageAdapter",
      "runAwaitChangeLoop",
      "AfterCommit",
      "AdapterFault",
      "NotSupportedError",
      "scheduleExpiry",
      "cancelExpiry",
      "awaitChange",
      "JsonCodec",
      "streamIdentity",
    ].join("|"),
    ["packages/**", "examples/**", "site/**", "!site/content/**"],
  ),
  scan(
    "runtime conversion only at the host and test edges",
    String.raw`runPromise|runSync|runFork|runCallback|ManagedRuntime\.make`,
    [...authored, "!packages/serve/src/bun.ts", ...testKits.map((path) => `!${path}`)],
  ),
  scan(
    "core has no Promise, async, abort, timer or cloning plumbing",
    String.raw`\b(?:Promise|async|AbortSignal|AbortController|setTimeout|structuredClone)\b|tryPromise|Effect\.promise|\.then\(`,
    ["packages/core/src/**", "!*.test.ts"],
  ),
  scan("HTTP uses Effect streams", String.raw`new\s+ReadableStream\b`, [
    "packages/core/src/http/**",
    "!*.test.ts",
  ]),
  scan("no authored abort plumbing", String.raw`AbortSignal|AbortController`, [
    "packages/core/src/**",
    "packages/serve/src/**",
    "!*.test.ts",
  ]),
  scan(
    "no forbidden storage seams",
    String.raw`structuredClone|durability|counter:|status: "error"`,
    ["packages/core/src/**"],
  ),
  scan("no SQL in core", String.raw`bun:sqlite|@effect/sql`, ["packages/core/**"]),
  scan(
    "Vitest only in the official memory suite",
    String.raw`(?:\bfrom\s*|\bimport\s*(?:\(\s*)?|\brequire\s*\(\s*)["']vitest(?:/[^"']*)?["']`,
    [
      "*.{ts,tsx,js,jsx,mts,cts,mjs,cjs}",
      "!parked/**",
      "!packages/conformance-tests/src/memory.conformance.test.ts",
    ],
  ),
  scan("no Effect Vitest integration", "@effect[/]vitest", ["!parked/**"]),
];
for (const path of testKits) {
  const source = readFileSync(join(repoRoot, path), "utf8");
  const isTestKit = source.includes('from "bun:test"');
  checks.push(isTestKit);
  console.log(`${isTestKit ? "ok  " : "FAIL"} test registration boundary: ${path}`);
}
const files = spawnSync("git", ["ls-files", "-z", "--", "*package.json"], {
  cwd: repoRoot,
  encoding: "utf8",
});
if (files.status !== 0) throw new Error("Cannot inventory manifests");
for (const path of files.stdout.split("\0").filter(Boolean)) {
  if (path.startsWith("parked/")) continue;
  const manifest: {
    dependencies?: Record<string, string>;
    devDependencies?: Record<string, string>;
    overrides?: Record<string, string>;
  } = JSON.parse(readFileSync(join(repoRoot, path), "utf8"));
  for (const deps of [manifest.dependencies, manifest.devDependencies, manifest.overrides]) {
    if (deps?.effect !== undefined) {
      const pinned = deps.effect === "4.0.0-rc.112";
      checks.push(pinned);
      console.log(`${pinned ? "ok  " : "FAIL"} Effect pin: ${path}`);
    }
    if (deps?.vitest !== undefined) checks.push(path === "packages/conformance-tests/package.json");
  }
}
const suppressions = spawnSync("rg", ["--count", "oxlint-disable", "packages", "examples"], {
  cwd: repoRoot,
  encoding: "utf8",
});
if (suppressions.status !== 0 && suppressions.status !== 1)
  throw new Error("Cannot inventory suppressions");
console.log(`Lint suppression inventory (existing reasons retained):\n${suppressions.stdout}`);
if (checks.some((passed) => !passed)) process.exit(1);
console.log("Perimeter checks passed; site content remains deferred to Batch 7.");
