/** Final release perimeter, including all authored site content. */
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
    maxBuffer: 64 * 1024 * 1024,
  },
);
if (inventory.status !== 0) throw new Error("Cannot inventory repository inputs");
const inputFiles = [...new Set(inventory.stdout.split("\0").filter(Boolean))];
const contents = new Map<string, Array<string>>();
const readLines = (path: string): Array<string> => {
  const cached = contents.get(path);
  if (cached !== undefined) return cached;
  const lines = readFileSync(join(repoRoot, path), "utf8").split("\n");
  contents.set(path, lines);
  return lines;
};
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
  const expression = new RegExp(pattern);
  const findings: Array<string> = [];
  for (const path of paths) {
    for (const [index, line] of readLines(path).entries()) {
      if (expression.test(line)) findings.push(`${path}:${index + 1}:${line}`);
    }
  }
  const passed = findings.length === 0;
  console.log(`${passed ? "ok  " : "FAIL"} ${label}`);
  for (const finding of findings) console.log(finding);
  return passed;
};
const historical = ["!parked/**", "!docs/**"];
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
    String.raw`@streamsy/(?:http-client|state|streams|projection|tanstack-db|sinks|core/json)\b`,
    historical,
  ),
  scan(
    "no legacy storage entry imports",
    String.raw`@streamsy/storage/(?:fs|sqlite|durable-object/(?:adapter|storage))\b`,
    historical,
  ),
  scan(
    "no retired core APIs",
    String.raw`\b(?:${[
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
    ].join("|")})\b`,
    ["packages/**", "examples/**", "site/**"],
  ),
  scan(
    "runtime conversion only at the host and test edges",
    String.raw`runPromise|runSync|runFork|runCallback|ManagedRuntime\.make`,
    [
      ...authored,
      "!packages/serve/src/bun.ts",
      "!packages/serve/src/cloudflare/object.ts",
      ...testKits.map((path) => `!${path}`),
    ],
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
    "Vitest only in the official conformance suites",
    String.raw`(?:\bfrom\s*|\bimport\s*(?:\(\s*)?|\brequire\s*\(\s*)["']vitest(?:/[^"']*)?["']`,
    [
      "*.{ts,tsx,js,jsx,mts,cts,mjs,cjs}",
      "!parked/**",
      "!packages/conformance-tests/src/memory.conformance.test.ts",
      "!packages/conformance-tests/src/fetch.conformance.test.ts",
      "!packages/conformance-tests/src/sqlite.conformance.test.ts",
      "!packages/conformance-tests/src/workerd.conformance.test.ts",
    ],
  ),
  scan("no Effect Vitest integration", ["@effect", "vitest"].join("[/]"), [
    "!parked/**",
    // Alchemy declares this transitively; authored imports and manifests stay forbidden.
    "!bun.lock",
    "!hosted/bun.lock",
  ]),
];
for (const path of testKits) {
  const source = readFileSync(join(repoRoot, path), "utf8");
  const isTestKit = source.includes('from "bun:test"');
  checks.push(isTestKit);
  console.log(`${isTestKit ? "ok  " : "FAIL"} test registration boundary: ${path}`);
}
const dependencySections = [
  "dependencies",
  "devDependencies",
  "peerDependencies",
  "optionalDependencies",
  "overrides",
  "resolutions",
] as const;
const effectVitest = ["@effect", "vitest"].join("/");
/** The one accepted `effect` pin. Every manifest section that declares `effect` must match it. */
const effectPin = "4.0.0-rc.115";
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This checker receives parsed JSON values and recursively validates every dependency-bearing manifest section.
const inspectDependencyTree = (
  path: string,
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This checker receives parsed JSON values and recursively validates every dependency-bearing manifest section.
  value: unknown,
  section: string,
): void => {
  if (!(value instanceof Object) || Array.isArray(value)) return;
  for (const [key, target] of Object.entries(value)) {
    // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Manifest dependency targets are JSON strings or nested objects; this branch distinguishes those raw forms before scanning them.
    const targetText = typeof target === "string" ? target : "";
    const effectVitestDeclared = key.includes(effectVitest) || targetText.includes(effectVitest);
    if (effectVitestDeclared) {
      checks.push(false);
      console.log(`FAIL Effect Vitest declaration: ${path} (${section}.${key})`);
    }
    if (key === "effect") {
      const pinned = targetText === effectPin;
      checks.push(pinned);
      console.log(`${pinned ? "ok  " : "FAIL"} Effect pin: ${path} (${section})`);
    }
    if (key === "vitest" || targetText.includes("vitest")) {
      const official = path === "packages/conformance-tests/package.json";
      checks.push(official);
      console.log(
        `${official ? "ok  " : "FAIL"} Vitest manifest ownership: ${path} (${section}.${key})`,
      );
    }
    inspectDependencyTree(path, target, `${section}.${key}`);
  }
};

for (const path of inputFiles.filter(
  (file) => file === "package.json" || file.endsWith("/package.json"),
)) {
  if (path.startsWith("parked/")) continue;
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- This is the checker’s parsed manifest boundary; each inspected section is recursively validated before use.
  const manifest: Record<string, unknown> = JSON.parse(readFileSync(join(repoRoot, path), "utf8"));
  for (const section of dependencySections) {
    inspectDependencyTree(path, manifest[section], section);
  }
  const workspaces = manifest.workspaces;
  if (workspaces instanceof Object && !Array.isArray(workspaces) && "catalog" in workspaces) {
    inspectDependencyTree(path, workspaces.catalog, "workspaces.catalog");
  }
}
console.log("Lint suppression inventory (existing reasons retained):");
for (const path of inputFiles.filter(
  (file) =>
    (file.startsWith("packages/") || file.startsWith("examples/") || file.startsWith("hosted/")) &&
    !file.startsWith("hosted/node_modules/") &&
    !file.startsWith("hosted/dist/"),
)) {
  const count = readLines(path).filter((line) => /oxlint-disable/.test(line)).length;
  if (count > 0) console.log(`${path}:${count}`);
}
if (checks.some((passed) => !passed)) process.exit(1);
console.log("Perimeter checks passed, including site content.");
