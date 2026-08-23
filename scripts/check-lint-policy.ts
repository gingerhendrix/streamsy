/**
 * Verifies the lint policy split:
 *
 * - the general policy (`.oxlintrc.json`) scans the whole repository and never
 *   applies Effect rules;
 * - the Effect policy (`.oxlintrc.effect.json`) is applied only to the
 *   Effect-owned areas listed in the `lint:effect` script.
 *
 * The probe files below are written, linted and removed inside this script so
 * that no lint fixtures are committed to the repository.
 */
import { spawnSync } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

const repoRoot = join(import.meta.dirname, "..");

const effectAreas = [
  "packages/experimental",
  "examples/fold-agent",
  "examples/hackernews-newest-stream",
  "examples/issue-tracker-projections",
  "examples/risk-demo",
] as const;

/** An async function is an Effect diagnostic; `Array#sort()` is a general one. */
const probeSource = [
  "export async function lintPolicyProbe(): Promise<number> {",
  "  return 1",
  "}",
  "",
  "export function lintPolicyProbeSort(values: Array<number>): Array<number> {",
  "  return values.sort()",
  "}",
  "",
].join("\n");

const generalProbe = "packages/core/src/__lint-policy-probe__.ts";
const effectProbe = "packages/experimental/src/__lint-policy-probe__.ts";

interface Diagnostic {
  code: string;
  filename: string;
}

const isDiagnosticReport = (value: unknown): value is { diagnostics: Array<Diagnostic> } =>
  typeof value === "object" &&
  value !== null &&
  "diagnostics" in value &&
  Array.isArray(value.diagnostics);

const lint = (args: Array<string>): Array<Diagnostic> => {
  const result = spawnSync("./node_modules/.bin/oxlint", [...args, "-f", "json"], {
    cwd: repoRoot,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.error !== undefined) throw result.error;
  const stdout = result.stdout.trim();
  if (!stdout.startsWith("{")) {
    throw new Error(
      `oxlint did not produce JSON for ${args.join(" ")}:\n${stdout}\n${result.stderr}`,
    );
  }
  const parsed: unknown = JSON.parse(stdout);
  if (!isDiagnosticReport(parsed)) {
    throw new Error(`oxlint JSON output had no diagnostics array for ${args.join(" ")}`);
  }
  return parsed.diagnostics;
};

const normalise = (filename: string): string => filename.replace(/^\.\//, "");

const isEffectCode = (code: string): boolean => code.startsWith("effecttsgo(");

const failures: Array<string> = [];
const check = (ok: boolean, message: string): void => {
  console.log(`${ok ? "ok  " : "FAIL"} ${message}`);
  if (!ok) failures.push(message);
};

const writeProbe = (relative: string): void => {
  mkdirSync(join(repoRoot, dirname(relative)), { recursive: true });
  writeFileSync(join(repoRoot, relative), probeSource);
};

try {
  writeProbe(generalProbe);
  writeProbe(effectProbe);

  const general = lint(["."]);
  const effect = lint(["--config", ".oxlintrc.effect.json", ...effectAreas]);

  const generalProbeFindings = general.filter((d) => normalise(d.filename) === generalProbe);
  check(
    generalProbeFindings.some((d) => d.code === "unicorn(no-array-sort)"),
    "general policy reports a general violation in a non-Effect package",
  );
  check(
    !generalProbeFindings.some((d) => isEffectCode(d.code)),
    "general policy reports no Effect violation in a non-Effect package",
  );
  check(
    !general.some((d) => isEffectCode(d.code)),
    "general policy reports no Effect diagnostic anywhere in the repository",
  );

  check(
    !effect.some((d) => normalise(d.filename) === generalProbe),
    "Effect policy does not scan a non-Effect package",
  );
  check(
    effect.some(
      (d) => normalise(d.filename) === effectProbe && d.code === "effecttsgo(async-function)",
    ),
    "Effect policy reports an Effect violation in an Effect-owned area",
  );
  check(
    effect.every((d) => effectAreas.some((area) => normalise(d.filename).startsWith(`${area}/`))),
    "Effect policy reports only inside the Effect-owned areas",
  );
  check(
    effect.every((d) => isEffectCode(d.code)),
    "Effect policy reports only Effect diagnostics",
  );
} finally {
  rmSync(join(repoRoot, generalProbe), { force: true });
  rmSync(join(repoRoot, effectProbe), { force: true });
}

if (failures.length > 0) {
  console.error(`\n${failures.length} lint policy check(s) failed.`);
  process.exit(1);
}
console.log("\nLint policy checks passed.");
