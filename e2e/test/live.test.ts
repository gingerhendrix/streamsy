import { afterEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BunServices } from "@effect/platform-bun";
import { Effect, Exit, Layer } from "effect";
import type { EvidenceReport, RunIdentity } from "../src/contract.ts";
import { liveOperations, livePaths, liveReports } from "../src/live.ts";
import { HttpOperation } from "../src/measurement.ts";
import { runEvidence } from "../src/run.ts";

const stage = "c-s4l6mo-abcdef";
const workerName = `streamsy-conf-${stage}`;

// Stands in for `alchemy deploy|destroy --stage <stage>` with the same state layout.
const fakeAlchemy = `
const { appendFileSync, mkdirSync, rmSync, writeFileSync } = require("node:fs");
const [command, , stage] = process.argv.slice(2);
appendFileSync("calls.log", command + " " + stage + "\\n");
const dir = ".alchemy/state/streamsy-conf/" + stage;
if (command === "deploy") {
  mkdirSync(dir, { recursive: true });
  writeFileSync(dir + "/Server.json", "{}");
  writeFileSync(dir + "/__stack_output__.json", JSON.stringify({
    url: "https://streamsy-conf-" + stage + ".example.workers.dev",
    workerName: "streamsy-conf-" + stage,
  }));
} else if (process.env.FAKE_KEEP_STATE !== "1") {
  rmSync(dir, { recursive: true, force: true });
}
`;

// Stands in for the conformance package `test:deployed` vitest run.
const fakeSuite = `
const { appendFileSync, writeFileSync } = require("node:fs");
const output = process.argv.find((arg) => arg.startsWith("--outputFile.json=")).split("=")[1];
appendFileSync("../../e2e/calls.log", "suite " + process.env.SERVER_BASE_URL + "\\n");
writeFileSync(output, JSON.stringify({
  success: true, numPassedTests: 332, numPendingTests: 6, numTodoTests: 0,
}));
`;

let workspace: string | undefined;
afterEach(() => {
  if (workspace !== undefined) rmSync(workspace, { recursive: true, force: true });
  delete process.env.FAKE_KEEP_STATE;
});

const setup = () => {
  workspace = mkdtempSync(join(tmpdir(), "streamsy-e2e-live-"));
  const root = join(workspace, "e2e");
  mkdirSync(join(root, "node_modules/alchemy/bin"), { recursive: true });
  writeFileSync(join(root, "node_modules/alchemy/bin/cli.js"), fakeAlchemy);
  const suite = join(workspace, "packages/conformance-tests");
  mkdirSync(suite, { recursive: true });
  writeFileSync(join(suite, "suite.cjs"), fakeSuite);
  writeFileSync(
    join(suite, "package.json"),
    JSON.stringify({ scripts: { "test:deployed": "node suite.cjs" } }),
  );
  return livePaths(root);
};

const identity = (cwd: string): RunIdentity => ({
  runId: "abcdef",
  stage,
  workerName,
  cwd,
  sourceSha: "source-sha",
  artifactSha256: "artifact-sha",
});

// Serves the readiness marker until destroy has run, then 404.
const http = (root: string) =>
  Layer.succeed(HttpOperation, {
    request: () =>
      Effect.sync(() =>
        readFileSync(join(root, "calls.log"), "utf8").includes("destroy")
          ? { status: 404, body: "" }
          : { status: 400, body: "Stream path required: /{path}" },
      ),
  });

const run = (paths: ReturnType<typeof setup>) =>
  Effect.runPromise(
    Effect.exit(
      runEvidence(identity(paths.root)).pipe(
        Effect.provide(
          Layer.mergeAll(liveOperations(paths), liveReports(paths), http(paths.root)).pipe(
            Layer.provide(BunServices.layer),
          ),
        ),
      ),
    ),
  );

const readReport = (paths: ReturnType<typeof setup>): EvidenceReport =>
  JSON.parse(readFileSync(join(paths.reports, `${stage}.json`), "utf8"));

test("the live layers deploy, run the suite on the stack URL, destroy and pass the audit", async () => {
  const paths = setup();
  const exit = await run(paths);
  expect(Exit.isSuccess(exit)).toBe(true);
  expect(readFileSync(join(paths.root, "calls.log"), "utf8").trim().split("\n")).toEqual([
    `deploy ${stage}`,
    `suite https://${workerName}.example.workers.dev/`,
    `destroy ${stage}`,
    `destroy ${stage}`,
  ]);
  const report = readReport(paths);
  expect(report.conformance).toEqual({
    status: "success",
    value: { passed: 332, skipped: 6, status: "success" },
  });
  expect(report.metadata.status).toBe("success");
  expect(report.audit).toEqual({ status: "success", value: { complete: true, workers: [] } });
}, 30_000);

test("state left after destroy fails the audit", async () => {
  const paths = setup();
  process.env.FAKE_KEEP_STATE = "1";
  const exit = await run(paths);
  expect(Exit.isFailure(exit)).toBe(true);
  const report = readReport(paths);
  expect(report.audit.status).toBe("failure");
  expect(report.cleanupFailures).toContain("audit: planned worker still present");
}, 30_000);
