import { test, expect } from "bun:test";
import { access, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { assertExcerpt, pairs, runExcerpt } from "./check-excerpts.ts";

const exists = async (path: string): Promise<boolean> => {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
};

const waitForFile = async (path: string, timeoutMs = 1_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!(await exists(path))) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${path}`);
    await delay(10);
  }
};

const isAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ESRCH") return false;
    throw error;
  }
};

const pair = { doc: "guide.mdx", source: "src/example.ts", execute: false } as const;
const code = "export const answer = 42;";
const citedDocument = `Source: [${pair.source}](https://example.test/${pair.source}).\n\n\x60\x60\x60ts\n${code}\n\x60\x60\x60`;

test("excerpt validation requires a citation outside the code fence", () => {
  expect(() => assertExcerpt(pair, `\x60\x60\x60ts\n${code}\n\x60\x60\x60`, code)).toThrow(
    "citation",
  );
});

test("excerpt validation rejects code drift", () => {
  expect(() => assertExcerpt(pair, citedDocument, "export const answer = 7;")).toThrow("verbatim");
});

test("excerpt execution is restricted to the explicit allow-list", () => {
  const withExtraCitedProgram = `${citedDocument}\n[other](https://example.test/other.ts)`;
  expect(() => assertExcerpt(pair, withExtraCitedProgram, code)).not.toThrow();
  expect(pairs.map((item) => item.source)).not.toContain("other.ts");
});

test("hung excerpt descendants and owned state are reaped before root removal", async () => {
  const probeRoot = await mkdtemp(join(tmpdir(), ".streamsy-excerpt-probe-"));
  const source = join(probeRoot, "hung.mjs");
  const pidFile = join(probeRoot, "child.pid");
  await writeFile(
    source,
    [
      'import { writeFileSync } from "node:fs";',
      'import { spawn } from "node:child_process";',
      "const root = process.env.STREAMSY_EXCERPT_ROOT;",
      "const pidFile = process.env.PROBE_PID_FILE;",
      'writeFileSync(`${root}/state`, "owned");',
      'const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });',
      "writeFileSync(pidFile, String(child.pid));",
      "setInterval(() => {}, 1000);",
    ].join("\n"),
  );

  let ownedRoot = "";
  try {
    const execution = runExcerpt(source, "hung excerpt", {
      timeoutMs: 300,
      graceMs: 100,
      reapTimeoutMs: 1_000,
      environment: { PROBE_PID_FILE: pidFile },
      onRoot: (path) => {
        ownedRoot = path;
      },
    });
    await waitForFile(pidFile);
    await expect(execution).rejects.toThrow("exceeded 300 ms");
    const pid = Number.parseInt(await readFile(pidFile, "utf8"), 10);
    expect(isAlive(pid)).toBe(false);
    expect(await exists(ownedRoot)).toBe(false);
  } finally {
    await rm(probeRoot, { recursive: true, force: true });
  }
});
