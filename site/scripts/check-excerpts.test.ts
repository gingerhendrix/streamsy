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

/** Resolves with the rejection error; the declared `rejects` matcher is not a Promise. */
const rejection = async (execution: Promise<void>): Promise<Error> => {
  try {
    await execution;
  } catch (error) {
    if (error instanceof Error) return error;
    throw new Error("The excerpt run rejected with a non-Error value", { cause: error });
  }
  throw new Error("Expected the excerpt run to reject");
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
const repositorySourcePrefix =
  "https://github.com/gingerhendrix/streamsy/blob/effect-first-live-perimeter/";
const citation = `Compiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}).`;
const citedDocument = `${citation}\n\n\x60\x60\x60ts\n${code}\n\x60\x60\x60`;

test("excerpt validation requires a citation outside the code fence", () => {
  expect(() => assertExcerpt(pair, `\x60\x60\x60ts\n${code}\n\x60\x60\x60`, code)).toThrow(
    "citation",
  );
});

test("excerpt validation requires a rendered source destination", () => {
  const bypasses = [
    `Compiled source: \\[${pair.source}](${repositorySourcePrefix}${pair.source}).`,
    `Compiled source: [${pair.source}] (${repositorySourcePrefix}${pair.source}).`,
    `Compiled source: [${pair.source}]\n(${repositorySourcePrefix}${pair.source}).`,
    `Compiled source: ![${pair.source}](${repositorySourcePrefix}${pair.source}).`,
    `Compiled source: ![outer [${pair.source}](${repositorySourcePrefix}${pair.source})](image.png).`,
    `~~~md\nCompiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}).\n~~~`,
    `~~~md\n~~~still-code\nCompiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}).\n~~~`,
    `> ~~~md\n> Compiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}).\n> ~~~`,
    `<pre>\nCompiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}).\n</pre>`,
    `\`Compiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}).\``,
    `<!-- Compiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}). -->`,
    `    Compiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}).`,
    `Compiled source: [${pair.source}](${repositorySourcePrefix}unrelated.ts).`,
    `Compiled source: [${pair.source}](${repositorySourcePrefix}unrelated?source=${pair.source}).`,
    `Compiled source: [${pair.source}](${repositorySourcePrefix}unrelated#${pair.source}).`,
    `Compiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}.bak).`,
  ];
  for (const bypass of bypasses) {
    expect(() =>
      assertExcerpt(pair, `${bypass}\n\n\x60\x60\x60ts\n${code}\n\x60\x60\x60`, code),
    ).toThrow("citation");
  }
});

test("excerpt validation accepts only the exact repository path with an optional line anchor", () => {
  const withLineAnchor =
    `Compiled source: [${pair.source}](${repositorySourcePrefix}${pair.source}#L12-L18).\n\n` +
    `\x60\x60\x60ts\n${code}\n\x60\x60\x60`;
  expect(() => assertExcerpt(pair, withLineAnchor, code)).not.toThrow();
});

test("excerpt validation rejects ambiguous multiline Markdown context", () => {
  const multilineCases = [
    `\x60start\n${citation}\nend\x60`,
    `\x60\x60start\n${citation}\nend\x60\x60`,
    `![outer\n${citation}\n](image.png)`,
    `~~~md\n~~~<!-- still code -->\n${citation}\n~~~`,
    `~~~md\n~~~\x60\x60\x60\n${citation}\n~~~`,
    `~~~md\n> ~~~\n${citation}\n~~~`,
    `Intro <!-- first --><!--\n${citation}\n-->`,
    citation.replace(`${pair.source})`, `src/ex<!--x-->ample.ts)`),
    citation.replace(`${pair.source})`, `src/ex\x60x\x60ample.ts)`),
  ];
  for (const candidate of multilineCases) {
    expect(() =>
      assertExcerpt(pair, `${candidate}\n\n\x60\x60\x60ts\n${code}\n\x60\x60\x60`, code),
    ).toThrow("citation");
  }
});

test("excerpt validation rejects parser-only code, image and raw HTML contexts", () => {
  const nbsp = "\u00a0";
  const parserContexts = [
    `~~~\x60\x60\x60\n${citation}\n~~~`,
    `\x60\x60\x60~~~\n${citation}\n\x60\x60\x60`,
    `~~~<pre>\n</pre>\n${citation}\n~~~`,
    `<!--\n~~~\n-->\n~~~\n${citation}\n~~~`,
    `\x60start\n${nbsp}\n${citation}\n${nbsp}\nend\x60`,
    `\x60\x60start\n${nbsp}\n${citation}\n${nbsp}\nend\x60\x60`,
    `![outer\n${nbsp}\n${citation}\n${nbsp}\n](image.png)`,
    `~~~md\n~~~${nbsp}\n\n${citation}\n\n~~~`,
    `<script>\n\n${citation}\n\n</script>`,
    `<style>\n\n${citation}\n\n</style>`,
  ];
  for (const candidate of parserContexts) {
    expect(() =>
      assertExcerpt(pair, `${candidate}\n\n\x60\x60\x60ts\n${code}\n\x60\x60\x60`, code),
    ).toThrow("citation");
  }
});

test("excerpt validation requires a top-level citation paragraph", () => {
  expect(() =>
    assertExcerpt(pair, `Intro ${citation}\n\n\x60\x60\x60ts\n${code}\n\x60\x60\x60`, code),
  ).toThrow("citation");
  expect(() =>
    assertExcerpt(
      pair,
      `${citation} trailing prose\n\n\x60\x60\x60ts\n${code}\n\x60\x60\x60`,
      code,
    ),
  ).toThrow("citation");
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
    expect((await rejection(execution)).message).toContain("exceeded 300 ms");
    const pid = Number.parseInt(await readFile(pidFile, "utf8"), 10);
    expect(isAlive(pid)).toBe(false);
    expect(await exists(ownedRoot)).toBe(false);
  } finally {
    await rm(probeRoot, { recursive: true, force: true });
  }
});

test("resistant descendants are hard-killed at the outer deadline", async () => {
  const probeRoot = await mkdtemp(join(tmpdir(), ".streamsy-excerpt-resistant-"));
  const source = join(probeRoot, "resistant.mjs");
  const pidFile = join(probeRoot, "child.pid");
  const readyFile = join(probeRoot, "child.ready");
  await writeFile(
    source,
    [
      'import { writeFileSync } from "node:fs";',
      'import { spawn } from "node:child_process";',
      "const root = process.env.STREAMSY_EXCERPT_ROOT;",
      "const pidFile = process.env.PROBE_PID_FILE;",
      "const readyFile = process.env.PROBE_READY_FILE;",
      "const child = spawn(process.execPath, [\"-e\", `process.on('SIGTERM', () => {}); require('node:fs').writeFileSync(process.env.PROBE_READY_FILE, 'ready'); setInterval(() => {}, 1000);`], {",
      "  env: { ...process.env, PROBE_READY_FILE: readyFile },",
      '  stdio: "ignore",',
      "});",
      "writeFileSync(pidFile, String(child.pid));",
      'process.on("SIGTERM", () => process.exit(0));',
      'writeFileSync(`${root}/state`, "owned");',
      "setInterval(() => {}, 1000);",
    ].join("\n"),
  );

  let ownedRoot = "";
  let execution: Promise<void> | undefined;
  const timeoutMs = 300;
  const startedAt = Date.now();
  try {
    execution = runExcerpt(source, "resistant excerpt", {
      timeoutMs,
      graceMs: 100,
      reapTimeoutMs: 1_000,
      environment: { PROBE_PID_FILE: pidFile, PROBE_READY_FILE: readyFile },
      onRoot: (path) => {
        ownedRoot = path;
      },
    });
    await waitForFile(readyFile);
    const pid = Number.parseInt(await readFile(pidFile, "utf8"), 10);
    const beforeDeadline = Math.max(0, timeoutMs - (Date.now() - startedAt) - 30);
    await delay(beforeDeadline);
    expect(isAlive(pid)).toBe(true);
    expect((await rejection(execution)).message).toContain("exceeded 300 ms");
    expect(Date.now() - startedAt).toBeLessThan(timeoutMs + 700);
    expect(isAlive(pid)).toBe(false);
    expect(await exists(ownedRoot)).toBe(false);
  } finally {
    await execution?.catch(() => undefined);
    await rm(probeRoot, { recursive: true, force: true });
  }
});

test("excerpt citations accept the Step 4 source branch and reject unrelated refs", () => {
  expect(() =>
    assertExcerpt(
      pair,
      citedDocument.replace("effect-first-live-perimeter", "step-4-fetch-transport"),
      code,
    ),
  ).not.toThrow();
  expect(() =>
    assertExcerpt(pair, citedDocument.replace("effect-first-live-perimeter", "unrelated"), code),
  ).toThrow("citation");
});
