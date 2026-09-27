import { expect, test } from "bun:test";
import {
  checkDemoPage,
  demoExamples,
  excerptLines,
  firstMissingLine,
  notYetMigrated,
  type SourceReader,
} from "../../scripts/check-demo-excerpts.ts";

const fence = "\x60\x60\x60";
const source = [
  'import { Effect } from "effect";',
  "",
  "export const run = Effect.gen(function* () {",
  "  const a = yield* first;",
  "  const b = yield* second;",
  "  return a + b;",
  "});",
].join("\n");
const files = new Map([["src/run.ts", source]]);
const reader: SourceReader = (path) => files.get(path);
const page = (info: string, body: string): string =>
  `# Page\n\n${fence}${info}\n${body}\n${fence}\n`;

test("excerpt lines leave out blank lines and elisions", () => {
  expect(excerptLines("a\n\n  // ... left out ...\nb  ")).toEqual(["a", "b"]);
});

test("an in-order run of source lines passes, with elided lines", () => {
  const body = "export const run = Effect.gen(function* () {\n  // ...\n  return a + b;\n});";
  expect(firstMissingLine(body, source)).toBeUndefined();
});

test("lines out of order fail at the first line that has no later match", () => {
  expect(firstMissingLine("  return a + b;\n  const a = yield* first;", source)).toBe(
    "  const a = yield* first;",
  );
});

test("an excerpt may sit a constant number of spaces to the left", () => {
  expect(firstMissingLine("const a = yield* first;\nreturn a + b;", source)).toBeUndefined();
});

test("relative indentation must match", () => {
  expect(firstMissingLine("const a = yield* first;\n  return a + b;", source)).toBe(
    "const a = yield* first;",
  );
  expect(firstMissingLine("    return a + b;", source)).toBe("    return a + b;");
});

test("a titled excerpt that matches its file passes", () => {
  const text = page('ts title="src/run.ts"', "  const b = yield* second;\n  return a + b;");
  expect(checkDemoPage(text, reader)).toEqual([]);
});

test("tsx fences are checked too", () => {
  expect(checkDemoPage(page('tsx title="src/run.ts"', "  return a + b;"), reader)).toEqual([]);
  expect(checkDemoPage(page('tsx title="src/run.ts"', "not in the file"), reader)).toHaveLength(1);
});

test("a ts fence without a title fails", () => {
  const [problem] = checkDemoPage(page("ts", "  return a + b;"), reader);
  expect(problem).toContain("no title");
});

test("a changed line fails with the line that drifted", () => {
  const [problem] = checkDemoPage(page('ts title="src/run.ts"', "  return a - b;"), reader);
  expect(problem).toContain("return a - b;");
});

test("a missing file and a path outside the example fail", () => {
  expect(checkDemoPage(page('ts title="src/gone.ts"', "x"), reader)[0]).toContain("does not exist");
  expect(checkDemoPage(page('ts title="../other/run.ts"', "x"), reader)[0]).toContain(
    "not a path inside",
  );
  expect(checkDemoPage(page('ts title="/src/run.ts"', "x"), reader)[0]).toContain(
    "not a path inside",
  );
});

test("other languages are not checked", () => {
  expect(checkDemoPage(page("bash", "bun run dev"), reader)).toEqual([]);
});

test("every page on the allowlist has an example folder", () => {
  for (const name of notYetMigrated) expect(demoExamples.has(name)).toBe(true);
});
