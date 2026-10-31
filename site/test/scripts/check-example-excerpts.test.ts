import { Array } from "effect";
import { expect, test } from "bun:test";
import {
  checkExamplePage,
  excerptSegments,
  firstMissingLine,
  resolveTitle,
  type SourceReader,
} from "../../scripts/check-example-excerpts.ts";

const fence = "\x60\x60\x60";

const source = Array.join(
  [
    'import { Effect } from "effect";',
    "",
    "export const run = Effect.gen(function* () {",
    "  const a = yield* first;",
    "  const b = yield* second;",
    "  return a + b;",
    "});",
  ],
  "\n",
);

const files = new Map([["src/run.ts", source]]);

const reader: SourceReader = (path) => files.get(path);

const page = (info: string, body: string): string =>
  `# Page\n\n${fence}${info}\n${body}\n${fence}\n`;

test("elisions split an excerpt into runs without blank ends", () => {
  expect(excerptSegments("a\n\nb\n\n  // ... left out ...\n\nc  \n// ...")).toEqual([
    ["a", "", "b"],
    ["c"],
  ]);
});

test("an in-order run of source lines passes, with elided lines", () => {
  const body = "export const run = Effect.gen(function* () {\n  // ...\n  return a + b;\n});";
  expect(firstMissingLine(body, source)).toBeUndefined();
});

test("a gap in the source with no elision fails at the line after the gap", () => {
  expect(firstMissingLine("  const a = yield* first;\n  return a + b;", source)).toBe(
    "  return a + b;",
  );
});

test("a blank line inside a run must be in the source", () => {
  expect(firstMissingLine("  const a = yield* first;\n\n  const b = yield* second;", source)).toBe(
    "",
  );
});

test("runs out of order fail", () => {
  expect(firstMissingLine("  return a + b;\n// ...\n  const a = yield* first;", source)).toBe(
    "  const a = yield* first;",
  );
});

test("lines out of order fail at the first line that has no later match", () => {
  expect(firstMissingLine("  return a + b;\n  const a = yield* first;", source)).toBe(
    "  const a = yield* first;",
  );
});

test("an excerpt may sit a constant number of spaces to the left", () => {
  expect(firstMissingLine("const b = yield* second;\nreturn a + b;", source)).toBeUndefined();
});

test("relative indentation must match", () => {
  expect(firstMissingLine("const a = yield* first;\n  return a + b;", source)).toBe(
    "const a = yield* first;",
  );
  expect(firstMissingLine("    return a + b;", source)).toBe("    return a + b;");
});

test("a titled excerpt that matches its file passes", () => {
  const text = page('ts title="src/run.ts"', "  const b = yield* second;\n  return a + b;");
  expect(checkExamplePage(text, reader)).toEqual([]);
});

test("tsx fences are checked too", () => {
  expect(checkExamplePage(page('tsx title="src/run.ts"', "  return a + b;"), reader)).toEqual([]);
  expect(checkExamplePage(page('tsx title="src/run.ts"', "not in the file"), reader)).toHaveLength(
    1,
  );
});

test("a ts fence without a title fails", () => {
  const [problem] = checkExamplePage(page("ts", "  return a + b;"), reader);
  expect(problem).toContain("no title");
});

test("a changed line fails with the line that drifted", () => {
  const [problem] = checkExamplePage(page('ts title="src/run.ts"', "  return a - b;"), reader);
  expect(problem).toContain("return a - b;");
});

test("a missing file and a path outside the example fail", () => {
  expect(checkExamplePage(page('ts title="src/gone.ts"', "x"), reader)[0]).toContain(
    "does not exist",
  );
  expect(checkExamplePage(page('ts title="../other/run.ts"', "x"), reader)[0]).toContain(
    "not a path inside",
  );
  expect(checkExamplePage(page('ts title="/src/run.ts"', "x"), reader)[0]).toContain(
    "not a path inside",
  );
});

test("other languages are not checked", () => {
  expect(checkExamplePage(page("bash", "bun run dev"), reader)).toEqual([]);
});

test("a title that starts with a listed folder name quotes that folder", () => {
  const folders = ["examples/app", "examples/app-contract"] as const;
  expect(resolveTitle(folders, "server/index.ts")).toEqual({
    folder: "examples/app",
    path: "server/index.ts",
  });
  expect(resolveTitle(folders, "app-contract/api.ts")).toEqual({
    folder: "examples/app-contract",
    path: "api.ts",
  });
  // The page's own folder name is not a prefix: its files are quoted directly.
  expect(resolveTitle(folders, "app/server.ts")).toEqual({
    folder: "examples/app",
    path: "app/server.ts",
  });
});
