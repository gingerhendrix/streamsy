import { expect, test } from "bun:test";
import {
  blockName,
  pageFiles,
  pageLocations,
  pageSnippets,
  type SnippetFile,
} from "../../scripts/check-snippets.ts";

const fence = "\x60\x60\x60";
const text = [
  "# Page",
  "",
  `${fence}ts`,
  "export const a = 1;",
  fence,
  "",
  `${fence}bash`,
  "bun run dev",
  fence,
  "",
  `${fence}ts`,
  "const b = a + 1;",
  fence,
  "",
].join("\n");

test("only ts blocks are snippets, numbered from 1 in page order", () => {
  expect(pageSnippets(text)).toEqual([
    { index: 1, code: "export const a = 1;", line: 3 },
    { index: 2, code: "const b = a + 1;", line: 11 },
  ]);
});

test("blocks keep their numbered names without support", () => {
  const names = pageFiles("page", pageSnippets(text)).map((file) => file.name);
  expect(names).toEqual([blockName(1), blockName(2)]);
});

test("support renames, prefixes, skips and adds modules", () => {
  const files = pageFiles("page", pageSnippets(text), {
    renames: { 1: "a.ts" },
    preludes: { 2: 'import { a } from "./a.ts";\n' },
    modules: { "extra.ts": "export {};\n" },
  });
  expect(files.map((file) => file.name)).toEqual(["a.ts", "block02.ts", "extra.ts"]);
  expect(files[1]?.text).toBe('import { a } from "./a.ts";\nconst b = a + 1;\n');
  expect(files[1]?.origin).toEqual({ fenceLine: 11, preludeLines: 1 });
  expect(pageFiles("page", pageSnippets(text), { skip: [1] }).map((file) => file.name)).toEqual([
    "block02.ts",
  ]);
});

test("support for a block that the page does not have fails", () => {
  expect(() => pageFiles("page", pageSnippets(text), { preludes: { 3: "export {};" } })).toThrow(
    "no such block",
  );
  expect(() => pageFiles("page", pageSnippets(text), { skip: [9] })).toThrow("no such block");
});

test("a support module cannot replace a block", () => {
  expect(() =>
    pageFiles("page", pageSnippets(text), { renames: { 1: "a.ts" }, modules: { "a.ts": "" } }),
  ).toThrow("has the name of a block");
});

test("compiler locations map back to page lines", () => {
  const origins = new Map<string, SnippetFile["origin"]>([
    ["streams__reading/block02.ts", { fenceLine: 40, preludeLines: 2 }],
  ]);
  const output = [
    "streams__reading/block02.ts(5,3): error TS1: body",
    "streams__reading/block02.ts(1,1): error TS2: prelude",
    "streams__reading/streams.ts(1,1): error TS3: module",
  ].join("\n");
  expect(pageLocations(output, origins).split("\n")).toEqual([
    "streams__reading/block02.ts(5,3) [content/docs/streams/reading.mdx:43]: error TS1: body",
    "streams__reading/block02.ts(1,1) [prelude of content/docs/streams/reading.mdx:40]: error TS2: prelude",
    "streams__reading/streams.ts(1,1): error TS3: module",
  ]);
});
