import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { describeError, isCheckedFence, stripTwoslash } from "../twoslash.ts";

const site = join(import.meta.dirname, "..");

test("plain ts fences are checked, titled and opted-out fences are not", () => {
  expect(isCheckedFence("ts", "")).toBe(true);
  expect(isCheckedFence("ts", "include tutorial-streams")).toBe(true);
  expect(isCheckedFence("ts", 'title="src/app.ts"')).toBe(false);
  expect(isCheckedFence("ts", "", "src/app.ts")).toBe(false);
  expect(isCheckedFence("ts", "no-twoslash")).toBe(false);
  expect(isCheckedFence("tsx", "")).toBe(false);
  expect(isCheckedFence("bash", "")).toBe(false);
});

test("stripTwoslash keeps only the code that readers see", () => {
  const code = [
    "// @filename: streams.ts",
    'export * from "./shop-streams.ts";',
    "// @filename: index.ts",
    "// ---cut---",
    'import { Orders } from "./streams.ts";',
    "// ---cut-start---",
    "declare const hidden: number;",
    "// ---cut-end---",
    "const shown = Orders;",
    "//    ^?",
    "// ---cut-after---",
    "const assertion: true = true;",
  ].join("\n");
  expect(stripTwoslash(code)).toBe('import { Orders } from "./streams.ts";\nconst shown = Orders;');
});

test("stripTwoslash leaves code without notations unchanged", () => {
  const code = 'import { Effect } from "effect";\n\nconst a = 1; // a comment';
  expect(stripTwoslash(code)).toBe(code);
});

test("describeError quotes the line at each offset, after includes are expanded", () => {
  const includes = new Map([["page-streams", "// @filename: streams.ts\nexport const a = 1;"]]);
  const code = "// @include: page-streams\n// @filename: index.ts\nconst b: string = a;";
  const offset = "// @filename: streams.ts\nexport const a = 1;\n// @filename: index.ts\n".length;
  const message = `Compiler Errors:\n\n[2322] index.ts:${offset} - Type 'number' is not assignable to type 'string'.`;
  expect(describeError(message, code, includes)).toBe(
    `Compiler Errors:\n\n[2322] index.ts:${offset} - Type 'number' is not assignable to type 'string'.\n    at: const b: string = a;`,
  );
});

// The TypeScript 7 sync API that Twoslash uses does not run under Bun, so this part runs
// the site transformer in Node, as `vite build` does. It needs the packages built.
const highlight = (code: string) =>
  spawnSync(
    "node",
    [
      "--input-type=module",
      "-e",
      `import { codeToHast } from "shiki";
import { twoslash } from "./twoslash.ts";
await codeToHast(process.env.CODE, { lang: "ts", theme: "github-dark", meta: { __raw: "" }, transformers: [twoslash()] });`,
    ],
    { cwd: site, encoding: "utf8", env: { ...process.env, CODE: code } },
  );

const header =
  '// @filename: streams.ts\nexport * from "./shop-streams.ts";\n// @filename: index.ts\n// ---cut---\n';

test("a block that type-checks against the shared streams passes", () => {
  const result = highlight(
    `${header}import { Orders } from "./streams.ts";\nexport const orders = Orders;\n`,
  );
  expect(result.stderr).toBe("");
  expect(result.status).toBe(0);
}, 60_000);

test("a type error in a block fails with the failing line", () => {
  const result = highlight(`${header}const id: number = "o-17";\n`);
  expect(result.status).not.toBe(0);
  expect(result.stderr).toContain("Errors were thrown in the sample");
  expect(result.stderr).toContain('at: const id: number = "o-17";');
}, 60_000);
