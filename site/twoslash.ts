import { Record } from "effect";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { transformerTwoslash } from "fumadocs-twoslash";
import type { Code } from "mdast";
import { defaultHandlers, type Handle } from "mdast-util-to-markdown";
import type { ShikiTransformer } from "shiki";

/**
 * The workspace package that the docs code blocks compile in. Its `node_modules` holds
 * `effect`, `@streamsy/*` and the platform packages, and its `src` folder holds the
 * modules that many blocks share. The `@streamsy/*` packages must be built first.
 */
const snippetsPackage = join(process.cwd(), "../tools/docs-snippets");

/**
 * Shared modules, put beside every block. A block imports one through a hidden
 * `streams.ts`, for example `export * from "./shop-streams.ts";`.
 */
const sharedModules = ["shop-streams.ts", "notes-streams.ts"];

/** Compiler options for docs code blocks, on top of the Twoslash defaults. */
export const snippetCompilerOptions = {
  target: "ES2023",
  lib: ["ES2023", "DOM"],
  types: ["bun"],
  allowImportingTsExtensions: true,
  noUnusedLocals: false,
  allowUnusedLabels: true,
};

/**
 * The Twoslash options for docs code blocks. Hover popups are off: Effect types are long,
 * and popups on every identifier made the docs bundle about 10 times larger, so the
 * nitro build ran out of memory. A block can turn them on with
 * `// @noStaticSemanticInfo: false`, or ask for one type with a `^?` query.
 */
export const snippetTwoslashOptions = () => ({
  cwd: snippetsPackage,
  compilerOptions: snippetCompilerOptions,
  handbookOptions: { noStaticSemanticInfo: true },
  extraFiles: Record.fromEntries(
    sharedModules.map((name) => [name, readFileSync(join(snippetsPackage, "src", name), "utf8")]),
  ),
});

/**
 * True when Twoslash checks this fence: a `ts` fence without a `title`, and without
 * `no-twoslash` in the meta. A fence with a `title` quotes an example file, and
 * `check:demos` checks it against that file.
 */
export const isCheckedFence = (lang: string, meta: string, title?: string): boolean =>
  lang === "ts" &&
  title === undefined &&
  !/(?:^|\s)title=/.test(meta) &&
  !/(?:^|\s)no-twoslash(?:\s|$)/.test(meta);

const includeMarker = /\/\/ @include: (.*)$/gm;

const compilerError = /^\[\d+\] [\w.-]+:(\d+) - .*$/gm;

/**
 * Twoslash reports a compiler error as `[code] file:offset`, where the offset counts
 * characters in the block after `// @include:` lines are expanded. This adds the line
 * of the block at that offset, so the message shows which code failed.
 */
export const describeError = (
  message: string,
  code: string,
  includes: ReadonlyMap<string, string>,
): string => {
  const expanded = code.replaceAll(includeMarker, (line, key: string) => includes.get(key) ?? line);

  return message.replaceAll(compilerError, (whole, offset: string) => {
    const start = expanded.lastIndexOf("\n", Number(offset) - 1) + 1;
    const end = expanded.indexOf("\n", start);
    const line = expanded.slice(start, end === -1 ? undefined : end).trim();

    return `${whole}\n    at: ${line}`;
  });
};

/**
 * Type-checks docs code blocks while fumadocs compiles the MDX. A compiler error that
 * the block does not expect with `// @errors` throws, so `vite build` fails.
 */
export const twoslash = (): ShikiTransformer => {
  const includesMap = new Map<string, string>();

  return transformerTwoslash({
    // fumadocs parses `title` out of the meta string, so `__raw` no longer holds it.
    filter: (lang, _code, options) =>
      // oxlint-disable-next-line eslint/no-underscore-dangle -- __raw is Shiki’s public metadata field.
      isCheckedFence(lang, options.meta?.__raw ?? "", options.meta?.title),
    twoslashOptions: snippetTwoslashOptions(),
    includesMap,
    onTwoslashError: (error, code) => {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(describeError(message, code, includesMap), { cause: error });
    },
  });
};

const flagLine = /^\/\/\s?@\w+(?::.*)?$/;

const queryLine = /^\s*\/\/\s*\^(?:\?|\||\^+)(?: .*)?$/;

const cutLine = (line: string, kind = ""): boolean => line.trim() === `// ---cut${kind}---`;

/**
 * The code that a reader sees: Twoslash removes the same lines when it renders the block.
 * The lines before the last `---cut---`, from `---cut-after---` to the end, between
 * `---cut-start---` and `---cut-end---`, and `// @flag` and `^?` lines are left out.
 */
export const stripTwoslash = (code: string): string => {
  let lines = code.split("\n");
  const lastCut = lines.findLastIndex((line) => cutLine(line) || cutLine(line, "-before"));

  if (lastCut !== -1) lines = lines.slice(lastCut + 1);
  const cutAfter = lines.findIndex((line) => cutLine(line, "-after"));

  if (cutAfter !== -1) lines = lines.slice(0, cutAfter);
  const shown: Array<string> = [];
  let hidden = 0;

  for (const line of lines) {
    if (cutLine(line, "-start")) hidden++;
    else if (cutLine(line, "-end")) hidden--;
    else if (hidden === 0 && !flagLine.test(line) && !queryLine.test(line)) shown.push(line);
  }

  return shown.join("\n");
};

/**
 * Writes a checked code block to the processed Markdown of a page as readers see it,
 * without the hidden setup and the `include` meta. Other code blocks are unchanged.
 */
export const processedMarkdownCode: Handle = (node: Code, parent, state, info) => {
  const meta = node.meta ?? "";

  if (!isCheckedFence(node.lang ?? "", meta))
    return defaultHandlers.code(node, parent, state, info);

  const shown: Code = {
    ...node,
    value: stripTwoslash(node.value),
    meta: meta.replace(/(?:^|\s)include\s+[\w-]+/, "").trim() || null,
  };

  return defaultHandlers.code(shown, parent, state, info);
};
