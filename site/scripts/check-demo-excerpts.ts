import { existsSync, readFileSync } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import { isAbsolute, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { codeFences, fenceTitle } from "./mdx-code.ts";

const repo = fileURLToPath(new URL("../../", import.meta.url));
const demos = fileURLToPath(new URL("../content/docs/demos/", import.meta.url));

/**
 * Each demo page, by slug, and the example folder that its titled excerpts quote.
 * A fence title is a path relative to this folder. `index` lists the demos and
 * quotes no example. Every other page in `content/docs/demos` needs an entry.
 */
export const demoExamples = new Map<string, string>([
  ["hackernews-newest-stream", "examples/hackernews-newest-stream"],
  ["hex-board", "examples/hex-board"],
  ["issue-tracker", "examples/issue-tracker"],
  ["issue-tracker-demo", "examples/issue-tracker-demo"],
  ["memory-server", "examples/memory-server"],
  // The fold-agent page is added on another branch. The entry can come first.
  ["fold-agent", "examples/fold-agent"],
]);

/**
 * Pages that still have untitled excerpts. Another branch moves them to titled
 * excerpts. The check reports their problems but does not fail on them, and it
 * fails when a listed page passes, so the list gets shorter as pages move.
 * Remove this list when all demo pages use titled excerpts.
 */
export const notYetMigrated: ReadonlySet<string> = new Set(["hex-board"]);

const excerptLanguages = new Set(["ts", "tsx"]);

/** An excerpt line is `// ...` when it marks lines that the page leaves out. */
const isElision = (line: string): boolean => line.trim().startsWith("// ...");

/** The excerpt lines that must be in the source: not blank and not an elision. */
export const excerptLines = (body: string): ReadonlyArray<string> =>
  body
    .split("\n")
    .map((line) => line.trimEnd())
    .filter((line) => line.trim() !== "" && !isElision(line));

const missingWithIndent = (
  lines: ReadonlyArray<string>,
  sourceLines: ReadonlyArray<string>,
  indent: string,
): string | undefined => {
  let position = 0;
  for (const line of lines) {
    const found = sourceLines.indexOf(`${indent}${line}`, position);
    if (found === -1) return line;
    position = found + 1;
  }
  return undefined;
};

const leadingSpaces = (line: string): number => line.length - line.trimStart().length;

/**
 * The first excerpt line that the source does not have after the previous match.
 * Lines compare exactly after trailing spaces are removed. The formatter removes the
 * common indentation of a fence, so the whole excerpt may sit a constant number of
 * spaces to the left of the source. Relative indentation must still match.
 * Returns `undefined` when all lines are an in-order run of source lines.
 */
export const firstMissingLine = (body: string, source: string): string | undefined => {
  const sourceLines = source.split("\n").map((line) => line.trimEnd());
  const lines = excerptLines(body);
  const unshifted = missingWithIndent(lines, sourceLines, "");
  if (unshifted === undefined) return undefined;
  const shifts = new Set(sourceLines.map(leadingSpaces).filter((spaces) => spaces > 0));
  for (const shift of shifts) {
    if (missingWithIndent(lines, sourceLines, " ".repeat(shift)) === undefined) return undefined;
  }
  return unshifted;
};

/** Reads a file relative to the example folder, or gives `undefined` if it is missing. */
export type SourceReader = (path: string) => string | undefined;

const isInsideExample = (path: string): boolean =>
  !isAbsolute(path) && !normalize(path).startsWith("..");

/** The problems in one demo page. An empty list means the page passes. */
export const checkDemoPage = (text: string, readSource: SourceReader): ReadonlyArray<string> => {
  const problems: Array<string> = [];
  for (const fence of codeFences(text)) {
    if (fence.lang === null || !excerptLanguages.has(fence.lang)) continue;
    const title = fenceTitle(fence.meta);
    if (title === undefined) {
      problems.push(`line ${fence.line}: ${fence.lang} fence has no title="<path>"`);
      continue;
    }
    if (!isInsideExample(title)) {
      problems.push(`line ${fence.line}: title ${title} is not a path inside the example`);
      continue;
    }
    const source = readSource(title);
    if (source === undefined) {
      problems.push(`line ${fence.line}: ${title} does not exist in the example`);
      continue;
    }
    const missing = firstMissingLine(fence.value, source);
    if (missing !== undefined) {
      problems.push(`line ${fence.line}: ${title} has no line, in order: ${missing.trim()}`);
    }
  }
  return problems;
};

const exampleReader =
  (example: string): SourceReader =>
  (path) => {
    const file = join(repo, example, path);
    return existsSync(file) ? readFileSync(file, "utf8") : undefined;
  };

export const checkDemoExcerpts = async (): Promise<void> => {
  const failures: Array<string> = [];
  for (const example of demoExamples.values()) {
    if (!existsSync(join(repo, example))) failures.push(`missing example folder ${example}`);
  }
  const pages = (await readdir(demos))
    .filter((name) => name.endsWith(".mdx"))
    .map((name) => name.slice(0, -".mdx".length))
    .toSorted();
  for (const page of notYetMigrated) {
    if (!pages.includes(page)) failures.push(`${page}: listed as not yet migrated, but no page`);
  }
  for (const page of pages) {
    if (page === "index") continue;
    const example = demoExamples.get(page);
    if (example === undefined) {
      failures.push(`${page}: no example folder in demoExamples`);
      continue;
    }
    const text = await readFile(join(demos, `${page}.mdx`), "utf8");
    const problems = checkDemoPage(text, exampleReader(example));
    if (notYetMigrated.has(page)) {
      if (problems.length === 0) {
        failures.push(`${page}: passes now; remove it from notYetMigrated`);
      } else {
        console.log(`not yet migrated ${page}: ${problems.length} problems, not enforced`);
      }
      continue;
    }
    if (problems.length === 0) {
      console.log(`ok demo excerpts ${page} ← ${example}`);
      continue;
    }
    for (const problem of problems) failures.push(`${page}.mdx ${problem}`);
  }
  if (failures.length > 0) {
    for (const failure of failures) console.error(failure);
    throw new Error(`Demo excerpt check failed with ${failures.length} problems`);
  }
};

if (import.meta.main) await checkDemoExcerpts();
