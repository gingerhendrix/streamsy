import { existsSync, readFileSync } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import { basename, isAbsolute, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { codeFences, fenceTitle } from "./mdx-code.ts";

const repo = fileURLToPath(new URL("../../", import.meta.url));

const demos = fileURLToPath(new URL("../content/docs/demos/", import.meta.url));

/**
 * Each demo page, by slug, and the example folders that its titled excerpts quote.
 * The first folder is the page's own example. A fence title is a path relative to
 * it. A title that starts with the name of another listed folder, for example
 * `issue-tracker-demo-contract/api.ts`, quotes that folder. `index` lists the
 * demos and quotes no example. Every other page in `content/docs/demos` needs an
 * entry.
 */
export const demoExamples = new Map<string, readonly [string, ...Array<string>]>([
  ["hackernews-newest-stream", ["examples/hackernews-newest-stream"]],
  ["hex-board", ["examples/hex-board"]],
  ["issue-tracker", ["examples/issue-tracker-demo", "examples/issue-tracker-demo-contract"]],
  [
    "issue-tracker-event-sourced",
    [
      "examples/issue-tracker-demo-event-sourced",
      "examples/issue-tracker-demo",
      "examples/issue-tracker-demo-contract",
    ],
  ],
  ["bugsy", ["examples/bugsy"]],
  ["memory-server", ["examples/memory-server"]],
  ["fold-agent", ["examples/fold-agent"]],
]);

const excerptLanguages = new Set(["ts", "tsx"]);

/** An excerpt line is `// ...` when it marks lines that the page leaves out. */
const isElision = (line: string): boolean => line.trim().startsWith("// ...");

/**
 * The runs of an excerpt between `// ...` lines. Each run must be a contiguous run of
 * source lines. Blank lines at the start and end of a run do not count. Lines lose
 * their trailing spaces.
 */
export const excerptSegments = (body: string): ReadonlyArray<ReadonlyArray<string>> => {
  const segments: Array<Array<string>> = [[]];

  for (const line of body.split("\n").map((text) => text.trimEnd())) {
    if (isElision(line)) segments.push([]);
    else segments.at(-1)?.push(line);
  }

  return segments.flatMap((segment) => {
    const first = segment.findIndex((line) => line !== "");
    const last = segment.findLastIndex((line) => line !== "");

    return first === -1 ? [] : [segment.slice(first, last + 1)];
  });
};

const indented = (line: string, indent: string): string => (line === "" ? "" : `${indent}${line}`);

/**
 * Where a segment starts in the source at or after `from`, or the first segment line that
 * no candidate start reaches. A candidate start is a source line equal to the first
 * segment line. The drifted line is the one after the longest matching prefix.
 */
const findSegment = (
  segment: ReadonlyArray<string>,
  sourceLines: ReadonlyArray<string>,
  from: number,
  indent: string,
): { readonly at: number } | { readonly missing: string } => {
  let longest = 0;

  for (let start = from; start < sourceLines.length; start++) {
    let matched = 0;

    while (
      matched < segment.length &&
      sourceLines[start + matched] === indented(segment[matched] ?? "", indent)
    ) {
      matched++;
    }

    if (matched === segment.length) return { at: start };
    longest = Math.max(longest, matched);
  }

  return { missing: segment[longest] ?? "" };
};

const missingWithIndent = (
  segments: ReadonlyArray<ReadonlyArray<string>>,
  sourceLines: ReadonlyArray<string>,
  indent: string,
): string | undefined => {
  let position = 0;

  for (const segment of segments) {
    const found = findSegment(segment, sourceLines, position, indent);

    if ("missing" in found) return found.missing;
    position = found.at + segment.length;
  }

  return undefined;
};

const leadingSpaces = (line: string): number => line.length - line.trimStart().length;

/**
 * The first excerpt line that drifted from the source, or `undefined` when the excerpt
 * matches. Each run between `// ...` lines must be a contiguous run of source lines, and
 * the runs must be in source order. Lines compare exactly after trailing spaces are
 * removed. The formatter removes the common indentation of a fence, so the whole excerpt
 * may sit a constant number of spaces to the left of the source. Relative indentation
 * must still match.
 */
export const firstMissingLine = (body: string, source: string): string | undefined => {
  const sourceLines = source.split("\n").map((line) => line.trimEnd());
  const segments = excerptSegments(body);
  const unshifted = missingWithIndent(segments, sourceLines, "");

  if (unshifted === undefined) return undefined;
  const shifts = new Set(sourceLines.map(leadingSpaces).filter((spaces) => spaces > 0));

  for (const shift of shifts) {
    if (missingWithIndent(segments, sourceLines, " ".repeat(shift)) === undefined) return undefined;
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
      const shown = missing.trim() === "" ? "(a blank line)" : missing.trim();
      problems.push(`line ${fence.line}: ${title} drifts from the source at: ${shown}`);
    }
  }

  return problems;
};

/**
 * Where a fence title points: the listed folder whose name is the first part of the
 * title, with the rest of the title, or else the first folder with the whole title.
 */
export const resolveTitle = (
  folders: readonly [string, ...Array<string>],
  title: string,
): { readonly folder: string; readonly path: string } => {
  const [head, ...rest] = normalize(title).split("/");
  const quoted = folders.slice(1).find((folder) => basename(folder) === head);

  return quoted === undefined || rest.length === 0
    ? { folder: folders[0], path: title }
    : { folder: quoted, path: rest.join("/") };
};

const exampleReader =
  (folders: readonly [string, ...Array<string>]): SourceReader =>
  (title) => {
    const { folder, path } = resolveTitle(folders, title);
    const file = join(repo, folder, path);

    return existsSync(file) ? readFileSync(file, "utf8") : undefined;
  };

export const checkDemoExcerpts = async (): Promise<void> => {
  const failures: Array<string> = [];

  for (const example of new Set([...demoExamples.values()].flat())) {
    if (!existsSync(join(repo, example))) failures.push(`missing example folder ${example}`);
  }

  const pages = (await readdir(demos))
    .filter((name) => name.endsWith(".mdx"))
    .map((name) => name.slice(0, -".mdx".length))
    .toSorted();

  for (const page of pages) {
    if (page === "index") continue;
    const folders = demoExamples.get(page);

    if (folders === undefined) {
      failures.push(`${page}: no example folder in demoExamples`);
      continue;
    }

    const text = await readFile(join(demos, `${page}.mdx`), "utf8");
    const problems = checkDemoPage(text, exampleReader(folders));

    if (problems.length === 0) {
      console.log(`ok demo excerpts ${page} ← ${folders.join(", ")}`);
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
