import { spawnSync } from "node:child_process";
import {
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  realpath,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { codeFences } from "./mdx-code.ts";
import { snippetSupport, type PageSupport } from "./snippets-support.ts";

const repo = fileURLToPath(new URL("../../", import.meta.url));
const docs = fileURLToPath(new URL("../content/docs/", import.meta.url));

/**
 * Folders under `content/docs` that this check leaves out. Demo pages quote their
 * examples, so `check-demo-excerpts.ts` checks them against the example files.
 */
export const excludedFolders: ReadonlyArray<string> = ["demos"];

/** Packages that the snippets import, and the workspace folder that links each one. */
const linkedDependencies: ReadonlyArray<readonly [name: string, from: string]> = [
  ["effect", "packages/core"],
  ["@types/bun", "packages/core"],
  ["@effect/platform-bun", "packages/serve"],
  ["@effect/sql-sqlite-bun", "packages/storage"],
  ["@cloudflare/workers-types", "packages/serve"],
  ["alchemy", "packages/serve"],
];
const streamsyPackages = ["core", "storage", "projection", "serve", "views"];
const tsc = join(repo, "packages/core/node_modules/typescript/bin/tsc");

const compilerOptions = {
  target: "ES2023",
  lib: ["ES2023", "DOM"],
  strict: true,
  module: "ESNext",
  moduleResolution: "Bundler",
  moduleDetection: "force",
  allowImportingTsExtensions: true,
  isolatedModules: true,
  noEmit: true,
  skipLibCheck: true,
  noUnusedLocals: false,
  allowUnusedLabels: true,
  types: ["bun"],
};

/** A `ts` block of a page, numbered from 1 in page order. */
export interface Snippet {
  readonly index: number;
  readonly code: string;
  /** Page line of the opening fence. */
  readonly line: number;
}

/** One file that the compiler sees, with the map back to the page. */
export interface SnippetFile {
  readonly name: string;
  readonly text: string;
  /** For a block: the page line of its fence, and the number of prelude lines before it. */
  readonly origin?: { readonly fenceLine: number; readonly preludeLines: number };
}

export const blockName = (index: number): string => `block${String(index).padStart(2, "0")}.ts`;

/** The `ts` blocks of a page, in page order. */
export const pageSnippets = (text: string): ReadonlyArray<Snippet> =>
  codeFences(text)
    .filter((fence) => fence.lang === "ts")
    .map((fence, position) => ({ index: position + 1, code: fence.value, line: fence.line }));

/**
 * The files for one page folder: each block, with its prelude and module name, and the
 * support modules. Throws when a support entry names a block that the page does not have,
 * because the entries then no longer match the page.
 */
export const pageFiles = (
  page: string,
  snippets: ReadonlyArray<Snippet>,
  support: PageSupport = {},
): ReadonlyArray<SnippetFile> => {
  const indexes = new Set(snippets.map((snippet) => snippet.index));
  const referenced = [
    ...Object.keys(support.renames ?? {}),
    ...Object.keys(support.preludes ?? {}),
    ...(support.skip ?? []).map(String),
  ].map(Number);
  for (const index of referenced) {
    if (!indexes.has(index)) {
      throw new Error(
        `${page}: snippet support names block ${index}, but the page has no such block`,
      );
    }
  }
  const skipped = new Set(support.skip ?? []);
  const files: Array<SnippetFile> = [];
  for (const snippet of snippets) {
    if (skipped.has(snippet.index)) continue;
    const prelude = support.preludes?.[snippet.index]?.trimEnd();
    const preludeLines = prelude === undefined ? 0 : prelude.split("\n").length;
    files.push({
      name: support.renames?.[snippet.index] ?? blockName(snippet.index),
      text: prelude === undefined ? `${snippet.code}\n` : `${prelude}\n${snippet.code}\n`,
      origin: { fenceLine: snippet.line, preludeLines },
    });
  }
  for (const [name, text] of Object.entries(support.modules ?? {})) {
    if (files.some((file) => file.name === name)) {
      throw new Error(`${page}: support module ${name} has the name of a block`);
    }
    files.push({ name, text });
  }
  return files;
};

/** The folder name for a page path, for example `streams/reading` to `streams__reading`. */
export const pageFolder = (page: string): string => page.replaceAll("/", "__");

/** Maps compiler locations such as `streams__reading/block02.ts(5,3)` back to page lines. */
export const pageLocations = (
  output: string,
  origins: ReadonlyMap<string, SnippetFile["origin"]>,
): string =>
  output.replace(/^([\w-]+\/[\w.-]+\.ts)\((\d+),(\d+)\)/gm, (whole, path: string, line: string) => {
    const origin = origins.get(path);
    if (origin === undefined) return whole;
    const page = path.slice(0, path.indexOf("/")).replaceAll("__", "/");
    const blockLine = Number(line) - origin.preludeLines;
    if (blockLine < 1) return `${whole} [prelude of content/docs/${page}.mdx:${origin.fenceLine}]`;
    return `${whole} [content/docs/${page}.mdx:${origin.fenceLine + blockLine}]`;
  });

const docsPages = async (folder: string): Promise<ReadonlyArray<string>> => {
  const pages: Array<string> = [];
  for (const entry of await readdir(join(docs, folder), { withFileTypes: true })) {
    const path = folder === "" ? entry.name : `${folder}/${entry.name}`;
    if (entry.isDirectory()) {
      if (!excludedFolders.includes(path)) pages.push(...(await docsPages(path)));
    } else if (entry.name.endsWith(".mdx")) {
      pages.push(path.slice(0, -".mdx".length));
    }
  }
  return pages.toSorted();
};

const linkDependencies = async (root: string): Promise<void> => {
  const links: Array<readonly [string, string]> = [
    ...linkedDependencies.map(
      ([name, from]) => [name, join(repo, from, "node_modules", name)] as const,
    ),
    ...streamsyPackages.map((name) => [`@streamsy/${name}`, join(repo, "packages", name)] as const),
  ];
  for (const [name, target] of links) {
    const link = join(root, "node_modules", name);
    await mkdir(dirname(link), { recursive: true });
    await symlink(await realpath(target), link);
  }
};

export const checkSnippets = async (): Promise<void> => {
  const pages = await docsPages("");
  for (const page of snippetSupport.keys()) {
    if (!pages.includes(page)) throw new Error(`Snippet support for ${page}, but no such page`);
  }
  const root = await mkdtemp(join(tmpdir(), ".streamsy-snippets-"));
  const keep = process.env.STREAMSY_SNIPPETS_KEEP === "1";
  try {
    await linkDependencies(root);
    const origins = new Map<string, SnippetFile["origin"]>();
    let blocks = 0;
    let compiledPages = 0;
    for (const page of pages) {
      const snippets = pageSnippets(await readFile(join(docs, `${page}.mdx`), "utf8"));
      if (snippets.length === 0) continue;
      compiledPages++;
      const folder = join(root, pageFolder(page));
      await mkdir(folder);
      const files = pageFiles(page, snippets, snippetSupport.get(page));
      for (const file of files) {
        await writeFile(join(folder, file.name), file.text);
        if (file.origin !== undefined) {
          origins.set(`${pageFolder(page)}/${file.name}`, file.origin);
          blocks++;
        }
      }
      console.log(`snippets ${page}: ${files.filter((file) => file.origin).length} blocks`);
    }
    await writeFile(
      join(root, "tsconfig.json"),
      JSON.stringify({ compilerOptions, include: ["*/*.ts"] }, null, 2),
    );
    const result = spawnSync(process.execPath, [tsc, "-p", "tsconfig.json", "--pretty", "false"], {
      cwd: root,
      encoding: "utf8",
    });
    if (result.error !== undefined) throw result.error;
    if (result.status !== 0) {
      console.error(pageLocations(`${result.stdout}${result.stderr}`.trim(), origins));
      throw new Error(
        `Site snippets do not compile (${relative(repo, tsc)} exit ${result.status})`,
      );
    }
    console.log(`ok snippets: ${blocks} blocks on ${compiledPages} pages compile`);
  } finally {
    if (keep) console.log(`kept snippet folder ${root}`);
    else await rm(root, { recursive: true, force: true });
  }
};

if (import.meta.main) await checkSnippets();
