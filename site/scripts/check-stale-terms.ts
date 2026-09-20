import { readdir, readFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { staleTerms } from "../../scripts/site-stale-terms";

const site = new URL("..", import.meta.url).pathname;
const repo = dirname(site);
interface Manifest {
  readonly name?: string;
  readonly private?: boolean;
  readonly overrides?: { readonly effect?: string };
}
// SAFETY: package manifests are trusted repository JSON; optional fields are checked before use.
const rootManifest = JSON.parse(await readFile(join(repo, "package.json"), "utf8")) as Manifest;
const effectPin = rootManifest.overrides?.effect;
if (effectPin === undefined) throw new Error("Root manifest has no Effect pin");
const skipped = new Set([
  "node_modules",
  ".git",
  ".source",
  ".output",
  ".tanstack",
  "dist",
  ".alchemy",
  ".wrangler",
]);
let files = 0;
let hits = 0;
async function scan(directory: string): Promise<void> {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (!skipped.has(entry.name)) await scan(path);
    } else if (/\.(?:mdx?|json|[cm]?[jt]sx?|html|css|svg|txt)$/.test(entry.name)) {
      files++;
      const lines = (await readFile(path, "utf8")).split("\n");
      const sitePath = relative(site, path);
      for (const [index, line] of lines.entries()) {
        for (const term of staleTerms) {
          if (line.toLowerCase().includes(term.toLowerCase())) {
            console.error(`${sitePath}:${index + 1}: forbidden term ${term}`);
            hits++;
          }
        }
        if (sitePath.startsWith("content/docs/") || sitePath.startsWith("src/")) {
          for (const match of line.matchAll(/\bEffect(?:@|\s+)(\d+\.\d+\.\d+(?:-[\w.]+)?)/g)) {
            const version = match[1];
            if (version !== undefined && version !== effectPin) {
              console.error(
                `${sitePath}:${index + 1}: Effect version ${version} does not match ${effectPin}`,
              );
              hits++;
            }
          }
        }
      }
    }
  }
}
await scan(site);
if (files === 0) throw new Error("Empty site language scope");

const packageNames: Array<string> = [];
for (const entry of await readdir(join(repo, "packages"), { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const manifestPath = join(repo, "packages", entry.name, "package.json");
  try {
    // SAFETY: package manifests are trusted repository JSON; optional fields are checked before use.
    const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as Manifest;
    if (manifest.private !== true && manifest.name !== undefined) packageNames.push(manifest.name);
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
  }
}
const docsIndex = await readFile(join(site, "content", "docs", "index.mdx"), "utf8");
const documentedPackages = [...docsIndex.matchAll(/^- `(@streamsy\/[^`]+)`:/gm)].map(
  (match) => match[1],
);
const expectedPackages = packageNames.toSorted();
const actualPackages = documentedPackages.toSorted();
if (JSON.stringify(actualPackages) !== JSON.stringify(expectedPackages)) {
  console.error(
    `content/docs/index.mdx: package set ${actualPackages.join(", ")} does not match manifests ${expectedPackages.join(", ")}`,
  );
  hits++;
}
console.log(
  `Site terms: ${hits} hits across ${files} authored files (all content, docs and articles).`,
);
if (hits > 0) process.exit(1);
