import { readdir, readFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { staleTerms } from "../../scripts/site-stale-terms";

const site = new URL("..", import.meta.url).pathname;
const repo = dirname(site);
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
      }
    }
  }
}
await scan(site);
if (files === 0) throw new Error("Empty site language scope");

const rootManifest = JSON.parse(await readFile(join(repo, "package.json"), "utf8"));
const effectPin: unknown = rootManifest.overrides?.effect;
if (typeof effectPin !== "string") throw new Error("Root manifest has no Effect pin");
for (const path of [join(site, "content", "docs"), join(site, "src")]) {
  for (const entry of await Array.fromAsync(new Bun.Glob("**/*.{md,mdx,ts,tsx}").scan(path))) {
    const source = await readFile(join(path, entry), "utf8");
    for (const match of source.matchAll(/\bEffect(?:@|\s+)(\d+\.\d+\.\d+(?:-[\w.]+)?)/g)) {
      if (match[1] !== effectPin) {
        console.error(
          `${relative(site, join(path, entry))}: Effect version ${match[1]} does not match ${effectPin}`,
        );
        hits++;
      }
    }
  }
}

const packageNames: Array<string> = [];
for (const entry of await readdir(join(repo, "packages"), { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const manifestPath = join(repo, "packages", entry.name, "package.json");
  try {
    const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
    if (manifest.private !== true && typeof manifest.name === "string")
      packageNames.push(manifest.name);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
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
