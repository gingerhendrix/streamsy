import { Array, Order } from "effect";
import { z } from "zod";
import { readdir, readFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { staleTerms, retiredNames } from "../../scripts/site-stale-terms";

const site = new URL("..", import.meta.url).pathname;

const repo = dirname(site);

const manifestSchema = z.object({
  name: z.string().optional(),
  private: z.boolean().optional(),
  overrides: z.object({ effect: z.string().optional() }).optional(),
});

const rootManifest = manifestSchema.parse(
  JSON.parse(await readFile(join(repo, "package.json"), "utf8")),
);

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

/**
 * Published articles that record the API of their date. Gareth owns their text, so the
 * term scan does not ask for edits to them.
 */
const frozenArticles = new Set(["content/articles/the-simplest-possible-sync-engine.mdx"]);

let files = 0;

let hits = 0;

async function scan(directory: string): Promise<void> {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);

    if (entry.isDirectory()) {
      if (!skipped.has(entry.name)) await scan(path);
    } else if (/\.(?:mdx?|json|[cm]?[jt]sx?|html|css|svg|txt)$/.test(entry.name)) {
      const sitePath = relative(site, path);

      if (frozenArticles.has(sitePath)) continue;
      files++;
      const lines = (await readFile(path, "utf8")).split("\n");

      for (const [index, line] of lines.entries()) {
        for (const term of [...staleTerms, ...retiredNames]) {
          if (
            retiredNames.includes(term)
              ? line.includes(term)
              : line.toLowerCase().includes(term.toLowerCase())
          ) {
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
    const manifest = manifestSchema.parse(JSON.parse(await readFile(manifestPath, "utf8")));

    if (manifest.private !== true && manifest.name !== undefined) packageNames.push(manifest.name);
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
  }
}

const docsIndex = await readFile(join(site, "content", "docs", "index.mdx"), "utf8");

// The package list is a list item (`- \`@streamsy/x\`:`) or a table row (`| \`@streamsy/x\` |`).
const documentedPackages = Array.map(
  [...docsIndex.matchAll(/^(?:- `(@streamsy\/[^`]+)`:|\| `(@streamsy\/[^`]+)` +\|)/gm)],
  (match) => match[1] ?? match[2],
);

const expectedPackages = Array.sort(packageNames, Order.String);

const actualPackages = Array.sort(documentedPackages, Order.String);

if (JSON.stringify(actualPackages) !== JSON.stringify(expectedPackages)) {
  console.error(
    `content/docs/index.mdx: package set ${Array.join(actualPackages, ", ")} does not match manifests ${Array.join(expectedPackages, ", ")}`,
  );
  hits++;
}

console.log(
  `Site terms: ${hits} hits across ${files} authored files (all content, docs and articles).`,
);

if (hits > 0) process.exit(1);
