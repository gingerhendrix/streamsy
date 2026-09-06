import { readdir, readFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { staleTerms } from "../../scripts/site-stale-terms";

const site = new URL("..", import.meta.url).pathname;
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
      for (const [index, line] of lines.entries()) {
        for (const term of staleTerms) {
          if (line.toLowerCase().includes(term.toLowerCase())) {
            console.error(`${relative(site, path)}:${index + 1}: forbidden term ${term}`);
            hits++;
          }
        }
      }
    }
  }
}
await scan(site);
if (files === 0) throw new Error("Empty site language scope");
console.log(
  `Site terms: ${hits} hits across ${files} authored files (all content, docs and articles).`,
);
if (hits > 0) process.exit(1);
