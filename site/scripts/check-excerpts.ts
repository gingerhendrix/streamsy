import { readFile } from "node:fs/promises";

const root = new URL("../../", import.meta.url);
const pairs = [
  ["packages/core/README.md", "packages/core/test/readme.ts"],
  ["site/content/docs/user/basic-usage.mdx", "packages/serve/test/basic-usage.ts"],
  ["site/content/docs/user/basic-usage.mdx", "packages/serve/test/host.ts"],
];
for (const [doc, source] of pairs) {
  const text = await readFile(new URL(doc, root), "utf8");
  const code = (await readFile(new URL(source, root), "utf8")).trim();
  if (!text.includes(source) || !text.includes(`\x60\x60\x60ts\n${code}\n\x60\x60\x60`)) {
    throw new Error(`Excerpt drift: ${doc} must cite and include ${source} verbatim`);
  }
  console.log(`ok excerpt ${doc} ← ${source}`);
}
