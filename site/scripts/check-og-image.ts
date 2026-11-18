import { readdir, readFile } from "node:fs/promises";

const siteDir = new URL("..", import.meta.url).pathname;

// The docs head declares `og:image:type` as `image/webp`, so every social image
// must be a WebP file: the shared `/og.webp` and each frontmatter `image`.
const imagePaths = new Set(["/og.webp"]);

const docFiles = await readdir(`${siteDir}/content/docs`, { recursive: true });

for (const file of docFiles.filter((name) => name.endsWith(".mdx"))) {
  const text = await readFile(`${siteDir}/content/docs/${file}`, "utf8");
  const image = /^---\n[\s\S]*?^image:\s*(\S+)\s*$[\s\S]*?^---$/m.exec(text)?.[1];

  if (image !== undefined) imagePaths.add(image);
}

for (const path of imagePaths) {
  const bytes = new Uint8Array(await readFile(`${siteDir}/public${path}`));
  const ascii = (start: number, end: number) => String.fromCharCode(...bytes.slice(start, end));

  if (bytes.length === 0) throw new Error(`OG image ${path} is empty`);

  if (ascii(0, 4) !== "RIFF" || ascii(8, 12) !== "WEBP") {
    throw new Error(`OG image ${path} does not have a valid WebP signature`);
  }

  console.log(`valid WebP OG image ${path} (${bytes.length} bytes)`);
}
