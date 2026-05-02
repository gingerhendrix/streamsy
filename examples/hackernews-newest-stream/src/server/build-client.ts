import { mkdir, rm, writeFile } from "node:fs/promises";
import { join, relative } from "node:path";

const projectRoot = join(import.meta.dir, "../..");
const distDir = join(projectRoot, "dist");
const assetsDir = join(distDir, "assets");
const entrypoint = join(projectRoot, "src/client/main.tsx");

await rm(distDir, { recursive: true, force: true });
await mkdir(assetsDir, { recursive: true });

const result = await Bun.build({
  entrypoints: [entrypoint],
  outdir: assetsDir,
  target: "browser",
  format: "esm",
  splitting: true,
  sourcemap: "linked",
  minify: process.env.NODE_ENV === "production",
});

if (!result.success) {
  for (const log of result.logs) console.error(log);
  process.exit(1);
}

const scripts = result.outputs
  .filter((output) => output.path.endsWith(".js"))
  .map((output) => `    <script type="module" src="/${relative(distDir, output.path)}"></script>`);
const styles = result.outputs
  .filter((output) => output.path.endsWith(".css"))
  .map((output) => `    <link rel="stylesheet" href="/${relative(distDir, output.path)}" />`);

if (scripts.length === 0) {
  console.error("Bun build did not produce a browser JavaScript entrypoint");
  process.exit(1);
}

const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Streamsy Hacker News Newest Demo</title>
${styles.join("\n")}
  </head>
  <body>
    <div id="root"></div>
${scripts.join("\n")}
  </body>
</html>
`;

await writeFile(join(distDir, "index.html"), html);
console.log(`Built Hacker News newest client to ${distDir}`);
