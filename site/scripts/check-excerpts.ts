import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

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
  const run = spawnSync(process.execPath, [source], {
    cwd: fileURLToPath(root),
    stdio: "inherit",
    timeout: 15_000,
    killSignal: "SIGKILL",
  });
  if (run.error || run.status !== 0) {
    throw new Error(
      `Excerpt execution failed: ${source} (status ${run.status}, signal ${run.signal})`,
      { cause: run.error },
    );
  }
  console.log(`ok executed ${source} (15s bound)`);
}
