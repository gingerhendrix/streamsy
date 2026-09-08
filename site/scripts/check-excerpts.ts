import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const root = new URL("../../", import.meta.url);
const pairs = [
  { doc: "packages/core/README.md", source: "packages/core/test/readme.ts", execute: true },
  {
    doc: "site/content/docs/user/basic-usage.mdx",
    source: "packages/serve/test/basic-usage.ts",
    execute: true,
  },
  {
    doc: "site/content/docs/user/basic-usage.mdx",
    source: "packages/serve/test/host.ts",
    execute: true,
  },
  {
    doc: "site/content/docs/user/sql-storage.mdx",
    source: "packages/storage/test/shared-transaction-usage.ts",
    execute: true,
  },
  {
    doc: "site/content/docs/user/cloudflare-hosting.mdx",
    source: "packages/serve/test/cloudflare/example-worker.ts",
    execute: false,
  },
  {
    doc: "site/content/docs/user/cloudflare-hosting.mdx",
    source: "packages/serve/test/cloudflare-usage.ts",
    execute: true,
  },
  {
    doc: "site/content/docs/user/cloudflare-hosting.mdx",
    source: "hosted/alchemy.run.ts",
    execute: false,
  },
];
const run = (args: string[], label: string): void => {
  const result = spawnSync(process.execPath, args, {
    cwd: fileURLToPath(root),
    stdio: "inherit",
    timeout: 15_000,
    killSignal: "SIGKILL",
  });
  if (result.error || result.status !== 0) {
    throw new Error(`${label} failed (status ${result.status}, signal ${result.signal})`, {
      cause: result.error,
    });
  }
};

run(["run", "--cwd", "packages/serve", "typecheck"], "serve Worker typecheck");
run(["run", "--cwd", "hosted", "typecheck"], "hosted typecheck");

for (const { doc, source, execute } of pairs) {
  const text = await readFile(new URL(doc, root), "utf8");
  const code = (await readFile(new URL(source, root), "utf8")).trim();
  if (!text.includes(source) || !text.includes(`\x60\x60\x60ts\n${code}\n\x60\x60\x60`)) {
    throw new Error(`Excerpt drift: ${doc} must cite and include ${source} verbatim`);
  }
  console.log(`ok excerpt ${doc} ← ${source}`);
  if (!execute) {
    console.log(`ok typechecked-only ${source}`);
    continue;
  }
  run([source], `Excerpt execution: ${source}`);
  console.log(`ok executed ${source} (15s bound)`);
}
