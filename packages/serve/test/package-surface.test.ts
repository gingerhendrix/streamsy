/* oxlint-disable effecttsgo/async-function -- Bun file/package import boundary. */
import { expect, test } from "bun:test";
import { Effect } from "effect";
import * as DenoHost from "@streamsy/serve/deno";

const entry = (name: string) => ({
  types: `./dist/${name}.d.ts`,
  import: `./dist/${name}.js`,
  default: `./dist/${name}.js`,
});

test("runtime platforms stay optional peers", async () => {
  const manifest: unknown = await Bun.file(new URL("../package.json", import.meta.url)).json();
  expect(manifest).toMatchObject({
    exports: { "./bun": entry("bun"), "./node": entry("node"), "./deno": entry("deno") },
    peerDependencies: {
      "@effect/platform-bun": "4.0.0-rc.115",
      "@effect/platform-deno": "4.0.0-rc.115",
      "@effect/platform-node": "4.0.0-rc.115",
    },
    peerDependenciesMeta: {
      "@effect/platform-bun": { optional: true },
      "@effect/platform-deno": { optional: true },
      "@effect/platform-node": { optional: true },
    },
  });
  const root = await Bun.file(new URL("../src/index.ts", import.meta.url)).text();
  for (const platform of ["platform-bun", "platform-node", "platform-deno"])
    expect(root).not.toContain(platform);
});

test("the Deno SQLite helper leaves node:sqlite able to open a database", async () => {
  await Effect.runPromise(DenoHost.nodeSqliteCompat);
  await Effect.runPromise(DenoHost.nodeSqliteCompat);
  const { DatabaseSync } = await import("node:sqlite");
  const prototype: { enableLoadExtension?: unknown } = DatabaseSync.prototype;
  expect(prototype.enableLoadExtension).toBeFunction();
});
