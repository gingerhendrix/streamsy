import { test, expect } from "@effect/vitest";
import { Miniflare } from "miniflare";
import { Array, Schema } from "effect";

const Result = Schema.Struct({
  units: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      billing: Schema.Struct({ rowsRead: Schema.Finite, rowsWritten: Schema.Finite }),
      outputChanges: Schema.Finite,
      txids: Schema.Array(Schema.String),
    }),
  ),
  noopUnits: Schema.Finite,
});

test("runs the shop circuit on local workerd and observes SQLite reads and writes", async () => {
  const scratch = Bun.fileURLToPath(new URL("../../build/workerd", import.meta.url));

  const built = await Bun.build({
    entrypoints: [`${import.meta.dirname}/support/worker.ts`],
    outdir: `${scratch}/bundle`,
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });

  expect(built.success).toBe(true);
  const output = built.outputs[0];

  if (output === undefined) throw new Error("No workerd bundle");

  const miniflare = new Miniflare({
    rootPath: scratch,
    modulesRoot: `${scratch}/bundle`,
    scriptPath: output.path,
    modules: true,
    compatibilityDate: "2026-08-06",
    cf: false,
    durableObjects: { SHOP: { className: "ShopObject", useSQLite: true } },
  });

  try {
    const response = await miniflare.dispatchFetch("http://shop.test/proof");
    const text = await response.text();

    if (response.status !== 200) throw new Error(text);
    const result = Schema.decodeSync(Schema.fromJsonString(Result))(text);
    expect(result.noopUnits).toBe(1);
    expect(Array.map(result.units, ({ name, outputChanges }) => [name, outputChanges])).toEqual([
      ["load", 93],
      ["single-order", 7],
      ["customer-rename", 10],
      ["100-changes", 94],
    ]);

    for (const unit of result.units) {
      expect(unit.billing.rowsRead).toBeGreaterThan(0);
      expect(unit.billing.rowsWritten).toBeGreaterThan(0);
      expect(unit.txids).toHaveLength(1);
    }
  } finally {
    await miniflare.dispose();
  }
}, 30000);
