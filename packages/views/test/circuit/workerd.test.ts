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

test("same spike C circuit on local workerd with billable SQL rows per unit", async () => {
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
      ["load", 90],
      ["single-order", 4],
      ["customer-rename", 10],
      ["100-changes", 91],
    ]);

    for (const unit of result.units) {
      expect(unit.billing.rowsRead).toBeGreaterThan(0);
      expect(unit.billing.rowsWritten).toBeGreaterThan(0);
      expect(unit.txids).toHaveLength(1);
    }

    await Bun.write(`${scratch}/billing.json`, JSON.stringify(result, null, 2));
    console.log(
      JSON.stringify(
        Array.map(result.units, ({ name, billing, outputChanges }) => ({
          name,
          ...billing,
          outputChanges,
        })),
      ),
    );
  } finally {
    await miniflare.dispose();
  }
}, 30000);
