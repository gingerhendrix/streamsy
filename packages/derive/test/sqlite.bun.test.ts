/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/strict-effect-provide -- Bun owns file-backed runtime reopen and test Layer graphs. */
import { mkdtempSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Config, Effect, Layer, ManagedRuntime } from "effect";
import { Protocol } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import * as Sqlite from "../src/sqlite.ts";
import { Projection } from "../src/index.ts";
import { composition, definition, initialize, inspect } from "./scenarios.ts";

const host = (filename: string) =>
  Layer.merge(Protocol.layer(), Sqlite.layer).pipe(
    Layer.provideMerge(BunStorage.layer({ client: { filename } })),
  );

const scratch = Effect.runSync(
  Config.string("STREAMSY_DERIVE_SCRATCH").pipe(Config.withDefault("/tmp")),
);

test("Bun SQLite fuses output, state and checkpoint, including after-sink rollback", async () => {
  const runtime = ManagedRuntime.make(host(":memory:"));
  try {
    const result = await runtime.runPromise(composition);
    expect(result.after).toEqual(result.before);
    expect(result.failed).toBe("Failure");
    expect(result.stored.output).toEqual([1, 3]);
    expect(result.stored.state.encoded).toBe("3");
    expect(result.restart.items).toBe(0);
  } finally {
    await runtime.dispose();
  }
});

test("file-backed Bun reopen resumes without repeated output", async () => {
  const filename = join(mkdtempSync(join(scratch, "derive-reopen-")), "derive.sqlite");
  const first = ManagedRuntime.make(host(filename));
  try {
    await first.runPromise(initialize);
    await first.runPromise(
      definition.pipe(Effect.flatMap((projection) => Projection.catchUp(projection, { items: 1 }))),
    );
  } finally {
    await first.dispose();
  }
  const second = ManagedRuntime.make(host(filename));
  try {
    const resumed = await second.runPromise(definition.pipe(Effect.flatMap(Projection.catchUp)));
    expect(resumed.items).toBe(2);
    const stored = await second.runPromise(inspect);
    expect(stored.output).toEqual([1, 3]);
    expect(stored.state.encoded).toBe("3");
    expect(stored.state.revision).toBe(stored.checkpoint.revision);
  } finally {
    await second.dispose();
  }
});
