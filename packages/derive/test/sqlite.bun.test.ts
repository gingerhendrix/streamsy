/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/strict-effect-provide -- Bun owns file-backed runtime reopen and test Layer graphs. */
import { mkdtempSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Config, Effect, Layer, ManagedRuntime, Schema } from "effect";
import { Protocol } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import * as Sqlite from "../src/sqlite.ts";
import { Projection } from "../src/index.ts";
import { composition, definition, initialize } from "./scenarios.ts";

const host = (filename: string) =>
  Layer.merge(Protocol.layer(), Sqlite.layer).pipe(
    Layer.provideMerge(BunStorage.layer({ client: { filename } })),
  );

const scratch = Effect.runSync(
  Config.String("STREAMSY_DERIVE_SCRATCH").pipe(Config.withDefault("/tmp")),
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

test("a new Bun process reopens the file and resumes without repeated output", async () => {
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
  const child = Bun.spawnSync(
    [
      process.execPath,
      "-e",
      `
      import { Effect, Layer, ManagedRuntime } from "effect";
      import { Protocol } from "@streamsy/core";
      import * as BunStorage from "@streamsy/storage/bun";
      import * as Sqlite from "./src/sqlite.ts";
      import { Projection } from "./src/index.ts";
      import { definition, inspect } from "./test/scenarios.ts";
      const host = Layer.merge(Protocol.layer(), Sqlite.layer).pipe(
        Layer.provideMerge(BunStorage.layer({ client: { filename: Bun.argv.at(-1) } })),
      );
      const runtime = ManagedRuntime.make(host);
      try {
        const resumed = await runtime.runPromise(definition.pipe(Effect.flatMap(Projection.catchUp)));
        const stored = await runtime.runPromise(inspect);
        console.log(JSON.stringify({ items: resumed.items, output: stored.output,
          encoded: stored.state.encoded, revision: stored.state.revision,
          checkpointRevision: stored.checkpoint.revision }));
      } finally { await runtime.dispose(); }
    `,
      filename,
    ],
    { cwd: join(import.meta.dir, "..") },
  );
  expect(child.exitCode).toBe(0);
  const result = Schema.decodeSync(
    Schema.fromJsonString(
      Schema.Struct({
        items: Schema.Finite,
        output: Schema.Array(Schema.Finite),
        encoded: Schema.String,
        revision: Schema.Finite,
        checkpointRevision: Schema.Finite,
      }),
    ),
  )(child.stdout.toString());
  expect(result.items).toBe(2);
  expect(result.output).toEqual([1, 3]);
  expect(result.encoded).toBe("3");
  expect(result.revision).toBe(result.checkpointRevision);
});
