import { mkdtempSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  Config,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  ManagedRuntime,
  Option,
  Schema,
} from "effect";
import { SqlClient } from "effect/unstable/sql";
import { Protocol, Streams } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import * as Sqlite from "../src/sqlite.ts";
import { Checkpoints, Projection, ProjectionFault } from "../src/index.ts";
import { composition, initialize, input, inspect, positives } from "./scenarios.ts";

const host = (filename: string, readLimit = 1000) =>
  Layer.merge(Protocol.layer({ readLimit }), Sqlite.layer).pipe(
    Layer.provideMerge(BunStorage.layer({ client: { filename } })),
  );

const scratch = Effect.runSync(
  Config.String("STREAMSY_PROJECTION_SCRATCH").pipe(Config.withDefault("/tmp")),
);

test("Bun SQLite fuses the handler's append and the checkpoint, including rollback at save", async () => {
  const runtime = ManagedRuntime.make(host(":memory:", 1));
  try {
    const result = await runtime.runPromise(composition);
    expect(result.first.status).toBe("limit-reached");
    expect(result.first.items).toBe(1);
    expect(result.failed).toBe("Failure");
    expect(result.after).toEqual(result.before);
    expect(result.final.status).toBe("caught-up");
    expect(result.final.items).toBe(2);
    expect(result.restart.items).toBe(0);
    expect(result.stored.output).toEqual([1, 3]);
    expect(result.stored.loaded.token).toBe("3");
    expect(Option.map(result.stored.loaded.record, (record) => record.inputs)).toEqual(
      Option.some(result.final.record.inputs),
    );
  } finally {
    await runtime.dispose();
  }
});

test("a handler's SQL through the shared client commits and rolls back with the checkpoint", async () => {
  const runtime = ManagedRuntime.make(host(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql.unsafe("CREATE TABLE seen (item INTEGER NOT NULL)");
        yield* Streams.create(input);
        yield* Streams.append(input, [1, -1, 3]);
        const projection = Projection.make({
          id: "seen",
          input,
          process: Projection.each(({ item }) =>
            sql.unsafe("INSERT INTO seen (item) VALUES (?)", [item]),
          ),
        });
        const count = sql
          .unsafe<{ readonly n: number }>("SELECT COUNT(*) AS n FROM seen")
          .pipe(Effect.map((rows) => rows[0]?.n));
        const owner = yield* Checkpoints;
        const failing = Checkpoints.of({
          ...owner,
          save: () =>
            Effect.fail(
              new ProjectionFault({ phase: "checkpoint", reason: "storage-failure", message: "" }),
            ),
        });
        const failed = yield* Projection.run(projection).pipe(
          Effect.provideService(Checkpoints, failing),
          Effect.result,
        );
        expect(failed._tag).toBe("Failure");
        expect(yield* count).toBe(0);
        expect((yield* owner.load(Projection.key(projection))).token).toBe("0");
        const result = yield* Projection.run(projection);
        expect(result.status).toBe("caught-up");
        expect(result.items).toBe(3);
        expect(yield* count).toBe(3);
        expect((yield* owner.load(Projection.key(projection))).token).toBe("1");
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("interruption inside the Bun SQLite transaction rolls back the handler's SQL, the append and the checkpoint", async () => {
  const runtime = ManagedRuntime.make(host(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql.unsafe("CREATE TABLE seen (item INTEGER NOT NULL)");
        yield* initialize;
        const written = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const blocking = Projection.make({
          id: positives.id,
          input,
          process: (batch, unit) =>
            Effect.gen(function* () {
              yield* Effect.forEach(batch.input.items, (item) =>
                sql.unsafe("INSERT INTO seen (item) VALUES (?)", [item]),
              );
              yield* positives.process(batch, unit);
              yield* Deferred.succeed(written, undefined);
              yield* Deferred.await(release);
            }),
        });
        const count = sql
          .unsafe<{ readonly n: number }>("SELECT COUNT(*) AS n FROM seen")
          .pipe(Effect.map((rows) => rows[0]?.n));
        const fiber = yield* Effect.forkChild(Projection.run(blocking));
        yield* Deferred.await(written);
        yield* Fiber.interrupt(fiber);
        const [exit] = yield* Fiber.awaitAll([fiber]);
        expect(exit !== undefined && Exit.hasInterrupts(exit)).toBe(true);
        expect(yield* count).toBe(0);
        const owner = yield* Checkpoints;
        expect((yield* owner.load(Projection.key(positives))).token).toBe("0");
        const result = yield* Projection.run(positives);
        expect(result.status).toBe("caught-up");
        expect(result.items).toBe(3);
        expect((yield* inspect).output).toEqual([1, 3]);
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("a new Bun process reopens the file and resumes without repeated output", async () => {
  const filename = join(mkdtempSync(join(scratch, "projection-reopen-")), "projection.sqlite");
  const first = ManagedRuntime.make(host(filename, 1));
  try {
    await first.runPromise(initialize);
    await first.runPromise(Projection.run(positives, { limit: 1 }));
  } finally {
    await first.dispose();
  }
  const child = Bun.spawnSync(
    [
      process.execPath,
      "-e",
      `
      import { Layer, ManagedRuntime, Option } from "effect";
      import { Protocol, Streams } from "@streamsy/core";
      import * as BunStorage from "@streamsy/storage/bun";
      import * as Sqlite from "./src/sqlite.ts";
      import { Projection } from "./src/index.ts";
      import { input, inspect, positives } from "./test/scenarios.ts";
      const host = Layer.merge(Protocol.layer(), Sqlite.layer).pipe(
        Layer.provideMerge(BunStorage.layer({ client: { filename: Bun.argv.at(-1) } })),
      );
      const runtime = ManagedRuntime.make(host);
      try {
        const resumed = await runtime.runPromise(Projection.run(positives));
        const stored = await runtime.runPromise(inspect);
        const tail = (await runtime.runPromise(Streams.head(input))).nextOffset;
        console.log(JSON.stringify({ status: resumed.status, items: resumed.items,
          output: stored.output, token: stored.loaded.token,
          accepted: Option.getOrThrow(stored.loaded.record).inputs.input, tail }));
      } finally { await runtime.dispose(); }
    `,
      filename,
    ],
    { cwd: join(import.meta.dir, "..") },
  );
  expect(child.stderr.toString()).toBe("");
  expect(child.exitCode).toBe(0);
  const result = Schema.decodeSync(
    Schema.fromJsonString(
      Schema.Struct({
        status: Schema.String,
        items: Schema.Finite,
        output: Schema.Array(Schema.Finite),
        token: Schema.String,
        accepted: Schema.String,
        tail: Schema.String,
      }),
    ),
  )(child.stdout.toString());
  expect(result.status).toBe("caught-up");
  expect(result.items).toBe(2);
  expect(result.output).toEqual([1, 3]);
  expect(result.token).toBe("2");
  expect(result.accepted).toBe(result.tail);
});

test("a checkpoint SQL failure retains its underlying cause", async () => {
  const runtime = ManagedRuntime.make(host(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql.unsafe("DROP TABLE streamsy_projection_v1_records");
        const result = yield* (yield* Checkpoints).load(positives).pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(result.failure.phase).toBe("load");
          expect(result.failure.reason).toBe("storage-failure");
          expect(result.failure.cause).toMatchObject({ _tag: "SqlError" });
        }
      }),
    );
  } finally {
    await runtime.dispose();
  }
});
