import { Commit, Transaction } from "@streamsy/projection";
import { Database } from "bun:sqlite";
import { recordKey } from "@streamsy/projection/checkpoint";
import * as FamilyFixture from "./support/families.ts";
import { BunServices } from "@effect/platform-bun";

import { expect, layer } from "@streamsy/effect-test";
import { FileSystem, Path, Deferred, Effect, Exit, Fiber, Layer, Option, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Streams } from "@streamsy/core/protocol";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { composition, initialize, input, inspect, positives } from "./support/scenarios.ts";

const protocolHost = (filename: string, readLimit = 1000) =>
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(BunStorage.layerProtocol({ client: { filename }, readLimit })),
  );

const host = (filename: string, readLimit = 1000) =>
  FamilyFixture.withFamilies(protocolHost(filename, readLimit));

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect(
    "Bun SQLite fuses the handler's append and the checkpoint, including rollback at save",
    () =>
      Effect.gen(function* () {
        const services = yield* Layer.build(host(":memory:", 1));

        const result = yield* Effect.provideContext(composition, services);
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
          Option.some(result.final.offsets),
        );
      }),
  );
});

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect(
    "a handler's SQL through the shared client commits and rolls back with the checkpoint",
    () =>
      Effect.gen(function* () {
        const services = yield* Layer.build(host(":memory:"));

        yield* Effect.provideContext(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql.unsafe("CREATE TABLE seen (item INTEGER NOT NULL)");
            yield* Streams.create(input);
            yield* Streams.append(input, [1, -1, 3]);

            const projection = Projection.process({
              commit: Commit.transaction,
              id: "seen",
              params: {},
              inputs: { input },
              process: Projection.each(({ item }) =>
                sql.unsafe("INSERT INTO seen (item) VALUES (?)", [item]),
              ),
            }).at({});

            const count = sql
              .unsafe<{ readonly n: number }>("SELECT COUNT(*) AS n FROM seen")
              .pipe(Effect.map((rows) => rows[0]?.n));

            const owner = yield* CheckpointStore;

            const failing = CheckpointStore.of({
              ...owner,
              compareAndSet: () =>
                Effect.fail(
                  new ProjectionFault({
                    phase: "checkpoint",
                    reason: "storage-failure",
                    message: "",
                  }),
                ),
            });

            const failed = yield* Projection.run(projection).pipe(
              Effect.provideService(CheckpointStore, failing),
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
          services,
        );
      }),
  );
});

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect(
    "interruption inside the Bun SQLite transaction rolls back the handler's SQL, the append and the checkpoint",
    () =>
      Effect.gen(function* () {
        const services = yield* Layer.build(host(":memory:"));

        yield* Effect.provideContext(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql.unsafe("CREATE TABLE seen (item INTEGER NOT NULL)");
            yield* initialize;
            const written = yield* Deferred.make<void>();
            const release = yield* Deferred.make<void>();

            const blocking = Projection.process({
              commit: Commit.transaction,
              id: positives.id,
              params: {},
              inputs: { input },
              process: (batch, unit) =>
                Effect.gen(function* () {
                  yield* Effect.forEach(batch.input.items, (item) =>
                    sql.unsafe("INSERT INTO seen (item) VALUES (?)", [item]),
                  );
                  yield* positives.process(batch, unit);
                  yield* Deferred.succeed(written, undefined);
                  yield* Deferred.await(release);
                }),
            }).at({});

            const count = sql
              .unsafe<{ readonly n: number }>("SELECT COUNT(*) AS n FROM seen")
              .pipe(Effect.map((rows) => rows[0]?.n));

            const fiber = yield* Effect.forkChild(Projection.run(blocking));
            yield* Deferred.await(written);
            yield* Fiber.interrupt(fiber);
            const [exit] = yield* Fiber.awaitAll([fiber]);
            expect(exit !== undefined && Exit.hasInterrupts(exit)).toBe(true);
            expect(yield* count).toBe(0);
            const stored = yield* inspect;
            expect(stored.output).toEqual([]);
            expect(Option.isNone(stored.loaded.record)).toBe(true);
            expect(stored.loaded.token).toBe("0");
            const result = yield* Projection.run(positives);
            expect(result.status).toBe("caught-up");
            expect(result.items).toBe(3);
            expect((yield* inspect).output).toEqual([1, 3]);
          }),
          services,
        );
      }),
  );
});

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect("a new Bun process reopens the file and resumes without repeated output", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "projection-reopen-" });
      const filename = path.join(directory, "projection.sqlite");
      yield* Effect.scoped(
        Layer.build(host(filename, 1)).pipe(
          Effect.flatMap((services) =>
            initialize.pipe(
              Effect.andThen(Projection.run(positives, { limit: 1 })),
              Effect.provideContext(services),
            ),
          ),
        ),
      );

      const child = Bun.spawnSync(
        [
          Bun.argv[0]!,
          "-e",
          `
      import * as FamilyFixture from "./test/support/families.ts";
      import { Layer, ManagedRuntime, Option } from "effect";
      import { Streams } from "@streamsy/core/protocol";
      import { BunStorage } from "@streamsy/storage/bun";
      import { CheckpointStore, Projection, Transaction } from "./src/index.ts";
      import { input, inspect, positives } from "./test/support/scenarios.ts";
      const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
        Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: Bun.argv.at(-1) } })),
      );
      const runtime = ManagedRuntime.make(FamilyFixture.withFamilies(host));
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
        { cwd: path.join(import.meta.dir, "..") },
      );

      expect(child.stderr.toString()).toBe("");
      expect(child.exitCode).toBe(0);

      const result = yield* Schema.decodeEffect(
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
    }),
  );
});

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect("a checkpoint SQL failure retains its underlying cause", () =>
    Effect.gen(function* () {
      const services = yield* Layer.build(host(":memory:"));

      yield* Effect.provideContext(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql.unsafe("DROP TABLE streamsy_owner_records");
          const result = yield* (yield* CheckpointStore).load(positives).pipe(Effect.result);
          expect(result._tag).toBe("Failure");

          if (result._tag === "Failure") {
            expect(result.failure.phase).toBe("load");
            expect(result.failure.reason).toBe("storage-failure");
            expect(result.failure.cause).toMatchObject({
              _tag: "StorageFault",
              cause: { _tag: "SqlError" },
            });
          }
        }),
        services,
      );
    }),
  );
});

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect("a SQLite file containing only v1 projection tables rebuilds from zero", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "projection-v1-" });
      const filename = path.join(directory, "old.sqlite");

      const old = yield* Effect.acquireRelease(
        Effect.sync(() => new Database(filename)),
        (db) => Effect.sync(() => db.close()),
      );

      old.exec(
        "CREATE TABLE streamsy_projection_v1_records (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
      );
      old.exec(
        "CREATE TABLE streamsy_projection_v1_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
      );
      old
        .query("INSERT INTO streamsy_projection_v1_records VALUES (?, ?)")
        .run(recordKey(positives), "old record must not be decoded");
      old
        .query("INSERT INTO streamsy_projection_v1_state VALUES (?, ?)")
        .run(recordKey(positives), "999");

      const services = yield* Layer.build(host(filename));

      yield* Effect.provideContext(
        Effect.gen(function* () {
          expect(yield* (yield* CheckpointStore).load(positives)).toEqual({
            record: Option.none(),
            token: "0",
          });
          yield* initialize;
          expect((yield* Projection.run(positives)).items).toBe(3);
          expect((yield* inspect).output).toEqual([1, 3]);
          const sql = yield* SqlClient.SqlClient;
          expect(yield* sql.unsafe("SELECT value FROM streamsy_projection_v1_state")).toEqual([
            { value: "999" },
          ]);
          expect(yield* sql.unsafe("SELECT owner_key FROM streamsy_owner_records")).toHaveLength(1);
        }),
        services,
      );
    }),
  );
});
