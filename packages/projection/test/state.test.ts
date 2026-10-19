import { Commit, Transaction } from "@streamsy/projection";
import { BunServices } from "@effect/platform-bun";
import { restore } from "../src/checkpoint-store.ts";
import * as FamilyFixture from "./support/families.ts";
import { Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { expect, layer } from "@effect/vitest";
import { Effect, FileSystem, Layer, Option, Path, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { type StorageFault } from "@streamsy/core";
import { Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, type ProjectionFault } from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";
import { BunStorage } from "@streamsy/storage/bun";
import { forget } from "../src/forget.ts";

class numbers extends Streamsy.json<numbers>()("state.test/numbers", "state/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
}) {}

const input = FamilyFixture.json("state-input", { schema: Schema.Finite });

const total = Projection.fold({
  id: "state-total",
  params: {},
  version: 2,
  inputs: { input },
  state: { schema: Schema.Finite, initial: 0 },
  fold: (sum, { item }) => sum + item,
}).at({});

const sqliteProtocol = (filename = ":memory:") =>
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(BunStorage.layerProtocol({ client: { filename } })),
  );

const sqlite = (filename = ":memory:") => FamilyFixture.withFamilies(sqliteProtocol(filename));

const seed = Effect.gen(function* () {
  yield* Streams.create(input);
  yield* Streams.append(input, [2, 3]);
});

const hosts: ReadonlyArray<
  readonly [
    string,
    Layer.Layer<
      CheckpointStore | import("@streamsy/core").Streamsy.Wildcard | StreamsReader | StreamsWriter,
      StorageFault | ProjectionFault
    >,
  ]
> = [
  ["memory", FamilyFixture.withFamilies(memoryHost())],
  ["SQLite", sqlite()],
];

for (const [name, host] of hosts) {
  layer(host)((it) => {
    it.effect(
      `${name}: fold round trip, version key, CheckpointStore provision, and a failing step saves nothing`,
      () =>
        Effect.gen(function* () {
          yield* seed;
          expect((yield* Projection.run(total)).items).toBe(2);
          expect((yield* Projection.read(total)).state).toBe(5);
          expect(
            Option.flatMap(
              (yield* (yield* CheckpointStore).load({ ...total, version: 1 })).record,
              (record) => Option.fromUndefinedOr(record.state),
            ),
          ).toEqual(Option.none());
          expect((yield* Projection.read({ ...total, version: 1 })).state).toBe(0);
          const checkpoints = yield* CheckpointStore;
          const before = yield* checkpoints.load(total);
          yield* Streams.append(input, [7, 11]);

          const failing = Projection.fold({
            id: total.id,
            params: {},
            version: total.version,
            inputs: { input },
            state: { schema: Schema.Finite, initial: 0 },
            fold: (sum, { item }) =>
              item === 11 ? Effect.fail("second item") : Effect.succeed(sum + item),
          }).at({});

          const failure = yield* Projection.run(failing).pipe(Effect.result);
          expect(failure._tag).toBe("Failure");
          expect(yield* checkpoints.load(total)).toEqual(before);
          expect((yield* Projection.read(total)).state).toBe(5);
          expect((yield* Projection.run(total)).items).toBe(2);
          expect((yield* Projection.read(total)).state).toBe(23);
        }),
    );
  });

  layer(host)((it) => {
    it.effect(`${name}: token conflict at compareAndSet preserves the winning record`, () =>
      Effect.gen(function* () {
        yield* seed;
        yield* Projection.run(total);
        yield* Streams.append(input, [7]);
        const owner = yield* CheckpointStore;
        const before = yield* owner.load(total);

        const failed = yield* Projection.run(total).pipe(
          Effect.provideService(CheckpointStore, {
            ...owner,
            compareAndSet: (key, _token, record) => owner.compareAndSet(key, "stale-token", record),
          }),
          Effect.result,
        );

        expect(failed._tag).toBe("Failure");

        if (failed._tag === "Failure")
          expect(failed.failure).toMatchObject({ phase: "checkpoint", reason: "token-conflict" });
        expect(yield* owner.load(total)).toEqual(before);
        expect((yield* Projection.read(total)).state).toBe(5);
      }),
    );
  });

  layer(host)((it) => {
    it.effect(`${name}: forget deletes one record and is idempotent`, () =>
      Effect.gen(function* () {
        yield* seed;
        yield* Projection.run(total);
        yield* forget(total);
        const checkpoints = yield* CheckpointStore;
        expect(yield* checkpoints.load(total)).toEqual({ record: Option.none(), token: "0" });
        expect(
          Option.flatMap((yield* (yield* CheckpointStore).load(total)).record, (record) =>
            Option.fromUndefinedOr(record.state),
          ),
        ).toEqual(Option.none());
        expect(yield* Projection.read(total)).toEqual({
          state: 0,
          offsets: { input: ZERO_OFFSET },
        });
        yield* Projection.forget(total);
        yield* Projection.forget({ ...total, id: "never-ran" });
        expect((yield* Projection.run(total)).items).toBe(2);
      }),
    );
  });

  layer(host)((it) => {
    it.effect(`${name}: failed record delete retains state`, () =>
      Effect.gen(function* () {
        yield* seed;
        yield* Projection.run(total);
        const checkpoints = yield* CheckpointStore;
        const before = yield* checkpoints.load(total);

        const result = yield* forget(total).pipe(
          Effect.provideService(CheckpointStore, {
            ...checkpoints,
            remove: () => Effect.die("delete failed"),
          }),
          Effect.exit,
        );

        expect(result._tag).toBe("Failure");
        expect(yield* checkpoints.load(total)).toEqual(before);
        expect((yield* Projection.read(total)).state).toBe(5);
        yield* forget(total);
      }),
    );
  });
}

layer(sqlite())((it) => {
  it.effect(`SQLite: handler failure after a record write rolls back state and offsets`, () =>
    Effect.gen(function* () {
      yield* seed;
      yield* Projection.run(total);
      const checkpoints = yield* CheckpointStore;
      const before = yield* checkpoints.load(total);
      yield* Streams.append(input, [7]);

      const saveThenFail = Projection.process({
        commit: Commit.transaction,
        id: total.id,
        params: {},
        version: total.version,
        inputs: { input },
        process: () =>
          Effect.gen(function* () {
            const store = yield* CheckpointStore;
            const loaded = yield* restore(total, store);
            yield* store.compareAndSet(total, loaded.token, {
              identity: loaded.record.identity,
              inputs: loaded.record.inputs,
              adapters: loaded.record.adapters,
              state: "999",
            });

            return yield* Effect.fail("after save");
          }),
      }).at({});

      const failed = yield* Projection.run(saveThenFail).pipe(Effect.result);
      expect(failed._tag).toBe("Failure");
      expect(yield* checkpoints.load(total)).toEqual(before);
      expect((yield* Projection.read(total)).state).toBe(5);
    }),
  );
});

layer(BunServices.layer)((it) => {
  it.effect("SQLite file restart retains folded state and reads zero new items", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "projection-state-" });
      const filename = path.join(directory, "state.sqlite");
      yield* Effect.scoped(
        Layer.build(sqlite(filename)).pipe(
          Effect.flatMap((services) =>
            Effect.provideContext(seed.pipe(Effect.andThen(Projection.run(total))), services),
          ),
        ),
      );
      yield* Effect.scoped(
        Layer.build(sqlite(filename)).pipe(
          Effect.flatMap((services) =>
            Effect.gen(function* () {
              expect((yield* Projection.run(total)).items).toBe(0);
              expect((yield* Projection.read(total)).state).toBe(5);
            }).pipe(Effect.provideContext(services)),
          ),
        ),
      );
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), numbers))((it) => {
  it.effect(
    "invalid JSON and schema-invalid state fail typed reads; invalid encoding rolls back",
    () =>
      Effect.gen(function* () {
        const state = yield* CheckpointStore;
        const checkpoints = yield* CheckpointStore;

        for (const encoded of ["{broken", '"not a number"']) {
          const before = yield* restore(total, checkpoints);
          yield* checkpoints.compareAndSet(total, before.token, {
            identity: before.record.identity,
            inputs: before.record.inputs,
            adapters: before.record.adapters,
            state: encoded,
          });
          const failed = yield* Projection.read(total).pipe(Effect.result);
          expect(failed._tag).toBe("Failure");

          if (failed._tag === "Failure")
            expect(failed.failure).toMatchObject({ phase: "load", reason: "invalid-record" });
        }

        yield* Projection.forget(total);
        yield* seed;

        const invalid = Projection.fold({
          id: total.id,
          params: {},
          version: total.version,
          inputs: { input },
          state: { schema: Schema.Finite, initial: 0 },
          fold: () => Infinity,
        }).at({});

        const failed = yield* Projection.run(invalid).pipe(Effect.result);

        if (failed._tag === "Failure")
          expect(failed.failure).toMatchObject({ phase: "process", reason: "invalid-output" });
        expect(failed._tag).toBe("Failure");
        expect((yield* state.load(total)).record).toEqual(Option.none());
      }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), numbers))((it) => {
  it.effect(
    "stream state rows are separate and initial factory receives params only on first use",
    () =>
      Effect.gen(function* () {
        const calls: string[] = [];

        const declared = Projection.fold({
          id: "state-stream",
          params: { name: Schema.String },
          inputs: { numbers },
          state: {
            schema: Schema.Finite,
            initial: (params) => {
              calls.push(params.name);

              return params.name === "a" ? 10 : 20;
            },
          },
          fold: (sum, { item }) => sum + item,
        });

        for (const name of ["a", "b"]) {
          const bound = declared.at({ name });
          yield* Streams.create(bound.inputs.numbers);
          yield* Streams.append(bound.inputs.numbers, [1]);
          yield* Projection.run(bound);
          yield* Streams.append(bound.inputs.numbers, [2]);
          yield* Projection.run(bound);
          expect((yield* Projection.read(bound)).state).toBe(name === "a" ? 13 : 23);
        }

        expect(calls).toEqual(["a", "b"]);
      }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), numbers))((it) => {
  it.effect("tagged multi-input fold narrows items and follows declaration order", () =>
    Effect.gen(function* () {
      const words = FamilyFixture.json("state-words", { schema: Schema.String });

      const projection = Projection.fold({
        id: "tagged",
        params: {},
        inputs: { input, words },
        state: { schema: Schema.String, initial: "" },
        fold: (value, entry) =>
          value + (entry.input === "input" ? entry.item.toFixed(0) : entry.item.toUpperCase()),
      }).at({});

      yield* seed;
      yield* Streams.create(words);
      yield* Streams.append(words, ["a"]);
      yield* Projection.run(projection);
      expect((yield* Projection.read(projection)).state).toBe("23A");
    }),
  );
});

layer(sqlite())((it) => {
  it.effect(
    "Checkpoint SQL failures retain phase and the storage fault with its driver cause",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const state = yield* CheckpointStore;
        yield* sql.unsafe("DROP TABLE streamsy_owner_records");

        for (const [phase, operation] of [
          ["load", state.load(total).pipe(Effect.asVoid)],
          [
            "load",
            state
              .compareAndSet(total, "0", {
                identity: { inputs: {} },
                inputs: {},
                adapters: {},
                state: "1",
              })
              .pipe(Effect.asVoid),
          ],
          ["checkpoint", state.remove(total)],
        ] as const) {
          const failed = yield* Effect.result(operation);
          expect(failed._tag).toBe("Failure");

          if (failed._tag === "Failure")
            expect(failed.failure).toMatchObject({
              phase,
              reason: "storage-failure",
              cause: { _tag: "StorageFault", cause: { _tag: "SqlError" } },
            });
        }
      }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), numbers))((it) => {
  it.effect(
    "fold saves one record per nonempty pass and skips the initial factory after creation",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const state = yield* CheckpointStore;
        let saves = 0;
        let starts = 0;

        const counted = Projection.fold({
          id: "counted",
          params: {},
          inputs: { input },
          state: {
            schema: Schema.Finite,
            initial: () => {
              starts += 1;

              return 0;
            },
          },
          fold: (sum, { item }) => sum + item,
        }).at({});

        const run = Projection.run(counted).pipe(
          Effect.provideService(CheckpointStore, {
            ...state,
            compareAndSet: (key, token, record) =>
              Effect.suspend(() => {
                saves += 1;

                return state.compareAndSet(key, token, record);
              }),
          }),
        );

        yield* run;
        yield* Streams.append(input, [7, 11]);
        yield* run;
        expect({ saves, starts }).toEqual({ saves: 2, starts: 1 });
      }),
  );
});
