import { tmpdir } from "node:os";
import { restore } from "../src/checkpoint-store.ts";
import { runKernel, run as runWithPermit } from "../src/run.ts";
import * as FamilyFixture from "./support/families.ts";
import { Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Effect, Layer, ManagedRuntime, Option, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { type StorageFault } from "@streamsy/core";
import { Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Projection,
  Transaction,
  type ProjectionFault,
} from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as ProjectionSql from "@streamsy/projection/sql";
import * as BunStorage from "@streamsy/storage/bun";
import { hasLock } from "../src/serialize.ts";
import { forget } from "../src/forget.ts";
import { recordKey } from "@streamsy/projection/checkpoint";

class numbers extends Streamsy.json<numbers>()("state.test/numbers", "state/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
}) {}

const input = FamilyFixture.json("state-input", { schema: Schema.Finite });
const total = Projection.atomic({
  id: "state-total",
  params: {},
  version: 2,
  inputs: { input },
  state: { schema: Schema.Finite, initial: 0 },
  fold: (sum, { item }) => sum + item,
}).at({});
const sqliteProtocol = (filename = ":memory:") =>
  ProjectionSql.layerStorage.pipe(
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
      | CheckpointStore
      | Transaction
      | import("@streamsy/core").Streamsy.Wildcard
      | StreamsReader
      | StreamsWriter,
      StorageFault | ProjectionFault
    >,
  ]
> = [
  ["memory", FamilyFixture.withFamilies(Memory.layerMemory())],
  ["SQLite", sqlite()],
];
for (const [name, host] of hosts) {
  test(`${name}: fold round trip, version key, CheckpointStore provision, and a failing step saves nothing`, async () => {
    const runtime = ManagedRuntime.make(host);
    try {
      await runtime.runPromise(
        Effect.gen(function* () {
          expect(Option.isNone((yield* Transaction).sql)).toBe(name === "memory");
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
          const failing = Projection.atomic({
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
    } finally {
      await runtime.dispose();
    }
  });

  test(`${name}: handler failure after a record write rolls back state and offsets`, async () => {
    const runtime = ManagedRuntime.make(host);
    try {
      await runtime.runPromise(
        Effect.gen(function* () {
          yield* seed;
          yield* Projection.run(total);
          const checkpoints = yield* CheckpointStore;
          const before = yield* checkpoints.load(total);
          yield* Streams.append(input, [7]);
          const saveThenFail = Projection.atomic({
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
    } finally {
      await runtime.dispose();
    }
  });

  test(`${name}: token conflict at compareAndSet preserves the winning record`, async () => {
    const runtime = ManagedRuntime.make(host);
    try {
      await runtime.runPromise(
        Effect.gen(function* () {
          yield* seed;
          yield* Projection.run(total);
          yield* Streams.append(input, [7]);
          const owner = yield* CheckpointStore;
          const before = yield* owner.load(total);
          const failed = yield* Projection.run(total).pipe(
            Effect.provideService(CheckpointStore, {
              ...owner,
              compareAndSet: (key, _token, record) =>
                owner.compareAndSet(key, "stale-token", record),
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
    } finally {
      await runtime.dispose();
    }
  });

  test(`${name}: forget deletes one record, prunes the lock, and is idempotent`, async () => {
    const runtime = ManagedRuntime.make(host);
    try {
      await runtime.runPromise(
        Effect.gen(function* () {
          yield* seed;
          yield* runWithPermit(total);
          expect(hasLock(recordKey(total))).toBe(true);
          // Source imports share the internal lock map, which is intentionally absent from dist's public surface.
          yield* forget(total);
          expect(hasLock(recordKey(total))).toBe(false);
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
    } finally {
      await runtime.dispose();
    }
  });

  test(`${name}: failed record delete rolls back state and retains the lock`, async () => {
    const runtime = ManagedRuntime.make(host);
    try {
      await runtime.runPromise(
        Effect.gen(function* () {
          yield* seed;
          yield* runWithPermit(total);
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
          expect(hasLock(recordKey(total))).toBe(true);
          expect(yield* checkpoints.load(total)).toEqual(before);
          expect((yield* Projection.read(total)).state).toBe(5);
          yield* forget(total);
        }),
      );
    } finally {
      await runtime.dispose();
    }
  });
}

test("SQLite file restart retains folded state and reads zero new items", async () => {
  const directory = mkdtempSync(join(tmpdir(), "projection-state-"));
  const filename = join(directory, "state.sqlite");
  const first = ManagedRuntime.make(sqlite(filename));
  try {
    await first.runPromise(seed.pipe(Effect.andThen(Projection.run(total))));
  } finally {
    await first.dispose();
  }
  const second = ManagedRuntime.make(sqlite(filename));
  try {
    expect((await second.runPromise(Projection.run(total))).items).toBe(0);
    expect((await second.runPromise(Projection.read(total))).state).toBe(5);
  } finally {
    await second.dispose();
    rmSync(directory, { recursive: true });
  }
});

test("invalid JSON and schema-invalid state fail typed reads; invalid encoding rolls back", async () => {
  await Effect.runPromise(
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
      const invalid = Projection.atomic({
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
    }).pipe(Effect.provide(FamilyFixture.withFamilies(Memory.layerMemory(), numbers))),
  );
});

test("stream state rows are separate and initial factory receives params only on first use", async () => {
  const calls: string[] = [];
  const declared = Projection.atomic({
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
  await Effect.runPromise(
    Effect.gen(function* () {
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
    }).pipe(Effect.provide(FamilyFixture.withFamilies(Memory.layerMemory(), numbers))),
  );
});

test("tagged multi-input fold narrows items and follows declaration order", async () => {
  const words = FamilyFixture.json("state-words", { schema: Schema.String });
  const projection = Projection.atomic({
    id: "tagged",
    params: {},
    inputs: { input, words },
    state: { schema: Schema.String, initial: "" },
    fold: (value, entry) =>
      value + (entry.input === "input" ? entry.item.toFixed(0) : entry.item.toUpperCase()),
  }).at({});
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      yield* Streams.create(words);
      yield* Streams.append(words, ["a"]);
      yield* Projection.run(projection);
      expect((yield* Projection.read(projection)).state).toBe("23A");
    }).pipe(Effect.provide(FamilyFixture.withFamilies(Memory.layerMemory(), numbers))),
  );
});

test("Checkpoint SQL failures retain phase and driver cause", async () => {
  const runtime = ManagedRuntime.make(sqlite());
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const state = yield* CheckpointStore;
        yield* sql.unsafe("DROP TABLE streamsy_projection_v2_records");
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
              cause: { _tag: "SqlError" },
            });
        }
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("SQLite competing commit during the read window preserves the winner's state", async () => {
  const runtime = ManagedRuntime.make(sqlite());
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* seed;
        yield* Projection.run(total);
        yield* Streams.append(input, [7]);
        const owner = yield* CheckpointStore;
        let firstLoad = true;
        const result = yield* Projection.run(total).pipe(
          Effect.provideService(CheckpointStore, {
            ...owner,
            load: (key) =>
              Effect.gen(function* () {
                const before = yield* owner.load(key);
                if (firstLoad) {
                  firstLoad = false;
                  yield* runKernel(total).pipe(Effect.provideService(CheckpointStore, owner));
                }
                return before;
              }),
          }),
          Effect.result,
        );
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure).toMatchObject({ phase: "checkpoint", reason: "token-conflict" });
        expect((yield* Projection.read(total)).state).toBe(12);
        expect((yield* owner.load(total)).token).toBe("2");
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("fold saves one record per nonempty pass and skips the initial factory after creation", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const state = yield* CheckpointStore;
      let loads = 0;
      let saves = 0;
      let starts = 0;
      const counted = Projection.atomic({
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
          load: (key) =>
            Effect.suspend(() => {
              loads += 1;
              return state.load(key);
            }),
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
      expect({ loads, saves, starts }).toEqual({ loads: 4, saves: 2, starts: 1 });
    }).pipe(Effect.provide(FamilyFixture.withFamilies(Memory.layerMemory(), numbers))),
  );
});
