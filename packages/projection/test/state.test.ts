import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Effect, Layer, ManagedRuntime, Option, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql";
import {
  StreamRef,
  StreamRoute,
  Streams,
  type StreamsReader,
  type StreamsWriter,
  type StorageFault,
} from "@streamsy/core";
import { Checkpoints, Projection, State, type ProjectionFault } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Sqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { serialized, hasLock } from "../src/serialized.ts";
import { forget } from "../src/forget.ts";
import { recordKey } from "@streamsy/projection/checkpoint";

const input = StreamRef.json("state-input", { schema: Schema.Finite });
const total = Projection.make({
  id: "state-total",
  version: 2,
  input,
  process: Projection.fold(Schema.Finite, 0, (sum, { item }) => sum + item),
});
const sqlite = (filename = ":memory:") =>
  Sqlite.layer.pipe(Layer.provideMerge(BunStorage.layerProtocol({ client: { filename } })));
const seed = Effect.gen(function* () {
  yield* Streams.create(input);
  yield* Streams.append(input, [2, 3]);
});

const hosts: ReadonlyArray<
  readonly [
    string,
    Layer.Layer<
      Checkpoints | State | StreamsReader | StreamsWriter,
      StorageFault | ProjectionFault
    >,
  ]
> = [
  ["memory", Memory.layerMemory()],
  ["SQLite", sqlite()],
];
for (const [name, host] of hosts) {
  test(`${name}: fold round trip, version key, State provision, and rollback on second item`, async () => {
    const runtime = ManagedRuntime.make(host);
    try {
      await runtime.runPromise(
        Effect.gen(function* () {
          yield* State;
          yield* seed;
          expect((yield* Projection.run(total)).items).toBe(2);
          expect(yield* Projection.loadState(total, Schema.Finite)).toEqual(Option.some(5));
          expect(yield* Projection.loadState({ ...total, version: 1 }, Schema.Finite)).toEqual(
            Option.none(),
          );
          const checkpoints = yield* Checkpoints;
          const before = yield* checkpoints.load(total);
          yield* Streams.append(input, [7, 11]);
          const failing = Projection.make({
            id: total.id,
            version: total.version,
            input,
            process: Projection.fold(Schema.Finite, 0, (sum, { item }) =>
              item === 11 ? Effect.fail("second item") : Effect.succeed(sum + item),
            ),
          });
          const failure = yield* Projection.run(failing).pipe(Effect.result);
          expect(failure._tag).toBe("Failure");
          expect(yield* checkpoints.load(total)).toEqual(before);
          expect(yield* Projection.loadState(total, Schema.Finite)).toEqual(Option.some(5));
          expect((yield* Projection.run(total)).items).toBe(2);
          expect(yield* Projection.loadState(total, Schema.Finite)).toEqual(Option.some(23));
        }),
      );
    } finally {
      await runtime.dispose();
    }
  });

  test(`${name}: forget deletes both rows, prunes the lock, and is idempotent`, async () => {
    const runtime = ManagedRuntime.make(host);
    try {
      await runtime.runPromise(
        Effect.gen(function* () {
          yield* seed;
          yield* serialized(total);
          expect(hasLock(recordKey(total))).toBe(true);
          // Source imports share the internal lock map, which is intentionally absent from dist's public surface.
          yield* forget(total);
          expect(hasLock(recordKey(total))).toBe(false);
          const checkpoints = yield* Checkpoints;
          expect(yield* checkpoints.load(total)).toEqual({ record: Option.none(), token: "0" });
          expect(yield* Projection.loadState(total, Schema.Finite)).toEqual(Option.none());
          yield* Projection.forget(total);
          yield* Projection.forget({ ...total, id: "never-ran" });
          expect((yield* Projection.serialized(total)).items).toBe(2);
        }),
      );
    } finally {
      await runtime.dispose();
    }
  });

  test(`${name}: failed second delete rolls back state and retains the lock`, async () => {
    const runtime = ManagedRuntime.make(host);
    try {
      await runtime.runPromise(
        Effect.gen(function* () {
          yield* seed;
          yield* serialized(total);
          const checkpoints = yield* Checkpoints;
          const before = yield* checkpoints.load(total);
          const result = yield* forget(total).pipe(
            Effect.provideService(Checkpoints, {
              ...checkpoints,
              remove: () => Effect.die("delete failed"),
            }),
            Effect.exit,
          );
          expect(result._tag).toBe("Failure");
          expect(hasLock(recordKey(total))).toBe(true);
          expect(yield* checkpoints.load(total)).toEqual(before);
          expect(yield* Projection.loadState(total, Schema.Finite)).toEqual(Option.some(5));
          yield* forget(total);
        }),
      );
    } finally {
      await runtime.dispose();
    }
  });
}

test("SQLite token conflict at save rolls back the state already written by fold", async () => {
  const runtime = ManagedRuntime.make(sqlite());
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* seed;
        yield* Projection.run(total);
        yield* Streams.append(input, [7]);
        const owner = yield* Checkpoints;
        const before = yield* owner.load(total);
        const failed = yield* Projection.run(total).pipe(
          Effect.provideService(Checkpoints, {
            ...owner,
            save: (key, record, token) =>
              owner.save(key, record, token).pipe(Effect.andThen(owner.save(key, record, token))),
          }),
          Effect.result,
        );
        expect(failed._tag).toBe("Failure");
        if (failed._tag === "Failure")
          expect(failed.failure).toMatchObject({ phase: "checkpoint", reason: "token-conflict" });
        expect(yield* owner.load(total)).toEqual(before);
        expect(yield* Projection.loadState(total, Schema.Finite)).toEqual(Option.some(5));
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("SQLite file restart retains folded state and reads zero new items", async () => {
  const directory = mkdtempSync("/tmp/projection-state-");
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
    expect(await second.runPromise(Projection.loadState(total, Schema.Finite))).toEqual(
      Option.some(5),
    );
  } finally {
    await second.dispose();
    rmSync(directory, { recursive: true });
  }
});

test("invalid JSON and schema-invalid state fail typed reads; invalid encoding rolls back", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const state = yield* State;
      const checkpoints = yield* Checkpoints;
      for (const encoded of ["{broken", '"not a number"']) {
        yield* checkpoints.withTransaction(state.save(total, encoded));
        const failed = yield* Projection.loadState(total, Schema.Finite).pipe(Effect.result);
        expect(failed._tag).toBe("Failure");
        if (failed._tag === "Failure")
          expect(failed.failure).toMatchObject({ phase: "load", reason: "invalid-record" });
      }
      yield* Projection.forget(total);
      yield* seed;
      const invalid = Projection.make({
        id: total.id,
        version: total.version,
        input,
        process: Projection.fold(Schema.Finite, 0, () => Infinity),
      });
      const failed = yield* Projection.run(invalid).pipe(Effect.result);
      if (failed._tag === "Failure")
        expect(failed.failure).toMatchObject({ phase: "checkpoint", reason: "invalid-record" });
      expect(failed._tag).toBe("Failure");
      expect(yield* state.load(total)).toEqual(Option.none());
    }).pipe(Effect.provide(Memory.layerMemory())),
  );
});

test("family state rows are separate and initial factory receives params only on first use", async () => {
  const numbers = StreamRoute.json("state/:name", {
    params: { name: Schema.String },
    schema: Schema.Finite,
  });
  const calls: string[] = [];
  const family = Projection.family({
    id: "state-family",
    params: { name: Schema.String },
    inputs: { numbers },
    process: Projection.fold(
      Schema.Finite,
      (params) => {
        calls.push(params.name!);
        return params.name === "a" ? 10 : 20;
      },
      (sum, { item }) => sum + item,
    ),
  });
  await Effect.runPromise(
    Effect.gen(function* () {
      for (const name of ["a", "b"]) {
        const member = family.member({ name });
        yield* Streams.create(member.inputs.numbers);
        yield* Streams.append(member.inputs.numbers, [1]);
        yield* Projection.run(member);
        yield* Streams.append(member.inputs.numbers, [2]);
        yield* Projection.run(member);
        expect(yield* Projection.loadState(member, Schema.Finite)).toEqual(
          Option.some(name === "a" ? 13 : 23),
        );
      }
      expect(calls).toEqual(["a", "b"]);
    }).pipe(Effect.provide(Memory.layerMemory())),
  );
});

test("tagged multi-input fold narrows items and follows declaration order", async () => {
  const words = StreamRef.json("state-words", { schema: Schema.String });
  const projection = Projection.make({
    id: "tagged",
    inputs: { input, words },
    process: Projection.fold(
      Schema.String,
      "",
      (value, entry) =>
        value + (entry.input === "input" ? entry.item.toFixed(0) : entry.item.toUpperCase()),
    ),
  });
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      yield* Streams.create(words);
      yield* Streams.append(words, ["a"]);
      yield* Projection.run(projection);
      expect(yield* Projection.loadState(projection, Schema.String)).toEqual(Option.some("23A"));
    }).pipe(Effect.provide(Memory.layerMemory())),
  );
});

test("State SQL failures retain phase and driver cause", async () => {
  const runtime = ManagedRuntime.make(sqlite());
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const state = yield* State;
        yield* sql.unsafe("DROP TABLE streamsy_projection_v1_state");
        for (const [phase, operation] of [
          ["load", state.load(total)],
          ["checkpoint", state.save(total, "1")],
          ["checkpoint", state.remove(total)],
        ] as const) {
          const failed = yield* operation.pipe(Effect.result);
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
        const owner = yield* Checkpoints;
        let firstLoad = true;
        const result = yield* Projection.run(total).pipe(
          Effect.provideService(Checkpoints, {
            ...owner,
            load: (key) =>
              Effect.gen(function* () {
                const before = yield* owner.load(key);
                if (firstLoad) {
                  firstLoad = false;
                  yield* Projection.run(total).pipe(Effect.provideService(Checkpoints, owner));
                }
                return before;
              }),
          }),
          Effect.result,
        );
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure).toMatchObject({ phase: "checkpoint", reason: "token-conflict" });
        expect(yield* Projection.loadState(total, Schema.Finite)).toEqual(Option.some(12));
        expect((yield* owner.load(total)).token).toBe("2");
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("fold performs one load and save per nonempty pass and skips the initial factory after creation", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const state = yield* State;
      let loads = 0;
      let saves = 0;
      let starts = 0;
      const counted = Projection.make({
        id: "counted",
        input,
        process: Projection.fold(
          Schema.Finite,
          () => {
            starts += 1;
            return 0;
          },
          (sum, { item }) => sum + item,
        ),
      });
      const run = Projection.run(counted).pipe(
        Effect.provideService(State, {
          ...state,
          load: (key) =>
            Effect.suspend(() => {
              loads += 1;
              return state.load(key);
            }),
          save: (key, value) =>
            Effect.suspend(() => {
              saves += 1;
              return state.save(key, value);
            }),
        }),
      );
      yield* run;
      yield* Streams.append(input, [7, 11]);
      yield* run;
      expect({ loads, saves, starts }).toEqual({ loads: 2, saves: 2, starts: 1 });
    }).pipe(Effect.provide(Memory.layerMemory())),
  );
});
