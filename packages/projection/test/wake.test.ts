import { layer } from "@effect/vitest";
import { expect } from "vitest";
import {
  Array,
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  Record,
  Result,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { DurableState, StorageFault, StreamAddress, Streamsy } from "@streamsy/core";
import { Protocol, Streams, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { layerSqlite } from "./support/sqlite.ts";
import { memoryHost } from "./support/memory-host.ts";
import { producerId } from "../src/commit/producer.ts";

class Files extends Streamsy.json<Files>()("WakeProjection/Files", "folders/:folderId/files", {
  params: { folderId: Schema.String },
  schema: Schema.Struct({ path: Schema.String }),
}) {}

class UserWake extends Streamsy.state<UserWake>()("WakeProjection/UserWake", "users/:user/wake", {
  params: { user: Schema.String },
  collections: Streamsy.wakeCollections,
}) {}

const filesWake = Projection.wake({
  id: "files-wake",
  params: { user: Schema.String, folderId: Schema.String },
  input: Files,
  output: UserWake,
});

const params = { user: "ann", folderId: "docs" };

const folder = { folderId: "docs" };

const sourceId = Files.at(folder).id;

/** The wake owner creates its stream at start, before any source write. */
const wakeCreated = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* (yield* UserWake).create({ user: "ann" });
  }),
);

/** The source lives on one owner with the checkpoints; the wake stream on another. */
const crossOwner = () => {
  const local = memoryHost();
  const foreign = memoryHost();

  return wakeCreated.pipe(
    Layer.provideMerge(
      Layer.merge(
        Protocol.bind(local, Streamsy.group(Files)),
        Protocol.bind(foreign, Streamsy.group(UserWake)),
      ),
    ),
    Layer.provideMerge(local),
  );
};

/** Every change of the wake stream, without headers. */
const changes = Effect.gen(function* () {
  const wake = yield* UserWake;
  const { items } = yield* wake.collect({ user: "ann" });

  return Array.flatMap(items, (item) =>
    StreamAddress.isChange(item)
      ? [
          StreamAddress.isUpsert(item)
            ? { op: item.headers.operation, key: item.key, value: item.value }
            : { op: item.headers.operation, key: item.key },
        ]
      : [],
  );
});

const rows = Effect.gen(function* () {
  const { items } = yield* (yield* UserWake).collect({ user: "ann" });
  const state = DurableState.materialize(DurableState.materialized(UserWake), items);

  return Record.fromEntries(state.rows.stream);
});

layer(crossOwner())((it) => {
  it.effect("one pass gives one row; ten appends in one unit give one row at the last offset", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(folder);
      const first = yield* files.append(folder, [{ path: "a" }]);
      const bound = filesWake.at(params);

      expect((yield* Projection.pass(bound)).units).toBe(1);
      expect(yield* changes).toEqual([
        { op: "upsert", key: sourceId, value: { stream: sourceId, offset: first.offset } },
      ]);

      let last = first;

      for (let index = 0; index < 10; index++)
        last = yield* files.append(folder, [{ path: `f${index}` }]);

      expect((yield* Projection.run(bound)).units).toBe(1);
      expect(yield* changes).toHaveLength(2);
      expect((yield* rows)[sourceId]).toEqual({ stream: sourceId, offset: last.offset });
    }),
  );
});

layer(crossOwner())((it) => {
  it.effect("a close with no body gives closed: true at the same offset, once", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(folder);
      const tail = yield* files.append(folder, [{ path: "a" }]);
      const bound = filesWake.at(params);
      yield* Projection.run(bound);

      const closed = yield* files.append(folder, [], { close: true });
      expect(closed.offset).toBe(tail.offset);

      expect((yield* Projection.run(bound)).status).toBe("source-closed");
      expect((yield* Projection.run(bound)).units).toBe(0);
      expect(yield* changes).toEqual([
        { op: "upsert", key: sourceId, value: { stream: sourceId, offset: tail.offset } },
        {
          op: "upsert",
          key: sourceId,
          value: { stream: sourceId, offset: tail.offset, closed: true },
        },
      ]);
    }),
  );
});

layer(crossOwner())((it) => {
  it.effect("a close with a body gives the closed row in the same unit", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(folder);
      const tail = yield* files.append(folder, [{ path: "a" }], { close: true });
      const bound = filesWake.at(params);
      yield* Projection.run(bound);
      yield* Projection.run(bound);

      expect(yield* changes).toEqual([
        {
          op: "upsert",
          key: sourceId,
          value: { stream: sourceId, offset: tail.offset, closed: true },
        },
      ]);
    }),
  );
});

/** Fail the save that settles a unit, after its append. Pins still save. */
const failSettle = <A, E, R>(body: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const store = yield* CheckpointStore;

    return yield* body.pipe(
      Effect.provideService(CheckpointStore, {
        ...store,
        compareAndSet: (key, token, record) =>
          record.pending === undefined
            ? Effect.fail(
                new ProjectionFault({
                  phase: "checkpoint",
                  reason: "storage-failure",
                  message: "after the wake append",
                }),
              )
            : store.compareAndSet(key, token, record),
      }),
    );
  });

for (const [name, host] of [
  ["memory", crossOwner],
  [
    "SQLite",
    () => {
      const sqlite = layerSqlite();
      const foreign = memoryHost();

      return wakeCreated.pipe(
        Layer.provideMerge(
          Layer.merge(
            Protocol.bind(sqlite, Streamsy.group(Files)),
            Protocol.bind(foreign, Streamsy.group(UserWake)),
          ),
        ),
        Layer.provideMerge(sqlite),
      );
    },
  ],
] as const) {
  layer(host())((it) => {
    it.effect(`${name}: a failed checkpoint after the append gives no second row`, () =>
      Effect.gen(function* () {
        const files = yield* Files;
        yield* files.create(folder);
        const tail = yield* files.append(folder, [{ path: "a" }]);
        const bound = filesWake.at(params);

        const failed = yield* failSettle(Projection.run(bound)).pipe(Effect.result);
        expect(failed._tag).toBe("Failure");
        expect(yield* changes).toHaveLength(1);

        // The retry sends the pinned tuple again, and the wake owner answers Duplicate.
        yield* Projection.run(bound);
        expect(yield* changes).toEqual([
          { op: "upsert", key: sourceId, value: { stream: sourceId, offset: tail.offset } },
        ]);
        const loaded = yield* (yield* CheckpointStore).load(bound);
        expect(Option.getOrThrow(loaded.record).pending).toBeUndefined();
      }),
    );
  });
}

layer(crossOwner())((it) => {
  it.effect("a removed source gives a State delete, and the binding is forgotten", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(folder);
      yield* files.append(folder, [{ path: "a" }]);
      const bound = filesWake.at(params);
      yield* Projection.run(bound);
      yield* files.remove(folder);

      const ended = yield* Projection.run(bound);
      expect(ended.status).toBe("source-closed");
      expect(Array.map(yield* changes, (change) => change.op)).toEqual(["upsert", "delete"]);
      expect(yield* rows).toEqual({});
      expect((yield* (yield* CheckpointStore).load(bound)).token).toBe("0");

      // A forgotten binding of a gone source keeps the read fault and writes nothing more.
      const again = yield* Projection.run(bound).pipe(Effect.result);
      expect(again._tag).toBe("Failure");
      expect(yield* changes).toHaveLength(2);
    }),
  );
});

layer(crossOwner())((it) => {
  it.effect("a source that never existed for the binding keeps its read fault", () =>
    Effect.gen(function* () {
      const result = yield* Projection.run(filesWake.at(params)).pipe(Effect.result);

      expect(result._tag).toBe("Failure");

      if (result._tag === "Failure") expect(result.failure.reason).toBe("history-unavailable");
      expect(yield* changes).toEqual([]);
    }),
  );
});

layer(crossOwner())((it) => {
  it.effect("an expired source gives a State delete", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(folder, { ttlSeconds: 1 });
      yield* files.append(folder, [{ path: "a" }]);
      const bound = filesWake.at(params);
      yield* Projection.run(bound);
      yield* TestClock.adjust(Duration.seconds(5));

      expect((yield* Projection.run(bound)).status).toBe("source-closed");
      expect(Array.map(yield* changes, (change) => change.op)).toEqual(["upsert", "delete"]);
    }),
  );
});

layer(crossOwner())((it) => {
  it.effect("a crash after the closed row, then a remove, still writes the delete", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(folder);
      yield* files.append(folder, [{ path: "a" }]);
      const bound = filesWake.at(params);
      yield* Projection.run(bound);
      yield* files.append(folder, [], { close: true });

      // The closed row takes the next sequence, and its save fails.
      expect((yield* failSettle(Projection.run(bound)).pipe(Effect.result))._tag).toBe("Failure");
      yield* files.remove(folder);

      // The empty closed pin replays before the final gone unit writes the delete.
      yield* Projection.run(bound);
      expect(Array.map(yield* changes, (change) => change.op)).toEqual([
        "upsert",
        "upsert",
        "delete",
      ]);
      expect(yield* rows).toEqual({});
    }),
  );
});

for (const appended of [false, true]) {
  layer(crossOwner())((it) => {
    it.effect(
      `a non-empty wake pin deleted ${appended ? "after" : "before"} append keeps a range-unreproducible fault`,
      () =>
        Effect.gen(function* () {
          const files = yield* Files;
          yield* files.create(folder);
          yield* files.append(folder, [{ path: "a" }]);
          const bound = filesWake.at(params);
          const wake = yield* UserWake;

          const crash = appended
            ? failSettle(Projection.run(bound))
            : Projection.run(bound).pipe(
                Effect.provideService(UserWake, {
                  ...wake,
                  wire: {
                    ...wake.wire,
                    append: () =>
                      Effect.fail(
                        new StorageFault({
                          operation: "append",
                          message: "before append",
                          retryable: true,
                        }),
                      ),
                  },
                }),
              );

          expect((yield* crash.pipe(Effect.result))._tag).toBe("Failure");
          yield* files.remove(folder);
          const result = yield* Projection.run(bound).pipe(Effect.result);
          expect(result._tag === "Failure" && result.failure.reason).toBe("range-unreproducible");
          expect(Array.map(yield* changes, (change) => change.op)).toEqual(
            appended ? ["upsert"] : [],
          );
          expect((yield* (yield* CheckpointStore).load(bound)).token).not.toBe("0");
        }),
    );
  });
}

layer(crossOwner())((it) => {
  it.effect("a crashed empty close unit replays once through Commit.producer", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(folder);
      yield* files.append(folder, [], { close: true });
      const bound = filesWake.at(params);
      expect((yield* failSettle(Projection.run(bound)).pipe(Effect.result))._tag).toBe("Failure");
      expect((yield* Projection.run(bound)).status).toBe("source-closed");
      expect((yield* Projection.run(bound)).units).toBe(0);
      expect(yield* changes).toHaveLength(1);
    }),
  );
});

layer(crossOwner())((it) => {
  it.effect("a close after a crashed open pin gets its own closed row", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(folder);
      yield* files.append(folder, [{ path: "a" }]);
      const bound = filesWake.at(params);
      expect((yield* failSettle(Projection.run(bound)).pipe(Effect.result))._tag).toBe("Failure");
      yield* files.append(folder, [], { close: true });
      expect((yield* Projection.run(bound)).status).toBe("source-closed");
      expect(yield* changes).toHaveLength(2);
      expect((yield* rows)[sourceId]?.closed).toBe(true);
    }),
  );
});

layer(crossOwner())((it) => {
  it.effect("a write from a raw StreamsWriter still gives a row", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(folder);
      const address = Files.at(folder);

      const raw = yield* Streams.append(address, [{ path: "raw" }]).pipe(
        Effect.provideService(StreamsWriter, yield* StreamsWriter),
      );

      yield* Projection.run(filesWake.at(params));
      expect((yield* rows)[sourceId]).toEqual({ stream: sourceId, offset: raw.offset });
    }),
  );
});

const sqliteSameOwner = () => {
  const sqlite = layerSqlite();

  return Protocol.bind(sqlite, Streamsy.group(Files, UserWake)).pipe(Layer.provideMerge(sqlite));
};

// The live clock: the test checks that a commit, not a timer, wakes the follower.
layer(sqliteSameOwner(), { excludeTestServices: true })((it) => {
  it.effect("SQLite: Projection.follow wakes on a source commit with no repair interval", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const files = yield* Files;
        yield* files.create(folder);
        yield* files.append(folder, [{ path: "a" }]);

        const fiber = yield* Projection.follow(filesWake.at(params), {
          repairIntervalMs: 3_600_000,
        }).pipe(Stream.runDrain, Effect.forkScoped);

        const wake = yield* UserWake;
        yield* wake.create({ user: "ann" });
        const tail = yield* files.append(folder, [{ path: "b" }]);

        const observed = yield* wake.follow({ user: "ann" }).pipe(
          Stream.flatMap((batch) => Stream.fromIterable(batch.items)),
          Stream.filter(
            (item) =>
              StreamAddress.isChange(item) &&
              StreamAddress.isUpsert(item) &&
              item.value.offset === tail.offset,
          ),
          Stream.take(1),
          Stream.runCollect,
          Effect.timeout("2 seconds"),
        );

        expect(observed).toHaveLength(1);

        yield* files.remove(folder);
        // The parked hint wakes when the source goes, and the next run writes the delete.
        const exit = yield* Fiber.await(fiber).pipe(Effect.timeoutOption(Duration.seconds(2)));
        expect(Option.isSome(exit)).toBe(true);
        expect(yield* rows).toEqual({});
      }),
    ),
  );
});

/** A hex folder id like artifacts-fs's: 480 characters, so the producer id has 509. */
const longFolder = { folderId: "6461696c792f".repeat(40) };

const longParams = { user: "ann", ...longFolder };

layer(sqliteSameOwner())((it) => {
  it.effect(
    "SQLite: a long param appends its row, and a retry of the pinned tuple is Duplicate",
    () =>
      Effect.gen(function* () {
        const files = yield* Files;
        yield* files.create(longFolder);
        const tail = yield* files.append(longFolder, [{ path: "a" }]);
        const bound = filesWake.at(longParams);
        const id = Files.at(longFolder).id;

        const failed = yield* failSettle(Projection.run(bound)).pipe(Effect.result);
        expect(Result.isFailure(failed)).toBe(true);
        expect(yield* rows).toEqual({ [id]: { stream: id, offset: tail.offset } });

        // The retry sends the pinned tuple again, and storage answers Duplicate.
        expect((yield* Projection.run(bound)).status).toBe("caught-up");
        expect(yield* changes).toHaveLength(1);
        const loaded = yield* (yield* CheckpointStore).load(bound);
        expect(Option.getOrThrow(loaded.record).pending).toBeUndefined();

        const { items } = yield* (yield* UserWake).collect({ user: "ann" });
        const row = Option.getOrThrow(Array.head(items));

        const again = yield* Streams.append(UserWake.at({ user: "ann" }), [row], {
          producer: {
            producerId: yield* producerId("files-wake", 1, longParams),
            producerEpoch: 1,
            producerSeq: 0,
          },
        });

        expect(again._tag).toBe("Duplicate");
      }),
  );
});
