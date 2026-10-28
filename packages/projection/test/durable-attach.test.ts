/* oxlint-disable effecttsgo/node-builtin-import, effecttsgo/crypto-random-uuid -- The restart cases keep one SQLite file across two runtimes. */
import { layer } from "@effect/vitest";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect } from "vitest";
import { Array, Cause, Context, Effect, Exit, Layer, Option, Schema, Scope } from "effect";
import { TestClock } from "effect/testing";
import { Storage, StreamAddress, Streamsy } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Protocol } from "@streamsy/core/protocol";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";

class Files extends Streamsy.json<Files>()("Durable/Files", "folders/:folderId/files", {
  params: { folderId: Schema.String },
  schema: Schema.String,
}) {}

class Wake extends Streamsy.wake<Wake>()("Durable/Wake", "wake", { params: {} }) {}

const filesWake = Projection.wake({
  id: "durable-files-wake",
  params: { folderId: Schema.String },
  input: Files,
  output: Wake,
});

const docs = { folderId: "docs" };

const notes = { folderId: "notes" };

const ParamsJson = Schema.fromJsonString(Schema.Record(Schema.String, Schema.String));

/** The canonical params of a one-param binding. */
const keyOf = Schema.encodeSync(ParamsJson);

const group = Streamsy.group(Files, Wake);

const memory = () => {
  const local = memoryHost({ readLimit: 1 });

  return Protocol.bind(local, group).pipe(Layer.provideMerge(local));
};

const sqlite = (filename: string) => {
  const local = CheckpointStore.layer.pipe(
    Layer.provideMerge(BunStorage.layerProtocol({ client: { filename }, readLimit: 1 })),
  );

  return Protocol.bind(local, group).pipe(Layer.provideMerge(local));
};

const scratchFile = () => join(tmpdir(), `streamsy-durable-${crypto.randomUUID()}.sqlite`);

/** The wake rows as `[operation, key]`, in order. */
const rows = Effect.gen(function* () {
  const { items } = yield* (yield* Wake).collect({});

  return Array.flatMap(items, (item) =>
    StreamAddress.isChange(item) ? [[item.headers.operation, item.key] as const] : [],
  );
}).pipe(Effect.catchTag("StreamNotFound", () => Effect.succeed([])));

const markOf = (projection: string, folder: { readonly folderId: string }) =>
  Effect.flatMap(ProjectionMarks, (marks) =>
    Effect.map(marks.read(projection, [keyOf(folder)]), (found) =>
      Option.fromUndefinedOr(found.get(keyOf(folder))),
    ),
  );

/** Let a forked runner work until `check` holds. */
const eventually = <A, E, R>(read: Effect.Effect<A, E, R>, check: (value: A) => boolean) =>
  Effect.gen(function* () {
    for (let turn = 0; turn < 1_000; turn += 1) {
      const value = yield* read;

      if (check(value)) return value;
      yield* Effect.yieldNow;
    }

    return yield* read;
  });

class Offline extends Schema.TaggedError<Offline, { readonly brand: unique symbol }>()(
  "Offline",
  {},
) {}

/** A projection that counts runs per folder and can fail or write during a run. */
const counting = (
  id: string,
  behaviour: {
    readonly fail?: (folderId: string) => boolean;
    readonly during?: (folderId: string, run: number) => Effect.Effect<void, never, Files>;
  } = {},
) => {
  const runs = new Map<string, number>();

  const projection = Projection.process({
    id,
    params: { folderId: Schema.String },
    inputs: { files: Files },
    commit: Commit.atLeastOnce,
    process: (_batch, unit) =>
      Effect.gen(function* () {
        const folderId = unit.params.folderId;
        const run = (runs.get(folderId) ?? 0) + 1;
        runs.set(folderId, run);

        if (behaviour.fail?.(folderId) === true) return yield* new Offline();

        if (behaviour.during !== undefined) yield* behaviour.during(folderId, run);

        return undefined;
      }),
  });

  return { projection, runs };
};

layer(memory())((it) => {
  it.effect("a normal write runs its binding once and clears its mark", () =>
    Effect.gen(function* () {
      const { projection, runs } = counting("once");
      const wakeup = yield* ProjectionWakeup.attach(projection);
      const files = yield* Files;
      yield* files.create(docs);
      yield* files.append(docs, ["a"]);
      expect(Option.map(yield* markOf("once", docs), (mark) => mark.seq)).toEqual(Option.some(2));

      expect(yield* wakeup.run).toEqual({ more: false });
      expect(runs.get("docs")).toBe(1);
      expect(yield* markOf("once", docs)).toEqual(Option.none());
      expect(yield* wakeup.next).toEqual(Option.none());
      // Nothing is pending, so another pass runs nothing.
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(runs.get("docs")).toBe(1);
    }),
  );
});

layer(memory())((it) => {
  it.effect("a write during a run keeps the mark, and the next pass clears it", () =>
    Effect.gen(function* () {
      const { projection, runs } = counting("during", {
        during: (folderId, run) =>
          run === 1
            ? Effect.orDie(
                Effect.asVoid(
                  Effect.flatMap(Files, (files) => files.append({ folderId }, ["late"])),
                ),
              )
            : Effect.void,
      });

      const wakeup = yield* ProjectionWakeup.attach(projection);
      const files = yield* Files;
      yield* files.create(docs);
      yield* files.append(docs, ["a"]);

      // The run's own write raised `seq`, so the clear by the old `seq` kept the row.
      expect(yield* wakeup.run).toEqual({ more: true });
      expect(Option.isSome(yield* markOf("during", docs))).toBe(true);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(runs.get("docs")).toBe(2);
      expect(yield* markOf("during", docs)).toEqual(Option.none());
    }),
  );
});

layer(memory())((it) => {
  it.effect("a failing binding defers its own mark while its neighbour runs", () =>
    Effect.gen(function* () {
      let broken = true;

      const { projection, runs } = counting("failing", {
        fail: (folderId) => broken && folderId === "docs",
      });

      const wakeup = yield* ProjectionWakeup.attach(projection);
      const files = yield* Files;
      yield* files.create(docs);
      yield* files.append(docs, ["a"]);
      yield* files.create(notes);
      yield* files.append(notes, ["b"]);

      // The failure does not fail the pass, so the host does not back off the projection.
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(runs.get("notes")).toBe(1);
      expect(yield* markOf("failing", notes)).toEqual(Option.none());
      expect(yield* markOf("failing", docs)).toMatchObject(
        Option.some({ attempts: 1, dueAt: 1_000 }),
      );
      expect(yield* wakeup.deferred).toBe(1);
      expect(yield* wakeup.next).toEqual(Option.some(1_000));

      // Before the due time the binding waits.
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(runs.get("docs")).toBe(1);

      // A write to a neighbour does not run the failing binding early.
      yield* files.append(notes, ["c"]);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(runs.get("docs")).toBe(1);
      expect(runs.get("notes")).toBe(2);

      yield* TestClock.adjust(1_000);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(runs.get("docs")).toBe(2);
      expect(yield* markOf("failing", docs)).toMatchObject(
        Option.some({ attempts: 2, dueAt: 3_000 }),
      );

      broken = false;
      yield* TestClock.adjust(2_000);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(runs.get("docs")).toBe(3);
      expect(yield* markOf("failing", docs)).toEqual(Option.none());
      expect(yield* wakeup.deferred).toBe(0);
      expect(yield* wakeup.next).toEqual(Option.none());
    }),
  );
});

layer(memory())((it) => {
  it.effect("a source removed before its first run deletes the mark", () =>
    Effect.gen(function* () {
      const wakeup = yield* ProjectionWakeup.attach(filesWake);
      const files = yield* Files;
      yield* files.create(docs);
      yield* files.append(docs, ["a"]);
      yield* files.remove(docs);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(yield* markOf(filesWake.id, docs)).toEqual(Option.none());
      expect(yield* wakeup.next).toEqual(Option.none());
      expect(yield* rows).toEqual([]);
    }),
  );
});

layer(memory())((it) => {
  it.effect("a mark whose params no longer decode is removed", () =>
    Effect.gen(function* () {
      const marks = yield* ProjectionMarks;
      yield* Effect.orDie(marks.mark(filesWake.id, ["not json", keyOf({ other: "x" })]));
      const wakeup = yield* ProjectionWakeup.attach(filesWake);
      expect(yield* wakeup.next).toEqual(Option.some(0));
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(yield* wakeup.next).toEqual(Option.none());
    }),
  );
});

layer(memory())((it) => {
  it.effect("one pass reads at most `batch` due marks and reports more", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      const wakeup = yield* ProjectionWakeup.attach(filesWake, { batch: 1 });

      for (const folder of [docs, notes]) {
        yield* files.create(folder);
        yield* files.append(folder, ["a"]);
      }

      // A fresh runner sees only marks, as after a restart.
      const restarted = yield* ProjectionWakeup.attach(filesWake, { batch: 1 });
      expect(yield* restarted.run).toEqual({ more: true });
      expect(yield* restarted.run).toEqual({ more: false });
      expect(Array.map(yield* rows, ([operation]) => operation)).toEqual(["upsert", "upsert"]);
      void wakeup;
    }),
  );
});

layer(memory())((it) => {
  it.effect("durable attach over storage with no marks fails with a clear defect", () =>
    Effect.gen(function* () {
      const storage = yield* Storage;

      // A new root fiber, so no service of the test layer leaks in.
      const attachWith = (durable: boolean) =>
        Effect.runSyncExit(
          ProjectionWakeup.attach(filesWake, { durable }).pipe(
            Effect.provideContext(Context.make(Storage, storage)),
            Effect.scoped,
          ),
        );

      const exit = attachWith(true);
      expect(Exit.isFailure(exit) ? String(Cause.squash(exit.cause)) : "attached").toContain(
        "cannot attach durably: the storage layer keeps no projection marks",
      );
      // In-memory mode needs no marks.
      expect(Exit.isSuccess(attachWith(false))).toBe(true);
    }),
  );
});

layer(Layer.empty)((it) => {
  it.effect("after a restart on SQLite the wakeup runs a binding from its mark", () =>
    Effect.gen(function* () {
      const filename = scratchFile();

      // First runtime: the write commits, and the runtime stops before any drain.
      const first = yield* Scope.make();
      const before = yield* Layer.build(sqlite(filename)).pipe(Scope.provide(first));

      yield* Effect.gen(function* () {
        yield* ProjectionWakeup.attach(filesWake);
        const files = yield* Files;
        yield* files.create(docs);
        yield* files.append(docs, ["a"]);
      }).pipe(Effect.provideContext(before), Scope.provide(first));
      yield* Scope.close(first, Exit.void);

      // Second runtime on the same file: the in-memory set is empty.
      const after = yield* Layer.build(sqlite(filename));

      yield* Effect.gen(function* () {
        const wakeup = yield* ProjectionWakeup.attach(filesWake);
        expect(yield* wakeup.next).toEqual(Option.some(0));
        expect(yield* wakeup.run).toEqual({ more: false });
        expect(yield* rows).toEqual([["upsert", Files.at(docs).id]]);
        expect(yield* wakeup.next).toEqual(Option.none());
      }).pipe(Effect.provideContext(after));
    }),
  );

  it.effect("Projection.attach with durable runs due marks when the layer starts", () =>
    Effect.gen(function* () {
      const filename = scratchFile();
      const first = yield* Scope.make();
      const before = yield* Layer.build(sqlite(filename)).pipe(Scope.provide(first));

      yield* Effect.gen(function* () {
        // Marks need a durable runner, but this one never drains.
        yield* ProjectionWakeup.attach(filesWake);
        const files = yield* Files;
        yield* files.create(notes);
        yield* files.append(notes, ["b"]);
      }).pipe(Effect.provideContext(before), Scope.provide(first));
      yield* Scope.close(first, Exit.void);

      const after = yield* Layer.build(
        Layer.provideMerge(Projection.attach(filesWake, { durable: true }), sqlite(filename)),
      );

      const found = yield* eventually(rows, (value) => value.length === 1).pipe(
        Effect.provideContext(after),
      );

      expect(found).toEqual([["upsert", Files.at(notes).id]]);
    }),
  );
});
