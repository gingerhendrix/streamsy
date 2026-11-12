import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  DurableState,
  Offset,
  StorageFault,
  StreamId,
  Streamsy,
  ZERO_OFFSET,
} from "@streamsy/core";
import { OwnerRecords, ProjectionMarks } from "@streamsy/core/internal";
import { Protocol } from "@streamsy/core/protocol";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";

class Files extends Streamsy.json<Files>()("Triggers/Files", "folders/:folderId/files", {
  params: { folderId: Schema.String },
  schema: Schema.String,
}) {}

class Wake extends Streamsy.wake<Wake>()("Triggers/Wake", "wake", { params: {} }) {}

class OtherWake extends Streamsy.wake<OtherWake>()("Triggers/OtherWake", "other-wake", {
  params: {},
}) {}

const group = Streamsy.group(Files, Wake, OtherWake);

const options = { triggers: [Wake, OtherWake] };

const key = (folderId: string) => JSON.stringify({ folderId });

const request = (folderId: string, wake: typeof Wake | typeof OtherWake = Wake) =>
  Effect.flatMap(
    Effect.service<Wake | OtherWake, Streamsy.Api<{}, Streamsy.WakeEvent>>(wake),
    (service) =>
      service.append(
        {},
        DurableState.changes(wake.address({}), {}, [
          DurableState.upsert("stream", {
            stream: Files.at({ folderId }).id,
            offset: Offset.make(ZERO_OFFSET),
          }),
        ]),
      ),
  );

const source = (folderId: string) =>
  Effect.gen(function* () {
    const files = yield* Files;
    yield* files.create({ folderId });
    yield* files.append({ folderId }, [folderId]);
  });

const counting = (
  id: string,
  work: (folderId: string) => Effect.Effect<void> = () => Effect.void,
) => {
  const runs: Array<string> = [];

  const projection = Projection.process({
    id,
    params: { folderId: Schema.String },
    inputs: { files: Files },
    commit: Commit.atLeastOnce,
    process: (_batch, unit) =>
      Effect.sync(() => {
        runs.push(unit.params.folderId);
      }).pipe(Effect.andThen(work(unit.params.folderId))),
  });

  return { projection, runs };
};

const eventually = <A, E, R>(read: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  Effect.gen(function* () {
    for (let turn = 0; turn < 1_000; turn++) {
      const value = yield* read;

      if (ready(value)) return value;
      yield* Effect.yieldNow;
    }

    return yield* read;
  });

for (const [name, host] of [
  ["memory", memoryHost()],
  [
    "sqlite",
    CheckpointStore.layer.pipe(
      Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
    ),
  ],
] as const) {
  const bound = Protocol.bind(host, group).pipe(Layer.provideMerge(host));

  for (const durable of [true, false]) {
    layer(bound)((it) =>
      it.effect(`${name}: Bun trigger runs matched bindings (durable: ${durable})`, () =>
        Effect.gen(function* () {
          const { projection, runs } = counting("bun");
          yield* source("docs");
          yield* (yield* Wake).create({});
          yield* Layer.build(Projection.attach(projection, { ...options, durable }));
          // The runner is already waiting. Only the wake row commits after attachment.
          yield* request("docs");
          expect(
            yield* eventually(
              Effect.sync(() => runs.length),
              (n) => n === 1,
            ),
          ).toBe(1);
          expect(runs).toEqual(["docs"]);
        }),
      ),
    );

    layer(bound)((it) =>
      it.effect(
        `${name}: cursor survives runner restart; interrupted work is ${durable ? "recovered" : "lost"}`,
        () =>
          Effect.gen(function* () {
            const started = yield* Deferred.make<void>();
            let paused = true;

            const { projection, runs } = counting("restart", () =>
              paused
                ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
                : Effect.void,
            );

            yield* source("docs");
            yield* source("later");
            yield* (yield* Wake).create({});
            yield* request("docs");
            const scope = yield* Scope.fork(yield* Scope.Scope);

            const first = yield* ProjectionWakeup.attach(projection, { ...options, durable }).pipe(
              Scope.provide(scope),
            );

            expect(yield* first.next).toEqual(Option.some(0));
            const running = yield* first.run.pipe(Effect.forkChild);
            yield* Deferred.await(started);
            yield* Fiber.interrupt(running);
            yield* Scope.close(scope, Exit.void);
            paused = false;
            // This row is written with no runner subscribed at all.
            yield* request("later");
            const second = yield* ProjectionWakeup.attach(projection, { ...options, durable });
            yield* second.run;
            expect(Array.filter(runs, (folder) => folder === "docs")).toHaveLength(durable ? 2 : 1);
            expect(Array.filter(runs, (folder) => folder === "later")).toHaveLength(1);
            expect(yield* second.next).toEqual(Option.none());
            const third = yield* ProjectionWakeup.attach(projection, { ...options, durable });
            expect(yield* third.next).toEqual(Option.none());
            yield* third.run;
            expect(runs).toHaveLength(durable ? 3 : 2);
          }),
      ),
    );
  }

  layer(bound)((it) =>
    it.effect(
      `${name}: multiple triggers coalesce bindings; unmatched rows advance without a run`,
      () =>
        Effect.gen(function* () {
          const { projection, runs } = counting("matching");
          yield* source("docs");
          yield* (yield* Wake).create({});
          yield* (yield* OtherWake).create({});
          const runner = yield* ProjectionWakeup.attach(projection, options);
          yield* (yield* Wake).append(
            {},
            DurableState.changes(Wake.address({}), {}, [
              DurableState.upsert("stream", {
                stream: StreamId.make("unmatched"),
                offset: Offset.make(ZERO_OFFSET),
              }),
            ]),
          );
          expect(yield* runner.next).toEqual(Option.some(0));
          yield* runner.run;
          expect(runs).toEqual([]);
          expect(yield* runner.next).toEqual(Option.none());
          yield* request("docs");
          yield* request("docs", OtherWake);
          yield* runner.run;
          expect(runs).toEqual(["docs"]);
          expect(yield* runner.next).toEqual(Option.none());
        }),
    ),
  );

  layer(bound)((it) =>
    it.effect(`${name}: a trigger failure backs off alone and new rows preserve its deadline`, () =>
      Effect.gen(function* () {
        const { projection, runs } = counting("backoff", (folder) =>
          folder === "broken" ? Effect.die("broken") : Effect.void,
        );

        for (const folder of ["broken", "healthy", "later"]) yield* source(folder);
        yield* (yield* Wake).create({});
        const runner = yield* ProjectionWakeup.attach(projection, options);
        yield* request("broken");
        yield* request("healthy");
        expect(yield* runner.run).toEqual({ more: false });
        expect(runs).toEqual(["broken", "healthy"]);
        expect(yield* runner.next).toEqual(Option.some(1_000));
        yield* request("broken");
        yield* request("later");
        yield* runner.run;
        expect(runs).toEqual(["broken", "healthy", "later"]);
        expect(yield* runner.next).toEqual(Option.some(1_000));
        yield* TestClock.adjust(1_000);
        yield* runner.run;
        expect(runs).toEqual(["broken", "healthy", "later", "broken"]);
      }),
    ),
  );

  layer(Projection.Registry.layer.pipe(Layer.provideMerge(bound)))((it) => {
    it.effect(`${name}: trigger bindings share registry queue, failure, run and drop events`, () =>
      Effect.gen(function* () {
        const registry = yield* Projection.Registry;
        const snapshots: Array<{ active: number; queued: number }> = [];

        const projection = Projection.process({
          id: "trigger-registry",
          params: { folderId: Schema.String },
          inputs: { files: Files },
          commit: Commit.atLeastOnce,
          process: (_batch, unit) =>
            Effect.gen(function* () {
              const status = (yield* registry.status)[0]!;
              snapshots.push({ active: status.active, queued: status.queued });

              if (unit.params.folderId === "broken") return yield* Effect.die("offline");

              return undefined;
            }),
        });

        yield* source("broken");
        yield* source("healthy");
        yield* (yield* Wake).create({});
        const runner = yield* ProjectionWakeup.attach(projection, options);
        const pull = yield* Stream.toPull(registry.events);

        for (const folder of ["broken", "healthy", "gone"]) yield* request(folder);
        yield* runner.run;
        expect(snapshots).toEqual([
          { active: 1, queued: 2 },
          { active: 1, queued: 1 },
        ]);

        const events = yield* Stream.fromPull(Effect.succeed(pull)).pipe(
          Stream.take(3),
          Stream.runCollect,
        );

        expect(Array.map(events, (event) => event._tag)).toEqual(["Failed", "Ran", "Dropped"]);
        expect((yield* registry.status)[0]).toMatchObject({ active: 0, queued: 0 });
      }),
    );
  });

  for (const afterWrite of [false, true]) {
    layer(bound)((it) =>
      it.effect(
        `${name}: failure ${afterWrite ? "after" : "before"} cursor write rolls back marks and cursor`,
        () =>
          Effect.gen(function* () {
            const { projection, runs } = counting("atomic");
            yield* source("docs");
            yield* (yield* Wake).create({});
            yield* request("docs");
            const records = yield* OwnerRecords;
            let failedKey = "";

            const runner = yield* ProjectionWakeup.attach(projection, options).pipe(
              Effect.provideService(OwnerRecords, {
                ...records,
                write: (recordKey, encoded) =>
                  Effect.gen(function* () {
                    failedKey = recordKey;

                    if (afterWrite) yield* records.write(recordKey, encoded);

                    return yield* new StorageFault({
                      operation: "test.cursor",
                      message: "injected",
                      retryable: true,
                    });
                  }),
              }),
            );

            expect(Exit.isFailure(yield* Effect.exit(runner.run))).toBe(true);
            expect(runs).toEqual([]);
            expect(yield* records.read(failedKey)).toEqual(Option.none());
            const marks = yield* ProjectionMarks;
            expect((yield* marks.read(projection.id, [key("docs")])).size).toBe(0);
            const restarted = yield* ProjectionWakeup.attach(projection, options);
            yield* restarted.run;
            expect(runs).toEqual(["docs"]);
            expect(yield* restarted.next).toEqual(Option.none());
          }),
      ),
    );
  }
}
