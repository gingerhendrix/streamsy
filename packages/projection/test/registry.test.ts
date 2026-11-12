import { layer } from "@effect/vitest";
import { expect } from "vitest";
import {
  Array,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { StorageFault, Streamsy } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Protocol } from "@streamsy/core/protocol";
import { Commit, Projection, type RunnerStatus } from "@streamsy/projection";
import { TestClock } from "effect/testing";
import { memoryHost } from "./support/memory-host.ts";

class Source extends Streamsy.json<Source>()("Registry/Source", "source/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

const host = () => {
  const local = memoryHost();

  return Protocol.bind(local, Streamsy.group(Source)).pipe(Layer.provideMerge(local));
};

const projection = (id: string, process: Effect.Effect<void> = Effect.void) =>
  Projection.process({
    id,
    params: { id: Schema.String },
    inputs: { source: Source },
    commit: Commit.atLeastOnce,
    process: () => process,
  });

const eventually = <A, E, R>(read: Effect.Effect<A, E, R>, check: (value: A) => boolean) =>
  Effect.gen(function* () {
    for (let turn = 0; turn < 1_000; turn += 1) {
      const value = yield* read;

      if (check(value)) return value;
      yield* Effect.yieldNow;
    }

    return yield* read;
  });

const only = (statuses: ReadonlyArray<RunnerStatus>) => {
  expect(statuses).toHaveLength(1);

  return statuses[0]!;
};

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host())))((it) => {
  it.effect("non-durable status and an initial changes snapshot exist before any pass", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      expect(yield* registry.status).toEqual([]);
      yield* Layer.build(Projection.attach(projection("plain")));

      const expected = [
        {
          projection: "plain",
          state: "running",
          failures: 0,
          retryAt: Option.none(),
          active: 0,
          queued: 0,
          lastPassAt: Option.none(),
          marks: Option.none(),
        },
      ];

      expect(yield* registry.status).toEqual(expected);
      expect(yield* Stream.runHead(registry.changes)).toEqual(Option.some(expected));
    }),
  );

  it.effect("two runners share a registry and each registration ends with its attach scope", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const first = yield* Scope.make();
      const second = yield* Scope.make();
      yield* Layer.build(Projection.attach(projection("one"))).pipe(Scope.provide(first));
      yield* Layer.build(Projection.attach(projection("two"), { durable: true })).pipe(
        Scope.provide(second),
      );
      expect(Array.map(yield* registry.status, (status) => status.projection)).toEqual([
        "one",
        "two",
      ]);
      expect((yield* registry.status)[1]?.marks).toEqual(
        Option.some({
          fresh: 0,
          retryDue: 0,
          backingOff: 0,
          nextDueAt: Option.none(),
        }),
      );
      yield* Scope.close(first, Exit.void);
      expect(only(yield* registry.status).projection).toBe("two");
      yield* Scope.close(second, Exit.void);
      expect(yield* registry.status).toEqual([]);
    }),
  );

  it.effect(
    "changes emits a full registry snapshot after a pass, including cleared durable marks",
    () =>
      Effect.gen(function* () {
        const registry = yield* Projection.Registry;
        const started = yield* Deferred.make<void>();
        yield* Layer.build(
          Layer.mergeAll(
            Projection.attach(projection("durable"), { durable: true }),
            Projection.attach(projection("peer")),
          ),
        );

        const change = yield* registry.changes.pipe(
          Stream.tap(() => Deferred.succeed(started, undefined)),
          Stream.filter(
            Array.some(
              (status) => status.projection === "durable" && Option.isSome(status.lastPassAt),
            ),
          ),
          Stream.runHead,
          Effect.forkScoped,
        );

        yield* Deferred.await(started);
        const source = yield* Source;
        yield* source.create({ id: "a" });
        yield* source.append({ id: "a" }, ["hello"]);
        const statuses = Option.getOrThrow(yield* Fiber.join(change));
        expect(statuses).toHaveLength(2);

        const durable = Option.getOrThrow(
          Array.findFirst(statuses, (status) => status.projection === "durable"),
        );

        expect(durable).toMatchObject({ active: 0, queued: 0, state: "running", failures: 0 });
        expect(durable.marks).toEqual(
          Option.some({
            fresh: 0,
            retryDue: 0,
            backingOff: 0,
            nextDueAt: Option.none(),
          }),
        );
      }),
  );

  it.effect(
    "status counts an executing binding and disjoint durable marks, including a due failed mark",
    () =>
      Effect.gen(function* () {
        const registry = yield* Projection.Registry;
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        yield* Layer.build(
          Projection.attach(
            projection(
              "counts",
              Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
            ),
            { durable: true },
          ),
        );
        const source = yield* Source;
        yield* source.create({ id: "a" });
        yield* source.append({ id: "a" }, ["hello"]);
        yield* Deferred.await(entered);
        const marks = yield* ProjectionMarks;
        yield* Effect.orDie(marks.mark("counts", ["fresh"]));
        yield* marks.defer("counts", "due", 1, 0);
        yield* marks.defer("counts", "later", 2, 1_000);
        const status = only(yield* registry.status);
        expect(status.active).toBe(1);
        expect(status.marks).toEqual(
          Option.some({
            fresh: 2,
            retryDue: 1,
            backingOff: 1,
            nextDueAt: Option.some(0),
          }),
        );
        yield* Deferred.succeed(release, undefined);
      }),
  );

  it.effect("durable bindings waiting behind an active binding remain queued", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const source = yield* Source;

      for (const id of ["a", "b"]) {
        yield* source.create({ id });
        yield* source.append({ id }, ["hello"]);
      }

      const marks = yield* ProjectionMarks;
      yield* Effect.orDie(marks.mark("queued", ['{"id":"a"}', '{"id":"b"}']));
      const entered = yield* Deferred.make<void>();
      yield* Layer.build(
        Projection.attach(
          projection(
            "queued",
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          ),
          { durable: true },
        ),
      );
      yield* Deferred.await(entered);
      expect(only(yield* registry.status)).toMatchObject({ active: 1, queued: 1 });
    }),
  );

  it.effect("an in-memory binding stays active while its handler is suspended", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const entered = yield* Deferred.make<void>();
      yield* Layer.build(
        Projection.attach(
          projection(
            "active",
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      const source = yield* Source;
      yield* source.create({ id: "a" });
      yield* source.append({ id: "a" }, ["hello"]);
      yield* Deferred.await(entered);
      expect(only(yield* registry.status)).toMatchObject({
        active: 1,
        queued: 0,
        marks: Option.none(),
      });
    }),
  );

  it.effect(
    "failed passes report consecutive failures and the actual retry time, then reset on success",
    () =>
      Effect.gen(function* () {
        const registry = yield* Projection.Registry;
        let broken = true;

        const failing = Projection.process({
          id: "retry",
          params: { id: Schema.String },
          inputs: { source: Source },
          commit: Commit.atLeastOnce,
          process: () => Effect.suspend(() => (broken ? Effect.fail("offline") : Effect.void)),
        });

        yield* Layer.build(Projection.attach(failing, { retry: Schedule.spaced("1 second") }));
        const source = yield* Source;
        yield* source.create({ id: "a" });
        yield* source.append({ id: "a" }, ["hello"]);

        const retrying = yield* eventually(
          registry.status,
          (statuses) => only(statuses).state === "retrying",
        );

        expect(only(retrying)).toMatchObject({
          state: "retrying",
          failures: 1,
          retryAt: Option.some(1_000),
          active: 0,
          queued: 1,
        });
        yield* TestClock.adjust(1_000);
        expect(
          only(yield* eventually(registry.status, (statuses) => only(statuses).failures === 2)),
        ).toMatchObject({
          failures: 2,
          retryAt: Option.some(2_000),
        });
        broken = false;
        yield* TestClock.adjust(1_000);
        expect(
          only(yield* eventually(registry.status, (statuses) => only(statuses).failures === 0)),
        ).toMatchObject({
          state: "running",
          failures: 0,
          retryAt: Option.none(),
          active: 0,
          queued: 0,
          lastPassAt: Option.some(2_000),
        });
      }),
  );

  it.effect("an exhausted retry policy leaves a failed runner with no retry time", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;

      const failing = Projection.process({
        id: "failed",
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: () => Effect.fail("offline"),
      });

      yield* Layer.build(Projection.attach(failing, { retry: Schedule.recurs(0) }));
      const source = yield* Source;
      yield* source.create({ id: "a" });
      yield* source.append({ id: "a" }, ["hello"]);
      expect(
        only(yield* eventually(registry.status, (statuses) => only(statuses).state === "failed")),
      ).toMatchObject({
        state: "failed",
        failures: 1,
        retryAt: Option.none(),
        active: 0,
        queued: 1,
      });
    }),
  );

  it.effect("summary faults fail observers without failing the projection pass", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const marks = yield* ProjectionMarks;
      const processed = yield* Deferred.make<void>();

      const fault = new StorageFault({
        operation: "marks.summary",
        message: "offline",
        retryable: true,
      });

      yield* Layer.build(
        Projection.attach(
          projection("read-fault", Deferred.succeed(processed, undefined).pipe(Effect.asVoid)),
          { durable: true },
        ),
      ).pipe(
        Effect.provideService(ProjectionMarks, { ...marks, summary: () => Effect.fail(fault) }),
      );
      expect(yield* Effect.flip(registry.status)).toEqual(fault);
      expect(yield* registry.changes.pipe(Stream.runHead, Effect.flip)).toEqual(fault);
      const source = yield* Source;
      yield* source.create({ id: "a" });
      yield* source.append({ id: "a" }, ["hello"]);
      yield* Deferred.await(processed);
      expect(
        yield* eventually(marks.summary("read-fault", 0), (summary) => summary.fresh === 0),
      ).toEqual({
        fresh: 0,
        retryDue: 0,
        backingOff: 0,
        nextDueAt: Option.none(),
      });
    }),
  );
});
