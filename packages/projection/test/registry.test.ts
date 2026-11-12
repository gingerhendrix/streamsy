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
import {
  Commit,
  NotAttached,
  Projection,
  ProjectionWakeup,
  type RunnerStatus,
} from "@streamsy/projection";
import { TestClock } from "effect/testing";
import { memoryHost } from "./support/memory-host.ts";

class Source extends Streamsy.json<Source>()("Registry/Source", "source/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

class Other extends Streamsy.json<Other>()("Registry/Other", "other/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

const host = () => {
  const local = memoryHost({ readLimit: 1 });

  return Protocol.bind(local, Streamsy.group(Source, Other)).pipe(Layer.provideMerge(local));
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
  for (const durable of [false, true]) {
    it.effect(`schedule runs pre-existing work without another commit (durable: ${durable})`, () =>
      Effect.gen(function* () {
        const source = yield* Source;
        yield* source.create({ id: "scheduled" });
        yield* source.append({ id: "scheduled" }, ["hello"]);
        const ran = yield* Deferred.make<void>();

        const declaration = projection(
          "scheduled",
          Deferred.succeed(ran, undefined).pipe(Effect.asVoid),
        );

        yield* Layer.build(Projection.attach(declaration, { durable }));
        const registry = yield* Projection.Registry;
        yield* registry.schedule(declaration.at({ id: "scheduled" }));
        yield* Deferred.await(ran);
        expect(
          only(yield* eventually(registry.status, (statuses) => only(statuses).active === 0)),
        ).toMatchObject({
          queued: 0,
          failures: 0,
        });
      }),
    );
  }

  it.effect(
    "NotAttached uses declaration identity, writes nothing, and applies after scope closure",
    () =>
      Effect.gen(function* () {
        const registry = yield* Projection.Registry;
        const declaration = projection("identity");
        const other = projection("identity");
        const scope = yield* Scope.make();
        yield* Layer.build(Projection.attach(declaration, { durable: true })).pipe(
          Scope.provide(scope),
        );
        expect(yield* Effect.flip(registry.schedule(other.at({ id: "a" })))).toEqual(
          new NotAttached({ projection: "identity" }),
        );
        expect(yield* Effect.flip(registry.retry(other))).toEqual(
          new NotAttached({ projection: "identity" }),
        );
        expect(yield* Effect.flip(registry.retry(other.at({ id: "a" })))).toEqual(
          new NotAttached({ projection: "identity" }),
        );
        const marks = yield* ProjectionMarks;
        expect(yield* marks.nextDue("identity")).toEqual(Option.none());
        yield* Scope.close(scope, Exit.void);
        expect(yield* Effect.flip(registry.schedule(declaration.at({ id: "a" })))).toEqual(
          new NotAttached({ projection: "identity" }),
        );
        expect(yield* marks.nextDue("identity")).toEqual(Option.none());
      }),
  );

  it.effect(
    "durable retry wakes a sleeping runner, selects a binding, and preserves attempts",
    () =>
      Effect.gen(function* () {
        const source = yield* Source;

        for (const id of ["a", "b"]) {
          yield* source.create({ id });
          yield* source.append({ id }, ["hello"]);
        }

        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();

        const declaration = projection(
          "retry-marks",
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        );

        const marks = yield* ProjectionMarks;
        yield* marks.defer(declaration.id, '{"id":"a"}', 3, 60_000);
        yield* marks.defer(declaration.id, '{"id":"b"}', 2, 60_000);
        yield* Layer.build(Projection.attach(declaration, { durable: true }));
        const registry = yield* Projection.Registry;
        expect(yield* registry.retry(declaration.at({ id: "a" }))).toBe(1);
        yield* Deferred.await(entered);
        expect((yield* marks.read(declaration.id, ['{"id":"a"}'])).get('{"id":"a"}')).toMatchObject(
          { attempts: 3, dueAt: 0 },
        );
        expect(yield* registry.retry(declaration.at({ id: "a" }))).toBe(0);
        expect(yield* registry.retry(declaration)).toBe(1);
        yield* Deferred.succeed(release, undefined);
        expect(
          (yield* eventually(marks.summary(declaration.id, 0), (summary) => summary.retryDue === 0))
            .retryDue,
        ).toBe(0);
      }),
  );

  it.effect(
    "non-durable retry counts failed bindings, ignores limit-only work and interrupts pass backoff",
    () =>
      Effect.gen(function* () {
        const source = yield* Source;

        for (const id of ["a", "b", "limit"]) {
          yield* source.create({ id });
          yield* source.append({ id }, ["one"]);
          yield* source.append({ id }, ["two"]);
        }

        let broken = true;
        const seen: Array<string> = [];

        const declaration = Projection.process({
          id: "retry-memory",
          params: { id: Schema.String },
          inputs: { source: Source },
          commit: Commit.atLeastOnce,
          process: (_batch, unit) =>
            Effect.suspend(() => {
              seen.push(unit.params.id);

              return broken && unit.params.id !== "limit" ? Effect.fail("offline") : Effect.void;
            }),
        });

        yield* Layer.build(
          Projection.attach(declaration, { limit: 1, retry: Schedule.spaced("1 hour") }),
        );
        const registry = yield* Projection.Registry;

        for (const id of ["a", "b", "limit"]) yield* registry.schedule(declaration.at({ id }));
        expect(
          only(
            yield* eventually(registry.status, (statuses) => only(statuses).state === "retrying"),
          ).retryAt,
        ).toEqual(Option.some(3_600_000));
        expect(yield* registry.retry(declaration.at({ id: "limit" }))).toBe(0);
        expect(yield* registry.retry(declaration.at({ id: "absent" }))).toBe(0);
        broken = false;
        expect(yield* registry.retry(declaration)).toBe(2);
        expect(
          only(
            yield* eventually(
              registry.status,
              (statuses) =>
                only(statuses).queued === 0 &&
                only(statuses).active === 0 &&
                only(statuses).failures === 0,
            ),
          ),
        ).toMatchObject({ failures: 0, retryAt: Option.none(), lastPassAt: Option.some(0) });
        expect(Array.filter(seen, (id) => id === "a").length).toBeGreaterThanOrEqual(2);
        expect(Array.filter(seen, (id) => id === "b").length).toBeGreaterThanOrEqual(2);
        expect(yield* registry.retry(declaration)).toBe(0);
      }),
  );

  it.effect(
    "non-durable retry(binding) selects failed work and a dropped binding is no longer failed",
    () =>
      Effect.gen(function* () {
        const source = yield* Source;
        yield* source.create({ id: "a" });
        yield* source.append({ id: "a" }, ["one"]);
        let broken = true;

        const declaration = Projection.process({
          id: "retry-one",
          params: { id: Schema.String },
          inputs: { source: Source },
          commit: Commit.atLeastOnce,
          process: () => Effect.suspend(() => (broken ? Effect.fail("offline") : Effect.void)),
        });

        const wakeup = yield* ProjectionWakeup.attach(declaration, { durable: false });
        const registry = yield* Projection.Registry;
        yield* registry.schedule(declaration.at({ id: "a" }));
        yield* Effect.exit(wakeup.run);
        expect(yield* registry.retry(declaration.at({ id: "a" }))).toBe(1);
        expect(yield* registry.retry(declaration.at({ id: "a" }))).toBe(0);
        broken = false;
        yield* wakeup.run;
        expect(yield* registry.retry(declaration)).toBe(0);
        yield* registry.schedule(declaration.at({ id: "missing" }));
        yield* wakeup.run;
        expect(yield* registry.retry(declaration.at({ id: "missing" }))).toBe(0);
      }),
  );

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

  it.effect("an exhausted retry policy leaves a failed runner that explicit retry can resume", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      let broken = true;

      const failing = Projection.process({
        id: "failed",
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: () => Effect.suspend(() => (broken ? Effect.fail("offline") : Effect.void)),
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
      broken = false;
      expect(yield* registry.retry(failing)).toBe(1);
      expect(
        only(yield* eventually(registry.status, (statuses) => only(statuses).failures === 0)),
      ).toMatchObject({ state: "running", queued: 0 });
    }),
  );

  it.effect("ten durable runners perform no summary reads without changes subscribers", () =>
    Effect.gen(function* () {
      const source = yield* Source;
      yield* source.create({ id: "summary-cost" });
      const marks = yield* ProjectionMarks;
      let summaries = 0;
      const nextReads = new Map<string, number>();
      const counted = {
        ...marks,
        summary: (id: string, now: number) =>
          Effect.suspend(() => {
            summaries += 1;
            return marks.summary(id, now);
          }),
        nextDue: (id: string) =>
          Effect.suspend(() => {
            nextReads.set(id, (nextReads.get(id) ?? 0) + 1);
            return marks.nextDue(id);
          }),
      };

      for (let n = 0; n < 10; n += 1) {
        const declaration = Projection.process({
          id: `summary-cost-${n}`,
          params: { id: Schema.String },
          inputs: { source: n === 0 ? Source : Other },
          commit: Commit.atLeastOnce,
          process: () => Effect.void,
        });
        yield* Layer.build(Projection.attach(declaration, { durable: true })).pipe(
          Effect.provideService(ProjectionMarks, counted),
        );
      }
      yield* eventually(
        Effect.sync(() => nextReads.size),
        (size) => size === 10,
      );
      nextReads.clear();
      yield* source.append({ id: "summary-cost" }, ["one"]);
      // Every runner reads nextDue after its drain and again when it waits.
      const completed = yield* eventually(
        Effect.sync(() => nextReads.size === 10 && [...nextReads.values()].every((n) => n >= 2)),
        (done) => done,
      );
      expect(completed).toBe(true);
      expect(summaries).toBe(0);
      const registry = yield* Projection.Registry;
      expect(yield* Stream.runHead(registry.changes)).toBeDefined();
      expect(summaries).toBe(10);
    }),
  );

  it.effect(
    "changes reads each subscriber's snapshots on consumption and reports lifecycle changes",
    () =>
      Effect.gen(function* () {
        const registry = yield* Projection.Registry;
        const one = yield* Stream.toPull(registry.changes);
        const two = yield* Stream.toPull(registry.changes);
        expect(yield* one).toEqual([[]]);
        expect(yield* two).toEqual([[]]);
        const marks = yield* ProjectionMarks;
        let summaries = 0;
        const scope = yield* Scope.make();
        yield* Layer.build(Projection.attach(projection("lazy-snapshots"), { durable: true })).pipe(
          Effect.provideService(ProjectionMarks, {
            ...marks,
            summary: (id, now) =>
              Effect.suspend(() => {
                summaries += 1;
                return marks.summary(id, now);
              }),
          }),
          Scope.provide(scope),
        );
        expect(summaries).toBe(0);
        expect(only((yield* one)[0]!).projection).toBe("lazy-snapshots");
        expect(summaries).toBe(1);
        expect(only((yield* two)[0]!).projection).toBe("lazy-snapshots");
        expect(summaries).toBe(2);
        yield* Scope.close(scope, Exit.void);
        expect(yield* one).toEqual([[]]);
        expect(yield* two).toEqual([[]]);
        expect(summaries).toBe(2);
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
