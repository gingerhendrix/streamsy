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
import { StorageFault } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { TestClock } from "effect/testing";
import { Source, Other, host, projection, only } from "./support/registry.ts";
import { eventually } from "./support/eventually.ts";

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
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
        Effect.sync(
          () => nextReads.size === 10 && Array.every([...nextReads.values()], (n) => n >= 2),
        ),
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
        expect(only((yield* one)[0]).projection).toBe("lazy-snapshots");
        expect(summaries).toBe(1);
        expect(only((yield* two)[0]).projection).toBe("lazy-snapshots");
        expect(summaries).toBe(2);
        yield* Scope.close(scope, Exit.void);
        expect(yield* one).toEqual([[]]);
        expect(yield* two).toEqual([[]]);
        expect(summaries).toBe(2);
      }),
  );

  it.effect("changes coalesces unread notifications for a slow subscriber", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const pull = yield* Stream.toPull(registry.changes);
      expect(yield* pull).toEqual([[]]);
      const wakeup = yield* ProjectionWakeup.attach(projection("sliding-changes"));

      for (let n = 0; n < 100; n += 1) yield* wakeup.changed;
      expect(only((yield* pull)[0]).projection).toBe("sliding-changes");
      const next = yield* pull.pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      expect(next.pollUnsafe()).toBeUndefined();
      yield* wakeup.changed;
      expect(only((yield* Fiber.join(next))[0]).projection).toBe("sliding-changes");
    }),
  );

  for (const initialFault of [false, true]) {
    it.effect(
      `changes skips a failed ${initialFault ? "initial" : "later"} snapshot and stays open`,
      () =>
        Effect.gen(function* () {
          const registry = yield* Projection.Registry;
          const marks = yield* ProjectionMarks;
          const faultRead = yield* Deferred.make<void>();
          let broken = initialFault;

          const fault = new StorageFault({
            operation: "marks.summary",
            message: "offline",
            retryable: true,
          });

          const source = yield* Source;
          yield* source.create({ id: "a" });
          yield* source.append({ id: "a" }, ["hello"]);
          const declaration = projection("read-fault");

          const wakeup = yield* ProjectionWakeup.attach(declaration).pipe(
            Effect.provideService(ProjectionMarks, {
              ...marks,
              summary: (id, now) =>
                Effect.suspend(() =>
                  broken
                    ? Deferred.succeed(faultRead, undefined).pipe(
                        Effect.andThen(Effect.fail(fault)),
                      )
                    : marks.summary(id, now),
                ),
            }),
          );

          const pull = yield* Stream.toPull(registry.changes);

          if (!initialFault) {
            expect(only((yield* pull)[0]).projection).toBe(declaration.id);
            broken = true;
            yield* wakeup.changed;
          }

          const next = yield* pull.pipe(Effect.forkScoped);
          yield* Deferred.await(faultRead);
          expect(yield* Effect.flip(registry.status)).toEqual(fault);
          expect(next.pollUnsafe()).toBeUndefined();
          yield* registry.schedule(declaration.at({ id: "a" }));
          yield* wakeup.run;
          broken = false;
          yield* wakeup.changed;
          const recovered = only((yield* Fiber.join(next))[0]);
          expect(recovered).toMatchObject({ projection: declaration.id, active: 0, queued: 0 });
          expect(recovered.marks).toEqual(
            Option.some({
              fresh: 0,
              retryDue: 0,
              backingOff: 0,
              nextDueAt: Option.none(),
            }),
          );
        }),
    );
  }
});
