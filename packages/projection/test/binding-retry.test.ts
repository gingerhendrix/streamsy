import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Exit, Layer, Option, Schedule, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Commit, Projection, ProjectionWakeup, ProjectionFault } from "@streamsy/projection";
import { Source, NumericSource, host } from "./support/registry.ts";
import { eventually } from "./support/eventually.ts";
import { bindingDelay } from "../src/binding-retry.ts";
import { deferDelay } from "../src/runner.ts";

interface AttemptLog {
  readonly [id: string]: number[];
}

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
  it.effect("fresh default schedule replay exactly preserves exponential delays and the cap", () =>
    Effect.gen(function* () {
      for (let attempts = 1; attempts <= 16; attempts += 1)
        expect(yield* bindingDelay(undefined, attempts)).toEqual(Option.some(deferDelay(attempts)));
    }),
  );

  it.effect("both attach calls reject a legacy retry key in an options variable", () =>
    Effect.sync(() => {
      const declaration = Projection.process({
        id: "legacy-retry",
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: () => Effect.void,
      });

      const options = { durable: true, retry: Schedule.spaced("1 second") };

      for (const attach of [Projection.attach, ProjectionWakeup.attach]) {
        expect(() => attach(declaration, options)).toThrow(RangeError);
        expect(() => attach(declaration, options)).toThrow("passRetry");
      }
    }),
  );

  for (const alarm of [false, true]) {
    for (const durable of [false, true]) {
      it.effect(
        `binding budgets, attempt context and commit bursts (alarm=${alarm}, durable=${durable})`,
        () =>
          Effect.gen(function* () {
            yield* TestClock.setTime(0);
            const id = `budget-${alarm}-${durable}`;
            const calls: AttemptLog = { a: [], b: [] };
            const hooks: string[] = [];
            let healthy = false;

            const declaration = Projection.process({
              id,
              params: { id: Schema.String },
              inputs: { source: Source },
              commit: Commit.atLeastOnce,
              retry: {
                schedule: Schedule.exponential("5 seconds"),
                maxAttempts: 3,
                onExhausted: (unit, failure) =>
                  Effect.sync(() => {
                    expect(failure.retryable).toBe(true);
                    hooks.push(unit.params.id);
                  }),
              },
              process: (_, unit, context) =>
                Effect.suspend(() => {
                  calls[unit.params.id]!.push(context.attempt);

                  return healthy ? Effect.void : Effect.fail("offline");
                }),
            });

            const source = yield* Source;

            for (const key of ["a", "b"]) {
              yield* source.create({ id: key });
              yield* source.append({ id: key }, ["one"]);
            }

            const wakeup = alarm
              ? yield* ProjectionWakeup.attach(declaration, { durable })
              : undefined;

            if (!alarm) yield* Layer.build(Projection.attach(declaration, { durable }));
            const registry = yield* Projection.Registry;
            const rows = registry.failed(declaration);
            const drain = wakeup?.run ?? Effect.void;

            const waitFor = (a: number, b: number) =>
              eventually(
                rows,
                (failed) =>
                  Option.getOrUndefined(Array.findFirst(failed, (row) => row.params.id === "a"))
                    ?.attempts === a &&
                  Option.getOrUndefined(Array.findFirst(failed, (row) => row.params.id === "b"))
                    ?.attempts === b,
              );

            for (const key of ["a", "b"]) yield* registry.schedule(declaration.at({ id: key }));
            yield* drain;
            expect(yield* waitFor(1, 1)).toHaveLength(2);

            for (let n = 0; n < 10; n += 1) {
              yield* source.append({ id: "a" }, [`burst-${n}`]);
              yield* drain;
              yield* Effect.yieldNow;
              expect(Array.map(yield* rows, (row) => row.attempts)).toEqual([1, 1]);
              expect(Array.map(yield* rows, (row) => row.dueAt)).toEqual([
                Option.some(5_000),
                Option.some(5_000),
              ]);
            }

            yield* TestClock.adjust(5_000);
            yield* drain;
            expect(Array.map(yield* waitFor(2, 2), (row) => row.dueAt)).toEqual([
              Option.some(15_000),
              Option.some(15_000),
            ]);
            // Exhaust just one binding; its neighbour keeps its own budget and deadline.
            expect(yield* registry.retry(declaration.at({ id: "a" }))).toBe(1);
            yield* drain;
            const oneStopped = yield* waitFor(3, 2);
            expect(
              Option.getOrUndefined(Array.findFirst(oneStopped, (row) => row.params.id === "a")),
            ).toMatchObject({ stopped: true, exhausted: true, dueAt: Option.none() });
            expect(
              Option.getOrUndefined(Array.findFirst(oneStopped, (row) => row.params.id === "b")),
            ).toMatchObject({ stopped: false, exhausted: false, dueAt: Option.some(15_000) });
            expect(hooks).toEqual(["a"]);
            yield* TestClock.adjust(10_000);
            yield* drain;
            expect(Array.every(yield* waitFor(3, 3), (row) => row.stopped && row.exhausted)).toBe(
              true,
            );
            expect(hooks).toEqual(["a", "b"]);
            expect(calls).toEqual({ a: [1, 2, 3], b: [1, 2, 3] });
            expect(yield* registry.retry(declaration.at({ id: "a" }))).toBe(1);
            yield* drain;
            expect(
              Option.getOrUndefined(
                Array.findFirst(yield* waitFor(4, 3), (row) => row.params.id === "a"),
              ),
            ).toMatchObject({ stopped: true, exhausted: true });
            yield* TestClock.adjust(60_000);
            yield* drain;
            expect(calls).toEqual({ a: [1, 2, 3, 4], b: [1, 2, 3] });
            healthy = true;
            yield* registry.retry(declaration.at({ id: "a" }));
            yield* drain;
            expect(yield* eventually(rows, (failed) => failed.length === 1)).toHaveLength(1);
            expect(calls.a![4]).toBe(5);
            expect(Array.every(calls.a!.slice(5), (attempt) => attempt === 5)).toBe(true);
          }),
      );
    }

    it.effect(
      `custom stored deadline survives restart without stepping the policy (alarm=${alarm})`,
      () =>
        Effect.gen(function* () {
          yield* TestClock.setTime(0);
          let steps = 0;

          const declaration = Projection.process({
            id: `restart-binding-policy-${alarm}`,
            params: { id: Schema.String },
            inputs: { source: Source },
            commit: Commit.atLeastOnce,
            retry: {
              schedule: Schedule.spaced("7 seconds").pipe(
                Schedule.modifyDelay(({ duration }) =>
                  Effect.sync(() => {
                    steps += 1;

                    return duration;
                  }),
                ),
              ),
            },
            process: () => Effect.fail("offline"),
          });

          const source = yield* Source;
          yield* source.create({ id: "a" });
          yield* source.append({ id: "a" }, ["one"]);
          const registry = yield* Projection.Registry;
          const first = yield* Scope.make();

          const wakeup = alarm
            ? yield* ProjectionWakeup.attach(declaration).pipe(Scope.provide(first))
            : undefined;

          if (!alarm)
            yield* Layer.build(Projection.attach(declaration, { durable: true })).pipe(
              Scope.provide(first),
            );
          yield* registry.schedule(declaration.at({ id: "a" }));

          if (wakeup) yield* wakeup.run;
          expect(
            (yield* eventually(registry.failed(declaration), (rows) => rows[0]?.attempts === 1))[0]
              ?.dueAt,
          ).toEqual(Option.some(7_000));
          yield* Scope.close(first, Exit.void);
          yield* TestClock.adjust(1_000);
          const second = alarm ? yield* ProjectionWakeup.attach(declaration) : undefined;

          if (!alarm) yield* Layer.build(Projection.attach(declaration, { durable: true }));

          if (second) {
            expect(yield* second.next).toEqual(Option.some(7_000));
            yield* second.run;
          }

          expect((yield* registry.failed(declaration))[0]?.dueAt).toEqual(Option.some(7_000));
          expect(steps).toBe(1);
        }),
    );

    for (const fault of ["handler", "before-unit", "non-retryable"] as const) {
      it.effect(`exhaustion hook rules for ${fault} (alarm=${alarm})`, () =>
        Effect.gen(function* () {
          let hooks = 0;
          let seenId: number | undefined;

          const declaration = Projection.process({
            id: `hook-${fault}-${alarm}`,
            params: { id: Schema.FiniteFromString },
            inputs: { source: NumericSource },
            commit: Commit.atLeastOnce,
            retry: {
              maxAttempts: 1,
              onExhausted: (unit) =>
                Effect.suspend(() => {
                  hooks += 1;
                  seenId = unit.params.id;

                  return Effect.fail("hook also failed");
                }),
            },
            process: () =>
              Effect.fail(
                fault === "non-retryable"
                  ? new ProjectionFault({
                      phase: "process",
                      reason: "invalid-output",
                      message: "bad",
                    })
                  : "offline",
              ),
          });

          const source = yield* NumericSource;
          yield* source.create({ id: 42 });
          yield* source.append({ id: 42 }, ["one"]);
          const marks = yield* ProjectionMarks;
          // A checkpoint load failure happens before unitOf.
          const base = declaration.at({ id: 42 });

          const broken =
            fault === "before-unit"
              ? {
                  ...declaration,
                  at: () => ({
                    ...base,
                    commit: {
                      ...base.commit,
                      load: () =>
                        Effect.fail(
                          new ProjectionFault({
                            phase: "load",
                            reason: "storage-failure",
                            message: "offline",
                          }),
                        ),
                    },
                  }),
                }
              : declaration;

          const wakeup = alarm ? yield* ProjectionWakeup.attach(broken) : undefined;

          if (!alarm) yield* Layer.build(Projection.attach(broken, { durable: true }));
          const registry = yield* Projection.Registry;
          yield* registry.schedule(base);

          if (wakeup) yield* wakeup.run;
          expect(
            (yield* eventually(registry.failed(broken), (rows) => rows[0]?.stopped === true))[0],
          ).toMatchObject({ stopped: true, attempts: 1, dueAt: Option.none() });
          expect(hooks).toBe(fault === "handler" ? 1 : 0);
          expect(seenId).toBe(fault === "handler" ? 42 : undefined);
          expect((yield* marks.summary(declaration.id, 0)).stopped).toBe(1);
        }),
      );
    }

    it.effect(`a finite binding schedule exhausts without maxAttempts (alarm=${alarm})`, () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(0);
        let hooks = 0;

        const declaration = Projection.process({
          id: `finite-binding-policy-${alarm}`,
          params: { id: Schema.String },
          inputs: { source: Source },
          commit: Commit.atLeastOnce,
          retry: {
            schedule: Schedule.recurs(1).pipe(Schedule.addDelay(() => Effect.succeed("2 seconds"))),
            onExhausted: () =>
              Effect.sync(() => {
                hooks += 1;
              }),
          },
          process: () => Effect.fail("offline"),
        });

        const source = yield* Source;
        yield* source.create({ id: "a" });
        yield* source.append({ id: "a" }, ["one"]);
        const wakeup = alarm ? yield* ProjectionWakeup.attach(declaration) : undefined;

        if (!alarm) yield* Layer.build(Projection.attach(declaration, { durable: true }));
        const registry = yield* Projection.Registry;
        yield* registry.schedule(declaration.at({ id: "a" }));

        if (wakeup) yield* wakeup.run;
        expect(
          (yield* eventually(registry.failed(declaration), (rows) => rows[0]?.attempts === 1))[0]
            ?.dueAt,
        ).toEqual(Option.some(2_000));
        yield* TestClock.adjust(2_000);

        if (wakeup) yield* wakeup.run;
        expect(
          (yield* eventually(registry.failed(declaration), (rows) => rows[0]?.stopped === true))[0],
        ).toMatchObject({ attempts: 2, stopped: true, exhausted: true });
        expect(hooks).toBe(1);
      }),
    );
  }

  it.effect("direct run and follow use attempt 1 and do not apply the binding policy", () =>
    Effect.gen(function* () {
      const attempts: number[] = [];

      const declaration = Projection.process({
        id: "direct-attempt",
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        retry: { maxAttempts: 1, onExhausted: () => Effect.die("direct runs must not exhaust") },
        process: (_, __, context) =>
          Effect.sync(() => {
            attempts.push(context.attempt);
          }),
      });

      const source = yield* Source;
      yield* source.create({ id: "direct-run-follow" });
      yield* source.append({ id: "direct-run-follow" }, ["one"]);
      yield* Projection.run(declaration.at({ id: "direct-run-follow" }));
      yield* source.append({ id: "direct-run-follow" }, ["two"]);
      yield* Projection.follow(declaration.at({ id: "direct-run-follow" })).pipe(
        Stream.take(1),
        Stream.runDrain,
      );
      expect(attempts).toEqual([1, 1]);
    }),
  );
});
