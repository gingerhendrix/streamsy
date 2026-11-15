import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Exit, Layer, Option, Schema, Scope } from "effect";
import { TestClock } from "effect/testing";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { Source, host, only } from "./support/registry.ts";
import { eventually } from "./support/eventually.ts";

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
  it.effect(
    "commits preserve durable backoff and schedule makes the binding due across restart",
    () =>
      Effect.gen(function* () {
        let runs = 0;

        const declaration = Projection.process({
          id: "commit-backoff",
          params: { id: Schema.String },
          inputs: { source: Source },
          commit: Commit.atLeastOnce,
          process: () =>
            Effect.suspend(() => {
              runs += 1;

              return Effect.fail("offline");
            }),
        });

        const registry = yield* Projection.Registry;
        const scope = yield* Scope.make();
        const wakeup = yield* ProjectionWakeup.attach(declaration).pipe(Scope.provide(scope));
        const source = yield* Source;
        const params = { id: "bad" };
        const key = '{"id":"bad"}';
        const marks = yield* ProjectionMarks;
        const mark = marks.read(declaration.id, [key]).pipe(Effect.map((rows) => rows.get(key)));
        yield* source.create(params);
        yield* source.append(params, ["one"]);
        yield* wakeup.run;
        expect(runs).toBe(1);
        expect(yield* mark).toMatchObject({ attempts: 1, dueAt: 1_000 });

        // Each commit reaches the fast path, but must leave the failed mark alone.
        for (let n = 0; n < 10; n += 1) {
          yield* source.append(params, [`burst-${n}`]);
          expect(yield* wakeup.run).toEqual({ more: false });
          expect(runs).toBe(1);
          expect(yield* mark).toMatchObject({ attempts: 1, dueAt: 1_000 });
          expect(only(yield* registry.status).queued).toBe(0);
          expect(yield* wakeup.next).toEqual(Option.some(1_000));
        }

        // An explicit schedule persists its override before the process loses its pending set.
        yield* registry.schedule(declaration.at(params));
        expect(yield* mark).toMatchObject({ attempts: 1, dueAt: 0 });
        yield* Scope.close(scope, Exit.void);
        const restarted = yield* ProjectionWakeup.attach(declaration);
        expect(yield* restarted.next).toEqual(Option.some(0));
        yield* restarted.run;
        expect(runs).toBe(2);
        expect(yield* mark).toMatchObject({ attempts: 2, dueAt: 2_000 });

        // The ordinary due-time retry still works.
        yield* TestClock.adjust(2_000);
        yield* restarted.run;
        expect(runs).toBe(3);
        expect(yield* mark).toMatchObject({ attempts: 3, dueAt: 6_000 });
      }),
  );

  it.effect("the Bun loop leaves commit bursts backed off and schedule runs immediately", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(0);
      let runs = 0;

      const declaration = Projection.process({
        id: "bun-commit-backoff",
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: () =>
          Effect.suspend(() => {
            runs += 1;

            return Effect.fail("offline");
          }),
      });

      const source = yield* Source;
      const params = { id: "bun-bad" };
      yield* source.create(params);
      yield* source.append(params, ["one"]);
      yield* Layer.build(Projection.attach(declaration, { durable: true }));
      const registry = yield* Projection.Registry;
      const marks = yield* ProjectionMarks;
      const key = '{"id":"bun-bad"}';
      const mark = marks.read(declaration.id, [key]).pipe(Effect.map((rows) => rows.get(key)));
      yield* registry.schedule(declaration.at(params));
      yield* eventually(mark, (row) => row?.attempts === 1);
      expect(yield* mark).toMatchObject({ attempts: 1, dueAt: 1_000 });

      for (let n = 1; n <= 10; n += 1) {
        yield* TestClock.adjust(1);
        yield* source.append(params, [`burst-${n}`]);

        const statuses = yield* eventually(
          registry.status,
          (snapshot) => Option.getOrElse(only(snapshot).lastPassAt, () => -1) === n,
        );

        expect(only(statuses).lastPassAt).toEqual(Option.some(n));
        expect(only(statuses).queued).toBe(0);
        expect(runs).toBe(1);
        expect(yield* mark).toMatchObject({ attempts: 1, dueAt: 1_000 });
      }

      yield* registry.schedule(declaration.at(params));
      yield* eventually(mark, (row) => row?.attempts === 2);
      expect(runs).toBe(2);
      expect(yield* mark).toMatchObject({ attempts: 2, dueAt: 2_010 });
    }),
  );
});
