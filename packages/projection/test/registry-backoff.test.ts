import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Exit, Layer, Option, Schedule, Schema, Scope } from "effect";
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

  for (const durable of [false, true]) {
    for (const alarm of [false, true]) {
      it.effect(
        `healthy commits run during a peer's backoff (durable: ${durable}, alarm: ${alarm})`,
        () =>
          Effect.gen(function* () {
            yield* TestClock.setTime(0);
            const seen: Array<string> = [];
            const bad = `bad-${durable}-${alarm}`;
            const good = `good-${durable}-${alarm}`;

            const declaration = Projection.process({
              id: `isolated-${durable}-${alarm}`,
              params: { id: Schema.String },
              inputs: { source: Source },
              commit: Commit.atLeastOnce,
              process: (_batch, unit) =>
                Effect.suspend(() => {
                  seen.push(unit.params.id === bad ? "bad" : "good");

                  return unit.params.id === bad ? Effect.fail("offline") : Effect.void;
                }),
            });

            const options = { durable, passRetry: Schedule.recurs(0) };
            const wakeup = alarm ? yield* ProjectionWakeup.attach(declaration, options) : undefined;

            if (!alarm) yield* Layer.build(Projection.attach(declaration, options));
            const registry = yield* Projection.Registry;
            const source = yield* Source;
            yield* source.create({ id: bad });
            yield* source.append({ id: bad }, ["one"]);

            if (wakeup !== undefined) expect(yield* wakeup.run).toEqual({ more: false });

            const failed = only(
              yield* eventually(
                registry.status,
                (statuses) => only(statuses).marks.backingOff === 1,
              ),
            );

            expect(failed).toMatchObject({
              state: "running",
              failures: 0,
              retryAt: Option.none(),
              marks: { backingOff: 1, nextDueAt: Option.some(1_000) },
            });
            yield* source.create({ id: good });
            yield* source.append({ id: good }, ["one"]);

            if (wakeup !== undefined) yield* wakeup.run;
            expect(
              yield* eventually(
                Effect.sync(() => seen.slice()),
                (values) => values.includes("good"),
              ),
            ).toEqual(["bad", "good"]);
            // A later write to the failed input also keeps the deadline and attempts.
            yield* source.append({ id: bad }, ["two"]);
            yield* source.append({ id: good }, ["two"]);

            if (wakeup !== undefined) yield* wakeup.run;
            expect(
              yield* eventually(
                Effect.sync(() => seen.slice()),
                (values) => values.length === 3,
              ),
            ).toEqual(["bad", "good", "good"]);
            expect(only(yield* registry.status).marks).toMatchObject({
              backingOff: 1,
              nextDueAt: Option.some(1_000),
            });
            yield* TestClock.adjust(1_000);

            if (wakeup !== undefined) yield* wakeup.run;
            expect(
              only(
                yield* eventually(registry.status, (statuses) =>
                  Option.contains(only(statuses).marks.nextDueAt, 3_000),
                ),
              ).marks.backingOff,
            ).toBe(1);
            expect(seen).toEqual(["bad", "good", "good", "bad"]);
          }),
      );
    }
  }

  for (const durable of [false, true]) {
    it.effect(`only durable marks retain backoff after the runner restarts (${durable})`, () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(0);

        const declaration = Projection.process({
          id: `restart-${durable}`,
          params: { id: Schema.String },
          inputs: { source: Source },
          commit: Commit.atLeastOnce,
          process: () => Effect.fail("offline"),
        });

        const scope = yield* Scope.make();

        const wakeup = yield* ProjectionWakeup.attach(declaration, { durable }).pipe(
          Scope.provide(scope),
        );

        const source = yield* Source;
        yield* source.create({ id: "bad" });
        yield* source.append({ id: "bad" }, ["one"]);
        yield* wakeup.run;
        expect(yield* wakeup.next).toEqual(Option.some(1_000));
        yield* Scope.close(scope, Exit.void);
        const restarted = yield* ProjectionWakeup.attach(declaration, { durable });
        expect(yield* restarted.next).toEqual(durable ? Option.some(1_000) : Option.none());
        const registry = yield* Projection.Registry;
        expect(only(yield* registry.status).marks.backingOff).toBe(durable ? 1 : 0);
      }),
    );
  }
});
