import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Deferred, Effect, Exit, Layer, Option, Schedule, Schema, Scope } from "effect";
import { ProjectionMarks } from "@streamsy/core/internal";
import {
  Commit,
  InvalidBinding,
  NotAttached,
  Projection,
  ProjectionWakeup,
} from "@streamsy/projection";
import { Source, NumericSource, host, projection, only } from "./support/registry.ts";
import { eventually } from "./support/eventually.ts";

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
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

  it.effect("schedule decodes plain encoded binding params with the registered declaration", () =>
    Effect.gen(function* () {
      const source = yield* NumericSource;
      yield* source.create({ id: 42 });
      yield* source.append({ id: 42 }, ["one"]);
      const seen: Array<number> = [];

      const declaration = Projection.process({
        id: "decoded-params",
        params: { id: Schema.FiniteFromString },
        inputs: { source: NumericSource },
        commit: Commit.atLeastOnce,
        process: (_batch, unit) =>
          Effect.sync(() => {
            seen.push(unit.params.id);
          }),
      });

      const wakeup = yield* ProjectionWakeup.attach(declaration);
      const registry = yield* Projection.Registry;
      yield* registry.schedule({ id: declaration.id, params: { id: "42" } });
      yield* wakeup.run;
      expect(seen).toEqual([42]);
    }),
  );

  for (const objectHost of [false, true]) {
    it.effect(
      `schedule rejects invalid encoded params with InvalidBinding (object host: ${objectHost})`,
      () =>
        Effect.gen(function* () {
          const declaration = Projection.process({
            id: "invalid-params",
            params: { id: Schema.FiniteFromString },
            inputs: { source: NumericSource },
            commit: Commit.atLeastOnce,
            process: () => Effect.void,
          });

          if (objectHost) yield* ProjectionWakeup.attach(declaration);
          else yield* Layer.build(Projection.attach(declaration, { durable: true }));
          const registry = yield* Projection.Registry;

          const cases: ReadonlyArray<Readonly<Record<string, string>>> = [
            {},
            { id: "not-a-number" },
            { id: "1.0" },
            { id: "1", other: "x" },
          ];

          for (const params of cases) {
            const binding = { id: declaration.id, params };
            expect(yield* Effect.flip(registry.schedule(binding))).toEqual(
              new InvalidBinding({ projection: declaration.id, params }),
            );
            expect(yield* registry.retry(binding)).toBe(0);
          }

          const marks = yield* ProjectionMarks;
          expect(yield* marks.nextDue(declaration.id)).toEqual(Option.none());
          expect(only(yield* registry.status).queued).toBe(0);
        }),
    );
  }

  it.effect("requests resolve by id and NotAttached writes nothing after scope closure", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const declaration = projection("identity");

      const other = Projection.process({
        id: "identity",
        generation: 99,
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: () => Effect.void,
      });

      const scope = yield* Scope.make();
      yield* Layer.build(Projection.attach(declaration, { durable: true })).pipe(
        Scope.provide(scope),
      );
      // A separately declared binding and a plain encoded binding resolve by id.
      yield* registry.schedule(other.at({ id: "a" }));
      yield* registry.schedule({ id: declaration.id, params: { id: "b" } });
      expect(yield* registry.retry(other)).toBe(0);
      expect(yield* registry.retry(other.at({ id: "a" }))).toBe(0);
      expect(yield* registry.retry(declaration.id)).toBe(0);
      const marks = yield* ProjectionMarks;
      yield* Scope.close(scope, Exit.void);
      expect(yield* Effect.flip(registry.schedule(declaration.at({ id: "a" })))).toEqual(
        new NotAttached({ projection: "identity" }),
      );
      const before = yield* marks.nextDue("identity");
      expect(yield* Effect.flip(registry.retry("identity"))).toEqual(
        new NotAttached({ projection: "identity" }),
      );
      expect(yield* marks.nextDue("identity")).toEqual(before);
    }),
  );

  for (const objectHost of [false, true]) {
    it.effect(`a hand-built registry dies at attachment (object host: ${objectHost})`, () =>
      Effect.gen(function* () {
        const registry = yield* Projection.Registry;
        const manual = Projection.Registry.of({ ...registry });
        const declaration = projection("manual-registry");

        const registration = objectHost
          ? ProjectionWakeup.attach(declaration).pipe(Effect.asVoid)
          : Layer.build(Projection.attach(declaration)).pipe(Effect.asVoid);

        const exit = yield* registration.pipe(
          Effect.provideService(Projection.Registry, manual),
          Effect.exit,
        );

        expect(Exit.isFailure(exit)).toBe(true);

        if (Exit.isFailure(exit))
          expect(String(exit.cause)).toContain("Projection runners require Registry.layer");
        expect(yield* registry.status).toEqual([]);
      }),
    );

    it.effect(`duplicate ids die at registration (object host: ${objectHost})`, () =>
      Effect.gen(function* () {
        const declaration = projection("duplicate-id");
        yield* Layer.build(Projection.attach(declaration));
        const duplicate = projection(declaration.id);

        const registration = objectHost
          ? ProjectionWakeup.attach(duplicate).pipe(Effect.asVoid)
          : Layer.build(Projection.attach(duplicate)).pipe(Effect.asVoid);

        const exit = yield* Effect.exit(registration);
        expect(Exit.isFailure(exit)).toBe(true);

        if (Exit.isFailure(exit)) expect(String(exit.cause)).toContain("duplicate-id");
        const registry = yield* Projection.Registry;
        expect(only(yield* registry.status).projection).toBe(declaration.id);
      }),
    );
  }

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
        yield* marks.fail(declaration.id, '{"id":"a"}', {
          attempts: 3,
          dueAt: Option.some(60_000),
          failure: { at: 1, reason: Option.none(), message: "failed" },
        });
        yield* marks.fail(declaration.id, '{"id":"b"}', {
          attempts: 2,
          dueAt: Option.some(60_000),
          failure: { at: 1, reason: Option.none(), message: "failed" },
        });
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
    "non-durable retry counts failed bindings, ignores limit-only work and overrides binding backoff",
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
          Projection.attach(declaration, { limit: 1, passRetry: Schedule.spaced("1 hour") }),
        );
        const registry = yield* Projection.Registry;

        for (const id of ["a", "b", "limit"]) yield* registry.schedule(declaration.at({ id }));
        expect(
          only(
            yield* eventually(registry.status, (statuses) => only(statuses).marks.backingOff === 2),
          ).marks.nextDueAt,
        ).toEqual(Option.some(1_000));
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
                only(statuses).marks.retryDue === 0,
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
});
