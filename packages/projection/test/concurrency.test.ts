import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Deferred, Effect, Exit, Layer, Order, Schema, Scope } from "effect";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { TestClock } from "effect/testing";
import { Source, host, only, projection } from "./support/registry.ts";
import { eventually } from "./support/eventually.ts";

const ids = ["a", "b", "c", "d"];

/** Seed durable work before the attach loop starts, so all four bindings share a pass. */
const seed = (id: string, keys = ids) =>
  Effect.gen(function* () {
    const source = yield* Source;

    for (const key of keys) {
      yield* source.create({ id: key });
      yield* source.append({ id: key }, [key]);
    }

    const marks = yield* ProjectionMarks;
    yield* marks.mark(
      id,
      Array.map(keys, (key) => JSON.stringify({ id: key })),
    );
  });

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
  it.effect("both hosts reject concurrency values that are not positive safe integers", () =>
    Effect.sync(() => {
      for (const concurrency of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
        for (const attach of [Projection.attach, ProjectionWakeup.attach]) {
          expect(() => attach(projection("invalid"), { concurrency })).toThrow(
            "Projection.attach concurrency must be a positive safe integer",
          );
        }
      }
    }),
  );

  it.effect("four bindings run together, and closing the attach scope interrupts all four", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const seen = new Set<string>();
      const interrupted = new Set<string>();

      const declaration = Projection.process({
        id: "parallel-scope",
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: (_batch, unit) =>
          Effect.gen(function* () {
            seen.add(unit.params.id);

            if (seen.size === 4) yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release).pipe(
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  interrupted.add(unit.params.id);
                }),
              ),
            );
          }),
      });

      yield* seed(declaration.id);
      const scope = yield* Scope.make();
      yield* Layer.build(Projection.attach(declaration, { durable: true, concurrency: 4 })).pipe(
        Scope.provide(scope),
      );
      yield* Deferred.await(entered);

      const registry = yield* Projection.Registry;
      expect(only(yield* registry.status)).toMatchObject({ active: 4, queued: 0 });
      expect(Array.sort([...seen], Order.String)).toEqual(ids);
      yield* Scope.close(scope, Exit.void);
      expect(Array.sort([...interrupted], Order.String)).toEqual(ids);
      expect(yield* registry.status).toEqual([]);
    }),
  );

  it.effect("the concurrency bound leaves remaining bindings queued until a slot opens", () =>
    Effect.gen(function* () {
      const keys = ["i", "j", "k", "l"];
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let runs = 0;

      const declaration = projection(
        "parallel-queue",
        Effect.gen(function* () {
          runs += 1;

          if (runs === 2) yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        }),
      );

      yield* seed(declaration.id, keys);
      yield* Layer.build(Projection.attach(declaration, { durable: true, concurrency: 2 }));
      yield* Deferred.await(entered);

      const registry = yield* Projection.Registry;
      expect(only(yield* registry.status)).toMatchObject({ active: 2, queued: 2 });
      expect(runs).toBe(2);
      yield* Deferred.succeed(release, undefined);

      const statuses = yield* eventually(
        registry.status,
        (rows) => only(rows).active === 0 && only(rows).marks.fresh === 0,
      );

      expect(only(statuses)).toMatchObject({ active: 0, queued: 0 });
      expect(runs).toBe(4);
    }),
  );

  it.effect("a failed binding backs off alone while its concurrent peers commit", () =>
    Effect.gen(function* () {
      const keys = ["e", "f", "g", "h"];
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const attempts = new Map<string, number>();

      const declaration = Projection.process({
        id: "parallel-failure",
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: (_batch, unit) =>
          Effect.gen(function* () {
            const attempt = (attempts.get(unit.params.id) ?? 0) + 1;
            attempts.set(unit.params.id, attempt);

            if (attempts.size === 4) yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);

            if (unit.params.id === "e" && attempt === 1) return yield* Effect.fail("offline");

            return undefined;
          }),
      });

      yield* seed(declaration.id, keys);
      yield* Layer.build(Projection.attach(declaration, { durable: true, concurrency: 4 }));
      const registry = yield* Projection.Registry;
      yield* Deferred.await(entered);
      expect(only(yield* registry.status).active).toBe(4);
      yield* Deferred.succeed(release, undefined);

      const statuses = yield* eventually(
        registry.status,
        (rows) => only(rows).active === 0 && only(rows).marks.backingOff === 1,
      );

      expect(only(statuses).marks).toMatchObject({ fresh: 0, backingOff: 1, stopped: 0 });
      const failed = yield* registry.failed(declaration);
      expect(failed).toHaveLength(1);
      expect(failed[0]).toMatchObject({ params: { id: "e" }, attempts: 1, stopped: false });

      for (const id of keys.slice(1)) {
        expect((yield* Projection.read(declaration.at({ id }))).offsets.source).not.toBe("");
        expect(attempts.get(id)).toBe(1);
      }

      yield* TestClock.adjust("1 second");
      yield* eventually(
        registry.status,
        (rows) =>
          only(rows).marks.backingOff === 0 &&
          only(rows).marks.retryDue === 0 &&
          only(rows).active === 0 &&
          attempts.get("e") === 2,
      );
      expect(attempts.get("e")).toBe(2);
      expect(yield* registry.failed(declaration)).toEqual([]);
    }),
  );
});
