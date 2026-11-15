import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer, References, Schedule } from "effect";
import { StorageFault } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Projection } from "@streamsy/projection";
import { host, only, projection, Source } from "./support/registry.ts";
import { eventually } from "./support/eventually.ts";

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)), { excludeTestServices: true })(
  (it) => {
    for (const [label, retry] of [
      ["long delay", Schedule.spaced("1 hour")],
      ["finite policy", Schedule.recurs(1)],
    ] as const) {
      it.effect(`a request permits one empty drain without spinning on nextDue (${label})`, () =>
        Effect.gen(function* () {
          const declaration = projection(`bounded-${label}`);
          const marks = yield* ProjectionMarks;
          let reads = 0;
          let drains = 0;
          yield* Layer.build(Projection.attach(declaration, { durable: true, retry })).pipe(
            Effect.provideService(ProjectionMarks, {
              ...marks,
              nextDue: () =>
                Effect.suspend(() => {
                  reads += 1;

                  return Effect.fail(
                    new StorageFault({
                      operation: "marks.nextDue",
                      message: "offline",
                      retryable: true,
                    }),
                  );
                }),
              due: (...args) =>
                Effect.suspend(() => {
                  drains += 1;

                  return marks.due(...args);
                }),
            }),
            Effect.provideService(References.MinimumLogLevel, "None"),
          );
          const registry = yield* Projection.Registry;
          yield* eventually(registry.status, (statuses) => only(statuses).failures > 0);
          expect(yield* registry.retry(declaration)).toBe(0);
          yield* Effect.sleep("200 millis");
          expect(reads).toBeLessThanOrEqual(5);
          expect(drains).toBe(1);
          expect(only(yield* registry.status).state).toBe(
            label === "finite policy" ? "failed" : "retrying",
          );
          const settled = reads;
          yield* Effect.sleep("50 millis");
          expect(reads).toBe(settled);
          expect(drains).toBe(1);
        }),
      );
    }
  },
);

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
  it.effect("a dying retry schedule exhausts the Bun runner and retry still starts a pass", () =>
    Effect.gen(function* () {
      const id = "dying-policy";
      const source = yield* Source;
      yield* source.create({ id });
      yield* source.append({ id }, ["one"]);
      let broken = true;

      const declaration = projection(id);
      const marks = yield* ProjectionMarks;

      yield* Layer.build(
        Projection.attach(declaration, {
          durable: true,
          retry: Schedule.spaced("1 hour").pipe(
            Schedule.modifyDelay(() => Effect.die("broken policy")),
          ),
        }),
      ).pipe(
        Effect.provideService(ProjectionMarks, {
          ...marks,
          due: (...args) =>
            Effect.suspend(() =>
              broken
                ? Effect.fail(
                    new StorageFault({
                      operation: "marks.due",
                      message: "offline",
                      retryable: true,
                    }),
                  )
                : marks.due(...args),
            ),
        }),
      );
      const registry = yield* Projection.Registry;
      yield* registry.schedule(declaration.at({ id }));
      expect(
        only(yield* eventually(registry.status, (statuses) => only(statuses).state === "failed")),
      ).toMatchObject({ failures: 1 });
      broken = false;
      expect(yield* registry.retry(declaration)).toBe(0);
      expect(
        only(
          yield* eventually(
            registry.status,
            (statuses) =>
              only(statuses).failures === 0 &&
              only(statuses).queued === 0 &&
              only(statuses).active === 0,
          ),
        ),
      ).toMatchObject({ state: "running", queued: 0 });
    }),
  );
});
