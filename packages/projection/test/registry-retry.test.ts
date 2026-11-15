import { canonicalParams } from "../src/unit.ts";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer, Match, Option, Schedule, Schema } from "effect";
import { StorageFault } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Commit, Projection, ProjectionFault } from "@streamsy/projection";
import { TestClock } from "effect/testing";
import { Source, host, projection, only } from "./support/registry.ts";
import { eventually } from "./support/eventually.ts";

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
  for (const failure of ["binding", "storage", "projection-storage"] as const) {
    for (const request of ["schedule", "retry"] as const) {
      it.effect(`${request} cuts pass backoff on Bun (${failure})`, () =>
        Effect.gen(function* () {
          const id = `backoff-${request}-${failure}`;
          const source = yield* Source;
          yield* TestClock.setTime(0);

          for (const key of ["a", "b"]) {
            yield* source.create({ id: `${id}-${key}` });
            yield* source.append({ id: `${id}-${key}` }, ["one"]);
          }

          let broken = true;
          const seen: Array<string> = [];

          const error = Match.value(failure).pipe(
            Match.when("binding", () => "offline"),
            Match.when(
              "storage",
              () => new StorageFault({ operation: "test", message: "offline", retryable: true }),
            ),
            Match.orElse(
              () =>
                new ProjectionFault({
                  phase: "process",
                  reason: "storage-failure",
                  message: "offline",
                }),
            ),
          );

          const declaration = Projection.process({
            id,
            params: { id: Schema.String },
            inputs: { source: Source },
            commit: Commit.atLeastOnce,
            process: (_batch, unit) =>
              Effect.suspend(() => {
                seen.push(unit.params.id);

                return broken ? Effect.fail(error) : Effect.void;
              }),
          });

          yield* Layer.build(
            Projection.attach(declaration, { retry: Schedule.spaced("1 second") }),
          );
          const registry = yield* Projection.Registry;
          yield* registry.schedule(declaration.at({ id: `${id}-a` }));
          expect(
            only(
              yield* eventually(registry.status, (statuses) => only(statuses).state === "retrying"),
            ).retryAt,
          ).toEqual(Option.some(1_000));
          broken = false;

          if (request === "retry") {
            expect(yield* registry.retry(declaration)).toBe(1);
          } else yield* registry.schedule(declaration.at({ id: `${id}-b` }));

          expect(
            only(
              yield* eventually(
                registry.status,
                (statuses) => only(statuses).failures === 0 && only(statuses).queued === 0,
              ),
            ),
          ).toMatchObject({ state: "running", failures: 0, queued: 0 });
          expect(seen).toEqual(
            request === "schedule" ? [`${id}-a`, `${id}-a`, `${id}-b`] : [`${id}-a`, `${id}-a`],
          );
        }).pipe(Effect.ensuring(TestClock.setTime(0))),
      );
    }
  }

  for (const request of ["schedule", "retry"] as const) {
    it.effect(`Bun ${request} preserves the next storage-fault delay`, () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(0);
        const id = `growing-${request}`;
        const source = yield* Source;
        yield* source.create({ id });
        yield* source.append({ id }, ["one"]);
        let runs = 0;

        const declaration = Projection.process({
          id,
          params: { id: Schema.String },
          inputs: { source: Source },
          commit: Commit.atLeastOnce,
          process: () =>
            Effect.suspend(() => {
              runs += 1;

              return Effect.fail(
                new StorageFault({ operation: "test", message: "offline", retryable: true }),
              );
            }),
        });

        yield* Layer.build(
          Projection.attach(declaration, { retry: Schedule.exponential("1 second") }),
        );
        const registry = yield* Projection.Registry;
        yield* registry.schedule(declaration.at({ id }));
        expect(
          only(yield* eventually(registry.status, (statuses) => only(statuses).failures === 1))
            .retryAt,
        ).toEqual(Option.some(1_000));

        if (request === "schedule") yield* registry.schedule(declaration.at({ id }));
        else expect(yield* registry.retry(declaration)).toBe(1);
        expect(
          only(yield* eventually(registry.status, (statuses) => only(statuses).failures === 2))
            .retryAt,
        ).toEqual(Option.some(2_000));
        yield* TestClock.adjust(1_999);
        expect(runs).toBe(2);
        yield* TestClock.adjust(1);
        expect(
          only(yield* eventually(registry.status, (statuses) => only(statuses).failures === 3))
            .retryAt,
        ).toEqual(Option.some(6_000));
      }).pipe(Effect.ensuring(TestClock.setTime(0))),
    );
  }

  for (const request of ["schedule", "retry"] as const) {
    it.effect(`durable Bun ${request} cuts a storage-read backoff`, () =>
      Effect.gen(function* () {
        const source = yield* Source;
        yield* TestClock.setTime(0);
        const id = `durable-storage-${request}`;
        const keyA = canonicalParams({ id: `${id}-a` });
        const keyB = canonicalParams({ id: `${id}-b` });

        for (const key of ["a", "b"]) {
          yield* source.create({ id: `${id}-${key}` });
          yield* source.append({ id: `${id}-${key}` }, ["one"]);
        }

        const seen: Array<string> = [];

        const declaration = Projection.process({
          id,
          params: { id: Schema.String },
          inputs: { source: Source },
          commit: Commit.atLeastOnce,
          process: (_batch, unit) =>
            Effect.sync(() => {
              seen.push(unit.params.id);
            }),
        });

        const marks = yield* ProjectionMarks;
        yield* marks.fail(id, keyA, {
          attempts: 3,
          dueAt: Option.some(0),
          failure: { at: 1, reason: Option.none(), message: "failed" },
        });

        if (request === "retry")
          yield* marks.fail(id, keyB, {
            attempts: 4,
            dueAt: Option.some(60_000),
            failure: { at: 1, reason: Option.none(), message: "failed" },
          });
        let broken = true;
        yield* Layer.build(
          Projection.attach(declaration, { durable: true, retry: Schedule.spaced("1 second") }),
        ).pipe(
          Effect.provideService(ProjectionMarks, {
            ...marks,
            nextDue: (projectionId) =>
              Effect.suspend(() =>
                broken
                  ? Effect.fail(
                      new StorageFault({
                        operation: "marks.nextDue",
                        message: "offline",
                        retryable: true,
                      }),
                    )
                  : marks.nextDue(projectionId),
              ),
          }),
        );
        const registry = yield* Projection.Registry;
        expect(
          only(
            yield* eventually(registry.status, (statuses) => only(statuses).state === "retrying"),
          ).retryAt,
        ).toEqual(Option.some(1_000));
        broken = false;

        if (request === "retry") expect(yield* registry.retry(declaration)).toBe(1);
        else {
          yield* registry.schedule(declaration.at({ id: `${id}-b` }));
          expect((yield* marks.read(id, [keyB])).get(keyB)).toMatchObject({
            attempts: 0,
            dueAt: 0,
          });
        }

        expect(
          only(
            yield* eventually(
              registry.status,
              (statuses) => only(statuses).failures === 0 && only(statuses).active === 0,
            ),
          ),
        ).toMatchObject({ state: "running", failures: 0, queued: 0 });
        expect(seen).toEqual(
          request === "schedule" ? [`${id}-b`, `${id}-a`] : [`${id}-a`, `${id}-b`],
        );
        expect(yield* marks.nextDue(id)).toEqual(Option.none());
      }).pipe(Effect.ensuring(TestClock.setTime(0))),
    );
  }

  for (const exhausted of [false, true]) {
    it.effect(
      `Bun retry requests a pass even when a failed storage read found no binding (${exhausted})`,
      () =>
        Effect.gen(function* () {
          const declaration = projection(`empty-storage-${exhausted}`);
          const marks = yield* ProjectionMarks;
          let broken = true;
          yield* Layer.build(
            Projection.attach(declaration, {
              durable: true,
              retry: exhausted ? Schedule.recurs(0) : Schedule.spaced("1 hour"),
            }),
          ).pipe(
            Effect.provideService(ProjectionMarks, {
              ...marks,
              nextDue: (id) =>
                Effect.suspend(() =>
                  broken
                    ? Effect.fail(
                        new StorageFault({
                          operation: "marks.nextDue",
                          message: "offline",
                          retryable: true,
                        }),
                      )
                    : marks.nextDue(id),
                ),
            }),
          );
          const registry = yield* Projection.Registry;
          yield* eventually(registry.status, (statuses) => only(statuses).failures === 1);
          broken = false;
          expect(yield* registry.retry(declaration)).toBe(0);
          expect(
            only(yield* eventually(registry.status, (statuses) => only(statuses).failures === 0)),
          ).toMatchObject({ state: "running", failures: 0, lastPassAt: Option.some(0) });
        }),
    );
  }

  it.effect("Bun requests each resume one pass after a storage-fault policy is exhausted", () =>
    Effect.gen(function* () {
      const source = yield* Source;
      const id = "exhausted-storage";
      yield* source.create({ id });
      yield* source.append({ id }, ["one"]);
      let runs = 0;

      const declaration = Projection.process({
        id,
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: () =>
          Effect.suspend(() => {
            runs += 1;

            return Effect.fail(
              new StorageFault({ operation: "test", message: "offline", retryable: true }),
            );
          }),
      });

      yield* Layer.build(Projection.attach(declaration, { retry: Schedule.recurs(0) }));
      const registry = yield* Projection.Registry;
      yield* registry.schedule(declaration.at({ id }));
      expect(
        only(yield* eventually(registry.status, (statuses) => only(statuses).state === "failed"))
          .state,
      ).toBe("failed");
      expect(yield* registry.retry(declaration)).toBe(1);
      expect(
        only(yield* eventually(registry.status, (statuses) => only(statuses).failures === 2)),
      ).toMatchObject({ state: "failed", failures: 2, retryAt: Option.none() });
      yield* registry.schedule(declaration.at({ id }));
      expect(
        only(yield* eventually(registry.status, (statuses) => only(statuses).failures === 3)),
      ).toMatchObject({ state: "failed", queued: 1, failures: 3, retryAt: Option.none() });
      expect(runs).toBe(3);
    }),
  );
});
