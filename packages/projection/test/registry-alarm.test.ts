import { canonicalParams } from "../src/unit.ts";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Context, Deferred, Effect, Fiber, Layer, Option, Schedule, Schema, Stream } from "effect";
import { StorageFault, Streamsy } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Protocol } from "@streamsy/core/protocol";
import { Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { AlarmSlot } from "../../serve/src/cloudflare/alarm.ts";
import { makeWakeups } from "../../serve/src/cloudflare/wakeup.ts";
import { memoryHost } from "./support/memory-host.ts";

class Source extends Streamsy.json<Source>()("RegistryAlarm/Source", "source/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

const local = memoryHost({ readLimit: 1 });

const host = Protocol.bind(local, Streamsy.group(Source)).pipe(Layer.provideMerge(local));

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
  for (const failure of ["binding", "storage"] as const) {
    for (const exhausted of [false, true]) {
      it.effect(
        `schedule after ${failure} failure (${exhausted ? "exhausted" : "1 hour floor"})`,
        () =>
          Effect.gen(function* () {
            const source = yield* Source;
            const prefix = `${failure}-${exhausted}`;

            for (const suffix of ["bad", "good"]) {
              const id = `${prefix}-${suffix}`;
              yield* source.create({ id });
              yield* source.append({ id }, ["one"]);
            }

            const seen: Array<string> = [];

            const declaration = Projection.process({
              id: prefix,
              params: { id: Schema.String },
              inputs: { source: Source },
              commit: Commit.atLeastOnce,
              process: (_batch, unit) =>
                Effect.suspend((): Effect.Effect<void, string | StorageFault> => {
                  seen.push(unit.params.id);

                  if (unit.params.id.endsWith("good")) return Effect.void;

                  return failure === "binding"
                    ? Effect.fail("offline")
                    : Effect.fail(
                        new StorageFault({
                          operation: "test",
                          message: "offline",
                          retryable: true,
                        }),
                      );
                }),
            });

            let armed = Option.none<number>();

            const context = yield* Effect.context<
              Layer.Success<typeof host> | Projection.Registry
            >();

            const scheduler = yield* makeWakeups(
              Context.add(context, AlarmSlot, {
                current: Effect.sync(() => armed),
                arm: (at) =>
                  Effect.sync(() => {
                    armed = Option.some(at);
                  }),
                clear: Effect.sync(() => {
                  armed = Option.none();
                }),
              }),
              [
                ProjectionWakeup.attach(declaration, {
                  durable: false,
                  retry: exhausted ? Schedule.recurs(0) : Schedule.spaced("1 hour"),
                }),
              ],
            );

            const registry = yield* Projection.Registry;
            yield* registry.schedule(declaration.at({ id: `${prefix}-bad` }));
            yield* scheduler.alarm;
            const before = exhausted ? Option.none() : Option.some(3_600_000);
            expect(armed).toEqual(before);
            yield* registry.schedule(declaration.at({ id: `${prefix}-good` }));
            expect(armed).toEqual(Option.some(1));
            const status = (yield* registry.status)[0]!;
            expect(status).toMatchObject({
              state: "retrying",
              failures: 1,
              retryAt: armed,
            });
            yield* scheduler.alarm;
            expect(seen).toEqual([`${prefix}-bad`, `${prefix}-bad`, `${prefix}-good`]);
          }),
      );
    }
  }

  it.effect("object requests preserve the next storage-fault delay", () =>
    Effect.gen(function* () {
      const id = "growing-object";
      const source = yield* Source;
      yield* source.create({ id });
      yield* source.append({ id }, ["one"]);

      const declaration = Projection.process({
        id,
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: () =>
          Effect.fail(new StorageFault({ operation: "test", message: "offline", retryable: true })),
      });

      let armed = Option.none<number>();
      const context = yield* Effect.context<Layer.Success<typeof host> | Projection.Registry>();

      const scheduler = yield* makeWakeups(
        Context.add(context, AlarmSlot, {
          current: Effect.sync(() => armed),
          arm: (at) =>
            Effect.sync(() => {
              armed = Option.some(at);
            }),
          clear: Effect.sync(() => {
            armed = Option.none();
          }),
        }),
        [
          ProjectionWakeup.attach(declaration, {
            durable: false,
            retry: Schedule.exponential("1 second"),
          }),
        ],
      );

      const registry = yield* Projection.Registry;
      yield* registry.schedule(declaration.at({ id }));
      yield* scheduler.alarm;
      expect(armed).toEqual(Option.some(1_000));
      expect(yield* registry.retry(declaration)).toBe(1);
      yield* scheduler.alarm;
      expect(armed).toEqual(Option.some(2_000));
      yield* registry.schedule(declaration.at({ id }));
      yield* scheduler.alarm;
      expect(armed).toEqual(Option.some(4_000));
      expect((yield* registry.status)[0]!.failures).toBe(3);
    }),
  );

  for (const durable of [false, true]) {
    it.effect(`retry selects failed work before the next pass (durable: ${durable})`, () =>
      Effect.gen(function* () {
        const id = `selected-${durable}`;
        const source = yield* Source;
        yield* source.create({ id });
        yield* source.append({ id }, ["one"]);

        const declaration = Projection.process({
          id,
          params: { id: Schema.String },
          inputs: { source: Source },
          commit: Commit.atLeastOnce,
          process: () => Effect.fail("offline"),
        });

        const wakeup = yield* ProjectionWakeup.attach(declaration, { durable });
        const registry = yield* Projection.Registry;
        yield* registry.schedule(declaration.at({ id }));
        yield* Effect.exit(wakeup.run);
        const marks = yield* ProjectionMarks;
        const before = yield* marks.read(id, [canonicalParams({ id })]);
        yield* wakeup.bind({
          reconcile: Effect.void,
          retry: Effect.void,
          state: Effect.succeed({ failures: 1, retryAt: Option.some(1_000) }),
        });
        expect(yield* registry.retry(declaration.at({ id }))).toBe(1);
        expect(yield* registry.retry(declaration)).toBe(durable ? 0 : 1);
        const after = yield* marks.read(id, [canonicalParams({ id })]);

        if (durable) {
          expect(after.get(canonicalParams({ id }))).toEqual({
            ...before.get(canonicalParams({ id })),
            dueAt: 0,
          });
        } else expect(after).toEqual(before);
      }),
    );
  }

  for (const exhausted of [false, true]) {
    it.effect(
      `object retry starts a pass after a storage fault with no failed binding (${exhausted})`,
      () =>
        Effect.gen(function* () {
          const marks = yield* ProjectionMarks;
          const id = `empty-object-${exhausted}`;
          let broken = true;
          let passes = 0;
          let armed = Option.none<number>();
          const context = yield* Effect.context<Layer.Success<typeof host> | Projection.Registry>();

          const declaration = Projection.process({
            id,
            params: { id: Schema.String },
            inputs: { source: Source },
            commit: Commit.atLeastOnce,
            process: () => Effect.void,
          });

          const scheduler = yield* makeWakeups(
            Context.add(context, AlarmSlot, {
              current: Effect.sync(() => armed),
              arm: (at) =>
                Effect.sync(() => {
                  armed = Option.some(at);
                }),
              clear: Effect.sync(() => {
                armed = Option.none();
              }),
            }),
            [
              ProjectionWakeup.attach(declaration, {
                retry: exhausted ? Schedule.recurs(0) : Schedule.spaced("1 hour"),
              }).pipe(
                Effect.provideService(ProjectionMarks, {
                  ...marks,
                  nextDue: (projection) =>
                    Effect.suspend(() =>
                      broken
                        ? Effect.fail(
                            new StorageFault({
                              operation: "marks.nextDue",
                              message: "offline",
                              retryable: true,
                            }),
                          )
                        : marks.nextDue(projection),
                    ),
                  due: (...args) =>
                    Effect.suspend(() => {
                      passes += 1;

                      return marks.due(...args);
                    }),
                }),
              ),
            ],
          );

          const registry = yield* Projection.Registry;
          expect((yield* registry.status)[0]!.failures).toBe(1);
          broken = false;
          expect(yield* registry.retry(declaration)).toBe(0);
          expect(armed).toEqual(Option.some(1));
          yield* scheduler.alarm;
          expect(passes).toBe(1);
          expect((yield* registry.status)[0]).toMatchObject({ state: "running", failures: 0 });
        }),
    );
  }

  it.effect("durable retry clears a deadline-read floor and preserves attempts", () =>
    Effect.gen(function* () {
      const marks = yield* ProjectionMarks;
      const id = "durable-deadline-floor";
      yield* marks.defer(id, '{"id":"a"}', 4, 60_000);
      let broken = true;
      const context = yield* Effect.context<Layer.Success<typeof host> | Projection.Registry>();
      let armed = Option.none<number>();

      const declaration = Projection.process({
        id,
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: () => Effect.void,
      });

      const scheduler = yield* makeWakeups(
        Context.add(context, AlarmSlot, {
          current: Effect.sync(() => armed),
          arm: (at) =>
            Effect.sync(() => {
              armed = Option.some(at);
            }),
          clear: Effect.sync(() => {
            armed = Option.none();
          }),
        }),
        [
          ProjectionWakeup.attach(declaration, { retry: Schedule.spaced("1 hour") }).pipe(
            Effect.provideService(ProjectionMarks, {
              ...marks,
              nextDue: (projection) =>
                Effect.suspend(() =>
                  broken
                    ? Effect.fail(
                        new StorageFault({
                          operation: "marks.nextDue",
                          message: "offline",
                          retryable: true,
                        }),
                      )
                    : marks.nextDue(projection),
                ),
            }),
          ),
        ],
      );

      broken = false;
      const registry = yield* Projection.Registry;
      expect(armed).toEqual(Option.some(3_600_000));
      expect(yield* registry.retry(declaration)).toBe(1);
      expect(yield* registry.retry(declaration.at({ id: "a" }))).toBe(0);
      expect((yield* marks.read(id, ['{"id":"a"}'])).get('{"id":"a"}')).toMatchObject({
        attempts: 4,
        dueAt: 0,
      });
      expect(armed).toEqual(Option.some(1));
      yield* scheduler.alarm;
      expect(yield* marks.read(id, ['{"id":"a"}'])).toEqual(new Map());
      expect((yield* registry.status)[0]!.retryAt).toEqual(Option.none());
    }),
  );

  for (const request of ["schedule", "retry"] as const) {
    for (const exhausted of [false, true]) {
      it.effect(
        `${request} reports an armed retry until the next pass (${exhausted ? "exhausted" : "backoff"})`,
        () =>
          Effect.gen(function* () {
            const source = yield* Source;
            const id = `armed-${request}-${exhausted}`;
            yield* source.create({ id });
            yield* source.append({ id }, ["one"]);
            let broken = true;
            const entered = yield* Deferred.make<void>();
            const release = yield* Deferred.make<void>();

            const declaration = Projection.process({
              id,
              params: { id: Schema.String },
              inputs: { source: Source },
              commit: Commit.atLeastOnce,
              process: () =>
                Effect.suspend(() =>
                  broken
                    ? Effect.fail("offline")
                    : Deferred.succeed(entered, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      ),
                ),
            });

            let armed = Option.none<number>();

            const context = yield* Effect.context<
              Layer.Success<typeof host> | Projection.Registry
            >();

            const scheduler = yield* makeWakeups(
              Context.add(context, AlarmSlot, {
                current: Effect.sync(() => armed),
                arm: (at) =>
                  Effect.sync(() => {
                    armed = Option.some(at);
                  }),
                clear: Effect.sync(() => {
                  armed = Option.none();
                }),
              }),
              [
                ProjectionWakeup.attach(declaration, {
                  durable: false,
                  retry: exhausted ? Schedule.recurs(0) : Schedule.spaced("1 hour"),
                }),
              ],
            );

            const registry = yield* Projection.Registry;
            yield* registry.schedule(declaration.at({ id }));
            yield* scheduler.alarm;
            expect((yield* registry.status)[0]!.state).toBe(exhausted ? "failed" : "retrying");
            const changes = yield* Stream.toPull(registry.changes);
            yield* changes;
            broken = false;

            if (request === "schedule") yield* registry.schedule(declaration.at({ id }));
            else expect(yield* registry.retry(declaration)).toBe(1);
            expect(armed).toEqual(Option.some(1));
            expect((yield* registry.status)[0]).toMatchObject({
              state: "retrying",
              failures: 1,
              retryAt: armed,
            });
            expect((yield* changes)[0][0]).toMatchObject({ state: "retrying", retryAt: armed });
            const pass = yield* scheduler.alarm.pipe(Effect.forkScoped);
            yield* Deferred.await(entered);
            expect((yield* registry.status)[0]).toMatchObject({
              state: "running",
              failures: 1,
              active: 1,
              retryAt: Option.none(),
            });
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(pass);
            expect((yield* registry.status)[0]).toMatchObject({
              state: "running",
              failures: 0,
              retryAt: Option.none(),
            });
            expect(armed).toEqual(Option.none());
          }),
      );
    }
  }
});
