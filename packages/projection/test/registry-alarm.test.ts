import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Context, Deferred, Effect, Fiber, Layer, Option, Schedule, Schema, Stream } from "effect";
import { StorageFault, Streamsy } from "@streamsy/core";
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
            expect(armed).toEqual(failure === "storage" ? before : Option.some(1));
            const status = (yield* registry.status)[0]!;
            expect(status).toMatchObject({
              state: failure === "storage" && exhausted ? "failed" : "retrying",
              failures: 1,
              retryAt: armed,
            });
            yield* scheduler.alarm;
            expect(seen).toEqual(
              failure === "storage"
                ? [`${prefix}-bad`]
                : [`${prefix}-bad`, `${prefix}-bad`, `${prefix}-good`],
            );
          }),
      );
    }
  }

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
