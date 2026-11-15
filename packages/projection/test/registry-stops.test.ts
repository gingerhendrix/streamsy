import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Context, Effect, Exit, Layer, Option, Schedule, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { DurableState, Offset, StorageFault, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { OwnerRecords, ProjectionMarks } from "@streamsy/core/internal";
import { Protocol, StreamsWriter } from "@streamsy/core/protocol";
import { BunStorage } from "@streamsy/storage/bun";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  ProjectionWakeup,
} from "@streamsy/projection";
import { AlarmSlot } from "../../serve/src/cloudflare/alarm.ts";
import { makeWakeups } from "../../serve/src/cloudflare/wakeup.ts";
import { memoryHost } from "./support/memory-host.ts";
import { eventually } from "./support/eventually.ts";
import type { Executable } from "../src/executable.ts";
import type { Attachable, AttachOptions } from "@streamsy/projection";
import { encodeKey } from "../src/unit.ts";

class Files extends Streamsy.json<Files>()("Stops/Files", "files/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

class Wake extends Streamsy.wake<Wake>()("Stops/Wake", "wake", { params: {} }) {}

class OtherWake extends Streamsy.wake<OtherWake>()("Stops/OtherWake", "other-wake", {
  params: {},
}) {}

const group = Streamsy.group(Files, Wake, OtherWake);

for (const [storageName, local] of [
  ["memory", memoryHost()],
  [
    "sqlite",
    CheckpointStore.layer.pipe(
      Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
    ),
  ],
] as const) {
  const host = Projection.Registry.layer.pipe(
    Layer.provideMerge(Protocol.bind(local, group).pipe(Layer.provideMerge(local))),
  );

  layer(host)((it) => {
    for (const alarm of [false, true]) {
      // Both adapters use the same storage graph; the alarm adapter exercises makeWakeups.
      const launch = (
        declaration: Attachable<
          Executable<
            { files: ReturnType<typeof Files.at> },
            ProjectionFault | StorageFault,
            never,
            CheckpointStore,
            void
          >
        >,
        options: AttachOptions,
      ) =>
        Effect.gen(function* () {
          if (!alarm) {
            yield* Layer.build(Projection.attach(declaration, options));

            return { tick: Effect.yieldNow };
          }

          const context = yield* Effect.context<Layer.Success<typeof host>>();

          const scheduler = yield* makeWakeups(
            Context.add(context, AlarmSlot, {
              current: Effect.succeedNone,
              arm: () => Effect.void,
              clear: Effect.void,
            }),
            [ProjectionWakeup.attach(declaration, options)],
          );

          return { tick: scheduler.alarm };
        });

      for (const durable of [false, true]) {
        it.effect(
          `${storageName} alarm=${alarm} durable=${durable}: a stopped binding reports, isolates and resumes`,
          () =>
            Effect.gen(function* () {
              yield* TestClock.setTime(0);
              const runs: string[] = [];
              let broken = true;

              const declaration = Projection.process({
                id: `binding-stop-${alarm}-${durable}`,
                params: { id: Schema.String },
                inputs: { files: Files },
                commit: Commit.atLeastOnce,
                process: (_batch, unit) =>
                  Effect.suspend(() => {
                    runs.push(unit.params.id);

                    return broken && unit.params.id === "bad"
                      ? Effect.fail(
                          new ProjectionFault({
                            phase: "process",
                            reason: "invalid-output",
                            message: "broken output",
                          }),
                        )
                      : Effect.void;
                  }),
              });

              const files = yield* Files;

              for (const id of ["bad", "good"]) {
                yield* files.create({ id });
                yield* files.append({ id }, ["one"]);
              }

              const registry = yield* Projection.Registry;
              const events = yield* Stream.toPull(registry.events);
              const { tick } = yield* launch(declaration, { durable });
              yield* registry.schedule(declaration.at({ id: "bad" }));
              yield* tick;
              const stopped = yield* eventually(registry.status, (s) => s[0]!.stopped === 1);
              expect(stopped[0]).toMatchObject({
                state: "running",
                failures: 0,
                failure: Option.none(),
                stopped: 1,
              });
              expect((yield* events)[0]).toMatchObject({
                _tag: "Failed",
                params: { id: "bad" },
                stopped: true,
                dueAt: Option.none(),
                failure: {
                  retryable: false,
                  reason: Option.some("invalid-output"),
                  message: expect.stringContaining("broken output"),
                },
              });
              const failed = yield* registry.failed(declaration, { limit: 1 });
              expect(failed).toHaveLength(1);
              expect(failed[0]).toMatchObject({
                params: { id: "bad" },
                attempts: 1,
                stopped: true,
                dueAt: Option.none(),
                failure: Option.some({
                  at: 0,
                  reason: Option.some("invalid-output"),
                  message: expect.stringContaining("broken output"),
                }),
              });
              expect(yield* registry.failed(declaration.at({ id: "good" }))).toEqual([]);
              yield* files.append({ id: "bad" }, ["two"]);
              yield* files.append({ id: "good" }, ["two"]);
              yield* tick;
              yield* eventually(
                Effect.sync(() => runs.length),
                (n) => n === 2,
              );
              yield* TestClock.adjust(3_600_000);
              yield* tick;
              expect(runs).toEqual(["bad", "good"]);
              expect((yield* registry.failed(declaration))[0]!.attempts).toBe(1);
              broken = false;
              expect(yield* registry.retry(declaration.at({ id: "bad" }))).toBe(1);
              yield* tick;
              yield* eventually(
                registry.status,
                (s) => s[0]!.stopped === 0 && s[0]!.marks.retryDue === 0,
              );
              expect(runs.slice(0, 3)).toEqual(["bad", "good", "bad"]);
              expect(yield* registry.failed(declaration)).toEqual([]);
            }),
        );

        // Converted from the H1 poison-trigger probes: the cursor stays, local work runs.
        it.effect(
          `${storageName} alarm=${alarm} durable=${durable}: poison stops only its trigger stream`,
          () =>
            Effect.gen(function* () {
              yield* TestClock.setTime(0);
              const runs: string[] = [];

              const declaration = Projection.process({
                id: `trigger-stop-${alarm}-${durable}`,
                params: { id: Schema.String },
                inputs: { files: Files },
                commit: Commit.atLeastOnce,
                process: (_batch, unit) =>
                  Effect.sync(() => {
                    runs.push(unit.params.id);
                  }),
              });

              const files = yield* Files;

              for (const id of ["local", "scheduled", "triggered"]) {
                yield* files.create({ id });
                yield* files.append({ id }, ["one"]);
              }

              yield* (yield* Wake).create({});
              yield* (yield* OtherWake).create({});
              const { tick } = yield* launch(declaration, { durable, triggers: [Wake, OtherWake] });
              const writer = yield* StreamsWriter;
              yield* writer.append(Wake.address({}).id, {
                data: new TextEncoder().encode('[{"hello":"world"}]'),
                contentType: "application/json",
              });
              yield* (yield* OtherWake).append(
                {},
                DurableState.changes(OtherWake.address({}), {}, [
                  DurableState.upsert("stream", {
                    stream: Files.at({ id: "triggered" }).id,
                    offset: Offset.make(ZERO_OFFSET),
                  }),
                ]),
              );
              yield* tick;
              const registry = yield* Projection.Registry;

              const stopped = yield* eventually(registry.status, (s) =>
                Option.isSome(s[0]!.triggers[0]!.failure),
              );

              expect(stopped[0]).toMatchObject({
                state: "running",
                failures: 0,
                triggers: [
                  {
                    stream: Wake.address({}).id,
                    failure: Option.some({
                      at: 0,
                      retryable: false,
                      reason: Option.some("invalid-source"),
                      message: expect.stringContaining("at offset 000"),
                    }),
                  },
                  { stream: OtherWake.address({}).id, failure: Option.none() },
                ],
              });
              yield* files.append({ id: "local" }, ["two"]);
              yield* tick;
              yield* registry.schedule(declaration.at({ id: "scheduled" }));
              yield* tick;
              yield* eventually(
                Effect.sync(() => runs.length),
                (n) => n === 3,
              );
              expect(runs).toEqual(expect.arrayContaining(["local", "scheduled", "triggered"]));
              const records = yield* OwnerRecords;

              const cursor = records.read(
                encodeKey(["streamsy.projection.trigger.v1", declaration.id, Wake.address({}).id]),
              );

              expect(yield* cursor).toEqual(Option.none());
              yield* TestClock.adjust(10_000);
              yield* tick;
              expect((yield* registry.status)[0]!.triggers[0]!.failure).toEqual(
                stopped[0]!.triggers[0]!.failure,
              );
              expect(yield* registry.retry(declaration.id)).toBe(0);
              yield* tick;

              const retried = yield* eventually(registry.status, (s) =>
                Option.isSome(s[0]!.triggers[0]!.failure),
              );

              expect(Option.getOrThrow(retried[0]!.triggers[0]!.failure).at).toBe(10_000);
              expect(yield* cursor).toEqual(Option.none());
            }),
        );
      }

      for (const stage of ["nextDue", "due", "resume"] as const) {
        it.effect(
          `${storageName} alarm=${alarm}: non-retryable ${stage} stops the runner without stepping its policy`,
          () =>
            Effect.gen(function* () {
              yield* TestClock.setTime(0);
              const marks = yield* ProjectionMarks;
              let broken = true;
              let steps = 0;

              const declaration = Projection.process({
                id: `runner-${alarm}-${stage}`,
                params: { id: Schema.String },
                inputs: { files: Files },
                commit: Commit.atLeastOnce,
                process: () => Effect.void,
              });

              const guard = <A>(operation: typeof stage, effect: Effect.Effect<A, StorageFault>) =>
                Effect.suspend(() =>
                  broken && stage === operation
                    ? Effect.fail(
                        new StorageFault({
                          operation,
                          retryable: false,
                          message: "store cannot open",
                        }),
                      )
                    : effect,
                );

              const { tick } = yield* launch(declaration, {
                durable: true,
                passRetry: Schedule.spaced("1 second").pipe(
                  Schedule.modifyDelay(({ duration }) =>
                    Effect.sync(() => {
                      steps += 1;

                      return duration;
                    }),
                  ),
                ),
              }).pipe(
                Effect.provideService(ProjectionMarks, {
                  ...marks,
                  nextDue: (...args) => guard("nextDue", marks.nextDue(...args)),
                  due: (...args) => guard("due", marks.due(...args)),
                  resume: (...args) => guard("resume", marks.resume(...args)),
                }),
              );

              const registry = yield* Projection.Registry;

              if (stage === "due") yield* registry.retry(declaration);
              yield* tick;
              const stopped = yield* eventually(registry.status, (s) => s[0]!.state === "failed");
              expect(stopped[0]).toMatchObject({
                state: "failed",
                failures: 1,
                retryAt: Option.none(),
                failure: Option.some({
                  at: 0,
                  retryable: false,
                  reason: Option.none(),
                  message: expect.stringContaining("store cannot open"),
                }),
              });
              expect(steps).toBe(0);
              yield* TestClock.adjust(10_000);
              yield* tick;
              expect((yield* registry.status)[0]!.failures).toBe(1);
              broken = false;
              expect(yield* registry.retry(declaration)).toBe(0);
              yield* tick;
              const recovered = yield* eventually(registry.status, (s) => s[0]!.failures === 0);
              expect(recovered[0]).toMatchObject({ state: "running", failure: Option.none() });
            }),
        );
      }

      it.effect(
        `${storageName} alarm=${alarm}: restart resumes a durable stopped binding once and retains its failure listing`,
        () =>
          Effect.gen(function* () {
            yield* TestClock.setTime(0);
            let runs = 0;

            const declaration = Projection.process({
              id: `restart-stop-${alarm}`,
              params: { id: Schema.String },
              inputs: { files: Files },
              commit: Commit.atLeastOnce,
              process: () =>
                Effect.suspend(() => {
                  runs += 1;

                  return Effect.fail(
                    new StorageFault({ operation: "handler", retryable: false, message: "broken" }),
                  );
                }),
            });

            const files = yield* Files;
            yield* files.create({ id: "a" });
            yield* files.append({ id: "a" }, ["one"]);
            const registry = yield* Projection.Registry;
            const first = yield* Scope.make();

            const { tick } = yield* launch(declaration, { durable: true }).pipe(
              Scope.provide(first),
            );

            yield* registry.schedule(declaration.at({ id: "a" }));
            yield* tick;
            yield* eventually(registry.status, (s) => s[0]!.stopped === 1);
            yield* Scope.close(first, Exit.void);
            const { tick: secondTick } = yield* launch(declaration, { durable: true });
            expect((yield* registry.failed(declaration))[0]!.failure).toMatchObject({
              _tag: "Some",
              value: { message: expect.stringContaining("broken") },
            });
            yield* secondTick;
            yield* eventually(registry.status, (s) => s[0]!.stopped === 1);
            expect(runs).toBe(2);
            yield* TestClock.adjust(3_600_000);
            yield* secondTick;
            expect(runs).toBe(2);
            expect((yield* registry.failed(declaration))[0]).toMatchObject({
              attempts: 2,
              stopped: true,
            });
          }),
      );
    }
  });
}
