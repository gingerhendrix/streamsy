import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Context, Effect, Layer, Option, Schedule, Schema } from "effect";
import { TestClock } from "effect/testing";
import { Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { StreamBusy } from "../../core/src/protocol/errors.ts";
import { Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { AlarmSlot } from "../../serve/src/cloudflare/alarm.ts";
import { makeWakeups } from "../../serve/src/cloudflare/wakeup.ts";
import { memoryHost } from "./support/memory-host.ts";
import { eventually } from "./support/eventually.ts";

class Input extends Streamsy.json<Input>()("Busy/Input", "input", {
  params: {},
  schema: Schema.String,
}) {}

class Output extends Streamsy.json<Output>()("Busy/Output", "output", {
  params: {},
  schema: Schema.String,
}) {}

const local = memoryHost();

const host = Projection.Registry.layer.pipe(
  Layer.provideMerge(
    Protocol.bind(local, Streamsy.group(Input, Output)).pipe(Layer.provideMerge(local)),
  ),
);

layer(host)((it) => {
  for (const alarm of [false, true]) {
    for (const durable of [false, true]) {
      it.effect(`busy output backs off without stopping (alarm=${alarm}, durable=${durable})`, () =>
        Effect.gen(function* () {
          yield* TestClock.setTime(0);
          let appends = 0;
          const output = yield* Output;
          const source = yield* Input;
          yield* source.create({});
          yield* source.append({}, ["one"]);
          yield* output.create({});

          const declaration = Projection.emit({
            id: `busy-${alarm}-${durable}`,
            params: {},
            inputs: { input: Input },
            outputs: { output: Output },
            commit: Commit.producer,
            emit: ({ item }) => ({ output: [item] }),
          });

          const patched = {
            ...output,
            wire: {
              ...output.wire,
              append: (...args: Parameters<typeof output.wire.append>) =>
                Effect.suspend(() => {
                  appends += 1;

                  return appends === 1
                    ? Effect.fail(new StreamBusy({ id: Output.address({}).id }))
                    : output.wire.append(...args);
                }),
            },
          };

          const adapter = Effect.gen(function* () {
            if (!alarm) {
              yield* Layer.build(
                Projection.attach(declaration, { durable, retry: Schedule.recurs(0) }),
              );

              return { tick: Effect.yieldNow };
            }

            const context = yield* Effect.context<Layer.Success<typeof host>>();

            const scheduler = yield* makeWakeups(
              Context.add(context, AlarmSlot, {
                current: Effect.succeedNone,
                arm: () => Effect.void,
                clear: Effect.void,
              }),
              [ProjectionWakeup.attach(declaration, { durable, retry: Schedule.recurs(0) })],
            );

            return { tick: scheduler.alarm };
          });

          const { tick } = yield* adapter.pipe(Effect.provideService(Output, patched));
          const registry = yield* Projection.Registry;
          yield* registry.schedule(declaration.at({}));
          yield* tick;
          yield* eventually(registry.status, (s) => s[0]!.marks.backingOff === 1);
          expect((yield* registry.failed(declaration))[0]).toMatchObject({
            stopped: false,
            attempts: 1,
            dueAt: Option.some(1_000),
            failure: Option.some({
              at: 0,
              reason: Option.some("output-busy"),
              message: expect.stringContaining("busy"),
            }),
          });
          expect((yield* registry.status)[0]).toMatchObject({
            state: "running",
            failures: 0,
            stopped: 0,
          });
          yield* TestClock.adjust(1_000);
          yield* tick;
          yield* eventually(registry.failed(declaration), (rows) => rows.length === 0);
          expect(appends).toBeGreaterThanOrEqual(2);
          expect(yield* registry.failed(declaration)).toEqual([]);
        }),
      );
    }
  }
});
