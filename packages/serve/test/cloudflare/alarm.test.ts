import { expect, test } from "bun:test";
import { Effect, Layer, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { Storage, StreamId } from "@streamsy/core";
import { DEFAULT_LONG_POLL_TIMEOUT_MS, layerProtocol } from "@streamsy/storage/durable-object";
import { Alarm, reconcileAlarm } from "../../src/cloudflare/alarm.ts";

const fakeStorage = (next: Option.Option<{ readonly at: number; readonly streamId: string }>) =>
  Storage.of({
    capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
    record: () => Effect.succeed(Option.none()),
    messages: () => Effect.succeed([]),
    producer: () => Effect.succeed(Option.none()),
    mutate: () => Effect.die("unused"),
    changes: () => Stream.empty,
    nextExpiry: Effect.succeed(
      Option.isSome(next)
        ? Option.some({ ...next.value, streamId: StreamId.make(next.value.streamId) })
        : Option.none(),
    ),
  });

const run = (
  next: Option.Option<{ readonly at: number; readonly streamId: string }>,
  current: Option.Option<number>,
) => {
  const actions: Array<string | number> = [];
  const alarm = Alarm.of({
    current: Effect.succeed(current),
    arm: (at) => Effect.sync(() => actions.push(at)),
    clear: Effect.sync(() => actions.push("clear")),
  });
  const layer = Layer.mergeAll(
    Layer.succeed(Storage, fakeStorage(next)),
    Layer.succeed(Alarm, alarm),
    TestClock.layer(),
  );
  return Effect.runPromise(reconcileAlarm().pipe(Effect.provide(layer))).then(() => actions);
};

test("reconcileAlarm follows the indexed deadline table", async () => {
  expect(await run(Option.none(), Option.some(10_000))).toEqual(["clear"]);
  expect(await run(Option.some({ at: 10_000, streamId: "a" }), Option.none())).toEqual([10_000]);
  expect(await run(Option.some({ at: 10_000, streamId: "a" }), Option.some(10_000))).toEqual([]);
  expect(await run(Option.some({ at: 10_000, streamId: "a" }), Option.some(9_000))).toEqual([]);
  expect(await run(Option.some({ at: 10_000, streamId: "a" }), Option.some(11_000))).toEqual([
    10_000,
  ]);
  expect(await run(Option.some({ at: 0, streamId: "a" }), Option.some(0))).toEqual(["clear", 1]);
  expect(await run(Option.some({ at: 0, streamId: "a" }), Option.none())).toEqual([1]);
});

test("the Durable Object protocol entry publishes the settled long-poll default", () => {
  expect(DEFAULT_LONG_POLL_TIMEOUT_MS).toBe(25_000);
  expect(layerProtocol).toBeInstanceOf(Function);
});
