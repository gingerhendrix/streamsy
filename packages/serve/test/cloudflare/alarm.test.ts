import { expect, layer } from "@streamsy/effect-test";
import { Effect, Layer, Option, Stream } from "effect";
import { Storage, StreamId } from "@streamsy/core";
import { Alarm, reconcileAlarm } from "../../src/cloudflare/alarm.ts";

const fakeStorage = (next: Option.Option<{ readonly at: number; readonly streamId: string }>) =>
  Storage.of({
    capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
    record: () => Effect.succeedNone,
    messages: () => Effect.succeed([]),
    producer: () => Effect.succeedNone,
    mutate: () => Effect.die("unused"),
    changes: () => Stream.empty,
    nextExpiry: Effect.succeed(
      Option.isSome(next)
        ? Option.some({ ...next.value, streamId: StreamId.make(next.value.streamId) })
        : Option.none(),
    ),
  });

const cases = [
  [Option.none(), Option.some(10_000), ["clear"]],
  [Option.some({ at: 10_000, streamId: "a" }), Option.none(), [10_000]],
  [Option.some({ at: 10_000, streamId: "a" }), Option.some(10_000), []],
  [Option.some({ at: 10_000, streamId: "a" }), Option.some(9_000), []],
  [Option.some({ at: 10_000, streamId: "a" }), Option.some(11_000), [10_000]],
  [Option.some({ at: 0, streamId: "a" }), Option.some(0), ["clear", 1]],
  [Option.some({ at: 0, streamId: "a" }), Option.none(), [1]],
] as const;

for (const [index, [next, current, expected]] of cases.entries()) {
  const actions: Array<string | number> = [];
  layer(
    Layer.mergeAll(
      Layer.succeed(Storage, fakeStorage(next)),
      Layer.succeed(Alarm, {
        current: Effect.succeed(current),
        arm: (at) => Effect.sync(() => actions.push(at)),
        clear: Effect.sync(() => actions.push("clear")),
      }),
    ),
  )((it) => {
    it.effect(`reconcileAlarm follows the indexed deadline table (row ${index})`, () =>
      reconcileAlarm().pipe(
        Effect.andThen(() =>
          Effect.sync(() => {
            expect(actions).toEqual([...expected]);
          }),
        ),
      ),
    );
  });
}
