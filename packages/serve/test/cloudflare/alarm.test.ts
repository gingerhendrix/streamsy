import { expect, layer } from "@effect/vitest";
import { Array, Effect, Layer, Option, Stream } from "effect";
import { Storage, StreamId } from "@streamsy/core";
import { AlarmSlot } from "../../src/cloudflare/alarm.ts";

import { makeWakeups, Wakeups } from "../../src/cloudflare/wakeup.ts";

const fakeStorage = (next: Option.Option<{ readonly at: number; readonly streamId: string }>) =>
  Storage.of({
    capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
    record: () => Effect.succeedNone,
    messages: () => Effect.succeed([]),
    producer: () => Effect.succeedNone,
    mutate: () => Effect.die("unused"),
    changes: () => Stream.empty,
    committed: Effect.die("unexpected committed subscription"),
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
      Layer.succeed(AlarmSlot, {
        current: Effect.succeed(current),
        arm: (at) => Effect.sync(() => actions.push(at)),
        clear: Effect.sync(() => actions.push("clear")),
      }),
    ),
  )((it) => {
    it.effect(`reconcileAlarm follows the indexed deadline table (row ${index})`, () =>
      Effect.gen(function* () {
        const context = yield* Effect.context<Storage | AlarmSlot>();
        yield* makeWakeups(context);
      }).pipe(
        Effect.andThen(() =>
          Effect.sync(() => {
            expect(actions).toEqual([...expected]);
          }),
        ),
      ),
    );
  });
}

layer(
  Layer.mergeAll(
    Layer.succeed(Storage, fakeStorage(Option.none())),
    Layer.succeed(AlarmSlot, {
      current: Effect.succeedNone,
      arm: () => Effect.void,
      clear: Effect.void,
    }),
  ),
)((it) => {
  it.effect("status reports each wakeup's deferred count, 0 when it has none", () =>
    Effect.gen(function* () {
      const context = yield* Effect.context<Storage | AlarmSlot>();

      const { context: object } = yield* makeWakeups(context, [
        {
          name: "with-deferred",
          next: Effect.succeedNone,
          run: Effect.void,
          deferred: Effect.succeed(3),
        },
      ]);

      const status = yield* Effect.flatMap(Wakeups, (wakeups) => wakeups.status).pipe(
        Effect.provideContext(object),
      );

      expect(Array.map(status, ({ name, deferred }) => [name, deferred])).toEqual([
        ["streamsy/expiry", 0],
        ["with-deferred", 3],
      ]);
    }),
  );
});
