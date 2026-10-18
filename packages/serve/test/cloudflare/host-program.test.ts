import { expect, it as baseIt, layer } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Logger, Stream } from "effect";
import { Storage, StorageFault } from "@streamsy/core";
import { AlarmSlot } from "../../src/cloudflare/alarm.ts";
import { withMutationReconciliation } from "../../src/cloudflare/host-program.ts";
import { makeWakeups } from "../../src/cloudflare/wakeup.ts";

let failDeadline = false;

const failingStorage = Storage.of({
  capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
  record: () => Effect.succeedNone,
  messages: () => Effect.succeed([]),
  producer: () => Effect.succeedNone,
  mutate: () => Effect.die("unused"),
  changes: () => Stream.empty,
  nextExpiry: Effect.suspend(() =>
    failDeadline
      ? Effect.fail(
          new StorageFault({
            operation: "test.nextExpiry",
            message: "test failure",
            retryable: true,
          }),
        )
      : Effect.succeedNone,
  ),
});

baseIt.effect("mutation reconciliation runs after an interrupted committed mutation", () =>
  Effect.gen(function* () {
    const events: Array<string> = [];

    const result = yield* Effect.gen(function* () {
      const committed = yield* Deferred.make<void>();

      const fiber = yield* Effect.forkChild(
        withMutationReconciliation(
          Effect.gen(function* () {
            yield* Effect.uninterruptible(
              Effect.sync(() => events.push("mutation committed")).pipe(
                Effect.andThen(Deferred.succeed(committed, undefined)),
              ),
            );

            return yield* Effect.never;
          }),
          Effect.sync(() => events.push("reconciled")),
        ),
      );

      yield* Deferred.await(committed);
      yield* Fiber.interrupt(fiber);
    });

    expect(result).toBeUndefined();
    expect(events).toEqual(["mutation committed", "reconciled"]);
  }),
);

baseIt.effect("a reconciliation defect cannot replace a committed mutation result", () =>
  Effect.gen(function* () {
    const response = yield* withMutationReconciliation(
      Effect.succeed("committed"),
      Effect.die("reconcile defect"),
    );

    expect(response).toBe("committed");
  }),
);

baseIt.effect("a reconciliation failure after a mutation is logged", () =>
  Effect.gen(function* () {
    const messages: Array<string> = [];

    const response = yield* withMutationReconciliation(
      Effect.succeed("committed"),
      Effect.fail("reconcile failure"),
    ).pipe(
      Effect.provideService(
        Logger.CurrentLoggers,
        new Set([
          Logger.make((options) => {
            messages.push(...[options.message].flat().map(String));
          }),
        ]),
      ),
    );

    expect(response).toBe("committed");
    expect(messages).toContain("Wakeup reconciliation failed");
  }),
);

layer(
  Layer.mergeAll(
    Layer.succeed(Storage, failingStorage),
    Layer.succeed(AlarmSlot, {
      current: Effect.succeedNone,
      arm: () => Effect.void,
      clear: Effect.void,
    }),
  ),
)((it) => {
  it.effect("an alarm isolates a deadline failure without platform retry", () =>
    Effect.gen(function* () {
      failDeadline = false;
      const context = yield* Effect.context<Storage | AlarmSlot>();
      const scheduler = yield* makeWakeups(context);
      failDeadline = true;
      yield* scheduler.alarm;
      failDeadline = false;
    }),
  );
});
