import { expect, it as baseIt, layer } from "@streamsy/effect-test";
import { Deferred, Effect, Exit, Fiber, Layer, Stream } from "effect";
import { Storage, StorageFault } from "@streamsy/core";
import { Alarm } from "../../src/cloudflare/alarm.ts";
import { withMutationReconciliation } from "../../src/cloudflare/host-program.ts";
import { alarm } from "../../src/cloudflare/host-program.ts";

const failingStorage = Storage.of({
  capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
  record: () => Effect.succeedNone,
  messages: () => Effect.succeed([]),
  producer: () => Effect.succeedNone,
  mutate: () => Effect.die("unused"),
  changes: () => Stream.empty,
  nextExpiry: Effect.fail(
    new StorageFault({
      operation: "test.nextExpiry",
      message: "test failure",
      retryable: true,
    }),
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

layer(
  Layer.mergeAll(
    Layer.succeed(Storage, failingStorage),
    Layer.succeed(Alarm, {
      current: Effect.succeedNone,
      arm: () => Effect.void,
      clear: Effect.void,
    }),
  ),
)((it) => {
  it.effect("an alarm exposes StorageFault for platform retry without an HTTP request", () =>
    Effect.gen(function* () {
      const result = yield* Effect.exit(alarm);
      expect(Exit.isFailure(result)).toBe(true);

      if (Exit.isFailure(result))
        expect(result.cause.reasons).toMatchObject([
          { _tag: "Fail", error: { _tag: "StorageFault", operation: "test.nextExpiry" } },
        ]);
    }),
  );
});
