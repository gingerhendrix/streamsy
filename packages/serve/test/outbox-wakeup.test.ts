/* oxlint-disable effecttsgo/any-unknown-in-error-context -- Wakeup.run accepts arbitrary application failures at the host supervision boundary. */
/* oxlint-disable effecttsgo/strict-effect-provide -- Test entry point owns the complete TestClock layer. */
import { expect, expectTypeOf, test } from "vitest";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { StorageFault } from "@streamsy/core";
import {
  Outbox,
  OutboxStore,
  InvalidConfiguration,
  InvalidStoredRecord,
  OutboxUnavailable,
  Settlement,
} from "@streamsy/outbox";
import { layerMemory } from "@streamsy/outbox/memory";
import { OutboxWakeup } from "@streamsy/serve/alchemy";
import { TestClock } from "effect/testing";

const definition = Outbox.define({
  name: "a/b",
  payload: Schema.String,
  idempotencyKey: (value) => value,
  retry: { maxAttempts: 3, initialDelayMs: 100, factor: 2, maxDelayMs: 1000 },
});

test("wakeup names encode the name and partition independently, including an empty partition", () => {
  const parent = Outbox.define({ ...definition, name: "a" });
  expect([
    OutboxWakeup.make(definition, () => Effect.void).name,
    OutboxWakeup.make(parent, () => Effect.void, { partitionId: "b" }).name,
    OutboxWakeup.make(parent, () => Effect.void, { partitionId: "b/c" }).name,
    OutboxWakeup.make(parent, () => Effect.void, { partitionId: "" }).name,
  ]).toEqual([
    "streamsy/outbox/a%2Fb",
    "streamsy/outbox/a/b",
    "streamsy/outbox/a/b%2Fc",
    "streamsy/outbox/a/",
  ]);
});

test("next uses the selected ordered scope and run reports the default bounded pass", () => {
  const wakeup = OutboxWakeup.make(definition, () => Effect.void);

  const ordered = OutboxWakeup.make(definition, () => Effect.void, {
    partitionId: "",
    ordered: true,
  });

  return Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* OutboxStore;
      yield* Outbox.enqueue(
        definition,
        Array.from({ length: 101 }, (_, i) => String(i)),
      );
      yield* store.settle(
        1,
        Settlement.Retry({ attempts: 1, nextAttemptAtMs: 1000, detail: "later" }),
      );
      expect(yield* wakeup.next).toEqual(Option.some(0));
      expect(yield* ordered.next).toEqual(Option.some(1000));
      expect(yield* ordered.run).toEqual({ more: false });
      expect(yield* wakeup.run).toEqual({ more: true });
      expect(yield* wakeup.next).toEqual(Option.some(1000));
      yield* TestClock.setTime(1000);
      expect(yield* wakeup.run).toEqual({ more: false });
      expect(yield* wakeup.next).toEqual(Option.none());
    }).pipe(Effect.provide(Layer.merge(layerMemory, TestClock.layer()))),
  );
});

for (const fault of [
  new OutboxUnavailable({ operation: "nextDue", detail: "offline" }),
  new InvalidStoredRecord({ operation: "nextDue", detail: "corrupt" }),
]) {
  test(`next maps ${fault._tag} to a retryable host storage fault`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const store = yield* OutboxStore;

        return yield* OutboxWakeup.make(definition, () => Effect.void).next.pipe(
          Effect.provideService(OutboxStore, { ...store, nextDue: () => Effect.fail(fault) }),
          Effect.flip,
        );
      }).pipe(
        Effect.provide(layerMemory),
        Effect.tap((error) =>
          Effect.sync(() => {
            expect(error).toBeInstanceOf(StorageFault);
            expect(error).toMatchObject({
              operation: "outbox.nextDue",
              message: fault.detail,
              retryable: true,
            });
          }),
        ),
      ),
    ));
}

test("run fails with InvalidConfiguration for an invalid limit, not a storage fault", () =>
  Effect.runPromise(
    OutboxWakeup.make(definition, () => Effect.void, { limit: 0 }).run.pipe(
      Effect.provide(layerMemory),
      Effect.flip,
      Effect.tap((error) =>
        Effect.sync(() => {
          expect(error).toBeInstanceOf(InvalidConfiguration);
        }),
      ),
    ),
  ));

class Sender extends Context.Service<Sender, { readonly send: Effect.Effect<void> }>()(
  "OutboxWakeupTest/Sender",
) {}

const requiringSender = OutboxWakeup.make(definition, () => Sender.use((sender) => sender.send));

expectTypeOf<Effect.Services<typeof requiringSender.run>>().toEqualTypeOf<OutboxStore | Sender>();
