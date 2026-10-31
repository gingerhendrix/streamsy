import { describe, expect, test } from "vitest";
import { Array, Deferred, Effect, Fiber, Option, Schema } from "effect";
import { TestClock } from "effect/testing";
import {
  Outbox,
  OutboxStore,
  InvalidConfiguration,
  OutboxUnavailable,
  RetryableDeliveryFailure,
  PermanentDeliveryFailure,
  Settlement,
  type OutboxBacking,
} from "../src/index.ts";
import { definition, draft, note, withStore } from "./support.ts";

const retryable = () => Effect.fail(new RetryableDeliveryFailure({ detail: "down" }));

for (const mode of ["memory", "sqlite"] as const) {
  describe(mode, () => {
    test("delivers bounded batches, preserving the durable key and partition", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          yield* Outbox.enqueue(definition, [note("a"), note("b", "other"), note("c")]);
          const deliveries: Outbox.Delivery<typeof definition.payload.Type>[] = [];

          const deliver = (value: Outbox.Delivery<typeof definition.payload.Type>) =>
            Effect.sync(() => {
              deliveries.push(value);
            });

          expect(yield* Outbox.drain(definition, deliver, { limit: 1 })).toMatchObject({
            attempted: 1,
            delivered: 1,
          });
          expect(yield* Outbox.drain(definition, deliver, { partitionId: "other" })).toMatchObject({
            delivered: 1,
          });
          expect(yield* Outbox.drain(definition, deliver)).toMatchObject({ delivered: 1 });
          expect(
            Array.map(deliveries, (value) => [
              value.idempotencyKey,
              value.attempt,
              value.partitionId,
            ]),
          ).toEqual([
            ["a", 1, "main"],
            ["b", 1, "other"],
            ["c", 1, "main"],
          ]);
          expect(yield* store.nextDue({ name: definition.name })).toEqual(Option.none());
        }),
      ));

    test("persists retry deadlines and exhausts the attempt budget", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          yield* Outbox.enqueue(definition, [note("a")]);
          expect(yield* Outbox.drain(definition, retryable)).toMatchObject({ retried: 1 });
          expect((yield* store.list({ name: definition.name }))[0]).toMatchObject({
            attempts: 1,
            nextAttemptAtMs: 100,
          });
          yield* TestClock.setTime(99);
          expect(yield* Outbox.drain(definition, retryable)).toMatchObject({ attempted: 0 });
          yield* TestClock.setTime(100);
          expect(yield* Outbox.drain(definition, retryable)).toMatchObject({ retried: 1 });
          expect(yield* store.nextDue({ name: definition.name })).toEqual(Option.some(300));
          yield* TestClock.setTime(300);
          expect(yield* Outbox.drain(definition, retryable)).toMatchObject({ deadLettered: 1 });
          expect((yield* store.list({ name: definition.name }))[0]).toMatchObject({
            state: "dead",
            attempts: 3,
            deadLetterReason: "attempts-exhausted",
          });
          expect(yield* Outbox.drain(definition, retryable)).toMatchObject({ attempted: 0 });
        }),
      ));

    test("exponential retry deadlines reach and retain the finite cap", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          const capped = Outbox.define({
            ...definition,
            retry: { ...definition.retry, maxAttempts: 7 },
          });

          yield* Outbox.enqueue(capped, [note("a")]);

          for (const [at, next] of [
            [0, 100],
            [100, 300],
            [300, 700],
            [700, 1500],
            [1500, 2500],
            [2500, 3500],
          ] as const) {
            yield* TestClock.setTime(at);
            expect(yield* Outbox.drain(capped, retryable)).toMatchObject({ retried: 1 });
            expect(yield* store.nextDue({ name: capped.name })).toEqual(Option.some(next));
          }
        }),
      ));

    test("permanent refusal and poison payloads dead-letter while healthy work progresses", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          yield* store.enqueue([
            { ...draft("poison"), encodedPayload: '{"wrong":true}' },
            { ...draft("invalid-json"), encodedPayload: "{" },
          ]);
          yield* Outbox.enqueue(definition, [note("permanent"), note("healthy")]);
          const delivered: string[] = [];
          expect(
            yield* Outbox.drain(definition, (value) =>
              value.payload.id === "permanent"
                ? Effect.fail(new PermanentDeliveryFailure({ detail: "gone" }))
                : Effect.sync(() => {
                    delivered.push(value.payload.id);
                  }),
            ),
          ).toMatchObject({ deadLettered: 3, delivered: 1 });
          expect(delivered).toEqual(["healthy"]);
          expect(
            Array.map(
              yield* store.list({ name: definition.name }),
              (entry) => entry.deadLetterReason,
            ),
          ).toEqual(["payload-poison", "payload-poison", "permanent", undefined]);
        }),
      ));

    for (const ordered of [true, false]) {
      test(`ordered=${ordered} includes future heads and respects partition filtering`, () =>
        withStore(mode, (store) =>
          Effect.gen(function* () {
            yield* Outbox.enqueue(definition, [note("first"), note("second", "other")]);
            yield* store.settle(
              1,
              Settlement.Retry({ attempts: 1, nextAttemptAtMs: 500, detail: "retry" }),
            );
            expect(yield* store.nextDue({ name: definition.name, ordered })).toEqual(
              Option.some(ordered ? 500 : 0),
            );
            expect(yield* Outbox.drain(definition, () => Effect.void, { ordered })).toMatchObject({
              delivered: ordered ? 0 : 1,
            });

            if (ordered)
              expect(
                yield* Outbox.drain(definition, () => Effect.void, {
                  ordered,
                  partitionId: "other",
                }),
              ).toMatchObject({ delivered: 1 });
          }),
        ));
    }

    test("ordered retries stop the current pass; terminal failures unblock it", () =>
      withStore(mode, () =>
        Effect.gen(function* () {
          yield* Outbox.enqueue(definition, [note("first"), note("second")]);
          expect(yield* Outbox.drain(definition, retryable, { ordered: true })).toMatchObject({
            attempted: 1,
            retried: 1,
          });
          yield* TestClock.setTime(100);
          expect(
            yield* Outbox.drain(
              definition,
              () => Effect.fail(new PermanentDeliveryFailure({ detail: "gone" })),
              { ordered: true },
            ),
          ).toMatchObject({ attempted: 2, deadLettered: 2 });
        }),
      ));

    for (const kind of ["throw", "defect"] as const) {
      test(`supervises a delivery ${kind} and keeps progressing`, () =>
        withStore(mode, (store) =>
          Effect.gen(function* () {
            yield* Outbox.enqueue(definition, [note("bad"), note("good")]);
            expect(
              yield* Outbox.drain(definition, (value) => {
                if (value.payload.id === "good") return Effect.void;

                if (kind === "throw") throw new Error("client failed");

                return Effect.die(new Error("client failed"));
              }),
            ).toMatchObject({ retried: 1, delivered: 1 });
            expect((yield* store.list({ name: definition.name }))[0]?.lastError).toContain(
              "client failed",
            );
          }),
        ));
    }

    test("interruption leaves the entry unsettled and releases the permit", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          yield* Outbox.enqueue(definition, [note("a")]);
          const entered = yield* Deferred.make<void>();

          const fiber = yield* Outbox.drain(definition, () =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          ).pipe(Effect.forkScoped);

          yield* Deferred.await(entered);
          yield* Fiber.interrupt(fiber);
          expect((yield* store.list({ name: definition.name }))[0]).toMatchObject({
            state: "pending",
            attempts: 0,
          });
          expect(yield* Outbox.drain(definition, () => Effect.void)).toMatchObject({
            delivered: 1,
          });
        }),
      ));

    test("same-store overlapping drains serialize whole-name and partition scopes", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          yield* Outbox.enqueue(definition, [note("a")]);
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const secondStarted = yield* Deferred.make<void>();
          const reads: string[] = [];

          const observed: OutboxBacking = {
            ...store,
            readDue: (...args) =>
              Effect.sync(() => {
                reads.push("read");
              }).pipe(Effect.andThen(store.readDue(...args))),
          };

          const first = yield* Outbox.drain(definition, () =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ).pipe(Effect.provideService(OutboxStore, observed), Effect.forkScoped);

          yield* Deferred.await(entered);

          const second = yield* Deferred.succeed(secondStarted, undefined).pipe(
            Effect.andThen(
              Outbox.drain(definition, () => Effect.die("duplicate"), { partitionId: "main" }),
            ),
            Effect.provideService(OutboxStore, observed),
            Effect.forkScoped,
          );

          yield* Deferred.await(secondStarted);
          yield* Effect.yieldNow;
          expect(reads).toEqual(["read"]);
          yield* Deferred.succeed(release, undefined);
          expect(yield* Fiber.join(first)).toMatchObject({ delivered: 1 });
          expect(yield* Fiber.join(second)).toMatchObject({ attempted: 0 });
          expect(reads).toEqual(["read", "read"]);
        }),
      ));

    test("failed settlement redelivers with the same durable key and attempt", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          yield* Outbox.enqueue(definition, [note("a")]);
          let fail = true;

          const flaky: OutboxBacking = {
            ...store,
            settle: (...args) => {
              if (fail) {
                fail = false;

                return Effect.fail(
                  new OutboxUnavailable({ operation: "settle", detail: "offline" }),
                );
              }

              return store.settle(...args);
            },
          };

          const calls: (readonly [string, number])[] = [];

          const drain = Outbox.drain(definition, (value) =>
            Effect.sync(() => {
              calls.push([value.idempotencyKey, value.attempt]);
            }),
          ).pipe(Effect.provideService(OutboxStore, flaky));

          expect(yield* drain.pipe(Effect.flip)).toBeInstanceOf(OutboxUnavailable);
          expect((yield* store.list({ name: definition.name }))[0]?.attempts).toBe(0);
          expect(yield* drain).toMatchObject({ delivered: 1 });
          expect(calls).toEqual([
            ["a", 1],
            ["a", 1],
          ]);
        }),
      ));

    test("a retry deadline beyond the safe range clamps to it", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Number.MAX_SAFE_INTEGER - 10);
          yield* Outbox.enqueue(definition, [note("a")]);
          expect(yield* Outbox.drain(definition, retryable)).toMatchObject({ retried: 1 });
          expect((yield* store.list({ name: definition.name }))[0]).toMatchObject({
            attempts: 1,
            nextAttemptAtMs: Number.MAX_SAFE_INTEGER,
          });
        }),
      ));

    test("accepts safe attempt budgets beyond 32-bit SQL integer limits", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          const largeBudget = Outbox.define({
            ...definition,
            retry: { ...definition.retry, maxAttempts: Number.MAX_SAFE_INTEGER },
          });

          yield* Outbox.enqueue(largeBudget, [note("a")]);
          yield* store.settle(
            1,
            Settlement.Retry({
              attempts: Number.MAX_SAFE_INTEGER - 1,
              nextAttemptAtMs: 0,
              detail: "retry",
            }),
          );
          expect(yield* Outbox.drain(largeBudget, retryable)).toMatchObject({ deadLettered: 1 });
          expect((yield* store.list({ name: definition.name }))[0]).toMatchObject({
            state: "dead",
            attempts: Number.MAX_SAFE_INTEGER,
            deadLetterReason: "attempts-exhausted",
          });
        }),
      ));

    test("zero delay stays finite after an overflowing exponent; exponential retries cap", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          const zero = Outbox.define({
            ...definition,
            retry: { maxAttempts: 10_000, initialDelayMs: 0, factor: 10, maxDelayMs: 100 },
          });

          yield* Outbox.enqueue(zero, [note("a")]);
          yield* store.settle(
            1,
            Settlement.Retry({ attempts: 1_000, nextAttemptAtMs: 0, detail: "retry" }),
          );
          expect(yield* Outbox.drain(zero, retryable)).toMatchObject({ retried: 1 });
          expect(yield* store.nextDue({ name: definition.name })).toEqual(Option.some(0));
          const capped = Outbox.define({ ...zero, retry: { ...zero.retry, initialDelayMs: 1 } });
          expect(yield* Outbox.drain(capped, retryable)).toMatchObject({ retried: 1 });
          expect(yield* store.nextDue({ name: definition.name })).toEqual(Option.some(100));
        }),
      ));

    test("rejects invalid drain limits without reading or settling", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          yield* Outbox.enqueue(definition, [note("a")]);

          for (const limit of [0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])
            expect(
              yield* Outbox.drain(definition, () => Effect.void, { limit }).pipe(Effect.flip),
            ).toBeInstanceOf(InvalidConfiguration);
          expect((yield* store.list({ name: definition.name }))[0]?.attempts).toBe(0);
        }),
      ));

    test("Schema encode failure validates the complete batch before insertion", () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          const constrained = Outbox.define({
            ...definition,
            payload: Schema.Struct({
              id: Schema.String.pipe(Schema.check(Schema.isMinLength(2))),
              lane: Schema.String,
            }),
          });

          expect(
            yield* Outbox.enqueue(constrained, [note("valid"), note("x")]).pipe(Effect.flip),
          ).toMatchObject({ _tag: "PayloadEncodingFailure" });
          expect(yield* store.list({ name: definition.name })).toEqual([]);

          const throwing = Outbox.define({
            ...definition,
            idempotencyKey: () => {
              throw new Error("key failed");
            },
          });

          expect(yield* Outbox.enqueue(throwing, [note("valid")]).pipe(Effect.flip)).toMatchObject({
            _tag: "PayloadEncodingFailure",
          });
          expect(yield* store.list({ name: definition.name })).toEqual([]);
        }),
      ));
  });
}
