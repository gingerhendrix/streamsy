/**
 * The delivery runtime's contract, driven through the outbox a host would use.
 *
 * These are the properties that make an action sink safe to point at the real
 * world: one effect per idempotency key, a bounded retry budget, a terminus
 * when the budget runs out, and a failing delivery that cannot stop the healthy
 * ones behind it. Backoff is checked against a `TestClock` rather than a sleep,
 * so the pacing is asserted instead of waited for.
 */
import { describe, expect, test } from "bun:test";
import { Effect, Layer, ManagedRuntime } from "effect";
import { TestClock } from "effect/testing";
import { defineActionSink } from "./contract.ts";
import { ActionSinkDeliveryFailure, type OutboxUnavailable } from "./errors.ts";
import {
  makeMemoryOutboxBacking,
  OutboxStore,
  outboxStoreLayer,
  type OutboxBacking,
  type OutboxEntry,
} from "./outbox.ts";
import {
  drain,
  draftsFor,
  actionSinkHandler,
  type DrainOptions,
  type ActionSinkDelivery,
  type ActionSinkHandler,
} from "./runtime.ts";

interface Notification {
  readonly id: string;
  readonly workspaceId: string;
}

type NotificationRelation = { readonly key: "id" };

const sink = defineActionSink<Notification, NotificationRelation>({
  name: "test.notifications",
  from: { key: "id" },
  handler: { name: "test.notify", version: 1 },
  payload: {
    encode: (value) => JSON.stringify(value),
    decode: (value) => {
      if (!(value instanceof Object) || !("id" in value)) throw new TypeError("not a notification");
      // SAFETY: the guard above is this codec's parse boundary for a stored payload.
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion
      return value as Notification;
    },
  },
  idempotencyKey: (value) => value.id,
  partitionBy: (value) => value.workspaceId,
  delivery: { maxAttempts: 3, initialBackoffMs: 100, backoffFactor: 2, maxBackoffMs: 1_000 },
});

const note = (id: string, workspaceId = "main"): Notification => ({ id, workspaceId });

interface Recorder {
  readonly deliveries: ActionSinkDelivery<Notification>[];
  readonly accepted: Set<string>;
}

const recorder = (): Recorder => ({ deliveries: [], accepted: new Set() });
const runOutbox = <A>(effect: Effect.Effect<A, OutboxUnavailable>) =>
  Effect.runSync(Effect.orDie(effect));

/**
 * A handler that behaves the way the contract asks a real one to behave: it
 * absorbs a key it has already accepted, and it refuses whatever the test tells
 * it to refuse.
 */
const handlerFor = (
  log: Recorder,
  options: { readonly refuse?: (id: string) => "retryable" | "permanent" | undefined } = {},
): ActionSinkHandler<Notification> =>
  actionSinkHandler(sink, (delivery, refuse) =>
    Effect.gen(function* () {
      log.deliveries.push(delivery);
      const refusal = options.refuse?.(delivery.payload.id);
      if (refusal === "retryable") return yield* refuse.retryable("the notifier is down");
      if (refusal === "permanent") return yield* refuse.permanent("the recipient does not exist");
      // The idempotency key is the handler's half of the at-least-once contract.
      log.accepted.add(delivery.idempotencyKey);
      return yield* Effect.void;
    }),
  );

const enqueue = (outbox: OutboxBacking, notifications: readonly Notification[], atMs = 0) =>
  runOutbox(outbox.enqueue(draftsFor(sink, notifications, atMs)));

const drainAt = (
  handler: ActionSinkHandler<Notification>,
  atMs: number,
  options: DrainOptions = {},
) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(atMs);
    return yield* drain(sink, handler, options);
  });

const runWithOutbox = <A>(
  outbox: OutboxBacking,
  effect: Effect.Effect<A, OutboxUnavailable, OutboxStore>,
): Promise<A> => {
  const runtime = ManagedRuntime.make(Layer.merge(outboxStoreLayer(outbox), TestClock.layer()));
  return runtime.runPromise(effect).finally(() => runtime.dispose());
};

const states = (outbox: OutboxBacking): readonly Pick<OutboxEntry, "idempotencyKey" | "state">[] =>
  runOutbox(outbox.list(sink.name, undefined)).map((entry) => ({
    idempotencyKey: entry.idempotencyKey,
    state: entry.state,
  }));

describe("draining an action sink", () => {
  test("delivers each enqueued payload once and marks it delivered", () => {
    const outbox = makeMemoryOutboxBacking();
    const log = recorder();
    enqueue(outbox, [note("a"), note("b")]);

    return runWithOutbox(
      outbox,
      Effect.gen(function* () {
        const report = yield* drainAt(handlerFor(log), 0);
        expect(report).toEqual({
          sink: sink.name,
          claimed: 2,
          delivered: 2,
          retried: 0,
          deadLettered: 0,
        });
        expect(log.deliveries.map((delivery) => delivery.payload.id)).toEqual(["a", "b"]);
        expect(states(outbox)).toEqual([
          { idempotencyKey: "a", state: "delivered" },
          { idempotencyKey: "b", state: "delivered" },
        ]);

        // A second pass has nothing due: a delivered effect is never re-delivered.
        expect(yield* drainAt(handlerFor(log), 10_000)).toMatchObject({ claimed: 0 });
      }),
    );
  });

  test("a repeated enqueue of the same fact produces no second effect", () => {
    const outbox = makeMemoryOutboxBacking();
    const log = recorder();
    expect(enqueue(outbox, [note("a")])).toEqual({ enqueued: 1, absorbed: 0 });
    expect(enqueue(outbox, [note("a")])).toEqual({ enqueued: 0, absorbed: 1 });

    return runWithOutbox(
      outbox,
      Effect.gen(function* () {
        yield* drainAt(handlerFor(log), 0);
        expect(log.deliveries).toHaveLength(1);
      }),
    );
  });

  test("a redelivered entry is absorbed by the handler's idempotency key", () => {
    const outbox = makeMemoryOutboxBacking();
    const log = recorder();
    enqueue(outbox, [note("a")]);

    // A crash between a successful handler call and the outbox write leaves the
    // entry pending, so the next pass delivers it again. The handler sees the
    // same key and performs no second effect.
    return runWithOutbox(
      outbox,
      Effect.gen(function* () {
        yield* drainAt(handlerFor(log), 0);
        yield* outbox.reschedule(1, 0, 0, "simulated crash before the outbox write");
        yield* drainAt(handlerFor(log), 0);

        expect(log.deliveries).toHaveLength(2);
        expect(log.accepted.size).toBe(1);
      }),
    );
  });

  test("a failing handler retries on the declared backoff and then dead-letters", () => {
    const outbox = makeMemoryOutboxBacking();
    const log = recorder();
    enqueue(outbox, [note("a")]);
    const handler = handlerFor(log, { refuse: () => "retryable" });

    return runWithOutbox(
      outbox,
      Effect.gen(function* () {
        expect(yield* drainAt(handler, 0)).toMatchObject({
          delivered: 0,
          retried: 1,
          deadLettered: 0,
        });
        const entries = yield* outbox.list(sink.name, undefined);
        expect(entries[0]).toMatchObject({
          state: "pending",
          attempts: 1,
          nextAttemptAtMs: 100,
          lastError: "the notifier is down",
        });

        // Before the backoff elapses there is nothing to claim, so the retry is
        // genuinely paced rather than spun.
        expect(yield* drainAt(handler, 99)).toMatchObject({ claimed: 0 });

        expect(yield* drainAt(handler, 100)).toMatchObject({ retried: 1 });
        const retried = yield* outbox.list(sink.name, undefined);
        expect(retried[0]).toMatchObject({
          attempts: 2,
          nextAttemptAtMs: 300,
        });

        expect(yield* drainAt(handler, 300)).toMatchObject({
          delivered: 0,
          retried: 0,
          deadLettered: 1,
        });
        const dead = yield* outbox.list(sink.name, undefined);
        expect(dead[0]).toMatchObject({
          state: "dead",
          attempts: 3,
          deadLetterReason: "attempts-exhausted",
        });
        expect(log.deliveries).toHaveLength(3);

        // A dead letter is a terminus, not a pause.
        expect(yield* drainAt(handler, 1_000_000)).toMatchObject({ claimed: 0 });
      }),
    );
  });

  test("a permanent refusal spends no further attempts", () => {
    const outbox = makeMemoryOutboxBacking();
    const log = recorder();
    enqueue(outbox, [note("a")]);

    return runWithOutbox(
      outbox,
      Effect.gen(function* () {
        expect(yield* drainAt(handlerFor(log, { refuse: () => "permanent" }), 0)).toMatchObject({
          deadLettered: 1,
          retried: 0,
        });
        const entries = yield* outbox.list(sink.name, undefined);
        expect(entries[0]).toMatchObject({
          state: "dead",
          attempts: 1,
          deadLetterReason: "permanent",
        });
      }),
    );
  });

  test("a failing delivery does not block the healthy ones behind it", () => {
    const outbox = makeMemoryOutboxBacking();
    const log = recorder();
    enqueue(outbox, [note("bad"), note("good"), note("also-good")]);

    return runWithOutbox(
      outbox,
      Effect.gen(function* () {
        const report = yield* drainAt(
          handlerFor(log, { refuse: (id) => (id === "bad" ? "retryable" : undefined) }),
          0,
        );
        expect(report).toMatchObject({ claimed: 3, delivered: 2, retried: 1 });
        expect(states(outbox)).toEqual([
          { idempotencyKey: "bad", state: "pending" },
          { idempotencyKey: "good", state: "delivered" },
          { idempotencyKey: "also-good", state: "delivered" },
        ]);
        // Serialized, in enqueue order — never concurrent, never reordered.
        expect(log.deliveries.map((delivery) => delivery.payload.id)).toEqual([
          "bad",
          "good",
          "also-good",
        ]);
      }),
    );
  });

  test("a thrown handler is a failed delivery, not a failed pass", () => {
    const outbox = makeMemoryOutboxBacking();
    enqueue(outbox, [note("a"), note("b")]);
    const thrower = actionSinkHandler<Notification, NotificationRelation>(sink, (delivery) =>
      delivery.payload.id === "a"
        ? Effect.sync(() => {
            throw new TypeError("the notifier client blew up");
          })
        : Effect.void,
    );

    return runWithOutbox(
      outbox,
      Effect.gen(function* () {
        expect(yield* drainAt(thrower, 0)).toMatchObject({ delivered: 1, retried: 1 });
        const entries = yield* outbox.list(sink.name, undefined);
        expect(entries[0]?.lastError).toContain("the notifier client blew up");
      }),
    );
  });

  test("a stored payload the codec now rejects dead-letters alone", () => {
    const outbox = makeMemoryOutboxBacking();
    const log = recorder();
    runOutbox(
      outbox.enqueue([
        {
          sink: sink.name,
          partitionId: "main",
          idempotencyKey: "poison",
          payload: JSON.stringify({ nothing: true }),
          enqueuedAtMs: 0,
        },
        ...draftsFor(sink, [note("healthy")], 0),
      ]),
    );

    return runWithOutbox(
      outbox,
      Effect.gen(function* () {
        expect(yield* drainAt(handlerFor(log), 0)).toMatchObject({
          deadLettered: 1,
          delivered: 1,
        });
        const entries = yield* outbox.list(sink.name, undefined);
        expect(entries[0]).toMatchObject({
          state: "dead",
          deadLetterReason: "payload-poison",
        });
        expect(log.deliveries.map((delivery) => delivery.payload.id)).toEqual(["healthy"]);
      }),
    );
  });

  test("draining one lane leaves the other lanes untouched", () => {
    const outbox = makeMemoryOutboxBacking();
    const log = recorder();
    enqueue(outbox, [note("left", "alpha"), note("right", "beta")]);

    return runWithOutbox(
      outbox,
      Effect.gen(function* () {
        expect(yield* drainAt(handlerFor(log), 0, { partitionId: "beta" })).toMatchObject({
          claimed: 1,
          delivered: 1,
        });
        expect(states(outbox)).toEqual([
          { idempotencyKey: "left", state: "pending" },
          { idempotencyKey: "right", state: "delivered" },
        ]);
      }),
    );
  });
});

describe("a delivery refusal", () => {
  test("names the sink's declared handler", () => {
    const failure = ActionSinkDeliveryFailure.retryable(sink.handler.name, "a", "down");
    const { _tag: tag } = failure;
    expect(tag).toBe("ActionSinkDeliveryFailure");
    expect(failure.handler).toBe("test.notify");
    expect(failure.retryable).toBe(true);
  });
});
