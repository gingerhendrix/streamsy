import { Effect, Option } from "effect";
import { expect } from "vitest";
import {
  Outbox,
  OutboxStore,
  type PruneOptions,
  Settlement,
  OutboxUnavailable,
  type OutboxBacking,
  type OutboxDraft,
} from "../src/index.ts";

const name = "additions";
const draft = (key: string, partitionId = "main"): OutboxDraft => ({
  name,
  partitionId,
  idempotencyKey: key,
  encodedPayload: JSON.stringify(key),
  enqueuedAtMs: 100,
});

export const dueTimes = (store: OutboxBacking) =>
  Effect.gen(function* () {
    yield* store.enqueue([
      { ...draft("future"), notBeforeMs: 1000 },
      { ...draft("past"), notBeforeMs: 50 },
      draft("now"),
    ]);
    expect(yield* store.nextDue({ name })).toEqual(Option.some(100));
    expect(yield* store.nextDue({ name, ordered: true })).toEqual(Option.some(1000));
    expect((yield* store.readDue({ name }, 999, 10)).map((row) => row.idempotencyKey)).toEqual([
      "past",
      "now",
    ]);
    expect((yield* store.readDue({ name, ordered: true }, 999, 10))[0]?.idempotencyKey).toBe(
      "future",
    );
    expect(yield* store.enqueue([{ ...draft("future", "changed"), notBeforeMs: 2000 }])).toEqual({
      enqueued: 0,
      absorbed: 1,
    });
    expect((yield* store.list({ name }))[0]).toMatchObject({
      nextAttemptAtMs: 1000,
      partitionId: "main",
    });
    expect(yield* store.readDue({ name }, 1000, 10)).toHaveLength(3);
  });

export const guardedSettlement = (store: OutboxBacking) =>
  Effect.gen(function* () {
    const delivered = Settlement.Delivered({ attempts: 1, atMs: 200 });
    expect(yield* store.settle(999, delivered).pipe(Effect.flip)).toMatchObject({
      _tag: "OutboxUnavailable",
      operation: "settle",
    });
    yield* store.enqueue([draft("delivered"), draft("dead")]);
    const entries = yield* store.list({ name });
    yield* store.settle(entries[0]!.id, delivered);
    yield* store.settle(
      entries[1]!.id,
      Settlement.Dead({ attempts: 1, reason: "permanent", detail: "gone", atMs: 200 }),
    );
    const before = yield* store.list({ name });
    for (const entry of before) {
      for (const settlement of [
        delivered,
        Settlement.Retry({ attempts: 2, nextAttemptAtMs: 300, detail: "retry" }),
        Settlement.Dead({ attempts: 2, reason: "permanent", detail: "overwrite", atMs: 300 }),
      ])
        expect(yield* store.settle(entry.id, settlement).pipe(Effect.flip)).toBeInstanceOf(
          OutboxUnavailable,
        );
    }
    expect(yield* store.list({ name })).toEqual(before);
  });

export const filteredReads = (store: OutboxBacking) =>
  Effect.gen(function* () {
    yield* store.enqueue([
      draft("a"),
      draft("b", "other"),
      { ...draft("c"), notBeforeMs: 300 },
      { ...draft("alien"), name: "other" },
    ]);
    const initial = yield* store.list({ name });
    yield* store.settle(
      initial[1]!.id,
      Settlement.Dead({ attempts: 1, reason: "permanent", detail: "gone", atMs: 200 }),
    );
    expect(yield* store.list({ name, state: "delivered" })).toEqual([]);
    expect(
      (yield* store.list({ name, state: "pending", afterId: 0, limit: 1 })).map(
        (row) => row.idempotencyKey,
      ),
    ).toEqual(["a"]);
    expect(
      (yield* store.list({ name, state: "pending", afterId: initial[0]!.id, limit: 1 })).map(
        (row) => row.idempotencyKey,
      ),
    ).toEqual(["c"]);
    expect(
      (yield* store.list({
        name,
        state: "dead",
        partitionId: "other",
        afterId: initial[0]!.id,
      })).map((row) => row.idempotencyKey),
    ).toEqual(["b"]);
    expect(yield* store.count({ name })).toBe(3);
    expect(yield* store.count({ name, state: "pending" })).toBe(2);
    expect(yield* store.count({ name, state: "pending", dueBeforeMs: 100 })).toBe(1);
    expect(yield* store.count({ name, state: "pending", dueBeforeMs: 99 })).toBe(0);
    expect(yield* store.count({ name, state: "pending", dueBeforeMs: 300 })).toBe(2);
    expect(
      yield* store.count({ name, state: "dead", partitionId: "other", dueBeforeMs: 100 }),
    ).toBe(1);
    expect(yield* store.count({ name, partitionId: "missing" })).toBe(0);
    for (const limit of [0, -1, 1.5, Infinity, NaN])
      expect(yield* store.list({ name, limit }).pipe(Effect.flip)).toBeInstanceOf(
        OutboxUnavailable,
      );
    expect(yield* store.list({ name, afterId: -1 }).pipe(Effect.flip)).toBeInstanceOf(
      OutboxUnavailable,
    );
    expect(yield* store.count({ name, dueBeforeMs: Infinity }).pipe(Effect.flip)).toBeInstanceOf(
      OutboxUnavailable,
    );
  });

export const boundedPruning = (store: OutboxBacking) =>
  Effect.gen(function* () {
    yield* store.enqueue([
      draft("a"),
      draft("b"),
      draft("c"),
      draft("dead"),
      draft("boundary"),
      draft("pending"),
      { ...draft("alien"), name: "other" },
    ]);
    const entries = yield* store.list({ name });
    for (const entry of entries.slice(0, 3))
      yield* store.settle(entry.id, Settlement.Delivered({ attempts: 1, atMs: 100 }));
    yield* store.settle(
      entries[3]!.id,
      Settlement.Dead({ attempts: 1, reason: "permanent", detail: "gone", atMs: 100 }),
    );
    yield* store.settle(entries[4]!.id, Settlement.Delivered({ attempts: 1, atMs: 200 }));
    const alien = (yield* store.list({ name: "other" }))[0]!;
    yield* store.settle(alien.id, Settlement.Delivered({ attempts: 1, atMs: 100 }));
    const options: PruneOptions = { name, state: "delivered", settledBeforeMs: 200, limit: 2 };
    for (const invalid of [
      { ...options, limit: 0 },
      { ...options, limit: 1.5 },
      { ...options, limit: Infinity },
      { ...options, settledBeforeMs: NaN },
      { ...options, state: "pending" as PruneOptions["state"] },
    ])
      expect(yield* store.prune(invalid).pipe(Effect.flip)).toBeInstanceOf(OutboxUnavailable);
    expect(yield* Outbox.prune(options).pipe(Effect.provideService(OutboxStore, store))).toBe(2);
    expect((yield* store.list({ name })).map((row) => row.idempotencyKey)).toEqual([
      "c",
      "dead",
      "boundary",
      "pending",
    ]);
    expect(yield* store.prune(options)).toBe(1);
    expect(yield* store.prune(options)).toBe(0);
    expect(yield* store.prune({ ...options, state: "dead" })).toBe(1);
    expect(yield* store.count({ name: "other" })).toBe(1);
    expect(yield* store.count({ name, state: "pending" })).toBe(1);
    expect(yield* store.enqueue([draft("a")])).toEqual({ enqueued: 1, absorbed: 0 });
    expect((yield* store.list({ name })).at(-1)?.id).toBeGreaterThan(alien.id);
  });

export const deadRequeue = (store: OutboxBacking) =>
  Effect.gen(function* () {
    yield* store.enqueue([draft("dead"), draft("delivered"), draft("pending")]);
    const initial = yield* store.list({ name });
    const dead = initial[0]!;
    yield* store.settle(
      dead.id,
      Settlement.Dead({ attempts: 3, reason: "attempts-exhausted", detail: "failed", atMs: 200 }),
    );
    yield* store.settle(initial[1]!.id, Settlement.Delivered({ attempts: 1, atMs: 200 }));
    for (const id of [999, initial[1]!.id, initial[2]!.id])
      expect(yield* store.requeue(id, { atMs: 500 }).pipe(Effect.flip)).toMatchObject({
        _tag: "OutboxUnavailable",
        operation: "requeue",
      });
    for (const atMs of [Infinity, NaN, 1.5, Number.MAX_SAFE_INTEGER + 1])
      expect(yield* store.requeue(dead.id, { atMs }).pipe(Effect.flip)).toBeInstanceOf(
        OutboxUnavailable,
      );
    yield* Outbox.requeue(dead.id, { atMs: 500 }).pipe(Effect.provideService(OutboxStore, store));
    expect((yield* store.list({ name }))[0]).toEqual({
      ...dead,
      state: "pending",
      attempts: 0,
      nextAttemptAtMs: 500,
      lastError: undefined,
      deadLetterReason: undefined,
      settledAtMs: undefined,
    });
    expect((yield* store.readDue({ name }, 499, 10)).map((row) => row.idempotencyKey)).toEqual([
      "pending",
    ]);
    expect((yield* store.readDue({ name }, 500, 10)).map((row) => row.idempotencyKey)).toEqual([
      "dead",
      "pending",
    ]);
    expect(yield* store.enqueue([draft("dead", "changed")])).toEqual({ enqueued: 0, absorbed: 1 });
    expect(yield* store.requeue(dead.id, { atMs: 0 }).pipe(Effect.flip)).toBeInstanceOf(
      OutboxUnavailable,
    );
    yield* store.settle(dead.id, Settlement.Delivered({ attempts: 1, atMs: 500 }));
    expect((yield* store.list({ name }))[0]).toMatchObject({
      state: "delivered",
      attempts: 1,
      deadLetterReason: undefined,
      lastError: undefined,
    });
  });
