import { Effect, Option } from "effect";
import { expect } from "vitest";
import {
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
