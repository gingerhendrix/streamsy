/**
 * Memory and SQLite outbox parity.
 *
 * Both backings run the same conformance body, because a declaration that
 * behaves one way on the in-memory host and another way on the durable one is
 * not a contract. Absorbing a repeated enqueue is checked here rather than in
 * the delivery runtime: the store is the only layer that can decide it without
 * a race, so this is where the idempotency guarantee actually lives.
 */
import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { makeMemoryOutboxBacking, type OutboxBacking, type OutboxDraft } from "./outbox.ts";
import { createSqliteOutboxBacking } from "./sqlite.ts";

const draft = (key: string, partitionId = "main", enqueuedAtMs = 1_000): OutboxDraft => ({
  sink: "test.sink",
  partitionId,
  idempotencyKey: key,
  payload: JSON.stringify({ key }),
  enqueuedAtMs,
});

const backings: readonly (readonly [string, () => OutboxBacking])[] = [
  ["memory", () => makeMemoryOutboxBacking()],
  ["sqlite", () => createSqliteOutboxBacking(new Database(":memory:", { create: true }))],
];

for (const [name, open] of backings) {
  describe(`the ${name} outbox`, () => {
    test("absorbs a repeated idempotency key instead of enqueuing twice", () => {
      const outbox = open();
      expect(outbox.enqueue([draft("a"), draft("b")])).toEqual({ enqueued: 2, absorbed: 0 });
      expect(outbox.enqueue([draft("a"), draft("c")])).toEqual({ enqueued: 1, absorbed: 1 });
      expect(outbox.list("test.sink", undefined).map((entry) => entry.idempotencyKey)).toEqual([
        "a",
        "b",
        "c",
      ]);
    });

    test("claims pending work in enqueue order and only when it is due", () => {
      const outbox = open();
      outbox.enqueue([draft("a"), draft("b"), draft("c")]);
      expect(outbox.claimDue("test.sink", undefined, 1_000, 2).map((entry) => entry.id)).toEqual([
        1, 2,
      ]);
      outbox.reschedule(1, 1, 5_000, "not yet");
      expect(
        outbox.claimDue("test.sink", undefined, 1_000, 10).map((entry) => entry.idempotencyKey),
      ).toEqual(["b", "c"]);
      expect(
        outbox.claimDue("test.sink", undefined, 5_000, 10).map((entry) => entry.idempotencyKey),
      ).toEqual(["a", "b", "c"]);
    });

    test("keeps lanes independent", () => {
      const outbox = open();
      outbox.enqueue([draft("a", "left"), draft("b", "right")]);
      expect(
        outbox.claimDue("test.sink", "right", 1_000, 10).map((entry) => entry.idempotencyKey),
      ).toEqual(["b"]);
      expect(outbox.list("test.sink", "left").map((entry) => entry.idempotencyKey)).toEqual(["a"]);
    });

    test("settled entries leave the pending set and record why", () => {
      const outbox = open();
      outbox.enqueue([draft("a"), draft("b")]);
      outbox.markDelivered(1, 1, 2_000);
      outbox.deadLetter(2, 3, "attempts-exhausted", "handler down", 2_100);
      expect(outbox.claimDue("test.sink", undefined, 9_000, 10)).toHaveLength(0);
      const [delivered, dead] = outbox.list("test.sink", undefined);
      expect(delivered).toMatchObject({ state: "delivered", attempts: 1, settledAtMs: 2_000 });
      expect(dead).toMatchObject({
        state: "dead",
        attempts: 3,
        deadLetterReason: "attempts-exhausted",
        lastError: "handler down",
      });
    });
  });
}

describe("the SQLite outbox", () => {
  test("survives a reopen of the same database file", () => {
    const database = new Database(":memory:", { create: true });
    const outbox = createSqliteOutboxBacking(database);
    outbox.enqueue([draft("a")]);
    outbox.reschedule(1, 1, 4_000, "first failure");

    // A second backing over the same database is what a restart looks like from
    // the outbox's side: the schema is re-applied and the pending work is still
    // there, with its attempt count and its next attempt instant intact.
    const reopened = createSqliteOutboxBacking(database);
    expect(reopened.claimDue("test.sink", undefined, 3_999, 10)).toHaveLength(0);
    expect(reopened.claimDue("test.sink", undefined, 4_000, 10)[0]).toMatchObject({
      attempts: 1,
      idempotencyKey: "a",
      lastError: "first failure",
    });
  });

  test("enqueue joins the caller's transaction, so a rollback enqueues nothing", () => {
    const database = new Database(":memory:", { create: true });
    const outbox = createSqliteOutboxBacking(database);
    const commit = database.transaction((key: string) => {
      outbox.enqueue([draft(key)]);
      throw new Error("the caller's own write failed");
    });
    expect(() => commit("a")).toThrow("the caller's own write failed");
    expect(outbox.list("test.sink", undefined)).toHaveLength(0);
  });
});
