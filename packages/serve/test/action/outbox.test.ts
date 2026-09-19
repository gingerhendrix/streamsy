/* oxlint-disable effecttsgo/node-builtin-import -- Tests create disposable SQLite files via Bun-compatible Node APIs. */
/**
 * Memory and SQLite outbox parity.
 *
 * Both backings run the same conformance body, because a declaration that
 * behaves one way on the in-memory host and another way on the durable one is
 * not a contract. Absorbing a repeated enqueue is checked here rather than in
 * the delivery runtime: the store is the only layer that can decide it without
 * a race, so this is where the idempotency guarantee actually lives.
 */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Exit, ManagedRuntime } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { OutboxUnavailable } from "@streamsy/serve/action/errors";
import {
  makeMemoryOutboxBacking,
  type OutboxBacking,
  type OutboxDraft,
} from "@streamsy/serve/action/outbox";
import { createSqliteOutboxBacking } from "@streamsy/serve/action/sqlite";

const draft = (key: string, partitionId = "main", enqueuedAtMs = 1_000): OutboxDraft => ({
  sink: "test.sink",
  partitionId,
  idempotencyKey: key,
  payload: JSON.stringify({ key }),
  enqueuedAtMs,
});

type Runner = <A>(
  effect: (
    outbox: OutboxBacking,
  ) => Effect.Effect<A, import("@streamsy/serve/action/errors").OutboxUnavailable>,
) => Promise<A>;

const runMemory: Runner = (effect) => Effect.runPromise(effect(makeMemoryOutboxBacking()));

const runSqlite = <A, E>(
  filename: string,
  effect: (options: {
    readonly outbox: OutboxBacking;
    readonly sql: SqlClient.SqlClient;
  }) => Effect.Effect<A, E>,
) => {
  const runtime = ManagedRuntime.make(SqliteClient.layer({ filename, create: true }));
  const sql = runtime.runSync(SqlClient.SqlClient);
  const outbox = createSqliteOutboxBacking(sql);
  return runtime.runPromise(effect({ outbox, sql })).finally(() => runtime.dispose());
};

const sqliteFilename = () =>
  join(mkdtempSync(join(tmpdir(), "streamsy-effect-outbox-")), "outbox.sqlite");

const cases: readonly (readonly [string, Runner])[] = [
  ["memory", runMemory],
  ["sqlite", (effect) => runSqlite(sqliteFilename(), ({ outbox }) => effect(outbox))],
];

for (const [name, run] of cases) {
  describe(`the ${name} outbox`, () => {
    test("absorbs a repeated idempotency key instead of enqueuing twice", () =>
      run((outbox) =>
        Effect.gen(function* () {
          expect(yield* outbox.enqueue([draft("a"), draft("b")])).toEqual({
            enqueued: 2,
            absorbed: 0,
          });
          expect(yield* outbox.enqueue([draft("a"), draft("c")])).toEqual({
            enqueued: 1,
            absorbed: 1,
          });
          expect(
            (yield* outbox.list("test.sink", undefined)).map((entry) => entry.idempotencyKey),
          ).toEqual(["a", "b", "c"]);
        }),
      ));

    test("keeps NUL-containing sink and key tuples distinct", () =>
      run((outbox) =>
        Effect.gen(function* () {
          const left = { ...draft("c"), sink: "a\u0000b" };
          const right = { ...draft("b\u0000c"), sink: "a" };
          expect(yield* outbox.enqueue([left, right])).toEqual({ enqueued: 2, absorbed: 0 });
          expect(yield* outbox.list(left.sink, undefined)).toHaveLength(1);
          expect(yield* outbox.list(right.sink, undefined)).toHaveLength(1);
        }),
      ));

    test("claims pending work in enqueue order and only when it is due", () =>
      run((outbox) =>
        Effect.gen(function* () {
          yield* outbox.enqueue([draft("a"), draft("b"), draft("c")]);
          expect(
            (yield* outbox.claimDue("test.sink", undefined, 1_000, 2)).map((entry) => entry.id),
          ).toEqual([1, 2]);
          yield* outbox.reschedule(1, 1, 5_000, "not yet");
          expect(
            (yield* outbox.claimDue("test.sink", undefined, 1_000, 10)).map(
              (entry) => entry.idempotencyKey,
            ),
          ).toEqual(["b", "c"]);
          expect(
            (yield* outbox.claimDue("test.sink", undefined, 5_000, 10)).map(
              (entry) => entry.idempotencyKey,
            ),
          ).toEqual(["a", "b", "c"]);
        }),
      ));

    test("keeps lanes independent", () =>
      run((outbox) =>
        Effect.gen(function* () {
          yield* outbox.enqueue([draft("a", "left"), draft("b", "right")]);
          expect(
            (yield* outbox.claimDue("test.sink", "right", 1_000, 10)).map(
              (entry) => entry.idempotencyKey,
            ),
          ).toEqual(["b"]);
          expect(
            (yield* outbox.list("test.sink", "left")).map((entry) => entry.idempotencyKey),
          ).toEqual(["a"]);
        }),
      ));

    test("settled entries leave the pending set and record why", () =>
      run((outbox) =>
        Effect.gen(function* () {
          yield* outbox.enqueue([draft("a"), draft("b")]);
          yield* outbox.markDelivered(1, 1, 2_000);
          yield* outbox.deadLetter(2, 3, "attempts-exhausted", "handler down", 2_100);
          expect(yield* outbox.claimDue("test.sink", undefined, 9_000, 10)).toHaveLength(0);
          const [delivered, dead] = yield* outbox.list("test.sink", undefined);
          expect(delivered).toMatchObject({ state: "delivered", attempts: 1, settledAtMs: 2_000 });
          expect(dead).toMatchObject({
            state: "dead",
            attempts: 3,
            deadLetterReason: "attempts-exhausted",
            lastError: "handler down",
          });
        }),
      ));
  });
}

describe("the SQLite outbox", () => {
  test("survives a reopen of the same database file", () => {
    const filename = sqliteFilename();
    return runSqlite(filename, ({ outbox }) =>
      Effect.gen(function* () {
        yield* outbox.enqueue([draft("a")]);
        yield* outbox.reschedule(1, 1, 4_000, "first failure");
      }),
    ).then(() =>
      runSqlite(filename, ({ outbox }) =>
        Effect.gen(function* () {
          expect(yield* outbox.claimDue("test.sink", undefined, 3_999, 10)).toHaveLength(0);
          expect((yield* outbox.claimDue("test.sink", undefined, 4_000, 10))[0]).toMatchObject({
            attempts: 1,
            idempotencyKey: "a",
            lastError: "first failure",
          });
        }),
      ),
    );
  });

  test("a rolled-back first-operation migration is retried on the same backing", () => {
    const filename = sqliteFilename();
    return runSqlite(filename, ({ outbox, sql }) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* outbox.enqueue([draft("a")]);
              return yield* new OutboxUnavailable({
                operation: "testRollback",
                detail: "the caller's own write failed",
              });
            }),
          ),
        );
        expect(Exit.isFailure(exit)).toBe(true);
        expect(yield* outbox.list("test.sink", undefined)).toHaveLength(0);
        expect(yield* outbox.enqueue([draft("retry")])).toEqual({ enqueued: 1, absorbed: 0 });
        expect((yield* outbox.list("test.sink", undefined))[0]?.idempotencyKey).toBe("retry");
      }),
    );
  });
});
