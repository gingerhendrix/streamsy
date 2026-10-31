/* oxlint-disable effecttsgo/node-builtin-import -- Reopen tests own and clean disposable SQLite files. */
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Array, Effect, Option } from "effect";
import { TestClock } from "effect/testing";
import { expect, test } from "vitest";
import { Outbox, InvalidStoredRecord, OutboxUnavailable, Settlement } from "../src/index.ts";
import { definition, draft, note, withSqlite, withStore } from "./support.ts";

for (const mode of ["memory", "sqlite"] as const) {
  test(`${mode}: reads bounded due entries in enqueue order and filters inspection by partition`, () =>
    withStore(mode, (store) =>
      Effect.gen(function* () {
        expect(yield* store.nextDue({ name: definition.name })).toEqual(Option.none());
        yield* store.enqueue([draft("a", "left"), draft("b", "right", 10), draft("c", "left")]);
        expect(
          Array.map(yield* store.readDue({ name: definition.name }, 10, 2), (row) => row.id),
        ).toEqual([1, 2]);
        yield* store.settle(
          1,
          Settlement.Retry({ attempts: 1, nextAttemptAtMs: 500, detail: "retry" }),
        );
        expect(yield* store.nextDue({ name: definition.name })).toEqual(Option.some(0));
        expect(yield* store.nextDue({ name: definition.name, partitionId: "right" })).toEqual(
          Option.some(10),
        );
        expect(yield* store.nextDue({ name: definition.name, ordered: true })).toEqual(
          Option.some(500),
        );
        expect(
          Array.map(yield* store.readDue({ name: definition.name }, 10, 10), (row) => row.id),
        ).toEqual([2, 3]);
        expect(
          Array.map(
            yield* store.readDue({ name: definition.name, partitionId: "left" }, 500, 10),
            (row) => row.id,
          ),
        ).toEqual([1, 3]);
        expect(
          Array.map(
            yield* store.list({ name: definition.name, partitionId: "right" }),
            (row) => row.idempotencyKey,
          ),
        ).toEqual(["b"]);
        yield* store.settle(1, Settlement.Delivered({ attempts: 2, atMs: 500 }));
        yield* store.settle(
          2,
          Settlement.Dead({ attempts: 1, reason: "permanent", detail: "gone", atMs: 500 }),
        );
        yield* store.settle(3, Settlement.Delivered({ attempts: 1, atMs: 500 }));
        expect(yield* store.readDue({ name: definition.name }, 1000, 10)).toEqual([]);
        expect(yield* store.nextDue({ name: definition.name })).toEqual(Option.none());
      }),
    ));

  for (const state of ["pending", "delivered", "dead"] as const) {
    test(`${mode}: duplicate enqueue retains original payload and partition after ${state}`, () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          yield* Outbox.enqueue(definition, [note("a"), note("b", "other")]);

          if (state === "delivered")
            yield* store.settle(1, Settlement.Delivered({ attempts: 1, atMs: 10 }));

          if (state === "dead")
            yield* store.settle(
              1,
              Settlement.Dead({ attempts: 1, reason: "permanent", detail: "gone", atMs: 10 }),
            );
          expect(yield* Outbox.enqueue(definition, [note("a", "changed"), note("c")])).toEqual({
            enqueued: 1,
            absorbed: 1,
          });
          expect((yield* store.list({ name: definition.name }))[0]).toMatchObject({
            encodedPayload: draft("a").encodedPayload,
            partitionId: "main",
            state,
          });
          expect(
            Array.map(
              yield* store.readDue({ name: definition.name }, 0, 10),
              (entry) => entry.idempotencyKey,
            ),
          ).toEqual(state === "pending" ? ["a", "b", "c"] : ["b", "c"]);
        }),
      ));
  }

  test(`${mode}: duplicate identity is a tuple; distinct names and NUL-containing tuples stay apart`, () =>
    withStore(mode, (store) =>
      Effect.gen(function* () {
        expect(
          yield* store.enqueue([
            { ...draft("c"), name: "a\u0000b" },
            { ...draft("b\u0000c"), name: "a" },
            { ...draft("c"), name: "a" },
            { ...draft("c"), name: "other" },
          ]),
        ).toEqual({ enqueued: 4, absorbed: 0 });
        expect(yield* store.list({ name: "a" })).toHaveLength(2);
      }),
    ));
}

test("SQLite retry deadlines and retained deduplication survive reopening", () => {
  const directory = mkdtempSync(join(import.meta.dirname, ".sqlite-"));
  const filename = join(directory, "outbox.sqlite");

  return withSqlite(
    (_, store) =>
      Effect.gen(function* () {
        yield* store.enqueue([draft("retry"), draft("delivered"), draft("dead")]);
        yield* store.settle(
          1,
          Settlement.Retry({ attempts: 1, nextAttemptAtMs: 500, detail: "later" }),
        );
        yield* store.settle(2, Settlement.Delivered({ attempts: 1, atMs: 10 }));
        yield* store.settle(
          3,
          Settlement.Dead({ attempts: 1, reason: "permanent", detail: "gone", atMs: 10 }),
        );
      }),
    filename,
  )
    .then(() =>
      withSqlite(
        (_, store) =>
          Effect.gen(function* () {
            expect(yield* store.readDue({ name: definition.name }, 499, 10)).toEqual([]);
            expect((yield* store.readDue({ name: definition.name }, 500, 10))[0]).toMatchObject({
              attempts: 1,
              lastError: "later",
            });
            expect(
              yield* store.enqueue([draft("delivered", "changed"), draft("dead", "changed")]),
            ).toEqual({ enqueued: 0, absorbed: 2 });
            expect(
              Array.map(yield* store.list({ name: definition.name }), (entry) => entry.partitionId),
            ).toEqual(["main", "main", "main"]);
          }),
        filename,
      ),
    )
    .finally(() => rmSync(directory, { recursive: true }));
});

test("SQLite causal write and enqueue roll back together", () =>
  withSqlite((sql, store) =>
    Effect.gen(function* () {
      yield* sql`CREATE TABLE causal (id TEXT PRIMARY KEY)`;

      const transaction = sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO causal VALUES ('a')`;
          yield* Outbox.enqueue(definition, [note("a")]);

          return yield* new OutboxUnavailable({ operation: "causal", detail: "rollback" });
        }),
      );

      expect(yield* transaction.pipe(Effect.flip)).toBeInstanceOf(OutboxUnavailable);
      expect(yield* sql`SELECT * FROM causal`).toEqual([]);
      expect(yield* store.list({ name: definition.name })).toEqual([]);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO causal VALUES ('a')`;
          yield* Outbox.enqueue(definition, [note("a")]);
        }),
      );
      expect(yield* sql`SELECT * FROM causal`).toHaveLength(1);
      expect(yield* Outbox.drain(definition, () => Effect.void)).toMatchObject({ delivered: 1 });
    }),
  ));

test("SQLite SQL failures become typed operational failures", () =>
  withSqlite((sql, store) =>
    Effect.gen(function* () {
      yield* store.enqueue([draft("a")]);
      yield* sql`CREATE TRIGGER refuse_settlement BEFORE UPDATE ON streamsy_outbox BEGIN SELECT RAISE(FAIL, 'offline'); END`;
      expect(yield* Outbox.drain(definition, () => Effect.void).pipe(Effect.flip)).toBeInstanceOf(
        OutboxUnavailable,
      );
      expect((yield* store.list({ name: definition.name }))[0]?.attempts).toBe(0);
    }),
  ));

for (const assignment of [
  "state = 'unknown'",
  "attempts = -1",
  "attempts = 1.5",
  "attempts = 9007199254740993",
  "next_attempt_at_ms = 0.5",
  "enqueued_at_ms = 9007199254740993",
  "dead_letter_reason = 'unknown'",
  "state = 'dead'",
  "state = 'delivered'",
  "id = 9007199254740993",
  "settled_at_ms = 1",
]) {
  test(`SQLite reports malformed rows as typed failures: ${assignment}`, () =>
    withSqlite((sql, store) =>
      Effect.gen(function* () {
        yield* store.enqueue([draft("a")]);
        yield* sql.unsafe(`UPDATE streamsy_outbox SET ${assignment}`);
        expect(yield* store.list({ name: definition.name }).pipe(Effect.flip)).toBeInstanceOf(
          InvalidStoredRecord,
        );

        if (
          assignment !== "state = 'unknown'" &&
          assignment !== "state = 'dead'" &&
          assignment !== "state = 'delivered'"
        ) {
          yield* TestClock.setTime(1);
          expect(
            yield* Outbox.drain(definition, () => Effect.die("must not deliver")).pipe(Effect.flip),
          ).toBeInstanceOf(InvalidStoredRecord);
        }
      }),
    ));
}

test("SQLite nextDue validates BIGINT conversion and corrupt timestamps", () =>
  withSqlite((sql, store) =>
    Effect.gen(function* () {
      yield* store.enqueue([draft("a")]);
      yield* sql`UPDATE streamsy_outbox SET next_attempt_at_ms = 9007199254740993`;
      expect(yield* store.nextDue({ name: definition.name }).pipe(Effect.flip)).toBeInstanceOf(
        InvalidStoredRecord,
      );
    }),
  ));

test("the new schema leaves the old outbox table untouched", () =>
  withSqlite((sql, store) =>
    Effect.gen(function* () {
      yield* sql`CREATE TABLE streamsy_effect_outbox (sentinel TEXT)`;
      yield* sql`INSERT INTO streamsy_effect_outbox VALUES ('legacy')`;
      yield* store.enqueue([draft("new")]);
      expect(yield* sql`SELECT * FROM streamsy_effect_outbox`).toEqual([{ sentinel: "legacy" }]);
    }),
  ));
