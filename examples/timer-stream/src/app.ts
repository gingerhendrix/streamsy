/* oxlint-disable effecttsgo/strict-effect-provide -- This executable example composes its complete local host. */
import { Array, Clock, Cron, Effect, Layer, Option, Schedule } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, Streamsy } from "@streamsy/core";
import { Protocol, Streams } from "@streamsy/core/protocol";
import { Commit, Projection } from "@streamsy/projection";
import { Accepted, Fired, Timers } from "./streams.ts";
import { Storage } from "./host.ts";

export const fire = Projection.process({
  id: "fire-timers",
  params: {},
  inputs: { timers: Timers },
  outputs: { fired: Fired },
  accepted: Accepted,
  commit: Commit.transaction,
  process: (batch, unit, { outputs }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      for (const event of batch.timers.items) {
        if (!("type" in event)) {
          if (event.headers.control === "reset") yield* sql`DELETE FROM due_timers`;

          if (event.headers.control === "snapshot-start")
            yield* sql`UPDATE due_timers SET seen = 0`;

          if (event.headers.control === "snapshot-end")
            yield* sql`DELETE FROM due_timers WHERE seen = 0`;
        } else if ("value" in event) {
          const row = event.value;
          yield* sql`INSERT INTO due_timers (id, due_at, cron, seen) VALUES (${row.id}, ${row.dueAt}, ${row.cron}, 1)
          ON CONFLICT (id) DO UPDATE SET due_at = excluded.due_at, cron = excluded.cron, seen = 1`;
        } else {
          yield* sql`DELETE FROM due_timers WHERE id = ${event.key}`;
        }
      }

      const now = yield* Clock.currentTimeMillis;

      if (Option.isSome(unit.due)) {
        const rows = yield* sql<{ id: string; due_at: number; cron: string | null }>`
        SELECT id, due_at, cron FROM due_timers WHERE due_at <= ${unit.due.value} ORDER BY due_at, id`;

        for (const row of rows) {
          yield* Streams.append(outputs.fired, [{ id: row.id, dueAt: row.due_at, firedAt: now }]);

          if (row.cron === null) {
            yield* sql`DELETE FROM due_timers WHERE id = ${row.id}`;
          } else {
            // Catch up once: publish one overdue fact, then advance strictly past now.
            const next = Cron.next(Cron.parseUnsafe(row.cron, "UTC"), now).getTime();
            yield* sql`UPDATE due_timers SET due_at = ${next} WHERE id = ${row.id}`;
          }
        }
      }

      const rows = yield* sql<{ at: number | null }>`SELECT MIN(due_at) AS at FROM due_timers`;

      return rows[0]?.at == null ? undefined : Projection.wakeAt(rows[0].at);
    }),
});

const local = Protocol.bind(Storage, Streamsy.group(Timers, Fired, Accepted)).pipe(
  Layer.provideMerge(Storage),
);

const host = Projection.attach(fire, { durable: true }).pipe(
  Layer.provideMerge(Projection.Registry.layer),
  Layer.provideMerge(local),
);

const flow = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE due_timers (id TEXT PRIMARY KEY, due_at INTEGER NOT NULL, cron TEXT, seen INTEGER NOT NULL)`;
  const timers = yield* Timers;
  yield* timers.create({});
  yield* (yield* Fired).create({});
  yield* (yield* Accepted).create({});
  const now = yield* Clock.currentTimeMillis;
  const onceAt = now + 1_000;
  const expression = "* * * * * *";
  const cronAt = Cron.next(Cron.parseUnsafe(expression, "UTC"), onceAt).getTime();
  yield* timers.append(
    {},
    DurableState.changes(Timers.at({}), {}, [
      DurableState.upsert("timer", { id: "once", dueAt: onceAt, cron: null }),
      DurableState.upsert("timer", { id: "cancelled", dueAt: onceAt, cron: null }),
      DurableState.upsert("timer", { id: "cron", dueAt: cronAt, cron: expression }),
    ]),
  );

  const cancelled = yield* timers.append(
    {},
    DurableState.changes(Timers.at({}), {}, [DurableState.remove("timer", "cancelled")]),
  );

  yield* Streamsy.awaitWake(Accepted, {}, { stream: Timers.at({}).id, offset: cancelled.offset });

  const pending = yield* sql<{
    count: number;
    at: number;
  }>`SELECT COUNT(*) AS count, MIN(due_at) AS at FROM due_timers`;

  const registry = yield* Projection.Registry;
  yield* registry.status.pipe(
    Effect.repeat({
      schedule: Schedule.spaced("10 millis"),
      until: (statuses) => statuses[0]?.marks.sleeping === 1,
    }),
  );

  if (pending[0]?.count !== 2 || pending[0].at !== onceAt)
    return yield* Effect.die(new Error("Due table mismatch"));
  const fired = yield* Fired;

  const facts = yield* fired.collect({}).pipe(
    Effect.repeat({
      schedule: Schedule.spaced("10 millis"),
      until: ({ items }) =>
        Array.some(items, (item) => item.id === "once") &&
        Array.some(items, (item) => item.id === "cron"),
    }),
  );

  const once = Array.filter(facts.items, (item) => item.id === "once");
  const cron = Array.findFirst(facts.items, (item) => item.id === "cron");

  if (
    once.length !== 1 ||
    once[0]?.dueAt !== onceAt ||
    Option.isNone(cron) ||
    cron.value.dueAt !== cronAt ||
    Array.some(facts.items, (item) => item.id === "cancelled" || item.firedAt < item.dueAt)
  ) {
    return yield* Effect.die(new Error("Timer facts mismatch or early wake"));
  }

  const remaining = yield* sql<{ id: string; due_at: number }>`SELECT id, due_at FROM due_timers`;

  if (remaining.length !== 1 || remaining[0]?.id !== "cron" || remaining[0].due_at <= cronAt) {
    return yield* Effect.die(new Error("Cron did not advance"));
  }

  return {
    oneShot: once.length,
    cancelled: 0,
    cron: 1,
    sleepingBindings: 1,
    timerRows: pending[0].count,
  };
});

export const smoke = flow.pipe(Effect.provide(host), Effect.scoped, Effect.timeout("8 seconds"));
