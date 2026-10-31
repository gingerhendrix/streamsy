# @streamsy/outbox

Transactional enqueue and bounded, at-least-once delivery with Effect. The package
has memory, SQLite, Postgres and MySQL stores and no host dependencies.

```ts
import { Effect, Schema } from "effect";
import { Outbox, RetryableDeliveryFailure } from "@streamsy/outbox";
import { layerMemory } from "@streamsy/outbox/memory";

const receipts = Outbox.define({
  name: "shop.receipt",
  payload: Schema.Struct({ orderId: Schema.String }),
  idempotencyKey: (receipt) => `receipt/${receipt.orderId}`,
  partitionBy: (receipt) => receipt.orderId,
  retry: {
    maxAttempts: 8,
    initialDelayMs: 1_000,
    factor: 2,
    maxDelayMs: 60_000,
  },
});

const program = Effect.gen(function* () {
  yield* Outbox.enqueue(receipts, [{ orderId: "42" }]);
  return yield* Outbox.drain(
    receipts,
    ({ payload, idempotencyKey, attempt }) =>
      sendReceipt(payload.orderId, idempotencyKey, attempt).pipe(
        Effect.mapError(() => new RetryableDeliveryFailure({ detail: "Provider unavailable" })),
      ),
    { limit: 100 },
  );
});
// Provide layerMemory and the delivery function's own services at your entry point.
```

The application supplies `sendReceipt`. Delivery functions are ordinary Effects;
their service requirements propagate through `drain`. Use
`PermanentDeliveryFailure` for an expected refusal that cannot recover. Synchronous
throws and non-interrupt Effect defects consume retry attempts. Invalid payloads
become dead letters. Interruption leaves the current entry unsettled.

Payload types are inferred from a service-free `Schema.Codec`. Schema JSON
encoding and decoding preserve transformations. Enqueue encodes and validates
the complete batch before inserting anything. Definition validates retry settings,
throws `InvalidConfiguration` for invalid values, and copies and freezes the
policy. Limits and attempt budgets must be positive safe integers; retry
parameters must be finite, with nonnegative delays, a factor of at least one,
and a safe integer cap no smaller than the initial delay. Definition and
delivery types live on the namespace: `Outbox.Definition`, `Outbox.Deliver`,
`Outbox.DrainOptions` and so on.

## SQL and transaction ownership

```ts
import { layerSql, migrateOutbox } from "@streamsy/outbox/sql";

yield * migrateOutbox(sql); // Required at startup, outside any transaction.
yield *
  sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`INSERT INTO orders ${sql.insert({ id: "42" })}`;
      yield* Outbox.enqueue(receipts, [{ orderId: "42" }]);
    }),
  );
// Provide layerSql with the application's SqlClient layer. Drain after commit.
```

`layerSql` builds the store from the `SqlClient` in context, so the store uses
the application's client and its active transaction at execution time. It does
not start a batch transaction: outside a caller transaction, a later insert
failure can leave earlier inserts committed. Run `migrateOutbox` once at startup
on every dialect. Store operations run no DDL, and they fail with
`OutboxUnavailable` when the table is missing.

The new table is `streamsy_outbox`. No legacy table is changed or migrated.
Identity is `(name, idempotencyKey)`. Duplicate enqueue retains the original
payload and partition even after delivery or dead-lettering. Settled rows remain
as deduplication history. MySQL names and partitions support 255 Unicode code
points, and keys 512, with case-sensitive, trailing-space-sensitive comparison.

## Draining and scheduling

A drain performs one serial pass of at most `limit` entries (default 100) without
sleeping. Retry deadlines are persisted using the Effect clock. A deadline beyond
`Number.MAX_SAFE_INTEGER` clamps to it. A pass returns
`{ name, attempted, delivered, retried, deadLettered, more }`. `more` is true
when the pass stopped at its limit, so a scheduler can run another pass at once.

An omitted `partitionId` selects all partitions. With `ordered: true`, the oldest
pending entry blocks the selected scope until its retry is due. A retry also
stops that ordered pass. Terminal dead letters allow subsequent entries to run.
Without ordering, future retries do not block other entries.

All drains of one name through the **same built store** share a permit, including
whole-name and partition drains. A `partitionId` filters a pass; it does not let
partitions drain concurrently. A delivery must not recursively drain that name:
the permit is non-reentrant. Hosts must ensure one owner for overlapping scopes
across different store objects or processes. `readDue` only reads; it does not
acquire a distributed lease.

Use the store's `nextDue({ name, partitionId, ordered })` to schedule another
pass. It returns an Effect `Option<number>`, excluding settled entries.
`list({ name, partitionId })` provides inspection. `OutboxBacking` is the adapter
protocol behind `OutboxStore`. Applications must not call its write methods
(`enqueue`, `readDue`, `settle`) directly. `settle` takes a `Settlement`, a
`Data.TaggedEnum` with `Delivered`, `Retry` and `Dead` cases.

Store failures are `OutboxUnavailable` for storage errors and
`InvalidStoredRecord` for malformed rows, distinct from payload poison. `drain`
also fails with `InvalidConfiguration` for an invalid limit. Enqueue codec or
identity callback failures use `PayloadEncodingFailure`.

Receivers must deduplicate by the supplied key. A successful external call
followed by failed settlement can repeat with the same attempt number.
`maxAttempts` bounds persisted failure accounting, not absolute external calls.
Applications own handler timeouts: a bounded entry count does not bound pass
duration. There is no host scheduler, replay API or retention policy here.

Run `bun run test:unit` for memory and SQLite checks. For live dialect tests, set
`STREAMSY_PG_URL` and/or `STREAMSY_MYSQL_URL` and run `bun run test:dialects`.
Missing URLs skip those dialect checks.
