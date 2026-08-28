/* oxlint-disable effecttsgo/async-function -- bun:test owns the Promise-native real-workerd harness. */
import { afterEach, describe, expect, test } from "bun:test";
import { Schema } from "effect";
import { decodeResponse, workerdHarness, type WorkerdHarness } from "./cloudflare-support.ts";

const SnapshotParityResult = Schema.Struct({
  sourceCursor: Schema.String,
  rows: Schema.Array(
    Schema.Struct({ key: Schema.String, value: Schema.Struct({ revision: Schema.Finite }) }),
  ),
});
const HistoryParityResult = Schema.Struct({
  epoch: Schema.Finite,
  first: Schema.Finite,
  latest: Schema.Finite,
});
const ChangesParityResult = Schema.Array(
  Schema.Struct({
    position: Schema.Struct({ epoch: Schema.Finite, sequence: Schema.Finite }),
    changes: Schema.Array(Schema.Struct({ kind: Schema.String })),
  }),
);
const CheckpointParityResult = Schema.Struct({
  generation: Schema.Finite,
  sourceCursor: Schema.String,
  entries: Schema.Array(
    Schema.Struct({ key: Schema.String, value: Schema.Struct({ revision: Schema.Finite }) }),
  ),
});
const OutboxRollbackResult = Schema.Struct({
  failed: Schema.Boolean,
  rollbackCount: Schema.Finite,
  retried: Schema.Struct({ enqueued: Schema.Finite, absorbed: Schema.Finite }),
  retryCount: Schema.Finite,
});
const ReceiptRollbackResult = Schema.Struct({
  failed: Schema.Boolean,
  receiptRolledBack: Schema.Boolean,
  outboxRollbackCount: Schema.Finite,
  retryReceipt: Schema.String,
  retryOutboxCount: Schema.Finite,
});

const open: WorkerdHarness[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((harness) => harness.close()));
});

async function run<S extends Schema.ConstraintDecoder<unknown>>(
  path: string,
  schema: S,
): Promise<S["Type"]> {
  const harness = await workerdHarness("test/cloudflare-parity-worker.ts");
  open.push(harness);
  const response = await harness.fetch(path);
  expect(response.status).toBe(200);
  return decodeResponse(response, schema);
}

describe("@effect/sql-sqlite-do parity on full DurableObjectStorage", () => {
  test("snapshotRows materializes progress and rows from one revision", async () => {
    expect(await run("/snapshot", SnapshotParityResult)).toEqual({
      sourceCursor: "1",
      rows: [{ key: "issue", value: { revision: 1 } }],
    });
  });

  test("historyBounds cannot mix the retained range and epoch", async () => {
    expect(await run("/history", HistoryParityResult)).toEqual({ epoch: 1, first: 1, latest: 1 });
  });

  test("changesAfter retains every row of the selected pre-prune batch", async () => {
    const batches = await run("/changes", ChangesParityResult);
    expect(batches).toHaveLength(1);
    expect(batches[0]?.position).toEqual({ epoch: 1, sequence: 1 });
    expect(batches[0]?.changes).toHaveLength(1);
  });

  test("loadCheckpoint retains the selected manifest and all entries", async () => {
    expect(await run("/checkpoint", CheckpointParityResult)).toMatchObject({
      generation: 1,
      sourceCursor: "1",
      entries: [{ key: "issue", value: { revision: 1 } }],
    });
  });

  test("rolled-back first enqueue retries on the same resident backing", async () => {
    expect(await run("/outbox-rollback", OutboxRollbackResult)).toEqual({
      failed: true,
      rollbackCount: 0,
      retried: { enqueued: 1, absorbed: 0 },
      retryCount: 1,
    });
  });

  test("receipt and outbox post-enqueue failure roll back together", async () => {
    expect(await run("/receipt-rollback", ReceiptRollbackResult)).toEqual({
      failed: true,
      receiptRolledBack: true,
      outboxRollbackCount: 0,
      retryReceipt: "0000000000000001",
      retryOutboxCount: 1,
    });
  });
});
