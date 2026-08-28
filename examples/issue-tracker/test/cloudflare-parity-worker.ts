/* oxlint-disable effecttsgo/async-function -- This is a workerd-only test executable and Durable Object platform edge. */
/* oxlint-disable effecttsgo/new-promise -- Deterministic test gates deliberately expose Promise controls to the workerd handler. */
/* oxlint-disable typescript/no-unsafe-type-assertion -- The test proxy preserves the exact SqlClient surface and taps one selected query. */
import { SqliteClient } from "@effect/sql-sqlite-do";
import { OutboxUnavailable, type OutboxBacking } from "@streamsy/effect-sink";
import { createSqliteOutboxBacking } from "@streamsy/effect-sink/sqlite";
import type { Checkpoint, HistoryBounds, Snapshot, StoredChangeBatch } from "@streamsy/views-store";
import { migrateViewStoreSql, sqliteService } from "@streamsy/views-store/sqlite";
import type { DurableObjectState } from "@cloudflare/workers-types";
import { Effect, Exit, ManagedRuntime } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { createSqliteIssueStoreBoundary, migrateApplicationStore } from "../server/store-sql.ts";

interface Env {
  readonly PARITY: {
    idFromName(name: string): NonNullable<unknown>;
    get(id: NonNullable<unknown>): { fetch(request: Request): Promise<Response> };
  };
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.PARITY.get(env.PARITY.idFromName("parity")).fetch(request);
  },
};

const identity = {
  planName: "do-parity",
  planHash: "hash-1",
  partition: "main",
  sourceId: "source",
};
const relation = { ...identity, id: "rows" };

const clientRuntime = (storage: DurableObjectState["storage"]) =>
  ManagedRuntime.make(SqliteClient.layer({ storage }));

const interleaved = (
  sql: SqlClient.SqlClient,
  marker: string,
  reached: () => void,
  resume: Promise<void>,
): SqlClient.SqlClient => {
  let paused = false;
  // SAFETY: the proxy delegates every client member and preserves unsafe's overload contract.
  const unsafe = ((statement: string, parameters?: ReadonlyArray<unknown>) => {
    const result = sql.unsafe<object>(statement, parameters);
    if (paused || !statement.includes(marker)) return result;
    paused = true;
    return Effect.flatMap(result, (rows) =>
      Effect.as(
        Effect.promise(() => {
          reached();
          return resume;
        }),
        rows,
      ),
    );
  }) as SqlClient.SqlClient["unsafe"];
  // SAFETY: replacing unsafe is the only structural change to this test-only SqlClient proxy.
  return {
    ...sql,
    unsafe,
  } as SqlClient.SqlClient;
};

interface OutboxRollbackResult {
  readonly failed: boolean;
  readonly rollbackCount: number;
  readonly retried: { readonly enqueued: number; readonly absorbed: number };
  readonly retryCount: number;
}

interface ReceiptRollbackResult {
  readonly failed: boolean;
  readonly receiptRolledBack: boolean;
  readonly outboxRollbackCount: number;
  readonly retryReceipt: string | undefined;
  readonly retryOutboxCount: number;
}

export class SqlParityObject {
  constructor(private readonly ctx: DurableObjectState) {}

  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (path === "/snapshot") return Response.json(await this.snapshot());
    if (path === "/history") return Response.json(await this.history());
    if (path === "/changes") return Response.json(await this.changes());
    if (path === "/checkpoint") return Response.json(await this.checkpoint());
    if (path === "/outbox-rollback") return Response.json(await this.outboxRollback());
    if (path === "/receipt-rollback") return Response.json(await this.receiptRollback());
    return Response.json({ error: "not-found" }, { status: 404 });
  }

  private async snapshot(): Promise<Snapshot> {
    const readerRuntime = clientRuntime(this.ctx.storage);
    const writerRuntime = clientRuntime(this.ctx.storage);
    try {
      const readerSql = readerRuntime.runSync(SqliteClient.SqliteClient);
      const writerSql = writerRuntime.runSync(SqliteClient.SqliteClient);
      await writerRuntime.runPromise(migrateViewStoreSql(writerSql));
      const writer = sqliteService(writerSql);
      await Effect.runPromise(
        writer.commit({
          identity,
          expectedCursor: undefined,
          afterExclusiveCursor: "1",
          batchId: "batch-1",
          committedAtMs: 1,
          rows: [{ kind: "put", namespace: relation, key: "issue", value: { revision: 1 } }],
        }),
      );
      let markReached!: () => void;
      let resume!: () => void;
      const reached = new Promise<void>((resolve) => {
        markReached = resolve;
      });
      const resumed = new Promise<void>((resolve) => {
        resume = resolve;
      });
      const reader = sqliteService(
        interleaved(
          readerSql,
          "SELECT source_cursor FROM streamsy_view_partitions",
          markReached,
          resumed,
        ),
      );
      const reading = Effect.runPromise(reader.snapshotRows(relation));
      await reached;
      await Effect.runPromise(
        writer.commit({
          identity,
          expectedCursor: "1",
          afterExclusiveCursor: "2",
          batchId: "batch-2",
          committedAtMs: 2,
          rows: [{ kind: "put", namespace: relation, key: "issue", value: { revision: 2 } }],
        }),
      );
      resume();
      return await reading;
    } finally {
      await readerRuntime.dispose();
      await writerRuntime.dispose();
    }
  }

  private async history(): Promise<HistoryBounds> {
    return this.withViewClients(async (readerSql, writerSql) => {
      const writer = sqliteService(writerSql);
      await Effect.runPromise(
        writer.commit({
          identity,
          expectedCursor: undefined,
          afterExclusiveCursor: "1",
          batchId: "batch-1",
          committedAtMs: 1,
        }),
      );
      const gate = makeGate();
      const reader = sqliteService(
        interleaved(
          readerSql,
          "SELECT MIN(history_seq) first,MAX(history_seq) latest",
          gate.reached,
          gate.resume,
        ),
      );
      const reading = Effect.runPromise(reader.historyBounds(identity));
      await gate.wait;
      await Effect.runPromise(
        writerSql.withTransaction(
          Effect.gen(function* () {
            yield* writerSql.unsafe("DELETE FROM streamsy_view_change_batches");
            yield* writerSql.unsafe(
              "UPDATE streamsy_view_partitions SET history_epoch=2,history_floor=2,next_history_seq=3",
            );
            yield* writerSql.unsafe(
              "INSERT INTO streamsy_view_change_batches VALUES (?,?,?,?,?,?,?,?,?)",
              [
                identity.planName,
                identity.partition,
                2,
                2,
                identity.sourceId,
                "2",
                "batch-2",
                identity.planHash,
                2,
              ],
            );
          }),
        ),
      );
      gate.continue();
      return reading;
    });
  }

  private async changes(): Promise<readonly StoredChangeBatch[]> {
    return this.withViewClients(async (readerSql, writerSql) => {
      const writer = sqliteService(writerSql);
      await Effect.runPromise(
        writer.commit({
          identity,
          expectedCursor: undefined,
          afterExclusiveCursor: "1",
          batchId: "batch-1",
          committedAtMs: 1,
          changes: [
            { kind: "enter", relationId: relation.id, key: "issue", after: { revision: 1 } },
          ],
        }),
      );
      const gate = makeGate();
      const reader = sqliteService(
        interleaved(
          readerSql,
          "SELECT history_seq,source_cursor FROM streamsy_view_change_batches",
          gate.reached,
          gate.resume,
        ),
      );
      const reading = Effect.runPromise(reader.changesAfter(identity, undefined, 10));
      await gate.wait;
      await Effect.runPromise(
        writer.commit(
          {
            identity,
            expectedCursor: "1",
            afterExclusiveCursor: "2",
            batchId: "batch-2",
            committedAtMs: 2,
          },
          { keepLastBatches: 1 },
        ),
      );
      gate.continue();
      return reading;
    });
  }

  private async checkpoint(): Promise<Checkpoint | undefined> {
    return this.withViewClients(async (readerSql, writerSql) => {
      const descriptor = { ...identity, reducerId: "reducer", reducerVersion: 1 };
      const writer = sqliteService(writerSql);
      await Effect.runPromise(
        writer.saveCheckpoint({
          ...descriptor,
          sourceCursor: "1",
          createdAtMs: 1,
          entries: [{ key: "issue", value: { revision: 1 } }],
        }),
      );
      const gate = makeGate();
      const reader = sqliteService(
        interleaved(
          readerSql,
          "SELECT generation,source_cursor,created_at_ms,entry_count",
          gate.reached,
          gate.resume,
        ),
      );
      const reading = Effect.runPromise(reader.loadCheckpoint(descriptor));
      await gate.wait;
      await Effect.runPromise(
        writer.saveCheckpoint({
          ...descriptor,
          sourceCursor: "2",
          createdAtMs: 2,
          entries: [{ key: "issue", value: { revision: 2 } }],
          keepGenerations: 1,
        }),
      );
      gate.continue();
      return reading;
    });
  }

  private async outboxRollback(): Promise<OutboxRollbackResult> {
    const runtime = clientRuntime(this.ctx.storage);
    try {
      const sql = runtime.runSync(SqliteClient.SqliteClient);
      await runtime.runPromise(
        migrateApplicationStore().pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      );
      const backing = createSqliteOutboxBacking(sql);
      const draft = {
        sink: "parity",
        partitionId: "main",
        idempotencyKey: "delivery-1",
        payload: "{}",
        enqueuedAtMs: 1,
      };
      const failed = await Effect.runPromiseExit(
        sql.withTransaction(backing.enqueue([draft]).pipe(Effect.andThen(Effect.fail("rollback")))),
      );
      const afterRollback = await Effect.runPromise(backing.list("parity", "main"));
      const retried = await Effect.runPromise(backing.enqueue([draft]));
      const afterRetry = await Effect.runPromise(backing.list("parity", "main"));
      return {
        failed: Exit.isFailure(failed),
        rollbackCount: afterRollback.length,
        retried,
        retryCount: afterRetry.length,
      };
    } finally {
      await runtime.dispose();
    }
  }

  private async receiptRollback(): Promise<ReceiptRollbackResult> {
    const runtime = clientRuntime(this.ctx.storage);
    try {
      const sql = runtime.runSync(SqliteClient.SqliteClient);
      await runtime.runPromise(
        migrateApplicationStore().pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      );
      const backing = createSqliteOutboxBacking(sql);
      const failing: OutboxBacking = {
        ...backing,
        enqueue: (drafts) =>
          backing
            .enqueue(drafts)
            .pipe(
              Effect.andThen(
                Effect.fail(
                  new OutboxUnavailable({ operation: "post-enqueue", detail: "injected" }),
                ),
              ),
            ),
      };
      const receipt = {
        commandId: "command-1",
        workspaceId: "main",
        commandKind: "create-issue" as const,
        targetId: "issue-1",
        requestHash: "hash",
        eventId: "event-1",
        eventSequence: 0,
        eventOffset: "0000000000000001",
      };
      const draft = {
        sink: "parity",
        partitionId: "main",
        idempotencyKey: "delivery-1",
        payload: "{}",
        enqueuedAtMs: 1,
      };
      const failedBoundary = createSqliteIssueStoreBoundary(sql, failing);
      const failed = await Effect.runPromiseExit(failedBoundary.recordReceipt(receipt, [draft]));
      const receiptAfter = await Effect.runPromise(failedBoundary.receipt("main", "command-1"));
      const outboxAfter = await Effect.runPromise(backing.list("parity", "main"));
      const healthy = createSqliteIssueStoreBoundary(sql, backing);
      await Effect.runPromise(healthy.recordReceipt(receipt, [draft]));
      return {
        failed: Exit.isFailure(failed),
        receiptRolledBack: receiptAfter === undefined,
        outboxRollbackCount: outboxAfter.length,
        retryReceipt: (await Effect.runPromise(healthy.receipt("main", "command-1")))?.eventOffset,
        retryOutboxCount: (await Effect.runPromise(backing.list("parity", "main"))).length,
      };
    } finally {
      await runtime.dispose();
    }
  }

  private async withViewClients<A>(
    use: (reader: SqlClient.SqlClient, writer: SqlClient.SqlClient) => Promise<A>,
  ): Promise<A> {
    const readerRuntime = clientRuntime(this.ctx.storage);
    const writerRuntime = clientRuntime(this.ctx.storage);
    try {
      const reader = readerRuntime.runSync(SqliteClient.SqliteClient);
      const writer = writerRuntime.runSync(SqliteClient.SqliteClient);
      await writerRuntime.runPromise(migrateViewStoreSql(writer));
      return await use(reader, writer);
    } finally {
      await readerRuntime.dispose();
      await writerRuntime.dispose();
    }
  }
}

function makeGate() {
  let reached!: () => void;
  let resume!: () => void;
  const wait = new Promise<void>((resolve) => {
    reached = resolve;
  });
  const continuing = new Promise<void>((resolve) => {
    resume = resolve;
  });
  return { reached, wait, resume: continuing, continue: resume };
}
