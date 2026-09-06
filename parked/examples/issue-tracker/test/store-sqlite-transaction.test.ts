/* oxlint-disable effecttsgo/node-builtin-import -- This test needs disposable SQLite files on the host filesystem. */
import { OutboxUnavailable } from "@streamsy/sinks/action/errors";
import { type OutboxBacking } from "@streamsy/sinks/action/outbox";
import { createSqliteOutboxBacking } from "@streamsy/sinks/action/sqlite";
import { expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Exit, ManagedRuntime } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  createSqliteIssueStoreBoundary,
  sqliteClientLayer,
  type SqliteStoreOptions,
} from "../server/persistence/store-sqlite.ts";
import type { CommandReceipt } from "../server/persistence/store.ts";

const sqliteOptions = (): SqliteStoreOptions => ({
  filename: join(mkdtempSync(join(tmpdir(), "issue-tracker-store-sqlite-")), "view.sqlite"),
});

test("a failed outbox enqueue rolls back the receipt and the queued delivery together", () =>
  (() => {
    const runtime = ManagedRuntime.make(sqliteClientLayer(sqliteOptions()));
    return runtime
      .runPromise(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const durableOutbox = createSqliteOutboxBacking(sql);
          const failingOutbox: OutboxBacking = {
            ...durableOutbox,
            enqueue: (drafts) =>
              durableOutbox.enqueue(drafts).pipe(
                Effect.andThen(
                  Effect.fail(
                    new OutboxUnavailable({
                      operation: "enqueue",
                      detail: "injected after durable enqueue",
                    }),
                  ),
                ),
              ),
          };
          const boundary = createSqliteIssueStoreBoundary(sql, failingOutbox);
          const receipt: CommandReceipt = {
            workspaceId: "main",
            commandId: "cmd-1",
            commandKind: "create-issue",
            targetId: "issue-1",
            requestHash: "hash-1",
            eventId: "evt-1",
            eventSequence: 1,
            eventOffset: "00000001",
          };
          const draft = {
            sink: "issue-tracker.assignment-notifications",
            partitionId: "main",
            idempotencyKey: "main/evt-1",
            payload: '{"workspaceId":"main","issueId":"issue-1"}',
            enqueuedAtMs: 1_000,
          } as const;

          const exit = yield* Effect.exit(boundary.recordReceipt(receipt, [draft]));
          expect(Exit.isFailure(exit)).toBe(true);
          expect(yield* boundary.receipt(receipt.workspaceId, receipt.commandId)).toBeUndefined();
          expect(yield* durableOutbox.list(draft.sink, draft.partitionId)).toHaveLength(0);
        }),
      )
      .finally(() => runtime.dispose());
  })());
