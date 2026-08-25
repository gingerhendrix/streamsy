/* oxlint-disable anti-slop/require-safety-comment-for-type-assertion, anti-slop/no-chained-type-assertions -- This test converts schema-decoded IssueEvent and IssueRow fixtures into the package's transport-neutral JSON persistence grammar. */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Effect } from "effect";
import { planHash } from "@streamsy/views";
import type { JsonObject } from "@streamsy/views-ir";
import { recover, type JsonValue } from "@streamsy/views-store";
import { migrateViewStore, sqliteService } from "@streamsy/views-store/sqlite";
import { issueLifecycle, issues } from "../domain/declaration.ts";
import { decodeIssueRow, type IssueEvent, type IssueRow } from "../domain/issue.ts";
import { maintain } from "../views/engine.ts";

const created: IssueEvent = {
  type: "IssueCreated",
  eventId: "e1",
  workspaceId: "main",
  issueId: "i1",
  sequence: 0,
  occurredAt: "2026-08-24T10:00:00.000Z",
  title: "Recover",
  projectId: "streamsy",
  status: "backlog",
};
const moved: IssueEvent = {
  type: "IssueStatusChanged",
  eventId: "e2",
  workspaceId: "main",
  issueId: "i1",
  sequence: 1,
  occurredAt: "2026-08-24T10:01:00.000Z",
  status: "todo",
};
const finished: IssueEvent = {
  type: "IssueStatusChanged",
  eventId: "e3",
  workspaceId: "main",
  issueId: "i1",
  sequence: 2,
  occurredAt: "2026-08-24T10:02:00.000Z",
  status: "done",
};
const jsonObject = (event: IssueEvent): JsonObject =>
  JSON.parse(JSON.stringify(event)) as JsonObject;

test("forced reopen produces deterministic issue rows from checkpoint plus suffix", async () => {
  const filename = join(mkdtempSync(join(tmpdir(), "issue-checkpoint-")), "views.sqlite");
  const identity = {
    planName: issues.name,
    planHash: planHash(issues.plan),
    partition: "main",
    sourceId: "issue-tracker.issue-events",
    reducerId: issueLifecycle.ref.name,
    reducerVersion: issueLifecycle.ref.version,
  } as const;
  const prefix = maintain<IssueRow>({
    plan: issues.plan,
    reducer: issueLifecycle,
    decodeRow: decodeIssueRow,
    current: new Map(),
    items: [created, moved].map(jsonObject),
  });
  let database = new Database(filename, { create: true });
  migrateViewStore(database, 1);
  let store = sqliteService(database);
  await Effect.runPromise(
    store.saveCheckpoint({
      ...identity,
      sourceCursor: "offset-2",
      createdAtMs: 2,
      entries: [...prefix.rows].map(([key, value]) => ({
        key,
        value: value as unknown as JsonValue,
      })),
    }),
  );
  database.close(false);
  database = new Database(filename);
  database.run("PRAGMA foreign_keys=ON");
  migrateViewStore(database, 2);
  store = sqliteService(database);
  const cursors: (string | undefined)[] = [];
  const result = await Effect.runPromise(
    recover({
      store,
      checkpoint: identity,
      saveCheckpoint: true,
      now: () => 3,
      source: {
        readAfter: (cursor) =>
          Effect.sync(() => {
            cursors.push(cursor);
            return cursor === "offset-2"
              ? { items: [jsonObject(finished)], afterExclusiveCursor: "offset-3" }
              : { items: [], afterExclusiveCursor: cursor };
          }),
      },
      reducer: {
        fold: (state, items) =>
          Effect.sync(() => {
            const current = new Map<string, IssueRow>();
            for (const [encoded, value] of state)
              current.set(JSON.parse(encoded) as string, decodeIssueRow(value));
            const folded = maintain<IssueRow>({
              plan: issues.plan,
              reducer: issueLifecycle,
              decodeRow: decodeIssueRow,
              current,
              items,
            });
            return {
              state: new Map(
                [...folded.rows].map(([key, value]) => [
                  key,
                  { key, value: value as unknown as JsonValue },
                ]),
              ),
              commit: {
                identity,
                batchId: "offset-3",
                committedAtMs: 3,
                rows: [...folded.rows].map(([key, value]) => ({
                  kind: "put" as const,
                  namespace: { ...identity, id: issues.name },
                  key,
                  value: value as unknown as JsonValue,
                })),
                reducerStates: [...folded.rows].map(([key, value]) => ({
                  kind: "put" as const,
                  namespace: { ...identity, id: issueLifecycle.ref.name },
                  key,
                  value: value as unknown as JsonValue,
                })),
                changes: folded.changes.map((change) =>
                  change.kind === "enter"
                    ? {
                        ...change,
                        relationId: issues.name,
                        after: change.after as unknown as JsonValue,
                      }
                    : change.kind === "update"
                      ? {
                          ...change,
                          relationId: issues.name,
                          before: change.before as unknown as JsonValue,
                          after: change.after as unknown as JsonValue,
                        }
                      : {
                          ...change,
                          relationId: issues.name,
                          before: change.before as unknown as JsonValue,
                        },
                ),
              },
            };
          }),
      },
    }),
  );
  expect(result.folded).toBe(1);
  expect(cursors).toEqual(["offset-2"]);
  const recovered = (
    await Effect.runPromise(store.snapshotRows({ ...identity, id: issues.name }))
  ).rows.map((row) => decodeIssueRow(row.value));
  const uninterrupted = maintain<IssueRow>({
    plan: issues.plan,
    reducer: issueLifecycle,
    decodeRow: decodeIssueRow,
    current: new Map(),
    items: [created, moved, finished].map(jsonObject),
  }).rows;
  expect(recovered).toEqual([...uninterrupted.values()]);
  const historyBefore = await Effect.runPromise(store.historyBounds(identity));
  const idle = await Effect.runPromise(
    recover({
      store,
      checkpoint: identity,
      source: {
        readAfter: (cursor) => Effect.succeed({ items: [], afterExclusiveCursor: cursor }),
      },
      reducer: { fold: () => Effect.die("must not fold at tail") },
    }),
  );
  expect(idle.committed).toBe(false);
  expect(await Effect.runPromise(store.historyBounds(identity))).toEqual(historyBefore);
  database.close(false);
});
