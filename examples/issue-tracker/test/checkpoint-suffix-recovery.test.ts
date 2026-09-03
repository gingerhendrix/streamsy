// oxlint-disable-next-line effecttsgo/node-builtin-import -- Recovery must reopen a real on-disk SQLite database to exercise the forced migration boundary.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- This joins the package-owned temporary recovery fixture path.
import { join } from "node:path";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Context, Effect, Layer, ManagedRuntime, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import { planHash } from "@streamsy/views";
import type { JsonObject } from "@streamsy/views/ir";
import { recover } from "@streamsy/views/store";
import { migrateViewStore, sqliteService } from "@streamsy/views/store/sqlite";
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
const decodeJsonObject = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Json));
const decodeJsonValue = Schema.decodeUnknownSync(Schema.Json);
const decodeJsonKey = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.String));
const jsonObject = (event: IssueEvent): JsonObject => decodeJsonObject(event);
const sqliteClientLayer = (filename: string) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const client = yield* SqliteClient.make({ filename, create: true });
      yield* client.unsafe<Record<string, never>>("PRAGMA foreign_keys = ON").pipe(Effect.asVoid);
      return Context.empty().pipe(
        Context.add(SqliteClient.SqliteClient, client),
        Context.add(SqlClient.SqlClient, client),
      );
    }),
  ).pipe(Layer.provide(Reactivity.layer));

test("forced reopen produces deterministic issue rows from checkpoint plus suffix", () =>
  Effect.runPromise(
    Effect.gen(function* () {
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
      let runtime = ManagedRuntime.make(sqliteClientLayer(filename));
      let store = runtime.runSync(Effect.map(SqlClient.SqlClient, sqliteService));
      yield* store.saveCheckpoint({
        ...identity,
        sourceCursor: "offset-2",
        createdAtMs: 2,
        entries: [...prefix.rows].map(([key, value]) => ({
          key,
          value: decodeJsonValue(value),
        })),
      });
      yield* Effect.promise(() => runtime.dispose());
      database.close(false);
      database = new Database(filename);
      database.run("PRAGMA foreign_keys=ON");
      migrateViewStore(database, 2);
      runtime = ManagedRuntime.make(sqliteClientLayer(filename));
      store = runtime.runSync(Effect.map(SqlClient.SqlClient, sqliteService));
      const cursors: (string | undefined)[] = [];
      const result = yield* recover({
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
                current.set(decodeJsonKey(encoded), decodeIssueRow(value));
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
                    { key, value: decodeJsonValue(value) },
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
                    value: decodeJsonValue(value),
                  })),
                  reducerStates: [...folded.rows].map(([key, value]) => ({
                    kind: "put" as const,
                    namespace: { ...identity, id: issueLifecycle.ref.name },
                    key,
                    value: decodeJsonValue(value),
                  })),
                  changes: folded.changes.map((change) =>
                    change.kind === "enter"
                      ? {
                          ...change,
                          relationId: issues.name,
                          after: decodeJsonValue(change.after),
                        }
                      : change.kind === "update"
                        ? {
                            ...change,
                            relationId: issues.name,
                            before: decodeJsonValue(change.before),
                            after: decodeJsonValue(change.after),
                          }
                        : {
                            ...change,
                            relationId: issues.name,
                            before: decodeJsonValue(change.before),
                          },
                  ),
                },
              };
            }),
        },
      });
      expect(result.folded).toBe(1);
      expect(cursors).toEqual(["offset-2"]);
      const recovered = (yield* store.snapshotRows({ ...identity, id: issues.name })).rows.map(
        (row) => decodeIssueRow(row.value),
      );
      const uninterrupted = maintain<IssueRow>({
        plan: issues.plan,
        reducer: issueLifecycle,
        decodeRow: decodeIssueRow,
        current: new Map(),
        items: [created, moved, finished].map(jsonObject),
      }).rows;
      expect(recovered).toEqual([...uninterrupted.values()]);
      const historyBefore = yield* store.historyBounds(identity);
      const idle = yield* recover({
        store,
        checkpoint: identity,
        source: {
          readAfter: (cursor) => Effect.succeed({ items: [], afterExclusiveCursor: cursor }),
        },
        reducer: { fold: () => Effect.die("must not fold at tail") },
      });
      expect(idle.committed).toBe(false);
      expect(yield* store.historyBounds(identity)).toEqual(historyBefore);
      yield* Effect.promise(() => runtime.dispose());
      database.close(false);
    }),
  ));
