import { Projection } from "@streamsy/projection";
import { Effect, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { Identifier, IssueRow, foldIssue, type IssueEvent } from "../domain/issue.ts";
import { checkCatalogWorkspace } from "./catalog.ts";
import { events, labelEvents, labels, projects, users } from "./streams.ts";

type StoredIssue = {
  readonly workspace_id: string;
  readonly issue_id: string;
  readonly project_id: string;
  readonly title: string;
  readonly status: (typeof IssueRow.Type)["status"];
  readonly sequence: number;
  readonly updated_at: string;
  readonly assignee_id: string | null;
};
const toIssue = (row: StoredIssue): typeof IssueRow.Type => ({
  workspaceId: row.workspace_id,
  issueId: row.issue_id,
  projectId: row.project_id,
  title: row.title,
  status: row.status,
  sequence: row.sequence,
  updatedAt: row.updated_at,
  assigneeId: row.assignee_id ?? undefined,
});

const applyIssue = (sql: SqlClient.SqlClient, event: IssueEvent) =>
  Effect.gen(function* () {
    const rows = yield* sql.unsafe<StoredIssue>(
      `SELECT * FROM issues WHERE workspace_id=? AND issue_id=?`,
      [event.workspaceId, event.issueId],
    );
    const before = rows[0] === undefined ? undefined : toIssue(rows[0]);
    const after = foldIssue(before, event);
    if (after === undefined || after === before) return;
    yield* sql.unsafe(
      `INSERT INTO issues (workspace_id,issue_id,project_id,title,status,sequence,updated_at,assignee_id) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(workspace_id,issue_id) DO UPDATE SET project_id=excluded.project_id,title=excluded.title,status=excluded.status,sequence=excluded.sequence,updated_at=excluded.updated_at,assignee_id=excluded.assignee_id`,
      [
        after.workspaceId,
        after.issueId,
        after.projectId,
        after.title,
        after.status,
        after.sequence,
        after.updatedAt,
        after.assigneeId ?? null,
      ],
    );
    yield* sql.unsafe(
      `INSERT OR IGNORE INTO issue_changes (workspace_id,issue_id,event_id,event_type,sequence,occurred_at,status) VALUES (?,?,?,?,?,?,?)`,
      [
        event.workspaceId,
        event.issueId,
        event.eventId,
        event.type,
        event.sequence,
        event.occurredAt,
        after.status,
      ],
    );
    if (event.type === "IssueAssigned")
      yield* sql.unsafe(
        `INSERT OR IGNORE INTO notification_drafts (event_id,workspace_id,issue_id,assignee_id,title,status,occurred_at) VALUES (?,?,?,?,?,?,?)`,
        [
          event.eventId,
          event.workspaceId,
          event.issueId,
          event.assigneeId,
          after.title,
          after.status,
          event.occurredAt,
        ],
      );
  });

export const issueRows = Projection.family({
  id: "issue-rows",
  params: { workspaceId: Identifier },
  inputs: { events, labelEvents, projects, users, labels },
  process: Projection.each((entry, unit) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      switch (entry.input) {
        case "events":
          yield* applyIssue(sql, entry.item);
          break;
        case "labelEvents": {
          const item = entry.item;
          yield* sql.unsafe(
            `INSERT INTO issue_labels (workspace_id,membership_id,issue_id,label_id,attached,sequence,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(workspace_id,membership_id) DO UPDATE SET attached=excluded.attached,sequence=excluded.sequence,updated_at=excluded.updated_at WHERE excluded.sequence > issue_labels.sequence`,
            [
              item.workspaceId,
              item.membershipId,
              item.issueId,
              item.labelId,
              item.type === "LabelAttached" ? 1 : 0,
              item.sequence,
              item.occurredAt,
            ],
          );
          break;
        }
        case "projects":
        case "users":
        case "labels": {
          const item = entry.item;
          const table = entry.input;
          const idColumn =
            table === "projects" ? "project_id" : table === "users" ? "user_id" : "label_id";
          // The family codec guarantees this parameter on every member.
          const workspaceId = unit.params.workspaceId!;
          if (!("value" in item)) {
            yield* sql.unsafe(`DELETE FROM ${table} WHERE workspace_id=? AND ${idColumn}=?`, [
              workspaceId,
              item.key,
            ]);
          } else {
            yield* checkCatalogWorkspace(workspaceId, item.value.workspaceId, item.type, item.key);
            yield* sql.unsafe(
              `INSERT INTO ${table} (workspace_id,${idColumn},value) VALUES (?,?,?) ON CONFLICT(workspace_id,${idColumn}) DO UPDATE SET value=excluded.value`,
              [workspaceId, item.key, JSON.stringify(item.value)],
            );
          }
          break;
        }
      }
    }),
  ),
});

export const decodeIssueRow = Schema.decodeUnknownSync(IssueRow);
