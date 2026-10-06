import { Commit } from "@streamsy/projection";
import { Projection } from "@streamsy/projection";
import { Effect, Schema } from "effect";
import { SqlClient } from "effect/sql";
import {
  Identifier,
  IssueRow,
  foldIssue,
  type IssueEvent,
  type IssueLabelEvent,
} from "../domain/issue.ts";
import { checkCatalogWorkspace } from "./catalog.ts";
import { Events, LabelEvents, Labels, Projects, Users } from "./streams.ts";

type StoredIssue = {
  readonly workspace_id: string;
  readonly issue_id: string;
  readonly project_id: string;
  readonly title: string;
  readonly status: (typeof IssueRow.Type)["status"];
  readonly sequence: number | bigint;
  readonly updated_at: string;
  readonly assignee_id: string | null;
};
const toIssue = (row: StoredIssue): typeof IssueRow.Type => ({
  workspaceId: row.workspace_id,
  issueId: row.issue_id,
  projectId: row.project_id,
  title: row.title,
  status: row.status,
  sequence: Number(row.sequence),
  updatedAt: row.updated_at,
  assigneeId: row.assignee_id ?? undefined,
});

/** One issue row, as `issueRows` last wrote it. */
export const storedIssue = (sql: SqlClient.SqlClient, workspaceId: string, issueId: string) =>
  sql<StoredIssue>`SELECT * FROM issues WHERE workspace_id = ${workspaceId} AND issue_id = ${issueId}`.pipe(
    Effect.map((rows) => (rows[0] === undefined ? undefined : toIssue(rows[0]))),
  );

/** `INSERT ... ON CONFLICT DO NOTHING`, or `INSERT IGNORE` on MySQL. */
const insertIgnore = (
  sql: SqlClient.SqlClient,
  table: "issue_changes" | "notification_drafts",
  row: ReturnType<SqlClient.SqlClient["insert"]>,
) =>
  sql.onDialectOrElse({
    mysql: () => sql`INSERT IGNORE INTO ${sql(table)} ${row}`,
    orElse: () => sql`INSERT INTO ${sql(table)} ${row} ON CONFLICT DO NOTHING`,
  });

const applyIssue = (sql: SqlClient.SqlClient, event: IssueEvent) =>
  Effect.gen(function* () {
    const before = yield* storedIssue(sql, event.workspaceId, event.issueId);
    const after = foldIssue(before, event);
    if (after === undefined || after === before) return;
    const issue = sql.insert({
      workspace_id: after.workspaceId,
      issue_id: after.issueId,
      project_id: after.projectId,
      title: after.title,
      status: after.status,
      sequence: after.sequence,
      updated_at: after.updatedAt,
      assignee_id: after.assigneeId ?? null,
    });
    yield* sql.onDialectOrElse({
      mysql: () =>
        sql`INSERT INTO issues ${issue} ON DUPLICATE KEY UPDATE project_id = VALUES(project_id), title = VALUES(title), status = VALUES(status), sequence = VALUES(sequence), updated_at = VALUES(updated_at), assignee_id = VALUES(assignee_id)`,
      orElse: () =>
        sql`INSERT INTO issues ${issue} ON CONFLICT (workspace_id, issue_id) DO UPDATE SET project_id = excluded.project_id, title = excluded.title, status = excluded.status, sequence = excluded.sequence, updated_at = excluded.updated_at, assignee_id = excluded.assignee_id`,
    });
    yield* insertIgnore(
      sql,
      "issue_changes",
      sql.insert({
        workspace_id: event.workspaceId,
        issue_id: event.issueId,
        event_id: event.eventId,
        event_type: event.type,
        sequence: event.sequence,
        occurred_at: event.occurredAt,
        status: after.status,
      }),
    );
    if (event.type === "IssueAssigned")
      yield* insertIgnore(
        sql,
        "notification_drafts",
        sql.insert({
          event_id: event.eventId,
          workspace_id: event.workspaceId,
          issue_id: event.issueId,
          assignee_id: event.assigneeId,
          title: after.title,
          status: after.status,
          occurred_at: event.occurredAt,
        }),
      );
  });

/**
 * A label membership. An existing row changes only when the event has a higher
 * sequence. MySQL has no `WHERE` on the update, so each column keeps its value
 * with `IF`. MySQL assigns left to right, so `sequence` must come last.
 */
const applyLabel = (sql: SqlClient.SqlClient, item: IssueLabelEvent) => {
  const row = sql.insert({
    workspace_id: item.workspaceId,
    membership_id: item.membershipId,
    issue_id: item.issueId,
    label_id: item.labelId,
    attached: item.type === "LabelAttached" ? 1 : 0,
    sequence: item.sequence,
    updated_at: item.occurredAt,
  });
  return sql.onDialectOrElse({
    mysql: () =>
      sql`INSERT INTO issue_labels ${row} ON DUPLICATE KEY UPDATE attached = IF(VALUES(sequence) > sequence, VALUES(attached), attached), updated_at = IF(VALUES(sequence) > sequence, VALUES(updated_at), updated_at), sequence = IF(VALUES(sequence) > sequence, VALUES(sequence), sequence)`,
    orElse: () =>
      sql`INSERT INTO issue_labels ${row} ON CONFLICT (workspace_id, membership_id) DO UPDATE SET attached = excluded.attached, sequence = excluded.sequence, updated_at = excluded.updated_at WHERE excluded.sequence > issue_labels.sequence`,
  });
};

/** The catalog tables and their key columns. Table and column names come only from here. */
const catalogTables = {
  projects: "project_id",
  users: "user_id",
  labels: "label_id",
} as const;

const deleteCatalog = (
  sql: SqlClient.SqlClient,
  table: keyof typeof catalogTables,
  workspaceId: string,
  key: string,
) =>
  sql`DELETE FROM ${sql(table)} WHERE workspace_id = ${workspaceId} AND ${sql(catalogTables[table])} = ${key}`;

const upsertCatalog = (
  sql: SqlClient.SqlClient,
  table: keyof typeof catalogTables,
  workspaceId: string,
  key: string,
  value: string,
) => {
  const idColumn = catalogTables[table];
  const row = sql.insert({ workspace_id: workspaceId, [idColumn]: key, value });
  return sql.onDialectOrElse({
    mysql: () =>
      sql`INSERT INTO ${sql(table)} ${row} ON DUPLICATE KEY UPDATE value = VALUES(value)`,
    orElse: () =>
      sql`INSERT INTO ${sql(table)} ${row} ON CONFLICT (workspace_id, ${sql(idColumn)}) DO UPDATE SET value = excluded.value`,
  });
};

export const issueRows = Projection.process({
  commit: Commit.transaction,
  id: "issue-rows",
  params: { workspaceId: Identifier },
  inputs: {
    events: Events,
    labelEvents: LabelEvents,
    projects: Projects,
    users: Users,
    labels: Labels,
  },
  process: Projection.each((entry, unit) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      switch (entry.input) {
        case "events":
          yield* applyIssue(sql, entry.item);
          break;
        case "labelEvents":
          yield* applyLabel(sql, entry.item);
          break;
        case "projects":
        case "users":
        case "labels": {
          const item = entry.item;
          const workspaceId = unit.params.workspaceId;
          if (!("value" in item)) {
            yield* deleteCatalog(sql, entry.input, workspaceId, item.key);
          } else {
            yield* checkCatalogWorkspace(workspaceId, item.value.workspaceId, item.type, item.key);
            yield* upsertCatalog(
              sql,
              entry.input,
              workspaceId,
              item.key,
              JSON.stringify(item.value),
            );
          }
          break;
        }
      }
    }),
  ),
});

export const decodeIssueRow = Schema.decodeUnknownSync(IssueRow);
