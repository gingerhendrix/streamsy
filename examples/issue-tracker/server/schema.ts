import { Effect } from "effect";
import { SqlClient } from "effect/unstable/sql";

const statements = [
  `CREATE TABLE IF NOT EXISTS issues (workspace_id TEXT NOT NULL, issue_id TEXT NOT NULL, project_id TEXT NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL, sequence INTEGER NOT NULL, updated_at TEXT NOT NULL, assignee_id TEXT, PRIMARY KEY(workspace_id, issue_id))`,
  `CREATE TABLE IF NOT EXISTS issue_labels (workspace_id TEXT NOT NULL, membership_id TEXT NOT NULL, issue_id TEXT NOT NULL, label_id TEXT NOT NULL, attached INTEGER NOT NULL, sequence INTEGER NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(workspace_id, membership_id))`,
  `CREATE TABLE IF NOT EXISTS projects (workspace_id TEXT NOT NULL, project_id TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(workspace_id, project_id))`,
  `CREATE TABLE IF NOT EXISTS users (workspace_id TEXT NOT NULL, user_id TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(workspace_id, user_id))`,
  `CREATE TABLE IF NOT EXISTS labels (workspace_id TEXT NOT NULL, label_id TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(workspace_id, label_id))`,
  `CREATE TABLE IF NOT EXISTS issue_changes (id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, issue_id TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE, event_type TEXT NOT NULL, sequence INTEGER NOT NULL, occurred_at TEXT NOT NULL, status TEXT NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS notification_drafts (event_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, issue_id TEXT NOT NULL, assignee_id TEXT NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL, occurred_at TEXT NOT NULL)`,
] as const;

export const prepareSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* Effect.forEach(statements, (statement) => sql.unsafe(statement), { discard: true });
});
