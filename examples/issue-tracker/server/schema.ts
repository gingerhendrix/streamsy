import { Effect } from "effect";
import { SqlClient } from "effect/sql";

/** The application tables, in one dialect. */
interface Columns {
  /** An identifier (64 characters at most, see `Identifier`) that is part of a key. */
  readonly id: string;
  /** A membership id (160 characters at most, see `MembershipId`) that is part of a key. */
  readonly membership: string;
  /** A short value that is not part of a key: status, event type, timestamp. */
  readonly short: string;
  readonly text: string;
  readonly integer: string;
  /** The generated key of `issue_changes`. */
  readonly identity: string;
}

const tables = (c: Columns) => [
  `CREATE TABLE IF NOT EXISTS issues (workspace_id ${c.id} NOT NULL, issue_id ${c.id} NOT NULL, project_id ${c.id} NOT NULL, title ${c.text} NOT NULL, status ${c.short} NOT NULL, sequence ${c.integer} NOT NULL, updated_at ${c.short} NOT NULL, assignee_id ${c.id}, PRIMARY KEY(workspace_id, issue_id))`,
  `CREATE TABLE IF NOT EXISTS issue_labels (workspace_id ${c.id} NOT NULL, membership_id ${c.membership} NOT NULL, issue_id ${c.id} NOT NULL, label_id ${c.id} NOT NULL, attached ${c.integer} NOT NULL, sequence ${c.integer} NOT NULL, updated_at ${c.short} NOT NULL, PRIMARY KEY(workspace_id, membership_id))`,
  `CREATE TABLE IF NOT EXISTS projects (workspace_id ${c.id} NOT NULL, project_id ${c.id} NOT NULL, value ${c.text} NOT NULL, PRIMARY KEY(workspace_id, project_id))`,
  `CREATE TABLE IF NOT EXISTS users (workspace_id ${c.id} NOT NULL, user_id ${c.id} NOT NULL, value ${c.text} NOT NULL, PRIMARY KEY(workspace_id, user_id))`,
  `CREATE TABLE IF NOT EXISTS labels (workspace_id ${c.id} NOT NULL, label_id ${c.id} NOT NULL, value ${c.text} NOT NULL, PRIMARY KEY(workspace_id, label_id))`,
  `CREATE TABLE IF NOT EXISTS issue_changes (id ${c.identity}, workspace_id ${c.id} NOT NULL, issue_id ${c.id} NOT NULL, event_id ${c.id} NOT NULL UNIQUE, event_type ${c.short} NOT NULL, sequence ${c.integer} NOT NULL, occurred_at ${c.short} NOT NULL, status ${c.short} NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS notification_drafts (event_id ${c.id} PRIMARY KEY, workspace_id ${c.id} NOT NULL, issue_id ${c.id} NOT NULL, assignee_id ${c.id} NOT NULL, title ${c.text} NOT NULL, status ${c.short} NOT NULL, occurred_at ${c.short} NOT NULL)`,
];

// MySQL cannot index TEXT, so keys are VARCHAR. The binary collation keeps keys
// that differ by case or trailing spaces distinct, as on SQLite and Postgres.
const mysqlKey = (length: number) =>
  `VARCHAR(${length}) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin`;

const statements = {
  sqlite: tables({
    id: "TEXT",
    membership: "TEXT",
    short: "TEXT",
    text: "TEXT",
    integer: "INTEGER",
    identity: "INTEGER PRIMARY KEY AUTOINCREMENT",
  }),
  pg: tables({
    id: "TEXT",
    membership: "TEXT",
    short: "TEXT",
    text: "TEXT",
    integer: "INTEGER",
    identity: "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY",
  }),
  mysql: tables({
    id: mysqlKey(64),
    membership: mysqlKey(160),
    short: "VARCHAR(64)",
    text: "TEXT",
    integer: "INT",
    identity: "BIGINT AUTO_INCREMENT PRIMARY KEY",
  }),
} as const;

/**
 * Creates the application tables. On MySQL, DDL commits any open transaction,
 * so run this at startup, outside a transaction.
 */
export const prepareSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const dialect = sql.onDialectOrElse({
    pg: () => "pg" as const,
    mysql: () => "mysql" as const,
    orElse: () => "sqlite" as const,
  });
  yield* Effect.forEach(statements[dialect], (statement) => sql.unsafe(statement), {
    discard: true,
  });
});
