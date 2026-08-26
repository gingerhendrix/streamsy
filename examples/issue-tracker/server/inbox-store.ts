/**
 * One user's inbox, as a service.
 *
 * A user partition owns exactly one of these, over exactly one connection, for
 * exactly one user — the same rule the workspace partition follows for its
 * `IssueStore` and `OutboxStore`. The partition key is therefore not a filter
 * the application remembers to apply: rows for another user are not in this
 * database at all.
 *
 * `upsert` is keyed by the derived `inboxId`, so applying the same source fact
 * twice writes the same row twice and produces one row. That is what lets the
 * exchange advance its cursor *after* it applies — the window between the two
 * is a repeat, never a duplicate.
 */
import { Database } from "bun:sqlite";
import { Context, Effect, Layer } from "effect";
import { compareInboxRows, decodeInboxRow, type InboxRow } from "../domain/inbox.ts";
import { InboxRestorePoison, InboxUnavailable } from "./domain-errors.ts";

export interface InboxStoreService {
  /**
   * Write rows for this partition's user.
   *
   * A row placed at another user is refused rather than written: the store is
   * the last boundary before durable state, and a mis-keyed row that reaches
   * disk is a leak no later filter can undo.
   */
  readonly upsert: (
    userId: string,
    rows: readonly InboxRow[],
  ) => Effect.Effect<number, InboxUnavailable>;
  readonly rows: (
    userId: string,
  ) => Effect.Effect<readonly InboxRow[], InboxUnavailable | InboxRestorePoison>;
}

export class InboxStore extends Context.Service<InboxStore, InboxStoreService>()(
  "issue-tracker/InboxStore",
) {}

const INBOX_SCHEMA = `CREATE TABLE IF NOT EXISTS inbox_rows (
  user_id  TEXT NOT NULL,
  inbox_id TEXT NOT NULL,
  value    TEXT NOT NULL,
  PRIMARY KEY (user_id, inbox_id)
);`;

interface InboxValueRow {
  readonly inbox_id: string;
  readonly value: string;
}

const misplaced = (userId: string, row: InboxRow): InboxUnavailable =>
  new InboxUnavailable({
    operation: "upsert",
    detail: `row ${row.inboxId} is placed at user ${row.userId}, not ${userId}`,
  });

/** The in-memory inbox. Same placement rule and same restore path as SQLite. */
export const inboxMemoryLayer = (): Layer.Layer<InboxStore> =>
  Layer.sync(InboxStore, () => {
    const stored = new Map<string, string>();
    return InboxStore.of({
      upsert: Effect.fn("InboxStore.upsert")(function* (userId: string, rows) {
        for (const row of rows) {
          if (row.userId !== userId) return yield* misplaced(userId, row);
        }
        return yield* Effect.sync(() => {
          for (const row of rows) stored.set(`${userId}\u0000${row.inboxId}`, JSON.stringify(row));
          return rows.length;
        });
      }),
      rows: Effect.fn("InboxStore.rows")(function* (userId: string) {
        const prefix = `${userId}\u0000`;
        const restored: InboxRow[] = [];
        for (const [key, value] of stored) {
          if (!key.startsWith(prefix)) continue;
          restored.push(yield* restore(userId, key.slice(prefix.length), value));
        }
        return restored.toSorted(compareInboxRows);
      }),
    });
  });

/** The durable inbox, one file per user partition. */
export const inboxSqliteLayer = (options: { readonly filename: string }): Layer.Layer<InboxStore> =>
  Layer.effect(
    InboxStore,
    Effect.acquireRelease(
      Effect.sync(() => {
        const database = new Database(options.filename, { create: true });
        database.exec("PRAGMA journal_mode = WAL");
        database.exec(INBOX_SCHEMA);
        return database;
      }),
      (database) => Effect.sync(() => database.close(false)),
    ).pipe(Effect.map(inboxService)),
  );

function inboxService(database: Database): InboxStoreService {
  const upsertRow = database.query<never, [string, string, string]>(
    "INSERT INTO inbox_rows (user_id, inbox_id, value) VALUES (?, ?, ?)" +
      " ON CONFLICT (user_id, inbox_id) DO UPDATE SET value = excluded.value",
  );
  const selectRows = database.query<InboxValueRow, [string]>(
    "SELECT inbox_id, value FROM inbox_rows WHERE user_id = ? ORDER BY inbox_id",
  );
  // One transaction per pass, so a partition that dies mid-apply leaves the
  // inbox at a row boundary the cursor can be replayed against.
  const applyRows = database.transaction((userId: string, rows: readonly InboxRow[]) => {
    for (const row of rows) upsertRow.run(userId, row.inboxId, JSON.stringify(row));
  });

  return InboxStore.of({
    upsert: Effect.fn("InboxStore.upsert")(function* (userId: string, rows) {
      for (const row of rows) {
        if (row.userId !== userId) return yield* misplaced(userId, row);
      }
      yield* Effect.try({
        try: () => applyRows(userId, rows),
        catch: (cause) => new InboxUnavailable({ operation: "upsert", detail: describe(cause) }),
      });
      return rows.length;
    }),
    rows: Effect.fn("InboxStore.rows")(function* (userId: string) {
      const found = yield* Effect.try({
        try: () => selectRows.all(userId),
        catch: (cause) => new InboxUnavailable({ operation: "rows", detail: describe(cause) }),
      });
      const restored: InboxRow[] = [];
      for (const row of found) restored.push(yield* restore(userId, row.inbox_id, row.value));
      return restored.toSorted(compareInboxRows);
    }),
  });
}

const restore = (userId: string, key: string, value: string) =>
  Effect.try({
    try: () => decodeInboxRow(JSON.parse(value)),
    catch: (cause) => new InboxRestorePoison({ userId, key, detail: describe(cause) }),
  });

function describe(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}
