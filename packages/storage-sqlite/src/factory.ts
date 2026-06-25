/**
 * Bun SQLite `StreamFactory`.
 *
 * Opens (or adopts) a single `bun:sqlite` database, applies migrations, and
 * returns protocol-facing `SqliteStream` instances bound to one id each. The
 * returned factory also exposes the underlying `state` so callers/tests can
 * reach the `Database` or close it.
 */
import type {
  CreatePlan,
  DeleteCommit,
  DeletePlan,
  ForkPlan,
  Stream,
  StreamFactory,
  StreamId,
} from "@streamsy/core";
import { recordToRow, rowToRecord, type StreamRow } from "./lib/codec.ts";
import { SqliteStreamState, type SqliteStreamStateOptions } from "./state.ts";
import { INSERT_SQL } from "./stores/record-store.ts";
import { runImmediateTransactionWithBusyRetry } from "./utils/transaction.ts";

export interface SqliteStreamFactoryOptions extends SqliteStreamStateOptions {
  /** Share an existing state instead of opening a new database. */
  state?: SqliteStreamState;
}

export interface SqliteStreamFactory extends StreamFactory {
  readonly state: SqliteStreamState;
  /** Close the underlying database connection. */
  close(): void;
}

export function createSqliteStreamFactory(
  options: SqliteStreamFactoryOptions = {},
): SqliteStreamFactory {
  const { state: existing, ...stateOptions } = options;
  const state = existing ?? new SqliteStreamState(stateOptions);

  return {
    state,
    async getStream(streamId: StreamId): Promise<Stream> {
      return state.getStream(streamId);
    },
    async create(plan: CreatePlan) {
      const stream = state.getStream(plan.record.id);
      const closedAt = plan.record.lifecycle.closedAt;
      const result = await stream.commit({
        createRecord: plan.record,
        preconditions: {},
        appendMessages: plan.initialMessages,
        recordPatch: plan.closeAfter
          ? { lifecycle: { closed: true, ...(closedAt !== undefined ? { closedAt } : {}) } }
          : undefined,
      });
      if (result.status === "committed") return { status: "created", record: result.record };
      return { status: "exists", record: result.record ?? plan.record };
    },
    async fork(plan: ForkPlan) {
      return runImmediateTransactionWithBusyRetry(state.db, () => {
        // Durable Streams offsets are protocol-defined fixed-width sortable
        // strings, so SQL lexical ordering is the required liveness check here.
        const source = state.db
          .query<{ current_offset: string }, [StreamId, string]>(
            `select current_offset from streamsy_streams
             where stream_id = ? and soft_deleted = 0 and current_offset >= ?`,
          )
          .get(plan.sourceId, plan.precondition.sourceLiveAtOffset);
        if (!source) return { status: "fork-source-gone" as const };

        const inserted = state.db.run(INSERT_SQL, recordToRow(plan.child));
        if (inserted.changes === 0) return { status: "exists" as const };

        if (plan.initialMessages && plan.initialMessages.length > 0) {
          const insertMessage = state.db.query(
            "insert into streamsy_messages (stream_id, offset, timestamp, data) values (?, ?, ?, ?)",
          );
          for (const message of plan.initialMessages) {
            insertMessage.run(plan.child.id, message.offset, message.timestamp, message.data);
          }
        }

        return { status: "created" as const, record: plan.child };
      });
    },
    delete(plan: DeletePlan) {
      return runImmediateTransactionWithBusyRetry(state.db, () => deleteInTransaction(state, plan));
    },
    close() {
      state.close();
    },
  };
}

function deleteInTransaction(state: SqliteStreamState, plan: DeletePlan): DeleteCommit {
  const record = readRecord(state, plan.streamId);
  if (!record) return { status: "not-found" };
  if (plan.reason === "delete" && record.lifecycle.softDeleted) return { status: "gone" };

  if (countDependents(state, plan.streamId) > 0) {
    state.db.run("update streamsy_streams set soft_deleted = 1 where stream_id = ?", [
      plan.streamId,
    ]);
    return { status: "retained-soft-deleted" };
  }

  purgeAndCascadeInTransaction(state, record.id, record.lifecycle.forkedFrom);
  return { status: "purged" };
}

function purgeAndCascadeInTransaction(
  state: SqliteStreamState,
  streamId: StreamId,
  forkedFrom: StreamId | undefined,
): void {
  purgeStream(state, streamId);

  let parentId = forkedFrom;
  while (parentId) {
    const parent = readRecord(state, parentId);
    if (!parent || parent.lifecycle.softDeleted !== true) return;
    if (countDependents(state, parentId) > 0) return;

    purgeStream(state, parent.id);
    parentId = parent.lifecycle.forkedFrom;
  }
}

function purgeStream(state: SqliteStreamState, streamId: StreamId): void {
  state.db.run("delete from streamsy_messages where stream_id = ?", [streamId]);
  state.db.run("delete from streamsy_producers where stream_id = ?", [streamId]);
  state.db.run("delete from streamsy_streams where stream_id = ?", [streamId]);
  state.deleteFromCache(streamId);
}

function countDependents(state: SqliteStreamState, parent: StreamId): number {
  const row = state.db
    .query<{ count: number }, [StreamId]>(
      "select count(*) as count from streamsy_streams where forked_from = ?",
    )
    .get(parent);
  return row?.count ?? 0;
}

function readRecord(state: SqliteStreamState, id: StreamId) {
  const row =
    state.db
      .query<StreamRow, [StreamId]>("select * from streamsy_streams where stream_id = ?")
      .get(id) ?? null;
  return row ? rowToRecord(row) : null;
}
