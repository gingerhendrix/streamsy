/**
 * Bun SQLite {@link StorageAdapter}.
 *
 * Opens (or adopts) a single `bun:sqlite` database, applies migrations, and owns
 * a per-id cache of private `SqliteStream` handles. The adapter is flat: every
 * per-stream method takes `streamId` and delegates to `state.getStream(id)`. It
 * also exposes the underlying `state` so callers/tests can reach the `Database`
 * or close it.
 */
import type { CreatePlan, DeletePlan, ForkPlan, StorageAdapter, StreamId } from "@streamsy/core";
import { recordToRow, rowToRecord, type StreamRow } from "./lib/codec.ts";
import { SqliteStreamState, type SqliteStreamStateOptions } from "./state.ts";
import { INSERT_SQL } from "./stores/record-store.ts";
import { runImmediateTransactionWithBusyRetry } from "./utils/transaction.ts";

export interface SqliteStorageAdapterOptions extends SqliteStreamStateOptions {
  /** Share an existing state instead of opening a new database. */
  state?: SqliteStreamState;
}

export interface SqliteStorageAdapter extends StorageAdapter {
  readonly state: SqliteStreamState;
  /** Close the underlying database connection. */
  close(): void;
}

export function createSqliteStorageAdapter(
  options: SqliteStorageAdapterOptions = {},
): SqliteStorageAdapter {
  const { state: existing, ...stateOptions } = options;
  const state = existing ?? new SqliteStreamState(stateOptions);

  return {
    state,
    getRecord: (streamId) => Promise.resolve(state.getStream(streamId).getRecord()),
    listMessages: (streamId, listOptions) =>
      Promise.resolve(state.getStream(streamId).listMessages(listOptions)),
    getProducerState: (streamId, producerId) =>
      Promise.resolve(state.getStream(streamId).getProducerState(producerId)),
    append: (streamId, plan) => state.getStream(streamId).append(plan),
    awaitChange: (streamId, awaitOptions) => state.getStream(streamId).awaitChange(awaitOptions),
    scheduleExpiry: (streamId, at) => state.getStream(streamId).scheduleExpiry(at),
    cancelExpiry: (streamId) => state.getStream(streamId).cancelExpiry(),
    async create(plan: CreatePlan) {
      const stream = state.getStream(plan.record.id);
      // `plan.record` is the single source of truth — a created-closed stream
      // arrives with `lifecycle.closed`/`closedAt` already folded in by core.
      const result = await stream.applyMutation({
        createRecord: plan.record,
        preconditions: {},
        messages: plan.initialMessages,
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
        if (inserted.changes === 0) {
          // `exists` carries the existing child so core can run its
          // config-match idempotency (identical racing forks are not conflicts).
          return {
            status: "exists" as const,
            record: readRecord(state, plan.child.id) ?? plan.child,
          };
        }

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

// `as const` keeps the `status` discriminants from widening to `string`; the
// inferred union is the adapter `DeleteResult`, validated by the flat seam.
function deleteInTransaction(state: SqliteStreamState, plan: DeletePlan) {
  const record = readRecord(state, plan.streamId);
  if (!record) return { status: "not-found" as const };
  if (plan.reason === "delete" && record.lifecycle.softDeleted) return { status: "gone" as const };

  if (countDependents(state, plan.streamId) > 0) {
    state.db.run("update streamsy_streams set soft_deleted = 1 where stream_id = ?", [
      plan.streamId,
    ]);
    // Surface the soft-delete transition to any parked live waiter on this stream.
    state.getExistingStream(plan.streamId)?.wake();
    return { status: "retained-soft-deleted" as const };
  }

  purgeAndCascadeInTransaction(state, record.id, record.lifecycle.forkedFrom);
  return { status: "purged" as const };
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
  // Wake parked live waiters before evicting the cached instance: their loop
  // still re-reads through it and observes the now-absent record (`!present`).
  state.getExistingStream(streamId)?.wake();
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
