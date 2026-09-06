/* oxlint-disable effecttsgo/async-function -- StorageAdapter is the Promise-native Streamsy platform seam. */
/* oxlint-disable effecttsgo/global-timers, effecttsgo/new-promise -- Durable-stream live waits bridge the Promise-native adapter seam. */
import type {
  AppendPlan,
  AwaitChangeOptions,
  AwaitChangeResult,
  DeletePlan,
  ListMessagesOptions,
  ProducerState,
  StorageAdapter,
  StorageDeleteResult,
  StoredMessage,
  StreamRecord,
} from "@streamsy/core";
import { runAwaitChangeLoop } from "@streamsy/core";
import type { DurableObjectStorage, SqlStorageValue } from "@cloudflare/workers-types";
import { Schema } from "effect";

const StreamRecordSchema = Schema.Struct({
  id: Schema.String,
  config: Schema.Struct({
    contentType: Schema.String,
    ttlSeconds: Schema.optionalKey(Schema.Finite),
    expiresAt: Schema.optionalKey(Schema.String),
    createdAt: Schema.Finite,
  }),
  lifecycle: Schema.Struct({
    lastSeq: Schema.optionalKey(Schema.String),
    closed: Schema.optionalKey(Schema.Boolean),
    closedAt: Schema.optionalKey(Schema.Finite),
    forkedFrom: Schema.optionalKey(Schema.String),
    forkOffset: Schema.optionalKey(Schema.String),
    forkSubOffset: Schema.optionalKey(Schema.Finite),
    softDeleted: Schema.optionalKey(Schema.Boolean),
    expiresAtMs: Schema.optionalKey(Schema.Finite),
  }),
  currentOffset: Schema.String,
  counter: Schema.Finite,
});
type DecodedStreamRecord = typeof StreamRecordSchema.Type;
const checkedRecord = (record: StreamRecord): DecodedStreamRecord => record;
const decodeRecord = Schema.decodeUnknownSync(Schema.fromJsonString(StreamRecordSchema));

const STREAM_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS issue_tracker_streams (
    stream_id TEXT PRIMARY KEY,
    record_json TEXT NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS issue_tracker_messages (
    stream_id TEXT NOT NULL,
    offset TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (stream_id, offset),
    FOREIGN KEY (stream_id) REFERENCES issue_tracker_streams(stream_id) ON DELETE CASCADE
  )`,
  `CREATE TABLE IF NOT EXISTS issue_tracker_producers (
    stream_id TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    last_seq INTEGER NOT NULL,
    PRIMARY KEY (stream_id, producer_id),
    FOREIGN KEY (stream_id) REFERENCES issue_tracker_streams(stream_id) ON DELETE CASCADE
  )`,
  `CREATE TABLE IF NOT EXISTS issue_tracker_stream_expiries (
    stream_id TEXT PRIMARY KEY,
    expires_at_ms INTEGER NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES issue_tracker_streams(stream_id) ON DELETE CASCADE
  )`,
  `CREATE INDEX IF NOT EXISTS issue_tracker_stream_expiries_due
    ON issue_tracker_stream_expiries (expires_at_ms, stream_id)`,
] as const;

interface RecordRow extends Record<string, SqlStorageValue> {
  readonly record_json: string;
}
interface MessageRow extends Record<string, SqlStorageValue> {
  readonly offset: string;
  readonly timestamp: number;
  readonly data: ArrayBuffer;
}
interface ProducerRow extends Record<string, SqlStorageValue> {
  readonly epoch: number;
  readonly last_seq: number;
}
interface ExpiryRow extends Record<string, SqlStorageValue> {
  readonly stream_id: string;
  readonly expires_at_ms: number;
}
type FailureReason = "offset" | "closed" | "producer";

export interface WorkspaceStreamStorage {
  readonly adapter: StorageAdapter;
  readonly dueExpiryStreamIds: (nowMs: number, limit: number) => readonly string[];
  readonly expireStreamIfDue: (streamId: string, nowMs: number) => boolean;
  readonly nextExpiryAt: () => number | undefined;
}

export interface WorkspaceStreamStorageOptions {
  readonly afterCreateCommit?: () => Promise<void>;
  readonly beforeAppendCommit?: () => Promise<void>;
  readonly afterAppendCommit?: () => Promise<void>;
  readonly beforeExpiryDeleteCommit?: () => Promise<void>;
  readonly beforeCancelExpiry?: () => Promise<void>;
}

/** All streams selected for one workspace stay inside that workspace object. */
export function createWorkspaceStreamStorage(
  storage: DurableObjectStorage,
  hooks: WorkspaceStreamStorageOptions = {},
): WorkspaceStreamStorage {
  const sql = storage.sql;
  const waiters = new Set<() => void>();
  for (const statement of STREAM_SCHEMA) sql.exec(statement);

  const readRecord = (streamId: string): StreamRecord | null => {
    const row = [
      ...sql.exec<RecordRow>(
        "SELECT record_json FROM issue_tracker_streams WHERE stream_id = ?",
        streamId,
      ),
    ][0];
    return row === undefined ? null : checkedRecord(decodeRecord(row.record_json));
  };
  const putRecord = (record: StreamRecord): void => {
    sql.exec(
      `INSERT INTO issue_tracker_streams(stream_id, record_json) VALUES (?, ?)
       ON CONFLICT(stream_id) DO UPDATE SET record_json = excluded.record_json`,
      record.id,
      JSON.stringify(record),
    );
  };
  const producer = (streamId: string, producerId: string): ProducerState | undefined => {
    const row = [
      ...sql.exec<ProducerRow>(
        "SELECT epoch, last_seq FROM issue_tracker_producers WHERE stream_id = ? AND producer_id = ?",
        streamId,
        producerId,
      ),
    ][0];
    return row === undefined ? undefined : { epoch: row.epoch, lastSeq: row.last_seq };
  };
  const preconditionFailure = (
    record: StreamRecord,
    preconditions: AppendPlan["preconditions"],
  ): FailureReason | undefined => {
    if (
      preconditions.expectedOffset !== undefined &&
      preconditions.expectedOffset !== record.currentOffset
    )
      return "offset";
    if (
      preconditions.expectedClosed !== undefined &&
      preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return "closed";
    const lane = preconditions.producer;
    if (lane !== undefined) {
      const current = producer(record.id, lane.producerId);
      if (
        current?.epoch !== lane.expected?.epoch ||
        current?.lastSeq !== lane.expected?.lastSeq ||
        (current === undefined) !== (lane.expected === undefined)
      )
        return "producer";
    }
    return undefined;
  };
  const writeMessages = (
    streamId: string,
    messages: readonly StoredMessage[] | undefined,
  ): void => {
    for (const message of messages ?? []) {
      const data = message.data.buffer.slice(
        message.data.byteOffset,
        message.data.byteOffset + message.data.byteLength,
      );
      sql.exec(
        "INSERT INTO issue_tracker_messages(stream_id, offset, timestamp, data) VALUES (?, ?, ?, ?)",
        streamId,
        message.offset,
        message.timestamp,
        data,
      );
    }
  };
  const wake = (): void => {
    for (const waiter of waiters) waiter();
    waiters.clear();
  };
  const waitForWake = (timeoutMs: number): Promise<void> =>
    new Promise((resolve) => {
      let settled = false;
      const done = () => {
        if (settled) return;
        settled = true;
        waiters.delete(done);
        clearTimeout(timeout);
        resolve();
      };
      const timeout = setTimeout(done, Math.max(0, timeoutMs));
      waiters.add(done);
    });

  const deleteStream = (plan: DeletePlan, dueAtOrBefore?: number): StorageDeleteResult["status"] =>
    storage.transactionSync(() => {
      const record = readRecord(plan.streamId);
      if (record === null) return "not-found";
      if (plan.reason === "expiry") {
        const row = [
          ...sql.exec<ExpiryRow>(
            "SELECT stream_id, expires_at_ms FROM issue_tracker_stream_expiries WHERE stream_id = ?",
            plan.streamId,
          ),
        ][0];
        if (
          row === undefined ||
          row.expires_at_ms !== plan.expectedExpiresAtMs ||
          record.lifecycle.expiresAtMs !== plan.expectedExpiresAtMs ||
          (dueAtOrBefore !== undefined && row.expires_at_ms > dueAtOrBefore)
        ) {
          return "expiry-mismatch";
        }
      } else if (record.lifecycle.softDeleted === true) {
        return "gone";
      }
      sql.exec("DELETE FROM issue_tracker_messages WHERE stream_id = ?", plan.streamId);
      sql.exec("DELETE FROM issue_tracker_producers WHERE stream_id = ?", plan.streamId);
      sql.exec("DELETE FROM issue_tracker_stream_expiries WHERE stream_id = ?", plan.streamId);
      sql.exec("DELETE FROM issue_tracker_streams WHERE stream_id = ?", plan.streamId);
      return "purged";
    });

  const adapter: StorageAdapter = {
    getRecord: async (streamId) => readRecord(streamId),
    async listMessages(streamId, options: ListMessagesOptions = {}) {
      const clauses = ["stream_id = ?"];
      const parameters: Array<string | number> = [streamId];
      if (options.after !== undefined) {
        clauses.push("offset > ?");
        parameters.push(options.after);
      }
      if (options.until !== undefined) {
        clauses.push("offset <= ?");
        parameters.push(options.until);
      }
      let statement = `SELECT offset, timestamp, data FROM issue_tracker_messages
        WHERE ${clauses.join(" AND ")} ORDER BY offset`;
      if (options.limit !== undefined) {
        statement += " LIMIT ?";
        parameters.push(options.limit);
      }
      return [...sql.exec<MessageRow>(statement, ...parameters)].map((row) => ({
        offset: row.offset,
        timestamp: row.timestamp,
        data: new Uint8Array(row.data),
      }));
    },
    getProducerState: async (streamId, producerId) => producer(streamId, producerId),
    async append(streamId, plan) {
      await hooks.beforeAppendCommit?.();
      const outcome = storage.transactionSync(() => {
        const record = readRecord(streamId);
        if (record === null) return { record: null, reason: "offset" as const };
        const reason = preconditionFailure(record, plan.preconditions);
        if (reason !== undefined) return { record, reason };
        const updated: StreamRecord = {
          ...record,
          config: { ...record.config, ...plan.recordPatch.config },
          lifecycle: { ...record.lifecycle, ...plan.recordPatch.lifecycle },
          currentOffset: plan.recordPatch.currentOffset ?? record.currentOffset,
          counter: plan.recordPatch.counter ?? record.counter,
        };
        putRecord(updated);
        const expiresAtMs = updated.lifecycle.expiresAtMs;
        if (expiresAtMs === undefined) {
          sql.exec("DELETE FROM issue_tracker_stream_expiries WHERE stream_id = ?", streamId);
        } else {
          sql.exec(
            `INSERT INTO issue_tracker_stream_expiries(stream_id, expires_at_ms) VALUES (?, ?)
             ON CONFLICT(stream_id) DO UPDATE SET expires_at_ms = excluded.expires_at_ms`,
            streamId,
            expiresAtMs,
          );
        }
        const lane = plan.preconditions.producer;
        if (lane !== undefined) {
          sql.exec(
            `INSERT INTO issue_tracker_producers(stream_id, producer_id, epoch, last_seq)
             VALUES (?, ?, ?, ?) ON CONFLICT(stream_id, producer_id) DO UPDATE
             SET epoch = excluded.epoch, last_seq = excluded.last_seq`,
            streamId,
            lane.producerId,
            lane.next.epoch,
            lane.next.lastSeq,
          );
        }
        writeMessages(streamId, plan.messages);
        return { record: updated };
      });
      if ("reason" in outcome) {
        return {
          status: "precondition-failed",
          record: outcome.record,
          reason: outcome.reason ?? "offset",
        };
      }
      await hooks.afterAppendCommit?.();
      wake();
      return { status: "appended", record: outcome.record };
    },
    async create(plan) {
      const outcome = storage.transactionSync(() => {
        const existing = readRecord(plan.record.id);
        if (existing !== null) return { existing };
        putRecord(plan.record);
        writeMessages(plan.record.id, plan.initialMessages);
        const expiresAtMs = plan.record.lifecycle.expiresAtMs;
        if (expiresAtMs !== undefined) {
          sql.exec(
            "INSERT INTO issue_tracker_stream_expiries(stream_id, expires_at_ms) VALUES (?, ?)",
            plan.record.id,
            expiresAtMs,
          );
        }
        return { existing: null };
      });
      if (outcome.existing !== null) return { status: "exists", record: outcome.existing };
      await hooks.afterCreateCommit?.();
      wake();
      return { status: "created", record: plan.record };
    },
    async delete(plan: DeletePlan) {
      if (plan.reason === "expiry") await hooks.beforeExpiryDeleteCommit?.();
      const status = deleteStream(plan);
      if (status === "purged") wake();
      return { status };
    },
    awaitChange(streamId, options: AwaitChangeOptions): Promise<AwaitChangeResult> {
      return runAwaitChangeLoop(
        {
          readRecord: () => readRecord(streamId),
          waitForWake,
          totalCapMs: 30_000,
          parkCapMs: 1_000,
        },
        options,
      );
    },
    scheduleExpiry: async (streamId, at) => {
      storage.transactionSync(() => {
        if (readRecord(streamId)?.lifecycle.expiresAtMs !== at) return;
        sql.exec(
          `INSERT INTO issue_tracker_stream_expiries(stream_id, expires_at_ms) VALUES (?, ?)
           ON CONFLICT(stream_id) DO UPDATE SET expires_at_ms = excluded.expires_at_ms`,
          streamId,
          at,
        );
      });
    },
    cancelExpiry: async (streamId) => {
      await hooks.beforeCancelExpiry?.();
      storage.transactionSync(() => {
        if (readRecord(streamId) !== null) return;
        sql.exec("DELETE FROM issue_tracker_stream_expiries WHERE stream_id = ?", streamId);
      });
    },
  };
  return {
    adapter,
    dueExpiryStreamIds: (nowMs, limit) =>
      [
        ...sql.exec<ExpiryRow>(
          "SELECT stream_id, expires_at_ms FROM issue_tracker_stream_expiries" +
            " WHERE expires_at_ms <= ? ORDER BY expires_at_ms, stream_id LIMIT ?",
          nowMs,
          limit,
        ),
      ].map((row) => row.stream_id),
    expireStreamIfDue: (streamId, nowMs) => {
      const row = [
        ...sql.exec<ExpiryRow>(
          "SELECT stream_id, expires_at_ms FROM issue_tracker_stream_expiries WHERE stream_id = ?",
          streamId,
        ),
      ][0];
      if (row === undefined) return false;
      const purged =
        deleteStream(
          { streamId, reason: "expiry", expectedExpiresAtMs: row.expires_at_ms },
          nowMs,
        ) === "purged";
      if (purged) wake();
      return purged;
    },
    nextExpiryAt: () =>
      [
        ...sql.exec<ExpiryRow>(
          "SELECT stream_id, expires_at_ms FROM issue_tracker_stream_expiries" +
            " ORDER BY expires_at_ms, stream_id LIMIT 1",
        ),
      ][0]?.expires_at_ms,
  };
}
