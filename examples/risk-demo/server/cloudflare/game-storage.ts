/* oxlint-disable effecttsgo/async-function -- Cloudflare Durable Object storage and fetch handlers are Promise-native platform adapters over the shared game services. */
/* oxlint-disable effecttsgo/global-timers, effecttsgo/new-promise -- Cloudflare's storage subscription adapter is callback/Promise-native and owns timer cancellation at this platform boundary. */
import {
  runAwaitChangeLoop,
  type AppendPlan,
  type AwaitChangeOptions,
  type AwaitChangeResult,
  type DeletePlan,
  type ForkPlan,
  type ListMessagesOptions,
  type ProducerState,
  type StorageAdapter,
  type StoredMessage,
  type StreamRecord,
} from "@streamsy/core";
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
type StreamRecordSchemaType = typeof StreamRecordSchema.Type;
const streamRecordSchemaInput = (record: StreamRecord): StreamRecordSchemaType => record;
const decodeStreamRecord = Schema.decodeUnknownSync(Schema.fromJsonString(StreamRecordSchema));

const SCHEMA = [
  `create table if not exists risk_streams (
    stream_id text primary key,
    record_json text not null
  )`,
  `create table if not exists risk_messages (
    stream_id text not null,
    offset text not null,
    timestamp integer not null,
    data blob not null,
    primary key (stream_id, offset)
  )`,
  `create table if not exists risk_producers (
    stream_id text not null,
    producer_id text not null,
    epoch integer not null,
    last_seq integer not null,
    primary key (stream_id, producer_id)
  )`,
  `create index if not exists risk_stream_parent on risk_streams(
    json_extract(record_json, '$.lifecycle.forkedFrom')
  )`,
];

interface RecordRow {
  [key: string]: SqlStorageValue;
  record_json: string;
}

interface MessageRow {
  [key: string]: SqlStorageValue;
  offset: string;
  timestamp: number;
  data: ArrayBuffer;
}

interface ProducerRow {
  [key: string]: SqlStorageValue;
  epoch: number;
  last_seq: number;
}

type FailureReason = "offset" | "closed" | "producer";

/**
 * Streamsy storage whose entire namespace is one game Durable Object's SQLite
 * database. Unlike @streamsy/storage-durable-object, this adapter never routes
 * by stream id: canonical events, projections, turns and metadata remain in the
 * already-selected game actor.
 */
export function createGameStorageAdapter(
  storage: DurableObjectStorage,
  onExpiryChange?: () => void,
): StorageAdapter {
  const sql = storage.sql;
  const waiters = new Set<() => void>();
  for (const statement of SCHEMA) sql.exec(statement);

  const readRecord = (streamId: string): StreamRecord | null => {
    const row = [
      ...sql.exec<RecordRow>("select record_json from risk_streams where stream_id = ?", streamId),
    ][0];
    return row ? streamRecordSchemaInput(decodeStreamRecord(row.record_json)) : null;
  };

  const putRecord = (record: StreamRecord): void => {
    sql.exec(
      `insert into risk_streams (stream_id, record_json) values (?, ?)
       on conflict(stream_id) do update set record_json = excluded.record_json`,
      record.id,
      JSON.stringify(record),
    );
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

  const producer = (streamId: string, producerId: string): ProducerState | undefined => {
    const row = [
      ...sql.exec<ProducerRow>(
        "select epoch, last_seq from risk_producers where stream_id = ? and producer_id = ?",
        streamId,
        producerId,
      ),
    ][0];
    return row ? { epoch: row.epoch, lastSeq: row.last_seq } : undefined;
  };

  const reasonFor = (
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
    const expectedProducer = preconditions.producer;
    if (expectedProducer) {
      const current = producer(record.id, expectedProducer.producerId);
      const expected = expectedProducer.expected;
      if (
        current?.epoch !== expected?.epoch ||
        current?.lastSeq !== expected?.lastSeq ||
        (!current && expected) ||
        (current && !expected)
      )
        return "producer";
    }
    return undefined;
  };

  const writeMessages = (streamId: string, messages: StoredMessage[] | undefined): void => {
    for (const message of messages ?? []) {
      const data = message.data.buffer.slice(
        message.data.byteOffset,
        message.data.byteOffset + message.data.byteLength,
      );
      sql.exec(
        "insert into risk_messages (stream_id, offset, timestamp, data) values (?, ?, ?, ?)",
        streamId,
        message.offset,
        message.timestamp,
        data,
      );
    }
  };

  const adapter: StorageAdapter = {
    getRecord: async (streamId) => readRecord(streamId),
    async listMessages(streamId, options: ListMessagesOptions = {}) {
      const clauses = ["stream_id = ?"];
      const params: (string | number)[] = [streamId];
      if (options.after !== undefined) {
        clauses.push("offset > ?");
        params.push(options.after);
      }
      if (options.until !== undefined) {
        clauses.push("offset <= ?");
        params.push(options.until);
      }
      let statement = `select offset, timestamp, data from risk_messages
        where ${clauses.join(" and ")} order by offset asc`;
      if (options.limit !== undefined) {
        statement += " limit ?";
        params.push(options.limit);
      }
      return [...sql.exec<MessageRow>(statement, ...params)].map((row) => ({
        offset: row.offset,
        timestamp: row.timestamp,
        data: new Uint8Array(row.data),
      }));
    },
    getProducerState: async (streamId, producerId) => producer(streamId, producerId),
    async append(streamId, plan) {
      const outcome = storage.transactionSync(() => {
        const record = readRecord(streamId);
        if (!record) return { record: null, reason: "offset" as const };
        const reason = reasonFor(record, plan.preconditions);
        if (reason) return { record, reason };
        const updated: StreamRecord = {
          ...record,
          config: { ...record.config, ...plan.recordPatch.config },
          lifecycle: { ...record.lifecycle, ...plan.recordPatch.lifecycle },
          currentOffset: plan.recordPatch.currentOffset ?? record.currentOffset,
          counter: plan.recordPatch.counter ?? record.counter,
        };
        putRecord(updated);
        const nextProducer = plan.preconditions.producer;
        if (nextProducer) {
          sql.exec(
            `insert into risk_producers (stream_id, producer_id, epoch, last_seq)
             values (?, ?, ?, ?)
             on conflict(stream_id, producer_id) do update
             set epoch = excluded.epoch, last_seq = excluded.last_seq`,
            streamId,
            nextProducer.producerId,
            nextProducer.next.epoch,
            nextProducer.next.lastSeq,
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
      wake();
      return { status: "appended", record: outcome.record };
    },
    async create(plan) {
      const created = storage.transactionSync(() => {
        const existing = readRecord(plan.record.id);
        if (existing) return { existing };
        putRecord(plan.record);
        writeMessages(plan.record.id, plan.initialMessages);
        return { existing: null };
      });
      if (created.existing) return { status: "exists", record: created.existing };
      wake();
      return { status: "created", record: plan.record };
    },
    async fork(plan: ForkPlan) {
      const result = storage.transactionSync(() => {
        const source = readRecord(plan.sourceId);
        if (
          !source ||
          source.lifecycle.softDeleted ||
          source.currentOffset < plan.precondition.sourceLiveAtOffset
        )
          return { status: "fork-source-gone" as const };
        const existing = readRecord(plan.child.id);
        if (existing) return { status: "exists" as const, record: existing };
        putRecord(plan.child);
        writeMessages(plan.child.id, plan.initialMessages);
        return { status: "created" as const, record: plan.child };
      });
      wake();
      return result;
    },
    async delete(plan: DeletePlan) {
      const result = storage.transactionSync(() => {
        const record = readRecord(plan.streamId);
        if (!record) return { status: "not-found" as const };
        if (plan.reason === "delete" && record.lifecycle.softDeleted)
          return { status: "gone" as const };
        const dependent = [
          ...sql.exec<{ count: number }>(
            `select count(*) as count from risk_streams
           where json_extract(record_json, '$.lifecycle.forkedFrom') = ?`,
            plan.streamId,
          ),
        ][0]?.count;
        if ((dependent ?? 0) > 0) {
          putRecord({ ...record, lifecycle: { ...record.lifecycle, softDeleted: true } });
          return { status: "retained-soft-deleted" as const };
        }
        sql.exec("delete from risk_messages where stream_id = ?", plan.streamId);
        sql.exec("delete from risk_producers where stream_id = ?", plan.streamId);
        sql.exec("delete from risk_streams where stream_id = ?", plan.streamId);
        return { status: "purged" as const };
      });
      wake();
      return result;
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
    scheduleExpiry: async (_streamId, _at) => onExpiryChange?.(),
    cancelExpiry: async (_streamId) => onExpiryChange?.(),
  };

  return adapter;
}
