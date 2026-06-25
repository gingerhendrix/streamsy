import type { Database } from "bun:sqlite";
import type {
  CommitResult,
  ListMessagesOptions,
  MutationPlan,
  ProducerState,
  StoredMessage,
  Stream,
  StreamEventType,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
  WaitForEventOptions,
  WaitForEventResult,
} from "@streamsy/core";
import { recordToRow, rowToRecord, STREAM_COLUMNS, type StreamRow } from "./lib/codec.ts";
import { MessageStore } from "./stores/message-store.ts";
import { ProducerStore } from "./stores/producer-store.ts";
import { INSERT_SQL, RecordStore } from "./stores/record-store.ts";
import { StreamNotifier } from "./utils/notifier.ts";
import { TimeoutScheduler } from "./utils/timeout-scheduler.ts";
import {
  runImmediateTransactionWithBusyRetry,
  SqliteBusyRetryExhausted,
} from "./utils/transaction.ts";

/**
 * SQLite-backed protocol stream bound to one id. Record/message/producer state
 * is persisted in the shared database; the live-read notifier and expiry timer
 * are process-local runtime capabilities (see their modules).
 */
export class SqliteStream implements Stream {
  private readonly records: RecordStore;
  private readonly messages: MessageStore;
  private readonly producers: ProducerStore;
  private readonly notifier = new StreamNotifier();
  private readonly timeout: TimeoutScheduler;

  constructor(
    private readonly db: Database,
    readonly id: StreamId,
    private readonly deleteFromCache: () => void,
    onScheduledExpiry?: () => Promise<void> | void,
  ) {
    this.records = new RecordStore(db, id);
    this.messages = new MessageStore(db, id);
    this.producers = new ProducerStore(db, id);
    this.timeout = new TimeoutScheduler(onScheduledExpiry);
  }

  getRecord(): Promise<StreamRecord | null> {
    return this.records.getRecord();
  }

  async commit(plan: MutationPlan): Promise<CommitResult> {
    try {
      return await runImmediateTransactionWithBusyRetry(this.db, () =>
        this.commitInTransaction(plan),
      );
    } catch (error) {
      if (error instanceof PreconditionFailed || error instanceof SqliteBusyRetryExhausted)
        return { status: "precondition-failed", record: this.readRecord() };
      throw error;
    }
  }

  purgeSelfSync(): void {
    const purge = this.db.transaction(() => {
      this.db.run("delete from streamsy_messages where stream_id = ?", [this.id]);
      this.db.run("delete from streamsy_producers where stream_id = ?", [this.id]);
      this.db.run("delete from streamsy_streams where stream_id = ?", [this.id]);
    });
    purge();
    this.timeout.cancel();
    this.deleteFromCache();
  }

  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]> {
    return this.messages.listMessages(options);
  }

  getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.producers.getProducerState(producerId);
  }

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return this.notifier.waitForEvent(options);
  }

  notify(type: StreamEventType): Promise<void> | void {
    return this.notifier.notify(type);
  }

  scheduleExpiry(at: number): Promise<void> | void {
    return this.timeout.schedule(at);
  }

  cancelExpiry(): Promise<void> | void {
    return this.timeout.cancel();
  }

  private commitInTransaction(plan: MutationPlan): CommitResult {
    let record = this.readRecord();

    if (plan.createRecord) {
      if (plan.createRecord.id !== this.id) {
        throw new Error(`Record id ${plan.createRecord.id} does not match bound stream ${this.id}`);
      }
      if (record) return { status: "precondition-failed", record };
      const inserted = this.db.run(INSERT_SQL, recordToRow(plan.createRecord));
      if (inserted.changes === 0)
        return { status: "precondition-failed", record: this.readRecord() };
      record = plan.createRecord;
    }

    if (!record) return { status: "precondition-failed", record: null };

    if (plan.recordPatch) {
      const updated = this.updateRecordConditional(record, plan.recordPatch, plan);
      if (!updated) return { status: "precondition-failed", record: this.readRecord() };
      record = updated;
    } else if (!this.recordPreconditionsHold(record, plan)) {
      return { status: "precondition-failed", record };
    }

    const producer = plan.preconditions.producer;
    if (producer) {
      const changed = producer.expected
        ? this.updateProducerIfExpected(producer.producerId, producer.expected, producer.next)
        : this.insertProducerIfAbsent(producer.producerId, producer.next);
      if (!changed) throw new PreconditionFailed();
    }

    if (plan.appendMessages && plan.appendMessages.length > 0)
      this.messages.appendMessagesSync(plan.appendMessages);

    return { status: "committed", record: this.readRecord() ?? record };
  }

  private updateRecordConditional(
    record: StreamRecord,
    patch: StreamRecordPatch,
    plan: MutationPlan,
  ): StreamRecord | null {
    const next: StreamRecord = {
      ...record,
      config: { ...record.config, ...patch.config },
      lifecycle: { ...record.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? record.currentOffset,
      counter: patch.counter ?? record.counter,
    };
    const [, ...values] = recordToRow(next);
    const where = ["stream_id = ?"];
    const params: (string | number | null)[] = [...values, this.id];
    if (plan.preconditions.expectedOffset !== undefined) {
      where.push("current_offset = ?");
      params.push(plan.preconditions.expectedOffset);
    }
    if (plan.preconditions.expectedClosed !== undefined) {
      where.push("closed = ?");
      params.push(plan.preconditions.expectedClosed ? 1 : 0);
    }

    const columns = STREAM_COLUMNS.filter((column) => column !== "stream_id");
    const result = this.db.run(
      `update streamsy_streams set ${columns.map((column) => `${column} = ?`).join(", ")}
       where ${where.join(" and ")}`,
      params,
    );
    return result.changes > 0 ? next : null;
  }

  private recordPreconditionsHold(record: StreamRecord, plan: MutationPlan): boolean {
    if (
      plan.preconditions.expectedOffset !== undefined &&
      plan.preconditions.expectedOffset !== record.currentOffset
    )
      return false;
    if (
      plan.preconditions.expectedClosed !== undefined &&
      plan.preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return false;
    return true;
  }

  private insertProducerIfAbsent(producerId: string, next: ProducerState): boolean {
    const result = this.db.run(
      `insert into streamsy_producers (stream_id, producer_id, epoch, last_seq)
       values (?, ?, ?, ?)
       on conflict(stream_id, producer_id) do nothing`,
      [this.id, producerId, next.epoch, next.lastSeq],
    );
    return result.changes > 0;
  }

  private updateProducerIfExpected(
    producerId: string,
    expected: ProducerState,
    next: ProducerState,
  ): boolean {
    const result = this.db.run(
      `update streamsy_producers
       set epoch = ?, last_seq = ?
       where stream_id = ? and producer_id = ? and epoch = ? and last_seq = ?`,
      [next.epoch, next.lastSeq, this.id, producerId, expected.epoch, expected.lastSeq],
    );
    return result.changes > 0;
  }

  private readRecord(): StreamRecord | null {
    const row =
      this.db
        .query<StreamRow, [StreamId]>("select * from streamsy_streams where stream_id = ?")
        .get(this.id) ?? null;
    return row ? rowToRecord(row) : null;
  }
}

class PreconditionFailed extends Error {}
