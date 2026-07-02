import type { Database } from "bun:sqlite";
import type {
  AppendPlan,
  AwaitChangeOptions,
  AwaitChangeResult,
  ListMessagesOptions,
  ProducerState,
  StorageAppendResult,
  StoredMessage,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
} from "@streamsy/core";
import { runAwaitChangeLoop } from "@streamsy/core";
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

type FailureReason = "offset" | "closed" | "producer";

/**
 * Adapter-internal write engine input. Superset of `AppendPlan` with an optional
 * `createRecord` — the `create`/`fork` intents reuse the engine to insert the
 * record, while the public `append` never carries `createRecord`.
 */
interface WritePlan {
  createRecord?: StreamRecord;
  preconditions: AppendPlan["preconditions"];
  messages?: StoredMessage[];
  recordPatch?: StreamRecordPatch;
}

type WriteResult =
  | { status: "committed"; record: StreamRecord }
  | { status: "precondition-failed"; record: StreamRecord | null; reason?: FailureReason };

/**
 * SQLite-backed per-stream handle bound to one id. Record/message/producer state
 * is persisted in the shared database; the live-read notifier and expiry timer
 * are process-local runtime capabilities (see their modules). This handle is
 * adapter-private — the seam is the flat `StorageAdapter` returned by
 * `createSqliteStorageAdapter`.
 */
export class SqliteStream {
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

  getRecord(): StreamRecord | null {
    return this.records.getRecord();
  }

  /** Public append intent (no `createRecord`). */
  async append(plan: AppendPlan): Promise<StorageAppendResult> {
    const out = await this.applyMutation({
      preconditions: plan.preconditions,
      messages: plan.messages,
      recordPatch: plan.recordPatch,
    });
    if (out.status === "committed") return { status: "appended", record: out.record };
    // `reason` is required on the seam. SQLite's conditional UPDATE is opaque,
    // so attribution comes from a post-failure re-read (exact in the
    // single-writer case the kit tests; best-effort under concurrency, falling
    // back to "offset" when unattributable, e.g. busy-retry exhaustion).
    return { status: "precondition-failed", record: out.record, reason: out.reason ?? "offset" };
  }

  /**
   * Adapter-internal write engine shared by `append` (no `createRecord`) and the
   * `create`/`fork` intents (with `createRecord`). Wraps the mutation in one
   * immediate transaction with busy-retry and wakes parked `awaitChange` waiters
   * after a successful commit.
   */
  async applyMutation(plan: WritePlan): Promise<WriteResult> {
    try {
      const result = await runImmediateTransactionWithBusyRetry(this.db, () =>
        this.applyMutationInTransaction(plan),
      );
      // Over-waking is safe: a woken waiter re-reads and re-parks if nothing
      // relevant changed.
      if (result.status === "committed") this.notifier.wake();
      return result;
    } catch (error) {
      if (error instanceof PreconditionFailed)
        return error.reason !== undefined
          ? { status: "precondition-failed", record: this.readRecord(), reason: error.reason }
          : { status: "precondition-failed", record: this.readRecord() };
      if (error instanceof SqliteBusyRetryExhausted)
        return { status: "precondition-failed", record: this.readRecord() };
      throw error;
    }
  }

  purgeSelf(): void {
    const purge = this.db.transaction(() => {
      this.db.run("delete from streamsy_messages where stream_id = ?", [this.id]);
      this.db.run("delete from streamsy_producers where stream_id = ?", [this.id]);
      this.db.run("delete from streamsy_streams where stream_id = ?", [this.id]);
    });
    purge();
    this.timeout.cancel();
    this.deleteFromCache();
    this.notifier.wake();
  }

  /** Wake parked `awaitChange` waiters. Used by the delete path to surface a
   * soft-delete / purge transition without waiting for the poll timeout. */
  wake(): void {
    this.notifier.wake();
  }

  listMessages(options?: ListMessagesOptions): StoredMessage[] {
    return this.messages.listMessages(options);
  }

  getProducerState(producerId: string): ProducerState | undefined {
    return this.producers.getProducerState(producerId);
  }

  /**
   * Level-triggered live wait via the shared exported loop. Re-reads durable
   * state first (so a commit that landed between the caller's observation and
   * this call is never missed), then parks on the in-process wake bus until the
   * state advances or the budget expires. Cross-process writes are surfaced on
   * the next timeout re-read.
   */
  awaitChange(options: AwaitChangeOptions): Promise<AwaitChangeResult> {
    return runAwaitChangeLoop(
      {
        readRecord: () => this.readRecord(),
        waitForWake: (timeoutMs) => this.notifier.waitForWake(timeoutMs),
      },
      options,
    );
  }

  scheduleExpiry(at: number): Promise<void> | void {
    return this.timeout.schedule(at);
  }

  cancelExpiry(): Promise<void> | void {
    return this.timeout.cancel();
  }

  private applyMutationInTransaction(plan: WritePlan): WriteResult {
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
      if (!updated)
        return {
          status: "precondition-failed",
          record: this.readRecord(),
          reason: this.reason(plan),
        };
      record = updated;
    } else if (!this.recordPreconditionsHold(record, plan)) {
      return { status: "precondition-failed", record, reason: this.reason(plan) };
    }

    const producer = plan.preconditions.producer;
    if (producer) {
      const changed = producer.expected
        ? this.updateProducerIfExpected(producer.producerId, producer.expected, producer.next)
        : this.insertProducerIfAbsent(producer.producerId, producer.next);
      if (!changed) throw new PreconditionFailed("producer");
    }

    if (plan.messages && plan.messages.length > 0) this.messages.appendMessages(plan.messages);

    return { status: "committed", record: this.readRecord() ?? record };
  }

  /** Best-effort attribution of an offset/closed precondition failure. */
  private reason(plan: WritePlan): FailureReason | undefined {
    const record = this.readRecord();
    if (!record) return undefined;
    if (
      plan.preconditions.expectedOffset !== undefined &&
      plan.preconditions.expectedOffset !== record.currentOffset
    )
      return "offset";
    if (
      plan.preconditions.expectedClosed !== undefined &&
      plan.preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return "closed";
    return undefined;
  }

  private updateRecordConditional(
    record: StreamRecord,
    patch: StreamRecordPatch,
    plan: WritePlan,
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

  private recordPreconditionsHold(record: StreamRecord, plan: WritePlan): boolean {
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

class PreconditionFailed extends Error {
  constructor(readonly reason?: FailureReason) {
    super();
  }
}
