import type { AppendPlan, StorageAppendResult } from "../../types/storage-adapter.ts";
import type {
  AwaitChangeOptions,
  AwaitChangeResult,
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
} from "../../types/storage.ts";
import { runAwaitChangeLoop } from "../../protocol/helpers/await-change-loop.ts";
import { MessageStore } from "./stores/message-store.ts";
import { ProducerStore } from "./stores/producer-store.ts";
import { RecordStore } from "./stores/record-store.ts";
import { MemoryNotifier } from "./utils/notifier.ts";
import { TimeoutScheduler } from "./utils/timeout-scheduler.ts";

/**
 * Adapter-internal write engine input. Superset of {@link AppendPlan} with an
 * optional `createRecord` — the `create`/`fork` intents reuse the same engine to
 * insert the record, while the public `append` never carries `createRecord`.
 */
interface WritePlan {
  createRecord?: StreamRecord;
  preconditions: AppendPlan["preconditions"];
  messages?: StoredMessage[];
  recordPatch?: StreamRecordPatch;
}

type WriteResult =
  | { status: "committed"; record: StreamRecord }
  | {
      status: "precondition-failed";
      record: StreamRecord | null;
      reason?: "offset" | "closed" | "producer";
    };

/** Adapter-private per-stream handle. Not on the seam — owned by the memory state. */
export class MemoryStream {
  private readonly records: RecordStore;
  private readonly messages: MessageStore;
  private readonly producers: ProducerStore;
  private readonly notifier: MemoryNotifier;
  private readonly timeout: TimeoutScheduler;

  constructor(
    readonly id: StreamId,
    private readonly deleteFromState: () => void,
    onScheduledExpiry?: () => Promise<void> | void,
  ) {
    this.records = new RecordStore(id);
    this.messages = new MessageStore(this.records);
    this.producers = new ProducerStore(this.records);
    this.notifier = new MemoryNotifier();
    this.timeout = new TimeoutScheduler(onScheduledExpiry);
  }

  getRecord(): StreamRecord | null {
    return this.records.getRecord();
  }

  /** Public append intent (no `createRecord`). */
  append(plan: AppendPlan): StorageAppendResult {
    const out = this.applyMutation({
      preconditions: plan.preconditions,
      messages: plan.messages,
      recordPatch: plan.recordPatch,
    });
    if (out.status === "committed") return { status: "appended", record: out.record };
    // `reason` is required on the seam; an unattributable failure (the record
    // was concurrently purged) reports "offset" per the seam contract.
    return { status: "precondition-failed", record: out.record, reason: out.reason ?? "offset" };
  }

  /**
   * Adapter-internal write engine shared by `append` (no `createRecord`) and the
   * `create`/`fork` intents (with `createRecord`).
   */
  applyMutation(plan: WritePlan): WriteResult {
    let record = this.records.getRecord();

    if (plan.createRecord) {
      if (record) return { status: "precondition-failed", record };
      const created = this.records.createRecord(plan.createRecord);
      if (created.status === "exists")
        return { status: "precondition-failed", record: created.record };
      record = this.records.getRecord();
    }

    if (!record) return { status: "precondition-failed", record: null };
    if (
      plan.preconditions.expectedOffset !== undefined &&
      plan.preconditions.expectedOffset !== record.currentOffset
    )
      return { status: "precondition-failed", record, reason: "offset" };
    if (
      plan.preconditions.expectedClosed !== undefined &&
      plan.preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return { status: "precondition-failed", record, reason: "closed" };

    const producer = plan.preconditions.producer;
    if (producer) {
      const current = this.producers.getProducerState(producer.producerId);
      if (!producerStatesEqual(current, producer.expected))
        return { status: "precondition-failed", record, reason: "producer" };
    }

    if (plan.messages && plan.messages.length > 0) this.messages.appendMessages(plan.messages);
    if (plan.recordPatch) record = this.records.updateRecord(plan.recordPatch);
    if (producer) this.producers.setProducerState(producer.producerId, producer.next);

    // Wake `awaitChange` waiters now that durable facts are visible. Over-waking
    // is safe: a woken waiter re-reads and re-parks if nothing relevant changed.
    this.notifier.wake();
    return { status: "committed", record: this.records.getRecord() ?? record };
  }

  listMessages(options?: ListMessagesOptions): StoredMessage[] {
    return this.messages.listMessages(options);
  }

  getProducerState(producerId: string): ProducerState | undefined {
    return this.producers.getProducerState(producerId);
  }

  purgeSelf(): void {
    this.messages.deleteMessages();
    this.producers.deleteProducerStates();
    this.records.deleteRecord();
    this.timeout.cancel();
    this.deleteFromState();
    this.notifier.wake();
  }

  softDelete(): void {
    this.records.updateRecord({ lifecycle: { softDeleted: true } });
    this.notifier.wake();
  }

  /**
   * Level-triggered live wait via the shared exported loop: re-read durable
   * state first (so a mutation that landed between the caller's observation and
   * this call is never missed), then park on the wake bus until the state
   * advances or the budget expires. Read and park are synchronous with the wake
   * bus here, so no per-park cap is needed.
   */
  awaitChange(options: AwaitChangeOptions): Promise<AwaitChangeResult> {
    return runAwaitChangeLoop(
      {
        readRecord: () => this.records.getRecord(),
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
}

function producerStatesEqual(
  left: ProducerState | undefined,
  right: ProducerState | undefined,
): boolean {
  if (!left || !right) return left === right;
  return left.epoch === right.epoch && left.lastSeq === right.lastSeq;
}
