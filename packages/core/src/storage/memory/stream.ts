import type { CommitResult, MutationPlan, Stream } from "../../types/factory.ts";
import type {
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamEventType,
  StreamId,
  StreamRecord,
  WaitForEventOptions,
  WaitForEventResult,
} from "../../types/storage.ts";
import { MessageStore } from "./stores/message-store.ts";
import { ProducerStore } from "./stores/producer-store.ts";
import { RecordStore } from "./stores/record-store.ts";
import { MemoryNotifier } from "./utils/notifier.ts";
import { TimeoutScheduler } from "./utils/timeout-scheduler.ts";

export class MemoryStream implements Stream {
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

  getRecord(): Promise<StreamRecord | null> {
    return this.records.getRecord();
  }

  getRecordSync(): StreamRecord | null {
    return this.records.getRecordSync();
  }

  commit(plan: MutationPlan): Promise<CommitResult> {
    return Promise.resolve(this.commitSync(plan));
  }

  commitSync(plan: MutationPlan): CommitResult {
    let record = this.records.getRecordSync();

    if (plan.createRecord) {
      if (record) return { status: "precondition-failed", record };
      const created = this.records.createRecordSync(plan.createRecord);
      if (created.status === "exists")
        return { status: "precondition-failed", record: created.record };
      record = this.records.getRecordSync();
    }

    if (!record) return { status: "precondition-failed", record: null };
    if (
      plan.preconditions.expectedOffset !== undefined &&
      plan.preconditions.expectedOffset !== record.currentOffset
    )
      return { status: "precondition-failed", record };
    if (
      plan.preconditions.expectedClosed !== undefined &&
      plan.preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return { status: "precondition-failed", record };

    const producer = plan.preconditions.producer;
    if (producer) {
      const current = this.producers.getProducerStateSync(producer.producerId);
      if (!producerStatesEqual(current, producer.expected))
        return { status: "precondition-failed", record };
    }

    if (plan.appendMessages && plan.appendMessages.length > 0)
      this.messages.appendMessagesSync(plan.appendMessages);
    if (plan.recordPatch) record = this.records.updateRecordSync(plan.recordPatch);
    if (producer) this.producers.setProducerStateSync(producer.producerId, producer.next);

    return { status: "committed", record: this.records.getRecordSync() ?? record };
  }

  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]> {
    return this.messages.listMessages(options);
  }

  getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.producers.getProducerState(producerId);
  }

  purgeSelfSync(): void {
    this.messages.deleteMessagesSync();
    this.producers.deleteProducerStatesSync();
    this.records.deleteRecordSync();
    this.timeout.cancel();
    this.deleteFromState();
  }

  softDeleteSync(): void {
    this.records.updateRecordSync({ lifecycle: { softDeleted: true } });
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
}

function producerStatesEqual(
  left: ProducerState | undefined,
  right: ProducerState | undefined,
): boolean {
  if (!left || !right) return left === right;
  return left.epoch === right.epoch && left.lastSeq === right.lastSeq;
}
