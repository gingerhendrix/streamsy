import type {
  CreateStreamRecordResult,
  ListMessagesOptions,
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
import { MessageStore } from "./stores/message-store.ts";
import { ProducerStore } from "./stores/producer-store.ts";
import { RecordStore } from "./stores/record-store.ts";
import { MemoryLock } from "./utils/lock.ts";
import { MemoryNotifier } from "./utils/notifier.ts";
import { TimeoutScheduler } from "./utils/timeout-scheduler.ts";

export class MemoryStream implements Stream {
  private readonly records: RecordStore;
  private readonly messages: MessageStore;
  private readonly producers: ProducerStore;
  private readonly lock: MemoryLock;
  private readonly notifier: MemoryNotifier;
  private readonly timeout: TimeoutScheduler;

  constructor(
    readonly id: StreamId,
    private readonly deleteFromState: () => void,
  ) {
    this.records = new RecordStore(id);
    this.messages = new MessageStore(this.records);
    this.producers = new ProducerStore(this.records);
    this.lock = new MemoryLock();
    this.notifier = new MemoryNotifier();
    this.timeout = new TimeoutScheduler();
  }

  getRecord(): Promise<StreamRecord | null> {
    return this.records.getRecord();
  }

  createRecord(record: StreamRecord): Promise<CreateStreamRecordResult> {
    return this.records.createRecord(record);
  }

  updateRecord(patch: StreamRecordPatch): Promise<StreamRecord> {
    return this.records.updateRecord(patch);
  }

  async deleteRecord(): Promise<void> {
    await this.records.deleteRecord();
    await this.messages.deleteMessages();
    await this.producers.deleteProducerStates();
    this.timeout.cancel();
    this.deleteFromState();
  }

  appendMessages(messages: StoredMessage[]): Promise<void> {
    return this.messages.appendMessages(messages);
  }

  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]> {
    return this.messages.listMessages(options);
  }

  deleteMessages(): Promise<void> {
    return this.messages.deleteMessages();
  }

  getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.producers.getProducerState(producerId);
  }

  setProducerState(producerId: string, state: ProducerState): Promise<void> {
    return this.producers.setProducerState(producerId, state);
  }

  deleteProducerStates(): Promise<void> {
    return this.producers.deleteProducerStates();
  }

  incrementChildRefCount(): Promise<number> {
    return this.records.incrementChildRefCount();
  }

  decrementChildRefCount(): Promise<number> {
    return this.records.decrementChildRefCount();
  }

  withMutationLock<T>(fn: () => Promise<T>): Promise<T> {
    return this.lock.withLock(fn);
  }

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return this.notifier.waitForEvent(options);
  }

  notify(type: StreamEventType): Promise<void> | void {
    return this.notifier.notify(type);
  }

  scheduleExpiry(at: number, callback?: () => Promise<void>): Promise<void> | void {
    return this.timeout.schedule(at, callback);
  }

  cancelExpiry(): Promise<void> | void {
    return this.timeout.cancel();
  }
}
