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
import { MemoryEventHub } from "./event-hub.ts";
import { MemoryExpiryScheduler } from "./expiry-scheduler.ts";
import { MemoryMessageStore } from "./message-store.ts";
import { MemoryMutationCoordinator } from "./mutation-coordinator.ts";
import { MemoryProducerStore } from "./producer-store.ts";
import { MemoryRecordStore } from "./record-store.ts";
import { MemoryReferenceStore } from "./reference-store.ts";

export class MemoryStream implements Stream {
  private readonly records: MemoryRecordStore;
  private readonly messages: MemoryMessageStore;
  private readonly producers: MemoryProducerStore;
  private readonly references: MemoryReferenceStore;
  private readonly mutations: MemoryMutationCoordinator;
  private readonly events: MemoryEventHub;
  private readonly expiry: MemoryExpiryScheduler;

  constructor(
    readonly id: StreamId,
    private readonly deleteFromState: () => void,
  ) {
    this.records = new MemoryRecordStore(id);
    this.messages = new MemoryMessageStore(this.records);
    this.producers = new MemoryProducerStore(this.records);
    this.references = new MemoryReferenceStore(this.records);
    this.mutations = new MemoryMutationCoordinator();
    this.events = new MemoryEventHub();
    this.expiry = new MemoryExpiryScheduler();
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
    await this.expiry.cancelExpiry();
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
    return this.references.incrementChildRefCount();
  }

  decrementChildRefCount(): Promise<number> {
    return this.references.decrementChildRefCount();
  }

  withMutationLock<T>(fn: () => Promise<T>): Promise<T> {
    return this.mutations.withMutationLock(fn);
  }

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return this.events.waitForEvent(options);
  }

  notify(type: StreamEventType): Promise<void> | void {
    return this.events.notify(type);
  }

  scheduleExpiry(at: number, callback?: () => Promise<void>): Promise<void> | void {
    return this.expiry.scheduleExpiry(at, callback);
  }

  cancelExpiry(): Promise<void> | void {
    return this.expiry.cancelExpiry();
  }
}
