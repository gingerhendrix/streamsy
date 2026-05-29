import type {
  CreateStreamRecordResult,
  ListMessagesOptions,
  StoredMessage,
  Stream,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
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
  readonly producers: MemoryProducerStore;
  readonly references: MemoryReferenceStore;
  readonly mutations: MemoryMutationCoordinator;
  readonly events: MemoryEventHub;
  readonly expiry: MemoryExpiryScheduler;

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
}
