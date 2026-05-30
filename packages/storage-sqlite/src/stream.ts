import type { Database } from "bun:sqlite";
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
import { StreamLock } from "./utils/lock.ts";
import { StreamNotifier } from "./utils/notifier.ts";
import { TimeoutScheduler } from "./utils/timeout-scheduler.ts";

/**
 * SQLite-backed protocol stream bound to one id. Record/message/producer state
 * is persisted in the shared database; the mutation lock, live-read notifier,
 * and expiry timer are process-local runtime capabilities (see their modules).
 */
export class SqliteStream implements Stream {
  private readonly records: RecordStore;
  private readonly messages: MessageStore;
  private readonly producers: ProducerStore;
  private readonly lock = new StreamLock();
  private readonly notifier = new StreamNotifier();
  private readonly timeout = new TimeoutScheduler();

  constructor(
    private readonly db: Database,
    readonly id: StreamId,
    private readonly deleteFromCache: () => void,
  ) {
    this.records = new RecordStore(db, id);
    this.messages = new MessageStore(db, id, this.records);
    this.producers = new ProducerStore(db, id, this.records);
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
    const purge = this.db.transaction(() => {
      this.db.run("delete from streamsy_messages where stream_id = ?", [this.id]);
      this.db.run("delete from streamsy_producers where stream_id = ?", [this.id]);
      this.db.run("delete from streamsy_streams where stream_id = ?", [this.id]);
    });
    purge();
    this.timeout.cancel();
    this.deleteFromCache();
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
