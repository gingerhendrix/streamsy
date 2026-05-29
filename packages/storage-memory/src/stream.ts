import type {
  CreateStreamRecordResult,
  ListMessagesOptions,
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
import type { MemoryEntry } from "./memory-entry.ts";
import { MemoryMessageStore } from "./message-store.ts";
import { MemoryMutationCoordinator } from "./mutation-coordinator.ts";
import { MemoryProducerStore } from "./producer-store.ts";
import { MemoryRecordStore } from "./record-store.ts";
import { MemoryReferenceStore } from "./reference-store.ts";

export class MemoryStream implements Stream {
  entry?: MemoryEntry;

  private readonly records: MemoryRecordStore;
  private readonly messages: MemoryMessageStore;
  readonly producers: MemoryProducerStore;
  readonly references: MemoryReferenceStore;
  readonly mutations: MemoryMutationCoordinator;
  readonly events: MemoryEventHub;
  readonly expiry: MemoryExpiryScheduler;

  private readonly waiters = new Set<(result: WaitForEventResult) => void>();
  private timer?: ReturnType<typeof setTimeout>;
  private lock?: Promise<void>;

  constructor(
    readonly id: StreamId,
    private readonly deleteFromState: () => void,
  ) {
    this.records = new MemoryRecordStore(this);
    this.messages = new MemoryMessageStore(this);
    this.producers = new MemoryProducerStore(this);
    this.references = new MemoryReferenceStore(this);
    this.mutations = new MemoryMutationCoordinator(this);
    this.events = new MemoryEventHub(this);
    this.expiry = new MemoryExpiryScheduler(this);
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

  deleteRecord(): Promise<void> {
    return this.records.deleteRecord();
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

  mustEntry(): MemoryEntry {
    if (!this.entry) throw new Error(`Stream not found: ${this.id}`);
    return this.entry;
  }

  async withMutationLock<T>(fn: () => Promise<T>): Promise<T> {
    while (this.lock) await this.lock;
    let release!: () => void;
    this.lock = new Promise<void>((resolve) => (release = resolve));
    try {
      return await fn();
    } finally {
      this.lock = undefined;
      release();
    }
  }

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return new Promise((resolve) => {
      const timeout = setTimeout(() => finish({ status: "timeout" }), options.timeoutMs);
      const finish = (result: WaitForEventResult) => {
        clearTimeout(timeout);
        this.waiters.delete(finish);
        resolve(result);
      };
      this.waiters.add(finish);
      options.signal?.addEventListener("abort", () => finish({ status: "aborted" }), {
        once: true,
      });
    });
  }

  notify(type: StreamEventType): void {
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const waiter of waiters) waiter({ status: "notified", type });
  }

  scheduleExpiry(at: number, callback?: () => Promise<void>): void {
    void this.cancelExpiry();
    if (!callback) return;
    const delay = Math.max(0, at - Date.now());
    this.timer = setTimeout(() => void callback(), delay);
  }

  async cancelExpiry(): Promise<void> {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }

  onDeleted(): void {
    this.deleteFromState();
  }
}
