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
import type { DurableObjectStreamStorage } from "./storage.ts";

type DurableObjectStreamStub = DurableObjectStub<DurableObjectStreamStorage>;

/**
 * Protocol-facing Stream for a Durable Object-backed stream.
 *
 * The Durable Object class exposes the storage RPC method surface directly.
 * This proxy is intentionally thin: it keeps the local stream id as a typed
 * readonly property and implements `withMutationLock` around explicit DO
 * lock-token RPCs so callbacks can call back into the same stub without the DO
 * deadlocking while it waits for a remote callback to complete.
 */
export class DurableObjectStreamProxy implements Stream {
  constructor(
    readonly id: StreamId,
    private readonly stub: DurableObjectStreamStub,
  ) {}

  getRecord(): Promise<StreamRecord | null> {
    return this.stub.getRecord();
  }

  createRecord(record: StreamRecord): Promise<CreateStreamRecordResult> {
    return this.stub.createRecord(record);
  }

  updateRecord(patch: StreamRecordPatch): Promise<StreamRecord> {
    return this.stub.updateRecord(patch);
  }

  deleteRecord(): Promise<void> {
    return this.stub.deleteRecord();
  }

  appendMessages(messages: StoredMessage[]): Promise<void> {
    return this.stub.appendMessages(messages);
  }

  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]> {
    return this.stub.listMessages(options);
  }

  deleteMessages(): Promise<void> {
    return this.stub.deleteMessages();
  }

  getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.stub.getProducerState(producerId);
  }

  setProducerState(producerId: string, state: ProducerState): Promise<void> {
    return this.stub.setProducerState(producerId, state);
  }

  deleteProducerStates(): Promise<void> {
    return this.stub.deleteProducerStates();
  }

  incrementChildRefCount(): Promise<number> {
    return this.stub.incrementChildRefCount();
  }

  decrementChildRefCount(): Promise<number> {
    return this.stub.decrementChildRefCount();
  }

  async withMutationLock<T>(fn: () => Promise<T>): Promise<T> {
    const key = `stream:${this.id}`;
    const token = await this.stub.acquireLock(key);
    try {
      return await fn();
    } finally {
      await this.stub.releaseLock(key, token);
    }
  }

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return this.stub.waitForEvent(options);
  }

  notify(type: StreamEventType): Promise<void> | void {
    return this.stub.notify(type);
  }

  scheduleExpiry(at: number, callback?: () => Promise<void>): Promise<void> | void {
    return this.stub.scheduleExpiry(at, callback);
  }

  cancelExpiry(): Promise<void> | void {
    return this.stub.cancelExpiry();
  }
}
