/**
 * Native Durable Object `StreamFactory`.
 *
 * Implements the one-stream-per-Durable-Object model: each public stream id is
 * routed to a per-stream `DurableObjectStreamStorage` instance via
 * `namespace.idFromName(streamId)`. The Durable Object owns persistent stream
 * state and exposes the storage RPC methods; this module provides the small
 * protocol-facing proxy needed for local-only `Stream` concerns (`id` and
 * callback-based mutation locking) that do not map cleanly to Cloudflare RPC.
 */
import type {
  CreateStreamRecordResult,
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  Stream,
  StreamEventType,
  StreamFactory,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
  WaitForEventOptions,
  WaitForEventResult,
} from "@streamsy/core";
import type { DurableObjectStreamStorage } from "./storage.ts";

export interface DurableObjectStreamFactoryOptions {
  namespace: DurableObjectNamespace<DurableObjectStreamStorage>;
}

export function createDurableObjectStreamFactory(
  options: DurableObjectStreamFactoryOptions,
): StreamFactory {
  const { namespace } = options;

  return {
    async getStream(streamId: StreamId): Promise<Stream> {
      const stub = namespace.get(namespace.idFromName(streamId));
      await stub.init(streamId);
      return new DurableObjectStreamProxy(streamId, stub);
    },
  };
}

type DurableObjectStreamStub = DurableObjectStub<DurableObjectStreamStorage>;

/**
 * Protocol-facing Stream for a Durable Object-backed stream.
 *
 * The Durable Object class still implements the storage RPC method surface
 * directly. This proxy is intentionally thin: it keeps the local stream id as a
 * typed readonly property and implements `withMutationLock` around explicit DO
 * lock-token RPCs so callbacks can call back into the same stub without the DO
 * deadlocking while it waits for a remote callback to complete.
 */
class DurableObjectStreamProxy implements Stream {
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
