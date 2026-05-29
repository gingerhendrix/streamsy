/**
 * Native Durable Object `StreamFactory`.
 *
 * Implements the one-stream-per-Durable-Object model: each public stream id is
 * routed to a per-stream `DurableObjectStreamStorage` instance via
 * `namespace.idFromName(streamId)`. The factory returns a `DurableObjectStream`
 * that implements the protocol-facing `Stream` methods directly while keeping
 * stub/RPC details private.
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
  const stubFor = (streamId: StreamId) => namespace.get(namespace.idFromName(streamId));

  return {
    async getStream(streamId: StreamId): Promise<Stream> {
      return new DurableObjectStream(streamId, stubFor(streamId));
    },
  };
}

class DurableObjectStream implements Stream {
  constructor(
    readonly id: StreamId,
    private readonly stub: DurableObjectStub<DurableObjectStreamStorage>,
  ) {}

  getRecord(): Promise<StreamRecord | null> {
    return this.stub.get();
  }

  createRecord(record: StreamRecord): Promise<CreateStreamRecordResult> {
    return this.stub.create(record);
  }

  updateRecord(patch: StreamRecordPatch): Promise<StreamRecord> {
    return this.stub.update(patch);
  }

  deleteRecord(): Promise<void> {
    return this.stub.deleteStream();
  }

  appendMessages(messages: StoredMessage[]): Promise<void> {
    return this.stub.appendToStream(messages);
  }

  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]> {
    return this.stub.list(options);
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

  scheduleExpiry(at: number): Promise<void> | void {
    return this.stub.scheduleExpiry(at);
  }

  cancelExpiry(): Promise<void> | void {
    return this.stub.cancelExpiry();
  }
}
