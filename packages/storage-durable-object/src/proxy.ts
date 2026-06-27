import type {
  CommitResult,
  ListMessagesOptions,
  MutationPlan,
  ProducerState,
  StoredMessage,
  Stream,
  StreamEventType,
  StreamId,
  StreamRecord,
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
 * readonly property while forwarding storage operations to the initialized DO.
 */
export class DurableObjectStreamProxy implements Stream {
  constructor(
    readonly id: StreamId,
    private readonly stub: DurableObjectStreamStub,
  ) {}

  getRecord(): Promise<StreamRecord | null> {
    return this.stub.getRecord();
  }

  commit(plan: MutationPlan): Promise<CommitResult> {
    return this.stub.commit(plan);
  }

  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]> {
    return this.stub.listMessages(options);
  }

  getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.stub.getProducerState(producerId);
  }

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    const { signal, ...serializableOptions } = options;
    if (!signal) return this.stub.waitForEvent(serializableOptions);
    if (signal.aborted) return Promise.resolve({ status: "aborted" });

    return new Promise<WaitForEventResult>((resolve, reject) => {
      let settled = false;
      const finish = (result: WaitForEventResult) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", abort);
        resolve(result);
      };
      const abort = () => finish({ status: "aborted" });

      signal.addEventListener("abort", abort, { once: true });
      this.stub.waitForEvent(serializableOptions).then(finish, (error) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", abort);
        reject(error);
      });
    });
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
