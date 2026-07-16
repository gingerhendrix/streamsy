import type { JsonValue, ReadEndResult, StreamBatch, StreamReadSession } from "./types.ts";

interface PendingBatch<T extends JsonValue> {
  batch: StreamBatch<T>;
  accepted: () => void;
}

interface WaitingNext<T extends JsonValue> {
  resolve: (result: IteratorResult<StreamBatch<T>>) => void;
}

/**
 * The single client read-session implementation, shared by every adapter.
 *
 * It is a one-consumer, one-batch queue whose producer is backpressured by
 * iterator delivery. Iteration never throws for operational failures: the
 * terminal outcome is delivered through {@link ClientReadSession.done}, which
 * always resolves. Only programmer misuse (double iteration, concurrent
 * `next()`) throws.
 */
export class ClientReadSession<T extends JsonValue = JsonValue>
  implements StreamReadSession<T>, AsyncIterator<StreamBatch<T>>
{
  readonly done: Promise<ReadEndResult>;
  readonly contentType?: string;
  readonly startOffset?: string;
  offset: string;
  cursor?: string;
  upToDate: boolean;
  streamClosed: boolean;

  private doneResolve!: (result: ReadEndResult) => void;
  private pending?: PendingBatch<T>;
  private waiting?: WaitingNext<T>;
  private ended = false;
  private iterated = false;
  private cancelHook?: (reason?: unknown) => void;

  constructor(options: {
    contentType?: string;
    startOffset: string;
    offset?: string;
    cursor?: string;
    upToDate?: boolean;
    streamClosed?: boolean;
    onCancel?: (reason?: unknown) => void;
  }) {
    this.contentType = options.contentType;
    this.startOffset = options.startOffset;
    this.offset = options.offset ?? options.startOffset;
    this.cursor = options.cursor;
    this.upToDate = options.upToDate ?? false;
    this.streamClosed = options.streamClosed ?? false;
    this.cancelHook = options.onCancel;
    this.done = new Promise<ReadEndResult>((resolve) => {
      this.doneResolve = resolve;
    });
  }

  setCancelHook(hook: (reason?: unknown) => void): void {
    this.cancelHook = hook;
  }

  [Symbol.asyncIterator](): AsyncIterator<StreamBatch<T>> {
    if (this.iterated) throw new Error("A stream read session can only be iterated once");
    this.iterated = true;
    return this;
  }

  next(): Promise<IteratorResult<StreamBatch<T>>> {
    if (this.pending) return Promise.resolve(this.acceptPending());
    if (this.ended) return Promise.resolve({ done: true, value: undefined });
    if (this.waiting)
      return Promise.reject(new Error("Concurrent iterator.next() calls are invalid"));
    return new Promise((resolve) => {
      this.waiting = { resolve };
    });
  }

  return(): Promise<IteratorResult<StreamBatch<T>>> {
    this.cancel();
    return Promise.resolve({ done: true, value: undefined });
  }

  cancel(reason?: unknown): void {
    if (this.ended) return;
    this.cancelHook?.(reason);
    this.end({ status: "cancelled" });
  }

  /** Producer side: hand a batch to the consumer, backpressured until accepted. */
  async deliver(batch: StreamBatch<T>): Promise<void> {
    if (this.ended) return;
    if (this.pending) throw new Error("Read session producer exceeded its one-batch buffer");
    if (this.waiting) {
      const waiting = this.waiting;
      this.waiting = undefined;
      this.updateState(batch);
      waiting.resolve({ done: false, value: batch });
      return;
    }
    await new Promise<void>((accepted) => {
      this.pending = { batch, accepted };
    });
  }

  /** Producer side: terminate the session with its outcome. Always resolves `done`. */
  end(result: ReadEndResult): void {
    if (this.ended) return;
    this.ended = true;
    this.pending?.accepted();
    this.pending = undefined;
    this.waiting?.resolve({ done: true, value: undefined });
    this.waiting = undefined;
    this.doneResolve(result);
  }

  private acceptPending(): IteratorResult<StreamBatch<T>> {
    const pending = this.pending!;
    this.pending = undefined;
    this.updateState(pending.batch);
    pending.accepted();
    return { done: false, value: pending.batch };
  }

  private updateState(batch: StreamBatch<T>): void {
    this.offset = batch.offset;
    this.cursor = batch.cursor;
    this.upToDate = batch.upToDate;
    this.streamClosed = batch.streamClosed;
  }
}
