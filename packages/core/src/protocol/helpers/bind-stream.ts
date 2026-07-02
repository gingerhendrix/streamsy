/**
 * Core-internal per-stream view over the flat {@link StorageAdapter}.
 *
 * The seam adapters implement is flat (`method(streamId, …)`), but core's call
 * sites read nicer with a per-stream handle. `bindStream` builds that handle on
 * the *core* side — it is NOT part of the seam, just a thin closure over the
 * adapter with `streamId` pre-applied. This keeps ergonomics in core while the
 * seam stays flat and free of lifetime-bearing handles.
 */
import type {
  AppendPlan,
  StorageAdapter,
  StorageAppendResult,
} from "../../types/storage-adapter.ts";
import type {
  AwaitChangeOptions,
  AwaitChangeResult,
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamId,
  StreamRecord,
} from "../../types/storage.ts";

/** The per-stream surface core call sites use (was the seam `Stream`). */
export interface BoundStream {
  readonly id: StreamId;
  getRecord(): Promise<StreamRecord | null>;
  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]>;
  getProducerState(producerId: string): Promise<ProducerState | undefined>;
  append(plan: AppendPlan): Promise<StorageAppendResult>;
  awaitChange(options: AwaitChangeOptions): Promise<AwaitChangeResult>;
  scheduleExpiry(at: number): Promise<void> | void;
  cancelExpiry(): Promise<void> | void;
}

export function bindStream(adapter: StorageAdapter, id: StreamId): BoundStream {
  return {
    id,
    getRecord: () => adapter.getRecord(id),
    listMessages: (options) => adapter.listMessages(id, options),
    getProducerState: (producerId) => adapter.getProducerState(id, producerId),
    append: (plan) => adapter.append(id, plan),
    awaitChange: (options) => adapter.awaitChange(id, options),
    scheduleExpiry: (at) => adapter.scheduleExpiry(id, at),
    cancelExpiry: () => adapter.cancelExpiry(id),
  };
}
