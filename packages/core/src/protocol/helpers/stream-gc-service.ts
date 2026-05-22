/**
 * GC / delete orchestration for the durable streams protocol.
 *
 * Responsibilities:
 *
 *   1. Explicit `delete(streamId)`:
 *      - missing -> { status: "not-found" }
 *      - soft-deleted -> { status: "gone" }
 *      - childRefCount > 0 -> update lifecycle softDeleted true,
 *        notify "soft-deleted", return ok
 *      - otherwise purgeWithCascade and return ok
 *   2. `handleScheduledExpiry(streamId)`:
 *      - load record, no-op if missing
 *      - re-check the persisted deadline via injected `isExpired(record)`
 *        callback, no-op if stale (touch reschedule races)
 *      - childRefCount > 0 -> soft-delete and notify "soft-deleted"
 *      - otherwise purgeWithCascade
 *   3. `purgeWithCascade(streamId, record)`:
 *      - cancelExpiry, deleteMessages, deleteProducerStates, delete record
 *      - notify "deleted"
 *      - if forked, decrement parent refcount, load parent
 *      - if parent exists, new refcount is zero, and parent is soft-deleted,
 *        recursively purge parent
 *
 * Used by `StreamProtocol.delete`, `StreamProtocol.handleScheduledExpiry`,
 * and expiry-policy callbacks. The explicit-delete lazy-expiry prelude
 * (`expireIfNeeded(streamId)`) stays in `StreamProtocol.delete`, so this
 * service operates on the post-prelude record state. The scheduled-expiry
 * stale-timer guard uses the injected `isExpired(record)` callback so this
 * module does not own deadline policy.
 *
 * Not exported from `packages/core/src/index.ts`; service-level tests import
 * directly from this module.
 */

import type { DeleteResult } from "../../types/protocol.ts";
import type { StreamId, StreamRecord, StreamStoreAdapter } from "../../types/storage.ts";

export interface StreamGcServiceMutators {
  isExpired(record: StreamRecord): boolean;
}

export class StreamGcService {
  constructor(
    private store: StreamStoreAdapter,
    private mutators: StreamGcServiceMutators,
  ) {}

  async delete(streamId: StreamId): Promise<DeleteResult> {
    const record = await this.store.get(streamId);
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };
    if (record.lifecycle.childRefCount > 0) {
      await this.store.update(streamId, { lifecycle: { softDeleted: true } });
      await this.store.notify?.(streamId, "soft-deleted");
      return { status: "ok" };
    }
    await this.purgeWithCascade(streamId, record);
    return { status: "ok" };
  }

  async handleScheduledExpiry(streamId: StreamId): Promise<void> {
    const record = await this.store.get(streamId);
    if (!record) return;
    if (!this.mutators.isExpired(record)) return;
    if (record.lifecycle.childRefCount > 0) {
      await this.store.update(streamId, { lifecycle: { softDeleted: true } });
      await this.store.notify?.(streamId, "soft-deleted");
      return;
    }
    await this.purgeWithCascade(streamId, record);
  }

  private async purgeWithCascade(streamId: StreamId, record: StreamRecord): Promise<void> {
    await this.store.cancelExpiry?.(streamId);
    await this.store.deleteMessages(streamId);
    await this.store.deleteProducerStates(streamId);
    await this.store.delete(streamId);
    await this.store.notify?.(streamId, "deleted");

    const parentId = record.lifecycle.forkedFrom;
    if (!parentId) return;
    const newRefCount = await this.store.decrementChildRefCount(parentId);
    const parent = await this.store.get(parentId);
    if (parent && newRefCount === 0 && parent.lifecycle.softDeleted)
      await this.purgeWithCascade(parentId, parent);
  }
}
