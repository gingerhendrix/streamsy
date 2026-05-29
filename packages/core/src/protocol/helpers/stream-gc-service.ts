/** GC / delete orchestration using storage-bound streams. */

import type { DeleteResult } from "../../types/protocol.ts";
import type { Stream } from "../../types/factory.ts";
import type { StreamId, StreamRecord } from "../../types/storage.ts";

export interface StreamGcServiceMutators {
  isExpired(record: StreamRecord): boolean;
}

export type ResolveStorageStream = (streamId: StreamId) => Promise<Stream> | Stream;

export interface StreamGcServiceDeps {
  resolve: ResolveStorageStream;
  mutators: StreamGcServiceMutators;
}

export class StreamGcService {
  constructor(private deps: StreamGcServiceDeps) {}

  async deleteStream(stream: Stream): Promise<DeleteResult> {
    const record = await stream.getRecord();
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };
    if (record.lifecycle.childRefCount > 0) {
      await stream.updateRecord({ lifecycle: { softDeleted: true } });
      await stream.notify("soft-deleted");
      return { status: "ok" };
    }
    await this.purgeWithCascade(stream, record);
    return { status: "ok" };
  }

  async handleScheduledExpiry(streamId: StreamId): Promise<void> {
    const stream = await this.deps.resolve(streamId);
    const record = await stream.getRecord();
    if (!record) return;
    if (!this.deps.mutators.isExpired(record)) return;
    if (record.lifecycle.childRefCount > 0) {
      await stream.updateRecord({ lifecycle: { softDeleted: true } });
      await stream.notify("soft-deleted");
      return;
    }
    await this.purgeWithCascade(stream, record);
  }

  private async purgeWithCascade(stream: Stream, record: StreamRecord): Promise<void> {
    await stream.cancelExpiry();
    await stream.deleteMessages();
    await stream.deleteProducerStates();
    await stream.deleteRecord();
    await stream.notify("deleted");

    const parentId = record.lifecycle.forkedFrom;
    if (!parentId) return;
    const parentStream = await this.deps.resolve(parentId);
    const newRefCount = await parentStream.decrementChildRefCount();
    const parent = await parentStream.getRecord();
    if (parent && newRefCount === 0 && parent.lifecycle.softDeleted)
      await this.purgeWithCascade(parentStream, parent);
  }
}
