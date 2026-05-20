/** Stored message mutation helpers for the durable streams protocol. */

import type { Clock, StreamRecord, StreamStoreAdapter } from "../../types/storage.ts";
import { allocate as allocateOffsets } from "../helpers/offset-generator.ts";
import { ExpiryPolicy } from "../helpers/expiry-policy.ts";

export class StreamMessageWriter {
  constructor(
    private store: StreamStoreAdapter,
    private clock: Clock,
    private expiryPolicy: ExpiryPolicy,
  ) {}

  async appendMessages(
    streamId: string,
    record: StreamRecord,
    data: Uint8Array[],
    seq?: string,
  ): Promise<string> {
    await this.expiryPolicy.touch(streamId, record, "append");
    const allocation = allocateOffsets(record.counter, data.length);
    const now = this.clock.now();
    const messages = data.map((bytes, i) => ({
      data: bytes,
      offset: allocation.offsets[i]!,
      timestamp: now,
    }));
    if (messages.length > 0) await this.store.append(streamId, messages);
    await this.store.update(streamId, {
      currentOffset: allocation.nextOffset,
      counter: allocation.endCounter,
      lifecycle: seq ? { lastSeq: seq } : undefined,
    });
    await this.store.notify?.(streamId, "message");
    return allocation.nextOffset;
  }

  async closeRecord(
    streamId: string,
    record: StreamRecord,
    data: Uint8Array[],
    seq?: string,
  ): Promise<string> {
    let latest = record;
    let nextOffset = record.currentOffset;
    if (data.length > 0) {
      nextOffset = await this.appendMessages(streamId, record, data, seq);
      latest = (await this.store.get(streamId)) ?? record;
    } else {
      await this.expiryPolicy.touch(streamId, record, "close");
    }
    await this.store.update(streamId, {
      lifecycle: { closed: true, closedAt: this.clock.now(), ...(seq ? { lastSeq: seq } : {}) },
      currentOffset: nextOffset,
      counter: latest.counter,
    });
    await this.store.notify?.(streamId, "closed");
    return nextOffset;
  }
}
