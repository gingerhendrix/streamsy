/** Stored message mutation helpers for one storage-bound stream. */

import type { StreamEventHub } from "../../types/factory.ts";
import type { Clock, StoredMessage, StreamRecord, StreamRecordPatch } from "../../types/storage.ts";
import { allocate as allocateOffsets } from "../helpers/offset-generator.ts";
import { ExpiryPolicy } from "../helpers/expiry-policy.ts";

/** Narrow storage view the message writer mutates through. */
export interface MessageWriterStore {
  getRecord(): Promise<StreamRecord | null>;
  updateRecord(patch: StreamRecordPatch): Promise<StreamRecord>;
  appendMessages(messages: StoredMessage[]): Promise<void>;
  readonly events?: StreamEventHub;
}

export class StreamMessageWriter {
  constructor(
    private stream: MessageWriterStore,
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
    if (messages.length > 0) await this.stream.appendMessages(messages);
    await this.stream.updateRecord({
      currentOffset: allocation.nextOffset,
      counter: allocation.endCounter,
      lifecycle: seq ? { lastSeq: seq } : undefined,
    });
    await this.stream.events?.notify("message");
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
      latest = (await this.stream.getRecord()) ?? record;
    } else {
      await this.expiryPolicy.touch(streamId, record, "close");
    }
    await this.stream.updateRecord({
      lifecycle: { closed: true, closedAt: this.clock.now(), ...(seq ? { lastSeq: seq } : {}) },
      currentOffset: nextOffset,
      counter: latest.counter,
    });
    await this.stream.events?.notify("closed");
    return nextOffset;
  }
}
