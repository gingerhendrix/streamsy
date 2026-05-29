/** Stored message mutation helpers for one storage-bound stream. */

import type { Stream } from "../../types/factory.ts";
import type { Clock, StreamRecord } from "../../types/storage.ts";
import { allocate as allocateOffsets } from "../helpers/offset-generator.ts";
import { ExpiryPolicy } from "../helpers/expiry-policy.ts";

export interface StreamMessageWriterDeps {
  stream: Stream;
  clock: Clock;
  expiryPolicy: ExpiryPolicy;
}

export class StreamMessageWriter {
  constructor(private deps: StreamMessageWriterDeps) {}

  async appendMessages(record: StreamRecord, data: Uint8Array[], seq?: string): Promise<string> {
    await this.deps.expiryPolicy.touch(this.deps.stream, record, "append");
    const allocation = allocateOffsets(record.counter, data.length);
    const now = this.deps.clock.now();
    const messages = data.map((bytes, i) => ({
      data: bytes,
      offset: allocation.offsets[i]!,
      timestamp: now,
    }));
    if (messages.length > 0) await this.deps.stream.appendMessages(messages);
    await this.deps.stream.updateRecord({
      currentOffset: allocation.nextOffset,
      counter: allocation.endCounter,
      lifecycle: seq ? { lastSeq: seq } : undefined,
    });
    await this.deps.stream.events?.notify("message");
    return allocation.nextOffset;
  }

  async closeRecord(record: StreamRecord, data: Uint8Array[], seq?: string): Promise<string> {
    let latest = record;
    let nextOffset = record.currentOffset;
    if (data.length > 0) {
      nextOffset = await this.appendMessages(record, data, seq);
      latest = (await this.deps.stream.getRecord()) ?? record;
    } else {
      await this.deps.expiryPolicy.touch(this.deps.stream, record, "close");
    }
    await this.deps.stream.updateRecord({
      lifecycle: {
        closed: true,
        closedAt: this.deps.clock.now(),
        ...(seq ? { lastSeq: seq } : {}),
      },
      currentOffset: nextOffset,
      counter: latest.counter,
    });
    await this.deps.stream.events?.notify("closed");
    return nextOffset;
  }
}
